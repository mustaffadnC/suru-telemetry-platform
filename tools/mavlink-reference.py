#!/usr/bin/env python3
"""Independent reference decoder for MAVLink v1/v2 framing, used to
differential-test the Java decoder.

Written from the MAVLink specification rather than from the Java implementation:
an oracle that shares the code under test's assumptions cannot falsify them.

    v2:  0xFD | len | incompat | compat | seq | sysid | compid | msgid(3)
              | payload(len) | crc(2) [ | signature(13) if incompat & 0x01 ]
    v1:  0xFE | len | seq | sysid | compid | msgid(1) | payload(len) | crc(2)

The checksum is CRC-16/MCRF4XX over everything from the length byte through the
payload, then one more byte: the message's CRC_EXTRA. It is *not* CRC-16/X-25 —
X-25 finishes with an XOR of 0xFFFF, MAVLink does not.

CRC_EXTRA values come from the generated C headers' MAVLINK_MESSAGE_CRCS table.

Usage:
    python3 tools/mavlink-reference.py --header <ardupilotmega.h> --generate <dir> <stream.bin>
    python3 tools/mavlink-reference.py --header <ardupilotmega.h> --dump <stream.bin>
    python3 tools/mavlink-reference.py --selftest
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

ENTRY_RE = re.compile(r"\{\s*(\d+)\s*,\s*(\d+)\s*,")

STX_V1 = 0xFE
STX_V2 = 0xFD
SIGNATURE_LENGTH = 13


def load_crc_extra(header_path: Path) -> dict[int, int]:
    text = header_path.read_text(encoding="utf-8", errors="replace")
    idx = text.find("#define MAVLINK_MESSAGE_CRCS")
    if idx < 0:
        raise SystemExit("MAVLINK_MESSAGE_CRCS not found")
    end = text.find("\n", idx)
    line = text[idx:end if end > 0 else len(text)]
    table = {int(m.group(1)): int(m.group(2)) for m in ENTRY_RE.finditer(line)}
    if not table:
        raise SystemExit("no CRC_EXTRA entries parsed")
    return table


def crc_mcrf4xx(data: bytes, seed: int = 0xFFFF) -> int:
    crc = seed
    for b in data:
        crc ^= b
        for _ in range(8):
            crc = (crc >> 1) ^ 0x8408 if crc & 1 else crc >> 1
    return crc & 0xFFFF


def decode(blob: bytes, crc_extra: dict[int, int]):
    """Yield frame dicts, then a final stats dict."""
    pos = 0
    n = len(blob)
    frames = crc_errors = unknown = resync = lost = signed = v1_frames = 0
    last_seq: dict[int, int] = {}

    while pos < n:
        b = blob[pos]

        if b == STX_V2 and pos + 10 <= n:
            plen = blob[pos + 1]
            incompat = blob[pos + 2]
            siglen = SIGNATURE_LENGTH if incompat & 0x01 else 0
            total = 10 + plen + 2 + siglen
            if pos + total <= n:
                msgid = blob[pos + 7] | (blob[pos + 8] << 8) | (blob[pos + 9] << 16)
                extra = crc_extra.get(msgid)
                if extra is None:
                    unknown += 1
                    pos += total
                    continue
                calc = crc_mcrf4xx(blob[pos + 1:pos + 10 + plen])
                calc = crc_mcrf4xx(bytes([extra]), calc)
                stored = blob[pos + 10 + plen] | (blob[pos + 11 + plen] << 8)
                if calc == stored:
                    seq, sysid, compid = blob[pos + 4], blob[pos + 5], blob[pos + 6]
                    key = (sysid << 8) | compid
                    if key in last_seq:
                        expected = (last_seq[key] + 1) & 0xFF
                        if seq != expected:
                            lost += (seq - expected) & 0xFF
                    last_seq[key] = seq
                    frames += 1
                    if siglen:
                        signed += 1
                    yield {"v": 2, "sys": sysid, "comp": compid, "msg": msgid,
                           "seq": seq, "payload": bytes(blob[pos + 10:pos + 10 + plen])}
                    pos += total
                    continue
                # A checksum failure is charged to crc_errors, not to resync.
                crc_errors += 1
                pos += 1
                continue

        elif b == STX_V1 and pos + 6 <= n:
            plen = blob[pos + 1]
            total = 6 + plen + 2
            if pos + total <= n:
                msgid = blob[pos + 5]
                extra = crc_extra.get(msgid)
                if extra is None:
                    unknown += 1
                    pos += total
                    continue
                calc = crc_mcrf4xx(blob[pos + 1:pos + 6 + plen])
                calc = crc_mcrf4xx(bytes([extra]), calc)
                stored = blob[pos + 6 + plen] | (blob[pos + 7 + plen] << 8)
                if calc == stored:
                    seq, sysid, compid = blob[pos + 2], blob[pos + 3], blob[pos + 4]
                    key = (sysid << 8) | compid
                    if key in last_seq:
                        expected = (last_seq[key] + 1) & 0xFF
                        if seq != expected:
                            lost += (seq - expected) & 0xFF
                    last_seq[key] = seq
                    frames += 1
                    v1_frames += 1
                    yield {"v": 1, "sys": sysid, "comp": compid, "msg": msgid,
                           "seq": seq, "payload": bytes(blob[pos + 6:pos + 6 + plen])}
                    pos += total
                    continue
                crc_errors += 1
                pos += 1
                continue

        pos += 1
        resync += 1

    yield {"stats": True, "frames": frames, "crcErrors": crc_errors,
           "unknown": unknown, "resyncBytes": resync, "lost": lost,
           "signed": signed, "v1": v1_frames, "endpoints": len(last_seq)}


def dump(blob: bytes, crc_extra: dict[int, int]) -> str:
    lines = []
    stats = {}
    for item in decode(blob, crc_extra):
        if item.get("stats"):
            stats = item
            break
        lines.append("v{v},sys={sys},comp={comp},msg={msg},seq={seq},payload={p}".format(
            p=item["payload"].hex(), **item))
    lines.append(
        "#stats frames={frames} crcErrors={crcErrors} unknown={unknown} "
        "resyncBytes={resyncBytes} lost={lost} signed={signed} v1={v1} "
        "endpoints={endpoints}".format(**stats))
    return "\n".join(lines) + "\n"


def selftest() -> int:
    # HEARTBEAT: msgid 0, CRC_EXTRA 50, 9-byte payload.
    crc_extra = {0: 50}
    payload = bytes(range(9))
    body = bytes([9, 0, 0, 7, 1, 1, 0, 0, 0]) + payload
    crc = crc_mcrf4xx(body)
    crc = crc_mcrf4xx(bytes([50]), crc)
    frame = bytes([STX_V2]) + body + bytes([crc & 0xFF, crc >> 8])

    blob = b"garbage\x00\x01" + frame + b"\xfd\x00"
    out = dump(blob, crc_extra)
    ok = ("v2,sys=1,comp=1,msg=0,seq=7,payload=" + payload.hex()) in out \
         and "frames=1" in out and "crcErrors=0" in out
    print(out)
    print(f"selftest: {'OK' if ok else 'FAIL'}")
    return 0 if ok else 1


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--header", help="path to ardupilotmega.h")
    ap.add_argument("--generate", metavar="DIR", help="write <name>.expected.txt into DIR")
    ap.add_argument("--dump", action="store_true")
    ap.add_argument("stream", nargs="?", help="binary stream to decode")
    ap.add_argument("--selftest", action="store_true")
    args = ap.parse_args()

    if args.selftest:
        return selftest()
    if not args.header or not args.stream:
        ap.print_help()
        return 2

    crc_extra = load_crc_extra(Path(args.header))
    blob = Path(args.stream).read_bytes()
    text = dump(blob, crc_extra)

    if args.generate:
        out = Path(args.generate)
        out.mkdir(parents=True, exist_ok=True)
        name = Path(args.stream).stem
        header = ("# Generated by tools/mavlink-reference.py — do not edit.\n"
                  f"# Oracle for MavlinkDecoder over {Path(args.stream).name}.\n")
        (out / f"{name}.expected.txt").write_text(header + text, encoding="utf-8", newline="\n")
        print(f"wrote {out}/{name}.expected.txt ({text.count(chr(10))} lines)", file=sys.stderr)
        return 0

    sys.stdout.write(text)
    return 0


if __name__ == "__main__":
    sys.exit(main())
