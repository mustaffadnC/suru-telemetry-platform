#!/usr/bin/env python3
"""Independent reference implementation of the ÇARGE 'HK' log framing, used to
differential-test the Java decoder.

The framing logic here is a transcription of `tools/hk_log_reader.py` from the
HavaKarakolu-Firmware repository — the decoder written against the firmware that
produces these files. Keeping a copy in this repository is deliberate: the test
must run in CI without checking out an unrelated project, and a second, separately
written implementation is only useful as an oracle if it stays available.

    'H' 'K' | ver(1) | type(1) | len(1) | payload(len) | crc16(2)
    crc16 = CRC-16/CCITT-FALSE over ver..payload, little-endian.

Floats are rendered as raw IEEE-754 hex rather than decimals. Comparing decimals
would test Java's and Python's float formatting against each other (half-up
versus half-even) instead of testing whether both read the same bytes from the
same offsets.

Usage:
    python3 tools/hk-reference.py --generate protocol/src/test/resources/hk
    python3 tools/hk-reference.py --dump FL_0001.BIN
    python3 tools/hk-reference.py --selftest
"""

from __future__ import annotations

import argparse
import struct
import sys
from pathlib import Path

MAGIC = b"HK"
VERSION = 1

T_META, T_ENV, T_IMU, T_GPS, T_EVENT = 1, 2, 3, 4, 5


# ------------------------------------------------------------------ framing --

def crc16_ccitt_false(data: bytes, seed: int = 0xFFFF) -> int:
    crc = seed
    for b in data:
        crc ^= b << 8
        for _ in range(8):
            crc = ((crc << 1) ^ 0x1021) & 0xFFFF if crc & 0x8000 else (crc << 1) & 0xFFFF
    return crc


def parse_frames(blob: bytes):
    """Yield (type, payload); finish with (None, stats). Mirrors the firmware reader."""
    pos = 0
    good = bad_crc = resyncs = 0
    n = len(blob)
    while pos + 7 <= n:
        if blob[pos:pos + 2] != MAGIC or blob[pos + 2] != VERSION:
            pos += 1
            resyncs += 1
            continue
        plen = blob[pos + 4]
        total = 5 + plen + 2
        if pos + total > n:
            pos += 1
            resyncs += 1
            continue
        crc_stored = blob[pos + 5 + plen] | (blob[pos + 6 + plen] << 8)
        crc_calc = crc16_ccitt_false(blob[pos + 2:pos + 5 + plen])
        if crc_stored != crc_calc:
            pos += 1
            bad_crc += 1
            continue
        good += 1
        yield blob[pos + 3], bytes(blob[pos + 5:pos + 5 + plen])
        pos += total
    yield None, {"frames": good, "crcErrors": bad_crc,
                 "resyncBytes": resyncs, "tailBytes": n - pos}


def frame(typ: int, payload: bytes) -> bytes:
    body = bytes([VERSION, typ, len(payload)]) + payload
    crc = crc16_ccitt_false(body)
    return MAGIC + body + bytes([crc & 0xFF, crc >> 8])


# ------------------------------------------------------- canonical rendering --

def _f32(payload: bytes, off: int) -> str:
    return "%08x" % struct.unpack_from("<I", payload, off)[0]


def _f64(payload: bytes, off: int) -> str:
    return "%016x" % struct.unpack_from("<Q", payload, off)[0]


def canonical_line(typ: int, p: bytes) -> str:
    if typ == T_META:
        t, fw, rst, ver = struct.unpack("<IHBB", p)
        return f"META,t={t},fw={fw},reset={rst},logver={ver}"
    if typ == T_ENV:
        t, st = struct.unpack_from("<IB", p)
        return (f"ENV,t={t},state={st},"
                f"p={_f32(p, 5)},alt={_f32(p, 9)},tbmp={_f32(p, 13)},"
                f"t1={_f32(p, 17)},rh1={_f32(p, 21)},t2={_f32(p, 25)},"
                f"rh2={_f32(p, 29)},vbat={_f32(p, 33)},soc={_f32(p, 37)}")
    if typ == T_IMU:
        t = struct.unpack_from("<I", p)[0]
        return (f"IMU,t={t},ax={_f32(p, 4)},ay={_f32(p, 8)},az={_f32(p, 12)},"
                f"gx={_f32(p, 16)},gy={_f32(p, 20)},gz={_f32(p, 24)},"
                f"roll={_f32(p, 28)},pitch={_f32(p, 32)}")
    if typ == T_GPS:
        t = struct.unpack_from("<I", p)[0]
        sats, fix, valid = struct.unpack_from("<3B", p, 32)
        return (f"GPS,t={t},lat={_f64(p, 4)},lon={_f64(p, 12)},"
                f"alt={_f32(p, 20)},spd={_f32(p, 24)},crs={_f32(p, 28)},"
                f"sats={sats},fix={fix},valid={valid}")
    if typ == T_EVENT:
        t, frm, to, arg = struct.unpack("<IBBH", p)
        return f"EVENT,t={t},from={frm},to={to},arg={arg}"
    return f"UNKNOWN,type={typ},len={len(p)},payload={p.hex()}"


def dump(blob: bytes) -> str:
    lines = []
    stats = {}
    for typ, payload in parse_frames(blob):
        if typ is None:
            stats = payload
            break
        lines.append(canonical_line(typ, payload))
    lines.append("#stats frames={frames} crcErrors={crcErrors} "
                 "resyncBytes={resyncBytes} tailBytes={tailBytes}".format(**stats))
    return "\n".join(lines) + "\n"


# ------------------------------------------------------------------ fixture --

def build_fixture() -> bytes:
    """A deterministic blob covering every path the decoder has to survive."""
    meta = struct.pack("<IHBB", 0, 0x0200, 3, VERSION)
    env = struct.pack("<IB9f", 1500, 2, 101325.0, 1.5, 25.0,
                      24.5, 40.0, 26.0, 42.0, 11.8, 0.9)
    imu = struct.pack("<I8f", 1600, 0.01, -0.02, 9.81,
                      0.001, -0.002, 0.003, 1.25, -0.5)
    gps = struct.pack("<I2d3f3B", 1700, 39.925533, 32.866287,
                      850.0, 3.5, 271.25, 11, 3, 1)
    event = struct.pack("<IBBH", 2100, 2, 3, 0)

    # A frame whose checksum has been broken: must be skipped, not fatal.
    corrupted = bytearray(frame(T_ENV, env))
    corrupted[9] ^= 0xFF

    # A frame truncated mid-payload at the very end of the file: the shape a
    # capsule that lost power mid-write actually leaves behind.
    torn_tail = frame(T_IMU, imu)[:20]

    return b"".join([
        b"\n\nInit capsule build 0200\n",   # boot banner, like ArduPilot's
        frame(T_META, meta),
        frame(T_ENV, env),
        b"HK\x01",                          # false magic: header starts, nothing follows
        frame(T_IMU, imu),
        bytes(corrupted),
        frame(T_GPS, gps),
        b"\x00\xff\x00\xff",                # mid-stream noise
        frame(T_EVENT, event),
        torn_tail,
    ])


# ----------------------------------------------------------------- selftest --

def selftest() -> int:
    blob = build_fixture()
    text = dump(blob)
    stats_line = text.strip().splitlines()[-1]
    lines = text.strip().splitlines()[:-1]

    checks = {
        "5 records decoded": len(lines) == 5,
        "META first": lines[0].startswith("META,t=0,fw=512,reset=3,logver=1"),
        "ENV decoded": lines[1].startswith("ENV,t=1500,state=2,"),
        "IMU decoded": lines[2].startswith("IMU,t=1600,"),
        "GPS decoded": lines[3].startswith("GPS,t=1700,"),
        "EVENT decoded": lines[4] == "EVENT,t=2100,from=2,to=3,arg=0",
        # Two checksum failures, not one. The deliberately corrupted frame is the
        # obvious one. The second comes from the bare b"HK\x01" spliced into the
        # stream: it is not merely unrecognised, it *parses* — the following frame's
        # own magic supplies a plausible type and length, so the parser reads a
        # frame-shaped span and only the checksum rejects it. That is precisely why
        # recovery advances a single byte instead of skipping the claimed length.
        "corruption seen": "crcErrors=2" in stats_line,
        "resync happened": "resyncBytes=" in stats_line and "resyncBytes=0 " not in stats_line,
    }
    print(text)
    for name, ok in checks.items():
        print(f"  {'ok  ' if ok else 'FAIL'} {name}")
    passed = all(checks.values())
    print(f"selftest: {'OK' if passed else 'FAIL'}")
    return 0 if passed else 1


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--generate", metavar="DIR",
                    help="write sample.bin and sample.expected.txt into DIR")
    ap.add_argument("--dump", metavar="BIN", help="dump an existing log file")
    ap.add_argument("--selftest", action="store_true")
    args = ap.parse_args()

    if args.selftest:
        return selftest()

    if args.generate:
        out = Path(args.generate)
        out.mkdir(parents=True, exist_ok=True)
        blob = build_fixture()
        (out / "sample.bin").write_bytes(blob)
        text = ("# Generated by tools/hk-reference.py — do not edit.\n"
                "# Oracle for HkDecoder: the Java decoder must reproduce this exactly.\n"
                + dump(blob))
        (out / "sample.expected.txt").write_text(text, encoding="utf-8", newline="\n")
        print(f"wrote {out}/sample.bin ({len(blob)} bytes) and sample.expected.txt",
              file=sys.stderr)
        return 0

    if args.dump:
        sys.stdout.write(dump(Path(args.dump).read_bytes()))
        return 0

    ap.print_help()
    return 2


if __name__ == "__main__":
    sys.exit(main())
