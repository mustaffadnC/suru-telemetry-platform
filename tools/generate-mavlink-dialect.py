#!/usr/bin/env python3
"""Generate the MAVLink dialect table (message id -> CRC_EXTRA, length bounds)
as a Java source file.

MAVLink v2 cannot validate a frame's checksum without the per-message CRC_EXTRA
byte, which is derived from the message's field definitions. The authoritative
values live in the generated C headers as MAVLINK_MESSAGE_CRCS:

    #define MAVLINK_MESSAGE_CRCS {{0, 50, 9, 9, 0, 0, 0}, ...}
                                   ^   ^  ^  ^
                                   |   |  |  max payload length (with extensions)
                                   |   |  min payload length
                                   |   CRC_EXTRA
                                   message id

Transcribing ~300 entries by hand is a defect waiting to happen, so the table is
generated and this script is checked in alongside it.

Source headers are the MIT-licensed MAVLink c_library_v2 (ardupilotmega dialect),
vendored in the kerkenez-gcs repository.

Usage:
    python3 tools/generate-mavlink-dialect.py <path-to-ardupilotmega.h> [-o <java-file>]
    python3 tools/generate-mavlink-dialect.py --selftest
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

PACKAGE = "io.github.mustaffadnc.suru.protocol.mavlink"
CLASS_NAME = "ArduPilotMegaDialect"

ENTRY_RE = re.compile(r"\{\s*(\d+)\s*,\s*(\d+)\s*,\s*(\d+)\s*,\s*(\d+)\s*,")


def parse_header(text: str) -> list[tuple[int, int, int, int]]:
    """Return sorted [(msgid, crc_extra, min_len, max_len)]."""
    marker = "#define MAVLINK_MESSAGE_CRCS"
    idx = text.find(marker)
    if idx < 0:
        raise SystemExit(f"MAVLINK_MESSAGE_CRCS not found")
    line_end = text.find("\n", idx)
    line = text[idx:line_end if line_end > 0 else len(text)]

    entries = [
        (int(m.group(1)), int(m.group(2)), int(m.group(3)), int(m.group(4)))
        for m in ENTRY_RE.finditer(line)
    ]
    if not entries:
        raise SystemExit("MAVLINK_MESSAGE_CRCS found but no entries parsed")

    entries.sort(key=lambda e: e[0])
    ids = [e[0] for e in entries]
    if len(set(ids)) != len(ids):
        raise SystemExit("duplicate message ids in table")
    for _, crc, lo, hi in entries:
        if not 0 <= crc <= 255:
            raise SystemExit(f"crc_extra out of byte range: {crc}")
        if lo > hi:
            raise SystemExit(f"min_len {lo} > max_len {hi}")
    return entries


def java_bytes(values: list[int]) -> str:
    """Format as Java byte literals, wrapped. Values >127 need a cast."""
    out = [f"(byte) {v}" if v > 127 else str(v) for v in values]
    return wrap(out)


def wrap(items: list[str], per_line: int = 8, indent: str = " " * 12) -> str:
    lines = []
    for i in range(0, len(items), per_line):
        lines.append(indent + ", ".join(items[i:i + per_line]))
    return ",\n".join(lines)


def render(entries: list[tuple[int, int, int, int]], source_note: str) -> str:
    ids = [e[0] for e in entries]
    crcs = [e[1] for e in entries]
    mins = [e[2] for e in entries]
    maxs = [e[3] for e in entries]

    return f"""package {PACKAGE};

/**
 * MAVLink dialect table for ArduPilotMega — message id to CRC_EXTRA and payload length bounds.
 *
 * <p><b>GENERATED FILE — do not edit by hand.</b> Regenerate with:
 *
 * <pre>python3 tools/generate-mavlink-dialect.py &lt;path-to-ardupilotmega.h&gt;</pre>
 *
 * <p>Derived from the MIT-licensed MAVLink {{@code c_library_v2}} headers ({source_note}).
 * The ardupilotmega dialect is required rather than plain common: ArduPilot emits its own
 * messages, and a decoder that does not know them cannot compute their checksum.
 *
 * <p>Lookup is a binary search over a sorted primitive array — no boxing, no map, no allocation
 * on the decode path.
 */
final class {CLASS_NAME} implements MavlinkDialect {{

    /** Sorted message ids. */
    private static final int[] MSG_IDS = {{
{wrap([str(v) for v in ids])}
    }};

    /** CRC_EXTRA per message id, parallel to {{@link #MSG_IDS}}. */
    private static final byte[] CRC_EXTRA = {{
{java_bytes(crcs)}
    }};

    /** Minimum payload length (pre-extension), parallel to {{@link #MSG_IDS}}. */
    private static final short[] MIN_PAYLOAD = {{
{wrap([str(v) for v in mins])}
    }};

    /** Maximum payload length (with extensions), parallel to {{@link #MSG_IDS}}. */
    private static final short[] MAX_PAYLOAD = {{
{wrap([str(v) for v in maxs])}
    }};

    static final {CLASS_NAME} INSTANCE = new {CLASS_NAME}();

    private {CLASS_NAME}() {{}}

    private static int indexOf(int msgId) {{
        int lo = 0;
        int hi = MSG_IDS.length - 1;
        while (lo <= hi) {{
            int mid = (lo + hi) >>> 1;
            int v = MSG_IDS[mid];
            if (v < msgId) {{
                lo = mid + 1;
            }} else if (v > msgId) {{
                hi = mid - 1;
            }} else {{
                return mid;
            }}
        }}
        return -1;
    }}

    @Override
    public int crcExtra(int msgId) {{
        int i = indexOf(msgId);
        return i < 0 ? UNKNOWN : (CRC_EXTRA[i] & 0xFF);
    }}

    @Override
    public int maxPayloadLength(int msgId) {{
        int i = indexOf(msgId);
        return i < 0 ? UNKNOWN : MAX_PAYLOAD[i];
    }}

    @Override
    public int minPayloadLength(int msgId) {{
        int i = indexOf(msgId);
        return i < 0 ? UNKNOWN : MIN_PAYLOAD[i];
    }}

    @Override
    public int messageCount() {{
        return MSG_IDS.length;
    }}

    @Override
    public String name() {{
        return "ardupilotmega";
    }}
}}
"""


def selftest() -> int:
    """Check the parser against a handful of values fixed by the MAVLink spec."""
    sample = (
        "#define MAVLINK_MESSAGE_CRCS {{0, 50, 9, 9, 0, 0, 0}, "
        "{1, 124, 31, 43, 0, 0, 0}, {2, 137, 12, 12, 0, 0, 0}, "
        "{33, 104, 28, 28, 0, 0, 0}}\n"
    )
    entries = parse_header(sample)
    expected = [(0, 50, 9, 9), (1, 124, 31, 43), (2, 137, 12, 12), (33, 104, 28, 28)]
    ok = entries == expected
    print(f"parsed: {entries}")
    print(f"selftest: {'OK' if ok else 'FAIL'}")

    # HEARTBEAT (0) CRC_EXTRA is 50 and SYSTEM_TIME (2) is 137 in every dialect;
    # those are stable anchors, so a mis-parse shows up immediately.
    return 0 if ok else 1


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("header", nargs="?", help="path to ardupilotmega.h")
    ap.add_argument("-o", "--output", help="Java file to write (default: stdout)")
    ap.add_argument("--selftest", action="store_true")
    args = ap.parse_args()

    if args.selftest:
        return selftest()
    if not args.header:
        ap.print_help()
        return 2

    path = Path(args.header)
    text = path.read_text(encoding="utf-8", errors="replace")
    entries = parse_header(text)

    java = render(entries, f"{path.name}")

    if args.output:
        Path(args.output).write_text(java, encoding="utf-8")
        print(
            f"wrote {args.output}: {len(entries)} messages, "
            f"id range {entries[0][0]}..{entries[-1][0]}",
            file=sys.stderr,
        )
    else:
        sys.stdout.write(java)
    return 0


if __name__ == "__main__":
    sys.exit(main())
