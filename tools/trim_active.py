#!/usr/bin/env python3
"""
Cut a recording down to its active stretches, for replay and tests.

Activity = rotation speed: the L2 norm of `gyro` / `corrected_gyro`
frames (deg/s) from any device. A moment is active when that exceeds
--threshold; active moments closer than --merge seconds join into one
segment, each segment is padded by --pad seconds on both sides, and
segments shorter than --min-length are dropped. Every frame from every
device inside a segment is kept, so multi-device timing is preserved.

Idle time between segments is collapsed to --gap seconds by shifting
later timestamps back, so `tools/replay_osc.py` doesn't sit silent
through it. Non-frame records (`_meta`, `_session`, `_gesture`) are
dropped except the `_meta` header, which gains a `trimmed_from` note.

    python tools/trim_active.py recordings/live-play-20261106.jsonl
    # -> recordings/live-play-20261106-active.jsonl
"""

import argparse
import json
import math
import sys
from pathlib import Path

ACTIVITY_SENSORS = ("gyro", "corrected_gyro")


def find_segments(path, threshold, merge, pad, min_length):
    active_t = []
    t_first = t_last = None
    with open(path, encoding="utf-8") as fh:
        for line in fh:
            try:
                rec = json.loads(line)
            except ValueError:
                continue  # tolerate a torn final line
            if "sensor" not in rec:
                continue
            t = rec["t_recv"]
            t_first = t if t_first is None else min(t_first, t)
            t_last = t if t_last is None else max(t_last, t)
            if rec["sensor"] in ACTIVITY_SENSORS:
                if math.sqrt(sum(v * v for v in rec["values"])) > threshold:
                    active_t.append(t)
    active_t.sort()
    segments = []
    for t in active_t:
        if segments and t - segments[-1][1] <= merge:
            segments[-1][1] = t
        else:
            segments.append([t, t])
    padded = []
    for a, b in segments:
        if b - a < min_length:
            continue
        a, b = max(t_first, a - pad), min(t_last, b + pad)
        if padded and a - padded[-1][1] <= merge:
            padded[-1][1] = max(padded[-1][1], b)
        else:
            padded.append([a, b])
    return padded, (t_first, t_last)


def write_trimmed(src, dst, segments, gap):
    # offset[i]: how much to shift timestamps in segment i back by.
    offsets, removed = [], 0.0
    for i, (a, b) in enumerate(segments):
        if i:
            removed += max(0.0, (a - segments[i - 1][1]) - gap)
        offsets.append(removed)
    kept = 0
    with open(src, encoding="utf-8") as fin, open(dst, "w", encoding="utf-8", newline="\n") as fout:
        seg = 0
        for line in fin:
            try:
                rec = json.loads(line)
            except ValueError:
                continue
            if "_meta" in rec:
                rec["trimmed_from"] = Path(src).name
                rec["trim_segments"] = [[round(a, 3), round(b, 3)] for a, b in segments]
                fout.write(json.dumps(rec) + "\n")
                continue
            if "sensor" not in rec:
                continue
            t = rec["t_recv"]
            # Frames are near-sorted by time; find the containing segment.
            while seg < len(segments) and t > segments[seg][1]:
                seg += 1
            if seg >= len(segments):
                # Late out-of-order frames could still belong earlier; scan.
                hit = next((i for i, (a, b) in enumerate(segments) if a <= t <= b), None)
            elif t >= segments[seg][0]:
                hit = seg
            else:
                hit = next((i for i, (a, b) in enumerate(segments[:seg]) if a <= t <= b), None)
            if hit is None:
                continue
            rec["t_recv"] = t - offsets[hit]
            fout.write(json.dumps(rec, separators=(",", ":")) + "\n")
            kept += 1
    return kept


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0],
                                 epilog=__doc__.split("\n\n", 1)[1],
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("paths", nargs="+", help="JSONL recording(s)")
    ap.add_argument("--threshold", type=float, default=60.0,
                    help="rotation speed (deg/s) counted as active (default 60)")
    ap.add_argument("--merge", type=float, default=3.0,
                    help="join active moments closer than this many seconds (default 3)")
    ap.add_argument("--pad", type=float, default=1.5,
                    help="seconds kept before/after each segment (default 1.5)")
    ap.add_argument("--min-length", type=float, default=2.0,
                    help="drop segments shorter than this many seconds (default 2)")
    ap.add_argument("--gap", type=float, default=1.0,
                    help="idle time left between segments after trimming (default 1)")
    ap.add_argument("--suffix", default="-active", help="output name suffix (default -active)")
    args = ap.parse_args(argv)

    for p in args.paths:
        src = Path(p)
        segments, (t0, t1) = find_segments(src, args.threshold, args.merge, args.pad, args.min_length)
        if not segments:
            print("%s: no active segments (threshold %.0f deg/s)" % (src.name, args.threshold))
            continue
        dst = src.with_name(src.stem + args.suffix + src.suffix)
        kept = write_trimmed(src, dst, segments, args.gap)
        active = sum(b - a for a, b in segments)
        print("%s: %d segment(s), %.0fs active of %.0fs -> %s (%d frames, %.1f MB)"
              % (src.name, len(segments), active, t1 - t0, dst.name, kept,
                 dst.stat().st_size / 1e6))
        for a, b in segments:
            print("    %7.1fs .. %7.1fs  (%.0fs)" % (a - t0, b - t0, b - a))
    return 0


if __name__ == "__main__":
    sys.exit(main())
