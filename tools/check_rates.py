"""
Per-sensor sample-rate diagnostic for FS recordings.

Loads a JSONL recording and reports counts, effective rate (Hz), and
recording duration for each sensor stream. Useful for isolating whether
slow effective rates are firmware-side (different fusion outputs at
different intrinsic ODRs) or BLE-bandwidth-side (uniform reduction
under load).

Usage:
    python3 tools/check_rates.py recordings/session-XXX.jsonl
"""
import argparse
import json
import sys
from collections import defaultdict
from pathlib import Path


def main():
    ap = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    ap.add_argument("path", help="JSONL recording to inspect.")
    args = ap.parse_args()

    counts: dict = defaultdict(int)
    first_t: dict = {}
    last_t: dict = {}
    with Path(args.path).open("r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            rec = json.loads(line)
            if "device" not in rec or "sensor" not in rec:
                continue
            sensor = rec["sensor"]
            counts[sensor] += 1
            t = rec.get("t_recv")
            if t is None:
                continue
            if sensor not in first_t:
                first_t[sensor] = t
            last_t[sensor] = t

    if not counts:
        print("no frame records found in recording", file=sys.stderr)
        return 1

    print(f"{args.path}")
    print(f"{'sensor':<22} | {'count':>7} | {'rate (Hz)':>11} | {'duration (s)':>13}")
    print("-" * 65)
    for sensor in sorted(counts.keys()):
        n = counts[sensor]
        if sensor in first_t and sensor in last_t:
            duration = last_t[sensor] - first_t[sensor]
            rate = (n - 1) / duration if duration > 0 else float("nan")
            rate_str = f"{rate:.2f}" if rate == rate else "nan"
            duration_str = f"{duration:.2f}"
        else:
            rate_str = "nan"
            duration_str = "nan"
        print(f"{sensor:<22} | {n:>7} | {rate_str:>11} | {duration_str:>13}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
