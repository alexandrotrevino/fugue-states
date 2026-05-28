"""
Per-sensor sample-rate diagnostic for FS recordings.

Loads a JSONL recording and reports counts, effective rate (Hz), and
recording duration for each sensor stream. Useful for isolating whether
slow effective rates are firmware-side (different fusion outputs at
different intrinsic ODRs) or BLE-bandwidth-side (uniform reduction
under load).

By default rows are broken out per-(device, sensor) with a per-device
subtotal line — for 2+ device sessions this is how you tell whether the
~200 Hz aggregate cap is per-device or shared across the adapter. Pass
`--aggregate` for the older sensor-only view (sums across devices).

Usage:
    python3 tools/check_rates.py recordings/session-XXX.jsonl
    python3 tools/check_rates.py --aggregate recordings/session-XXX.jsonl
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
    ap.add_argument(
        "--aggregate", action="store_true",
        help="Aggregate across devices (sensor-only rows). Default is per-device.",
    )
    args = ap.parse_args()

    # Key: (device, sensor) for default view; (sensor,) for --aggregate.
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
            device = rec["device"]
            key = (sensor,) if args.aggregate else (device, sensor)
            counts[key] += 1
            t = rec.get("t_recv")
            if t is None:
                continue
            if key not in first_t:
                first_t[key] = t
            last_t[key] = t

    if not counts:
        print("no frame records found in recording", file=sys.stderr)
        return 1

    print(args.path)

    if args.aggregate:
        print(f"{'sensor':<22} | {'count':>7} | {'rate (Hz)':>11} | {'duration (s)':>13}")
        print("-" * 65)
        for key in sorted(counts.keys()):
            sensor, = key
            _print_row(sensor, counts[key], first_t.get(key), last_t.get(key))
        return 0

    # Per-device default view: group rows by device, print a subtotal
    # line per device (sum of counts, max duration observed across that
    # device's sensors -> aggregate Hz).
    by_device: dict = defaultdict(list)
    for (device, sensor), n in counts.items():
        by_device[device].append(sensor)

    print(f"{'device':<19} {'sensor':<22} | {'count':>7} | {'rate (Hz)':>11} | {'duration (s)':>13}")
    print("-" * 86)
    grand_count = 0
    grand_duration = 0.0
    for device in sorted(by_device):
        device_count = 0
        device_duration = 0.0
        sensors = sorted(by_device[device])
        for sensor in sensors:
            key = (device, sensor)
            n = counts[key]
            ft = first_t.get(key)
            lt = last_t.get(key)
            _print_row(sensor, n, ft, lt, device=device)
            device_count += n
            if ft is not None and lt is not None:
                device_duration = max(device_duration, lt - ft)
        # Per-device aggregate row: sum of counts, agg rate via longest
        # per-sensor duration as the denominator (sensors share a session).
        agg_rate = (device_count / device_duration) if device_duration > 0 else float("nan")
        agg_rate_str = f"{agg_rate:.2f}" if agg_rate == agg_rate else "nan"
        agg_dur_str = f"{device_duration:.2f}"
        print(f"{device:<19} {'  TOTAL':<22} | {device_count:>7} | "
              f"{agg_rate_str:>11} | {agg_dur_str:>13}")
        print("-" * 86)
        grand_count += device_count
        grand_duration = max(grand_duration, device_duration)

    if len(by_device) > 1:
        grand_rate = (grand_count / grand_duration) if grand_duration > 0 else float("nan")
        grand_rate_str = f"{grand_rate:.2f}" if grand_rate == grand_rate else "nan"
        grand_dur_str = f"{grand_duration:.2f}"
        print(f"{'ALL DEVICES':<19} {'  TOTAL':<22} | {grand_count:>7} | "
              f"{grand_rate_str:>11} | {grand_dur_str:>13}")

    return 0


def _print_row(sensor: str, n: int, first: float, last: float, device: str = None):
    if first is not None and last is not None:
        duration = last - first
        rate = (n - 1) / duration if duration > 0 else float("nan")
        rate_str = f"{rate:.2f}" if rate == rate else "nan"
        duration_str = f"{duration:.2f}"
    else:
        rate_str = "nan"
        duration_str = "nan"
    if device is None:
        print(f"{sensor:<22} | {n:>7} | {rate_str:>11} | {duration_str:>13}")
    else:
        print(f"{device:<19} {sensor:<22} | {n:>7} | {rate_str:>11} | {duration_str:>13}")


if __name__ == "__main__":
    sys.exit(main())
