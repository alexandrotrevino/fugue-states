"""
Offline drift analysis for the IMU+ZUPT position-tracking spike.

Loads a JSONL recording (typically captured with `run_fs.py --record`
in Sensor Fusion mode), extracts the linear_acc / quat / corrected_gyro
streams, and runs three parallel position estimators side-by-side:

  (a) Naive double-integration of raw linear_acc (rotated to world frame).
  (b) Bias-subtracted double-integration (bias = mean of stationary
      baseline computed from the recording's first N samples).
  (c) Bias-subtracted + ZUPT (velocity reset on rolling-std stationary
      detection — the same logic as `sense.position.PositionTracker`,
      replicated here so we can re-run without touching the live stack).

Outputs:
- Per-track drift curves saved as PNG (position vs time, x/y/z).
- Numerical drift summary at 1s, 5s, 15s, 30s.
- Optional ground-truth overlay if a `--ground-truth` JSON of (t, x,
  y, z) waypoints is supplied.
- ZUPT diagnostics: stationary fraction, stationary-window length
  distribution.

Usage:
    python3 tools/analyze_position.py recordings/session-XXX.jsonl \\
        --output position_analysis/

Requires numpy + matplotlib. Run on the Pi or scp + run on Windows.
"""
import argparse
import json
import math
import sys
from collections import deque
from pathlib import Path
from typing import Iterator, List, Tuple

import numpy as np


# --- Loading -----------------------------------------------------------------

def load_fusion_streams(path):
    """Returns dict with parallel arrays: t_acc, lin_acc(N,3), t_quat,
    quat(M,4), t_gyro, gyro_mag(K). The streams are NOT time-aligned —
    we resample via "latest value" lookup at integration time, matching
    what PositionTracker does live."""
    t_acc, lin_acc = [], []
    t_quat, quat = [], []
    t_gyro, gyro_mag = [], []
    with Path(path).open("r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            rec = json.loads(line)
            if "device" not in rec:
                continue
            sensor = rec.get("sensor")
            t = rec.get("t_recv")
            v = rec.get("values")
            if not v or t is None:
                continue
            if sensor == "linear_acc" and len(v) >= 3:
                t_acc.append(t)
                lin_acc.append([v[0], v[1], v[2]])
            elif sensor == "quat" and len(v) >= 4:
                t_quat.append(t)
                quat.append([v[0], v[1], v[2], v[3]])
            elif sensor == "corrected_gyro" and len(v) >= 3:
                t_gyro.append(t)
                gyro_mag.append(math.sqrt(v[0] ** 2 + v[1] ** 2 + v[2] ** 2))
    return {
        "t_acc": np.array(t_acc),
        "lin_acc": np.array(lin_acc),
        "t_quat": np.array(t_quat),
        "quat": np.array(quat),
        "t_gyro": np.array(t_gyro),
        "gyro_mag": np.array(gyro_mag),
    }


# --- Math helpers ------------------------------------------------------------

def quat_rotate(q, v):
    """Rotate 3-vec v by quaternion q=(w,x,y,z). Same as sense.position."""
    w, x, y, z = q
    vx, vy, vz = v
    r00 = 1.0 - 2.0 * (y * y + z * z)
    r01 = 2.0 * (x * y - w * z)
    r02 = 2.0 * (x * z + w * y)
    r10 = 2.0 * (x * y + w * z)
    r11 = 1.0 - 2.0 * (x * x + z * z)
    r12 = 2.0 * (y * z - w * x)
    r20 = 2.0 * (x * z - w * y)
    r21 = 2.0 * (y * z + w * x)
    r22 = 1.0 - 2.0 * (x * x + y * y)
    return (
        r00 * vx + r01 * vy + r02 * vz,
        r10 * vx + r11 * vy + r12 * vz,
        r20 * vx + r21 * vy + r22 * vz,
    )


def latest_at(t_query, t_arr, val_arr):
    """For each query time, find the index of the latest sample at or
    before that time. Returns array of indices (-1 if no prior sample)."""
    return np.searchsorted(t_arr, t_query, side="right") - 1


# --- Position integration ----------------------------------------------------

def integrate(streams, mode, *,
              acc_std_threshold=0.15,
              gyro_threshold=8.0,
              zupt_window=10,
              calibration_samples=125):
    """
    mode in {"raw", "bias", "zupt"}. Returns (t, position[N,3], stationary[N]).

    All three modes run on the same per-acc-frame timeline; mode just
    selects what corrections are applied.
    """
    t_acc = streams["t_acc"]
    lin_acc = streams["lin_acc"]
    t_quat = streams["t_quat"]
    quat = streams["quat"]
    t_gyro = streams["t_gyro"]
    gyro_mag = streams["gyro_mag"]

    n = len(t_acc)
    pos = np.zeros((n, 3))
    vel = np.zeros((n, 3))
    stationary = np.zeros(n, dtype=bool)
    if n == 0:
        return t_acc, pos, stationary

    # Pre-rotate every linear_acc to world frame using the latest quat.
    quat_idx = latest_at(t_acc, t_quat, quat)
    acc_w = np.zeros((n, 3))
    for i in range(n):
        if quat_idx[i] < 0:
            continue
        acc_w[i] = quat_rotate(quat[quat_idx[i]], lin_acc[i])

    # Bias estimate (modes "bias" and "zupt").
    if mode in ("bias", "zupt"):
        first = min(calibration_samples, n)
        bias = acc_w[:first].mean(axis=0)
    else:
        bias = np.zeros(3)
    acc = acc_w - bias

    # ZUPT detector — run continuously regardless of mode (so the
    # `stationary` array is always informative for diagnostics).
    buf = deque(maxlen=zupt_window)
    gyro_idx = latest_at(t_acc, t_gyro, gyro_mag)
    for i in range(n):
        mag_w = math.sqrt(acc_w[i, 0] ** 2 + acc_w[i, 1] ** 2 + acc_w[i, 2] ** 2)
        buf.append(mag_w)
        if len(buf) < zupt_window:
            continue
        std = np.std(buf)
        gm = gyro_mag[gyro_idx[i]] if gyro_idx[i] >= 0 else 0.0
        stationary[i] = (std < acc_std_threshold) and (gm < gyro_threshold)

    # Integrate.
    for i in range(1, n):
        # Skip frames before calibration completes for bias/zupt modes.
        if mode in ("bias", "zupt") and i < calibration_samples:
            continue
        dt = max(0.0, t_acc[i] - t_acc[i - 1])
        if mode == "zupt" and stationary[i]:
            vel[i] = 0.0
        else:
            vel[i] = vel[i - 1] + acc[i] * dt
        pos[i] = pos[i - 1] + vel[i] * dt

    return t_acc, pos, stationary


# --- Reporting ---------------------------------------------------------------

def drift_at(t, pos, t0, dt):
    """Position drift (Euclidean magnitude from t0) at t0+dt."""
    if len(t) == 0:
        return float("nan")
    target = t0 + dt
    idx = np.searchsorted(t, target)
    if idx >= len(t):
        return float("nan")
    p0_idx = np.searchsorted(t, t0)
    if p0_idx >= len(t):
        return float("nan")
    return float(np.linalg.norm(pos[idx] - pos[p0_idx]))


def print_summary(streams, results):
    """Numerical drift summary at 1s/5s/15s/30s for each mode."""
    print("=" * 60)
    print("Drift summary")
    print("=" * 60)
    if not streams["t_acc"].size:
        print("  no linear_acc samples — empty recording")
        return
    t0 = streams["t_acc"][0]
    horizons = [1.0, 5.0, 15.0, 30.0]
    print(f"{'mode':<8} | {'1s':>10} | {'5s':>10} | {'15s':>10} | {'30s':>10}")
    print("-" * 60)
    for mode, (t, pos, _stat) in results.items():
        cells = []
        for dt in horizons:
            d = drift_at(t, pos, t0, dt)
            cells.append("nan" if math.isnan(d) else f"{d:.3f}m")
        print(f"{mode:<8} | " + " | ".join(f"{c:>10}" for c in cells))


def print_zupt_stats(stationary, t_acc):
    print("\n" + "=" * 60)
    print("ZUPT diagnostics")
    print("=" * 60)
    if stationary.size == 0:
        print("  (empty)")
        return
    frac = stationary.mean()
    print(f"  stationary fraction: {frac * 100:.1f}%  ({stationary.sum()} / {len(stationary)} samples)")
    # Run-length distribution of stationary windows.
    runs = []
    in_run, run_start = False, 0
    for i, s in enumerate(stationary):
        if s and not in_run:
            in_run, run_start = True, i
        elif not s and in_run:
            runs.append(t_acc[i - 1] - t_acc[run_start])
            in_run = False
    if in_run:
        runs.append(t_acc[-1] - t_acc[run_start])
    if runs:
        runs = np.array(runs)
        print(f"  stationary windows: n={len(runs)} min={runs.min():.2f}s "
              f"max={runs.max():.2f}s mean={runs.mean():.2f}s median={np.median(runs):.2f}s")
    else:
        print("  no stationary windows detected — try raising thresholds")


def plot_drift(results, output_dir):
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    # Older matplotlib (Pi/Buster ship 3.x but require explicit import to
    # register the '3d' projection — without this the 3D plot raises
    # ValueError("Unknown projection '3d'")).
    from mpl_toolkits.mplot3d import Axes3D  # noqa: F401

    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    fig, axes = plt.subplots(3, 1, figsize=(12, 10), sharex=True)
    axis_labels = ("x (m)", "y (m)", "z (m)")
    colors = {"raw": "tab:red", "bias": "tab:orange", "zupt": "tab:green"}

    if not results:
        return
    t0 = next(iter(results.values()))[0][0]
    for ax_idx, label in enumerate(axis_labels):
        ax = axes[ax_idx]
        for mode, (t, pos, _stat) in results.items():
            if len(t) == 0:
                continue
            ax.plot(t - t0, pos[:, ax_idx], color=colors.get(mode, "gray"),
                    label=mode, alpha=0.85, linewidth=1.2)
        ax.set_ylabel(label)
        ax.grid(True, alpha=0.3)
        ax.legend(loc="upper left")
    axes[-1].set_xlabel("seconds since start")
    fig.suptitle("Position drift — raw vs bias-subtracted vs ZUPT")
    fig.tight_layout()
    out_path = output_dir / "drift.png"
    fig.savefig(out_path, dpi=100)
    plt.close(fig)
    print(f"\n  saved {out_path}")

    # 3D trajectory of the ZUPT track. Wrapped defensively — the 2D
    # drift plot is the primary deliverable; if the 3D backend bails
    # for any reason, we still want the 2D PNG.
    if "zupt" in results:
        t, pos, _ = results["zupt"]
        if len(t) > 0:
            try:
                fig = plt.figure(figsize=(8, 8))
                ax = fig.add_subplot(111, projection="3d")
                ax.plot(pos[:, 0], pos[:, 1], pos[:, 2], color="tab:green",
                        linewidth=1.2)
                ax.scatter([0], [0], [0], color="black", s=40, label="start")
                ax.set_xlabel("x (m)")
                ax.set_ylabel("y (m)")
                ax.set_zlabel("z (m)")
                ax.set_title("ZUPT trajectory (3D)")
                ax.legend()
                fig.tight_layout()
                out_path = output_dir / "trajectory_3d.png"
                fig.savefig(out_path, dpi=100)
                plt.close(fig)
                print(f"  saved {out_path}")
            except BaseException as e:
                print(f"  WARN: 3D trajectory plot failed ({e}); skipping. "
                      f"drift.png still produced.")


# --- Main --------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("path", help="JSONL recording (must contain linear_acc, quat, corrected_gyro).")
    parser.add_argument("--output", default="position_analysis",
                        help="Directory for PNG output (default: ./position_analysis/).")
    parser.add_argument("--no-plots", action="store_true",
                        help="Skip plot generation; stats only.")
    parser.add_argument("--acc-std-threshold", type=float, default=0.15)
    parser.add_argument("--gyro-threshold", type=float, default=8.0)
    parser.add_argument("--zupt-window", type=int, default=10)
    parser.add_argument("--calibration-samples", type=int, default=125)
    args = parser.parse_args()

    print(f"loading {args.path}")
    streams = load_fusion_streams(args.path)
    print(f"  linear_acc: {len(streams['t_acc'])} samples")
    print(f"  quat:       {len(streams['t_quat'])} samples")
    print(f"  gyro:       {len(streams['t_gyro'])} samples")
    if not streams["t_acc"].size:
        print("ERROR: no linear_acc frames in recording. Did you run with "
              "Sensor Fusion configured for outputs=[linear_acc, quaternion, "
              "corrected_gyro]?", file=sys.stderr)
        return 1

    results = {}
    for mode in ("raw", "bias", "zupt"):
        results[mode] = integrate(
            streams, mode,
            acc_std_threshold=args.acc_std_threshold,
            gyro_threshold=args.gyro_threshold,
            zupt_window=args.zupt_window,
            calibration_samples=args.calibration_samples,
        )

    print_summary(streams, results)
    print_zupt_stats(results["zupt"][2], streams["t_acc"])

    if not args.no_plots:
        try:
            plot_drift(results, args.output)
        except ImportError as e:
            print(f"  matplotlib not available ({e}); use --no-plots", file=sys.stderr)
            return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
