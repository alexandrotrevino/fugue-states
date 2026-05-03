"""
Exploratory analysis of gesture recordings.

Loads JSONL files produced by `run_fs.py --capture-label`, extracts
labeled gesture instances, and emits:

1. Per-label stats to stdout (count, length distribution, value
   distribution per feature sensor).
2. Pairwise DTW distance summaries — intra-label and cross-label,
   computed BOTH with z-scoring and on raw values. The intra/cross
   gap is the discrimination ratio: how much further apart different
   gesture classes are than instances of the same class.
3. PNG plots (per label): side-by-side raw vs z-scored traces of
   every instance overlaid, one row per feature sensor.
4. Background analysis (optional): for each template, the minimum
   DTW distance against sliding windows of a no-gesture recording.
   Low values mean the background contains motion that resembles
   the template — i.e. the source of false positives.

Usage (run on Pi or Windows; just needs the recordings + numpy +
matplotlib + dtaidistance):

    python3 tools/analyze_gestures.py \\
        recordings/gesture-wave-XXX.jsonl \\
        recordings/gesture-chop-XXX.jsonl \\
        --background recordings/session-walking-XXX.jsonl \\
        --output gesture_analysis/

Plots land in --output (default ./gesture_analysis/). Stats go to
stdout. Run with `--no-plots` to skip the matplotlib step entirely.
"""
import argparse
import json
import sys
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Tuple

import numpy as np


# --- Loading -----------------------------------------------------------------

def load_gestures(paths, feature_sensors):
    """Returns list of dicts: label, device, instance, source_file,
    raw (dict[sensor] -> 1D ndarray of values within the gesture window)."""
    out = []
    for path in paths:
        path = Path(path)
        active = None
        per_sensor: Dict[str, List[float]] = {s: [] for s in feature_sensors}
        with path.open("r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                rec = json.loads(line)
                kind = rec.get("_gesture")
                if kind == "start":
                    active = {
                        "label": rec["label"],
                        "device": rec["device"],
                        "instance": rec["instance"],
                    }
                    per_sensor = {s: [] for s in feature_sensors}
                elif kind == "end":
                    if active and all(per_sensor[s] for s in feature_sensors):
                        n = min(len(per_sensor[s]) for s in feature_sensors)
                        raw = {
                            s: np.array(per_sensor[s][:n], dtype=float)
                            for s in feature_sensors
                        }
                        out.append({**active, "source_file": path.name, "raw": raw})
                    active = None
                elif active is not None and "device" in rec:
                    sensor = rec.get("sensor")
                    if (sensor in feature_sensors
                            and rec.get("device") == active["device"]
                            and rec.get("values")):
                        per_sensor[sensor].append(float(rec["values"][0]))
    return out


def load_background(path, feature_sensors):
    """Load all frames matching feature_sensors from a non-gesture
    recording into per-sensor 1D arrays."""
    per_sensor: Dict[str, List[float]] = {s: [] for s in feature_sensors}
    with Path(path).open("r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            rec = json.loads(line)
            # Skip metadata / session / gesture markers — they don't have "device".
            if "device" not in rec:
                continue
            sensor = rec.get("sensor")
            if sensor in feature_sensors and rec.get("values"):
                per_sensor[sensor].append(float(rec["values"][0]))
    n = min(len(per_sensor[s]) for s in feature_sensors)
    return {s: np.array(per_sensor[s][:n], dtype=float) for s in feature_sensors}


# --- Math helpers ------------------------------------------------------------

def stack_features(raw_dict, feature_sensors):
    """(n, k) ndarray, columns ordered to match feature_sensors."""
    n = min(len(raw_dict[s]) for s in feature_sensors)
    return np.stack([raw_dict[s][:n] for s in feature_sensors], axis=1).astype(np.double)


def zscore_columns(arr):
    arr = np.asarray(arr, dtype=np.double)
    if arr.size == 0:
        return arr.copy()
    mean = arr.mean(axis=0)
    std = arr.std(axis=0)
    safe = np.where(std < 1e-9, 1.0, std)
    out = (arr - mean) / safe
    out[:, std < 1e-9] = 0.0
    return out


# --- Stats -------------------------------------------------------------------

def print_per_label_stats(gestures, feature_sensors):
    by_label = defaultdict(list)
    for g in gestures:
        by_label[g["label"]].append(g)
    print("=" * 60)
    print("Per-label statistics")
    print("=" * 60)
    for label, items in sorted(by_label.items()):
        print(f"\n[{label}]  count={len(items)}")
        lengths = [len(g["raw"][feature_sensors[0]]) for g in items]
        print(f"  window length: min={min(lengths)} max={max(lengths)} "
              f"mean={np.mean(lengths):.1f}")
        for s in feature_sensors:
            all_vals = np.concatenate([g["raw"][s] for g in items])
            print(f"  {s:>15}: min={all_vals.min():>8.3f}  max={all_vals.max():>8.3f}  "
                  f"mean={all_vals.mean():>8.3f}  std={all_vals.std():>8.3f}")
        print(f"  source files: {sorted({g['source_file'] for g in items})}")


# --- Distance matrices -------------------------------------------------------

def compute_distance_summary(gestures, feature_sensors, zscore, band, psi):
    """Returns (intra_by_label, cross_by_pair, intra_max, cross_min)."""
    from dtaidistance import dtw_ndim

    # Pre-stack each gesture (raw or z-scored).
    arrays = []
    for g in gestures:
        a = stack_features(g["raw"], feature_sensors)
        if zscore:
            a = zscore_columns(a)
        arrays.append(a)

    by_label_idx = defaultdict(list)
    for i, g in enumerate(gestures):
        by_label_idx[g["label"]].append(i)

    intra_by_label: Dict[str, List[float]] = defaultdict(list)
    cross_by_pair: Dict[Tuple[str, str], List[float]] = defaultdict(list)

    labels = sorted(by_label_idx.keys())
    for li, label_a in enumerate(labels):
        idxs_a = by_label_idx[label_a]
        # Intra
        for ai, i in enumerate(idxs_a):
            for j in idxs_a[ai + 1:]:
                d = dtw_ndim.distance_fast(arrays[i], arrays[j],
                                           window=band, psi=psi)
                intra_by_label[label_a].append(d)
        # Cross
        for label_b in labels[li + 1:]:
            idxs_b = by_label_idx[label_b]
            for i in idxs_a:
                for j in idxs_b:
                    d = dtw_ndim.distance_fast(arrays[i], arrays[j],
                                               window=band, psi=psi)
                    cross_by_pair[(label_a, label_b)].append(d)

    intra_max = max((max(ds) for ds in intra_by_label.values() if ds), default=0.0)
    cross_min = min((min(ds) for ds in cross_by_pair.values() if ds),
                    default=float("inf"))
    return intra_by_label, cross_by_pair, intra_max, cross_min


def print_distance_summary(gestures, feature_sensors, band, psi):
    print("\n" + "=" * 60)
    print(f"Pairwise DTW distance summary (band={band}, psi={psi})")
    print("=" * 60)
    for zscore in (True, False):
        intra, cross, intra_max, cross_min = compute_distance_summary(
            gestures, feature_sensors, zscore, band, psi
        )
        tag = "z-scored" if zscore else "raw"
        print(f"\n--- {tag} ---")
        print("intra-label (consistency of training within a class):")
        for label, ds in sorted(intra.items()):
            if not ds:
                continue
            print(f"  {label:>15}: n={len(ds):>3}  "
                  f"min={min(ds):>8.3f}  max={max(ds):>8.3f}  "
                  f"mean={np.mean(ds):>8.3f}  std={np.std(ds):>8.3f}")
        print("cross-label (separability between classes):")
        for (l1, l2), ds in sorted(cross.items()):
            if not ds:
                continue
            print(f"  {l1:>10} vs {l2:<10}: n={len(ds):>3}  "
                  f"min={min(ds):>8.3f}  max={max(ds):>8.3f}  "
                  f"mean={np.mean(ds):>8.3f}")
        if intra_max > 0 and cross_min < float("inf"):
            ratio = cross_min / intra_max
            verdict = "GOOD" if ratio > 1.5 else "MARGINAL" if ratio > 1.0 else "POOR"
            print(f"\n  discrimination ratio (cross_min / intra_max) = "
                  f"{cross_min:.3f} / {intra_max:.3f} = {ratio:.2f}  [{verdict}]")
            print("  (>1.5 = clear separation; ~1.0 = overlapping; <1.0 = unseparable)")


# --- Background analysis -----------------------------------------------------

def analyze_background(bg_dict, gestures, feature_sensors, window, band, psi):
    """For each template, compute the minimum DTW distance against
    sliding windows of the background recording. Low values mean the
    background contains motion that resembles the template — direct
    measure of false-positive risk."""
    from dtaidistance import dtw_ndim

    bg_arr = stack_features(bg_dict, feature_sensors)
    n = bg_arr.shape[0]
    if n < window:
        print(f"\nbackground too short ({n} samples < {window}); skipping")
        return

    print("\n" + "=" * 60)
    print(f"Background analysis ({n} samples; window={window})")
    print("=" * 60)

    by_label = defaultdict(list)
    for g in gestures:
        by_label[g["label"]].append(g)

    for label, items in sorted(by_label.items()):
        per_template_min_z = []
        per_template_min_raw = []
        for tmpl in items:
            tmpl_raw = stack_features(tmpl["raw"], feature_sensors)
            tmpl_z = zscore_columns(tmpl_raw)
            ds_z, ds_raw = [], []
            # Stride 5 to keep this fast; finer stride would give a
            # tighter min but at this granularity we already see the
            # shape of the background-vs-template landscape.
            for start in range(0, n - window + 1, 5):
                w_raw = bg_arr[start:start + window]
                w_z = zscore_columns(w_raw)
                ds_z.append(dtw_ndim.distance_fast(
                    w_z, tmpl_z, window=band, psi=psi))
                ds_raw.append(dtw_ndim.distance_fast(
                    w_raw, tmpl_raw, window=band, psi=psi))
            per_template_min_z.append(min(ds_z))
            per_template_min_raw.append(min(ds_raw))
        print(f"\n[{label}] min DTW distance from any background window to each template:")
        print(f"  z-scored: min={min(per_template_min_z):.3f}  "
              f"max={max(per_template_min_z):.3f}  "
              f"mean={np.mean(per_template_min_z):.3f}")
        print(f"  raw:      min={min(per_template_min_raw):.3f}  "
              f"max={max(per_template_min_raw):.3f}  "
              f"mean={np.mean(per_template_min_raw):.3f}")
    print("\n  Compare these to the intra-label distances above.")
    print("  If background-min ≤ intra-label distances, false positives are")
    print("  algorithmically unavoidable at the current threshold.")


# --- Plots -------------------------------------------------------------------

def plot_per_label_traces(gestures, feature_sensors, output_dir):
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    by_label = defaultdict(list)
    for g in gestures:
        by_label[g["label"]].append(g)

    for label, items in by_label.items():
        n_features = len(feature_sensors)
        fig, axes = plt.subplots(
            n_features, 2, figsize=(12, 3 * n_features), squeeze=False,
        )
        for f_idx, sensor in enumerate(feature_sensors):
            ax_raw = axes[f_idx][0]
            ax_z = axes[f_idx][1]
            for g in items:
                vals = g["raw"][sensor]
                ax_raw.plot(vals, alpha=0.5, linewidth=1)
                std = vals.std()
                z = (vals - vals.mean()) / (std if std > 1e-9 else 1.0)
                ax_z.plot(z, alpha=0.5, linewidth=1)
            ax_raw.set_title(f"{label} — {sensor} (raw)")
            ax_z.set_title(f"{label} — {sensor} (z-scored)")
            for ax in (ax_raw, ax_z):
                ax.set_xlabel("sample")
                ax.grid(True, alpha=0.3)
        fig.tight_layout()
        out_path = output_dir / f"label-{label}.png"
        fig.savefig(out_path, dpi=100)
        plt.close(fig)
        print(f"  saved {out_path}")

    # Cross-label overlay (raw acc_mag only — most discriminative)
    if len(by_label) >= 2:
        fig, axes = plt.subplots(
            len(feature_sensors), 1,
            figsize=(10, 3 * len(feature_sensors)), squeeze=False,
        )
        colors = plt.cm.tab10.colors
        for f_idx, sensor in enumerate(feature_sensors):
            ax = axes[f_idx][0]
            for c_idx, (label, items) in enumerate(sorted(by_label.items())):
                color = colors[c_idx % len(colors)]
                for k, g in enumerate(items):
                    ax.plot(g["raw"][sensor], color=color, alpha=0.4,
                            linewidth=1, label=label if k == 0 else None)
            ax.set_title(f"All labels — {sensor} (raw)")
            ax.set_xlabel("sample")
            ax.grid(True, alpha=0.3)
            ax.legend(loc="upper right")
        fig.tight_layout()
        out_path = output_dir / "all-labels-raw.png"
        fig.savefig(out_path, dpi=100)
        plt.close(fig)
        print(f"  saved {out_path}")


# --- Main --------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("paths", nargs="+",
                        help="JSONL gesture-recording files to analyze.")
    parser.add_argument("--feature-sensors", default="acc_mag,gyro_mag",
                        help="Comma-separated feature sensors (default: acc_mag,gyro_mag).")
    parser.add_argument("--background", default=None,
                        help="Optional path to a no-gesture recording for false-positive analysis.")
    parser.add_argument("--output", default="gesture_analysis",
                        help="Directory for PNG output (default: ./gesture_analysis/).")
    parser.add_argument("--no-plots", action="store_true",
                        help="Skip plot generation; stats only.")
    parser.add_argument("--band", type=int, default=10,
                        help="Sakoe-Chiba band radius for DTW (default: 10).")
    parser.add_argument("--psi", type=int, default=10,
                        help="Subsequence relaxation for DTW (default: 10).")
    parser.add_argument("--window", type=int, default=50,
                        help="Sliding window size for background analysis (default: 50).")
    args = parser.parse_args()

    feature_sensors = tuple(s.strip() for s in args.feature_sensors.split(",") if s.strip())

    print(f"loading gestures from {len(args.paths)} file(s) "
          f"with feature_sensors={feature_sensors}")
    gestures = load_gestures(args.paths, feature_sensors)
    if not gestures:
        print("ERROR: no gestures extracted. Check that recordings contain "
              "_gesture markers and that feature_sensors match the recorded streams.",
              file=sys.stderr)
        return 1
    print(f"loaded {len(gestures)} gesture instance(s).")

    print_per_label_stats(gestures, feature_sensors)
    print_distance_summary(gestures, feature_sensors, args.band, args.psi)

    if args.background:
        bg = load_background(args.background, feature_sensors)
        analyze_background(bg, gestures, feature_sensors,
                           args.window, args.band, args.psi)

    if not args.no_plots:
        print("\n" + "=" * 60)
        print("Plots")
        print("=" * 60)
        try:
            plot_per_label_traces(gestures, feature_sensors, args.output)
        except ImportError as e:
            print(f"  matplotlib not available ({e}); use --no-plots or pip install matplotlib")
            return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
