"""
Multivariate gesture recognition via DTW (dtaidistance).

Multivariate DTW with Sakoe-Chiba band + subsequence relaxation. Each
gesture is matched in a feature space defined by a tuple of pipeline
sensors. The feature set is **auto-detected by default** from the
gesture-capture JSONLs (intersection of scalar streams present in
every captured window) so the recognizer uses whatever the recording
pipelines were producing — typically `acc_mag, gyro_mag, tilt,
acc_lp_mag` for the wrist-IMU config. Pass `feature_sensors=...`
explicitly (or `--gesture-features` from the CLI) to override.

Components:

- `_discover_feature_sensors(paths)` — scans recording JSONLs and
  returns the intersection of scalar (`len(values)==1`) sensors that
  appear in every gesture window. Used as the default for
  `GestureLibrary.from_files` when no explicit feature_sensors given.
- `_zscore_columns(arr)` — column-wise z-norm; flat columns zeroed.
  Optional per-library: data analysis on real captures showed raw
  multivariate distances often discriminate better than z-scored
  ones, since amplitude carries information that z-norm flattens.
  Default is raw (zscore=False); pass `--gesture-zscore` to opt in.
- `Template` — one labeled instance, `feature_series` is an
  `np.ndarray` of shape `(n_samples, n_features)`. Z-normed at build
  iff library.zscore is True; otherwise stored raw.
- `GestureLibrary.from_files(paths, ...)` — loads templates from
  `--capture-label` JSONL recordings, optionally filters outliers
  per label via Median Absolute Deviation, computes per-label
  thresholds from intra-label pairwise DTW.
- `GestureRecognizer(library, ...)` — Pipeline Stage. Inserted into
  every pipeline carrying a feature sensor. Maintains per-(device,
  sensor) buffers; ticks on the *primary* feature only
  (`feature_sensors[0]`); zips per-sensor buffers into a multivariate
  signal at tick time, optionally z-norms (matching the library),
  runs DTW against every template.

DTW backed by `dtaidistance.dtw_ndim.distance_fast` (C, multivariate,
Sakoe-Chiba `window`, subsequence `psi`). Z-normalization (when
enabled) happens in this module — dtaidistance does not z-norm
internally.

Per-tick recognizer latency scales with `n_features × n_templates`;
the cost is observable via `Pipeline.stats[GestureRecognizer]` after
a run. To compare configurations, re-run with different
`--gesture-features` sets and read the mean/max timings.
"""
import json
import logging
import time
from collections import defaultdict, deque
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Optional, Set, Tuple

import numpy as np
from dtaidistance import dtw_ndim

from .pipeline import IMUFrame, Stage

log = logging.getLogger("fs.gesture")


def _discover_feature_sensors(paths) -> Tuple[str, ...]:
    """
    Auto-discover scalar feature sensors from gesture-capture JSONLs.
    For each `_gesture` window, collect the set of sensor names that
    have at least one scalar (values length 1) frame within the
    window. Return the sorted intersection across every window in
    every file — features that appear in *every* captured gesture.

    Returns an empty tuple if no windows / no overlapping scalars are
    found; caller should fall back to explicit `feature_sensors=...`
    or warn.
    """
    per_window_scalars: List[Set[str]] = []
    for p in paths:
        active: Optional[Set[str]] = None
        with Path(p).open("r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                rec = json.loads(line)
                kind = rec.get("_gesture")
                if kind == "start":
                    active = set()
                elif kind == "end":
                    if active is not None:
                        per_window_scalars.append(active)
                    active = None
                elif active is not None and "device" in rec:
                    sensor = rec.get("sensor")
                    values = rec.get("values")
                    if (sensor and isinstance(values, list)
                            and len(values) == 1):
                        active.add(sensor)
    if not per_window_scalars:
        return ()
    return tuple(sorted(set.intersection(*per_window_scalars)))


def _zscore_columns(arr: np.ndarray) -> np.ndarray:
    """Z-normalize each column of a 2D array independently. Columns
    with std ≈ 0 are zeroed (flat signal carries no shape info).
    Returns a fresh float64 array."""
    arr = np.asarray(arr, dtype=np.double)
    if arr.size == 0:
        return arr.astype(np.double, copy=True)
    mean = arr.mean(axis=0)
    std = arr.std(axis=0)
    safe = np.where(std < 1e-9, 1.0, std)
    out = (arr - mean) / safe
    out[:, std < 1e-9] = 0.0
    return out


@dataclass
class Template:
    label: str
    device: str
    instance: int
    feature_series: np.ndarray   # (n_samples, n_features), z-normed


class GestureLibrary:
    """Templates grouped by label, with auto-derived per-label thresholds."""

    def __init__(self,
                 feature_sensors: Optional[Tuple[str, ...]] = None,
                 threshold_margin: float = 1.5,
                 band: int = 10,
                 psi: int = 10,
                 zscore: bool = False):
        self.templates: List[Template] = []
        self.thresholds: dict = {}
        # An empty tuple is valid here (e.g. from_files with no matching
        # recordings); from_files is the canonical path that auto-fills
        # this from the JSONLs when caller passes None.
        self.feature_sensors = tuple(feature_sensors) if feature_sensors else ()
        self.threshold_margin = threshold_margin
        self.band = band
        self.psi = psi
        # Whether templates AND runtime signals are z-score normalized
        # before DTW. Default off — analysis on real captures showed
        # raw distances discriminate better because amplitude carries
        # information that z-norm flattens.
        self.zscore = zscore

    @classmethod
    def from_files(cls, paths,
                   feature_sensors: Optional[Tuple[str, ...]] = None,
                   threshold_margin: float = 1.5,
                   band: int = 10,
                   psi: int = 10,
                   zscore: bool = False,
                   filter_outliers: bool = False,
                   outlier_mad_threshold: float = 2.5,
                   outlier_max_drop_fraction: float = 0.2,
                   outlier_min_n: int = 5) -> "GestureLibrary":
        # Auto-detect from the JSONL when caller didn't pin features
        # explicitly. Intersection across every gesture window — any
        # scalar stream that appeared in every capture is fair game.
        if feature_sensors is None:
            feature_sensors = _discover_feature_sensors(paths)
            if not feature_sensors:
                log.error(
                    "[gesture] no scalar features found in %s — every "
                    "captured window must share at least one scalar "
                    "stream. Pass feature_sensors=... (or --gesture-features) "
                    "explicitly, or re-capture with the desired streams.",
                    list(paths),
                )
            else:
                log.info("[gesture] auto-detected feature_sensors=%s "
                         "(intersection of scalar streams across all windows)",
                         list(feature_sensors))
        else:
            log.info("[gesture] using explicit feature_sensors=%s",
                     list(feature_sensors))

        lib = cls(feature_sensors=feature_sensors,
                  threshold_margin=threshold_margin,
                  band=band, psi=psi, zscore=zscore)
        for p in paths:
            for tmpl in cls._extract_templates(
                Path(p), lib.feature_sensors, zscore=lib.zscore,
            ):
                lib.templates.append(tmpl)
        if not lib.templates:
            log.warning("[gesture] no templates loaded from %s — check that "
                        "feature_sensors=%s match the recorded streams",
                        list(paths), lib.feature_sensors)
        if filter_outliers:
            lib._filter_outliers(
                mad_threshold=outlier_mad_threshold,
                max_drop_fraction=outlier_max_drop_fraction,
                min_n=outlier_min_n,
            )
        lib._compute_thresholds()
        return lib

    @staticmethod
    def _extract_templates(path: Path,
                           feature_sensors: Tuple[str, ...],
                           zscore: bool = False) -> List[Template]:
        templates: List[Template] = []
        active: Optional[dict] = None
        # Per-sensor frame lists during the currently-open window.
        per_sensor: dict = {s: [] for s in feature_sensors}
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
                    if active is not None and all(
                        len(per_sensor[s]) > 0 for s in feature_sensors
                    ):
                        # Truncate to the shortest stream — sensors at
                        # the same ODR should be aligned to ±1 sample.
                        n = min(len(per_sensor[s]) for s in feature_sensors)
                        matrix = np.array(
                            [
                                [per_sensor[s][i] for s in feature_sensors]
                                for i in range(n)
                            ],
                            dtype=np.double,
                        )
                        series = _zscore_columns(matrix) if zscore else matrix
                        templates.append(Template(
                            label=active["label"],
                            device=active["device"],
                            instance=active["instance"],
                            feature_series=series,
                        ))
                    active = None
                    per_sensor = {s: [] for s in feature_sensors}
                elif active is not None and "device" in rec:
                    sensor = rec.get("sensor")
                    if (sensor in feature_sensors
                            and rec.get("device") == active["device"]
                            and rec.get("values")):
                        per_sensor[sensor].append(float(rec["values"][0]))
        log.info("[gesture] loaded %d template(s) from %s (zscore=%s)",
                 len(templates), path.name, zscore)
        return templates

    def _filter_outliers(self,
                         mad_threshold: float = 2.5,
                         max_drop_fraction: float = 0.2,
                         min_n: int = 5) -> int:
        """
        Identify and remove outlier templates per label using Median
        Absolute Deviation on each template's mean DTW distance to its
        peers. Returns count of templates dropped.

        Guardrails:
        - Labels with fewer than `min_n` templates are skipped — too
          little data to compute reliable outlier statistics.
        - At most `max_drop_fraction` of a label's templates can be
          dropped. If candidate outliers exceed this, the filter
          warns and skips that label entirely (the *training set* is
          probably the issue, not individual outliers).
        - All decisions logged: which templates dropped, with their
          mean distance vs the median, MAD, and threshold.
        """
        by_label = defaultdict(list)
        for i, t in enumerate(self.templates):
            by_label[t.label].append((i, t))

        indices_to_drop: List[int] = []
        for label, items in by_label.items():
            n = len(items)
            if n < min_n:
                log.info("[gesture] outlier filter: label %s has only %d "
                         "templates (< min_n=%d) — skipping",
                         label, n, min_n)
                continue
            # Mean DTW distance from each template to all peers in the same label.
            mean_dists = []
            for i_local in range(n):
                ds = []
                for j_local in range(n):
                    if i_local == j_local:
                        continue
                    d = dtw_ndim.distance_fast(
                        items[i_local][1].feature_series,
                        items[j_local][1].feature_series,
                        window=self.band, psi=self.psi,
                    )
                    ds.append(d)
                mean_dists.append(float(np.mean(ds)))

            median = float(np.median(mean_dists))
            mad = float(np.median(np.abs(np.array(mean_dists) - median)))
            if mad < 1e-9:
                log.info("[gesture] outlier filter: label %s — MAD≈0, no outliers",
                         label)
                continue

            cutoff = median + mad_threshold * mad
            candidates = [(k, mean_dists[k]) for k in range(n) if mean_dists[k] > cutoff]

            max_drop = max(1, int(n * max_drop_fraction))
            if len(candidates) > max_drop:
                log.warning(
                    "[gesture] outlier filter: label %s — %d candidate outlier(s) "
                    "exceeds max_drop=%d (max_drop_fraction=%.2f of %d). "
                    "Skipping filter for this label — your training set may be the "
                    "issue, not individual outliers.",
                    label, len(candidates), max_drop, max_drop_fraction, n,
                )
                continue

            for k, md in candidates:
                global_idx, tmpl = items[k]
                log.info(
                    "[gesture] outlier filter: dropping label=%s instance=%d "
                    "device=%s (mean_dist=%.4f vs median=%.4f, MAD=%.4f, cutoff=%.4f)",
                    label, tmpl.instance, tmpl.device,
                    md, median, mad, cutoff,
                )
                indices_to_drop.append(global_idx)

        if indices_to_drop:
            keep = [t for i, t in enumerate(self.templates) if i not in set(indices_to_drop)]
            dropped = len(self.templates) - len(keep)
            self.templates = keep
            log.info("[gesture] outlier filter: %d template(s) dropped, %d remain",
                     dropped, len(keep))
            return dropped
        log.info("[gesture] outlier filter: no outliers found")
        return 0

    def _compute_thresholds(self) -> None:
        """For each label, threshold = max(intra-label pairwise DTW) * margin."""
        by_label: dict = {}
        for t in self.templates:
            by_label.setdefault(t.label, []).append(t)
        for label, tmpls in by_label.items():
            if len(tmpls) < 2:
                self.thresholds[label] = 0.5
                log.warning("[gesture] label %s has %d template(s) — "
                            "auto-threshold unreliable; capture more instances",
                            label, len(tmpls))
                continue
            distances = []
            for i in range(len(tmpls)):
                for j in range(i + 1, len(tmpls)):
                    d = dtw_ndim.distance_fast(
                        tmpls[i].feature_series,
                        tmpls[j].feature_series,
                        window=self.band,
                        psi=self.psi,
                    )
                    distances.append(d)
            max_d = max(distances)
            self.thresholds[label] = max_d * self.threshold_margin
            log.info("[gesture] label %s: %d templates, "
                     "max intra-distance=%.4f, threshold=%.4f (margin=%.2f)",
                     label, len(tmpls), max_d, self.thresholds[label],
                     self.threshold_margin)

    @property
    def labels(self) -> List[str]:
        return sorted(self.thresholds.keys())


class GestureRecognizer(Stage):
    """
    Multivariate sliding-window DTW recognizer.

    Insert the SAME instance into every pipeline that carries a
    feature sensor (acc and gyro pipelines for the default
    `("acc_mag", "gyro_mag")`). Each pipeline calls process(); the
    recognizer accumulates per-(device, sensor) buffers; only the
    primary feature's frames trigger a match attempt — so tick rate
    is determined by the primary sensor.

    On match, emits `IMUFrame(sensor="gesture/<label>", values=(c,))`
    where `c = 1 - best_ratio` is a confidence score in (0, 1]:
    `c ≈ 1.0` for a near-perfect match (distance close to zero),
    `c ≈ 0` for a borderline match just under threshold. PD/DAW can
    use this to fade gesture-triggered events by match quality.
    Flows downstream through OscEmit as `/<MAC>/gesture/<label> <c>`.

    Variance gate uses `max(per-feature std)` — match attempts when
    *any* feature shows enough movement (so rotation-only gestures
    aren't gated by low acc std, and translation-only gestures aren't
    gated by low gyro std).

    Tier-1 tunable params (safe mid-flow): min_std, cooldown_s,
    exit_threshold, tick_frames, debug. Each is read fresh on every
    tick / match — no buffers reallocate, no library mismatch.
    NOT tunable: band, psi, zscore (library was built with specific
    settings; runtime mismatch invalidates thresholds), window_samples
    + feature_sensors (require buffer reallocation = composition op).
    """
    is_terminal = False
    TUNABLE_PARAMS = {
        "min_std": float,
        "cooldown_s": float,
        "exit_threshold": float,
        "tick_frames": int,
        "debug": bool,
    }

    def __init__(self, library: GestureLibrary,
                 window_samples: int = 50,
                 tick_frames: int = 5,
                 cooldown_s: float = 0.2,
                 min_std: float = 0.3,
                 band: Optional[int] = None,
                 psi: Optional[int] = None,
                 exit_threshold: float = 1.2,
                 debug: bool = False):
        self.library = library
        self.feature_sensors = library.feature_sensors
        self.zscore = library.zscore
        self.window_samples = window_samples
        self.tick_frames = tick_frames
        self.cooldown_s = cooldown_s
        self.min_std = min_std
        # Default band/psi proportional to window so tuning window_samples
        # doesn't quietly leave them stale. Library's own band/psi (used
        # for threshold computation) are independent — they default the
        # same way in from_files.
        self.band = band if band is not None else window_samples // 5
        self.psi = psi if psi is not None else window_samples // 5
        # Hysteresis: per-device "armed" state suppresses re-firing while
        # the buffer is still inside a matching valley. After a fire we
        # disarm; we re-arm only after best_ratio rises back above
        # `exit_threshold` (i.e. the buffer has clearly exited the valley).
        # This is what gives us "one gesture = one trigger" without
        # blocking quick back-to-back distinct gestures: cooldown is just
        # a small sanity backstop now (200 ms default).
        self.exit_threshold = exit_threshold
        self.debug = debug
        # Per-device per-sensor sliding buffers.
        self._buffers: dict = {}        # device -> {sensor: deque}
        self._frame_counter: dict = {}  # device -> int
        self._last_match_at: dict = {}  # device -> mono_t
        self._armed: dict = {}          # device -> bool (default True via .get)

    def process(self, frame: IMUFrame) -> Iterable[IMUFrame]:
        # Pass-through every input frame.
        yield frame
        if frame.sensor not in self.feature_sensors or not frame.values:
            return
        # Maintain per-(device, sensor) buffer.
        device_bufs = self._buffers.setdefault(frame.device, {})
        buf = device_bufs.get(frame.sensor)
        if buf is None:
            buf = deque(maxlen=self.window_samples)
            device_bufs[frame.sensor] = buf
        buf.append(float(frame.values[0]))

        # Tick only on the primary feature — secondary features just
        # populate their buffer.
        if frame.sensor != self.feature_sensors[0]:
            return

        n = self._frame_counter.get(frame.device, 0) + 1
        if n < self.tick_frames:
            self._frame_counter[frame.device] = n
            return
        self._frame_counter[frame.device] = 0

        # Cooldown gate.
        now = time.monotonic()
        if now - self._last_match_at.get(frame.device, 0.0) < self.cooldown_s:
            return

        # Need a full window in EVERY feature buffer.
        if any(len(device_bufs.get(s, [])) < self.window_samples
               for s in self.feature_sensors):
            return

        # Build multivariate signal: zip per-sensor buffers into rows.
        per_sensor_lists = [list(device_bufs[s]) for s in self.feature_sensors]
        n_min = min(len(s) for s in per_sensor_lists)
        signal = np.array(
            [
                [per_sensor_lists[k][i] for k in range(len(self.feature_sensors))]
                for i in range(n_min)
            ],
            dtype=np.double,
        )

        # Variance gate — pass if ANY feature has enough movement.
        stds = signal.std(axis=0)
        max_std = float(stds.max()) if signal.size > 0 else 0.0
        if max_std < self.min_std:
            if self.debug:
                log.info("[%s] gesture tick: max_std=%.4f < min_std=%.4f, skip",
                         frame.device, max_std, self.min_std)
            return

        # Match the library's normalization choice. With zscore=True,
        # templates are pre-z-normed at build, so we z-norm the runtime
        # buffer here. With zscore=False (default — raw mode wins per
        # data analysis), both sides are raw.
        signal_for_match = _zscore_columns(signal) if self.zscore else signal

        # Find best match across all templates.
        best_label: Optional[str] = None
        best_ratio = float("inf")
        best_distance = float("inf")
        for tmpl in self.library.templates:
            d = dtw_ndim.distance_fast(
                signal_for_match, tmpl.feature_series,
                window=self.band, psi=self.psi,
            )
            threshold = self.library.thresholds.get(tmpl.label, float("inf"))
            ratio = d / threshold if threshold > 0 else float("inf")
            if ratio < best_ratio:
                best_ratio = ratio
                best_label = tmpl.label
                best_distance = d

        armed = self._armed.get(frame.device, True)
        if self.debug:
            log.info("[%s] gesture tick: max_std=%.4f best=%s distance=%.4f ratio=%.4f armed=%s",
                     frame.device, max_std,
                     best_label, best_distance, best_ratio, armed)

        if best_label is None:
            return  # empty library — nothing to match against

        matched = best_ratio < 1.0

        # Hysteresis: fire only on the *first* sub-threshold tick after
        # being armed. Subsequent in-valley ticks suppressed until the
        # buffer leaves the valley (best_ratio rises above exit_threshold).
        if matched and armed:
            self._last_match_at[frame.device] = now
            self._armed[frame.device] = False
            confidence = 1.0 - best_ratio
            log.info("[%s] gesture: %s (distance=%.4f, ratio=%.4f, confidence=%.4f)",
                     frame.device, best_label, best_distance, best_ratio, confidence)
            yield IMUFrame(
                device=frame.device,
                sensor=f"gesture/{best_label}",
                t_recv=frame.t_recv,
                values=(confidence,),
            )
            return

        # Re-arm path: disarmed and the buffer has clearly left the
        # matching valley. The 1.0 to exit_threshold gap is the
        # hysteresis band — we don't flip back to armed until we're
        # comfortably in "no-match" territory.
        if not armed and best_ratio >= self.exit_threshold:
            self._armed[frame.device] = True
            if self.debug:
                log.info("[%s] gesture re-armed (best_ratio=%.4f >= exit=%.4f)",
                         frame.device, best_ratio, self.exit_threshold)

    def outputs(self, input_sensor: str) -> List[str]:
        # Gesture addresses only emerge from the primary feature's
        # branch — that's the pipeline whose tick produces them.
        if input_sensor == self.feature_sensors[0]:
            return [input_sensor] + [f"gesture/{label}" for label in self.library.labels]
        return [input_sensor]
