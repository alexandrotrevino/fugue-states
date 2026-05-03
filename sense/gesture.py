"""
Multivariate gesture recognition via DTW (dtaidistance).

Round 2 — multivariate DTW with Sakoe-Chiba band + subsequence
relaxation. Each gesture is matched in a feature space defined by a
tuple of pipeline sensors (default `("acc_mag", "gyro_mag")` —
translation + rotation, the literature-flagged minimal set for
distinguishing wrist gestures of similar amplitude).

Components:

- `_zscore_columns(arr)` — column-wise z-norm; flat columns zeroed.
- `Template` — one labeled instance, `feature_series` is an
  `np.ndarray` of shape `(n_samples, n_features)`, z-normed at build.
- `GestureLibrary.from_files(paths, feature_sensors=...)` — loads
  templates from `--capture-label` JSONL recordings; per-label
  thresholds auto-derived from intra-label pairwise multivariate DTW.
- `GestureRecognizer(library, ...)` — Pipeline Stage. Inserted into
  every pipeline carrying a feature sensor (typically acc and gyro).
  Maintains per-(device, sensor) buffers; ticks on the *primary*
  feature only (`feature_sensors[0]`); zips per-sensor buffers into
  a multivariate signal at tick time, z-norms, runs DTW with band
  and psi against every template, picks lowest distance/threshold
  ratio, emits `gesture/<label>` on match.

DTW backed by `dtaidistance.dtw_ndim.distance_fast` (C, multivariate,
Sakoe-Chiba `window`, subsequence `psi`). Z-normalization happens in
this module — dtaidistance does not z-norm internally.
"""
import json
import logging
import time
from collections import deque
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Optional, Tuple

import numpy as np
from dtaidistance import dtw_ndim

from .pipeline import IMUFrame, Stage

log = logging.getLogger("fs.gesture")

DEFAULT_FEATURE_SENSORS: Tuple[str, ...] = ("acc_mag", "gyro_mag")


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
                 feature_sensors: Tuple[str, ...] = DEFAULT_FEATURE_SENSORS,
                 threshold_margin: float = 1.5,
                 band: int = 10,
                 psi: int = 10):
        self.templates: List[Template] = []
        self.thresholds: dict = {}
        self.feature_sensors = tuple(feature_sensors)
        self.threshold_margin = threshold_margin
        self.band = band
        self.psi = psi

    @classmethod
    def from_files(cls, paths,
                   feature_sensors: Tuple[str, ...] = DEFAULT_FEATURE_SENSORS,
                   threshold_margin: float = 1.5,
                   band: int = 10,
                   psi: int = 10) -> "GestureLibrary":
        lib = cls(feature_sensors=feature_sensors,
                  threshold_margin=threshold_margin,
                  band=band, psi=psi)
        for p in paths:
            for tmpl in cls._extract_templates(Path(p), lib.feature_sensors):
                lib.templates.append(tmpl)
        if not lib.templates:
            log.warning("[gesture] no templates loaded from %s — check that "
                        "feature_sensors=%s match the recorded streams "
                        "(have you re-captured since adding gyro_mag?)",
                        list(paths), lib.feature_sensors)
        lib._compute_thresholds()
        return lib

    @staticmethod
    def _extract_templates(path: Path,
                           feature_sensors: Tuple[str, ...]) -> List[Template]:
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
                        templates.append(Template(
                            label=active["label"],
                            device=active["device"],
                            instance=active["instance"],
                            feature_series=_zscore_columns(matrix),
                        ))
                    active = None
                    per_sensor = {s: [] for s in feature_sensors}
                elif active is not None and "device" in rec:
                    sensor = rec.get("sensor")
                    if (sensor in feature_sensors
                            and rec.get("device") == active["device"]
                            and rec.get("values")):
                        per_sensor[sensor].append(float(rec["values"][0]))
        log.info("[gesture] loaded %d template(s) from %s",
                 len(templates), path.name)
        return templates

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

    On match, emits `IMUFrame(sensor="gesture/<label>", values=(1.0,))`
    that flows through the primary pipeline's downstream OscEmit and
    publishes as `/<MAC>/gesture/<label>`. The 1.0 is the placeholder
    slot for future confidence values.

    Variance gate uses `max(per-feature std)` — match attempts when
    *any* feature shows enough movement (so rotation-only gestures
    aren't gated by low acc std, and translation-only gestures aren't
    gated by low gyro std).
    """
    is_terminal = False

    def __init__(self, library: GestureLibrary,
                 window_samples: int = 50,
                 tick_frames: int = 5,
                 cooldown_s: float = 0.5,
                 min_std: float = 0.3,
                 band: Optional[int] = None,
                 psi: Optional[int] = None,
                 debug: bool = False):
        self.library = library
        self.feature_sensors = library.feature_sensors
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
        self.debug = debug
        # Per-device per-sensor sliding buffers.
        self._buffers: dict = {}        # device -> {sensor: deque}
        self._frame_counter: dict = {}  # device -> int
        self._last_match_at: dict = {}  # device -> mono_t

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

        # Z-norm runtime signal column-wise; templates are pre-z-normed.
        signal_z = _zscore_columns(signal)

        # Find best match across all templates.
        best_label: Optional[str] = None
        best_ratio = float("inf")
        best_distance = float("inf")
        for tmpl in self.library.templates:
            d = dtw_ndim.distance_fast(
                signal_z, tmpl.feature_series,
                window=self.band, psi=self.psi,
            )
            threshold = self.library.thresholds.get(tmpl.label, float("inf"))
            ratio = d / threshold if threshold > 0 else float("inf")
            if ratio < best_ratio:
                best_ratio = ratio
                best_label = tmpl.label
                best_distance = d

        if self.debug:
            log.info("[%s] gesture tick: max_std=%.4f best=%s distance=%.4f ratio=%.4f",
                     frame.device, max_std,
                     best_label, best_distance, best_ratio)

        if best_label is None or best_ratio >= 1.0:
            return  # no match

        self._last_match_at[frame.device] = now
        log.info("[%s] gesture: %s (distance=%.4f, ratio=%.4f)",
                 frame.device, best_label, best_distance, best_ratio)
        yield IMUFrame(
            device=frame.device,
            sensor=f"gesture/{best_label}",
            t_recv=frame.t_recv,
            values=(1.0,),
        )

    def outputs(self, input_sensor: str) -> List[str]:
        # Gesture addresses only emerge from the primary feature's
        # branch — that's the pipeline whose tick produces them.
        if input_sensor == self.feature_sensors[0]:
            return [input_sensor] + [f"gesture/{label}" for label in self.library.labels]
        return [input_sensor]
