"""
Gesture recognition via 1D DTW over a pipeline feature.

Components:

- `dtw_distance(a, b)` — 1D Dynamic Time Warping with z-score
  normalization (scale-invariant). Path-length normalized so longer
  templates aren't penalized.
- `Template` — one labeled gesture instance, derived from a
  `_gesture: start`/`end` window in a recording.
- `GestureLibrary.from_files(paths)` — loads templates from one or more
  JSONL recordings produced by `run_fs.py --capture-label LABEL`,
  groups by label, auto-derives per-label thresholds from the intra-
  label pairwise distance distribution.
- `GestureRecognizer(library, ...)` — Pipeline Stage. Taps a feature
  sensor (default "acc_mag"), maintains a per-device sliding buffer,
  and every `tick_frames` frames runs DTW against every template. On a
  match (best-ratio label below threshold), emits a synthetic
  `gesture/<label>` frame so it flows through downstream OscEmit and
  shows up in `advertise()`. Per-recognizer cooldown after a match
  suppresses duplicate triggers.
"""
import json
import logging
import math
import time
from collections import deque
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Optional, Sequence

from .pipeline import IMUFrame, Stage

log = logging.getLogger("fs.gesture")


def _zscore(x: Sequence[float]) -> List[float]:
    """Z-score normalize a 1D series. Returns a list of zeros if std≈0
    (constant signal — DTW would then collapse to length difference)."""
    n = len(x)
    if n == 0:
        return []
    mean = sum(x) / n
    var = sum((xi - mean) ** 2 for xi in x) / n
    std = math.sqrt(var)
    if std < 1e-9:
        return [0.0] * n
    return [(xi - mean) / std for xi in x]


def dtw_distance(a: Sequence[float], b: Sequence[float],
                 normalize: bool = True) -> float:
    """
    1D DTW distance. Both sequences are z-scored first (when
    normalize=True) so the metric is scale-invariant — accommodates
    light vs heavy versions of the same gesture. Result is normalized
    by (n + m) so templates of different lengths aren't penalized for
    length alone.
    """
    if normalize:
        a = _zscore(a)
        b = _zscore(b)
    n, m = len(a), len(b)
    if n == 0 or m == 0:
        return float("inf")

    INF = float("inf")
    prev = [INF] * (m + 1)
    prev[0] = 0.0
    for i in range(1, n + 1):
        curr = [INF] * (m + 1)
        # curr[0] stays INF — can't end at j=0 for i>0
        for j in range(1, m + 1):
            cost = abs(a[i - 1] - b[j - 1])
            curr[j] = cost + min(prev[j], curr[j - 1], prev[j - 1])
        prev = curr
    return prev[m] / (n + m)


@dataclass
class Template:
    label: str
    device: str
    instance: int
    feature_series: List[float]   # raw 1D feature; DTW z-scores at match


class GestureLibrary:
    """Templates grouped by label, with auto-derived per-label thresholds."""

    def __init__(self, threshold_margin: float = 1.5):
        self.templates: List[Template] = []
        self.thresholds: dict = {}
        self.threshold_margin = threshold_margin

    @classmethod
    def from_files(cls, paths: Sequence,
                   feature_sensor: str = "acc_mag",
                   threshold_margin: float = 1.5) -> "GestureLibrary":
        lib = cls(threshold_margin=threshold_margin)
        for p in paths:
            for tmpl in cls._extract_templates(Path(p), feature_sensor):
                lib.templates.append(tmpl)
        if not lib.templates:
            log.warning("[gesture] no templates loaded from %s "
                        "(check feature_sensor=%s matches recorded frames)",
                        list(paths), feature_sensor)
        lib._compute_thresholds()
        return lib

    @staticmethod
    def _extract_templates(path: Path, feature_sensor: str) -> List[Template]:
        templates: List[Template] = []
        active: Optional[dict] = None
        active_features: List[float] = []
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
                    active_features = []
                elif kind == "end":
                    if active is not None and active_features:
                        templates.append(Template(
                            label=active["label"],
                            device=active["device"],
                            instance=active["instance"],
                            feature_series=active_features,
                        ))
                    active = None
                    active_features = []
                elif active is not None and "device" in rec:
                    # Frame inside an open gesture window. Only keep
                    # frames matching the recognizer's feature_sensor on
                    # the device that owns the window.
                    if (rec.get("sensor") == feature_sensor
                            and rec.get("device") == active["device"]
                            and rec.get("values")):
                        active_features.append(float(rec["values"][0]))
        log.info("[gesture] loaded %d template(s) from %s",
                 len(templates), path.name)
        return templates

    def _compute_thresholds(self) -> None:
        """For each label, threshold = max(intra-label pairwise DTW) * margin.
        Single-template labels get a placeholder threshold + warning."""
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
                    d = dtw_distance(tmpls[i].feature_series,
                                     tmpls[j].feature_series)
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
    Sliding-window 1D DTW recognizer. Drop into a pipeline anywhere
    after the stage that produces `feature_sensor` (default "acc_mag")
    and before the terminal OscEmit so gesture triggers flow downstream.

    Per-device buffers and frame counters — gestures are recognized
    independently on each device (per the per-device design call).
    Match every `tick_frames` frames; needs a full window before the
    first match. Cooldown after match suppresses duplicates.

    On match, emits an `IMUFrame(sensor="gesture/<label>", values=(1.0,))`
    that downstream `OscEmit` publishes as `/<MAC>/gesture/<label> 1.0`.
    The placeholder 1.0 is the slot for a confidence value in a future
    iteration.
    """
    is_terminal = False

    def __init__(self, library: GestureLibrary,
                 feature_sensor: str = "acc_mag",
                 window_samples: int = 50,
                 tick_frames: int = 5,
                 cooldown_s: float = 0.5,
                 min_std: float = 0.3,
                 debug: bool = False):
        self.library = library
        self.feature_sensor = feature_sensor
        self.window_samples = window_samples
        self.tick_frames = tick_frames
        self.cooldown_s = cooldown_s
        # Variance gate: when the buffer's raw std is below this, skip
        # matching. Z-score normalization inside DTW otherwise amplifies
        # near-flat signals into noisy z-scored "shapes" that fire false
        # matches. Tune per feature_sensor — 0.3 sits between typical
        # acc_mag stillness (~0.05-0.15) and gentle movement (~0.5+).
        self.min_std = min_std
        # When True, log every tick (not just on match) so the operator
        # can see what the recognizer is considering during near-misses
        # and tune min_std / threshold_margin accordingly.
        self.debug = debug
        self._buffers: dict = {}        # device -> deque of feature values
        self._frame_counter: dict = {}  # device -> int
        self._last_match_at: dict = {}  # device -> mono_t

    def process(self, frame: IMUFrame) -> Iterable[IMUFrame]:
        # Always pass the input frame through.
        yield frame
        # Only the configured feature feeds the buffer.
        if frame.sensor != self.feature_sensor or not frame.values:
            return
        feature = float(frame.values[0])

        buf = self._buffers.get(frame.device)
        if buf is None:
            buf = deque(maxlen=self.window_samples)
            self._buffers[frame.device] = buf
        buf.append(feature)

        # Tick gating — only run match every `tick_frames` frames.
        n = self._frame_counter.get(frame.device, 0) + 1
        if n < self.tick_frames:
            self._frame_counter[frame.device] = n
            return
        self._frame_counter[frame.device] = 0

        # Cooldown gating — quiet period after a match.
        now = time.monotonic()
        last_match = self._last_match_at.get(frame.device, 0.0)
        if now - last_match < self.cooldown_s:
            return

        # Need a full window before matching.
        if len(buf) < self.window_samples:
            return

        # Variance gate — compute raw std of the buffer; skip the match
        # entirely if the buffer is mostly still.
        signal = list(buf)
        n_signal = len(signal)
        signal_mean = sum(signal) / n_signal
        signal_std = math.sqrt(
            sum((x - signal_mean) ** 2 for x in signal) / n_signal
        )
        if signal_std < self.min_std:
            if self.debug:
                log.info("[%s] gesture tick: std=%.4f < min_std=%.4f, skip match",
                         frame.device, signal_std, self.min_std)
            return

        # Match: lowest distance/threshold ratio across all templates;
        # fire if best ratio < 1.0 (i.e. distance below that label's
        # threshold).
        best_label: Optional[str] = None
        best_ratio = float("inf")
        best_distance = float("inf")
        for tmpl in self.library.templates:
            d = dtw_distance(signal, tmpl.feature_series)
            threshold = self.library.thresholds.get(tmpl.label, float("inf"))
            ratio = d / threshold if threshold > 0 else float("inf")
            if ratio < best_ratio:
                best_ratio = ratio
                best_label = tmpl.label
                best_distance = d

        if self.debug:
            log.info("[%s] gesture tick: std=%.4f best=%s distance=%.4f ratio=%.4f",
                     frame.device, signal_std,
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
        # Gesture addresses only emerge from the feature sensor's branch.
        if input_sensor == self.feature_sensor:
            return [input_sensor] + [f"gesture/{label}" for label in self.library.labels]
        return [input_sensor]
