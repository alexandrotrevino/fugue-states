"""
Stream-processing pipeline for IMU frames.

Each MetaWearState owns a per-sensor pipeline. Sensor callbacks build
an `IMUFrame` and `push()` it; frames flow through `Stage`s in order;
the terminal `OscEmit` stage publishes them as `/<MAC>/<sensor>` OSC
messages.

OscEmit suppresses tracebacks on the transient unreachable-receiver
errnos (ENETUNREACH/EHOSTUNREACH/ECONNREFUSED) — at our frame rates,
log.exception per failed send dominates the BLE callback thread and
crashes throughput when the audio plane (PD) isn't listening.

Stages can:
- transform a frame's values (e.g. low-pass)
- emit additional derived frames alongside the original (e.g.
  `Magnitude` adds `<sensor>_mag`, `Tilt` adds `tilt`)
- drop frames (return an empty iterable)

Per-stage timing is recorded in `Pipeline.stats` so we can answer
"how much wall-clock does my LowPass actually cost?" while iterating.

The default pipeline is `[OscEmit(...)]`, which preserves the
pre-pipeline OSC vocabulary 1:1. Compose richer pipelines per sensor:

    state.pipelines["acc"].stages = [
        LowPass(cutoff_hz=5, fs=25),
        Magnitude(),
        Tilt(),
        OscEmit(state._osc_client),
    ]

Cross-sensor / cross-device fusion stages are intentionally not in
this first cut — they need a separate "latest-value latch" or buffered
device-frame abstraction. To be added when the basic shape is proven.
"""
import errno
import logging
import math
import threading
import time
from collections import deque
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Tuple

log = logging.getLogger("fs.pipeline")

# UDP send errnos that mean "receiver not reachable right now." Suppressed
# silently in OscEmit (no traceback, no warning per send) — at our frame
# rates anything above silent costs throughput. Matches the
# _osc_send_best_effort policy in state.py.
_TRANSIENT_OSC_ERRNOS = (errno.ENETUNREACH, errno.EHOSTUNREACH, errno.ECONNREFUSED)


@dataclass
class IMUFrame:
    device: str                 # MAC address (matches MetaWearState.address)
    sensor: str                 # "acc", "gyro", "mag", or derived ("acc_mag", "tilt", ...)
    t_recv: float               # time.monotonic() at receipt — Pi-side timeline
    values: Tuple[float, ...]   # variable length (1 = scalar, 3 = vec3, 4 = quat/euler)


class StageStats:
    """Rolling window of per-call elapsed times for a stage."""
    def __init__(self, capacity: int = 1024):
        self._samples: deque = deque(maxlen=capacity)

    def add(self, elapsed_s: float) -> None:
        self._samples.append(elapsed_s)

    @property
    def count(self) -> int:
        return len(self._samples)

    @property
    def mean_s(self) -> float:
        return sum(self._samples) / len(self._samples) if self._samples else 0.0

    @property
    def max_s(self) -> float:
        return max(self._samples) if self._samples else 0.0


class Stage:
    """
    Subclass and override `process`. Yield zero, one, or many frames.

    Override `outputs(input_sensor)` if the stage emits sensor names
    other than the input — `advertise()` walks the pipeline statically
    using this to know what addresses will be published, without
    needing to actually run frames through.

    Set `is_terminal = True` on stages that consume frames without
    forwarding them (e.g. `OscEmit`); pipeline introspection stops at
    the first terminal stage.

    Class-level metadata for C2 remote configuration:
    - `CONSTRUCTION_PARAMS`: params the stage accepts at __init__ time;
      `/cmd/pipeline/add` validates against this map. Only set on
      C2-constructible stages (those listed in `_STAGE_REGISTRY`).
    - `TUNABLE_PARAMS`: subset of attributes safe to mutate at runtime
      via `/cmd/pipeline/set`. Tier-1 only — pure scalar thresholds /
      rates read each tick. Stage state (deques, IIR memory, etc.)
      stays valid across a tune. See docs/c2.md for the policy.
    """
    is_terminal: bool = False
    CONSTRUCTION_PARAMS: Dict[str, type] = {}
    TUNABLE_PARAMS: Dict[str, type] = {}

    def process(self, frame: IMUFrame) -> Iterable[IMUFrame]:
        raise NotImplementedError

    def outputs(self, input_sensor: str) -> List[str]:
        """Static declaration of what sensor names this stage emits
        given a single input sensor name. Default: passthrough."""
        return [input_sensor]


class Pipeline:
    def __init__(self, stages: List[Stage]):
        self.stages = stages
        self.stats: dict = {}

    def push(self, frame: IMUFrame) -> None:
        frames = [frame]
        for stage in self.stages:
            stage_name = stage.__class__.__name__
            stats = self.stats.setdefault(stage_name, StageStats())
            next_frames: List[IMUFrame] = []
            for f in frames:
                t0 = time.monotonic()
                try:
                    outs = list(stage.process(f))
                except BaseException:
                    # A stage error is contained — log and drop the
                    # offending frame, don't take down the rest.
                    log.exception("[%s] %s raised on %s/%s",
                                  stage_name, stage_name, f.device, f.sensor)
                    outs = []
                stats.add(time.monotonic() - t0)
                next_frames.extend(outs)
            frames = next_frames
            if not frames:
                break

    def advertised_outputs(self, source: str) -> set:
        """
        Walk stages forward and return the set of sensor names that
        arrive at the first terminal stage (e.g. OscEmit). If no
        terminal stage exists, returns the set at the end of the chain.
        """
        sensors = {source}
        for stage in self.stages:
            if stage.is_terminal:
                return sensors
            new_sensors: set = set()
            for s in sensors:
                new_sensors.update(stage.outputs(s))
            sensors = new_sensors
        return sensors


# --- Cross-stream fusion primitives ------------------------------------------
#
# Pipelines are per-(device, sensor); a stage sees only the frames flowing
# through its own pipeline. Fusion stages that need to act on combinations
# of streams (linear_acc + quat for world-frame motion; two devices' acc_mag
# for collective gestures; etc.) coordinate through a shared `Latch`.
#
# Wiring pattern:
#   latch = Latch()
#   # in every pipeline whose stream should be visible to fusion stages:
#   pipe.stages.insert(0, LatchUpdate(latch))
#   # in the pipeline that drives the fusion (typically the "primary" input):
#   pipe.stages.insert(-1, MyFusionStage(latch=latch, ...))
#
# Threading: the latch is updated from libmetawear's per-device callback
# threads. Reads in fusion stages run on whichever callback thread owns the
# driving stream. A lock guards single-key access; cross-key reads are
# best-effort latest (frames may be milliseconds apart). Atomic snapshots
# across multiple keys are deliberately not in v1 — no current consumer
# needs them.


class Latch:
    """Thread-safe latest-value cache keyed by (device, sensor)."""
    def __init__(self):
        self._values: Dict[Tuple[str, str], IMUFrame] = {}
        self._lock = threading.Lock()

    def update(self, frame: IMUFrame) -> None:
        with self._lock:
            self._values[(frame.device, frame.sensor)] = frame

    def get(self, device: str, sensor: str) -> Optional[IMUFrame]:
        with self._lock:
            return self._values.get((device, sensor))

    def get_all(self, sensor: str) -> Dict[str, IMUFrame]:
        """All devices' latest frame for one sensor — for cross-device
        fusion stages (collective gesture recognition, swarm behaviour)."""
        with self._lock:
            return {
                dev: frame for (dev, s), frame in self._values.items()
                if s == sensor
            }


class LatchUpdate(Stage):
    """
    Pass-through stage that updates a shared `Latch` with every frame
    it sees. Insert wherever you want this stream to be visible to
    downstream fusion stages — typically at the head of a pipeline
    (latch sees raw values), but explicit positioning is the contract.
    Putting it after a transform (e.g. LowPass) makes the latch reflect
    filtered values instead.
    """
    def __init__(self, latch: Latch):
        self.latch = latch

    def process(self, frame: IMUFrame) -> Iterable[IMUFrame]:
        self.latch.update(frame)
        yield frame


class FusionStage(Stage):
    """
    Base class for stages that read multiple streams through a `Latch`.
    Subclasses override `process()` and call `self.latest(device, sensor)`
    or `self.latest_all(sensor)` to reach across to streams that flow
    through other pipelines.

    The driving frame (the one `process()` is invoked on) is whatever
    pipeline this stage was inserted into; cross-stream reads happen
    inside `process()` and pick up whatever the latch saw most recently.
    """
    def __init__(self, latch: Latch):
        self.latch = latch

    def latest(self, device: str, sensor: str) -> Optional[IMUFrame]:
        return self.latch.get(device, sensor)

    def latest_all(self, sensor: str) -> Dict[str, IMUFrame]:
        return self.latch.get_all(sensor)


# --- Concrete stages ---------------------------------------------------------

class Magnitude(Stage):
    """
    Pass through the original frame, plus emit a derived
    `<sensor>_mag` frame containing the L2 norm of the input values.
    Useful for collapsing a vec3 into a single 'how much motion' scalar.
    """
    def __init__(self, output_sensor: Optional[str] = None):
        self.output_sensor = output_sensor

    def process(self, frame: IMUFrame) -> Iterable[IMUFrame]:
        yield frame
        mag = math.sqrt(sum(v * v for v in frame.values))
        out = self.output_sensor or f"{frame.sensor}_mag"
        yield IMUFrame(
            device=frame.device, sensor=out,
            t_recv=frame.t_recv, values=(mag,),
        )

    def outputs(self, input_sensor: str) -> List[str]:
        out = self.output_sensor or f"{input_sensor}_mag"
        return [input_sensor, out]


class LowPass(Stage):
    """
    Single-pole IIR low-pass filter. State is keyed per (device,
    sensor) so the same instance can be inserted into a pipeline that
    sees multiple sensors — each one tracks independently.

    cutoff_hz: -3dB cutoff frequency
    fs: sample rate the input is arriving at (must match the sensor's ODR)
    output_sensor: if None (default), the filtered values *replace* the
        input frame's values in place — downstream stages see only the
        smoothed signal. If set (e.g. `"acc_lp"`), the raw frame is
        passed through unchanged AND a derived frame is emitted with
        the filtered values under the new sensor name. Useful for
        comparing raw vs. filtered, or for branching the pipeline so
        some derivations run on raw and others on filtered.

    cutoff_hz is exposed as a Tier-1 tunable: `setattr(stage,
    'cutoff_hz', N)` recomputes `alpha` via the property setter; the
    per-device IIR state stays valid and the filter retunes smoothly
    on the next frame.
    """
    CONSTRUCTION_PARAMS = {"cutoff_hz": float, "fs": float}
    TUNABLE_PARAMS = {"cutoff_hz": float}

    def __init__(
        self,
        cutoff_hz: float,
        fs: float,
        output_sensor: Optional[str] = None,
    ):
        # Set _fs first so the cutoff_hz setter has fs available when
        # it recomputes alpha.
        self._fs = float(fs)
        self.cutoff_hz = cutoff_hz  # triggers setter → _recompute_alpha
        self.output_sensor = output_sensor
        self._state: dict = {}

    @property
    def cutoff_hz(self) -> float:
        return self._cutoff_hz

    @cutoff_hz.setter
    def cutoff_hz(self, value: float) -> None:
        self._cutoff_hz = float(value)
        self._recompute_alpha()

    @property
    def fs(self) -> float:
        return self._fs

    @fs.setter
    def fs(self, value: float) -> None:
        self._fs = float(value)
        self._recompute_alpha()

    def _recompute_alpha(self) -> None:
        rc = 1.0 / (2.0 * math.pi * self._cutoff_hz)
        dt = 1.0 / self._fs
        self.alpha = dt / (rc + dt)

    def process(self, frame: IMUFrame) -> Iterable[IMUFrame]:
        # State key uses the *input* sensor so derived-mode and in-place
        # mode share filter state when applied to the same input.
        key = (frame.device, frame.sensor)
        prev = self._state.get(key)
        if prev is None or len(prev) != len(frame.values):
            new = frame.values
        else:
            new = tuple(
                prev[i] + self.alpha * (frame.values[i] - prev[i])
                for i in range(len(frame.values))
            )
        self._state[key] = new

        if self.output_sensor is None:
            # In-place: replace values, single output frame
            yield IMUFrame(
                device=frame.device, sensor=frame.sensor,
                t_recv=frame.t_recv, values=new,
            )
        else:
            # Derived: pass through raw, plus emit filtered alongside
            yield frame
            yield IMUFrame(
                device=frame.device, sensor=self.output_sensor,
                t_recv=frame.t_recv, values=new,
            )

    def outputs(self, input_sensor: str) -> List[str]:
        if self.output_sensor is None:
            return [input_sensor]
        return [input_sensor, self.output_sensor]


class Tilt(Stage):
    """
    For accelerometer frames: pass through the raw values, plus emit
    a single-value `tilt` frame with the angle (in degrees) between
    the device's z-axis and the gravity vector.

    0°   = device flat, z pointing up
    90°  = device on its edge
    180° = device flat, z pointing down

    Computed from the accelerometer alone, so it's only meaningful
    when the device isn't being shaken — under heavy linear
    acceleration the signal is dominated by motion, not gravity.
    """
    def process(self, frame: IMUFrame) -> Iterable[IMUFrame]:
        yield frame
        if frame.sensor != "acc" or len(frame.values) != 3:
            return
        x, y, z = frame.values
        mag = math.sqrt(x * x + y * y + z * z)
        if mag < 1e-6:
            return  # too small to compute angle reliably
        cos_theta = max(-1.0, min(1.0, z / mag))
        theta_deg = math.degrees(math.acos(cos_theta))
        yield IMUFrame(
            device=frame.device, sensor="tilt",
            t_recv=frame.t_recv, values=(theta_deg,),
        )

    def outputs(self, input_sensor: str) -> List[str]:
        if input_sensor == "acc":
            return [input_sensor, "tilt"]
        return [input_sensor]


class HighPass(Stage):
    """
    Single-pole IIR high-pass filter. Mirror of `LowPass` — same
    per-(device, sensor) state model and same `cutoff_hz`/`fs` property
    setters, but the recursion is `y[n] = α(y[n-1] + x[n] - x[n-1])`
    with `α = RC/(RC+dt)`.

    Default emit mode is **derived** (unlike LowPass, which defaults to
    in-place): the raw frame passes through and a derived frame is
    emitted at `output_sensor` (default `<sensor>_hp`). The motion vs.
    gravity split is the canonical use case — both branches need to
    coexist downstream, so derived is the obvious shape.

    `cutoff_hz`/`fs` are exposed as Tier-1 tunables; `output_sensor`
    can be passed explicitly to override the default name.
    """
    CONSTRUCTION_PARAMS = {"cutoff_hz": float, "fs": float}
    TUNABLE_PARAMS = {"cutoff_hz": float}

    def __init__(
        self,
        cutoff_hz: float,
        fs: float,
        output_sensor: Optional[str] = None,
    ):
        self._fs = float(fs)
        self.cutoff_hz = cutoff_hz  # triggers setter → _recompute_alpha
        self.output_sensor = output_sensor
        # (device, sensor) -> (prev_input_values, prev_output_values)
        self._state: dict = {}

    @property
    def cutoff_hz(self) -> float:
        return self._cutoff_hz

    @cutoff_hz.setter
    def cutoff_hz(self, value: float) -> None:
        self._cutoff_hz = float(value)
        self._recompute_alpha()

    @property
    def fs(self) -> float:
        return self._fs

    @fs.setter
    def fs(self, value: float) -> None:
        self._fs = float(value)
        self._recompute_alpha()

    def _recompute_alpha(self) -> None:
        rc = 1.0 / (2.0 * math.pi * self._cutoff_hz)
        dt = 1.0 / self._fs
        self.alpha = rc / (rc + dt)

    def process(self, frame: IMUFrame) -> Iterable[IMUFrame]:
        yield frame
        key = (frame.device, frame.sensor)
        prev = self._state.get(key)
        if prev is None or len(prev[0]) != len(frame.values):
            # First frame: HP output is zero (no high-frequency content
            # observed yet). Seed the state with current input so the
            # next frame's recursion is well-defined.
            new_out = tuple(0.0 for _ in frame.values)
        else:
            prev_in, prev_out = prev
            new_out = tuple(
                self.alpha * (prev_out[i] + frame.values[i] - prev_in[i])
                for i in range(len(frame.values))
            )
        self._state[key] = (frame.values, new_out)
        out = self.output_sensor or f"{frame.sensor}_hp"
        yield IMUFrame(
            device=frame.device, sensor=out,
            t_recv=frame.t_recv, values=new_out,
        )

    def outputs(self, input_sensor: str) -> List[str]:
        out = self.output_sensor or f"{input_sensor}_hp"
        return [input_sensor, out]


class Differentiator(Stage):
    """
    Numerical first derivative: `(v[n] - v[n-1]) / (t[n] - t[n-1])`,
    component-wise on multi-axis frames. Per-(device, sensor) state
    tracks the previous (values, t_recv).

    First frame yields pass-through only — no derivative available yet.
    Subsequent frames emit pass-through + derived frame at
    `output_sensor` (default `<sensor>_d`).

    Composes well: `acc → Differentiator → jerk`, `gyro → Differentiator
    → angular_accel`. For noisy derivatives, compose with `LowPass`
    upstream or downstream.
    """
    def __init__(self, output_sensor: Optional[str] = None):
        self.output_sensor = output_sensor
        # (device, sensor) -> (prev_values, prev_t_recv)
        self._state: dict = {}

    def process(self, frame: IMUFrame) -> Iterable[IMUFrame]:
        yield frame
        key = (frame.device, frame.sensor)
        prev = self._state.get(key)
        self._state[key] = (frame.values, frame.t_recv)
        if prev is None:
            return
        prev_vals, prev_t = prev
        dt = frame.t_recv - prev_t
        if dt <= 0 or len(prev_vals) != len(frame.values):
            # Out-of-order or arity mismatch — drop the derivative for
            # this frame, state is already advanced for the next one.
            return
        deriv = tuple(
            (frame.values[i] - prev_vals[i]) / dt
            for i in range(len(frame.values))
        )
        out = self.output_sensor or f"{frame.sensor}_d"
        yield IMUFrame(
            device=frame.device, sensor=out,
            t_recv=frame.t_recv, values=deriv,
        )

    def outputs(self, input_sensor: str) -> List[str]:
        out = self.output_sensor or f"{input_sensor}_d"
        return [input_sensor, out]


class EdgeDetector(Stage):
    """
    Threshold-cross trigger. Scalar input only — compose with
    `Magnitude` for vec3 streams (multi-axis input is rejected with a
    one-shot warning per (device, sensor); the pass-through frame
    still flows, but no event is emitted).

    On crossing `threshold` in the selected direction, emits a unit
    event frame at `output_sensor` (default `<sensor>_edge`,
    values=(1.0,)). Pass-through frame is always yielded.

    `hysteresis` ≥ 0 prevents thrashing on noise near the threshold:
    after a rising edge, re-arming requires the signal to fall below
    `threshold - hysteresis` (mirror for falling). `direction ∈
    {"rising", "falling", "either"}`.
    """
    CONSTRUCTION_PARAMS = {
        "threshold": float, "hysteresis": float, "direction": str,
    }
    TUNABLE_PARAMS = {"threshold": float, "hysteresis": float}

    _VALID_DIRECTIONS = ("rising", "falling", "either")

    def __init__(
        self,
        threshold: float,
        hysteresis: float = 0.0,
        direction: str = "rising",
        output_sensor: Optional[str] = None,
    ):
        self.threshold = float(threshold)
        self.hysteresis = float(hysteresis)
        if direction not in self._VALID_DIRECTIONS:
            raise ValueError(
                f"EdgeDetector direction must be one of "
                f"{self._VALID_DIRECTIONS}, got {direction!r}"
            )
        self.direction = direction
        self.output_sensor = output_sensor
        # (device, sensor) -> {"last": float, "armed_rising": bool,
        #                      "armed_falling": bool, "warned_vec": bool}
        self._state: dict = {}

    def process(self, frame: IMUFrame) -> Iterable[IMUFrame]:
        yield frame
        key = (frame.device, frame.sensor)
        if len(frame.values) != 1:
            st = self._state.setdefault(key, {})
            if not st.get("warned_vec"):
                log.warning(
                    "[EdgeDetector] %s/%s arity=%d not 1 — vec inputs "
                    "are not supported; compose with Magnitude first",
                    frame.device, frame.sensor, len(frame.values),
                )
                st["warned_vec"] = True
            return

        v = frame.values[0]
        st = self._state.setdefault(
            key,
            {"last": v, "armed_rising": True, "armed_falling": True,
             "warned_vec": False},
        )
        last = st["last"]
        st["last"] = v
        emit = False
        if self.direction in ("rising", "either"):
            if st["armed_rising"] and last < self.threshold <= v:
                emit = True
                st["armed_rising"] = False
            elif not st["armed_rising"] and v < self.threshold - self.hysteresis:
                st["armed_rising"] = True
        if self.direction in ("falling", "either"):
            if st["armed_falling"] and last > self.threshold >= v:
                emit = True
                st["armed_falling"] = False
            elif not st["armed_falling"] and v > self.threshold + self.hysteresis:
                st["armed_falling"] = True

        if emit:
            out = self.output_sensor or f"{frame.sensor}_edge"
            yield IMUFrame(
                device=frame.device, sensor=out,
                t_recv=frame.t_recv, values=(1.0,),
            )

    def outputs(self, input_sensor: str) -> List[str]:
        out = self.output_sensor or f"{input_sensor}_edge"
        return [input_sensor, out]


class Window(Stage):
    """
    Rolling N-sample window per (device, sensor). On each frame, emit
    the pass-through plus a derived frame containing the selected
    statistic over the last N samples.

    `n_samples` is set at construction (deque is sized once; resizing
    mid-stream is messy state surgery, so it's intentionally not in
    TUNABLE_PARAMS). `stat ∈ {mean, std, max, min, range, sum}` is
    tunable — the deque holds raw values; the stat is computed on
    every frame from the window contents.

    Multi-axis input: stat is computed independently per axis (output
    arity matches input arity). Until the window is full (first N-1
    frames per key), the derived frame is still emitted using whatever
    samples are available — operators usually want partial-window
    output during the warm-up rather than silence.

    Output sensor defaults to `<sensor>_<stat>` (e.g. `acc_mag_std`).
    """
    CONSTRUCTION_PARAMS = {"n_samples": int, "stat": str}
    TUNABLE_PARAMS = {"stat": str}

    _VALID_STATS = ("mean", "std", "max", "min", "range", "sum")

    def __init__(
        self,
        n_samples: int,
        stat: str = "mean",
        output_sensor: Optional[str] = None,
    ):
        if n_samples < 1:
            raise ValueError(f"Window n_samples must be ≥1, got {n_samples}")
        if stat not in self._VALID_STATS:
            raise ValueError(
                f"Window stat must be one of {self._VALID_STATS}, got {stat!r}"
            )
        self.n_samples = int(n_samples)
        self.stat = stat
        self.output_sensor = output_sensor
        # (device, sensor) -> deque[tuple[float, ...]]
        self._state: dict = {}

    def _compute(self, samples: list) -> Tuple[float, ...]:
        """Compute the configured stat per-axis over the windowed samples."""
        if not samples:
            return ()
        arity = len(samples[0])
        out = []
        for axis in range(arity):
            col = [s[axis] for s in samples]
            if self.stat == "mean":
                out.append(sum(col) / len(col))
            elif self.stat == "sum":
                out.append(sum(col))
            elif self.stat == "max":
                out.append(max(col))
            elif self.stat == "min":
                out.append(min(col))
            elif self.stat == "range":
                out.append(max(col) - min(col))
            elif self.stat == "std":
                m = sum(col) / len(col)
                var = sum((v - m) ** 2 for v in col) / len(col)
                out.append(math.sqrt(var))
        return tuple(out)

    def process(self, frame: IMUFrame) -> Iterable[IMUFrame]:
        yield frame
        key = (frame.device, frame.sensor)
        dq = self._state.get(key)
        if dq is None or dq.maxlen != self.n_samples:
            # First frame for this key (or n_samples reconstructed,
            # which currently can't happen since n_samples isn't
            # tunable — guard left for forward compatibility).
            dq = deque(maxlen=self.n_samples)
            self._state[key] = dq
        dq.append(frame.values)
        stat_values = self._compute(list(dq))
        out = self.output_sensor or f"{frame.sensor}_{self.stat}"
        yield IMUFrame(
            device=frame.device, sensor=out,
            t_recv=frame.t_recv, values=stat_values,
        )

    def outputs(self, input_sensor: str) -> List[str]:
        out = self.output_sensor or f"{input_sensor}_{self.stat}"
        return [input_sensor, out]


class Scale(Stage):
    """
    Affine map: `y = (x - offset) * scale`, applied uniformly per axis.
    Stateless — same scale/offset apply across all (device, sensor)
    streams that flow through this instance.

    Use case: map IMU ranges into PD/DAW parameter ranges. Example —
    map tilt (0°–90°) onto a normalized [0,1] for a filter cutoff:
    `Scale(scale=1/90, offset=0)`.

    Default emit is derived (raw passes through, scaled value at
    `<sensor>_scaled`); set `output_sensor` to override, or pass an
    explicit name to live in the same value space the consumer expects.
    """
    CONSTRUCTION_PARAMS = {"scale": float, "offset": float}
    TUNABLE_PARAMS = {"scale": float, "offset": float}

    def __init__(
        self,
        scale: float = 1.0,
        offset: float = 0.0,
        output_sensor: Optional[str] = None,
    ):
        self.scale = float(scale)
        self.offset = float(offset)
        self.output_sensor = output_sensor

    def process(self, frame: IMUFrame) -> Iterable[IMUFrame]:
        yield frame
        new = tuple((v - self.offset) * self.scale for v in frame.values)
        out = self.output_sensor or f"{frame.sensor}_scaled"
        yield IMUFrame(
            device=frame.device, sensor=out,
            t_recv=frame.t_recv, values=new,
        )

    def outputs(self, input_sensor: str) -> List[str]:
        out = self.output_sensor or f"{input_sensor}_scaled"
        return [input_sensor, out]


class OscEmit(Stage):
    """
    Terminal stage. Sends each frame to OSC as `/<device>/<sensor>`.
    Scalar values are sent unwrapped (matches the prior
    behaviour of `temp`, `light`); multi-value frames are sent as a
    tuple (matches `acc`, `gyro`, `quat`, etc.).

    `muted=True` skips the actual UDP send while still consuming the
    frame — used by capture mode so the JSONL recording captures the
    same post-pipeline frames the receiver would have seen, without
    side-effecting the audio plane mid-training.
    """
    is_terminal = True

    def __init__(self, osc_client):
        self.osc_client = osc_client
        self.muted = False

    def process(self, frame: IMUFrame) -> Iterable[IMUFrame]:
        if self.muted:
            return ()
        addr = f"/{frame.device}/{frame.sensor}"
        try:
            if len(frame.values) == 1:
                self.osc_client.send_message(addr, frame.values[0])
            else:
                self.osc_client.send_message(addr, frame.values)
        except OSError as e:
            # Receiver not reachable — silent. log.exception here would
            # dominate CPU at high frame rates and throttle the BLE
            # callback thread (observed: 199 Hz aggregate → 60 Hz when
            # PD is unreachable and every send raised + logged a traceback).
            if e.errno not in _TRANSIENT_OSC_ERRNOS:
                log.warning("[OscEmit] send_message %s failed: %s", addr, e)
        except BaseException as e:
            log.warning("[OscEmit] send_message %s failed: %s", addr, e)
        return ()


# --- Stage registry for /cmd/pipeline/add ------------------------------------
#
# Only stages with scalar constructor params (no runtime-dep references) are
# in this map. C2's /cmd/pipeline/add looks up the class here and rejects
# with /error/not-constructible otherwise. Stages NOT here can still be
# inspected and tuned (via TUNABLE_PARAMS) — they just can't be added/wired
# from OSC, since they need refs (osc_client, Latch, RecorderSink,
# GestureLibrary, state_lookup) that only run_fs.py can produce.

_STAGE_REGISTRY: Dict[str, type] = {
    "LowPass": LowPass,
    "HighPass": HighPass,
    "Magnitude": Magnitude,
    "Tilt": Tilt,
    "Differentiator": Differentiator,
    "EdgeDetector": EdgeDetector,
    "Window": Window,
    "Scale": Scale,
}


# --- Persisted-override application ------------------------------------------
#
# Counterpart to the C2 /cmd/pipeline/* persistence writers in sense/c2.py.
# When the process restarts, the run_fs.py wiring builds default pipelines +
# splices runtime-dep stages (Recorder, GestureRecognizer, PositionTracker,
# LatchUpdate). Then this function applies any persisted operator edits on
# top — composition overrides replace the constructible-stage chain while
# preserving the runtime-dep stages in their existing positions; tunings
# overrides setattr params onto live runtime-dep stages.

def apply_pipeline_overrides(states, config) -> None:
    """Apply `config["pipelines"]` overrides to live state pipelines.
    Idempotent + safe to call with no overrides present.

    `states` is duck-typed: each must expose `.address` (str) and
    `.pipelines` (dict[str -> Pipeline]). Decoupled from sense.state
    to avoid a circular import.
    """
    overrides = config.get("pipelines", {})
    if not overrides:
        return
    for state in states:
        device = overrides.get(state.address, {})
        if not device:
            continue
        for pipe_name, entry in device.items():
            pipe = state.pipelines.get(pipe_name)
            if pipe is None:
                log.warning("[%s] override targets unknown pipeline %s; skipping",
                            state.address, pipe_name)
                continue
            composition = entry.get("composition")
            if composition is not None:
                try:
                    pipe.stages = rebuild_composition_with_overrides(
                        pipe, composition,
                    )
                    log.info("[%s/%s] composition override applied: %s",
                             state.address, pipe_name,
                             [s.__class__.__name__ for s in pipe.stages])
                except BaseException:
                    log.exception("[%s/%s] composition apply failed",
                                  state.address, pipe_name)
            for cls_name, params in entry.get("tunings", {}).items():
                target = next(
                    (s for s in pipe.stages
                     if s.__class__.__name__ == cls_name),
                    None,
                )
                if target is None:
                    log.warning("[%s/%s] tunings: %s not in live pipeline; skipping",
                                state.address, pipe_name, cls_name)
                    continue
                for k, v in params.items():
                    if k not in target.TUNABLE_PARAMS:
                        log.warning("[%s/%s] tunings: %s.%s not in TUNABLE_PARAMS; skipping",
                                    state.address, pipe_name, cls_name, k)
                        continue
                    try:
                        setattr(target, k, v)
                        log.info("[%s/%s] tuning applied: %s.%s = %r",
                                 state.address, pipe_name, cls_name, k, v)
                    except BaseException:
                        log.exception("[%s/%s] tunings: setattr %s.%s failed",
                                      state.address, pipe_name, cls_name, k)


def rebuild_composition_with_overrides(pipe, composition_list) -> List["Stage"]:
    """Build a new stages list from a composition override. The override
    provides the constructible-stage chain in order; non-constructible
    stages (Recorder, GestureRecognizer, PositionTracker, LatchUpdate,
    OscEmit) are preserved from the current pipe.stages in their
    existing relative order."""
    new_stages: List["Stage"] = []
    for item in composition_list:
        cls_name = item.get("class")
        params = item.get("params", {})
        klass = _STAGE_REGISTRY.get(cls_name)
        if klass is None:
            log.warning("override references unknown class %r; skipping",
                        cls_name)
            continue
        try:
            stage = klass(**params)
        except BaseException:
            log.exception("override construction failed for %s(**%s)",
                          cls_name, params)
            continue
        new_stages.append(stage)
    for stage in pipe.stages:
        if stage.__class__.__name__ not in _STAGE_REGISTRY:
            new_stages.append(stage)
    return new_stages
