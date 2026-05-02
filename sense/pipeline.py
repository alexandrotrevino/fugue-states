"""
Stream-processing pipeline for IMU frames.

Each MetaWearState owns a per-sensor pipeline. Sensor callbacks build
an `IMUFrame` and `push()` it; frames flow through `Stage`s in order;
the terminal `OscEmit` stage publishes them as `/<MAC>/<sensor>` OSC
messages.

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
import logging
import math
import time
from collections import deque
from dataclasses import dataclass
from typing import Iterable, List, Optional, Tuple

log = logging.getLogger("fs.pipeline")


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
    """
    is_terminal: bool = False

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
    """
    def __init__(
        self,
        cutoff_hz: float,
        fs: float,
        output_sensor: Optional[str] = None,
    ):
        rc = 1.0 / (2.0 * math.pi * cutoff_hz)
        dt = 1.0 / fs
        self.alpha = dt / (rc + dt)
        self.output_sensor = output_sensor
        self._state: dict = {}

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
        except BaseException:
            log.exception("[OscEmit] send_message %s failed", addr)
        return ()
