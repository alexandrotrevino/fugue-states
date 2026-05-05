"""
Position tracking from IMU sensor-fusion outputs.

`PositionTracker` is a Pipeline Stage that double-integrates
gravity-removed acceleration (`linear_acc`) into world-frame relative
position, with quaternion-based device→world rotation, ZUPT-driven
velocity reset, and online bias refinement during stationary windows.

Inputs (from Sensor Fusion outputs — config requires NDOF mode with
`outputs: ["linear_acc", "quaternion", "corrected_gyro"]`):

- `linear_acc` (3-axis): gravity-removed acceleration in **device frame**.
  Bosch BSX fusion does not rotate this to world frame on its own — we
  do that here using the latest quaternion before integrating.
- `quat` (4-component, w-x-y-z): orientation. Rotates linear_acc into
  the fusion's reference frame established at session start.
- `corrected_gyro` (3-axis): rotation rate. Used solely for ZUPT
  detection (a stationary wrist has near-zero gyro magnitude).

Outputs (synthetic frames flowing downstream to OscEmit):

- `position` (3-axis world-frame meters relative to first integration step)
- `velocity` (3-axis world-frame m/s, optional, default on)
- `zupt` (scalar 1.0 stationary / 0.0 moving, optional, default on)

Cold-start protocol: the first `calibration_samples` linear_acc frames
are accumulated as the initial bias estimate. During this period, no
position frames are emitted and the tracker calls
`state.set_position_calibrating(True)` (via the supplied state-lookup
callable) so the LED can render YELLOW. Wearer holds still during this.

ZUPT detector: rolling-window std of acc magnitude AND latest gyro
magnitude both below their thresholds → stationary. On stationary,
velocity is reset to zero and the bias estimate is refined via slow
EMA. Position keeps accumulating (only velocity resets).

The same instance is inserted into all three pipelines (linear_acc,
quat, corrected_gyro) so it sees every relevant frame; it ticks /
emits only on linear_acc frames.
"""
import logging
import math
import time
from collections import deque
from typing import Callable, Iterable, List, Optional, Tuple

import numpy as np

from .pipeline import IMUFrame, Stage

log = logging.getLogger("fs.position")


def _quat_rotate(q: Tuple[float, float, float, float],
                 v: Tuple[float, float, float]) -> Tuple[float, float, float]:
    """
    Rotate a 3-vector v by quaternion q (w, x, y, z) — the standard
    sandwich product expressed as a 3x3 rotation matrix multiply,
    inlined for speed (called per-frame). Returns a fresh 3-tuple.
    """
    w, x, y, z = q
    vx, vy, vz = v
    # Rotation matrix elements (Hamilton convention):
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


class PositionTracker(Stage):
    """
    Double-integrate sensor-fusion `linear_acc` into world-frame
    relative position. See module docstring for the full protocol.

    Constructor knobs all have sensible defaults from the literature
    on consumer-IMU pedestrian dead reckoning, but every one is
    surfaced via `run_fs.py` CLI flags for tuning during the spike.
    """
    is_terminal = False

    INPUT_LINEAR_ACC = "linear_acc"
    INPUT_QUAT = "quat"
    INPUT_GYRO = "corrected_gyro"

    def __init__(
        self,
        zupt_acc_std_threshold: float = 0.15,    # m/s² — rolling std of acc-mag below = stationary candidate
        zupt_gyro_mag_threshold: float = 8.0,    # deg/s — instantaneous gyro magnitude below = stationary candidate
        zupt_window_samples: int = 10,            # rolling window for std computation (~0.4s at 25Hz)
        calibration_samples: int = 125,           # cold-start bias window (~5s at 25Hz)
        bias_ema_alpha: float = 0.05,             # online bias EMA smoothing during stationary windows
        # velocity and zupt default OFF — they're diagnostic, and emitting
        # them creates 3 downstream frames per linear_acc tick (Recorder
        # writes + OscEmit sends per channel), which back up the BLE
        # callback thread and cap the linear_acc input rate at ~9 Hz.
        # Opt them on via the run_fs.py CLI flags for debug/analysis runs.
        emit_velocity: bool = False,
        emit_zupt: bool = False,
        state_lookup: Optional[Callable] = None,  # (mac) -> MetaWearState | None, for LED control
        debug: bool = False,
    ):
        self.zupt_acc_std_threshold = zupt_acc_std_threshold
        self.zupt_gyro_mag_threshold = zupt_gyro_mag_threshold
        self.zupt_window_samples = zupt_window_samples
        self.calibration_samples = calibration_samples
        self.bias_ema_alpha = bias_ema_alpha
        self.emit_velocity = emit_velocity
        self.emit_zupt = emit_zupt
        self.state_lookup = state_lookup
        self.debug = debug

        # Per-device state
        self._velocity: dict = {}            # device -> [vx, vy, vz]
        self._position: dict = {}            # device -> [px, py, pz]
        self._bias: dict = {}                # device -> [bx, by, bz]  (world-frame, accumulated)
        self._latest_quat: dict = {}         # device -> (w, x, y, z)
        self._latest_gyro_mag: dict = {}     # device -> float (most recent)
        self._last_t: dict = {}              # device -> float (last linear_acc t_recv)
        self._acc_mag_buffer: dict = {}      # device -> deque[float]
        self._stationary: dict = {}          # device -> bool
        self._calibration_buffer: dict = {}  # device -> list[(ax, ay, az)] world-frame samples
        self._calibrated: dict = {}          # device -> bool

    # --- Public hooks --------------------------------------------------------

    def outputs(self, input_sensor: str) -> List[str]:
        # Position addresses only emerge from the linear_acc branch;
        # quat/gyro pipelines just feed state, no synthetic frames.
        if input_sensor != self.INPUT_LINEAR_ACC:
            return [input_sensor]
        out = [input_sensor, "position"]
        if self.emit_velocity:
            out.append("velocity")
        if self.emit_zupt:
            out.append("zupt")
        return out

    def process(self, frame: IMUFrame) -> Iterable[IMUFrame]:
        # Pass-through every frame — quat/gyro flow downstream unchanged
        # and linear_acc still publishes as `/<MAC>/linear_acc`.
        yield frame

        if frame.sensor == self.INPUT_QUAT:
            self._on_quat(frame)
            return
        if frame.sensor == self.INPUT_GYRO:
            self._on_gyro(frame)
            return
        if frame.sensor == self.INPUT_LINEAR_ACC:
            yield from self._on_linear_acc(frame)
            return

    # --- Per-sensor handlers -------------------------------------------------

    def _on_quat(self, frame: IMUFrame) -> None:
        if len(frame.values) >= 4:
            self._latest_quat[frame.device] = tuple(frame.values[:4])

    def _on_gyro(self, frame: IMUFrame) -> None:
        # Gyro magnitude — Euclidean norm of the 3 components.
        if len(frame.values) >= 3:
            gx, gy, gz = frame.values[:3]
            self._latest_gyro_mag[frame.device] = math.sqrt(
                gx * gx + gy * gy + gz * gz
            )

    def _on_linear_acc(self, frame: IMUFrame) -> Iterable[IMUFrame]:
        device = frame.device
        if len(frame.values) < 3:
            return

        # Need a quaternion in hand to rotate into world frame.
        quat = self._latest_quat.get(device)
        if quat is None:
            if self.debug:
                log.debug("[%s] position: skipping linear_acc — no quat yet",
                          device)
            return

        ax_d, ay_d, az_d = frame.values[:3]
        ax_w, ay_w, az_w = _quat_rotate(quat, (ax_d, ay_d, az_d))

        # Cold-start calibration: accumulate world-frame linear_acc until
        # we have enough samples for a stable bias estimate.
        if not self._calibrated.get(device, False):
            buf = self._calibration_buffer.setdefault(device, [])
            buf.append((ax_w, ay_w, az_w))
            # Mark calibrating on the first sample (LED → yellow).
            if len(buf) == 1:
                self._set_calibrating(device, True)
            if len(buf) >= self.calibration_samples:
                bias = (
                    sum(s[0] for s in buf) / len(buf),
                    sum(s[1] for s in buf) / len(buf),
                    sum(s[2] for s in buf) / len(buf),
                )
                self._bias[device] = list(bias)
                self._velocity[device] = [0.0, 0.0, 0.0]
                self._position[device] = [0.0, 0.0, 0.0]
                self._calibrated[device] = True
                self._last_t[device] = frame.t_recv
                self._set_calibrating(device, False)
                log.info("[%s] position calibrated: bias=(%.4f, %.4f, %.4f) m/s² "
                         "from %d samples",
                         device, bias[0], bias[1], bias[2], len(buf))
            return  # no emission during calibration

        # Bias-subtract.
        bx, by, bz = self._bias[device]
        ax, ay, az = ax_w - bx, ay_w - by, az_w - bz

        # ZUPT detection — uses RAW acc magnitude (pre-bias-subtract is
        # informative for "is the wrist still" since we want gross motion).
        acc_mag = math.sqrt(ax_w * ax_w + ay_w * ay_w + az_w * az_w)
        buf = self._acc_mag_buffer.setdefault(
            device, deque(maxlen=self.zupt_window_samples),
        )
        buf.append(acc_mag)
        gyro_mag = self._latest_gyro_mag.get(device, 0.0)

        stationary = False
        if len(buf) >= self.zupt_window_samples:
            mean = sum(buf) / len(buf)
            std = math.sqrt(sum((m - mean) ** 2 for m in buf) / len(buf))
            stationary = (
                std < self.zupt_acc_std_threshold
                and gyro_mag < self.zupt_gyro_mag_threshold
            )
        prev_stationary = self._stationary.get(device, False)
        self._stationary[device] = stationary

        # Integrate.
        last_t = self._last_t[device]
        dt = max(0.0, frame.t_recv - last_t)
        self._last_t[device] = frame.t_recv

        if stationary:
            # ZUPT: reset velocity to zero. Position keeps accumulating
            # (which means it sticks at its last value during stillness).
            self._velocity[device] = [0.0, 0.0, 0.0]
            # Refine bias toward current world-frame acc — during stillness
            # the residual after bias-subtract should average to zero, so
            # the EMA slowly tracks the true bias.
            alpha = self.bias_ema_alpha
            self._bias[device][0] = (1 - alpha) * bx + alpha * ax_w
            self._bias[device][1] = (1 - alpha) * by + alpha * ay_w
            self._bias[device][2] = (1 - alpha) * bz + alpha * az_w
        else:
            v = self._velocity[device]
            v[0] += ax * dt
            v[1] += ay * dt
            v[2] += az * dt
            p = self._position[device]
            p[0] += v[0] * dt
            p[1] += v[1] * dt
            p[2] += v[2] * dt

        if self.debug:
            log.debug("[%s] position: t=%.3f stationary=%s "
                      "acc_w=(%.3f,%.3f,%.3f) v=(%.3f,%.3f,%.3f) "
                      "p=(%.3f,%.3f,%.3f)",
                      device, frame.t_recv, stationary,
                      ax_w, ay_w, az_w,
                      *self._velocity[device],
                      *self._position[device])

        # Emit synthetic frames.
        p = self._position[device]
        yield IMUFrame(
            device=device, sensor="position", t_recv=frame.t_recv,
            values=(p[0], p[1], p[2]),
        )
        if self.emit_velocity:
            v = self._velocity[device]
            yield IMUFrame(
                device=device, sensor="velocity", t_recv=frame.t_recv,
                values=(v[0], v[1], v[2]),
            )
        if self.emit_zupt:
            # 1.0 if stationary, 0.0 otherwise. Emit on every tick (not just
            # transitions) so PD can react smoothly without edge-detection.
            yield IMUFrame(
                device=device, sensor="zupt", t_recv=frame.t_recv,
                values=(1.0 if stationary else 0.0,),
            )

    # --- Public control ------------------------------------------------------

    def request_recalibration(self, device: str) -> bool:
        """
        Force a fresh cold-start calibration for one device. Clears all
        per-device tracker state (bias, velocity, position, ZUPT
        windows) so the next linear_acc frame begins a new calibration
        buffer. Flips the LED to YELLOW immediately via state_lookup so
        the operator sees the request landed even before the next frame
        arrives. Returns True (always — recalibration is a request, not
        a contract that frames will follow soon).

        Driven by /cmd/calibrate handler in sense.c2.
        """
        # Clear every dict that holds per-device tracker state. The next
        # linear_acc frame will rebuild _calibration_buffer at length 1,
        # which would itself trigger _set_calibrating(True); we do it now
        # for instant LED feedback so streaming-but-late-frame setups
        # still show the operator their command landed.
        self._calibration_buffer.pop(device, None)
        self._calibrated[device] = False
        self._velocity.pop(device, None)
        self._position.pop(device, None)
        self._bias.pop(device, None)
        self._last_t.pop(device, None)
        self._acc_mag_buffer.pop(device, None)
        self._stationary.pop(device, None)
        log.info("[%s] position: recalibration requested", device)
        self._set_calibrating(device, True)
        return True

    # --- Helpers -------------------------------------------------------------

    def _set_calibrating(self, device: str, calibrating: bool) -> None:
        if self.state_lookup is None:
            return
        try:
            state = self.state_lookup(device)
        except BaseException:
            log.exception("[%s] position: state_lookup raised; LED will lag",
                          device)
            return
        if state is None:
            return
        try:
            state.set_position_calibrating(calibrating)
        except BaseException:
            log.exception("[%s] position: set_position_calibrating raised", device)
