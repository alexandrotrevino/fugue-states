"""
C2 — Remote Control protocol for Fugue States.

Spec: docs/c2.md.

This module owns the process-level control surface:
  - Shutdown token (regenerates per process start; rides every heartbeat).
  - /cmd/<verb> handlers (status, start, stop, calibrate, shutdown,
    plus configure/* stubs reserved for Pass 2).
  - Heartbeat tick driven from run_fs.py's watchdog loop.
  - Snapshot replies for /cmd/status.

Per-device state-change events (/state/<mac>/connected etc.) are emitted
from MetaWearState directly, where the transitions happen — see
MetaWearState._emit_state_event.

Pass-1 scope: observability outward. configure/* are wired so callers
get a clean rejection instead of silent drop, but real bodies land in
Pass 2.
"""
import errno
import logging
import os
import time
from typing import Callable, Dict, Optional

from .fs_setup import is_valid_ip, is_valid_port, write_local_overrides

log = logging.getLogger("fs.c2")

HEARTBEAT_PERIOD_S = 2.0

_TRANSIENT_OSC_ERRNOS = (errno.ENETUNREACH, errno.EHOSTUNREACH, errno.ECONNREFUSED)

# Knob whitelist for /cmd/configure/sensor. Keys here mirror the
# fs_config schema; values outside this map are rejected with
# /error/configure-rejected bad-key. Ambient Light is included even
# though MMRL doesn't have one — fs_setup drops it on validate, so an
# operator change is a no-op on MMRL but still type-safe.
_ALLOWED_SENSOR_KEYS = {
    "Accelerometer": ("odr", "range"),
    "Gyroscope": ("odr", "range"),
    "Gyroscope160": ("odr", "range"),
    "Magnetometer": ("odr",),
    "Temperature": ("period",),
    "Ambient Light": ("odr",),
    "Sensor Fusion": ("mode", "accRange", "gyroRange", "outputs"),
}


def send_best_effort(client, addr: str, value, op: str) -> bool:
    """Mirror of MetaWearState._osc_send_best_effort for module-level
    sends. Receiver-unreachable errnos are treated as soft failures
    (one WARNING line, no traceback)."""
    try:
        client.send_message(addr, value)
        return True
    except OSError as e:
        if e.errno in _TRANSIENT_OSC_ERRNOS:
            log.warning("[c2] %s: receiver unreachable (%s); skipping", op, e)
            return False
        log.warning("[c2] %s: %s", op, e)
        return False
    except BaseException as e:
        log.warning("[c2] %s: %s", op, e)
        return False


class Controller:
    """
    Process-level controller. One instance per run_fs.py process.

    Construction order in run_fs.py:
        1. Build OSC connection.
        2. Build MetaWearStates (each maps its own legacy handlers).
        3. Build Controller(osc, states, ...).
        4. controller.install()  → registers /cmd/* handlers.
        5. Main loop: call controller.tick() once per watchdog tick.

    The controller does NOT own the OSC server's lifecycle — the server
    is started by ControlledOSCConnection inside MetaWearState.set_OSC.
    Controller just adds new dispatcher mappings.
    """

    def __init__(
        self,
        osc,
        states: list,
        config_path: Optional[str] = None,
        recorder_path_provider: Callable[[], Optional[str]] = lambda: None,
        position_track_enabled: bool = False,
        position_trackers: Optional[Dict[str, "object"]] = None,
    ):
        self.osc = osc
        self.states = states
        self.config_path = config_path
        self._recorder_path_provider = recorder_path_provider
        self.position_track_enabled = position_track_enabled
        # Mapping {mac → PositionTracker} populated by run_fs.py when
        # --position-track is set. Empty/None means /cmd/calibrate is
        # rejected with not-enabled.
        self.position_trackers: Dict[str, "object"] = position_trackers or {}

        # 6-char hex token from os.urandom; lives in memory only,
        # rotates per process start. Distributed via /state/heartbeat.
        self.shutdown_token = os.urandom(3).hex()
        log.info("c2 shutdown token: %s", self.shutdown_token)

        self._t0 = time.monotonic()
        self._last_heartbeat_at = 0.0
        self._stop_requested = False

    @property
    def should_stop(self) -> bool:
        """Polled by run_fs.py main loop — set True by /cmd/shutdown."""
        return self._stop_requested

    def uptime_s(self) -> float:
        return time.monotonic() - self._t0

    @property
    def recording_path(self) -> Optional[str]:
        return self._recorder_path_provider()

    @property
    def recording_active(self) -> bool:
        return self.recording_path is not None

    # --- Handler registration -------------------------------------------------

    def install(self) -> None:
        """Register /cmd/... handlers on the OSC dispatcher. Idempotent
        per address (pythonosc allows multiple handlers per address;
        re-installing would double-fire — only call once)."""
        d = self.osc.server.dispatcher
        d.map("/cmd/status", self._on_status)
        d.map("/cmd/start", self._on_start)
        d.map("/cmd/stop", self._on_stop)
        d.map("/cmd/shutdown", self._on_shutdown)
        d.map("/cmd/calibrate", self._on_calibrate)
        d.map("/cmd/configure/sensor", self._on_configure_sensor)
        d.map("/cmd/configure/network", self._on_configure_network)
        log.info("c2 handlers installed (token=%s)", self.shutdown_token)

    def announce_initial_state(self) -> None:
        """Emit one-shot state events for things that won't otherwise
        change during the run. Called after install(), once, by run_fs.py.
        Recording is process-level and decided by CLI args at startup."""
        if self.recording_active:
            self._send("/state/recording", [1, self.recording_path])

    # --- Heartbeat / tick -----------------------------------------------------

    def tick(self) -> None:
        """Called from the watchdog loop (~1Hz). Emits heartbeat at
        HEARTBEAT_PERIOD_S cadence; cheap when not yet due."""
        now = time.monotonic()
        if (now - self._last_heartbeat_at) < HEARTBEAT_PERIOD_S:
            return
        self._last_heartbeat_at = now
        self._send(
            "/state/heartbeat",
            [round(self.uptime_s(), 2), self.shutdown_token],
        )

    # --- Internals ------------------------------------------------------------

    def _send(self, addr: str, value) -> bool:
        return send_best_effort(self.osc.client, addr, value, op=addr)

    def _device_snapshot(self, s) -> list:
        return [
            1 if s.connected else 0,
            1 if s.streaming else 0,
            1 if self.recording_active else 0,
            len(s.failed_sources),
        ]

    def _global_snapshot(self) -> list:
        return [
            len(self.states),
            1 if self.position_track_enabled else 0,
            1 if self.recording_active else 0,
            round(self.uptime_s(), 2),
        ]

    def _resolve_targets(self, mac_arg: str) -> list:
        """Empty / unset MAC → broadcast (all states). Otherwise filter
        by exact address match; empty list means unknown device."""
        if not mac_arg:
            return list(self.states)
        for s in self.states:
            if s.address == mac_arg:
                return [s]
        return []

    # --- Command handlers -----------------------------------------------------

    def _on_status(self, address, *args):
        log.info("c2 /cmd/status")
        for s in self.states:
            self._send(f"/state/{s.address}/snapshot", self._device_snapshot(s))
        self._send("/state/global/snapshot", self._global_snapshot())

    def _on_start(self, address, *args):
        mac = args[0] if args else ""
        targets = self._resolve_targets(mac)
        if mac and not targets:
            self._send("/error/not-connected", [mac, "unknown-device"])
            return
        log.info("c2 /cmd/start mac=%r → %d target(s)", mac, len(targets))
        for s in targets:
            if s.streaming:
                self._send("/error/already-streaming", [s.address])
                continue
            if not s.connected:
                self._send("/error/not-connected", [s.address, "not-connected"])
                continue
            try:
                s.start_sensors(s.sensor_config)
            except BaseException as e:
                log.exception("c2 /cmd/start: start_sensors raised on %s", s.address)
                self._send("/error/start-failed", [s.address, repr(e)])

    def _on_stop(self, address, *args):
        mac = args[0] if args else ""
        targets = self._resolve_targets(mac)
        if mac and not targets:
            self._send("/error/not-connected", [mac, "unknown-device"])
            return
        log.info("c2 /cmd/stop mac=%r → %d target(s)", mac, len(targets))
        for s in targets:
            if not s.streaming:
                self._send("/error/not-streaming", [s.address])
                continue
            try:
                s.stop_sensors(s.sensor_config)
            except BaseException as e:
                log.exception("c2 /cmd/stop: stop_sensors raised on %s", s.address)
                self._send("/error/stop-failed", [s.address, repr(e)])

    def _on_shutdown(self, address, *args):
        provided = args[0] if args else ""
        if not provided:
            self._send("/error/bad-token", ["no-token-yet"])
            return
        if str(provided) != self.shutdown_token:
            self._send("/error/bad-token", ["mismatch"])
            log.warning("c2 /cmd/shutdown: bad token (got %r)", provided)
            return
        log.warning("c2 /cmd/shutdown: accepted, requesting exit")
        self._stop_requested = True

    def _on_calibrate(self, address, *args):
        if not self.position_track_enabled or not self.position_trackers:
            self._send("/error/calibrate-failed", ["", "not-enabled"])
            return
        mac = args[0] if args else ""
        if mac:
            if mac not in self.position_trackers:
                self._send("/error/calibrate-failed", [mac, "unknown-device"])
                return
            targets = [mac]
        else:
            targets = list(self.position_trackers.keys())

        log.info("c2 /cmd/calibrate mac=%r → %d tracker(s)", mac, len(targets))
        for m in targets:
            try:
                self.position_trackers[m].request_recalibration(m)
            except BaseException as e:
                log.exception("c2 /cmd/calibrate: recalibrate %s raised", m)
                self._send("/error/calibrate-failed", [m, repr(e)])

    def _on_configure_sensor(self, address, *args):
        if len(args) < 4:
            self._send("/error/configure-rejected", ["bad-args"])
            return
        if any(s.streaming for s in self.states):
            self._send("/error/configure-rejected", ["streaming"])
            return
        if not self.config_path:
            self._send("/error/configure-rejected", ["no-config-path"])
            return

        mac = str(args[0])
        sensor = str(args[1])
        key = str(args[2])
        value = args[3]

        target = next((s for s in self.states if s.address == mac), None)
        if target is None:
            self._send("/error/not-connected", [mac, "unknown-device"])
            return

        if sensor not in _ALLOWED_SENSOR_KEYS:
            self._send("/error/configure-rejected", ["unknown-sensor"])
            return
        if key not in _ALLOWED_SENSOR_KEYS[sensor]:
            self._send("/error/configure-rejected", ["bad-key"])
            return

        # Sensor Fusion outputs from PD arrive as a comma-separated string
        # (PD's [list] objects don't naturally produce nested OSC lists).
        # Lists pass through. Anything else for this key is bad-value.
        if sensor == "Sensor Fusion" and key == "outputs":
            if isinstance(value, str):
                value = [v.strip() for v in value.split(",") if v.strip()]
            elif not isinstance(value, (list, tuple)):
                self._send("/error/configure-rejected", ["bad-value"])
                return
            value = [str(v).lower() for v in value]

        # Apply in-memory.
        sensor_block = target.sensor_config.setdefault(sensor, {})
        sensor_block[key] = value
        # Sensor Fusion outputs has a parallel attribute on the state
        # (set by the validator at __init__); keep them consistent so
        # advertise() and the fusion subscribe path see the same set.
        if sensor == "Sensor Fusion" and key == "outputs":
            target.fusion_outputs = list(value)

        # Persist. We rewrite the entire metawear.devices block in
        # local.json (deep-merge replaces lists wholesale, so per-MAC
        # patching can't be expressed). See write_local_overrides
        # docstring for the trade-off.
        try:
            self._persist({"metawear": {"devices": self._devices_payload()}})
        except BaseException as e:
            log.exception("c2 /cmd/configure/sensor: persist failed")
            self._send("/error/configure-rejected", [f"persist-failed:{e}"])
            return

        log.info("c2 /cmd/configure/sensor accepted: %s %s.%s = %r",
                 mac, sensor, key, value)
        # Re-advertise so receivers learn the new address space — fusion
        # output changes alter what /<MAC>/__advertise__ reports.
        try:
            target.advertise()
        except BaseException:
            log.exception("c2 /cmd/configure/sensor: advertise raised")
        self._send("/state/configured", ["sensor", f"{mac}/{sensor}/{key}"])

    def _on_configure_network(self, address, *args):
        if len(args) < 2:
            self._send("/error/configure-rejected", ["bad-args"])
            return
        if not self.config_path:
            self._send("/error/configure-rejected", ["no-config-path"])
            return

        ip = str(args[0])
        try:
            port = int(args[1])
        except (TypeError, ValueError):
            self._send("/error/configure-rejected", ["bad-port"])
            return

        if not is_valid_ip(ip) or not is_valid_port(port):
            self._send("/error/configure-rejected", ["bad-value"])
            return

        # Persist FIRST so the ack delivered to the new target is
        # consistent with what's on disk. If persistence fails we can
        # still bail without having swapped the live target.
        try:
            self._persist({"network": {"ip": ip, "port": port}})
        except BaseException as e:
            log.exception("c2 /cmd/configure/network: persist failed")
            self._send("/error/configure-rejected", [f"persist-failed:{e}"])
            return

        # Live target swap. Ack will go to the new target.
        self._replace_osc_target(ip, port)
        log.info("c2 /cmd/configure/network accepted: %s:%d", ip, port)
        self._send("/state/configured", ["network", f"{ip}:{port}"])

    # --- Configure helpers ----------------------------------------------------

    def _devices_payload(self) -> list:
        """Reconstruct a `metawear.devices` list from the current
        in-memory state. Fields mirror what fs_config schema expects so
        a future read_fugue_states_config + validate_config round-trip
        can pick up the local.json overrides cleanly."""
        return [
            {
                "mac": s.address,
                "name": s.model,
                "ble": s.ble,
                "sensors": s.sensor_config,
            }
            for s in self.states
        ]

    def _persist(self, override: dict) -> None:
        if self.config_path is None:
            raise RuntimeError("config_path not set; cannot persist")
        write_local_overrides(self.config_path, override)

    def _replace_osc_target(self, ip: str, port: int) -> None:
        """Swap the outbound OSC target on every reference we know
        about: osc.client (read by Controller._send), every state's
        _osc_client (read by state event hooks + indicator sends), and
        every pipeline stage carrying an `osc_client` attribute (the
        terminal OscEmit). Duck-typed on `osc_client` so future stages
        with their own sender pick up the swap automatically."""
        from pythonosc.udp_client import SimpleUDPClient
        new_client = SimpleUDPClient(ip, port)
        self.osc.client = new_client
        for s in self.states:
            s._osc_client = new_client
            s.ip = ip
            s.port = port
            for pipe in s.pipelines.values():
                for stage in pipe.stages:
                    if hasattr(stage, "osc_client"):
                        stage.osc_client = new_client
