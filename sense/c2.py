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
from typing import Callable, Optional

log = logging.getLogger("fs.c2")

HEARTBEAT_PERIOD_S = 2.0

_TRANSIENT_OSC_ERRNOS = (errno.ENETUNREACH, errno.EHOSTUNREACH, errno.ECONNREFUSED)


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
        recorder_path_provider: Callable[[], Optional[str]] = lambda: None,
        position_track_enabled: bool = False,
    ):
        self.osc = osc
        self.states = states
        self._recorder_path_provider = recorder_path_provider
        self.position_track_enabled = position_track_enabled

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
        # Pass 2 — wiring through to PositionTracker.request_recalibration
        # lands with the configure handlers. For Pass 1 we report a clean
        # rejection so callers know the address is reachable but inert.
        if not self.position_track_enabled:
            self._send("/error/calibrate-failed", ["", "not-enabled"])
            return
        mac = args[0] if args else ""
        log.info("c2 /cmd/calibrate mac=%r (Pass-2 stub)", mac)
        self._send("/error/calibrate-failed", [mac, "not-implemented"])

    def _on_configure_sensor(self, address, *args):
        log.info("c2 /cmd/configure/sensor %s (Pass-2 stub)", args)
        self._send("/error/configure-rejected", ["not-implemented"])

    def _on_configure_network(self, address, *args):
        log.info("c2 /cmd/configure/network %s (Pass-2 stub)", args)
        self._send("/error/configure-rejected", ["not-implemented"])
