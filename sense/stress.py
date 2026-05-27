"""
Stress tests for fugue-states reliability.

Each scenario boots the same way as run_fs.py (read config, build OSC + states),
exercises one specific failure mode, and exits 0 (PASS) or 1 (FAIL).

Run: python3 -m sense.stress --scenario <name>

Scenarios target Phase A reliability work. Add new scenarios here as later
phases land — the harness is intentionally a single file.
"""
import argparse
import logging
import os
import signal
import sys
import threading
import time

from sense.c_stderr import reroute_c_stderr_to_log
from sense.fs_setup import read_fugue_states_config, validate_config
from sense.osc import ControlledOSCConnection
from sense.state import MetaWearState

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    datefmt="%H:%M:%S",
)
reroute_c_stderr_to_log()
log = logging.getLogger("fs.stress")

CONFIG_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "..", "fs_config.json"
)


def boot():
    config = validate_config(read_fugue_states_config(CONFIG_PATH))
    assert config["valid"], "invalid configuration"
    network = config["network"]
    devices = config["metawear"]["devices"]
    osc = ControlledOSCConnection(ip=network["ip"], port=network["port"])
    states = [
        MetaWearState(device_config=d, network_config=network, OSC=osc)
        for d in devices
    ]
    return osc, states


def scenario_callback_injection() -> int:
    """
    Replace the first device's acc callback with one that always raises.
    Verify: that device records the failure and the run still completes
    cleanly; the second device (if present) is unaffected.
    """
    osc, states = boot()
    if not states:
        log.error("FAIL: no devices configured")
        return 1

    target = states[0]

    def boom(ctx, data):
        raise RuntimeError("stress: injected acc-callback failure")

    # Replace before start_sensors() — mbl_mw_datasignal_subscribe captures
    # the FnVoid_VoidP_DataP we hand it, so the swap must happen first.
    target.acc_callback = target._make_safe_cb(boom, "acc")
    log.info("injected boom() into %s acc callback", target.address)

    for s in states:
        s.start_sensors(s.sensor_config)
    time.sleep(3.0)
    for s in states:
        s.stop_sensors(s.sensor_config)
    for s in states:
        s.disconnect()
    osc.stop_server()

    if not target.failed:
        log.error("FAIL: %s should have recorded failures", target.address)
        return 1
    if not any(src.startswith("callback:acc") for src in target.failed_sources):
        log.error(
            "FAIL: %s failed_sources=%s — expected at least one callback:acc",
            target.address, target.failed_sources,
        )
        return 1
    log.info(
        "%s recorded %d failures from acc callback; last_error=%r",
        target.address, len(target.failed_sources), target.last_error,
    )

    if len(states) > 1:
        innocent = states[1]
        if innocent.failed:
            log.error(
                "FAIL: %s should NOT have failed (sources=%s)",
                innocent.address, innocent.failed_sources,
            )
            return 1
        # Also: its acc samples should be > 0 (callback ran successfully).
        if innocent.logger.get("acc", 0) == 0:
            log.error("FAIL: %s recorded zero acc samples", innocent.address)
            return 1
        log.info(
            "%s ran clean: acc=%d gyro=%d mag=%d",
            innocent.address,
            innocent.logger["acc"], innocent.logger["gyro"], innocent.logger["mag"],
        )

    log.info("PASS: callback-injection")
    return 0


def scenario_sigint() -> int:
    """
    Start streaming, fire SIGINT to ourselves after 3s, verify the handler
    runs full shutdown in under 3s.
    """
    osc, states = boot()
    sigint_sent_at = [0.0]

    def handler(signum, frame):
        recv_at = time.time()
        log.info(
            "SIGINT received %.2fs after send",
            recv_at - sigint_sent_at[0] if sigint_sent_at[0] else float("nan"),
        )
        cleanup_start = time.time()
        for s in states:
            try:
                s.shutdown()
            except BaseException:
                log.exception("shutdown error on %s", s.address)
        try:
            osc.stop_server()
        except BaseException:
            log.exception("OSC stop error")
        cleanup_elapsed = time.time() - cleanup_start

        if cleanup_elapsed < 3.0:
            log.info("PASS: SIGINT (cleanup %.2fs)", cleanup_elapsed)
            os._exit(0)
        else:
            log.error("FAIL: SIGINT cleanup %.2fs exceeds 3s budget", cleanup_elapsed)
            os._exit(1)

    signal.signal(signal.SIGINT, handler)

    def fire_sigint():
        # Wait long enough for both BLE handshakes to finish and streaming
        # to actually be running, so the SIGINT path is exercised mid-stream
        # rather than mid-handshake.
        time.sleep(10.0)
        sigint_sent_at[0] = time.time()
        os.kill(os.getpid(), signal.SIGINT)

    threading.Thread(target=fire_sigint, daemon=True).start()

    for s in states:
        s.start_sensors(s.sensor_config)

    # The SIGINT will land here and the handler will os._exit.
    time.sleep(15)
    log.error("FAIL: never received SIGINT after 15s")
    return 1


def scenario_repeat_shutdown() -> int:
    """
    Start, stream briefly, then call shutdown() three times per state.
    Verify the second and third calls are no-ops and the state ends up
    in the expected clean condition.
    """
    osc, states = boot()

    for s in states:
        s.start_sensors(s.sensor_config)
    time.sleep(2.0)

    for s in states:
        log.info("calling shutdown 3x on %s", s.address)
        s.shutdown()
        s.shutdown()
        s.shutdown()
    osc.stop_server()

    for s in states:
        if s._streaming_sensors:
            log.error(
                "FAIL: %s still has streaming sensors: %s",
                s.address, s._streaming_sensors,
            )
            return 1
        if s.connected:
            log.error("FAIL: %s still reports connected=True", s.address)
            return 1
        if not s._shutdown_done:
            log.error("FAIL: %s _shutdown_done=False after 3 calls", s.address)
            return 1

    log.info("PASS: repeat-shutdown")
    return 0


def scenario_unreachable_device() -> int:
    """
    Build a state for a deliberately-unreachable BLE MAC and call connect()
    with a short timeout. Verify it raises TimeoutError within budget,
    records the failure, and that a state for a real device still works
    afterwards on the same OSC connection.
    """
    config = validate_config(read_fugue_states_config(CONFIG_PATH))
    assert config["valid"], "invalid configuration"
    network = config["network"]
    devices = config["metawear"]["devices"]
    if not devices:
        log.error("FAIL: no real devices configured")
        return 1

    # Borrow settings from a real device but swap in an unused MAC.
    fake = dict(devices[0])
    fake["mac"] = "DE:AD:BE:EF:00:01"

    osc = ControlledOSCConnection(ip=network["ip"], port=network["port"])
    fake_state = MetaWearState(fake, network, osc)
    real_device = devices[1] if len(devices) > 1 else devices[0]
    real_state = MetaWearState(real_device, network, osc)

    test_timeout = 5.0
    slack = 2.0  # allow worker startup + thread scheduling overhead

    t0 = time.time()
    try:
        fake_state.connect(timeout=test_timeout)
    except TimeoutError as e:
        elapsed = time.time() - t0
        log.info("fake device timed out in %.2fs (limit %.1fs): %s",
                 elapsed, test_timeout, e)
        if not (test_timeout <= elapsed <= test_timeout + slack):
            log.error("FAIL: timeout took %.2fs (expected %.1f-%.1fs)",
                      elapsed, test_timeout, test_timeout + slack)
            return 1
    except BaseException as e:
        log.error("FAIL: unexpected exception type %r: %s", type(e).__name__, e)
        return 1
    else:
        log.error("FAIL: fake device unexpectedly connected")
        return 1

    if not fake_state.failed:
        log.error("FAIL: fake device should be marked failed")
        return 1
    if "connect:timeout" not in fake_state.failed_sources:
        log.error("FAIL: failed_sources=%s — expected connect:timeout",
                  fake_state.failed_sources)
        return 1
    if fake_state.connected:
        log.error("FAIL: fake device should not report connected=True")
        return 1

    log.info("now connecting the real device on the same OSC...")
    try:
        real_state.start_sensors(real_state.sensor_config)
    except BaseException:
        log.exception("FAIL: real device start_sensors raised")
        return 1
    time.sleep(2.0)
    real_state.stop_sensors(real_state.sensor_config)
    real_state.disconnect()
    osc.stop_server()

    if real_state.failed:
        log.error("FAIL: real device should not have failed (sources=%s)",
                  real_state.failed_sources)
        return 1
    if real_state.logger.get("acc", 0) == 0:
        log.error("FAIL: real device recorded zero acc samples")
        return 1
    log.info("real device ran clean: acc=%d gyro=%d mag=%d",
             real_state.logger["acc"], real_state.logger["gyro"], real_state.logger["mag"])

    log.info("PASS: unreachable-device")
    return 0


def scenario_stale_stream() -> int:
    """
    Boot one device, start streaming, then simulate a stale stream by
    backdating _last_frame_at past the threshold (we can't reliably make
    the C library actually drop frames mid-test, so we forge the
    detection-side evidence instead). Verify is_stale() fires, run
    try_recover(), and confirm the device ends up streaming again with
    fresh frames flowing into the logger.
    """
    osc, states = boot()
    if not states:
        log.error("FAIL: no devices configured")
        return 1
    s = states[0]

    s.start_sensors(s.sensor_config)
    time.sleep(2.0)

    if s.logger.get("acc", 0) == 0:
        log.error("FAIL: no initial acc frames after 2s")
        return 1
    pre_acc = s.logger["acc"]
    log.info("[%s] pre-recovery: acc=%d", s.address, pre_acc)

    # Forge staleness: backdate the last-frame timestamp past the threshold.
    s._last_frame_at = time.monotonic() - (s.stale_threshold + 1.0)

    if not s.is_stale():
        log.error("FAIL: is_stale() False after backdating _last_frame_at")
        return 1
    log.info("[%s] is_stale detected", s.address)

    log.info("[%s] calling try_recover()", s.address)
    if not s.try_recover():
        log.error("FAIL: try_recover() returned False")
        return 1

    if not s.connected:
        log.error("FAIL: not connected after recovery")
        return 1
    if not s.streaming:
        log.error("FAIL: not streaming after recovery")
        return 1

    # Give the new subscription a moment to deliver frames.
    time.sleep(2.0)
    post_acc = s.logger["acc"]
    if post_acc <= pre_acc:
        log.error("FAIL: no new acc frames after recovery (pre=%d post=%d)",
                  pre_acc, post_acc)
        return 1
    log.info("[%s] post-recovery: acc=%d (+%d)", s.address, post_acc, post_acc - pre_acc)

    # Clean shutdown
    s.stop_sensors(s.sensor_config)
    s.disconnect()
    # Stop the second device too if any (it was never started but boot()
    # constructed it so atexit-style cleanliness is irrelevant here).
    osc.stop_server()

    log.info("PASS: stale-stream")
    return 0


def scenario_button_toggle() -> int:
    """
    Drive _handle_button_state directly with synthetic press events
    (we can't trigger real BLE button events from a script). Verify:
      - single press alone doesn't toggle
      - two presses within the double-press window toggle ON
      - two more presses within the window toggle OFF
      - two presses with a gap larger than the window do NOT toggle
    """
    osc, states = boot()
    if not states:
        log.error("FAIL: no devices configured")
        return 1
    s = states[0]

    s.connect()

    # 1. Single press should leave state unchanged.
    log.info("test 1: single press should not toggle")
    if s._intended_sensors:
        log.error("FAIL: precondition — _intended_sensors should be empty")
        return 1
    s._handle_button_state(1)
    if s._intended_sensors:
        log.error("FAIL: single press toggled (intended=%s)", s._intended_sensors)
        return 1
    log.info("OK: single press registered, no toggle")

    # Wait long enough that the single press is forgotten.
    time.sleep(s.button_window_s + 0.2)

    # 2. Double press within window should start streaming.
    log.info("test 2: double press should toggle ON")
    s._handle_button_state(1)
    time.sleep(0.05)
    s._handle_button_state(1)
    if not s._intended_sensors:
        log.error("FAIL: double press didn't start streaming")
        return 1
    log.info("OK: started streaming (intended=%s)", sorted(s._intended_sensors))

    # Let frames accumulate so we can verify toggle-off cleared them.
    time.sleep(2.0)
    pre_acc = s.logger.get("acc", 0)
    if pre_acc == 0:
        log.error("FAIL: no acc frames after start (toggle did not actually start streams)")
        return 1
    log.info("post-start acc=%d", pre_acc)

    # 3. Double press within window should stop streaming.
    log.info("test 3: double press should toggle OFF")
    s._handle_button_state(1)
    time.sleep(0.05)
    s._handle_button_state(1)
    if s._intended_sensors:
        log.error("FAIL: double press didn't stop (intended=%s)", s._intended_sensors)
        return 1
    if s._streaming_sensors:
        log.error("FAIL: streaming_sensors not cleared (=%s)", s._streaming_sensors)
        return 1
    log.info("OK: stopped streaming")

    # 4. Two presses outside the window should NOT toggle.
    log.info("test 4: two presses outside window should not toggle")
    s._handle_button_state(1)
    time.sleep(s.button_window_s + 0.3)
    s._handle_button_state(1)
    if s._intended_sensors:
        log.error("FAIL: out-of-window presses toggled (intended=%s)", s._intended_sensors)
        return 1
    log.info("OK: out-of-window presses ignored")

    s.disconnect()
    osc.stop_server()
    log.info("PASS: button-toggle")
    return 0


# --- C2 (remote control) scenarios -----------------------------------------
#
# Pure dispatcher + controller integration tests — no BLE required. The
# rig replaces the OSC outbound client with a loopback listener so the
# controller's /state/* and /error/* replies can be captured and asserted.
# The Pi's main OSC listener (port 8001) still runs; commands are sent
# to 127.0.0.1:8001 which routes through the real Dispatcher → Controller.
# State-event hooks on MetaWearState (/state/<mac>/connected etc.) still
# point at the original osc.client (captured at set_OSC time), so they
# don't pollute these captures — only Controller-emitted traffic lands.


def _bind_loopback_listener():
    """Bind a one-off OSC listener on 127.0.0.1:<ephemeral> capturing
    every inbound message. Returns (server, port, captured)."""
    from pythonosc import dispatcher as _dispatcher, osc_server as _osc_server

    captured: list = []
    def grab(addr, *args):
        captured.append((addr, list(args)))

    d = _dispatcher.Dispatcher()
    d.set_default_handler(grab)
    server = _osc_server.ThreadingOSCUDPServer(("127.0.0.1", 0), d)
    port = server.server_address[1]
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server, port, captured


def _build_c2_test_rig(
    config_path=None,
    position_trackers=None,
    position_track_enabled=False,
):
    """Boot the standard fixture, redirect the controller's outbound OSC
    at a loopback listener, install /cmd/* handlers. Returns
    (osc, states, controller, listener, captured).

    config_path / position_trackers / position_track_enabled forward
    through to the Controller — Pass-2 scenarios use them to exercise
    /cmd/configure/* and /cmd/calibrate."""
    from pythonosc import udp_client
    from sense.c2 import Controller

    osc, states = boot()
    listener, port, captured = _bind_loopback_listener()
    # Only Controller's _send reads osc.client dynamically, so this swap
    # affects controller-emitted /state/* and /error/* but leaves each
    # state's _osc_client (captured at set_OSC time) pointing where the
    # config originally directed it.
    osc.client = udp_client.SimpleUDPClient("127.0.0.1", port)

    controller = Controller(
        osc=osc, states=states,
        config_path=config_path,
        recorder_path_provider=lambda: None,
        position_track_enabled=position_track_enabled,
        position_trackers=position_trackers,
    )
    controller.install()
    return osc, states, controller, listener, captured


def _send_cmd_local(addr, args=None):
    """Send an OSC command to the local Dispatcher at 127.0.0.1:8001."""
    from pythonosc import udp_client
    sender = udp_client.SimpleUDPClient("127.0.0.1", 8001)
    sender.send_message(addr, args if args is not None else [])


def _wait_for(predicate, timeout_s=2.0, poll_s=0.05) -> bool:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(poll_s)
    return False


def _teardown_rig(osc, listener) -> None:
    try:
        listener.shutdown()
    except BaseException:
        pass
    try:
        listener.server_close()
    except BaseException:
        pass
    try:
        osc.stop_server()
    except BaseException:
        pass
    # ControlledOSCConnection.stop_server flips is_running and wakes the
    # listener thread but leaves the ThreadingOSCUDPServer socket bound.
    # For scenarios that build two rigs in one process (e.g. calibrate-flow's
    # not-enabled vs. enabled paths), we need 8001 fully released before the
    # second boot(). Small sleep gives the listener thread a moment to exit
    # handle_request() before the socket is closed under it.
    time.sleep(0.1)
    try:
        osc.server.server_close()
    except BaseException:
        pass


def scenario_c2_status_roundtrip() -> int:
    """
    Send /cmd/status; verify one /state/<mac>/snapshot per device + one
    /state/global/snapshot land at the captured outbound target. Locks
    down both the dispatcher integration and the snapshot address shape.
    """
    osc, states, controller, listener, captured = _build_c2_test_rig()
    try:
        _send_cmd_local("/cmd/status", [])
        ok = _wait_for(
            lambda: sum(1 for a, _ in captured if a.endswith("/snapshot")) >= len(states) + 1,
            timeout_s=2.0,
        )
        snaps = [(a, args) for a, args in captured if a.endswith("/snapshot")]
        if not ok:
            log.error("FAIL: snapshot replies missing — got %d, want %d",
                      len(snaps), len(states) + 1)
            return 1
        addrs = sorted(a for a, _ in snaps)
        expected = sorted(
            [f"/state/{s.address}/snapshot" for s in states]
            + ["/state/global/snapshot"]
        )
        if addrs != expected:
            log.error("FAIL: snapshot addresses %s != expected %s", addrs, expected)
            return 1
        # Per-device snapshot is 4 ints; global is 3 ints + 1 float.
        global_snap = next(args for a, args in snaps if a == "/state/global/snapshot")
        if len(global_snap) != 4:
            log.error("FAIL: global snapshot arity %d != 4", len(global_snap))
            return 1
        for a, args in snaps:
            if a.endswith("/global/snapshot"):
                continue
            if len(args) != 4:
                log.error("FAIL: %s arity %d != 4 (args=%s)", a, len(args), args)
                return 1
        log.info("PASS: c2-status-roundtrip — %d snapshots received", len(snaps))
        return 0
    finally:
        _teardown_rig(osc, listener)


def scenario_c2_shutdown_token() -> int:
    """
    Three sub-checks: missing token rejected with no-token-yet, wrong
    token rejected with mismatch, correct token accepted and
    controller.should_stop flips True. should_stop must remain False
    after the two rejection cases.
    """
    osc, states, controller, listener, captured = _build_c2_test_rig()
    try:
        # 1. Missing token (no args).
        log.info("test 1: missing token")
        _send_cmd_local("/cmd/shutdown", [])
        _wait_for(
            lambda: any(a == "/error/bad-token" for a, _ in captured),
            timeout_s=1.0,
        )
        if controller.should_stop:
            log.error("FAIL: missing-token shutdown was accepted")
            return 1
        bad = [args for a, args in captured if a == "/error/bad-token"]
        if not bad or bad[-1][0] != "no-token-yet":
            log.error("FAIL: missing-token didn't get no-token-yet: %r", bad)
            return 1
        log.info("OK: missing-token rejected with no-token-yet")

        # 2. Wrong token.
        log.info("test 2: wrong token")
        captured.clear()
        _send_cmd_local("/cmd/shutdown", ["wrongtoken"])
        _wait_for(
            lambda: any(a == "/error/bad-token" for a, _ in captured),
            timeout_s=1.0,
        )
        if controller.should_stop:
            log.error("FAIL: wrong-token shutdown was accepted")
            return 1
        bad = [args for a, args in captured if a == "/error/bad-token"]
        if not bad or bad[-1][0] != "mismatch":
            log.error("FAIL: wrong-token didn't get mismatch: %r", bad)
            return 1
        log.info("OK: wrong-token rejected with mismatch")

        # 3. Correct token.
        log.info("test 3: correct token (=%s)", controller.shutdown_token)
        _send_cmd_local("/cmd/shutdown", [controller.shutdown_token])
        ok = _wait_for(lambda: controller.should_stop, timeout_s=1.0)
        if not ok:
            log.error("FAIL: correct-token didn't set should_stop")
            return 1
        log.info("OK: correct-token accepted, should_stop=True")

        log.info("PASS: c2-shutdown-token")
        return 0
    finally:
        _teardown_rig(osc, listener)


def scenario_c2_broadcast_vs_targeted() -> int:
    """
    /cmd/start with empty MAC fans out to all states; with a specific
    MAC affects only that state; with an unknown MAC produces
    /error/not-connected and zero start invocations. Each state's
    start_sensors is stubbed to count invocations without touching BLE.

    Adaptive to fs_config: with ≥2 devices the targeted sub-check
    actually distinguishes a "MAC arg ignored" regression; with 1
    device that distinction is vacuously satisfied (no non-targets
    exist) but broadcast and unknown-MAC paths still get covered.
    """
    osc, states, controller, listener, captured = _build_c2_test_rig()
    try:
        if not states:
            log.error("FAIL: no devices configured")
            return 1

        invocations = {s.address: 0 for s in states}
        def make_stub(addr):
            def stub(_cfg):
                invocations[addr] += 1
            return stub
        for s in states:
            # Skip the not-connected guard in Controller._on_start.
            s.connected = True
            s.streaming = False
            s.start_sensors = make_stub(s.address)

        # Test 1: broadcast (empty MAC).
        log.info("test 1: broadcast (empty MAC)")
        _send_cmd_local("/cmd/start", [""])
        _wait_for(
            lambda: sum(invocations.values()) >= len(states),
            timeout_s=1.0,
        )
        if any(v != 1 for v in invocations.values()):
            log.error("FAIL: broadcast didn't hit each state exactly once: %s",
                      invocations)
            return 1
        log.info("OK: broadcast hit %d state(s)", len(states))

        # Test 2: targeted (specific MAC).
        log.info("test 2: targeted")
        for s in states:
            invocations[s.address] = 0
            s.streaming = False
        target = states[0]
        _send_cmd_local("/cmd/start", [target.address])
        _wait_for(
            lambda: invocations[target.address] >= 1,
            timeout_s=1.0,
        )
        if invocations[target.address] != 1:
            log.error("FAIL: targeted didn't hit target: %s", invocations)
            return 1
        for s in states:
            if s.address != target.address and invocations[s.address] != 0:
                log.error("FAIL: targeted hit non-target %s: %s",
                          s.address, invocations)
                return 1
        log.info("OK: targeted hit only %s", target.address)

        # Test 3: unknown MAC → /error/not-connected, no start_sensors fired.
        log.info("test 3: unknown MAC → /error/not-connected")
        captured.clear()
        for s in states:
            invocations[s.address] = 0
        _send_cmd_local("/cmd/start", ["AA:BB:CC:DD:EE:FF"])
        _wait_for(
            lambda: any(a == "/error/not-connected" for a, _ in captured),
            timeout_s=1.0,
        )
        errs = [args for a, args in captured if a == "/error/not-connected"]
        if not errs:
            log.error("FAIL: unknown MAC didn't produce /error/not-connected")
            return 1
        if any(v != 0 for v in invocations.values()):
            log.error("FAIL: unknown MAC triggered start_sensors: %s", invocations)
            return 1
        log.info("OK: unknown MAC → /error/not-connected %s", errs[-1])

        log.info("PASS: c2-broadcast-vs-targeted")
        return 0
    finally:
        _teardown_rig(osc, listener)


def scenario_c2_configure_sensor_flow() -> int:
    """
    /cmd/configure/sensor: applied + persisted, rejected while streaming,
    rejected on bad-key, rejected on unknown-sensor. Persistence checked
    against a temp config_path so we don't touch the real fs_config.local.json.
    """
    import json as _json
    import shutil
    import tempfile
    from sense.fs_setup import _local_override_path

    tmp_dir = tempfile.mkdtemp(prefix="fs-c2-stress-")
    tmp_config = os.path.join(tmp_dir, "fs_config.json")  # never created
    osc, states, controller, listener, captured = _build_c2_test_rig(
        config_path=tmp_config,
    )
    try:
        if not states:
            log.error("FAIL: no devices configured")
            return 1
        s = states[0]

        # Test 1: applied + persisted.
        log.info("test 1: valid configure (Accelerometer odr 50)")
        # Ensure not streaming.
        s.streaming = False
        # Seed sensor_config so the change is visible.
        s.sensor_config.setdefault("Accelerometer", {})["odr"] = 25
        _send_cmd_local("/cmd/configure/sensor",
                        [s.address, "Accelerometer", "odr", 50])
        ok = _wait_for(
            lambda: any(a == "/state/configured" for a, _ in captured),
            timeout_s=1.0,
        )
        if not ok:
            log.error("FAIL: no /state/configured ack received")
            return 1
        if s.sensor_config["Accelerometer"]["odr"] != 50:
            log.error("FAIL: in-memory sensor_config not updated: %s",
                      s.sensor_config.get("Accelerometer"))
            return 1
        local_path = _local_override_path(tmp_config)
        if not os.path.exists(local_path):
            log.error("FAIL: local override file not written at %s", local_path)
            return 1
        with open(local_path, "r") as f:
            persisted = _json.load(f)
        try:
            persisted_odr = persisted["metawear"]["devices"][0]["sensors"]["Accelerometer"]["odr"]
        except (KeyError, IndexError, TypeError) as e:
            log.error("FAIL: persisted file shape unexpected (%s): %s", e, persisted)
            return 1
        if persisted_odr != 50:
            log.error("FAIL: persisted odr=%r, expected 50", persisted_odr)
            return 1
        log.info("OK: applied + persisted")

        # Test 2: rejected while streaming.
        log.info("test 2: rejected while streaming")
        captured.clear()
        s.streaming = True
        _send_cmd_local("/cmd/configure/sensor",
                        [s.address, "Accelerometer", "odr", 100])
        _wait_for(
            lambda: any(a == "/error/configure-rejected" for a, _ in captured),
            timeout_s=1.0,
        )
        errs = [args for a, args in captured if a == "/error/configure-rejected"]
        if not errs or errs[-1][0] != "streaming":
            log.error("FAIL: streaming rejection didn't emit /error/configure-rejected streaming: %r",
                      errs)
            return 1
        if s.sensor_config["Accelerometer"]["odr"] != 50:
            log.error("FAIL: streaming rejection didn't preserve prior value")
            return 1
        s.streaming = False
        log.info("OK: streaming → rejected")

        # Test 3: bad-key.
        log.info("test 3: bad-key")
        captured.clear()
        _send_cmd_local("/cmd/configure/sensor",
                        [s.address, "Accelerometer", "brightness", 7])
        _wait_for(
            lambda: any(a == "/error/configure-rejected" for a, _ in captured),
            timeout_s=1.0,
        )
        errs = [args for a, args in captured if a == "/error/configure-rejected"]
        if not errs or errs[-1][0] != "bad-key":
            log.error("FAIL: bad-key didn't get /error/configure-rejected bad-key: %r",
                      errs)
            return 1
        log.info("OK: bad-key rejected")

        # Test 4: unknown-sensor.
        log.info("test 4: unknown-sensor")
        captured.clear()
        _send_cmd_local("/cmd/configure/sensor",
                        [s.address, "Compass", "odr", 25])
        _wait_for(
            lambda: any(a == "/error/configure-rejected" for a, _ in captured),
            timeout_s=1.0,
        )
        errs = [args for a, args in captured if a == "/error/configure-rejected"]
        if not errs or errs[-1][0] != "unknown-sensor":
            log.error("FAIL: unknown-sensor didn't get /error/configure-rejected unknown-sensor: %r",
                      errs)
            return 1
        log.info("OK: unknown-sensor rejected")

        log.info("PASS: c2-configure-sensor-flow")
        return 0
    finally:
        _teardown_rig(osc, listener)
        try:
            shutil.rmtree(tmp_dir)
        except BaseException:
            pass


def scenario_c2_configure_network_live() -> int:
    """
    /cmd/configure/network: persists the new target, swaps every
    osc_client reference live, and the ack lands at the NEW target.
    A subsequent /cmd/status snapshot also lands at the new target —
    proves the swap reached every reference, not just controller's
    own send path.
    """
    import json as _json
    import shutil
    import tempfile
    from pythonosc import udp_client
    from sense.fs_setup import _local_override_path

    tmp_dir = tempfile.mkdtemp(prefix="fs-c2-stress-")
    tmp_config = os.path.join(tmp_dir, "fs_config.json")
    osc, states, controller, listener_A, captured_A = _build_c2_test_rig(
        config_path=tmp_config,
    )
    listener_B = None
    try:
        if not states:
            log.error("FAIL: no devices configured")
            return 1

        # Stop streaming on every state — network change shouldn't be
        # gated by streaming, but we want a deterministic snapshot count.
        for s in states:
            s.streaming = False

        # Bind a second listener for the post-swap target.
        listener_B, port_B, captured_B = _bind_loopback_listener()

        log.info("test 1: live target swap")
        _send_cmd_local("/cmd/configure/network", ["127.0.0.1", port_B])
        ok = _wait_for(
            lambda: any(a == "/state/configured" for a, _ in captured_B),
            timeout_s=1.0,
        )
        if not ok:
            log.error("FAIL: /state/configured ack didn't arrive at new target B (port %d)",
                      port_B)
            log.error("       captured_A=%r captured_B=%r", captured_A, captured_B)
            return 1
        # The ack must arrive at B — A should NOT see it.
        if any(a == "/state/configured" for a, _ in captured_A):
            log.error("FAIL: /state/configured arrived at OLD target A — swap was incomplete")
            return 1
        log.info("OK: /state/configured ack at new target")

        log.info("test 2: persisted")
        local_path = _local_override_path(tmp_config)
        if not os.path.exists(local_path):
            log.error("FAIL: local override file not written")
            return 1
        with open(local_path, "r") as f:
            persisted = _json.load(f)
        if persisted.get("network", {}).get("port") != port_B:
            log.error("FAIL: persisted network.port=%r, expected %d",
                      persisted.get("network", {}).get("port"), port_B)
            return 1
        if persisted.get("network", {}).get("ip") != "127.0.0.1":
            log.error("FAIL: persisted network.ip=%r, expected 127.0.0.1",
                      persisted.get("network", {}).get("ip"))
            return 1
        log.info("OK: persisted")

        log.info("test 3: subsequent snapshot also lands at new target")
        captured_B.clear()
        captured_A.clear()
        _send_cmd_local("/cmd/status", [])
        ok = _wait_for(
            lambda: sum(1 for a, _ in captured_B if a.endswith("/snapshot")) >= len(states) + 1,
            timeout_s=1.0,
        )
        if not ok:
            log.error("FAIL: /cmd/status snapshots didn't arrive at new target")
            return 1
        if any(a.endswith("/snapshot") for a, _ in captured_A):
            log.error("FAIL: snapshots also arrived at OLD target A")
            return 1
        log.info("OK: /cmd/status snapshots routed to new target")

        log.info("test 4: bad-port rejected")
        captured_A.clear()
        captured_B.clear()
        _send_cmd_local("/cmd/configure/network", ["127.0.0.1", "abc"])
        _wait_for(
            lambda: any(a == "/error/configure-rejected" for a, _ in captured_B),
            timeout_s=1.0,
        )
        errs = [args for a, args in captured_B if a == "/error/configure-rejected"]
        if not errs or errs[-1][0] != "bad-port":
            log.error("FAIL: bad-port didn't get rejected: %r", errs)
            return 1
        log.info("OK: bad-port rejected")

        log.info("PASS: c2-configure-network-live")
        return 0
    finally:
        if listener_B is not None:
            try: listener_B.shutdown()
            except BaseException: pass
            try: listener_B.server_close()
            except BaseException: pass
        _teardown_rig(osc, listener_A)
        try:
            shutil.rmtree(tmp_dir)
        except BaseException:
            pass


def scenario_c2_calibrate_flow() -> int:
    """
    /cmd/calibrate: not-enabled rejection (no --position-track), targeted
    recalibration calls request_recalibration on the matching tracker,
    unknown-MAC rejected, broadcast hits every tracker. Uses a fake
    tracker that records calls — the real PositionTracker is exercised
    elsewhere by tools/analyze_position.py.

    Builds two rigs (not-enabled, enabled-with-fakes) sequentially —
    _teardown_rig fully releases 8001 between them so the second boot
    can re-bind.
    """
    class FakeTracker:
        def __init__(self):
            self.calls = []
        def request_recalibration(self, device):
            self.calls.append(device)
            return True

    # Sub-test 1: not-enabled rejection.
    log.info("test 1: not-enabled rejection")
    osc1, states1, controller1, listener1, captured1 = _build_c2_test_rig(
        position_track_enabled=False,
    )
    try:
        if not states1:
            log.error("FAIL: no devices configured")
            return 1
        macs = [s.address for s in states1]
        _send_cmd_local("/cmd/calibrate", [])
        _wait_for(
            lambda: any(a == "/error/calibrate-failed" for a, _ in captured1),
            timeout_s=1.0,
        )
        errs = [args for a, args in captured1 if a == "/error/calibrate-failed"]
        if not errs or errs[-1][1] != "not-enabled":
            log.error("FAIL: not-enabled rejection missing or wrong: %r", errs)
            return 1
        log.info("OK: not-enabled rejected")
    finally:
        _teardown_rig(osc1, listener1)

    # Sub-tests 2-4 share a position-enabled rig with fake trackers.
    fakes = {mac: FakeTracker() for mac in macs}
    osc2, states2, controller2, listener2, captured2 = _build_c2_test_rig(
        position_trackers=fakes,
        position_track_enabled=True,
    )
    try:
        # Sub-test 2: targeted recalibration.
        log.info("test 2: targeted recalibration")
        target_mac = states2[0].address
        _send_cmd_local("/cmd/calibrate", [target_mac])
        _wait_for(
            lambda: target_mac in fakes[target_mac].calls,
            timeout_s=1.0,
        )
        if target_mac not in fakes[target_mac].calls:
            log.error("FAIL: targeted didn't call request_recalibration on %s", target_mac)
            return 1
        # Other devices' fake trackers untouched.
        for mac, fake in fakes.items():
            if mac == target_mac:
                continue
            if fake.calls:
                log.error("FAIL: targeted leaked to non-target %s: %r", mac, fake.calls)
                return 1
        log.info("OK: targeted hit only %s", target_mac)

        # Sub-test 3: unknown MAC.
        log.info("test 3: unknown MAC")
        captured2.clear()
        _send_cmd_local("/cmd/calibrate", ["AA:BB:CC:DD:EE:FF"])
        _wait_for(
            lambda: any(a == "/error/calibrate-failed" for a, _ in captured2),
            timeout_s=1.0,
        )
        errs = [args for a, args in captured2 if a == "/error/calibrate-failed"]
        if not errs or errs[-1][1] != "unknown-device":
            log.error("FAIL: unknown-device rejection missing: %r", errs)
            return 1
        log.info("OK: unknown MAC rejected")

        # Sub-test 4: broadcast.
        log.info("test 4: broadcast")
        for fake in fakes.values():
            fake.calls.clear()
        _send_cmd_local("/cmd/calibrate", [""])
        _wait_for(
            lambda: all(fake.calls for fake in fakes.values()),
            timeout_s=1.0,
        )
        for mac, fake in fakes.items():
            if not fake.calls:
                log.error("FAIL: broadcast didn't hit %s", mac)
                return 1
        log.info("OK: broadcast hit %d tracker(s)", len(fakes))

        log.info("PASS: c2-calibrate-flow")
        return 0
    finally:
        _teardown_rig(osc2, listener2)


def scenario_latch_basics() -> int:
    """
    Latch + LatchUpdate + FusionStage cross-stream primitives. No BLE.

    Validates: a frame pushed through one pipeline with LatchUpdate is
    visible to a FusionStage reading via .latest() / .latest_all()
    when driven from a different pipeline. Cross-device + cross-sensor
    keying both work; later updates overwrite earlier ones.
    """
    from sense.pipeline import (
        FusionStage, IMUFrame, Latch, LatchUpdate, Pipeline,
    )

    captured_fusion: list = []

    class CaptureFusion(FusionStage):
        """Records what each driving frame sees in the latch."""
        def process(self, frame):
            yield frame
            captured_fusion.append({
                "driving": (frame.device, frame.sensor, frame.values),
                "latest_quat": self.latest(frame.device, "quat"),
                "all_acc": {
                    dev: f.values for dev, f in self.latest_all("acc").items()
                },
            })

    latch = Latch()
    pA_acc = Pipeline([LatchUpdate(latch)])
    pA_quat = Pipeline([LatchUpdate(latch)])
    pB_acc = Pipeline([LatchUpdate(latch)])
    pA_fusion = Pipeline([CaptureFusion(latch)])

    # Populate the latch via three independent pipelines.
    pA_quat.push(IMUFrame(device="A", sensor="quat",
                          t_recv=0.10, values=(1.0, 0.0, 0.0, 0.0)))
    pA_acc.push(IMUFrame(device="A", sensor="acc",
                         t_recv=0.20, values=(0.1, 0.2, 9.8)))
    pB_acc.push(IMUFrame(device="B", sensor="acc",
                         t_recv=0.21, values=(0.0, 0.0, 9.81)))

    # Direct latch API.
    if latch.get("A", "quat") is None or latch.get("A", "acc") is None \
            or latch.get("B", "acc") is None:
        log.error("FAIL: latch.get returned None for an updated key")
        return 1
    if latch.get("A", "gyro") is not None:
        log.error("FAIL: latch.get returned non-None for a never-updated key")
        return 1
    if set(latch.get_all("acc").keys()) != {"A", "B"}:
        log.error("FAIL: latch.get_all('acc') keys = %s, expected {A, B}",
                  set(latch.get_all("acc").keys()))
        return 1
    log.info("OK: latch direct API (get / get_all)")

    # FusionStage reading across streams from a third pipeline.
    pA_fusion.push(IMUFrame(device="A", sensor="linear_acc",
                            t_recv=0.30, values=(0.01, 0.02, 0.03)))
    if not captured_fusion:
        log.error("FAIL: fusion stage didn't run")
        return 1
    snap = captured_fusion[-1]
    if snap["latest_quat"] is None:
        log.error("FAIL: FusionStage.latest('quat') = None despite latch update")
        return 1
    if set(snap["all_acc"].keys()) != {"A", "B"}:
        log.error("FAIL: FusionStage.latest_all('acc') keys = %s, expected {A, B}",
                  set(snap["all_acc"].keys()))
        return 1
    if snap["all_acc"]["A"] != (0.1, 0.2, 9.8):
        log.error("FAIL: latest_all returned wrong A/acc: %s", snap["all_acc"]["A"])
        return 1
    log.info("OK: FusionStage read cross-stream values via latch")

    # Newer update wins.
    pA_acc.push(IMUFrame(device="A", sensor="acc",
                         t_recv=0.40, values=(9.0, 0.0, 0.0)))
    if latch.get("A", "acc").values != (9.0, 0.0, 0.0):
        log.error("FAIL: latch didn't overwrite A/acc with newer value")
        return 1
    log.info("OK: latch overwrites with newer frame")

    log.info("PASS: latch-basics")
    return 0


def scenario_gesture_feature_autodiscover() -> int:
    """
    GestureLibrary auto-detects feature_sensors from JSONL recordings
    when none is passed explicitly. Builds two synthetic recordings in
    /tmp with overlapping scalar streams across gesture windows and
    verifies that _discover_feature_sensors returns the intersection
    (plus that from_files picks up the auto-detected set).
    """
    import json as _json
    import shutil
    import tempfile
    from sense.gesture import GestureLibrary, _discover_feature_sensors

    tmp_dir = tempfile.mkdtemp(prefix="fs-gesture-discover-")
    try:
        # Recording 1 — gesture windows with scalars {acc_mag, gyro_mag, tilt}.
        path1 = os.path.join(tmp_dir, "rec1.jsonl")
        with open(path1, "w") as fh:
            # Two windows to exercise per-window aggregation.
            for instance in (0, 1):
                fh.write(_json.dumps({
                    "_gesture": "start", "label": "wave",
                    "device": "A", "instance": instance,
                }) + "\n")
                for sensor in ("acc_mag", "gyro_mag", "tilt"):
                    # 8 frames per stream so the template has enough samples
                    # for the multivariate DTW machinery downstream.
                    for i in range(8):
                        fh.write(_json.dumps({
                            "device": "A", "sensor": sensor,
                            "t_recv": 0.1 + i * 0.04, "values": [1.0 + i * 0.1],
                        }) + "\n")
                # A 3-axis frame in the same window — should NOT show up as
                # a scalar feature.
                fh.write(_json.dumps({
                    "device": "A", "sensor": "acc",
                    "t_recv": 0.5, "values": [0.1, 0.2, 9.8],
                }) + "\n")
                fh.write(_json.dumps({
                    "_gesture": "end", "label": "wave",
                    "device": "A", "instance": instance,
                }) + "\n")

        # Recording 2 — different gesture, scalars {acc_mag, gyro_mag, light}.
        # Intersection across both files should be {acc_mag, gyro_mag}.
        path2 = os.path.join(tmp_dir, "rec2.jsonl")
        with open(path2, "w") as fh:
            fh.write(_json.dumps({
                "_gesture": "start", "label": "chop",
                "device": "A", "instance": 0,
            }) + "\n")
            for sensor in ("acc_mag", "gyro_mag", "light"):
                for i in range(8):
                    fh.write(_json.dumps({
                        "device": "A", "sensor": sensor,
                        "t_recv": 0.1 + i * 0.04, "values": [1.0 + i * 0.1],
                    }) + "\n")
            fh.write(_json.dumps({
                "_gesture": "end", "label": "chop",
                "device": "A", "instance": 0,
            }) + "\n")

        # Test 1: _discover_feature_sensors returns the intersection.
        log.info("test 1: _discover_feature_sensors intersection")
        discovered = _discover_feature_sensors([path1, path2])
        expected = ("acc_mag", "gyro_mag")
        if discovered != expected:
            log.error("FAIL: discovered=%s, expected=%s", discovered, expected)
            return 1
        log.info("OK: discovered=%s", discovered)

        # Test 2: single-file discovery picks up the full per-file scalar set.
        log.info("test 2: single-file discovery (rec1 only)")
        single = _discover_feature_sensors([path1])
        expected_single = ("acc_mag", "gyro_mag", "tilt")
        if single != expected_single:
            log.error("FAIL: single=%s, expected=%s", single, expected_single)
            return 1
        log.info("OK: single=%s", single)

        # Test 3: from_files uses the discovered set when feature_sensors=None.
        log.info("test 3: from_files auto-uses discovered set")
        lib = GestureLibrary.from_files([path1, path2])
        if lib.feature_sensors != expected:
            log.error("FAIL: lib.feature_sensors=%s, expected=%s",
                      lib.feature_sensors, expected)
            return 1
        if not lib.templates:
            log.error("FAIL: no templates loaded — auto-detect picked the "
                      "right features but extraction failed")
            return 1
        log.info("OK: lib auto-loaded with feature_sensors=%s, %d templates",
                 lib.feature_sensors, len(lib.templates))

        # Test 4: explicit feature_sensors overrides auto-detect.
        log.info("test 4: explicit override beats auto-detect")
        lib_explicit = GestureLibrary.from_files(
            [path1, path2], feature_sensors=("acc_mag",),
        )
        if lib_explicit.feature_sensors != ("acc_mag",):
            log.error("FAIL: explicit override ignored: %s",
                      lib_explicit.feature_sensors)
            return 1
        log.info("OK: explicit override respected")

        # Test 5: empty recordings → empty feature set, no crash.
        log.info("test 5: no recordings → empty discovery")
        empty = _discover_feature_sensors([])
        if empty != ():
            log.error("FAIL: empty discovery returned %s", empty)
            return 1
        log.info("OK: empty discovery returns ()")

        log.info("PASS: gesture-feature-autodiscover")
        return 0
    finally:
        try:
            shutil.rmtree(tmp_dir)
        except BaseException:
            pass


def scenario_gesture_confidence_emission() -> int:
    """
    GestureRecognizer emits `IMUFrame(values=(1 - best_ratio,))` on
    match — the confidence channel PD/DAW uses to fade by match quality.
    Builds a single-template library, pumps a slightly-perturbed copy
    of the template through the recognizer, and verifies the emitted
    value equals `1 - distance/threshold` (computed independently via
    dtw_ndim) within float tolerance.

    Catches both regressions: "still emits hardcoded 1.0" and "encoding
    changed without spec update."
    """
    import numpy as np
    from dtaidistance import dtw_ndim
    from sense.gesture import GestureLibrary, GestureRecognizer, Template
    from sense.pipeline import IMUFrame, Pipeline, Stage

    captured = []

    class Capture(Stage):
        def process(self, frame):
            captured.append(frame)
            yield frame

    # 30-sample template + buffer with constant offset on the primary
    # feature. Offset → non-zero DTW distance → ratio strictly between
    # 0 and 1, so confidence is neither hardcoded 1.0 nor a degenerate
    # 0/1 boundary value.
    samples = 30
    t = np.linspace(0, 4 * np.pi, samples)
    template_series = np.column_stack(
        [np.sin(t) + 2.0, np.cos(t) + 2.0]
    ).astype(np.double)
    buffer_series = template_series.copy()
    buffer_series[:, 0] += 0.5  # bias acc_mag axis by +0.5

    lib = GestureLibrary(feature_sensors=("acc_mag", "gyro_mag"))
    lib.templates.append(Template(
        label="test", device="A", instance=0, feature_series=template_series,
    ))
    # Pin a threshold large enough that the perturbed buffer still matches
    # (ratio < 1) but small enough that ratio is meaningfully > 0.
    lib.thresholds["test"] = 10.0

    rec = GestureRecognizer(
        lib,
        window_samples=samples,
        tick_frames=1,
        cooldown_s=0.0,
        min_std=0.01,  # very permissive variance gate
    )
    pipe = Pipeline([rec, Capture()])

    # Push paired (gyro_mag, acc_mag) frames so the primary acc_mag tick
    # always sees a full secondary buffer.
    for i in range(samples):
        for sensor, value in (
            ("gyro_mag", buffer_series[i, 1]),
            ("acc_mag", buffer_series[i, 0]),
        ):
            pipe.push(IMUFrame(
                device="A", sensor=sensor, t_recv=i * 0.04,
                values=(float(value),),
            ))

    gesture_frames = [f for f in captured if f.sensor == "gesture/test"]
    if not gesture_frames:
        log.error("FAIL: no gesture/test frame emitted (matched should have fired)")
        return 1

    emitted_conf = float(gesture_frames[0].values[0])

    # Compute the expected confidence independently via the same DTW
    # path the recognizer uses (it computes distance via dtw_ndim with
    # band=window_samples//5, psi=window_samples//5 by default — same
    # defaults the recognizer applies to its own DTW call).
    band = rec.band
    psi = rec.psi
    expected_distance = dtw_ndim.distance_fast(
        buffer_series, template_series, window=band, psi=psi,
    )
    expected_ratio = expected_distance / lib.thresholds["test"]
    expected_conf = 1.0 - expected_ratio

    log.info("emitted confidence=%.6f, expected=%.6f (distance=%.4f, threshold=%.4f, ratio=%.4f)",
             emitted_conf, expected_conf, expected_distance,
             lib.thresholds["test"], expected_ratio)

    if not (0.0 < emitted_conf < 1.0):
        log.error("FAIL: confidence %.6f outside (0, 1) — perturbed buffer "
                  "should yield strictly-interior value, neither 0.0 nor 1.0 "
                  "(0.0 would be borderline-match, 1.0 a hardcoded regression)",
                  emitted_conf)
        return 1
    if abs(emitted_conf - expected_conf) > 1e-6:
        log.error("FAIL: emitted confidence %.6f != expected %.6f (delta %.2e)",
                  emitted_conf, expected_conf, abs(emitted_conf - expected_conf))
        return 1
    log.info("OK: confidence matches 1 - distance/threshold exactly")

    log.info("PASS: gesture-confidence-emission")
    return 0


def scenario_pipeline_basics() -> int:
    """
    Build a Pipeline directly (no BLE), push synthetic frames through,
    verify each stage runs, derived frames appear with the expected
    sensor names, and stats are recorded.
    """
    from sense.pipeline import (
        IMUFrame, LowPass, Magnitude, Pipeline, Stage, Tilt,
    )

    captured = []

    class CaptureStage(Stage):
        def process(self, frame):
            captured.append(frame)
            return ()

    pipe = Pipeline([
        LowPass(cutoff_hz=5.0, fs=25.0),
        Magnitude(),
        Tilt(),
        CaptureStage(),
    ])

    # Push 10 synthetic acc frames, mostly z-up with a slight tilt.
    for i in range(10):
        pipe.push(IMUFrame(
            device="AA:BB:CC:DD:EE:FF",
            sensor="acc",
            t_recv=i * 0.04,  # 40ms = 25Hz
            values=(0.05 * i, 0.1, 9.81),
        ))

    # Stats: every stage should have been called at least once on the
    # original 10 frames (Magnitude doubles them, Tilt also doubles).
    expected_stages = {"LowPass", "Magnitude", "Tilt", "CaptureStage"}
    if not expected_stages.issubset(pipe.stats.keys()):
        log.error("FAIL: missing stage stats — got %s, expected %s",
                  set(pipe.stats), expected_stages)
        return 1
    if pipe.stats["LowPass"].count != 10:
        log.error("FAIL: LowPass count = %d, expected 10",
                  pipe.stats["LowPass"].count)
        return 1
    log.info("stage timings (mean μs): %s",
             {n: round(s.mean_s * 1e6, 1) for n, s in pipe.stats.items()})

    # Sensors emitted: Magnitude adds acc_mag; Tilt adds tilt.
    sensors_seen = {f.sensor for f in captured}
    expected = {"acc", "acc_mag", "tilt"}
    if not expected.issubset(sensors_seen):
        log.error("FAIL: expected sensors %s, captured %s", expected, sensors_seen)
        return 1
    log.info("captured sensors: %s", sorted(sensors_seen))

    # Sanity-check derived values on the last frame.
    acc_frames = [f for f in captured if f.sensor == "acc"]
    mag_frames = [f for f in captured if f.sensor == "acc_mag"]
    tilt_frames = [f for f in captured if f.sensor == "tilt"]
    if len(acc_frames) != len(mag_frames) != len(tilt_frames) != 10:
        log.error("FAIL: per-sensor counts: acc=%d mag=%d tilt=%d (expected 10 each)",
                  len(acc_frames), len(mag_frames), len(tilt_frames))
        return 1

    # Magnitude of (≈0.45, 0.1, 9.81) ≈ 9.82 (low-passed values, so smaller)
    last_mag = mag_frames[-1].values[0]
    if not (5.0 < last_mag < 12.0):
        log.error("FAIL: magnitude out of plausible range: %f", last_mag)
        return 1
    # Tilt should be small (z dominates) — under 10°
    last_tilt = tilt_frames[-1].values[0]
    if not (0.0 <= last_tilt < 15.0):
        log.error("FAIL: tilt out of plausible range: %f°", last_tilt)
        return 1
    log.info("last frame: mag=%.3f tilt=%.2f°", last_mag, last_tilt)

    log.info("PASS: pipeline-basics")
    return 0


SCENARIOS = {
    "callback-injection": scenario_callback_injection,
    "sigint": scenario_sigint,
    "repeat-shutdown": scenario_repeat_shutdown,
    "unreachable-device": scenario_unreachable_device,
    "stale-stream": scenario_stale_stream,
    "button-toggle": scenario_button_toggle,
    "pipeline-basics": scenario_pipeline_basics,
    "latch-basics": scenario_latch_basics,
    "gesture-feature-autodiscover": scenario_gesture_feature_autodiscover,
    "gesture-confidence-emission": scenario_gesture_confidence_emission,
    "c2-status-roundtrip": scenario_c2_status_roundtrip,
    "c2-shutdown-token": scenario_c2_shutdown_token,
    "c2-broadcast-vs-targeted": scenario_c2_broadcast_vs_targeted,
    "c2-configure-sensor-flow": scenario_c2_configure_sensor_flow,
    "c2-configure-network-live": scenario_c2_configure_network_live,
    "c2-calibrate-flow": scenario_c2_calibrate_flow,
}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scenario", required=True, choices=sorted(SCENARIOS))
    args = parser.parse_args()
    return SCENARIOS[args.scenario]()


if __name__ == "__main__":
    sys.exit(main())
