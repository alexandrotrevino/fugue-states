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

from sense.fs_setup import read_fugue_states_config, validate_config
from sense.osc import ControlledOSCConnection
from sense.state import MetaWearState

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    datefmt="%H:%M:%S",
)
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


SCENARIOS = {
    "callback-injection": scenario_callback_injection,
    "sigint": scenario_sigint,
    "repeat-shutdown": scenario_repeat_shutdown,
    "unreachable-device": scenario_unreachable_device,
    "stale-stream": scenario_stale_stream,
    "button-toggle": scenario_button_toggle,
}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scenario", required=True, choices=sorted(SCENARIOS))
    args = parser.parse_args()
    return SCENARIOS[args.scenario]()


if __name__ == "__main__":
    sys.exit(main())
