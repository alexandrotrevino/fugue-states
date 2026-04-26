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


SCENARIOS = {
    "callback-injection": scenario_callback_injection,
    "sigint": scenario_sigint,
    "repeat-shutdown": scenario_repeat_shutdown,
}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scenario", required=True, choices=sorted(SCENARIOS))
    args = parser.parse_args()
    return SCENARIOS[args.scenario]()


if __name__ == "__main__":
    sys.exit(main())
