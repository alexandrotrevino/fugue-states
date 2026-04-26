import argparse
import atexit
import logging
import os
import signal
import sys
import time
from time import sleep

from sense.fs_setup import read_fugue_states_config, validate_config
from sense.osc import ControlledOSCConnection
from sense.state import MetaWearState

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    datefmt="%H:%M:%S",
)
# When the controlling SSH session goes away mid-shutdown, stderr writes
# raise BrokenPipeError. Swallow logging errors so they don't abort the
# cleanup work itself.
logging.raiseExceptions = False
log = logging.getLogger("fs.run")

STREAM_DURATION_S = 5.0
WATCHDOG_TICK_S = 1.0

parser = argparse.ArgumentParser(description="Fugue States runtime")
parser.add_argument(
    "--mode",
    choices=("pi-driven", "button-driven"),
    default="pi-driven",
    help=(
        "pi-driven (default): connect, auto-start sensors, run for "
        f"{STREAM_DURATION_S}s, stop, disconnect. "
        "button-driven: connect and wait until SIGINT; the device "
        "button toggles streaming (double-press)."
    ),
)
args = parser.parse_args()

config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "fs_config.json")
config = read_fugue_states_config(config_path)
config = validate_config(config)
assert config["valid"], "Invalid configuration"

network = config["network"]
devices = config["metawear"]["devices"]

log.info("setting up OSC")
osc = ControlledOSCConnection(ip=network["ip"], port=network["port"])

log.info("building device states")
states = [MetaWearState(device_config=d, network_config=network, OSC=osc) for d in devices]


def _shutdown_all() -> None:
    for s in states:
        try:
            s.shutdown()
        except BaseException:
            log.exception("error during shutdown of %s", s.address)
    try:
        osc.stop_server()
    except BaseException:
        log.exception("error stopping OSC server")


atexit.register(_shutdown_all)


# mbientlab's MetaWear constructor os.fork()s a BLE worker process per
# device. The fork inherits our signal handlers and run_fs.py state
# (copy-on-write), so each child independently runs cleanup if it
# receives a signal. But a signal sent to *us* (the bash-tracked PID)
# doesn't reach those siblings unless they share a process group.
# Make ourselves a new process group leader so a single signal can fan
# out via os.killpg.
try:
    os.setpgrp()
except OSError:
    pass


def _sigint_handler(signum, frame):
    log.warning("signal %s received, shutting down", signum)
    _shutdown_all()
    # Re-broadcast to the group so any forked BLE workers also exit.
    # Re-installing the default handler on the same signal first means
    # we exit immediately on the broadcast rather than recurse.
    try:
        signal.signal(signum, signal.SIG_DFL)
        os.killpg(os.getpgrp(), signum)
    except BaseException:
        pass
    sys.exit(128 + signum)


signal.signal(signal.SIGINT, _sigint_handler)
# SIGHUP fires when our controlling terminal goes away — e.g., the user
# `ssh fugue-pi "..."` (no -t) and Ctrl-Cs the local ssh client. Without
# this, the remote python kept running as a zombie holding port 8001.
if hasattr(signal, "SIGHUP"):
    signal.signal(signal.SIGHUP, _sigint_handler)


if args.mode == "button-driven":
    # Connect each device but DON'T auto-start sensors — the user's
    # button (double-press) will toggle streaming per-device. Run until
    # SIGINT; the watchdog tick still drives stale-stream recovery for
    # whichever devices happen to be streaming.
    log.info("button-driven mode — connecting devices and waiting for button events")
    for s in states:
        try:
            s.connect()
        except BaseException:
            log.exception("failed to connect %s", s.address)
    log.info("ready — double-press a device button to toggle its stream; SIGINT to exit")
    while True:
        sleep(WATCHDOG_TICK_S)
        for s in states:
            try:
                s.check_and_recover()
            except BaseException:
                log.exception("[%s] check_and_recover raised", s.address)

else:
    # pi-driven (default): auto-start, run for STREAM_DURATION_S, stop.
    log.info("starting sensors")
    for s in states:
        try:
            s.start_sensors(s.sensor_config)
        except BaseException:
            log.exception("failed to start sensors on %s", s.address)

    deadline = time.monotonic() + STREAM_DURATION_S
    while time.monotonic() < deadline:
        sleep(WATCHDOG_TICK_S)
        for s in states:
            try:
                s.check_and_recover()
            except BaseException:
                log.exception("[%s] check_and_recover raised", s.address)

    log.info("stopping sensors")
    for s in states:
        try:
            s.stop_sensors(s.sensor_config)
        except BaseException:
            log.exception("failed to stop sensors on %s", s.address)

    sleep(1.0)

    log.info("disconnecting devices")
    for s in states:
        try:
            s.disconnect()
        except BaseException:
            log.exception("failed to disconnect %s", s.address)
        log.info("[%s] sample report: %s", s.address, s.logger)
        if s.failed:
            log.warning(
                "[%s] %d failure(s) recorded; sources=%s last_error=%r",
                s.address, len(s.failed_sources), s.failed_sources, s.last_error,
            )
