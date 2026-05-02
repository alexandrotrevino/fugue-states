import argparse
import atexit
import logging
import os
import signal
import sys
import time
from datetime import datetime, timezone
from time import sleep

from sense.c_stderr import reroute_c_stderr_to_log
from sense.fs_setup import read_fugue_states_config, validate_config
from sense.osc import ControlledOSCConnection
from sense.state import MetaWearState
from sense.pipeline import LowPass, Magnitude, Tilt, OscEmit
from sense.recorder import Recorder, RecorderSink

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    datefmt="%H:%M:%S",
)
# When the controlling SSH session goes away mid-shutdown, stderr writes
# raise BrokenPipeError. Swallow logging errors so they don't abort the
# cleanup work itself.
logging.raiseExceptions = False
# Route libmetawear's C-level BLE noise through a logger at DEBUG so
# INFO-level runs stay clean. Must happen after basicConfig but before
# any libmetawear calls touch fd 2.
reroute_c_stderr_to_log()
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
parser.add_argument(
    "--record",
    action="store_true",
    help="Record every emitted frame to recordings/session-<timestamp>.jsonl.",
)
parser.add_argument(
    "--record-to",
    type=str,
    default=None,
    metavar="PATH",
    help="Record to a specific JSONL path (implies --record).",
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

# Testing IMUFrame pipeline elements.
# LowPass with output_sensor="acc_lp" emits both raw and filtered acc,
# so you can compare side-by-side. Downstream Magnitude sees both
# inputs and emits acc_mag + acc_lp_mag automatically.
for s in states:
    s.pipelines["acc"].stages = [
        LowPass(cutoff_hz=5.0, fs=25.0, output_sensor="acc_lp"),
        Magnitude(),
        Tilt(),
        OscEmit(s._osc_client)
    ]

# --- Recording (opt-in) -------------------------------------------------------
# --record auto-generates recordings/session-<ts>.jsonl in the project
# dir; --record-to PATH overrides. Recorder is injected into every
# pipeline just before the first terminal stage so the JSONL captures
# exactly what the receiver sees (post-transform). All Recorders share
# one RecorderSink so a session is one file demuxable by device+sensor.
# A `_meta` first line anchors the session in wall-clock time and
# records the configured sensor settings; `_session` start/end markers
# bracket the file for boundary recovery on read.
recorder_sink = None
recorder_path = None
ts = time.strftime("%Y%m%dT%H%M%S")
if args.record_to:
    recorder_path = args.record_to
elif args.record:
    recorder_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "recordings",
        f"session-{ts}.jsonl",
    )

if recorder_path:
    metadata = {
        "version": 1,
        "session_id": ts,
        "wall_start_iso": datetime.now(timezone.utc).isoformat(),
        "wall_start_epoch": time.time(),
        "mono_start": time.monotonic(),
        "devices": [
            {"address": d["mac"], "name": d["name"], "sensors": d["sensors"]}
            for d in devices
        ],
    }
    recorder_sink = RecorderSink(recorder_path)
    recorder_sink.open(metadata=metadata)
    for s in states:
        for pipe in s.pipelines.values():
            insert_at = len(pipe.stages)
            for i, stage in enumerate(pipe.stages):
                if stage.is_terminal:
                    insert_at = i
                    break
            pipe.stages.insert(insert_at, Recorder(recorder_sink))

# Announce the OSC addresses each device will publish so receivers
# (PD, recorders) can subscribe without hardcoding. Fires once at
# startup; re-call s.advertise() interactively if pipelines change.
for s in states:
    s.advertise()


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
    if recorder_sink is not None:
        try:
            recorder_sink.close()
        except BaseException:
            log.exception("error closing recorder sink")


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
# SIGTERM is what systemd sends on `systemctl stop`. Without a handler
# we'd get SIGKILLed after TimeoutStopSec elapses and skip cleanup.
signal.signal(signal.SIGTERM, _sigint_handler)
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
