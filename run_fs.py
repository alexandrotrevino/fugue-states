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
log = logging.getLogger("fs.run")

STREAM_DURATION_S = 5.0
WATCHDOG_TICK_S = 1.0

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


def _sigint_handler(signum, frame):
    log.warning("SIGINT received, shutting down")
    _shutdown_all()
    sys.exit(130)


signal.signal(signal.SIGINT, _sigint_handler)


log.info("starting sensors")
for s in states:
    try:
        s.start_sensors(s.sensor_config)
    except BaseException:
        log.exception("failed to start sensors on %s", s.address)

# Watchdog tick loop. Each iteration wakes WATCHDOG_TICK_S, asks every
# state to check_and_recover (no-op when healthy), and exits at the
# stream deadline. SIGINT will interrupt the sleep and the handler
# tears down cleanly.
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
