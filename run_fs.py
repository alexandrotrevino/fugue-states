import os
from time import sleep

from sense.fs_setup import read_fugue_states_config, validate_config
from sense.osc import ControlledOSCConnection
from sense.state import MetaWearState

STREAM_DURATION_S = 5.0

config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "fs_config.json")
config = read_fugue_states_config(config_path)
config = validate_config(config)
assert config["valid"], "Invalid configuration"

network = config["network"]
devices = config["metawear"]["devices"]

print("Setting up OSC")
osc = ControlledOSCConnection(ip=network["ip"], port=network["port"])

print("Building device states")
states = [MetaWearState(device_config=d, network_config=network, OSC=osc) for d in devices]

try:
    print("Starting sensors")
    for s in states:
        s.start_sensors(s.sensor_config)

    sleep(STREAM_DURATION_S)

    print("Stopping sensors")
    for s in states:
        s.stop_sensors(s.sensor_config)

    sleep(1.0)

    print("Disconnecting devices")
    for s in states:
        s.disconnect()
        s.generate_sample_report()
finally:
    osc.stop_server()
