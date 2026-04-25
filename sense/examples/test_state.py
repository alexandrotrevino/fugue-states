import os

from sense.fs_setup import read_fugue_states_config
from sense.osc import ControlledOSCConnection
from sense.state import MetaWearState

# Resolve fs_config.json relative to repo root (two levels up from this file).
config_file = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "..", "..", "fs_config.json"
)
config = read_fugue_states_config(config_file)

# Setup OSC
ip = config["network"]["ip"]
port = config["network"]["port"]

osc = ControlledOSCConnection(ip=ip, port=port)

n_devices = len(config["metawear"]["devices"])

states = []

for i in range(n_devices):
    State = MetaWearState(device_config=config["metawear"]["devices"][i],
                          network_config=config["network"],
                          OSC=osc)
    states.append(State)

