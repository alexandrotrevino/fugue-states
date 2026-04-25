import sys
import os

parent_dir = os.path.dirname(os.path.dirname(os.path.relpath(__file__)))
sys.path.append(parent_dir)

from sense.fs_setup import read_fugue_states_config
from sense.osc import ControlledOSCConnection
from sense.state import MetaWearState

config_file = "/home/pi/Documents/fugue-states/fs_config.json"
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

