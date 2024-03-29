from sense import fs_setup, sensors, state

cf = "/home/pi/Documents/fugue-states/fs_config.json"

config = fs_setup.read_fugue_states_config(cf)

devices = {}
for device in config["devices"]:
    MW = state.MetaWearState(device, config["network"])
    devices[MW.address] = MW

