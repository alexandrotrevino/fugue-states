from sense import fs_setup
from pythonosc import udp_client
from time import sleep

file_path = "/home/pi/Documents/fugue-states/fs_config.json"
config = fs_setup.read_fugue_states_config(file_path)

# -- Validate configuration file
print("Validating and parsing config")
config = fs_setup.validate_config(config)
assert config["valid"]

# -- Parse network configuration
ip = config["network"]["ip"]
port = int(config["network"]["port"])

# -- Parse metwear configuration
devices = config["metawear"]["devices"]
sensors = config["metawear"]["sensors"]

# -- OSC client setup - IP, port
print("Setting up OSC")
client = udp_client.SimpleUDPClient(ip, port)

# Connect devices ----------------------
# TODO some control flow will be necessary to ensure that things that start, 
# always stop, and that strange states are never reached.

states = []
print("Connecting devices")
# start_time = time
for i in range(len(devices)):
    state = fs_setup.connect_device(devices[i], client)
    states.append(state)

print("Starting sensors")
for i, s in enumerate(states):
    fs_setup.start_sensors(s, sensors[i])

sleep(5.0)

print("Stopping sensors")
# end_time = time.time()
for i, s in enumerate(states):
    fs_setup.stop_sensors(s, sensors[i])


sleep(1.0)
print("Disconnecting devices")
#elapsed_time = start_time - end_time
for s in states:
    fs_setup.disconnect_device(s)
    fs_setup.generate_sample_report(s, 5.0)

