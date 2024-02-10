from mbientlab.metawear import MetaWear, libmetawear, parse_value, create_voidp, create_voidp_int
from mbientlab.metawear.cbindings import *
from time import sleep, time
from pythonosc import udp_client
from .sensors import start_sensor_stream, stop_sensor_stream
import json
from threading import Event
import ipaddress
import re

class State:
    """
    A class that holds the connection and configuration for MetaWear devices.
    Each State holds the device API, with methods to configure the board,
    access the MAC address, etc., as well as a callback - `data handler` - 
    and a mechanism to keep track of the data samples coming through.

    See also the MetaWear class.
    """
    # Initialize
    def __init__(self, device, osc_client):
        
        # Device (and device.board)
        self.device = device

        # Diagnostic
        self.samples = {"acc": 0, "gyro": 0, "mag": 0, "temp": 0, "light": 0, "fusion": 0} 

        # Callback functions
        self.acc_callback = FnVoid_VoidP_DataP(self.acc_data_handler)
        self.gyro_callback = FnVoid_VoidP_DataP(self.gyro_data_handler)
        self.mag_callback = FnVoid_VoidP_DataP(self.mag_data_handler)
        self.temp_callback = FnVoid_VoidP_DataP(self.temp_data_handler)
        self.light_callback = FnVoid_VoidP_DataP(self.light_data_handler)
        self.quat_callback = FnVoid_VoidP_DataP(self.quat_data_handler)
        self.euler_callback = FnVoid_VoidP_DataP(self.euler_data_handler)
        self.linear_acc_callback = FnVoid_VoidP_DataP(self.linear_acc_data_handler)
        self.gravity_callback = FnVoid_VoidP_DataP(self.gravity_data_handler)
        self.corrected_acc = FnVoid_VoidP_DataP(self.corrected_acc_data_handler)
        self.corrected_gyro = FnVoid_VoidP_DataP(self.corrected_gyro_data_handler)
        self.corrected_mag = FnVoid_VoidP_DataP(self.corrected_mag_data_handler)

        # Timer object
        self.timer = None

        # OSC Client
        self.client = osc_client

    # Timer definition
    # def start_timer(self, period):
    #     e = Event()
    #     create_voidp(lambda fn: libmetawear.mbl_mw_timer_create_indefinite(self.device.board, period, 0, None, fn), resource = "timer", event = e)

    # Callbacks
    # These functions handle the different data outputs of the MetaWear device. 
    # TODO - what does `ctx` do?
    def acc_data_handler(self, ctx, data):
        """
        Accelerometer data are expressed in terms of 'g' along the [x, y, z] direction.
        """
        parsed_data = parse_value(data)
        self.client.send_message("%/acc" % self.device.address, (parsed_data.x, parsed_data.y, parsed_data.z))
        self.samples["acc"] += 1

    def gryo_data_handler(self, ctx, data):
        """
        Gyrometer data are expressed in terms of degrees of rotation around the [x, y, z] axis.
        """
        parsed_data = parse_value(data)
        self.client.send_message("%/gyro" % self.device.address, (parsed_data.x, parsed_data.y, parsed_data.z))
        self.samples["gyro"] += 1

    def mag_data_handler(self, ctx, data):
        """
        Magnometer data are given in terms of the h-component for geomagnetic north,
        the d-component for east, and the z-component for vertical direction. 
        Components are expressed in nano Tesla (nT).
        """
        parsed_data = parse_value(data)
        self.client.send_message("%/mag" % self.device.address, (parsed_data.x, parsed_data.y, parsed_data.z))
        self.samples["mag"] += 1
    
    def temp_data_handler(self, ctx, data):
        """
        Temperature data are expressed in degrees Celsius. 
        """
        temperature = parse_value(data)
        self.client.send_message("%/temp" % self.device.address, temperature)
        self.samples["temp"] += 1

    def light_data_handler(self, ctx, data):
        """
        Ambient light data are expressed in lux units and the device is sensitive from 0.1-64k lux. 
        """
        light = parse_value(data)
        self.client.send_message("%/light" % self.device.address, light)
        self.samples["light"] += 1
    
    def quat_data_handler(self, ctx, data):
        """
        Quaternion data give the relative orientation of the device as a unit spatial
        quaternion. This is computed by sensor fusion onboard the MetaWear device.

        Accelerometer and gyrometer data should *not* be used in tandem with sensor
        fusion data.
        """
        parsed_data = parse_value(data)
        self.client.send_message("%/quat" % self.device.address, (parsed_data.w, parsed_data.x, parsed_data.y, parsed_data.z))
        self.samples["fusion"] += 1

    def euler_data_handler(self, ctx, data):
        """
        Euler angles provide the relative orientation of the device as computed from both
        accelerometer and gyrometer data together. This is computed by sensor fusion
        onboard the MetaWear device.

        Accelerometer and gyrometer data should *not* be used in tandem with sensor
        fusion data.
        """
        parsed_data = parse_value(data)
        self.client.send_message("%/euler" % self.device.address, (parsed_data.heading, parsed_data.pitch, parsed_data.roll, parsed_data.yaw))
        self.samples["fusion"] += 1

    def linear_acc_data_handler(self, ctx, data):
        parsed_data = parse_value(data)
        self.client.send_message("%/linear_acc" % self.device.address, (parsed_data.x, parsed_data.y, parsed_data.z))
        self.samples["fusion"] += 1

    def gravity_data_handler(self, ctx, data):
        parsed_data = parse_value(data)
        self.client.send_message("%/gravity" % self.device.address, (parsed_data.x, parsed_data.y, parsed_data.z))
        self.samples["fusion"] += 1

    def corrected_acc_data_handler(self, ctx, data):
        parsed_data = parse_value(data)
        self.client.send_message("%/corrected_acc" % self.device.address, (parsed_data.x, parsed_data.y, parsed_data.z))
        self.samples["fusion"] += 1

    def corrected_gyro_data_handler(self, ctx, data):
        parsed_data = parse_value(data)
        self.client.send_message("%/corrected_gyro" % self.device.address, (parsed_data.x, parsed_data.y, parsed_data.z))
        self.samples["fusion"] += 1

    def corrected_mag_data_handler(self, ctx, data):
        parsed_data = parse_value(data)
        self.client.send_message("%/corrected_mag" % self.device.address, (parsed_data.x, parsed_data.y, parsed_data.z))
        self.samples["fusion"] += 1

    
def read_fugue_states_config(x) -> dict:
    """
    Return Fugue States configuration details from an input file.
    network
    | JSON Key   | Command Line                 | Required
    |------------|------------------------------|---------
    | ip         | n/a                          | Y
    | port       | n/a                          | Y

    metawear
    | JSON Key   | Command Line                 | Required
    |------------|------------------------------|---------
    | command    | --command                    | N       
    | devices    | --device                     | Y       
    | sensors    | --sensor                     | Y       
    | resolution | --width, --height            | N       
    | txPower    | --tx-power                   | N       
    
    Example json:
    ```json
    {
        "network": {
            "ip": "162.01.01.192",
            "port": "12345"
        },
        "metawear": {
            "devices": [
	            {"mac": "EC:47:49:CF:53:C4", "name": "mms"}
            ],
            "sensors": [
                {
    	            "Accelerometer": {"odr": 25, "range": 4.0},
	                "Gyroscope": {"odr": 25, "range": 1000.0},
	                "Magnetometer": {"odr": 25},
	                "Temperature": {"period": 1}
                }
            ]
        }
    }
    ```
    """
    with open(x, 'r') as file:
        fs_config = json.load(file)
    
    return(fs_config)

def run_all(config):

    # Load configuration -------------------
    # -- Merge defaults
    # config = merge_config_with_defaults(config, default_settings=defaults)

    # -- Validate configuration file
    print("Validating and parsing config")
    assert validate_config(config)

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
    start_time = time.time()
    for i in range(len(devices)):
        state = connect_device(devices[i], client)
        states.append(state)
    
    print("Starting sensors")
    for i, s in enumerate(states):
        start_sensors(s, sensors[i])

    sleep(5.0)

    print("Stopping sensors")
    end_time = time.time()
    for i, s in enumerate(states):
        stop_sensors(s, sensors[i])

    print("Disconnecting devices")
    elapsed_time = start_time - end_time
    for s in states:
        disconnect_device(s)
        generate_sample_report(s)
    return(None) # perhaps in some future state, the states are actually saved for later, avoiding reconnect.

def start_sensors(state, sensor_config) -> None:
    print(f"Starting sensors:%" % "\n  - ".join(sensor_config.keys))

    # TODO Logic for sensor fusions - take precendence over acc/gyro
    try:
        for sensor in sensor_config.keys():
            start_sensor_stream[sensor](state, sensor_config)
    
    except Exception as e:
        print(f"Streaming interrupted unexpectedly: {e}")
    
    finally:
        for sensor in sensor_config.keys():
            stop_sensor_stream[sensor](state, sensor_config)
    return(None)

def stop_sensors(state, sensor_config) -> None:
    print(f"Stopping sensors:%" % "\n  - ".join(sensor_config.keys()))
    for sensor in sensor_config.keys():
        stop_sensor_stream[sensor](state, sensor_config)
    return(None)

def connect_device(device_config, osc_client):
    """
    A function that takes configuration data as input and intitializes a
    MetaWear device and it's configuration state.

    Returns a State object.
    """

    # Configure device (State)
    mac = device_config["mac"]
    d = MetaWear(mac)
    d.connect()
    print("Connected to " + mac + " over " + "BLE")
    state = State(d, osc_client)
    
    # Setup BLE
    print("Configuring %" % mac)
    libmetawear.mbl_mw_settings_set_connection_parameters(state.device.board, 7.5, 7.5, 0, 6000)
    sleep(1.0)

    return(state)


def disconnect_device(state) -> None:
    libmetawear.mbl_mw_debug_disconnect(state.device.board)
    return(None)


def generate_sample_report(state) -> None:
    print(state.samples)


def stop_devices(states):
    # TODO check the states exists and validate its class
    # tear down
    for s in states:
        # stop acc
        libmetawear.mbl_mw_acc_stop(s.device.board)
        libmetawear.mbl_mw_acc_disable_acceleration_sampling(s.device.board)
        # unsubscribe
        # TODO Also need to unsubscribe to all sensors
        signal = libmetawear.mbl_mw_acc_get_acceleration_data_signal(s.device.board)
        libmetawear.mbl_mw_datasignal_unsubscribe(signal)
        # disconnect
        libmetawear.mbl_mw_debug_disconnect(s.device.board)

    # recap
    # TODO make this total versus expected to quantify packet loss
        
    print("Total Samples Received")
    for s in states:
        print("%s -> %d" % (s.device.address, s.samples))

def validate_config(config):
    
    # Network validation ---
    print("Validating configuration file...")
    if "network" not in config.keys():
        print("OSC/Network configuration not found. Please check config.")
        return(False)
    
    if not is_valid_ip(config["network"]["ip"]):
        print("Invalid IP address in config file.")
        return(False)
    
    if not isinstance(config["network"]["port"], int):
        config["network"]["port"] = int(config["network"]["port"])
    
    if not is_valid_port(config["network"]["port"]):
        print("Invalid port number in config file.")
        return(False)
    
    # Metawear validation ---
    mw = config["metawear"]
    devices = mw["devices"]
    device_sensors = mw["sensors"]

    if not len(devices) == len(device_sensors):
        print("Device and sensor configurations do not match. Configurations should be of equal length")
        return(False)

    for i, device in enumerate(devices):

        sensors = device_sensors[i]

        if not is_valid_mac(device["mac"]):
            print("Invalid MAC address in config file.")
            return(False)
    
        if device["name"].lower() not in ["mms", "mmrl"]:
            print("Invalid sensor names. MMS and MMRL sensors are supported.")
            return(False)
        
        # Make sure sensor types are valid
        allowed_sensors = ["Accelerometer", "Gyroscope", "Gyroscope160", "Magnometer", "Temperature", "Ambient Light", "Sensor Fusion"]
        for sensor in sensors:
            if sensor not in allowed_sensors:
                print("Invalid config file - sensor not recognized:", sensor)
                return(False)

        # Do not allow Acc, Gyro, and Mag to be configured alongside Sensor Fusion
        non_fusion = ["Accelerometer", "Gyroscope", "Magnometer"]
        if "Sensor Fusion" in sensors.keys():
            for other in non_fusion:
                if other in sensors.keys():
                    print(other, "not compatible with Sensor Fusion - removing from config.")
                    del sensors[other]

        # Make changes required for MMRL type devices                  
        if device["name"].lower() == "mmrl":
            if "Ambient Light" in sensors.keys():
                print("The MMRL device lacks an ambient light sensor - removing from config.")
                del sensors["Ambient Light"]

            if "Gyroscope" in sensor.keys():
                sensors["Gyroscope160"] = sensors.pop("Gyroscope")

    return(True)

def is_valid_ip(ip):
    try:
        ipaddress.ip_address(ip)
        return True
    except ValueError:
        return False

def is_valid_port(port):
    try:
        port = int(port)
        return 0 <= port <= 65535
    except ValueError:
        return False

def is_valid_mac(mac):
    
    # Regex for validating MAC addresses
    mac_regex = r"^([0-9A-Fa-f]{2}[:-]){5}([0-9A-Fa-f]{2})$"

    return(bool(re.match(mac_regex, mac)))

def retrieve_default_settings(sensor, parameter): 
    
    default_settings = {
        "Accelerometer": {"odr": 25, "range": 16.0},
        "Gyroscope": {"odr": 25, "range": 2000.0},
        "Magnetometer": {"odr": 25},
        "Temperature": {"period": 1},
        "Ambient Light": {"odr": 10}
        # TODO finish defaults
    }
    try:
        setting = default_settings[sensor][parameter]
    except KeyError:
        setting = None

    return(setting)
