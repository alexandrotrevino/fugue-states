# TODO revise imports at the end
from mbientlab.metawear import MetaWear, libmetawear, parse_value
from mbientlab.metawear.cbindings import *
from time import sleep
from threading import Event
from pythonosc import udp_client

import platform
import sys
import json

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
        self.device = device
        self.samples = {"acc": 0, "gyro": 0, "quat": 0, "euler": 0, "mag": 0, "temp": 0, "light": 0}
        self.acc_callback = FnVoid_VoidP_DataP(self.acc_data_handler)
        self.gyro_callback = FnVoid_VoidP_DataP(self.gyro_data_handler)
        self.quat_callback = FnVoid_VoidP_DataP(self.quat_data_handler)
        self.euler_callback = FnVoid_VoidP_DataP(self.euler_data_handler)
        self.mag_callback = FnVoid_VoidP_DataP(self.mag_data_handler)
        self.temp_callback = FnVoid_VoidP_DataP(self.temp_data_handler)
        self.light_callback = FnVoid_VoidP_DataP(self.light_data_handler)
        self.client = osc_client

    # Callbacks
    # These functions handle the different data outputs of the MetaWear device. 
    # TODO - validate each of the `parsed_data.` dot outputs.
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

    def quat_data_handler(self, ctx, data):
        """
        Quaternion data give the relative orientation of the device as a unit spatial
        quaternion. This is computed by sensor fusion onboard the MetaWear device.

        Accelerometer and gyrometer data should *not* be used in tandem with sensor
        fusion data.
        """
        parsed_data = parse_value(data)
        self.client.send_message("%/quat" % self.device.address, (parsed_data.w, parsed_data.x, parsed_data.y, parsed_data.z))
        self.samples["quat"] += 1

    def euler_data_handler(self, ctx, data):
        """
        Euler angles provide the relative orientation of the device as computed from both
        accelerometer and gyrometer data together. This is computed by sensor fusion
        onboard the MetaWear device.

        Accelerometer and gyrometer data should *not* be used in tandem with sensor
        fusion data.
        """
        parsed_data = parse_value(data)
        self.client.send_message("%/euler" % self.device.address, (parsed_data.w, parsed_data.x, parsed_data.y, parsed_data.z))
        self.samples["euler"] += 1
    
    def mag_data_handler(self, ctx, data):
        """
        Magnometer data are given in terms of the h-component for geomagnetic north,
        the d-component for east, and the z-component for vertical direction. 
        Components are expressed in nano Tesla (nT).
        """
        parsed_data = parse_value(data)
        self.client.send_message("%/mag" % self.device.address, (parsed_data.h, parsed_data.d, parsed_data.z))
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

def setup_all(config):

    # Load configuration -------------------
    # -- Merge defaults
    # config = merge_config_with_defaults(config, default_settings=defaults)

    # -- Validate configuration file
    assert validate_config(config)

    # -- Parse network configuration
    network = config["network"]
    ip = network["ip"]
    port = int(network["port"])

    # -- Parse metwear configuration
    meta = config["metawear"]
    devices = meta["devices"]
    sensors = meta["sensors"]

    # -- OSC client setup - IP, port
    client = udp_client.SimpleUDPClient(ip, port)

    # Connect devices ----------------------
    # TODO some control flow will be necessary to ensure that things that start, 
    # always stop, and that strange states are never reached.

    states = []

    for i,  in range(len(devices)):
        start_device(devices[i], sensors[i], client)

    return(states)


def start_device(device_config, sensor_config, osc_client):
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
    libmetawear.mbl_mw_settings_set_connection_parameters(s.device.board, 7.5, 7.5, 0, 6000)
    sleep(0.5)
    # TODO abstractions for the setup of any sensors defined in config
    # TODO ensure we can pass States objects reliably 
    #   also use something else instead of 'sleeping'. Waiting for an input?

    return(state)
    

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
    # TODO implement some basic checks
    # - devices and sensors are same length
    # - sensor names are valid
    # - inputs to sensors are valid

    return(True)

def retrieve_default_settings(sensor, parameter): 
    
    default_settings = {
        "Accelerometer": {"odr": 25, "range": 16.0},
        "Gyroscope": {"odr": 25, "range": 2000.0},
        "Magnetometer": {"odr": 25},
        "Temperature": {"period": 1}
        "Ambient Light": {"odr": 10}
        # TODO finish defaults
    }
    try:
        setting = default_settings[sensor][parameter]
    except KeyError:
        setting = None
        
    return(setting)
