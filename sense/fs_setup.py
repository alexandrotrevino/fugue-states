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
    # init
    def __init__(self, device, device_config):
        self.device = device
        self.config = device_config
        self.samples = 0
        self.callback = FnVoid_VoidP_DataP(self.data_handler)
        self.osc = range(12345, len(self.config["sensors"]))

    # callback
    def data_handler(self, ctx, data):
        parsed_data = parse_value(data)
        client.send_message("/sensor/accelerometer/%s" % self.device.address, (parsed_data.x, parsed_data.y, parsed_data.z))
        self.samples+= 1

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

    # validate config
    validate_config(config)

    # parse network configuration
    network = config["network"]
    ip = network["ip"]
    port = int(network["port"])

    # OSC client setup - IP, port
    client = udp_client.SimpleUDPClient(ip, port)

    # parse metwear configuration
    meta = config["metawear"]
    devices = meta["devices"]

    states = []
    
    # Connect to all devices
    for i,  in range(len(devices)):
        d = MetaWear(i + 1)
        d.connect()
        print("Connected to " + d.address + " over " + ("USB" if d.usb.is_connected else "BLE"))
        states.append(State(d))


def start_device(device_config, sensor_config):
    """
    A function that takes configuration data as input and intitializes a
    MetaWear device and it's configuration state.

    Returns a State object.

    """

    # Configure device (State)
    d = MetaWear(device_config["mac"])
    d.connect()
    print("Connected to " + d.address + " over " + "BLE")
    state = State(d)
    

    for s in states:
        print("Configuring device")
        # setup ble
        libmetawear.mbl_mw_settings_set_connection_parameters(s.device.board, 7.5, 7.5, 0, 6000)
        sleep(1.5)

        # setup sensors
        # ...
    # TODO abstractions for the setup of any sensors defined in config
    # TODO ensure we can pass States objects reliably 
    #   also consider the possibility of just using something else instead of 'sleeping'. Waiting for an input?
        
    return(states)

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
    print("Total Samples Received")
    for s in states:
        print("%s -> %d" % (s.device.address, s.samples))

def validate_config(config):
    # TODO implement some basic checks
    # - devices and sensors are same length
    # - sensor names are valid
    # - inputs to sensors are valid

    return(0)