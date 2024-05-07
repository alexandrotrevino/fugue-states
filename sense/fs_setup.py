from mbientlab.metawear import MetaWear, libmetawear, parse_value, create_voidp, create_voidp_int
from mbientlab.metawear.cbindings import *
from time import sleep, time
from pythonosc import udp_client
from .sensors import start_sensor_stream, stop_sensor_stream
import json
from threading import Event
import ipaddress
import re

    
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

def validate_network_config(config):
    valid = True
    # Network validation ---
    print("Validating configuration file...")
    
    if not is_valid_ip(config["ip"]):
        print("Invalid IP address in config file.\n")
        valid = False
    
    if not isinstance(config["port"], int):
        config["network"]["port"] = int(config["network"]["port"])
    
    if not is_valid_port(config["port"]):
        print("Invalid port number in config file.\n")
        valid = False
    
    config["valid"] = valid
    return(config)

def validate_device_config(config):
    
    valid = True
    sensors = config["sensors"]

    if not is_valid_mac(config["mac"]):
        print("Invalid MAC address in config file.")
        valid = False

    if config["name"].lower() not in ["mms", "mmrl"]:
        print("Invalid sensor names. MMS and MMRL sensors are supported.")
        valid = False
    
    # Make sure sensor types are valid
    allowed_sensors = ["Accelerometer", "Gyroscope", "Gyroscope160", "Magnetometer", "Temperature", "Ambient Light", "Sensor Fusion"]
    for sensor in sensors:
        if sensor not in allowed_sensors:
            print("Invalid config file - sensor not recognized:", sensor)
            valid = False

    # Do not allow Acc, Gyro, and Mag to be configured alongside Sensor Fusion
    non_fusion = ["Accelerometer", "Gyroscope", "Magnetometer"]
    if "Sensor Fusion" in sensors.keys():
        for other in non_fusion:
            if other in sensors.keys():
                print(other, "not compatible with Sensor Fusion - removing from config.")
                del sensors[other]
        fusion_mode = sensors["Sensor Fusion"]["output"]
    else: 
        fusion_mode = None

    # Make changes required for MMRL type devices                  
    if config["name"].lower() == "mmrl":
        if "Ambient Light" in sensors.keys():
            print("The MMRL device lacks an ambient light sensor - removing from config.")
            del sensors["Ambient Light"]

        if "Gyroscope" in sensors.keys():
            sensors["Gyroscope160"] = sensors.pop("Gyroscope")
    config["fusion_mode"] = fusion_mode
    config["valid"] = valid
    return(config)


def validate_config(config):
    
    valid = True
    # Network validation ---
    print("Validating configuration file...")
    if "network" not in config.keys():
        print("OSC/Network configuration not found. Please check config.")
        valid = False
    
    if not is_valid_ip(config["network"]["ip"]):
        print("Invalid IP address in config file.")
        valid = False
    
    if not isinstance(config["network"]["port"], int):
        config["network"]["port"] = int(config["network"]["port"])
    
    if not is_valid_port(config["network"]["port"]):
        print("Invalid port number in config file.")
        valid = False
    
    # Metawear validation ---
    mw = config["metawear"]
    devices = mw["devices"]
        
    for device in devices:

        sensors = device["sensors"]

        if not is_valid_mac(device["mac"]):
            print("Invalid MAC address in config file.")
            valid = False
    
        if device["name"].lower() not in ["mms", "mmrl"]:
            print("Invalid sensor names. MMS and MMRL sensors are supported.")
            valid = False
        
        # Make sure sensor types are valid
        allowed_sensors = ["Accelerometer", "Gyroscope", "Gyroscope160", "Magnetometer", "Temperature", "Ambient Light", "Sensor Fusion"]
        for sensor in sensors:
            if sensor not in allowed_sensors:
                print("Invalid config file - sensor not recognized:", sensor)
                valid = False

        # Do not allow Acc, Gyro, and Mag to be configured alongside Sensor Fusion
        non_fusion = ["Accelerometer", "Gyroscope", "Magnetometer"]
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

            if "Gyroscope" in sensors.keys():
                sensors["Gyroscope160"] = sensors.pop("Gyroscope")

    config["valid"] = valid
    return(config)

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
