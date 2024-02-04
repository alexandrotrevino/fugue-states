# Abstractions to configure device sensors

# Imports
from mbientlab.metawear import MetaWear, libmetawear, parse_value
from mbientlab.metawear.cbindings import *
from time import sleep
from threading import Event
from pythonosc import udp_client

from fs_setup import State, retrieve_default_settings

import platform
import sys
import json

def acc_setup_stream(state, sensor_config) -> State:

    # Import parameters from configuration
    try:
        odr = float(sensor_config["Accelerometer"]["odr"])
    except KeyError:
        odr = retrieve_default_settings("Accelerometer", "odr")
    
    try:
        acc_range = float(sensor_config["Accelerometer"]["range"])
    except KeyError:
        acc_range = retrieve_default_settings("Accelerometer", "range")
    
    try:
        threshold = float(sensor_config["Accelerometer"]["threshold"])
    except KeyError:
        threshold = None
    


    # Setup acc
    libmetawear.mbl_mw_acc_set_odr(state.device.board, odr)
    libmetawear.mbl_mw_acc_set_range(state.device.board, acc_range)
    if threshold:
        libmetawear.mbl_mw_acc_bosch_set_any_motion_threshold(state.device.board, threshold)
    libmetawear.mbl_mw_acc_write_acceleration_config(state.device.board)
    
    # Get acc and subscribe
    signal = libmetawear.mbl_mw_acc_get_acceleration_data_signal(state.device.board)
    libmetawear.mbl_mw_datasignal_subscribe(signal, None, state.callback)
    
    # Start acc
    libmetawear.mbl_mw_acc_enable_acceleration_sampling(state.device.board)
    libmetawear.mbl_mw_acc_start(state.device.board)

    return(state)


def acc_stop_stream(state) -> State:
    
    # Stop acc
    libmetawear.mbl_mw_acc_stop(state.device.board)
    libmetawear.mbl_mw_acc_disable_acceleration_sampling(state.device.board)
    
    # Unsubscribe
    signal = libmetawear.mbl_mw_acc_get_acceleration_data_signal(state.device.board)
    libmetawear.mbl_mw_datasignal_unsubscribe(signal)

    return(state)
    