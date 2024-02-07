# Abstractions to configure device sensors

# Imports
from mbientlab.metawear import MetaWear, libmetawear
from mbientlab.metawear.cbindings import *
from fs_setup import State, retrieve_default_settings

def start_sensor_stream(sensor_name) -> function:
    d = {
        "Accelerometer": acc_setup_stream,
        "Gyroscope": gyro_bmi270_setup_stream
    }
    # if sensor_name not in d.keys():
    #     raise SensorConfigError
    return(d[sensor_name])

def stop_sensor_stream(sensor_name) -> function:
    d = {
        "Accelerometer": acc_stop_stream,
        "Gyroscope": gyro_bmi270_stop_stream
    }
    return(d[sensor_name])

# TODO abstractions for the setup of any sensors defined in config
# - gyroscope
# - mag
# - fusions
# - temp, light


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
    # if threshold is not None:
    #     libmetawear.mbl_mw_acc_bosch_set_any_motion_threshold(state.device.board, threshold)
    libmetawear.mbl_mw_acc_write_acceleration_config(state.device.board)
    
    # Get acc and subscribe
    signal = libmetawear.mbl_mw_acc_get_acceleration_data_signal(state.device.board)
    libmetawear.mbl_mw_datasignal_subscribe(signal, None, state.acc_callback)
    
    # Start acc
    libmetawear.mbl_mw_acc_enable_acceleration_sampling(state.device.board)
    libmetawear.mbl_mw_acc_start(state.device.board)

    return(None)


def acc_stop_stream(state) -> State:
    
    # Stop acc
    libmetawear.mbl_mw_acc_stop(state.device.board)
    libmetawear.mbl_mw_acc_disable_acceleration_sampling(state.device.board)
    
    # Unsubscribe
    signal = libmetawear.mbl_mw_acc_get_acceleration_data_signal(state.device.board)
    libmetawear.mbl_mw_datasignal_unsubscribe(signal)

    return(None)

def gyro_bmi270_setup_stream(state, sensor_config) -> State:

    # Import parameters from configuration
    try:
        odr = float(sensor_config["Gyroscope"]["odr"])
    except KeyError:
        odr = retrieve_default_settings("Gyroscope", "odr")
    
    try:
        gyro_range = float(sensor_config["Gyroscope"]["range"])
    except KeyError:
        gyro_range = retrieve_default_settings("Gyroscope", "range")
    
    # Setup gyro
    libmetawear.mbl_mw_gyro_bmi270_set_odr(state.device.board, odr)
    libmetawear.mbl_mw_gyro_bmi270_set_range(state.device.board, gyro_range)
    libmetawear.mbl_mw_gyro_bmi270_write_config(state.device.board)
    
    # Get gyro and subscribe
    gyro = libmetawear.mbl_mw_gyro_bmi270_get_rotation_data_signal(state.device.board)
    libmetawear.mbl_mw_datasignal_subscribe(gyro, None, state.gyro_callback)

    # Start gyro
    libmetawear.mbl_mw_gyro_bmi270_enable_rotation_sampling(state.device.board)
    libmetawear.mbl_mw_gyro_bmi270_start(state.device.board)

    return(None)


def gyro_bmi270_stop_stream(state) -> State:
    
    # Stop
    libmetawear.mbl_mw_gyro_bmi270_stop(state.device.board)
    libmetawear.mbl_mw_gyro_bmi270_disable_rotation_sampling(state.device.board)
    
    # Unsubscribe
    gyro = libmetawear.mbl_mw_gyro_bmi270_get_rotation_data_signal(state.device.board)
    libmetawear.mbl_mw_datasignal_unsubscribe(gyro)

    return(None)
    