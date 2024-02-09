# Abstractions to configure device sensors

# Imports
from mbientlab.metawear import MetaWear, libmetawear, create_voidp, create_voidp_int
from mbientlab.metawear.cbindings import *
from fs_setup import State, retrieve_default_settings
from threading import Event

def start_sensor_stream(sensor_name) -> function:
    d = {
        "Accelerometer": acc_setup_stream,
        "Gyroscope": gyro_bmi270_setup_stream,
        "Magnometer": mag_setup_stream,
        "Temperature": temp_setup_stream
    }
    # if sensor_name not in d.keys():
    #     raise SensorConfigError
    return(d[sensor_name])

def stop_sensor_stream(sensor_name) -> function:
    d = {
        "Accelerometer": acc_stop_stream,
        "Gyroscope": gyro_bmi270_stop_stream,
        "Magnometer": mag_stop_stream,
        "Temperature": temp_stop_stream
    }
    return(d[sensor_name])

# TODO abstractions for the setup of any sensors defined in config
# - gyroscope
# - mag
# - fusions
# - temp, light

def mag_setup_stream(state, sensor_config) -> None:
    
    # Configure
    odr = float(sensor_config["Magnometer"]["odr"])
    
    # Setup mag
    libmetawear.mbl_mw_mag_bmm150_configure(state.device.board, odr)

    # Get mag and subscribe
    signal = libmetawear.mbl_mw_mag_bmm150_get_b_field_data_signal(state.device.board)
    libmetawear.mbl_mw_datasignal_subscribe(signal, None, state.mag_callback)
    
    # Start mag
    libmetawear.mbl_mw_mag_bmm150_enable_b_field_sampling(state.device.board)
    libmetawear.mbl_mw_mag_bmm150_start(state.device.board)
    return(None)

def mag_stop_stream(state, sensor_config) -> None:

    # Stop mag
    libmetawear.mbl_mw_mag_bmm150_stop(state.device.board)
    libmetawear.mbl_mw_mag_bmm150_disable_b_field_sampling(state.device.board)
    
    # Unsubscribe
    signal = libmetawear.mbl_mw_mag_bmm150_get_b_field_data_signal(state.device.board)
    libmetawear.mbl_mw_datasignal_unsubscribe(signal)

    return(None)

def temp_start_stream(state, sensor_config) -> None:

    e = Event()
    period = sensor_config["Temperature"]["period"]
    signal = libmetawear.mbl_mw_multi_chnl_temp_get_temperature_data_signal(state.device.board, MetaWearRProChannel.ON_BOARD_THERMISTOR)
    
    # subscribe to temp signal
    libmetawear.mbl_mw_datasignal_subscribe(signal, None, callback)

    # create timer - fires ever 1000ms
    timer = create_voidp(lambda fn: libmetawear.mbl_mw_timer_create_indefinite(state.device.board, period, 0, None, fn), resource = "timer", event = e)
        
    # create event based on timer - read temp when timer fires
    libmetawear.mbl_mw_event_record_commands(timer)
    libmetawear.mbl_mw_datasignal_read(signal)
    create_voidp_int(lambda fn: libmetawear.mbl_mw_event_end_record(timer, None, fn), event = e)

    # start timer
    libmetawear.mbl_mw_timer_start(timer)

    return(None)

def temp_stop_stream(state, sensor_config) -> None:
    
    # remove timer
    libmetawear.mbl_mw_timer_remove(timer)
    sleep(1.0)

    # remove event
    libmetawear.mbl_mw_event_remove_all(d.board)
    sleep(1.0)

    # unsubscribe
    libmetawear.mbl_mw_datasignal_unsubscribe(signal)
    sleep(2.0)

    return(None)


def acc_setup_stream(state, sensor_config) -> None:

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


def acc_stop_stream(state) -> None:
    
    # Stop acc
    libmetawear.mbl_mw_acc_stop(state.device.board)
    libmetawear.mbl_mw_acc_disable_acceleration_sampling(state.device.board)
    
    # Unsubscribe
    signal = libmetawear.mbl_mw_acc_get_acceleration_data_signal(state.device.board)
    libmetawear.mbl_mw_datasignal_unsubscribe(signal)

    return(None)

def gyro_bmi270_setup_stream(state, sensor_config) -> None:

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


def gyro_bmi270_stop_stream(state) -> None:
    
    # Stop
    libmetawear.mbl_mw_gyro_bmi270_stop(state.device.board)
    libmetawear.mbl_mw_gyro_bmi270_disable_rotation_sampling(state.device.board)
    
    # Unsubscribe
    gyro = libmetawear.mbl_mw_gyro_bmi270_get_rotation_data_signal(state.device.board)
    libmetawear.mbl_mw_datasignal_unsubscribe(gyro)

    return(None)
    