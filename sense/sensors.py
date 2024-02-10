# Abstractions to configure device sensors

# Imports
from mbientlab.metawear import MetaWear, libmetawear, create_voidp, create_voidp_int
from mbientlab.metawear.cbindings import *
from fs_setup import State, retrieve_default_settings
from threading import Event
from time import sleep

def start_sensor_stream(sensor_name) -> function:
    d = {
        "Accelerometer": acc_setup_stream,
        "Gyroscope": gyro_bmi270_setup_stream,
        "Gyroscope160": gyro_bmi160_setup_stream,
        "Magnometer": mag_setup_stream,
        "Temperature": temp_setup_stream,
        "Ambient Light": light_setup_stream,
        "Sensor Fusion": sensor_fusion_setup_stream,
    }
    # if sensor_name not in d.keys():
    #     raise SensorConfigError
    return(d[sensor_name])

def stop_sensor_stream(sensor_name) -> function:
    d = {
        "Accelerometer": acc_stop_stream,
        "Gyroscope": gyro_bmi270_stop_stream,
        "Gyroscope160": gyro_bmi160_stop_stream,
        "Magnometer": mag_stop_stream,
        "Temperature": temp_stop_stream,
        "Ambient Light": light_stop_stream,
        "Sensor Fusion": sensor_fusion_stop_stream
    }
    return(d[sensor_name])

def light_setup_stream(state, sensor_config) -> None:

    # Configuration
    gain = sensor_config["Ambient Light"]["gain"]
    integration_time = sensor_config["Ambient Light"]["integration_time"]
    measurement_rate = sensor_config["Ambient Light"]["odr"]

    libmetawear.mbl_mw_als_ltr329_set_gain(state.device.board, gain)
    libmetawear.mbl_mw_als_ltr329_set_integration_time(state.device.board, integration_time)
    libmetawear.mbl_mw_als_ltr329_set_measurement_rate(state.device.board, measurement_rate)
    libmetawear.mbl_mw_als_ltr329_write_config(state.device.board)

    # Get data signal
    signal = libmetawear.mbl_mw_als_ltr329_get_illuminance_data_signal(state)
    libmetawear.mbl_mw_datasignal_subscribe(signal, None, state.light_callback)
    libmetawear.mbl_mw_als_ltr329_start(state.device.board)
    
    return(None)

def light_stop_stream(state, sensor_config) -> None:
    libmetawear.mbl_mw_als_ltr329_stop(state.device.board)
    signal = libmetawear.mbl_mw_als_ltr329_get_illuminance_data_signal(state.device.board)
    libmetawear.mbl_mw_datasignal_unsubscribe(signal)

    return(None)

def sensor_fusion_setup_stream(state, sensor_config) -> None:
    
    # Configurations ---
    # Sensor modes
    modes = {
        "ndof": SensorFusionMode.NDOF,
        "imuplus": SensorFusionMode.IMU_PLUS,
        "compass": SensorFusionMode.COMPASS,
        "m4g": SensorFusionMode.M4G
    }

    try:
        mode = sensor_config["Quaternion"]["mode"].lower()
    except KeyError:
        mode = "ndof"

    mode_call = modes[mode]

    # Ranges
    acc_ranges = {
        2: SensorFusionAccRange._2G,
        4: SensorFusionAccRange._4G,
        8: SensorFusionAccRange._8G,
        16: SensorFusionAccRange._16G
    }

    gyro_ranges = {
        250: SensorFusionGyroRange._250DPS,
        500: SensorFusionGyroRange._500DPS,
        1000: SensorFusionGyroRange._1000DPS,
        2000: SensorFusionGyroRange._2000DPS
    }

    acc_range = float(sensor_config["Sensor Fusion"]["accRange"])
    gyro_range = float(sensor_config["Sensor Fusion"]["gyroRange"])
    
    acc_range = min([2,4,8,16], key=lambda x:abs(x-acc_range))
    gyro_range = min([250,500,1000,2000], key=lambda x:abs(x-gyro_range))

    acc_range_call = acc_ranges[acc_range]
    gyro_range_call = gyro_ranges[gyro_range]

    # Outputs
    outputs = {
        "quaternion": SensorFusionData.QUATERNION,
        "euler_angle": SensorFusionData.EULER_ANGLE,
        "linear_acc": SensorFusionData.LINEAR_ACC,
        "gravity": SensorFusionData.GRAVITY_VECTOR,
        "corrected_acc": SensorFusionData.CORRECTED_ACC,
        "corrected_gyro": SensorFusionData.CORRECTED_GYRO,
        "corrected_mag": SensorFusionData.CORRECTED_MAG
    }

    output = sensor_config["Sensor Fusion"]["output"].lower()
    output_call = outputs[output]

    # Callback Selection
    callback_functions = {
        "quaternion": state.quat_callback,
        "euler_angle": state.euler_callback,
        "linear_acc": state.linear_acc_callback,
        "gravity": state.gravity_callback,
        "corrected_acc": state.corrected_acc_callback,
        "corrected_gyro": state.corrected_gyro_callback,
        "corrected_mag": state.corrected_mag_callback
    }

    callback = callback_functions[output]

    # Setup Stream ---
    libmetawear.mbl_mw_sensor_fusion_set_mode(state.device.board, mode_call)
    libmetawear.mbl_mw_sensor_fusion_set_acc_range(state.device.board, acc_range_call)
    libmetawear.mbl_mw_sensor_fusion_set_gyro_range(state.device.board, gyro_range_call)
    libmetawear.mbl_mw_sensor_fusion_write_config(state.device.board)

    # get quat signal and subscribe
    signal = libmetawear.mbl_mw_sensor_fusion_get_data_signal(state.device.board, output_call)
    libmetawear.mbl_mw_datasignal_subscribe(signal, None, callback)

    # start acc, gyro, mag
    libmetawear.mbl_mw_sensor_fusion_enable_data(state.device.board, output_call)
    libmetawear.mbl_mw_sensor_fusion_start(state.device.board)

def sensor_fusion_stop_stream(state, sensor_config) -> None:
    
    # Outputs
    outputs = {
        "quaternion": SensorFusionData.QUATERNION,
        "euler_angle": SensorFusionData.EULER_ANGLE,
        "linear_acc": SensorFusionData.LINEAR_ACC,
        "gravity": SensorFusionData.GRAVITY_VECTOR,
        "corrected_acc": SensorFusionData.CORRECTED_ACC,
        "corrected_gyro": SensorFusionData.CORRECTED_GYRO,
        "corrected_mag": SensorFusionData.CORRECTED_MAG
    }

    output = sensor_config["Sensor Fusion"]["output"].lower()
    output_call = outputs[output]

    # stop
    libmetawear.mbl_mw_sensor_fusion_stop(state.device.board)

    # unsubscribe to signal
    signal = libmetawear.mbl_mw_sensor_fusion_get_data_signal(state.device.board, output_call)
    libmetawear.mbl_mw_datasignal_unsubscribe(signal)

    
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

def temp_setup_stream(state, sensor_config) -> None:

    e = Event()
    period = sensor_config["Temperature"]["period"]
    signal = libmetawear.mbl_mw_multi_chnl_temp_get_temperature_data_signal(state.device.board, MetaWearRProChannel.ON_BOARD_THERMISTOR)
    
    # subscribe to temp signal
    libmetawear.mbl_mw_datasignal_subscribe(signal, None, state.temp_callback)

    # create timer
    timer = create_voidp(lambda fn: libmetawear.mbl_mw_timer_create_indefinite(state.device.board, period, 0, None, fn), resource = "timer", event = e)
        
    # create event based on timer - read temp when timer fires
    libmetawear.mbl_mw_event_record_commands(timer)
    libmetawear.mbl_mw_datasignal_read(signal)
    create_voidp_int(lambda fn: libmetawear.mbl_mw_event_end_record(timer, None, fn), event = e)

    # start timer
    libmetawear.mbl_mw_timer_start(timer)
    state.timer = timer

    return(None)

def temp_stop_stream(state, sensor_config) -> None:
    
    # remove timer
    timer = state.timer
    libmetawear.mbl_mw_timer_remove(timer)
    sleep(1.0)

    # remove event
    libmetawear.mbl_mw_event_remove_all(state.device.board)
    sleep(1.0)

    # unsubscribe
    signal = libmetawear.mbl_mw_multi_chnl_temp_get_temperature_data_signal(state.device.board, MetaWearRProChannel.ON_BOARD_THERMISTOR)
    libmetawear.mbl_mw_datasignal_unsubscribe(signal)
    sleep(2.0)

    return(None)


def acc_setup_stream(state, sensor_config) -> None:

    # Import parameters from configuration
    try:
        odr = int(sensor_config["Accelerometer"]["odr"])
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
    

def gyro_bmi160_setup_stream(state, sensor_config) -> None:

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
    libmetawear.mbl_mw_gyro_bmi160_set_odr(state.device.board, odr)
    libmetawear.mbl_mw_gyro_bmi160_set_range(state.device.board, gyro_range)
    libmetawear.mbl_mw_gyro_bmi160_write_config(state.device.board)
    
    # Get gyro and subscribe
    gyro = libmetawear.mbl_mw_gyro_bmi160_get_rotation_data_signal(state.device.board)
    libmetawear.mbl_mw_datasignal_subscribe(gyro, None, state.gyro_callback)

    # Start gyro
    libmetawear.mbl_mw_gyro_bmi160_enable_rotation_sampling(state.device.board)
    libmetawear.mbl_mw_gyro_bmi160_start(state.device.board)

    return(None)


def gyro_bmi160_stop_stream(state) -> None:
    
    # Stop
    libmetawear.mbl_mw_gyro_bmi160_stop(state.device.board)
    libmetawear.mbl_mw_gyro_bmi160_disable_rotation_sampling(state.device.board)
    
    # Unsubscribe
    gyro = libmetawear.mbl_mw_gyro_bmi160_get_rotation_data_signal(state.device.board)
    libmetawear.mbl_mw_datasignal_unsubscribe(gyro)

    return(None)