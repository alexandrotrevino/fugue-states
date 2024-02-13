# Abstractions to configure device sensors

# Imports
from mbientlab.metawear import MetaWear, libmetawear, create_voidp, create_voidp_int
from mbientlab.metawear.cbindings import *
from threading import Event
from time import sleep
from typing import Callable

def start_sensor_stream(sensor_name) -> Callable:
    d = {
        "Accelerometer": acc_setup_stream,
        "Gyroscope": gyro_bmi270_setup_stream,
        "Gyroscope160": gyro_bmi160_setup_stream,
        "Magnetometer": mag_setup_stream,
        "Temperature": temp_setup_stream,
        "Ambient Light": light_setup_stream,
        "Sensor Fusion": sensor_fusion_setup_stream
    }
    return(d[sensor_name])

def stop_sensor_stream(sensor_name) -> Callable:
    d = {
        "Accelerometer": acc_stop_stream,
        "Gyroscope": gyro_bmi270_stop_stream,
        "Gyroscope160": gyro_bmi160_stop_stream,
        "Magnetometer": mag_stop_stream,
        "Temperature": temp_stop_stream,
        "Ambient Light": light_stop_stream,
        "Sensor Fusion": sensor_fusion_stop_stream
    }
    return(d[sensor_name])

def closest(n, l):
    return(min(l, key=lambda x:abs(x - n)))

def light_setup_stream(state, sensor_config) -> None:

    # Configuration
    gain = sensor_config["Ambient Light"]["gain"]
    integration_time = sensor_config["Ambient Light"]["integration_time"]
    measurement_rate = sensor_config["Ambient Light"]["odr"]
    
    gains = {
        1: AlsLtr329Gain._1X,
        2: AlsLtr329Gain._2X,
        4: AlsLtr329Gain._4X,
        8: AlsLtr329Gain._8X,
        48: AlsLtr329Gain._48X,
        96: AlsLtr329Gain._96X
    }

    gain = closest(gain, [1,2,4,8,48,96])
    gain_call = gains[gain]

    integration_times = {
        50: AlsLtr329IntegrationTime._50ms,
        100: AlsLtr329IntegrationTime._100ms,
        150: AlsLtr329IntegrationTime._150ms,
        200: AlsLtr329IntegrationTime._200ms,
        250: AlsLtr329IntegrationTime._250ms,
        300: AlsLtr329IntegrationTime._300ms,
        350: AlsLtr329IntegrationTime._350ms,
        400: AlsLtr329IntegrationTime._400ms
    }

    integration_time = closest(integration_time, [50,100,150,200,250,300,350,400])
    integration_time_call = integration_times[integration_time]

    measurement_rates = {
        50: AlsLtr329MeasurementRate._50ms,
        100: AlsLtr329MeasurementRate._100ms,
        200: AlsLtr329MeasurementRate._200ms,
        500: AlsLtr329MeasurementRate._500ms,
        1000: AlsLtr329MeasurementRate._1000ms,
        2000: AlsLtr329MeasurementRate._2000ms
    }

    measurement_rate = closest(measurement_rate, [50,100,200,500,1000,2000])
    measurement_rate_call = measurement_rates[measurement_rate]

    libmetawear.mbl_mw_als_ltr329_set_gain(state.device.board, gain_call)
    libmetawear.mbl_mw_als_ltr329_set_integration_time(state.device.board, integration_time_call)
    libmetawear.mbl_mw_als_ltr329_set_measurement_rate(state.device.board, measurement_rate_call)
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
        mode = sensor_config["Sensor Fusion"]["mode"].lower()
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
    
    acc_range = min([2,4,8,16], key=lambda x:abs(x - acc_range))
    gyro_range = min([250,500,1000,2000], key=lambda x:abs(x - gyro_range))

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
    odr = sensor_config["Magnetometer"]["odr"]
    mag_odrs = {
        2: MagBmm150Odr._2Hz,
        6: MagBmm150Odr._6Hz,
        8: MagBmm150Odr._8Hz,
        10: MagBmm150Odr._10Hz,
        15: MagBmm150Odr._15Hz,
        20: MagBmm150Odr._20Hz,
        25: MagBmm150Odr._25Hz,
        30: MagBmm150Odr._30Hz
    }
    
    odr = min([2,6,8,10,15,20,25,30], key=lambda x:abs(x - odr))
    odr_call = mag_odrs[odr]
    # Setup mag
    # Note: Not sure why the middle 2 arguments exist or what they are. Found it in the MetaBase repo.
    libmetawear.mbl_mw_mag_bmm150_configure(state.device.board, 9, 15, odr_call) 

    # Get mag and subscribe
    signal = libmetawear.mbl_mw_mag_bmm150_get_b_field_data_signal(state.device.board)
    libmetawear.mbl_mw_datasignal_subscribe(signal, None, state.mag_callback)
    
    # Start mag
    libmetawear.mbl_mw_mag_bmm150_enable_b_field_sampling(state.device.board)
    libmetawear.mbl_mw_mag_bmm150_start(state.device.board)
    return(None)

def mag_stop_stream(state, sensor_config=None) -> None:

    # Stop mag
    libmetawear.mbl_mw_mag_bmm150_stop(state.device.board)
    libmetawear.mbl_mw_mag_bmm150_disable_b_field_sampling(state.device.board)
    
    # Unsubscribe
    signal = libmetawear.mbl_mw_mag_bmm150_get_b_field_data_signal(state.device.board)
    libmetawear.mbl_mw_datasignal_unsubscribe(signal)

    return(None)

def temp_setup_stream(state, sensor_config) -> None:

    # e = Event()
    # period = sensor_config["Temperature"]["period"]
    # signal = libmetawear.mbl_mw_multi_chnl_temp_get_temperature_data_signal(state.device.board, MetaWearRProChannel.ON_BOARD_THERMISTOR)
    
    # # subscribe to temp signal
    # libmetawear.mbl_mw_datasignal_subscribe(signal, None, state.temp_callback)

    # # create timer
    # timer = create_voidp(lambda fn: libmetawear.mbl_mw_timer_create_indefinite(state.device.board, period, 0, None, fn), resource = "timer", event = e)
        
    # # create event based on timer - read temp when timer fires
    # libmetawear.mbl_mw_event_record_commands(timer)
    # libmetawear.mbl_mw_datasignal_read(signal)
    # create_voidp_int(lambda fn: libmetawear.mbl_mw_event_end_record(timer, None, fn), event = e)

    # # start timer
    # libmetawear.mbl_mw_timer_start(timer)
    # state.timer = timer

    return(None)

def temp_stop_stream(state, sensor_config=None) -> None:
    
    # remove timer
    # timer = state.timer
    # libmetawear.mbl_mw_timer_remove(timer)
    # sleep(1.0)

    # # remove event
    # libmetawear.mbl_mw_event_remove_all(state.device.board)
    # sleep(1.0)

    # # unsubscribe
    # signal = libmetawear.mbl_mw_multi_chnl_temp_get_temperature_data_signal(state.device.board, MetaWearRProChannel.ON_BOARD_THERMISTOR)
    # libmetawear.mbl_mw_datasignal_unsubscribe(signal)
    # sleep(2.0)

    return(None)


def acc_setup_stream(state, sensor_config) -> None:

    # Import parameters from configuration
    odr = int(sensor_config["Accelerometer"]["odr"])
    acc_range = float(sensor_config["Accelerometer"]["range"])
    #threshold = None
    
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


def acc_stop_stream(state, sensor_config=None) -> None:
    
    # Stop acc
    libmetawear.mbl_mw_acc_stop(state.device.board)
    libmetawear.mbl_mw_acc_disable_acceleration_sampling(state.device.board)
    
    # Unsubscribe
    signal = libmetawear.mbl_mw_acc_get_acceleration_data_signal(state.device.board)
    libmetawear.mbl_mw_datasignal_unsubscribe(signal)

    return(None)

def gyro_bmi270_setup_stream(state, sensor_config) -> None:

    # Ranges
    gyro_ranges = {
        125: GyroBoschRange._125dps,
        250: GyroBoschRange._250dps,
        500: GyroBoschRange._500dps,
        1000: GyroBoschRange._1000dps,
        2000: GyroBoschRange._2000dps
    }
    
    #ODRs
    gyro_odrs = {
        25: GyroBoschOdr._25Hz,
        50: GyroBoschOdr._50Hz,
        100: GyroBoschOdr._100Hz,
        200: GyroBoschOdr._200Hz,
        400: GyroBoschOdr._400Hz,
        800: GyroBoschOdr._800Hz,
        1600: GyroBoschOdr._1600Hz,
        3200: GyroBoschOdr._3200Hz
    }

    # Import parameters from configuration
    odr = sensor_config["Gyroscope"]["odr"]
    gyro_range = sensor_config["Gyroscope"]["range"]

    odr = min([25,50,100,200,400,800,1600,3200], key=lambda x:abs(x - odr))
    gyro_range = min([250,500,1000,2000], key=lambda x:abs(x - gyro_range))
    
    gyro_range_call = gyro_ranges[gyro_range]
    gyro_odr_call = gyro_odrs[odr]

    # Setup gyro
    libmetawear.mbl_mw_gyro_bmi270_set_odr(state.device.board, gyro_odr_call)
    libmetawear.mbl_mw_gyro_bmi270_set_range(state.device.board, gyro_range_call)
    libmetawear.mbl_mw_gyro_bmi270_write_config(state.device.board)
    
    # Get gyro and subscribe
    gyro = libmetawear.mbl_mw_gyro_bmi270_get_rotation_data_signal(state.device.board)
    libmetawear.mbl_mw_datasignal_subscribe(gyro, None, state.gyro_callback)

    # Start gyro
    libmetawear.mbl_mw_gyro_bmi270_enable_rotation_sampling(state.device.board)
    libmetawear.mbl_mw_gyro_bmi270_start(state.device.board)

    return(None)


def gyro_bmi270_stop_stream(state, sensor_config=None) -> None:
    
    # Stop
    libmetawear.mbl_mw_gyro_bmi270_stop(state.device.board)
    libmetawear.mbl_mw_gyro_bmi270_disable_rotation_sampling(state.device.board)
    
    # Unsubscribe
    gyro = libmetawear.mbl_mw_gyro_bmi270_get_rotation_data_signal(state.device.board)
    libmetawear.mbl_mw_datasignal_unsubscribe(gyro)

    return(None)
    

def gyro_bmi160_setup_stream(state, sensor_config) -> None:

    #ODRs
    gyro_odrs = {
        25: GyroBoschOdr._25Hz,
        50: GyroBoschOdr._50Hz,
        100: GyroBoschOdr._100Hz,
        200: GyroBoschOdr._200Hz,
        400: GyroBoschOdr._400Hz,
        800: GyroBoschOdr._800Hz,
        1600: GyroBoschOdr._1600Hz,
        3200: GyroBoschOdr._3200Hz
    }
    
    gyro_odr = sensor_config["Gyroscope"]["odr"]
    gyro_odr = closest(gyro_odr, [25,50,100,200,400,800,1600,3200])
    gyro_odr_call = gyro_odrs[gyro_odr]

    # Ranges
    gyro_ranges = {
        150: GyroBoschRange._125dps,
        250: GyroBoschRange._250dps,
        500: GyroBoschRange._500dps,
        1000: GyroBoschRange._1000dps,
        2000: GyroBoschRange._2000dps
    }
    
    gyro_range = sensor_config["Gyroscope"]["range"]
    gyro_range = closest(gyro_range, [250,500,1000,2000])
    gyro_range_call = gyro_ranges[gyro_range]
    
    # Setup gyro
    libmetawear.mbl_mw_gyro_bmi160_set_odr(state.device.board, gyro_odr_call)
    libmetawear.mbl_mw_gyro_bmi160_set_range(state.device.board, gyro_range_call)
    libmetawear.mbl_mw_gyro_bmi160_write_config(state.device.board)
    
    # Get gyro and subscribe
    gyro = libmetawear.mbl_mw_gyro_bmi160_get_rotation_data_signal(state.device.board)
    libmetawear.mbl_mw_datasignal_subscribe(gyro, None, state.gyro_callback)

    # Start gyro
    libmetawear.mbl_mw_gyro_bmi160_enable_rotation_sampling(state.device.board)
    libmetawear.mbl_mw_gyro_bmi160_start(state.device.board)

    return(None)


def gyro_bmi160_stop_stream(state, sensor_config=None) -> None:
    
    # Stop
    libmetawear.mbl_mw_gyro_bmi160_stop(state.device.board)
    libmetawear.mbl_mw_gyro_bmi160_disable_rotation_sampling(state.device.board)
    
    # Unsubscribe
    gyro = libmetawear.mbl_mw_gyro_bmi160_get_rotation_data_signal(state.device.board)
    libmetawear.mbl_mw_datasignal_unsubscribe(gyro)

    return(None)