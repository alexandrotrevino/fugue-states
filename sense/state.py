from mbientlab.metawear import MetaWear, libmetawear, parse_value
from mbientlab.metawear.cbindings import *
from time import sleep
from .sensors import start_sensor_stream, stop_sensor_stream
from .fs_setup import validate_device_config, validate_network_config
from .osc import ControlledOSCConnection

# Client class

class MetaWearState:
    """
    A class that holds facilitates connection, configuration, and
    communication for MetaWear devices.

    The State provides an interface between Python and the device API,
    with methods to configure the board, BLE connection, and sensors.
    It also sets up an OSC server and client to send and receive data
    from a remote source (PlugData). 

    See also: MetaWear class, MetaWear C++ API, and the PyMetaWear package.

    :param dict device_config: Configuration settings for this device.
    :param dict network_config: Configuration settings for streaming data.
    :param OSC ControlledOSCServer: A dictionary of settings, or a file path. 
    """

    def __init__(self, device_config, network_config, OSC):
        """
        Constructor.
        state = MetaWearState(device_config, sensor_config)
        """

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
        self.corrected_acc_callback = FnVoid_VoidP_DataP(self.corrected_acc_data_handler)
        self.corrected_gyro_callback = FnVoid_VoidP_DataP(self.corrected_gyro_data_handler)
        self.corrected_mag_callback = FnVoid_VoidP_DataP(self.corrected_mag_data_handler)

        # Configs
        # - Validate
        device_config = validate_device_config(device_config)
        network_config = validate_network_config(network_config)

        try:
            assert device_config["valid"]
        except AssertionError:
            print("Invalid device config")
            exit(1)

        try:
            assert network_config["valid"]
        except AssertionError:
            print("Invalid network config")
            exit(1)

        self.valid_config = True
        
        # - Parse
        self.address = device_config["mac"]
        self.model = device_config["name"]
        self.ble = device_config["ble"]
        self.sensor_config = device_config["sensors"]

        self.ip = network_config["ip"]
        self.port = network_config["port"]

        # Device (and device.board)
        self.device = None
        self.connected = False
        self.streaming = False
        self.fusion_mode = device_config["fusion_mode"]
        
        # Diagnostic
        self.logger = {"acc": 0, "gyro": 0, "mag": 0, "temp": 0, "light": 0, "fusion": 0} 

        # OSC
        self.OSC = None
        self.set_OSC(OSC)

    # [ end __init__ ]
    # 
        # TODO - a function to re-check configuration after remote change.
    # Bluetooth Device Connection ----
                
    def connect(self) -> None:

        print("Connecting", self.address)
        self.device = MetaWear(self.address, hci_mac = self.ble)
        self.device.connect()
        self.connected = True

        print("> Connected to " + self.address + " over " + "BLE")
        self._osc_client.send_message("/indicator/conf", 1)
        self._osc_client.send_message("/indicator/dev", 1)
        

        # Setup BLE
        print("> Configuring", self.address)
        libmetawear.mbl_mw_settings_set_connection_parameters(self.device.board, 7.5, 7.5, 0, 6000)
        sleep(1.0)
        
        # Notify 
        self._osc_client.send_message("/indicator/ble", 1)
        return(None)

    def disconnect(self) -> None:

        libmetawear.mbl_mw_debug_disconnect(self.device.board)
        self._osc_client.send_message("/indicator/ble", 0)
        self._osc_client.send_message("/indicator/dev", 0)
        self.connected = False
        return(None)
    
    # OSC ----
    # Maybe this should go in a separate module / file?
    def set_OSC(self, OSC):
        
        if self.OSC is not None:
            self.OSC.stop_server()
        
        self.OSC = OSC

        self._osc_client = self.OSC.client
        self._osc_server = self.OSC.server

        # - Handlers
        def default_handler(address, *args):
            print(f"[default] {address}: {args}")
        
        def stop_server_handler(address, *args):
            print("Stopping")
            self.OSC.stop_server()

        def start_stream_handler(address, *args):
            if not self.streaming:
                print(f"Received start message {address} (arg {args})")
                self.start_sensors(sensor_config=self.sensor_config)
            else:
                print("Streaming is started!")

        def stop_stream_handler(address, *args):
            if self.streaming:
                print(f"Received stop message {address} (arg {args})")
                self.stop_sensors(sensor_config=self.sensor_config)
            else:
                print("Streaming is stopped!")
            

        def sensor_config_handler(address, *args):
            print(f"Received new sensor configuration {address}")
            if self.streaming:
                print(f"Streaming in progress - sensor config changes will be ignored.")
            
        def network_config_handler(address, *args):
            if self.streaming:
                print(f"Streaming in progress - network config changes will be ignored.")
            print(f"Received new network configuration {address}")

        def ready_check_handler(address, *args):
            print(f"Received readiness signal at {address}")
            if self.streaming:
                return(None)
            if self.valid_config and self.ip is not None and self.port is not None:
                self._osc_client.send_message("/indicator/conf", 1)
            if self.connected:
                self._osc_client.send_message("/indicator/dev", 1)
                self._osc_client.send_message("/indicator/ble", 1)
            if self.fusion_mode is not None:
                d = {
                    "euler_angle": 0,
                    "quaternion": 1,
                    "gravity": 2,
                    "linear_acc": 3
                }
                self._osc_client.send_message("/indicator/fusion", d[self.fusion_mode.lower()])


        self._osc_server.dispatcher.map("/stop_server", stop_server_handler)
        self._osc_server.dispatcher.map("/start_stream", start_stream_handler)
        self._osc_server.dispatcher.map("/stop_stream", stop_stream_handler)
        self._osc_server.dispatcher.map("/sensors", sensor_config_handler)
        self._osc_server.dispatcher.map("/network", network_config_handler)
        self._osc_server.dispatcher.map("/ready", ready_check_handler)
        self._osc_server.dispatcher.set_default_handler(default_handler)
        
        self.OSC.start_server()

    
    # Sensors ----
    # - High level functions

    def start_sensors(self, sensor_config) -> None:
        if not self.connected:
            self.connect()
        
        sensor_str = "\n  - ".join(sensor_config.keys())
        print(f"Starting sensors:\n{sensor_str}")

        for sensor in sensor_config.keys():
            start_sensor_stream(sensor)(self, sensor_config)

        self.streaming = True
        return(None)

    def stop_sensors(self, sensor_config) -> None:
        sensor_str = "\n  - ".join(sensor_config.keys())
        print(f"Stopping sensors:\n{sensor_str}")
        for sensor in sensor_config.keys():
            stop_sensor_stream(sensor)(self, sensor_config)

        self.streaming = False
        return(None)    # start stream, stop stream
    
    # - Callbacks

    def acc_data_handler(self, ctx, data):
        """
        Accelerometer data are expressed in terms of 'g' along the [x, y, z] direction.
        """
        parsed_data = parse_value(data)
        mac = self.device.address
        self._osc_client.send_message(f"/{mac}/acc", (parsed_data.x, parsed_data.y, parsed_data.z))
        self.logger["acc"] += 1

    def gyro_data_handler(self, ctx, data):
        """
        Gyrometer data are expressed in terms of degrees of rotation around the [x, y, z] axis.
        """
        parsed_data = parse_value(data)
        mac = self.device.address
        self._osc_client.send_message(f"/{mac}/gyro", (parsed_data.x, parsed_data.y, parsed_data.z))
        self.logger["gyro"] += 1

    def mag_data_handler(self, ctx, data):
        """
        Magnetometer data are given in terms of the h-component for geomagnetic north,
        the d-component for east, and the z-component for vertical direction. 
        Components are expressed in nano Tesla (nT).
        """
        parsed_data = parse_value(data)
        mac = self.device.address
        self._osc_client.send_message(f"/{mac}/mag", (parsed_data.x, parsed_data.y, parsed_data.z))
        self.logger["mag"] += 1
    
    def temp_data_handler(self, ctx, data):
        """
        Temperature data are expressed in degrees Celsius. 
        """
        temperature = parse_value(data)
        mac = self.device.address
        self._osc_client.send_message(f"/{mac}/temp", (temperature))
        self.logger["temp"] += 1

    def light_data_handler(self, ctx, data):
        """
        Ambient light data are expressed in lux units and the device is sensitive from 0.1-64k lux. 
        """
        light = parse_value(data)
        mac = self.device.address
        self._osc_client.send_message(f"/{mac}/light", light)
        self.logger["light"] += 1
    
    def quat_data_handler(self, ctx, data):
        """
        Quaternion data give the relative orientation of the device as a unit spatial
        quaternion. This is computed by sensor fusion onboard the MetaWear device.

        Accelerometer and gyrometer data should *not* be used in tandem with sensor
        fusion data.
        """
        parsed_data = parse_value(data)
        mac = self.device.address
        self._osc_client.send_message(f"/{mac}/quat", (parsed_data.w, parsed_data.x, parsed_data.y, parsed_data.z))
        self.logger["fusion"] += 1

    def euler_data_handler(self, ctx, data):
        """
        Euler angles provide the relative orientation of the device as computed from both
        accelerometer and gyrometer data together. This is computed by sensor fusion
        onboard the MetaWear device.

        Accelerometer and gyrometer data should *not* be used in tandem with sensor
        fusion data.
        """
        parsed_data = parse_value(data)
        mac = self.device.address
        self._osc_client.send_message(f"/{mac}/euler", (parsed_data.heading, parsed_data.pitch, parsed_data.roll, parsed_data.yaw))
        self.logger["fusion"] += 1

    def linear_acc_data_handler(self, ctx, data):
        parsed_data = parse_value(data)
        mac = self.device.address
        self._osc_client.send_message(f"/{mac}/linear_acc", (parsed_data.x, parsed_data.y, parsed_data.z))
        self.logger["fusion"] += 1

    def gravity_data_handler(self, ctx, data):
        parsed_data = parse_value(data)
        mac = self.device.address
        self._osc_client.send_message(f"/{mac}/gravity", (parsed_data.x, parsed_data.y, parsed_data.z))
        self.logger["fusion"] += 1

    def corrected_acc_data_handler(self, ctx, data):
        parsed_data = parse_value(data)
        mac = self.device.address
        self._osc_client.send_message(f"/{mac}/corrected_acc", (parsed_data.x, parsed_data.y, parsed_data.z))
        self.logger["fusion"] += 1

    def corrected_gyro_data_handler(self, ctx, data):
        parsed_data = parse_value(data)
        mac = self.device.address
        self._osc_client.send_message(f"/{mac}/corrected_gyro", (parsed_data.x, parsed_data.y, parsed_data.z))
        self.logger["fusion"] += 1

    def corrected_mag_data_handler(self, ctx, data):
        parsed_data = parse_value(data)
        mac =self.device.address
        self._osc_client.send_message(f"/{mac}/corrected_mag" % self.device.address, (parsed_data.x, parsed_data.y, parsed_data.z))
        self.logger["fusion"] += 1

    # Utils ----
    def generate_sample_report(self) -> None:
        print(self.logger)

