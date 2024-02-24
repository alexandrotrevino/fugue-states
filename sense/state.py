from mbientlab.metawear import MetaWear, libmetawear, parse_value, create_voidp, create_voidp_int
from mbientlab.metawear.cbindings import *
from time import sleep
import threading
from pythonosc import osc_server
from pythonosc import dispatcher
from pythonosc import udp_client
from .sensors import start_sensor_stream, stop_sensor_stream
from .fs_setup import validate_device_config, validate_network_config

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
    :param str config: A dictionary of settings, or a file path. 
    """

    def __init__(self, device_config, network_config):
        """
        Constructor.
        state = MetaWearState(device_config, sensor_config)
        """

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
        
        # - Parse
        self.address = device_config["mac"]
        self.model = device_config["name"]
        self.ble = device_config["ble"]
        self.sensor_config = device_config["sensors"]

        self.ip = network_config["ip"]
        self.port = network_config["port"]

        # Device (and device.board)
        self.device = None
        self._streaming = False

        # Diagnostic
        self.logger = {"acc": 0, "gyro": 0, "mag": 0, "temp": 0, "light": 0, "fusion": 0} 

        # OSC
        # - Server
        #   - Handlers
        def default_handler(address, *args):
            print(f"[default] {address}: {args}")
        
        def state_handler(address, *args):
            arg = args[0]
            if not self._streaming:
                if arg == 0:
                    pass
                if arg == 1:
                    print("Initializing stream")
                    #self.connect()
                    #self.start_sensors(self, self.sensor_config)
                    self._streaming = True
            elif self._streaming:
                if arg == 0:
                    print("Stopping stream")
                    #self.stop_sensors(self, self.sensor_config)
                    #self.disconnect()
                if arg == 1:
                    pass
        
        #   - Dispatcher
        osc_dispatcher = dispatcher.Dispatcher()
        osc_dispatcher.set_default_handler(default_handler)
        osc_dispatcher.map("/stream", state_handler)

        self._osc_server = osc_server.ThreadingOSCUDPServer(("0.0.0.0", 8001), dispatcher) # fixed listening port, for now
        
        # - Client
        self._osc_client = udp_client.SimpleUDPClient(self.ip, self.port)

    

    def connect(self) -> None:

        print("Connecting", self.address)
        self.device = MetaWear(self.address, hci_mac = self.ble)
        self.device.connect()

        print("> Connected to " + self.address + " over " + "BLE")

        # Setup BLE
        print("> Configuring", self.address)
        libmetawear.mbl_mw_settings_set_connection_parameters(self.device.board, 7.5, 7.5, 0, 6000)
        sleep(1.0)
        
        # Notify 
        self._osc_client.send_message("/ble_status", 1)
        return(None)

    def disconnect(self) -> None:

        libmetawear.mbl_mw_debug_disconnect(self.device.board)
        self._osc_client.send_message("/ble_status", 0)
        return(None)

    def start_sensors(self, sensor_config) -> None:
        sensor_str = "\n  - ".join(sensor_config.keys())
        print(f"Starting sensors:\n{sensor_str}")

        for sensor in sensor_config.keys():
            start_sensor_stream(sensor)(self, sensor_config)

        return(None)

    def stop_sensors(self, sensor_config) -> None:
        sensor_str = "\n  - ".join(sensor_config.keys())
        print(f"Stopping sensors:\n{sensor_str}")
        for sensor in sensor_config.keys():
            stop_sensor_stream(sensor)(self, sensor_config)

        return(None)    # start stream, stop stream
