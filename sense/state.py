from mbientlab.metawear import MetaWear, libmetawear, parse_value, create_voidp, create_voidp_int
from mbientlab.metawear.cbindings import *
from time import sleep

# Client class

class MetaWearState:
    """
    A class that holds facilitates connection, configuration, and
    communication for MetaWear devices.

    The State provides an interface between Python and the device API,
    with methods to configure the board, BLE connection, and sensors.
    It can also log data samples that come through the board. 

    See also: MetaWear class, MetaWear C++ API, and the PyMetaWear package.

    :param str address: The MAC address of the MetaWear device
    :param str ble: The MAC address (or hci designation, e.g. `hci0`) of the Bluetooth device
    :param str config: A dictionary of settings, or a file path. 
    """

    def __init__(self, device_config, sensor_config, ble="hci0"):
        """
        Constructor.
        state = MetaWearState(address = "XX:XX:XX:XX:XX:XX",
                              sensor_config = {Accelerometer})
        """

        # Configs
        self._address = device_config["mac"]
        self._model = device_config["name"]
        self._ble = device_config["ble"]
        self._sensor_config = sensor_config

        # Device (and device.board)
        self.device = None

        # Diagnostic
        self.logger = {"acc": 0, "gyro": 0, "mag": 0, "temp": 0, "light": 0, "fusion": 0} 
        
    def connect(self) -> None:
        
        print("Connecting", self._address)
        self.device = MetaWear(self._address, hci_mac = self._ble)
        self.device.connect()

        print("> Connected to " + self._address + " over " + "BLE")
           
        # Setup BLE
        print("> Configuring", self._address)
        libmetawear.mbl_mw_settings_set_connection_parameters(self.device.board, 7.5, 7.5, 0, 6000)
        sleep(1.0)
        return(None)

    def disconnect(self) -> None:

        libmetawear.mbl_mw_debug_disconnect(self.device.board)
        return(None)

    def start_sensors(state, sensor_config) -> None:
        sensor_str = "\n  - ".join(sensor_config.keys())
        print(f"Starting sensors: {sensor_str}")

        for sensor in sensor_config.keys():
            start_sensor_stream(sensor)(state, sensor_config)
        
        return(None)

    def stop_sensors(state, sensor_config) -> None:
        sensor_str = "\n  - ".join(sensor_config.keys())
        print(f"Stopping sensors: {sensor_str}")
        for sensor in sensor_config.keys():
            stop_sensor_stream(sensor)(state, sensor_config)

        return(None)    # start stream, stop stream

    # 