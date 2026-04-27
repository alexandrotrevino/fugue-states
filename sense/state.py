import logging
import threading
import time
from ctypes import byref
from typing import Optional

from mbientlab.metawear import MetaWear, libmetawear, parse_value
from mbientlab.metawear.cbindings import *
from time import sleep
from .sensors import start_sensor_stream, stop_sensor_stream
from .osc import ControlledOSCConnection
from .pipeline import IMUFrame, OscEmit, Pipeline

log = logging.getLogger("fs.state")

DEFAULT_CONNECT_TIMEOUT = 10.0    # seconds; A1 — caps initial BLE handshake
DEFAULT_STALE_THRESHOLD = 5.0     # seconds without a frame → declare stale
DEFAULT_RECOVERY_BACKOFF = 5.0    # seconds between successive recovery attempts
RECONNECT_SETTLE_S = 2.0          # let the BLE adapter release before reconnecting
DOUBLE_PRESS_WINDOW_S = 0.6       # button: two presses within this window = "event"
BUTTON_LED_COLOR = LedColor.GREEN  # solid LED while streaming

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

        # Failure tracking — set from any thread when a callback raises
        # or a lifecycle step errors out. A2 policy: a failure on one
        # device aborts that device, not the whole script.
        self._failed: bool = False
        self._last_error: Optional[BaseException] = None
        self._failed_sources: list = []
        self._failure_lock = threading.Lock()

        # Idempotent-shutdown bookkeeping
        self._streaming_sensors: set = set()
        self._shutdown_done: bool = False

        # Set when a connect() call gives up (timeout / cancellation). The
        # connect worker thread checks this on its way out so a late-arriving
        # successful handshake doesn't leave a zombie connection behind.
        self._connect_aborted: bool = False

        # A5 — stale-stream detection and recovery.
        # _intended_sensors is the set of sensors the caller asked to stream.
        # It survives an under-the-hood teardown so try_recover() knows what
        # to bring back up. _streaming_sensors tracks what's *currently* live.
        self._intended_sensors: set = set()
        self._last_frame_at: float = 0.0          # monotonic; 0 = no frame yet
        self._link_lost: bool = False             # set by mbientlab on_disconnect
        self._recovering: bool = False            # re-entry guard
        self._last_recovery_attempt: float = 0.0  # monotonic
        self.stale_threshold: float = DEFAULT_STALE_THRESHOLD
        self.recovery_backoff: float = DEFAULT_RECOVERY_BACKOFF

        # Button: per-device toggle with double-press accident-proofing.
        # Subscribed at the end of connect() so it survives A5 reconnects.
        self._last_button_press_at: float = 0.0
        self._button_signal = None
        self.button_window_s: float = DOUBLE_PRESS_WINDOW_S
        self._button_callback = self._make_safe_cb(self._button_data_handler, "button")

        # Callback functions — each wrapped so a Python exception inside
        # a libmetawear C callback never propagates back through ctypes
        # (which would either be swallowed silently and corrupt the BLE
        # state machine, or crash the interpreter on some platforms).
        self.acc_callback = self._make_safe_cb(self.acc_data_handler, "acc")
        self.gyro_callback = self._make_safe_cb(self.gyro_data_handler, "gyro")
        self.mag_callback = self._make_safe_cb(self.mag_data_handler, "mag")
        self.temp_callback = self._make_safe_cb(self.temp_data_handler, "temp")
        self.light_callback = self._make_safe_cb(self.light_data_handler, "light")
        self.quat_callback = self._make_safe_cb(self.quat_data_handler, "quat")
        self.euler_callback = self._make_safe_cb(self.euler_data_handler, "euler")
        self.linear_acc_callback = self._make_safe_cb(self.linear_acc_data_handler, "linear_acc")
        self.gravity_callback = self._make_safe_cb(self.gravity_data_handler, "gravity")
        self.corrected_acc_callback = self._make_safe_cb(self.corrected_acc_data_handler, "corrected_acc")
        self.corrected_gyro_callback = self._make_safe_cb(self.corrected_gyro_data_handler, "corrected_gyro")
        self.corrected_mag_callback = self._make_safe_cb(self.corrected_mag_data_handler, "corrected_mag")

        # Caller is expected to have run fs_setup.validate_config first
        # (B2: single-pass validation). We trust device_config / network_config
        # have been augmented with `fusion_mode` and validated.
        self.valid_config = True

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
        self.fusion_mode = device_config.get("fusion_mode")
        
        # Diagnostic
        self.logger = {"acc": 0, "gyro": 0, "mag": 0, "temp": 0, "light": 0, "fusion": 0} 

        # OSC
        self.OSC = None
        self.set_OSC(OSC)

        # Per-sensor processing pipelines. Each starts with just the
        # OscEmit terminal, preserving the pre-pipeline OSC vocabulary
        # 1:1. Compose richer pipelines by inserting stages before the
        # terminal: state.pipelines["acc"].stages.insert(-1, LowPass(5, 25)).
        self.pipelines: dict = {
            sensor: Pipeline([OscEmit(self._osc_client)])
            for sensor in (
                "acc", "gyro", "mag", "temp", "light",
                "quat", "euler",
                "linear_acc", "gravity",
                "corrected_acc", "corrected_gyro", "corrected_mag",
            )
        }

    # [ end __init__ ]
    #
    # TODO - a function to re-check configuration after remote change.

    # Failure tracking ----

    def _make_safe_cb(self, handler, name: str):
        """
        Wrap a bound data-handler method in a ctypes-safe callback. On
        successful completion, stamp _last_frame_at so the stale-stream
        watchdog knows fresh data is arriving.
        """
        def wrapped(ctx, data):
            try:
                handler(ctx, data)
            except BaseException as e:
                self._record_failure(f"callback:{name}", e)
            else:
                self._last_frame_at = time.monotonic()
        return FnVoid_VoidP_DataP(wrapped)

    def _record_failure(self, source: str, error: BaseException) -> None:
        with self._failure_lock:
            self._failed = True
            self._last_error = error
            self._failed_sources.append(source)
        log.exception("[%s] failure in %s: %r", self.address, source, error)

    @property
    def failed(self) -> bool:
        return self._failed

    @property
    def last_error(self) -> Optional[BaseException]:
        return self._last_error

    @property
    def failed_sources(self) -> list:
        with self._failure_lock:
            return list(self._failed_sources)

    # Button + LED (per-device toggle) ----

    def _subscribe_button(self) -> None:
        """
        Subscribe to the device's switch (button) signal. Called at the end
        of the connect() worker so it runs on initial connect and on every
        A5 reconnect automatically.
        """
        if not self.connected or self.device is None:
            return
        self._button_signal = libmetawear.mbl_mw_switch_get_state_data_signal(
            self.device.board
        )
        libmetawear.mbl_mw_datasignal_subscribe(
            self._button_signal, None, self._button_callback
        )
        log.info("[%s] button subscribed", self.address)

    def _button_data_handler(self, ctx, data) -> None:
        # Wrapped via _make_safe_cb in __init__, so exceptions here are
        # caught and recorded as callback:button failures.
        parsed = parse_value(data)  # 1 = pressed, 0 = released
        self._handle_button_state(int(parsed))

    def _handle_button_state(self, pressed: int) -> None:
        """
        Edge-filtered button event router. Called by _button_data_handler
        after parsing, and directly by stress tests so we don't need a real
        BLE event to verify the toggle logic.
        """
        if pressed != 1:
            return  # ignore release edges
        now = time.monotonic()
        elapsed = now - self._last_button_press_at
        if 0 < elapsed < self.button_window_s:
            log.info("[%s] button: double-press → toggle", self.address)
            self._last_button_press_at = 0.0
            self._on_button_event()
        else:
            self._last_button_press_at = now
            log.debug("[%s] button: first press registered (waiting for second)",
                      self.address)

    def _on_button_event(self) -> None:
        """Toggle streaming on this device. Called when a double-press fires."""
        if self._recovering or self._shutdown_done:
            log.warning("[%s] button event ignored (recovering=%s shutdown=%s)",
                        self.address, self._recovering, self._shutdown_done)
            return
        if self._intended_sensors:
            log.info("[%s] button: stopping stream", self.address)
            self.stop_sensors(self.sensor_config)
        else:
            log.info("[%s] button: starting stream", self.address)
            self.start_sensors(self.sensor_config)

    def _set_led_streaming(self, on: bool) -> None:
        """
        Solid GREEN while streaming, off when not. Best-effort — failures
        are recorded but never raised (LED is UX, not critical path).
        """
        if not self.connected or self.device is None:
            return
        try:
            if on:
                pattern = LedPattern()
                libmetawear.mbl_mw_led_load_preset_pattern(
                    byref(pattern), LedPreset.SOLID
                )
                libmetawear.mbl_mw_led_write_pattern(
                    self.device.board, byref(pattern), BUTTON_LED_COLOR
                )
                libmetawear.mbl_mw_led_play(self.device.board)
                log.debug("[%s] LED: solid green", self.address)
            else:
                libmetawear.mbl_mw_led_stop_and_clear(self.device.board)
                log.debug("[%s] LED: off", self.address)
        except BaseException as e:
            self._record_failure("led", e)

    # Stale-stream detection and recovery (A5) ----

    def _on_disconnect(self, status):
        """
        mbientlab on_disconnect callback. Fires when libmetawear detects
        the BLE link has gone away (supervision timeout, peer reset, etc.).
        We mark _link_lost so the next watchdog tick triggers recovery.
        Skipped during intentional disconnects/shutdowns so we don't
        recurse into try_recover from our own teardown.
        """
        if self._shutdown_done or self._recovering:
            return
        log.warning("[%s] BLE disconnect callback (status=%s) — link lost",
                    self.address, status)
        self._link_lost = True
        self.connected = False

    def is_stale(self) -> bool:
        """
        True if we believe sensors should be streaming but data hasn't
        arrived recently (or the link explicitly dropped). Cheap to call
        from a watchdog tick — no I/O.
        """
        if not self._intended_sensors:
            return False  # not supposed to be streaming
        if self._recovering or self._shutdown_done:
            return False
        if self._link_lost:
            return True
        if self._last_frame_at == 0.0:
            return False  # streaming hasn't actually started yet
        return (time.monotonic() - self._last_frame_at) > self.stale_threshold

    def check_and_recover(self) -> None:
        """Watchdog entry point. Caller invokes this on a tick (~1Hz)."""
        if not self.is_stale():
            return
        now = time.monotonic()
        if (now - self._last_recovery_attempt) < self.recovery_backoff:
            return  # too soon since last attempt; let it breathe
        self._last_recovery_attempt = now

        gap = now - self._last_frame_at if self._last_frame_at else float("inf")
        log.warning(
            "[%s] stale stream (last_frame=%.1fs ago, link_lost=%s) — recovering",
            self.address, gap, self._link_lost,
        )
        self.try_recover()

    def try_recover(self) -> bool:
        """
        Tear down the current connection and bring back the sensors that
        were intended to be streaming. Returns True iff streaming is
        restored. Re-entry-guarded; safe to call from anywhere. Failures
        are recorded on the state and don't propagate.
        """
        if self._recovering or self._shutdown_done:
            return False
        self._recovering = True
        try:
            previously_streaming = set(self._intended_sensors)
            log.info("[%s] try_recover: tearing down (was streaming %s)",
                     self.address, sorted(previously_streaming))

            # Stop whatever is currently live (best-effort — the link may
            # already be dead, in which case stop calls fail and we move on).
            for sensor in list(self._streaming_sensors):
                try:
                    stop_sensor_stream(sensor)(self, self.sensor_config)
                except BaseException as e:
                    self._record_failure(f"recover:stop:{sensor}", e)
                self._streaming_sensors.discard(sensor)
            self.streaming = False

            try:
                if self.connected:
                    self.disconnect()
            except BaseException as e:
                self._record_failure("recover:disconnect", e)
            self._link_lost = False  # disconnect handled, clear the flag

            # Let the BLE adapter release the resource before reconnecting.
            # Without this, libmetawear's warble layer can throw a C++
            # SocketConnectFailed("Device or resource busy") that bypasses
            # Python and aborts the process.
            sleep(RECONNECT_SETTLE_S)

            # Reconnect
            try:
                self.connect()
            except BaseException as e:
                self._record_failure("recover:connect", e)
                log.error("[%s] try_recover: reconnect failed", self.address)
                return False

            # Restart the sensors we were running before the trouble.
            for sensor in previously_streaming:
                if sensor not in self.sensor_config:
                    log.warning("[%s] try_recover: %s no longer in config; skipping",
                                self.address, sensor)
                    continue
                try:
                    start_sensor_stream(sensor)(self, self.sensor_config)
                    self._streaming_sensors.add(sensor)
                    log.info("[%s] try_recover: re-started %s", self.address, sensor)
                except BaseException as e:
                    self._record_failure(f"recover:start:{sensor}", e)

            self.streaming = bool(self._streaming_sensors)
            # Reset stale clock so the watchdog gives the new subscription
            # a fair chance before declaring stale again.
            self._last_frame_at = time.monotonic()
            return self.streaming
        finally:
            self._recovering = False

    # Bluetooth Device Connection ----

    def connect(self, timeout: float = DEFAULT_CONNECT_TIMEOUT) -> None:
        """
        Connect to the BLE device with a hard timeout. The blocking
        libmetawear handshake runs in a daemon worker thread; if it
        doesn't complete within `timeout` seconds we record a failure
        and raise TimeoutError. The hung C call cannot be safely
        interrupted from Python — the worker thread is left to exit on
        its own when the call eventually returns or the process exits.
        If a late-arriving handshake succeeds after we've already given
        up, the worker tears it down rather than leaving a zombie.
        """
        if self.connected:
            return None

        log.info("[%s] connecting (timeout=%.1fs)", self.address, timeout)
        self._connect_aborted = False
        done = threading.Event()
        worker_error: list = [None]

        def worker():
            device = None
            try:
                device = MetaWear(self.address, hci_mac=self.ble)
                device.on_disconnect = self._on_disconnect
                device.connect()
                # Caller may have given up while we were blocked in C.
                # If so, drop the freshly-handshaken link rather than
                # presenting a zombie connection to the rest of the system.
                if self._connect_aborted:
                    log.warning("[%s] late connect succeeded after abort; tearing down", self.address)
                    try:
                        libmetawear.mbl_mw_debug_disconnect(device.board)
                    except BaseException:
                        pass
                    return

                self.device = device
                self.connected = True
                self._link_lost = False
                log.info("[%s] connected over BLE", self.address)
                try:
                    self._osc_client.send_message("/indicator/conf", 1)
                    self._osc_client.send_message("/indicator/dev", 1)
                except BaseException as e:
                    self._record_failure("connect:osc_indicator", e)

                log.info("[%s] configuring", self.address)
                libmetawear.mbl_mw_settings_set_connection_parameters(
                    device.board, 7.5, 7.5, 0, 6000
                )
                sleep(1.0)

                try:
                    self._osc_client.send_message("/indicator/ble", 1)
                except BaseException as e:
                    self._record_failure("connect:osc_indicator", e)

                # Subscribe button signal (best-effort; failures recorded
                # but don't fail the connect — the device is otherwise usable).
                try:
                    self._subscribe_button()
                except BaseException as e:
                    self._record_failure("connect:subscribe_button", e)
            except BaseException as e:
                worker_error[0] = e
            finally:
                done.set()

        t = threading.Thread(
            target=worker,
            daemon=True,
            name=f"connect:{self.address}",
        )
        t.start()

        if not done.wait(timeout):
            self._connect_aborted = True
            err = TimeoutError(f"connect timed out after {timeout:.1f}s")
            self._record_failure("connect:timeout", err)
            raise err

        if worker_error[0] is not None:
            self._record_failure("connect:exception", worker_error[0])
            raise worker_error[0]

        return None

    def disconnect(self) -> None:
        if not self.connected:
            log.debug("[%s] disconnect: already disconnected", self.address)
            return None

        log.info("[%s] disconnecting", self.address)
        # Turn the LED off before tearing down so the device shows
        # "not active" state if it later reconnects via cache.
        try:
            self._set_led_streaming(False)
        except BaseException as e:
            self._record_failure("disconnect:led", e)

        try:
            libmetawear.mbl_mw_debug_disconnect(self.device.board)
        except BaseException as e:
            self._record_failure("disconnect:debug_disconnect", e)

        try:
            self._osc_client.send_message("/indicator/ble", 0)
            self._osc_client.send_message("/indicator/dev", 0)
        except BaseException as e:
            self._record_failure("disconnect:osc_indicator", e)

        self.connected = False
        log.info("[%s] disconnected", self.address)
        return None

    def shutdown(self) -> None:
        """
        Idempotent teardown. Stops any streaming sensors, disconnects BLE,
        and is safe to call multiple times (subsequent calls are no-ops).
        Each step is wrapped so a failure in one doesn't block the others.
        Wired into atexit/SIGINT in run_fs.py.
        """
        if self._shutdown_done:
            return None
        self._shutdown_done = True
        log.info("[%s] shutdown", self.address)

        if self._streaming_sensors:
            try:
                self.stop_sensors(self.sensor_config)
            except BaseException as e:
                self._record_failure("shutdown:stop_sensors", e)

        if self.connected:
            try:
                self.disconnect()
            except BaseException as e:
                self._record_failure("shutdown:disconnect", e)
        return None
    
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
            log.debug("[%s] OSC default: %s %s", self.address, address, args)

        def stop_server_handler(address, *args):
            log.info("[%s] OSC /stop_server", self.address)
            self.OSC.stop_server()

        def start_stream_handler(address, *args):
            if not self.streaming:
                log.info("[%s] OSC /start_stream %s", self.address, args)
                self.start_sensors(sensor_config=self.sensor_config)
            else:
                log.warning("[%s] OSC /start_stream ignored — already streaming", self.address)

        def stop_stream_handler(address, *args):
            if self.streaming:
                log.info("[%s] OSC /stop_stream %s", self.address, args)
                self.stop_sensors(sensor_config=self.sensor_config)
            else:
                log.warning("[%s] OSC /stop_stream ignored — not streaming", self.address)

        def sensor_config_handler(address, *args):
            log.info("[%s] OSC /sensors %s", self.address, args)
            if self.streaming:
                log.warning("[%s] streaming — sensor config change ignored", self.address)

        def network_config_handler(address, *args):
            log.info("[%s] OSC /network %s", self.address, args)
            if self.streaming:
                log.warning("[%s] streaming — network config change ignored", self.address)

        def ready_check_handler(address, *args):
            log.info("[%s] OSC /ready", self.address)
            if self.streaming:
                return None
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

        for sensor in sensor_config.keys():
            if sensor in self._streaming_sensors:
                log.debug("[%s] %s already streaming; skip", self.address, sensor)
                continue
            try:
                start_sensor_stream(sensor)(self, sensor_config)
                self._streaming_sensors.add(sensor)
                self._intended_sensors.add(sensor)
                log.info("[%s] started %s", self.address, sensor)
            except BaseException as e:
                self._record_failure(f"start_sensor:{sensor}", e)

        self.streaming = bool(self._streaming_sensors)
        # Baseline the stale clock so the watchdog gives the BLE link a
        # moment to start delivering frames before declaring stale.
        if self._intended_sensors:
            self._last_frame_at = time.monotonic()
        # LED feedback for the performer: solid green iff at least one
        # sensor actually started.
        self._set_led_streaming(bool(self._streaming_sensors))
        return None

    def stop_sensors(self, sensor_config) -> None:
        # Iterate the snapshot so we can mutate _streaming_sensors as we go.
        for sensor in list(self._streaming_sensors):
            try:
                stop_sensor_stream(sensor)(self, sensor_config)
                log.info("[%s] stopped %s", self.address, sensor)
            except BaseException as e:
                self._record_failure(f"stop_sensor:{sensor}", e)
            self._streaming_sensors.discard(sensor)

        # Caller-initiated stop: clear intent so the watchdog won't try to
        # auto-recover after this point.
        self._intended_sensors.clear()
        self.streaming = False
        self._set_led_streaming(False)
        return None
    
    # - Callbacks
    #
    # Each callback is the entry point for one libmetawear sensor signal.
    # It parses the C payload, builds an IMUFrame, and pushes it through
    # the device's pipeline for that sensor (default = OscEmit terminal).
    # The wrapping _make_safe_cb in __init__ catches any exception here
    # and stamps _last_frame_at on success — A2 + A5 plumbing.

    def _emit(self, sensor: str, values, logger_key=None) -> None:
        """Build an IMUFrame and push it through the named sensor's pipeline."""
        frame = IMUFrame(
            device=self.device.address,
            sensor=sensor,
            t_recv=time.monotonic(),
            values=tuple(values),
        )
        pipeline = self.pipelines.get(sensor)
        if pipeline is None:
            # Defensive: a stage emitted a sensor we didn't pre-register.
            pipeline = Pipeline([OscEmit(self._osc_client)])
            self.pipelines[sensor] = pipeline
        pipeline.push(frame)
        self.logger[logger_key or sensor] += 1

    def acc_data_handler(self, ctx, data):
        """Accelerometer values in g along [x, y, z]."""
        pd = parse_value(data)
        self._emit("acc", (pd.x, pd.y, pd.z))

    def gyro_data_handler(self, ctx, data):
        """Gyrometer values in degrees/sec around [x, y, z]."""
        pd = parse_value(data)
        self._emit("gyro", (pd.x, pd.y, pd.z))

    def mag_data_handler(self, ctx, data):
        """Magnetometer (h, d, z) components in nano Tesla."""
        pd = parse_value(data)
        self._emit("mag", (pd.x, pd.y, pd.z))

    def temp_data_handler(self, ctx, data):
        """Temperature in degrees Celsius."""
        temperature = parse_value(data)
        self._emit("temp", (temperature,))

    def light_data_handler(self, ctx, data):
        """Ambient light in lux (0.1–64k range)."""
        light = parse_value(data)
        self._emit("light", (light,))

    def quat_data_handler(self, ctx, data):
        """Sensor-fusion quaternion (w, x, y, z). Mutually exclusive with raw IMU."""
        pd = parse_value(data)
        self._emit("quat", (pd.w, pd.x, pd.y, pd.z), logger_key="fusion")

    def euler_data_handler(self, ctx, data):
        """Sensor-fusion Euler angles (heading, pitch, roll, yaw)."""
        pd = parse_value(data)
        self._emit("euler", (pd.heading, pd.pitch, pd.roll, pd.yaw), logger_key="fusion")

    def linear_acc_data_handler(self, ctx, data):
        pd = parse_value(data)
        self._emit("linear_acc", (pd.x, pd.y, pd.z), logger_key="fusion")

    def gravity_data_handler(self, ctx, data):
        pd = parse_value(data)
        self._emit("gravity", (pd.x, pd.y, pd.z), logger_key="fusion")

    def corrected_acc_data_handler(self, ctx, data):
        pd = parse_value(data)
        self._emit("corrected_acc", (pd.x, pd.y, pd.z), logger_key="fusion")

    def corrected_gyro_data_handler(self, ctx, data):
        pd = parse_value(data)
        self._emit("corrected_gyro", (pd.x, pd.y, pd.z), logger_key="fusion")

    def corrected_mag_data_handler(self, ctx, data):
        pd = parse_value(data)
        self._emit("corrected_mag", (pd.x, pd.y, pd.z), logger_key="fusion")

    # Utils ----
    def generate_sample_report(self) -> None:
        log.info("[%s] sample report: %s", self.address, self.logger)

