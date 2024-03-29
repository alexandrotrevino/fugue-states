from pythonosc import osc_server
from pythonosc import dispatcher
from pythonosc import udp_client
import threading
import time

class ControlledOSCConnection:
    """
    This class is an extension of the ThreadingOSCUDPServer class from pythonosc.
    The purpose of the class is to allow a listening server to be gracefully 
    shut down by a key message. 

    The server (listener) IP and port are hard coded: '0.0.0.0' and 8001. This is
    because the server needs to listen to messages from remote and local IPs. 

    :param str ip: The IP the client will send OSC messages to
    :param int port: The port the client will send to. Default is 8000.
    """
    def __init__(self, ip, port):

        # Server
        self.dispatcher = dispatcher.Dispatcher()
        self.server = osc_server.ThreadingOSCUDPServer(("0.0.0.0", 8001), self.dispatcher)
        self.is_running = False

        # Internal Client
        self._client = udp_client.SimpleUDPClient("127.0.0.1", 8001)

        # Remote Client
        self.client = udp_client.SimpleUDPClient(ip, port)

    def start_server(self):
        self.is_running = True
        server_thread = threading.Thread(target=self.run_server)
        server_thread.start()

    def run_server(self):
        while self.is_running:
            self.server.handle_request()

    def stop_server(self):
        self.is_running = False
        self._client.send_message("/stopped", "(dummy message)")
