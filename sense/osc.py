from pythonosc import osc_server
from pythonosc import dispatcher
from pythonosc import udp_client
import threading
import time

class OSCServer:
    """
    This class is an extension of the ThreadingOSCUDPServer class from pythonosc.
    The purpose of the class is to allow a listening server to be gracefully 
    shut down by a key message. 

    :param str ip: The IP the server will listen on. Default is 0.0.0.0
    :param int port: The port the server will listen through. Default is 8001.
    """
    def __init__(self, ip="0.0.0.0", port=8001):
        self.dispatcher = dispatcher.Dispatcher()
        self.server = osc_server.ThreadingOSCUDPServer((ip, port), self.dispatcher)
        self.is_running = False

        def stop_handler(address, *args):
            print(f"Received stop server message {address}: {args}")
            osc_server.stop_server()

        def default_handler(address, *args):
            print(f"Received message at {address}: {args}")

        self.server.dispatcher.map("/stop_server", stop_handler)
        self.server.dispatcher.set_default_handler(default_handler)

        self.client = udp_client.SimpleUDPClient("127.0.0.1", 8001)

    def start_server(self):
        self.is_running = True
        server_thread = threading.Thread(target=self.run_server)
        server_thread.start()

    def run_server(self):
        while self.is_running:
            self.server.handle_request()

    def stop_server(self):
        self.is_running = False
        self.client.send_message("/stop_server", 1)
        
class Stream:
    def __init__(self):
        self.streaming_thread = None
        self.running = False
    def start_process(self):
        if not self.running:
            self.running = True
            self.process_thread = threading.Thread(target = self.process)
            self.process_thread.start()
    def stop_process(self):
        if self.running:
            self.running = False
            self.process_thread.join()
    def process(self):
        while self.running:
            print("Process is running...")
            time.sleep(1)

def start_handler(address, *args):
    print(f"Recieved start msg {address}")

def stop_handler(address, *args):
    print(f"Received stop message {address}")

def start_handler(address, *args):
    print(f"Recieved start msg {address}")
    process_manager.start_process()

def stop_handler(address, *args):
    print(f"Received stop message {address}")
    process_manager.stop_process()
    