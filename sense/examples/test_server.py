from pythonosc import dispatcher
from pythonosc import osc_server
from pythonosc import udp_client
import threading
import time


class OSCControlledServer:
    def __init__(self, ip, port):
        self.dispatcher = dispatcher.Dispatcher()
        self.server = osc_server.ThreadingOSCUDPServer((ip, port), self.dispatcher)
        self.is_running = False
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

class Stream(self, process):
    def __init__(self):
        self.streaming_thread = None
        self.running = False

    def start(self):
        if not self.running:
            self.running = True
            self.process_thread = threading.Thread(target = self.process)
            self.process_thread.start()

    def stop(self):
        if self.running:
            self.running = False
            self.process_thread.join()

    def stream(self):
        while self.running:
            print("Process is running...")
            time.sleep(1)