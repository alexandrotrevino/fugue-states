"""
Live sphere data for the browser tools: listens for OSC (aim, sphere
levels, gate) and keeps the latest state for the page to stream.

Any OSC address ending in these channel names works:

  .../aim      vec3, pointing in the sphere's own frame (Alignment stage)
  .../sphere   per-target alignment values (Alignment stage)
  .../pointing vec3 world-frame pointing (Orientation stage) - used when
               no `aim` arrives, e.g. older recordings
  .../gate     how much fs_sphere's motion gate lets through (0..1)

When `sphere` values arrive they are shown as-is (they are what the
instrument hears); otherwise alignment is computed here with the same
Alignment code the Pi runs.

Sources: [fs_viz] in fs_play.pd (127.0.0.1:12346), or offline
`python tools/replay_osc.py <rec.jsonl> --port 12346 --only aim,sphere,pointing`.
"""

import collections
import socket
import sys
import threading
import time
from pathlib import Path

try:
    from pythonosc.osc_message import OscMessage
except ImportError:
    sys.exit("python-osc is required: python -m pip install --user python-osc")

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from sense.pipeline import Alignment, IMUFrame  # noqa: E402


class State:
    def __init__(self, points, cone, device_filter):
        self.lock = threading.Lock()
        self.new_data = threading.Condition()   # wakes browser streams on new aim
        self.alignment = Alignment(points=points, cone_deg=cone)
        self.device_filter = device_filter
        self.device = None
        self.last_t = 0.0
        self.aim = None
        self.aim_from = None          # "aim" or "pointing"
        self.aim_t = 0.0
        self.sphere = None
        self.sphere_t = 0.0
        self.gate = None
        self.gate_t = 0.0
        # Every aim sample, numbered, so each browser connection gets all
        # samples since its last update (a complete trail, no aliasing).
        self.aim_seq = 0
        self.aim_hist = collections.deque(maxlen=256)
        self.count = 0
        self.rate = 0.0
        self._rate_t0 = time.monotonic()
        self._rate_n = 0

    def feed(self, address, params):
        parts = [p for p in address.split("/") if p]
        if len(parts) < 2:
            return
        # /<MAC>/aim (Pi or replay) and /fs/<name>/aim (PD's fs_viz) both
        # name the device in the segment just before the channel.
        channel, device = parts[-1], parts[-2]
        if channel not in ("aim", "sphere", "pointing", "gate"):
            return
        if self.device_filter and self.device_filter not in device:
            return
        now = time.monotonic()
        with self.lock:
            if device != self.device:
                # Follow a new source once the current one has gone quiet
                # (e.g. switching from a replay to the live patch).
                if self.device is not None and now - self.last_t < 2.0:
                    return
                self.device, self.aim, self.aim_from, self.sphere = device, None, None, None
                self.gate = None
            self.last_t = now
            self._rate_n += 1
            if now - self._rate_t0 >= 1.0:
                self.rate = self._rate_n / (now - self._rate_t0)
                self._rate_t0, self._rate_n = now, 0
            vals = [float(v) for v in params]
            if channel == "sphere":
                self.sphere, self.sphere_t = vals, now
            elif channel == "gate" and vals:
                self.gate, self.gate_t = max(0.0, min(1.0, vals[0])), now
            elif channel == "aim" and len(vals) == 3:
                self._set_aim(vals, "aim", now)
            elif channel == "pointing" and len(vals) == 3:
                # Only fall back to pointing when no aim has arrived lately.
                if self.aim_from != "aim" or now - self.aim_t > 1.0:
                    self._set_aim(vals, "pointing", now)

    def _set_aim(self, vals, source, now):
        self.aim, self.aim_from, self.aim_t = vals, source, now
        self.aim_seq += 1
        self.aim_hist.append((self.aim_seq, vals))
        with self.new_data:
            self.new_data.notify_all()

    def aims_since(self, seq):
        """New aim samples after `seq` (oldest first) and the latest seq."""
        with self.lock:
            return [v for s, v in self.aim_hist if s > seq], self.aim_seq

    def snapshot(self):
        now = time.monotonic()
        with self.lock:
            aim = self.aim if self.aim and now - self.aim_t < 2.0 else None
            if self.sphere is not None and now - self.sphere_t < 1.0:
                values, source = self.sphere, "pi"
            elif aim is not None:
                out = [f for f in self.alignment.process(
                    IMUFrame(device="viz", sensor="pointing", t_recv=0.0, values=tuple(aim)))
                    if f.sensor == "sphere"]
                values, source = list(out[0].values), "computed"
            else:
                values, source = None, "waiting"
            gate = self.gate if self.gate is not None and now - self.gate_t < 1.0 else None
            return {"device": self.device, "aim": aim, "aim_from": self.aim_from,
                    "values": values, "source": source, "gate": gate,
                    "rate": round(self.rate, 1)}


def open_udp_sockets(port):
    """One IPv4 and one IPv6 socket: PD's netsend uses IPv4, replay_osc
    defaults to ::1. (A single dual-stack socket isn't portable: Python
    3.7 on Windows lacks socket.IPPROTO_IPV6.)"""
    socks = []
    v6 = socket.socket(socket.AF_INET6, socket.SOCK_DGRAM)
    try:
        v6.setsockopt(getattr(socket, "IPPROTO_IPV6", 41),
                      getattr(socket, "IPV6_V6ONLY", 27), 1)
        v6.bind(("::", port))
        socks.append(v6)
    except OSError as e:
        print("note: no IPv6 listener (%s)" % e)
        v6.close()
    v4 = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        v4.bind(("0.0.0.0", port))
        socks.append(v4)
    except OSError as e:
        print("note: no IPv4 listener (%s)" % e)
        v4.close()
    if not socks:
        sys.exit("could not bind udp/%d" % port)
    return socks


def udp_loop(state, sock):
    while True:
        try:
            data = sock.recv(65535)
        except OSError:
            continue  # e.g. Windows WSAECONNRESET after an ICMP unreachable
        try:
            m = OscMessage(data)
        except Exception:
            continue  # bundles / garbage: ignore
        state.feed(m.address, m.params)

