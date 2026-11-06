#!/usr/bin/env python3
"""
Headless Pd tests for the patch/ layer.

Pd itself is launched separately (so the command that runs an external
binary is explicit and easy to permission), then this script drives the
running patch over OSC and checks what it printed:

    # 1. start Pd headless on a test patch, logging stderr to a file
    <pd.com> -nogui -noprefs -noaudio -stderr -open patch/tests/test_receiver.pd 2> pd.log

    # 2. drive it and check the log
    python tools/pdtest.py run receiver --log pd.log

Protocol with the test patch: it prints `READY: bang` on load; this
script then sends the test's OSC messages, then `/test/quit`, which the
patch answers with `DONE: bang` and `; pd quit`. The patch also quits on
its own after 15 s so a failed run never leaves Pd hanging.

Pd version should match PlugData's embedded core (currently 0.55-2;
check with: grep -ao "Pd-0\\.[0-9.]*" "C:/Program Files/plugdata/Pd.dll").
"""

import argparse
import re
import sys
import time
from pathlib import Path

try:
    from pythonosc.udp_client import UDPClient
    from pythonosc.osc_message_builder import OscMessageBuilder
except ImportError:
    sys.exit("python-osc is required: python -m pip install --user python-osc")

def SLEEP(seconds):
    """A `send` entry that pauses instead of sending (for time-based logic)."""
    return ("__sleep__", [seconds])


D1 = "AA:BB:CC:DD:EE:01"
D2 = "AA:BB:CC:DD:EE:02"

# name -> spec. `send` is a list of (address, [args]); `expect` lines must
# all appear in the log, in order; `forbid` substrings must not appear.
# Values are chosen to be exact in float32 so Pd's %g printing is stable.
TESTS = {
    "receiver": {
        "patch": "patch/tests/test_receiver.pd",
        "port": 12360,
        "send": [
            ("/%s/__advertise__" % D1, ["/%s/acc_mag" % D1, "/%s/acc" % D1]),
            ("/%s/acc_mag" % D1, [1.5]),
            ("/%s/acc" % D1, [0.5, 0.25, -1.0]),
            ("/%s/gesture/roll" % D1, [0.75]),
            ("/%s/acc_mag" % D2, [2.5]),          # must reach dev2 only
            ("/%s/tilt" % D1, [45.0]),            # no tap: silent
            ("/FF:FF:FF:FF:FF:FF/acc_mag", [9.0]),  # unknown device: silent
            ("/state/heartbeat", ["abc123", 3.0]),  # status traffic: silent
        ],
        "expect": [
            "fs-dev1-advertise: /%s/acc_mag /%s/acc" % (D1, D1),
            "dev1/acc_mag: 1.5",
            "dev1/acc: 0.5 0.25 -1",
            "dev1/gesture/roll: 0.75",
            "dev2/acc_mag: 2.5",
        ],
        "forbid": ["dev1/acc_mag: 2.5", "acc_mag: 9", "error", "couldn't create", "abc123"],
    },
    "control": {
        "patch": "patch/tests/test_control.pd",
        "port": 12360,
        "send": [
            # fs_map 10 150 0 1 (clips), fs_map 0 1 100 50 (inverted output)
            ("/test/map", [10.0]), ("/test/map", [150.0]), ("/test/map", [80.0]),
            ("/test/map", [200.0]), ("/test/map", [0.0]),
            ("/test/map_inv", [0.0]), ("/test/map_inv", [0.5]), ("/test/map_inv", [2.0]),
            # fs_onset 2 1.3 100: fire, ignore while high, ignore re-cross inside
            # refractory even after dipping below rearm, then re-arm and fire.
            ("/test/onset", [1.0]), ("/test/onset", [2.5]), ("/test/onset", [3.0]),
            ("/test/onset", [1.0]), ("/test/onset", [2.75]),
            SLEEP(0.25),
            ("/test/onset", [1.0]), ("/test/onset", [2.25]),
            # fs_quantize 48 2, default pentatonic (10 steps over 2 octaves)
            ("/test/quant", [0.0]), ("/test/quant", [0.5]), ("/test/quant", [0.99]),
            ("/test/quant", [1.5]), ("/test/quant", [0.15]),
            # minor triad scale (6 steps)
            ("/test/qscale", [0.0, 3.0, 7.0]),
            ("/test/quant", [0.5]), ("/test/quant", [0.4]),
            # fs_smooth 0.5 0.1: fast up, slow down
            ("/test/smooth", [1.0]), ("/test/smooth", [1.0]),
            ("/test/smooth", [0.0]), ("/test/smooth", [0.0]),
            # fs_tune -> fs_cmd -> UDP 127.0.0.1:18001 (checked via "listen")
            ("/test/tune", [90.0]),
        ],
        "listen": {"port": 18001, "expect": [
            ("/cmd/pipeline/set", [D1, "quat", "Alignment", "yaw_offset", 90.0]),
        ]},
        "expect": [
            "map: 0", "map: 1", "map: 0.5", "map: 1", "map: 0",
            "map_inv: 100", "map_inv: 75", "map_inv: 50",
            "onset: 2.5", "onset: 2.25",
            "quant: 48", "quant: 60", "quant: 69", "quant: 69", "quant: 50",
            "quant: 60", "quant: 55",
            "smooth: 0.5", "smooth: 0.75", "smooth: 0.675", "smooth: 0.6075",
        ],
        "forbid": ["onset: 3", "onset: 2.75", "onset: 1", "error", "couldn't create"],
    },
    "instruments": {
        # Smoke test: audible (env~ > 40 dB, i.e. > -60 dBFS) when played,
        # silent again afterwards. Needs DSP running headless.
        "patch": "patch/tests/test_instruments.pd",
        "port": 12360,
        "send": [
            ("/%s/tilt" % D1, [90.0]),
            ("/%s/gyro_mag" % D1, [0.0]),
            ("/%s/acc_mag" % D1, [1.0]),
            SLEEP(0.3),                                   # still: all silent
            ("/%s/gyro_mag" % D1, [300.0]), SLEEP(0.5),   # turning: drone up
            ("/%s/gyro_mag" % D1, [0.0]), SLEEP(0.8),     # still: drone down
            ("/%s/acc_mag" % D1, [3.0]),                  # flick: pluck
            ("/%s/acc_mag" % D1, [1.0]), SLEEP(1.2),      # decays
            ("/%s/gesture/roll" % D1, [0.9]), SLEEP(0.8),  # hit, decays
            # fusion wrappers (fs_drone_point / fs_pluck_point)
            ("/%s/elevation" % D1, [30.0]),
            ("/%s/twist" % D1, [0.0]),
            ("/%s/corrected_gyro_mag" % D1, [0.0]),
            ("/%s/linear_acc_mag" % D1, [0.01]),
            SLEEP(0.3),
            ("/%s/corrected_gyro_mag" % D1, [300.0]), SLEEP(0.5),
            ("/%s/corrected_gyro_mag" % D1, [0.0]), SLEEP(0.8),
            ("/%s/linear_acc_mag" % D1, [2.5]),
            ("/%s/linear_acc_mag" % D1, [0.05]), SLEEP(1.2),
            # fs_sphere: dead-on at point 0 sounds; a stream of zeros rings it out
            ("/%s/sphere" % D1, [1.0] + [0.0] * 8),
            ("/%s/sphere" % D1, [1.0] + [0.0] * 8),
            SLEEP(0.2),
        ] + [("/%s/sphere" % D1, [0.0] * 9)] * 90 + [SLEEP(0.4)],
        "expect": ["drone: 1", "drone: 0", "pluck: 1", "pluck: 0", "hit: 1", "hit: 0",
                   "dronep: 1", "dronep: 0", "pluckp: 1", "pluckp: 0",
                   "sphere: 1", "sphere: 0"],
        "forbid": ["error", "couldn't create"],
    },
}


def send(client, address, args):
    msg = OscMessageBuilder(address=address)
    for a in args:
        msg.add_arg(a, "s" if isinstance(a, str) else "f")
    client.send(msg.build())


def wait_for(log, pattern, timeout):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if log.exists() and re.search(pattern, log.read_text(errors="replace"), re.M):
            return True
        time.sleep(0.05)
    return False


class Listener:
    """Collects OSC messages Pd sends out (e.g. C2 commands via fs_cmd)."""

    def __init__(self, port):
        import socket
        import threading
        from pythonosc.osc_message import OscMessage
        self.messages = []
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind(("127.0.0.1", port))
        self.sock.settimeout(0.2)
        self._stop = False

        def loop():
            while not self._stop:
                try:
                    data = self.sock.recv(65535)
                except OSError:
                    continue
                try:
                    m = OscMessage(data)
                    self.messages.append((m.address, list(m.params)))
                except Exception as e:  # malformed packet: record it
                    self.messages.append(("<unparseable>", [repr(e)]))
        self.thread = threading.Thread(target=loop, daemon=True)
        self.thread.start()

    def close(self):
        self._stop = True
        self.thread.join(1.0)
        self.sock.close()


def run(name, log, host, timeout):
    spec = TESTS[name]
    listener = Listener(spec["listen"]["port"]) if "listen" in spec else None
    try:
        return _run(name, spec, log, host, timeout, listener)
    finally:
        if listener:
            listener.close()


def _run(name, spec, log, host, timeout, listener):
    if not wait_for(log, r"^READY: bang", timeout):
        print("FAIL %s: patch never printed READY (is Pd running on %s?)" % (name, spec["patch"]))
        return False
    client = UDPClient(host, spec["port"])
    for address, args in spec["send"]:
        if address == "__sleep__":
            time.sleep(args[0])
            continue
        send(client, address, args)
        time.sleep(0.02)
    send(client, "/test/quit", [])
    if not wait_for(log, r"^DONE: bang", timeout):
        print("FAIL %s: patch never printed DONE (messages not arriving?)" % name)
        print(log.read_text(errors="replace"))
        return False
    time.sleep(0.2)

    text = log.read_text(errors="replace")
    # Only judge output between READY and DONE (skip Pd's startup banner).
    body = text[text.index("READY: bang"):text.index("DONE: bang")]
    lines = [l.strip() for l in body.splitlines()]
    ok = True
    pos = 0
    for exp in spec["expect"]:
        try:
            pos = lines.index(exp, pos) + 1
        except ValueError:
            print("  missing (or out of order): %r" % exp)
            ok = False
    for bad in spec["forbid"]:
        hits = [l for l in lines if bad in l and l not in spec["expect"]]
        if hits:
            print("  forbidden %r in: %s" % (bad, hits))
            ok = False
    if listener:
        got = listener.messages
        for exp in spec["listen"]["expect"]:
            if [exp[0], list(exp[1])] not in [[a, p] for a, p in got]:
                print("  missing outgoing OSC: %s %s (got %s)" % (exp[0], exp[1], got))
                ok = False
    print("%s %s (%d expected lines, %d forbidden checks)"
          % ("PASS" if ok else "FAIL", name, len(spec["expect"]), len(spec["forbid"])))
    if not ok:
        print("--- Pd output between READY and DONE ---")
        print("\n".join(lines))
    return ok


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0],
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd")
    r = sub.add_parser("run", help="drive a running test patch and check its log")
    r.add_argument("test", choices=sorted(TESTS))
    r.add_argument("--log", required=True, type=Path, help="file Pd's stderr is written to")
    r.add_argument("--host", default="::1")
    r.add_argument("--timeout", type=float, default=10.0)
    sub.add_parser("list", help="list tests and their patches")
    args = ap.parse_args(argv)
    if args.cmd == "list":
        for n, s in sorted(TESTS.items()):
            print("%-12s %s  (port %d)" % (n, s["patch"], s["port"]))
        return 0
    if args.cmd == "run":
        return 0 if run(args.test, args.log, args.host, args.timeout) else 1
    ap.print_help()
    return 2


if __name__ == "__main__":
    sys.exit(main())
