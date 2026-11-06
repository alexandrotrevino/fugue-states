#!/usr/bin/env python3
"""
Replay FS data-plane traffic to an OSC receiver (PlugData/PD) without
the Pi, BLE, or sensors in the loop.

Two sources:

  * JSONL recordings made with `run_fs.py --record` / `--capture-label`.
    Frames are re-published exactly as OscEmit would have sent them:
    `/<MAC>/<sensor> <values...>` (scalar frames as one float, vectors
    as N floats), paced by their original timestamps.

  * `--synthetic`: a generated two-device stream shaped like the default
    run_fs.py pipeline (acc, acc_lp, acc_mag, acc_lp_mag, tilt, gyro,
    gyro_mag, mag) plus occasional `gesture/<label>` triggers. Device MACs
    come from fs_config.json. Useful when no recording is at hand.

Before streaming, each device's `/<MAC>/__advertise__` message is sent
(derived from the file contents, or the synthetic channel set) so a
patch can discover channels the same way it would from a live Pi.

Defaults target `::1` (IPv6 loopback): PD on Windows binds `netreceive`
IPv6-only, so IPv4 packets to 127.0.0.1 are silently dropped. Pass
`--host 127.0.0.1` for receivers that listen on IPv4.

Runs on desktop Python (3.7+); needs only `python-osc`.
"""

import argparse
import itertools
import json
import math
import random
import sys
import time
from collections import Counter, OrderedDict
from pathlib import Path

try:
    from pythonosc.udp_client import UDPClient
    from pythonosc.osc_message_builder import OscMessageBuilder
except ImportError:
    sys.exit("python-osc is required: python -m pip install --user python-osc")

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))
from sense.recorder import replay  # noqa: E402  (path set up above)

EPILOG = """\
examples:
  # replay a recording at real speed to PD on this machine
  python tools/replay_osc.py recordings/session-20260527-101500.jsonl

  # loop it forever, half speed, only the channels a patch cares about
  python tools/replay_osc.py rec.jsonl --loop --speed 0.5 --only acc_mag,tilt

  # no recording handy: synthetic two-device stream at 50 Hz
  python tools/replay_osc.py --synthetic

  # send to an IPv4 receiver on another host
  python tools/replay_osc.py --synthetic --host 192.168.4.193 --port 12345
"""


# --- OSC send ---------------------------------------------------------------

class Sender:
    """Thin wrapper: builds float-typed messages, counts traffic."""

    def __init__(self, host, port, verbose=False):
        self.client = UDPClient(host, port)
        self.verbose = verbose
        self.counts = Counter()
        self.errors = 0

    def send(self, address, values):
        msg = OscMessageBuilder(address=address)
        for v in values:
            if isinstance(v, str):
                msg.add_arg(v, "s")
            else:
                msg.add_arg(float(v), "f")
        try:
            self.client.send(msg.build())
        except OSError as e:
            self.errors += 1
            if self.errors == 1:
                print("send failed (%s) - is the receiver reachable?" % e,
                      file=sys.stderr)
            return
        self.counts[address] += 1
        if self.verbose:
            print(address, *("%.4g" % v if not isinstance(v, str) else v
                             for v in values))


def advertise(sender, channels_by_device):
    for device, sensors in channels_by_device.items():
        addrs = ["/%s/%s" % (device, s) for s in sorted(sensors)]
        sender.send("/%s/__advertise__" % device, addrs)
        print("advertise %s: %s" % (device, ", ".join(sorted(sensors))))


def accept(sensor, only):
    return not only or sensor in only


# --- JSONL source ------------------------------------------------------------

def scan_channels(paths, only):
    """Return {device: set(sensor)} found in the recordings."""
    found = OrderedDict()
    for p in paths:
        with open(p, "r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                rec = json.loads(line)
                if "sensor" in rec and accept(rec["sensor"], only):
                    found.setdefault(rec["device"], set()).add(rec["sensor"])
    return found


def run_files(args, sender, only):
    channels = scan_channels(args.paths, only)
    if not channels:
        sys.exit("no frames found in %s" % ", ".join(args.paths))
    if not args.no_advertise:
        advertise(sender, channels)

    def on_frame(frame):
        if accept(frame.sensor, only):
            sender.send("/%s/%s" % (frame.device, frame.sensor), frame.values)

    passes = itertools.count(1) if args.loop else [1]
    for n in passes:
        for p in args.paths:
            print("pass %d: %s" % (n, p))
            replay(p, on_frame, speed=args.speed)


# --- Synthetic source --------------------------------------------------------

SYNTH_SENSORS = ["acc", "acc_lp", "acc_mag", "acc_lp_mag", "tilt",
                 "gyro", "gyro_mag", "mag"]
SYNTH_GESTURES = ["roll", "chop"]


def config_macs():
    try:
        cfg = json.loads((REPO / "fs_config.json").read_text())
        macs = [d["mac"] for d in cfg["metawear"]["devices"]]
        if macs:
            return macs
    except (OSError, KeyError, ValueError):
        pass
    return ["E0:2D:64:47:02:F5", "EC:47:49:CF:53:C4"]


class SynthDevice:
    """
    A wrist that wanders slowly in orientation, with occasional motion
    bursts (shakes/swings). Produces acc in g, gyro in deg/s, matching the
    units and rough ranges of the MetaWear stream.
    """

    def __init__(self, mac, seed, rate):
        self.mac = mac
        self.rng = random.Random(seed)
        self.dt = 1.0 / rate
        # independent slow orientation oscillators (pitch, roll)
        self.f_pitch = self.rng.uniform(0.04, 0.09)
        self.f_roll = self.rng.uniform(0.03, 0.07)
        self.phase = self.rng.uniform(0, 2 * math.pi)
        self.lp = None
        self.lp_alpha = 1 - math.exp(-2 * math.pi * 5.0 * self.dt)  # 5 Hz LowPass
        self.burst_until = -1.0
        self.next_burst = self.rng.uniform(2, 6)
        self.next_gesture = self.rng.uniform(6, 12)
        self.prev_angles = None

    def step(self, t):
        rng = self.rng
        pitch = 1.3 * math.sin(2 * math.pi * self.f_pitch * t + self.phase)
        roll = 1.6 * math.sin(2 * math.pi * self.f_roll * t)
        # gravity in device frame for that orientation
        gx = -math.sin(pitch)
        gy = math.cos(pitch) * math.sin(roll)
        gz = math.cos(pitch) * math.cos(roll)

        # motion bursts: ~0.6 s of shaking every few seconds
        if t >= self.next_burst:
            self.burst_until = t + rng.uniform(0.3, 0.9)
            self.next_burst = t + rng.uniform(2, 7)
        motion = (0.0, 0.0, 0.0)
        if t < self.burst_until:
            amp = rng.uniform(0.6, 1.8)
            w = 2 * math.pi * 6.0 * t
            motion = (amp * math.sin(w), amp * 0.6 * math.cos(w), amp * 0.3 * math.sin(2 * w))
        noise = lambda: rng.gauss(0, 0.015)  # noqa: E731
        acc = (gx + motion[0] + noise(), gy + motion[1] + noise(), gz + motion[2] + noise())

        if self.lp is None:
            self.lp = acc
        a = self.lp_alpha
        self.lp = tuple(p + a * (x - p) for p, x in zip(self.lp, acc))

        angles = (roll, pitch)
        if self.prev_angles is None:
            self.prev_angles = angles
        gyro = (math.degrees((angles[0] - self.prev_angles[0]) / self.dt) + rng.gauss(0, 1.5),
                math.degrees((angles[1] - self.prev_angles[1]) / self.dt) + rng.gauss(0, 1.5),
                (300.0 * math.sin(2 * math.pi * 6.0 * t) if t < self.burst_until else 0.0)
                + rng.gauss(0, 1.5))
        self.prev_angles = angles

        mag = (30 * gx + rng.gauss(0, 0.5), 30 * gy + rng.gauss(0, 0.5), -20 + 30 * gz)

        acc_mag = math.sqrt(sum(v * v for v in acc))
        tilt = math.degrees(math.acos(max(-1.0, min(1.0, acc[2] / acc_mag)))) if acc_mag else 0.0
        frames = [
            ("acc", acc),
            ("acc_lp", self.lp),
            ("acc_mag", (acc_mag,)),
            ("acc_lp_mag", (math.sqrt(sum(v * v for v in self.lp)),)),
            ("tilt", (tilt,)),
            ("gyro", gyro),
            ("gyro_mag", (math.sqrt(sum(v * v for v in gyro)),)),
            ("mag", mag),
        ]
        if t >= self.next_gesture:
            self.next_gesture = t + rng.uniform(5, 12)
            label = rng.choice(SYNTH_GESTURES)
            frames.append(("gesture/" + label, (round(rng.uniform(0.55, 0.95), 3),)))
        return frames


def run_synthetic(args, sender, only):
    macs = config_macs()
    devices = [SynthDevice(m, seed=args.seed + i, rate=args.rate) for i, m in enumerate(macs)]
    channels = OrderedDict(
        (d.mac, {s for s in SYNTH_SENSORS + ["gesture/" + g for g in SYNTH_GESTURES]
                 if accept(s, only)})
        for d in devices)
    if not args.no_advertise:
        advertise(sender, channels)

    period = 1.0 / args.rate
    t0 = time.monotonic()
    tick = 0
    end = args.duration if args.duration else float("inf")
    while True:
        t = tick * period
        if t >= end:
            break
        for d in devices:
            for sensor, values in d.step(t):
                if accept(sensor, only):
                    sender.send("/%s/%s" % (d.mac, sensor), values)
                    if sensor.startswith("gesture/") and not args.verbose:
                        print("%7.2fs  %s %s  conf=%.2f" % (t, d.mac, sensor, values[0]))
        tick += 1
        delay = t0 + tick * period - time.monotonic()
        if delay > 0:
            time.sleep(delay)


# --- main -------------------------------------------------------------------

def main(argv=None):
    ap = argparse.ArgumentParser(
        description="Replay FS OSC data to PlugData/PD without the Pi.",
        epilog=EPILOG, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("paths", nargs="*", help="JSONL recording(s), played in order")
    ap.add_argument("--synthetic", action="store_true",
                    help="generate a synthetic two-device stream instead of reading files")
    ap.add_argument("--host", default="::1",
                    help="receiver host (default ::1 - PD on Windows listens IPv6-only)")
    ap.add_argument("--port", type=int, default=None,
                    help="receiver port (default: network.port from fs_config.json)")
    ap.add_argument("--speed", type=float, default=1.0,
                    help="file replay pacing: 1.0 real-time, 2.0 double, 0 unpaced")
    ap.add_argument("--loop", action="store_true", help="repeat the file(s) forever")
    ap.add_argument("--only", default="",
                    help="comma-separated sensor names to send (e.g. acc_mag,tilt)")
    ap.add_argument("--rate", type=float, default=50.0,
                    help="synthetic frame rate per device in Hz (default 50)")
    ap.add_argument("--duration", type=float, default=0.0,
                    help="synthetic run length in seconds (default: until Ctrl-C)")
    ap.add_argument("--seed", type=int, default=1, help="synthetic RNG seed")
    ap.add_argument("--no-advertise", action="store_true",
                    help="skip the /<MAC>/__advertise__ messages at start")
    ap.add_argument("-v", "--verbose", action="store_true", help="print every message sent")
    args = ap.parse_args(argv)

    if args.synthetic == bool(args.paths):
        ap.error("give either recording path(s) or --synthetic")
    if args.port is None:
        try:
            cfg = json.loads((REPO / "fs_config.json").read_text())
            args.port = int(cfg["network"]["port"])
        except (OSError, KeyError, ValueError):
            args.port = 12345

    only = {s.strip() for s in args.only.split(",") if s.strip()}
    sender = Sender(args.host, args.port, verbose=args.verbose)
    print("sending to [%s]:%d" % (args.host, args.port))
    started = time.monotonic()
    try:
        if args.synthetic:
            run_synthetic(args, sender, only)
        else:
            run_files(args, sender, only)
    except KeyboardInterrupt:
        pass
    elapsed = max(time.monotonic() - started, 1e-9)
    total = sum(sender.counts.values())
    print("\nsent %d messages in %.1fs (%.0f msg/s), %d send errors"
          % (total, elapsed, total / elapsed, sender.errors))
    return 0


if __name__ == "__main__":
    sys.exit(main())
