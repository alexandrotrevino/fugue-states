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


def run(name, log, host, timeout):
    spec = TESTS[name]
    if not wait_for(log, r"^READY: bang", timeout):
        print("FAIL %s: patch never printed READY (is Pd running on %s?)" % (name, spec["patch"]))
        return False
    client = UDPClient(host, spec["port"])
    for address, args in spec["send"]:
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
