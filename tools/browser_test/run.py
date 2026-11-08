#!/usr/bin/env python3
"""
Headless browser test for the sphere editor (tools/sphere_edit.py).

Starts the editor against fake Pi/PD sinks (two local UDP ports whose
OSC is logged to a file), drives the page in headless Chrome over the
DevTools protocol (cdp.mjs, Node 22+, no packages), and checks both
what the page shows and exactly which messages each action sends.

    python tools/browser_test/run.py              # editor.mjs
    python tools/browser_test/run.py --shots out  # also keep screenshots

Needs Chrome (or set CHROME=<path>) and node on PATH. The real Pi and
PD are never contacted. Scenario temp files in presets/ are removed.
"""

import argparse
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path

from pythonosc.osc_message import OscMessage

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent.parent
PI_PORT, PD_PORT, HTTP_PORT, LIVE_PORT, DEBUG_PORT = 19001, 19002, 8799, 12399, 9333

CHROME_CANDIDATES = [
    r"C:\Program Files\Google\Chrome\Application\chrome.exe",
    r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe",
    "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
    "google-chrome", "chromium", "chromium-browser",
]


def find_chrome():
    if os.environ.get("CHROME"):
        return os.environ["CHROME"]
    for c in CHROME_CANDIDATES:
        if Path(c).is_file():
            return c
        if shutil.which(c):
            return shutil.which(c)
    sys.exit("Chrome not found; set CHROME=<path>")


def sink(port, who, out, lock):
    """Log every OSC message arriving on `port` as '<who> <address> <args>'."""
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.bind(("127.0.0.1", port))
    while True:
        m = OscMessage(s.recv(65535))
        with lock:
            out.write("%s %s %s\n" % (who, m.address, " ".join(str(p) for p in m.params)))
            out.flush()


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--scenario", default=str(HERE / "editor.mjs"))
    ap.add_argument("--preset", default="presets/dome9-a-major-pent.json")
    ap.add_argument("--shots", default="", help="keep screenshots in this directory")
    args = ap.parse_args()
    if not shutil.which("node"):
        sys.exit("node (22+) is required")

    tmp = Path(tempfile.mkdtemp(prefix="fs-browser-test-"))
    shots = Path(args.shots).resolve() if args.shots else tmp / "shots"
    shots.mkdir(parents=True, exist_ok=True)
    sink_path = tmp / "sink.txt"
    out, lock = open(sink_path, "w", encoding="utf-8"), threading.Lock()
    for port, who in ((PI_PORT, "PI"), (PD_PORT, "PD")):
        threading.Thread(target=sink, args=(port, who, out, lock), daemon=True).start()

    server_log = open(tmp / "server.log", "w")
    server = subprocess.Popen(
        [sys.executable, "tools/sphere_edit.py", args.preset, "--no-browser",
         "--pi", "127.0.0.1", "--pi-port", str(PI_PORT), "--pd", "127.0.0.1", "--pd-port", str(PD_PORT),
         "--http-port", str(HTTP_PORT), "--udp-port", str(LIVE_PORT)],
        cwd=ROOT, stdout=server_log, stderr=subprocess.STDOUT)
    chrome = subprocess.Popen(
        [find_chrome(), "--headless=new", "--remote-debugging-port=%d" % DEBUG_PORT,
         "--user-data-dir=" + str(tmp / "profile"), "--use-angle=swiftshader",
         "--enable-unsafe-swiftshader", "--no-first-run", "--window-size=1280,800", "about:blank"],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    rc = 1
    try:
        time.sleep(2.5)
        env = dict(os.environ, SINK=str(sink_path), PRESETS=str(ROOT / "presets"),
                   LIVE_PORT=str(LIVE_PORT))
        rc = subprocess.call(["node", str(HERE / "cdp.mjs"), str(DEBUG_PORT),
                              "http://localhost:%d/" % HTTP_PORT, args.scenario, str(shots)],
                             cwd=HERE, env=env)
    finally:
        for p in (chrome, server):
            p.terminate()
            try:
                p.wait(5)
            except subprocess.TimeoutExpired:
                p.kill()
        server_log.close()
        if rc:
            print("server log:\n" + (tmp / "server.log").read_text())
        time.sleep(0.5)
        shutil.rmtree(tmp / "profile", ignore_errors=True)
        if args.shots:
            print("screenshots in", shots)
    sys.exit(rc)


if __name__ == "__main__":
    main()
