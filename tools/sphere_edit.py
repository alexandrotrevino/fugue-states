#!/usr/bin/env python3
"""
Sphere editor: design an alignment-sphere instrument in the browser and
hear every change live.

    python tools/sphere_edit.py                       # opens presets/dome9-a-major-pent.json
    python tools/sphere_edit.py presets/arc7-d-dorian.json
    python tools/sphere_edit.py --no-pi               # edit notes/envelopes only, leave the Pi alone

The page (http://localhost:8765) shows the sphere in 3D with your live
aim, like sphere_viz.py, and lets you:

  * click a note to select it; drag it across the sphere to move it
  * double-click empty sphere to add a note there; Delete removes the selection
  * edit the selected note (pitch, position, zone, envelope) and the
    global settings (zone, gate, attack, release) in the side panel
  * start a new layout from N evenly spread points (as `preset.py new`)
  * load and save presets in presets/

With "live" on, each change goes straight to the instrument, using the
same messages as `tools/preset.py apply`: layout and zones to the Pi
(C2, persisted there), notes, envelopes and gate to PD (fs_sphere's
sphere_cfg channel). Only what changed is sent. "Send all" re-sends
everything - use it after reopening the patch in PlugData.

Live aim/levels arrive like sphere_viz.py: switch on "viz" in fs_play.pd
(or replay into udp/12346).
"""

import argparse
import json
import re
import sys
import threading
import time
import webbrowser
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
sys.path.insert(0, str(HERE))
import preset as presets  # noqa: E402  tools/preset.py
from sphere_viz import State, open_udp_sockets, udp_loop  # noqa: E402

PAGE = HERE / "sphere_edit.html"
PRESET_DIR = ROOT / "presets"
DEFAULT_PRESET = PRESET_DIR / "dome9-a-major-pent.json"
FILE_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,80}\.json$")


def _num_out(v):
    return int(v) if float(v).is_integer() else round(float(v), 3)


def to_raw(p):
    """Normalized preset -> canonical file form (overrides only when set)."""
    targets = []
    for t in p["targets"]:
        d = {"az": _num_out(t["az"]), "el": _num_out(t["el"]), "note": t["note"]}
        for k in ("cone_deg", "attack_ms", "release_ms"):
            if t[k] is not None:
                d[k] = _num_out(t[k])
        if t["label"]:
            d["label"] = t["label"]
        targets.append(d)
    return {
        "fugue_preset": 1,
        "name": p["name"],
        "description": p["description"],
        "device": dict(p["device"]),
        "sphere": {
            "cone_deg": _num_out(p["cone_deg"]), "gate": float(p["gate"]),
            "attack_ms": _num_out(p["attack_ms"]), "release_ms": _num_out(p["release_ms"]),
            "targets": targets,
        },
    }


class Engine:
    """Sends a preset to the Pi and PD, tracking what each already has so
    live edits only send the difference."""

    def __init__(self, pi, pd, gap_s=0.001):
        self.pi, self.pd = pi, pd            # (host, port) or None
        self.gap_s = gap_s
        self.lock = threading.Lock()
        self._clients = {}
        self.reset()

    def reset(self):
        self.pi_state = {}                   # "cone_deg"/"points" -> value
        self.pd_state = {}                   # (key, index) -> value
        self.mac = None

    def _client(self, dest):
        if dest not in self._clients:
            from pythonosc.udp_client import SimpleUDPClient
            self._clients[dest] = SimpleUDPClient(*dest)
        return self._clients[dest]

    def _send(self, dest, msgs):
        c = self._client(dest)
        for m in msgs:
            c.send_message(m[0], m[1:])
            if self.gap_s:
                time.sleep(self.gap_s)

    def pi_wanted(self, p):
        return [("cone_deg", p["cone_deg"]), ("points", presets.points_spec(p))]

    def pd_wanted(self, p):
        """Per-voice effective values. Voices past the layout are left as
        they are (they only sound if the layout grows, and then get sent)."""
        g_att, g_rel = p["attack_ms"], p["release_ms"]
        want = [(("attack", 0), g_att), (("release", 0), g_rel)]
        for i, t in enumerate(p["targets"], 1):
            want.append((("note", i), float(t["note"])))
            want.append((("attack_n", i), t["attack_ms"] if t["attack_ms"] is not None else g_att))
            want.append((("release_n", i), t["release_ms"] if t["release_ms"] is not None else g_rel))
        want.append((("gate", 0), p["gate"]))
        return want

    def apply(self, p, force=False):
        with self.lock:
            if force or p["device"]["mac"] != self.mac:
                self.reset()
                self.mac = p["device"]["mac"]
            sent = {"pi": 0, "pd": 0}
            if self.pi:
                base = ["/cmd/pipeline/set", self.mac, "quat", "Alignment"]
                msgs = []
                for k, v in self.pi_wanted(p):
                    if self.pi_state.get(k) != v:
                        msgs.append(base + [k, v])
                        self.pi_state[k] = v
                self._send(self.pi, msgs)
                sent["pi"] = len(msgs)
            if self.pd:
                addr = "/%s/sphere_cfg" % self.mac
                msgs = []
                for (k, i), v in self.pd_wanted(p):
                    if self.pd_state.get((k, i)) == v:
                        continue
                    msgs.append([addr, k, v] if i == 0 else [addr, k, float(i), v])
                    self.pd_state[(k, i)] = v
                    if k in ("attack", "release"):
                        # a global envelope resets every voice's override
                        for j in range(1, presets.MAX_TARGETS + 1):
                            self.pd_state[(k + "_n", j)] = v
                self._send(self.pd, msgs)
                sent["pd"] = len(msgs)
            return sent


class Editor:
    def __init__(self, state, engine, path):
        self.state, self.engine = state, engine
        self.lock = threading.Lock()
        self.file = None
        self.raw = None
        self.load_file(path)

    def load_file(self, path):
        p = presets.load(path)
        with self.lock:
            self.file = Path(path).name if Path(path).resolve().parent == PRESET_DIR.resolve() else None
            self.raw = to_raw(p)
        self.show(p)
        return self.raw

    def show(self, p):
        """Point the viewer's own Alignment (used when no live sphere
        values arrive) at this layout."""
        with self.state.lock:
            self.state.alignment.cone_deg = p["cone_deg"]
            self.state.alignment.points = presets.points_spec(p)

    def files(self):
        return sorted(f.name for f in PRESET_DIR.glob("*.json"))


def make_handler(editor, page_path):
    state, engine = editor.state, editor.engine

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *a):
            pass

        def _json(self, obj, code=200):
            body = json.dumps(obj).encode("utf-8")
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _body(self):
            n = int(self.headers.get("Content-Length") or 0)
            return json.loads(self.rfile.read(n).decode("utf-8") or "{}")

        def do_GET(self):
            if self.path in ("/", "/index.html"):
                body = page_path.read_bytes()   # re-read: edit the page, reload the tab
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            elif self.path == "/api/init":
                with editor.lock:
                    raw, file = editor.raw, editor.file
                self._json({"preset": raw, "file": file, "files": editor.files(),
                            "scales": sorted(presets.SCALES), "shapes": sorted(presets.SHAPES),
                            "max_targets": presets.MAX_TARGETS,
                            "pi": "%s:%d" % engine.pi if engine.pi else None,
                            "pd": "[%s]:%d" % engine.pd if engine.pd else None})
            elif self.path == "/events":
                self.send_response(200)
                self.send_header("Content-Type", "text/event-stream")
                self.send_header("Cache-Control", "no-cache")
                self.end_headers()
                seq = 0
                try:
                    while True:
                        with state.new_data:
                            if state.aim_seq == seq:
                                state.new_data.wait(timeout=0.25)
                        snap = state.snapshot()
                        snap["aims"], seq = state.aims_since(seq)
                        self.wfile.write(("data: %s\n\n" % json.dumps(snap)).encode())
                        self.wfile.flush()
                except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError):
                    return
            else:
                self.send_error(404)

        def do_POST(self):
            try:
                req = self._body()
                if self.path == "/api/apply":
                    # {preset, send: bool, force: bool}
                    p = presets.validate(req.get("preset"))
                    editor.show(p)
                    with editor.lock:
                        editor.raw = to_raw(p)
                    sent = engine.apply(p, force=bool(req.get("force"))) if req.get("send") else None
                    return self._json({"ok": True, "sent": sent})
                if self.path == "/api/save":
                    # {preset, file, overwrite}
                    name = str(req.get("file") or "")
                    if not name.endswith(".json"):
                        name += ".json"
                    if not FILE_RE.match(name):
                        return self._json({"ok": False, "error": "file name: letters, digits, . _ - only"})
                    p = presets.validate(req.get("preset"))
                    path = PRESET_DIR / name
                    if path.exists() and not req.get("overwrite"):
                        return self._json({"ok": False, "exists": True, "error": "%s exists" % name})
                    path.write_text(presets.dumps(to_raw(p)), encoding="utf-8")
                    with editor.lock:
                        editor.file, editor.raw = name, to_raw(p)
                    return self._json({"ok": True, "file": name, "files": editor.files()})
                if self.path == "/api/load":
                    name = str(req.get("file") or "")
                    if not FILE_RE.match(name) or not (PRESET_DIR / name).exists():
                        return self._json({"ok": False, "error": "no preset %r" % name})
                    raw = editor.load_file(PRESET_DIR / name)
                    return self._json({"ok": True, "preset": raw, "file": name})
                if self.path == "/api/new":
                    dev = req.get("device") or {}
                    root = presets.parse_note(req.get("root", "C4"))
                    raw = presets.generate(int(req.get("points", 9)), req.get("shape", "dome"),
                                           req.get("scale", "major"), root,
                                           octaves=int(req.get("octaves", 3)),
                                           device=(dev.get("name", "mms"), dev.get("mac", "EC:47:49:CF:53:C4")))
                    p = presets.validate(raw)
                    editor.show(p)
                    with editor.lock:
                        editor.file, editor.raw = None, to_raw(p)
                    return self._json({"ok": True, "preset": to_raw(p)})
                self.send_error(404)
            except presets.PresetError as e:
                self._json({"ok": False, "error": str(e)})
            except (ValueError, TypeError, KeyError) as e:
                self._json({"ok": False, "error": "bad request: %s" % e}, 400)

    return Handler


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0],
                                 epilog=__doc__.split("\n\n", 1)[1],
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("preset", nargs="?", default=str(DEFAULT_PRESET), help="preset to open")
    ap.add_argument("--pi", default=None, help="Pi IPv4 address (default $FS_PI_HOST or 192.168.4.21)")
    ap.add_argument("--pi-port", type=int, default=8001)
    ap.add_argument("--pd", default="::1", help="PD host (default ::1)")
    ap.add_argument("--pd-port", type=int, default=12345)
    ap.add_argument("--no-pi", action="store_true", help="never send to the Pi (layout changes stay local)")
    ap.add_argument("--no-pd", action="store_true", help="never send to PD")
    ap.add_argument("--udp-port", type=int, default=12346, help="live aim/levels in (default 12346, as sphere_viz)")
    ap.add_argument("--http-port", type=int, default=8765)
    ap.add_argument("--device", default="", help="only show live data from addresses containing this")
    ap.add_argument("--no-browser", action="store_true")
    args = ap.parse_args(argv)

    import os
    pi_host = args.pi or os.environ.get("FS_PI_HOST", "192.168.4.21")
    engine = Engine(None if args.no_pi else (pi_host, args.pi_port),
                    None if args.no_pd else (args.pd, args.pd_port))
    try:
        p0 = presets.load(args.preset)
    except presets.PresetError as e:
        sys.exit("preset error: %s" % e)
    state = State(presets.points_spec(p0), p0["cone_deg"], args.device)
    editor = Editor(state, engine, args.preset)

    for sock in open_udp_sockets(args.udp_port):
        threading.Thread(target=udp_loop, args=(state, sock), daemon=True).start()
    server = ThreadingHTTPServer(("127.0.0.1", args.http_port), make_handler(editor, PAGE))
    server.daemon_threads = True
    url = "http://localhost:%d/" % args.http_port
    print("sphere editor: %s - page at %s, live data on udp/%d (Ctrl-C to stop)"
          % (Path(args.preset).name, url, args.udp_port))
    print("  sends to Pi %s, PD %s" % ("off" if args.no_pi else "%s:%d" % engine.pi,
                                       "off" if args.no_pd else "[%s]:%d" % engine.pd))
    if not args.no_browser:
        webbrowser.open(url)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    return 0


if __name__ == "__main__":
    sys.exit(main())
