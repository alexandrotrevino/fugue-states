#!/usr/bin/env python3
"""
Live 3D view of the alignment sphere, in the browser.

Listens for OSC on --udp-port (default 12346) and serves a three.js page
on http://localhost:--http-port (default 8765). Each target of the
layout is drawn on a unit dome with its cone as a ring; it glows by how
strongly it is sounding. The aim direction is drawn as a ray with a
fading trail.

Data sources (any OSC address ending in these channel names works):

  .../aim      vec3, pointing in the sphere's own frame (Alignment stage)
  .../sphere   per-target alignment values (Alignment stage)
  .../pointing vec3 world-frame pointing (Orientation stage) — used when
               no `aim` arrives, e.g. older recordings

When `sphere` values arrive they are shown as-is (they are what the
instrument hears); otherwise alignment is computed here with the same
Alignment code the Pi runs, from --points / --cone.

Feeding it:
  * live: turn on the "viz" toggle in patch/fs_play.pd ([fs_viz] forwards
    aim + sphere from PD to 127.0.0.1:12346)
  * offline: python tools/replay_osc.py <rec.jsonl> --port 12346 --only aim,sphere,pointing

    python tools/sphere_viz.py
"""

import argparse
import json
import socket
import sys
import threading
import time
import webbrowser
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

try:
    from pythonosc.osc_message import OscMessage
except ImportError:
    sys.exit("python-osc is required: python -m pip install --user python-osc")

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from sense.pipeline import Alignment, IMUFrame, _SPHERE_LAYOUTS  # noqa: E402

DEFAULT_NOTES = "57,61,64,66,69,71,73,76,81"   # fs_play's fs_sphere for mms


class State:
    def __init__(self, points, cone, device_filter):
        self.lock = threading.Lock()
        self.alignment = Alignment(points=points, cone_deg=cone)
        self.device_filter = device_filter
        self.device = None
        self.last_t = 0.0
        self.aim = None
        self.aim_from = None          # "aim" or "pointing"
        self.aim_t = 0.0
        self.sphere = None
        self.sphere_t = 0.0
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
        if channel not in ("aim", "sphere", "pointing"):
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
            self.last_t = now
            self._rate_n += 1
            if now - self._rate_t0 >= 1.0:
                self.rate = self._rate_n / (now - self._rate_t0)
                self._rate_t0, self._rate_n = now, 0
            vals = [float(v) for v in params]
            if channel == "sphere":
                self.sphere, self.sphere_t = vals, now
            elif channel == "aim" and len(vals) == 3:
                self.aim, self.aim_from, self.aim_t = vals, "aim", now
            elif channel == "pointing" and len(vals) == 3:
                # Only fall back to pointing when no aim has arrived lately.
                if self.aim_from != "aim" or now - self.aim_t > 1.0:
                    self.aim, self.aim_from, self.aim_t = vals, "pointing", now

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
            return {"device": self.device, "aim": aim, "aim_from": self.aim_from,
                    "values": values, "source": source, "rate": round(self.rate, 1)}


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


PAGE = r"""<!doctype html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>FS Sphere</title>
<style>
  :root { --bg:#0e1116; --fg:#d8dee9; --dim:#7b8494; --bar:#2a3140; }
  html,body { margin:0; height:100%; background:var(--bg); color:var(--fg);
              font:13px/1.4 system-ui, sans-serif; overflow:hidden; }
  #hud { position:absolute; top:12px; left:12px; padding:10px 12px;
         background:rgba(14,17,22,.75); border:1px solid #232a36; border-radius:8px; min-width:210px; }
  #hud h1 { font-size:14px; margin:0 0 6px; font-weight:600; }
  .row { display:flex; align-items:center; gap:6px; margin:2px 0; }
  .lbl { width:34px; color:var(--dim); font-variant-numeric:tabular-nums; }
  .bar { flex:1; height:8px; background:var(--bar); border-radius:4px; overflow:hidden; }
  .bar > i { display:block; height:100%; width:0; }
  #meta { color:var(--dim); margin-top:6px; font-size:12px; }
  #help { position:absolute; bottom:10px; left:12px; color:var(--dim); font-size:12px; }
</style></head><body>
<div id="hud"><h1>FS sphere</h1><div id="bars"></div><div id="meta">waiting for data…</div></div>
<div id="help">drag to orbit · scroll to zoom · you stand at the centre facing the first ring note</div>
<script src="https://cdnjs.cloudflare.com/ajax/libs/three.js/r128/three.min.js"></script>
<script src="https://cdn.jsdelivr.net/npm/three@0.128.0/examples/js/controls/OrbitControls.js"></script>
<script>
const CFG = __CONFIG__;
const NOTE = ["C","C#","D","D#","E","F","F#","G","G#","A","A#","B"];
const noteName = m => NOTE[((m % 12) + 12) % 12] + (Math.floor(m / 12) - 1);
// data frame: x forward (az 0), y left (az 90, CCW), z up  ->  three: y up
const toThree = (x, y, z) => new THREE.Vector3(x, z, -y);
const dirOf = (az, el) => { const a = az * Math.PI / 180, e = el * Math.PI / 180;
  return toThree(Math.cos(e) * Math.cos(a), Math.cos(e) * Math.sin(a), Math.sin(e)); };

const renderer = new THREE.WebGLRenderer({ antialias: true });
renderer.setPixelRatio(window.devicePixelRatio);
document.body.appendChild(renderer.domElement);
const scene = new THREE.Scene();
scene.background = new THREE.Color(0x0e1116);
const camera = new THREE.PerspectiveCamera(50, 1, 0.01, 100);
camera.position.set(-2.4, 1.3, 0.9);
const controls = new THREE.OrbitControls(camera, renderer.domElement);
controls.target.set(0, 0.2, 0); controls.enableDamping = true;

// dome wireframe, horizon, floor
scene.add(new THREE.LineSegments(new THREE.WireframeGeometry(new THREE.SphereGeometry(1, 24, 12)),
  new THREE.LineBasicMaterial({ color: 0x2a3140, transparent: true, opacity: 0.45 })));
const ring = (radius, y, color, opacity) => { const pts = [];
  for (let i = 0; i <= 96; i++) { const t = i / 96 * Math.PI * 2; pts.push(new THREE.Vector3(Math.cos(t) * radius, y, Math.sin(t) * radius)); }
  return new THREE.Line(new THREE.BufferGeometry().setFromPoints(pts), new THREE.LineBasicMaterial({ color, transparent: true, opacity })); };
scene.add(ring(1, 0, 0x4c566a, 0.9));
const grid = new THREE.GridHelper(2.4, 12, 0x232a36, 0x1a1f29); grid.position.y = -1; scene.add(grid);
scene.add(new THREE.Mesh(new THREE.SphereGeometry(0.05, 16, 12), new THREE.MeshBasicMaterial({ color: 0x7b8494 })));
// front marker
const front = new THREE.ArrowHelper(toThree(1, 0, 0), new THREE.Vector3(0, -0.98, 0), 0.5, 0x4c566a, 0.08, 0.05); scene.add(front);

// targets
const targets = CFG.points.map((p, i) => {
  const dir = dirOf(p[0], p[1]);
  const hue = i / CFG.points.length;
  const color = new THREE.Color().setHSL(hue, 0.65, 0.55);
  const core = new THREE.Mesh(new THREE.SphereGeometry(0.045, 16, 12), new THREE.MeshBasicMaterial({ color: color.clone().multiplyScalar(0.35) }));
  core.position.copy(dir);
  const glow = new THREE.Mesh(new THREE.SphereGeometry(0.11, 20, 14), new THREE.MeshBasicMaterial({ color, transparent: true, opacity: 0, blending: THREE.AdditiveBlending, depthWrite: false }));
  glow.position.copy(dir);
  // cone rim on the unit sphere
  const c = CFG.cone * Math.PI / 180, up = Math.abs(dir.y) > 0.9 ? new THREE.Vector3(1, 0, 0) : new THREE.Vector3(0, 1, 0);
  const u = new THREE.Vector3().crossVectors(dir, up).normalize(), v = new THREE.Vector3().crossVectors(dir, u).normalize();
  const pts = []; for (let k = 0; k <= 72; k++) { const f = k / 72 * Math.PI * 2;
    pts.push(dir.clone().multiplyScalar(Math.cos(c)).add(u.clone().multiplyScalar(Math.sin(c) * Math.cos(f))).add(v.clone().multiplyScalar(Math.sin(c) * Math.sin(f)))); }
  const rim = new THREE.Line(new THREE.BufferGeometry().setFromPoints(pts), new THREE.LineBasicMaterial({ color, transparent: true, opacity: 0.2 }));
  // label
  const cv = document.createElement("canvas"); cv.width = 128; cv.height = 64;
  const g = cv.getContext("2d"); g.font = "bold 34px system-ui, sans-serif"; g.textAlign = "center"; g.textBaseline = "middle";
  g.fillStyle = "#" + color.getHexString(); g.fillText(CFG.notes[i] != null ? noteName(CFG.notes[i]) : String(i), 64, 32);
  const label = new THREE.Sprite(new THREE.SpriteMaterial({ map: new THREE.CanvasTexture(cv), transparent: true, depthWrite: false }));
  label.scale.set(0.22, 0.11, 1); label.position.copy(dir.clone().multiplyScalar(1.17));
  scene.add(core, glow, rim, label);
  return { core, glow, rim, color };
});

// aim ray, cursor, trail
const rayGeo = new THREE.BufferGeometry().setFromPoints([new THREE.Vector3(), new THREE.Vector3()]);
const ray = new THREE.Line(rayGeo, new THREE.LineBasicMaterial({ color: 0xeceff4 })); scene.add(ray);
const cursor = new THREE.Mesh(new THREE.SphereGeometry(0.03, 12, 10), new THREE.MeshBasicMaterial({ color: 0xffffff })); scene.add(cursor);
const TRAIL = 120, trailPos = new Float32Array(TRAIL * 3), trailCol = new Float32Array(TRAIL * 3);
const trailGeo = new THREE.BufferGeometry();
trailGeo.setAttribute("position", new THREE.BufferAttribute(trailPos, 3));
trailGeo.setAttribute("color", new THREE.BufferAttribute(trailCol, 3));
const trail = new THREE.Line(trailGeo, new THREE.LineBasicMaterial({ vertexColors: true, transparent: true, opacity: 0.9 }));
scene.add(trail); let trailN = 0;
ray.visible = cursor.visible = trail.visible = false;

// HUD
const bars = document.getElementById("bars"), meta = document.getElementById("meta");
const fills = CFG.points.map((p, i) => { const row = document.createElement("div"); row.className = "row";
  row.innerHTML = '<span class="lbl">' + (CFG.notes[i] != null ? noteName(CFG.notes[i]) : i) + '</span><span class="bar"><i></i></span>';
  bars.appendChild(row); const fill = row.querySelector("i"); fill.style.background = "#" + targets[i].color.getHexString(); return fill; });

function update(s) {
  const vals = s.values || [];
  targets.forEach((t, i) => { const a = vals[i] || 0;
    t.glow.material.opacity = Math.min(1, a * 0.9);
    t.glow.scale.setScalar(1 + a * 1.4);
    t.core.material.color.copy(t.color).multiplyScalar(0.35 + 0.65 * a);
    t.rim.material.opacity = 0.15 + 0.6 * a;
    fills[i].style.width = (a * 100).toFixed(1) + "%"; });
  if (s.aim) {
    const p = toThree(s.aim[0], s.aim[1], s.aim[2]).normalize();
    rayGeo.attributes.position.setXYZ(1, p.x * 1.02, p.y * 1.02, p.z * 1.02); rayGeo.attributes.position.needsUpdate = true;
    cursor.position.copy(p);
    trailPos.copyWithin(3, 0, (TRAIL - 1) * 3); trailPos[0] = p.x; trailPos[1] = p.y; trailPos[2] = p.z;
    trailN = Math.min(TRAIL, trailN + 1);
    for (let k = 0; k < TRAIL; k++) { const f = k < trailN ? Math.pow(1 - k / TRAIL, 1.5) : 0;
      trailCol[k * 3] = 0.55 * f; trailCol[k * 3 + 1] = 0.75 * f; trailCol[k * 3 + 2] = 0.95 * f; }
    trailGeo.attributes.position.needsUpdate = true; trailGeo.attributes.color.needsUpdate = true;
    trailGeo.setDrawRange(0, trailN);
    ray.visible = cursor.visible = trail.visible = true;
  } else { ray.visible = cursor.visible = false; }
  meta.textContent = s.device ? (s.device + " · " + s.rate + " msg/s · values: " + s.source +
    (s.aim_from === "pointing" ? " · aim from world pointing (no recenter)" : "")) : "waiting for data…";
}

const es = new EventSource("/events");
es.onmessage = e => update(JSON.parse(e.data));
es.onerror = () => { meta.textContent = "bridge not reachable — is sphere_viz.py running?"; };

function resize() { const w = window.innerWidth, h = window.innerHeight;
  renderer.setSize(w, h); camera.aspect = w / h; camera.updateProjectionMatrix(); }
window.addEventListener("resize", resize); resize();
(function loop() { requestAnimationFrame(loop); controls.update(); renderer.render(scene, camera); })();
</script></body></html>
"""


def make_handler(state, page_bytes, fps):
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *a):
            pass

        def do_GET(self):
            if self.path in ("/", "/index.html"):
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Content-Length", str(len(page_bytes)))
                self.end_headers()
                self.wfile.write(page_bytes)
            elif self.path == "/events":
                self.send_response(200)
                self.send_header("Content-Type", "text/event-stream")
                self.send_header("Cache-Control", "no-cache")
                self.end_headers()
                try:
                    while True:
                        self.wfile.write(("data: %s\n\n" % json.dumps(state.snapshot())).encode())
                        self.wfile.flush()
                        time.sleep(1.0 / fps)
                except (BrokenPipeError, ConnectionResetError, ConnectionAbortedError):
                    return
            else:
                self.send_error(404)
    return Handler


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0],
                                 epilog=__doc__.split("\n\n", 1)[1],
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--udp-port", type=int, default=12346, help="OSC listen port (default 12346)")
    ap.add_argument("--http-port", type=int, default=8765, help="web page port (default 8765)")
    ap.add_argument("--points", default="dome9",
                    help="layout name (%s) or 'az,el; ...' - must match the Pi's Alignment"
                         % ", ".join(sorted(_SPHERE_LAYOUTS)))
    ap.add_argument("--cone", type=float, default=35.0, help="cone half-angle in degrees (default 35)")
    ap.add_argument("--notes", default=DEFAULT_NOTES,
                    help="comma-separated MIDI notes per target, for labels (default: fs_play's)")
    ap.add_argument("--device", default="", help="only show addresses containing this (e.g. a MAC or 'mms')")
    ap.add_argument("--fps", type=float, default=30.0, help="browser update rate (default 30)")
    ap.add_argument("--no-browser", action="store_true", help="don't open a browser tab")
    args = ap.parse_args(argv)

    state = State(args.points, args.cone, args.device)
    notes = [int(n) for n in args.notes.split(",") if n.strip()]
    config = {"points": [list(d) for d in state.alignment._dirs], "cone": args.cone, "notes": notes}
    page = PAGE.replace("__CONFIG__", json.dumps(config)).encode("utf-8")

    for sock in open_udp_sockets(args.udp_port):
        threading.Thread(target=udp_loop, args=(state, sock), daemon=True).start()
    server = ThreadingHTTPServer(("127.0.0.1", args.http_port), make_handler(state, page, args.fps))
    server.daemon_threads = True
    url = "http://localhost:%d/" % args.http_port
    print("sphere viz: OSC on udp/%d, page at %s (Ctrl-C to stop)" % (args.udp_port, url))
    if not args.no_browser:
        webbrowser.open(url)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    return 0


if __name__ == "__main__":
    sys.exit(main())
