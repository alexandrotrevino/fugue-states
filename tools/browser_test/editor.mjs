import dgram from "node:dgram";
import { readFileSync, existsSync, unlinkSync } from "node:fs";

const SINK = process.env.SINK, PRESETS = process.env.PRESETS, LIVE = Number(process.env.LIVE_PORT || 12399);
const sinkLines = () => existsSync(SINK) ? readFileSync(SINK, "utf8").split(/\r?\n/).filter(Boolean) : [];

function osc(address, floats) {
  const pad = b => Buffer.concat([b, Buffer.alloc(4 - (b.length % 4))]);
  const parts = [pad(Buffer.from(address)), pad(Buffer.from("," + "f".repeat(floats.length)))];
  for (const f of floats) { const b = Buffer.alloc(4); b.writeFloatBE(f); parts.push(b); }
  return Buffer.concat(parts);
}

export default async function (api, check) {
  const { evaluate: ev, mouse, key, shot, sleep } = api;
  const screenOf = expr => ev(`(() => { const v = (${expr}).clone().project(camera), r = renderer.domElement.getBoundingClientRect();
      return [r.left + (v.x + 1) / 2 * r.width, r.top + (1 - v.y) / 2 * r.height, v.z]; })()`);

  check(await ev("T().length") === 9 && await ev("objs.length") === 9 && await ev("listRows.length") === 9, "opens dome9 (9 notes drawn + listed)");
  check(await ev("document.getElementById('file').textContent") === "dome9-a-major-pent.json", "file name shown");
  await shot("1-initial");

  // --- drag note 1 (front) to the right on screen
  let n0 = sinkLines().length;
  const [x, y] = await screenOf("objs[0].core.position");
  await mouse("mouseMoved", x, y); await mouse("mousePressed", x, y);
  for (let k = 1; k <= 10; k++) { await mouse("mouseMoved", x + k * 9, y - k * 4); await sleep(20); }
  await mouse("mouseReleased", x + 90, y - 40); await sleep(400);
  const t0 = await ev("T()[0]");
  check(t0.az !== 0 && t0.az % 5 === 0 && t0.el % 5 === 0, `drag moved note 1 with 5° snapping -> az ${t0.az}, el ${t0.el}`);
  check(await ev("sel") === 0 && await ev("undoStack.length") === 1, "drag selects the note, one undo step");
  let new_ = sinkLines().slice(n0);
  const lastPts = new_.filter(l => l.startsWith("PI") && l.includes(" points ")).pop() || "";
  check(lastPts.includes(`${t0.az},${t0.el}`) || lastPts.includes(`${t0.az},${t0.el};`), "Pi got the moved point: " + lastPts.slice(0, 120));
  check(new_.some(l => l.startsWith("PI") && l.includes(" cone_deg ")), "first live apply also sent cone_deg");
  check(new_.filter(l => l.startsWith("PD")).length >= 9 * 1 + 3, `first live apply sent PD the whole preset (${new_.filter(l => l.startsWith("PD")).length} msgs)`);
  check(new_.filter(l => l.includes("sphere_cfg audition")).length === 1 && new_.some(l => l.endsWith("audition 1.0 700.0")),
    "pressing on note 1 auditioned it once: " + JSON.stringify(new_.filter(l => l.includes("audition"))));
  await shot("2-dragged");

  // --- semitone up with the arrow key -> exactly one PD note message
  n0 = sinkLines().length;
  const before = await ev("T()[0].note");
  await key("ArrowUp", "ArrowUp", 38); await sleep(300);
  new_ = sinkLines().slice(n0);
  check(await ev("T()[0].note") === before + 1, "ArrowUp raises the note a semitone");
  check(new_.length === 2 && new_[0] === `PD /EC:47:49:CF:53:C4/sphere_cfg note 1.0 ${before + 1}.0` &&
        new_[1] === "PD /EC:47:49:CF:53:C4/sphere_cfg audition 1.0 700.0",
        "only the changed note was sent, then auditioned at its new pitch: " + JSON.stringify(new_));

  // --- hold Space: repeated auditions while held, none after release
  n0 = sinkLines().length;
  await api.send("Input.dispatchKeyEvent", { type: "keyDown", key: " ", code: "Space", windowsVirtualKeyCode: 32 });
  await sleep(700);
  await api.send("Input.dispatchKeyEvent", { type: "keyUp", key: " ", code: "Space", windowsVirtualKeyCode: 32 });
  await sleep(100);
  const held = sinkLines().slice(n0);
  await sleep(500);
  check(held.length >= 3 && held.every(l => l.endsWith("audition 1.0 450.0")) && sinkLines().length === n0 + held.length,
    `holding Space re-sends the audition (${held.length}x) and stops on release`);

  // --- hear on click off: pressing a note sends nothing
  await ev(`document.getElementById('hearOn').click()`);
  n0 = sinkLines().length;
  { const [hx, hy] = await screenOf("objs[1].core.position");
    await mouse("mouseMoved", hx, hy); await mouse("mousePressed", hx, hy); await mouse("mouseReleased", hx, hy); await sleep(300); }
  check(await ev("sel") === 1 && sinkLines().length === n0, "hear off: selecting a note sends nothing");
  await ev(`document.getElementById('hearOn').click()`); await ev("select(0)");

  // --- double-click empty sphere -> add a note there
  const spot = await ev(`(() => { const camDir = camera.position.clone().normalize();
      for (let el = 60; el >= -30; el -= 10) for (let az = 0; az < 360; az += 15) {
        const d = dirOf(az, el); if (d.dot(camDir) < 0.4) continue;
        if (T().every(t => dirOf(t.az, t.el).dot(d) < Math.cos(30 * Math.PI / 180))) return [az, el]; }
      return null; })()`);
  check(!!spot, "found an empty visible spot " + JSON.stringify(spot));
  const [sx, sy] = await screenOf(`dirOf(${spot[0]}, ${spot[1]})`);
  n0 = sinkLines().length;
  await mouse("mouseMoved", sx, sy);
  await mouse("mousePressed", sx, sy, { clickCount: 1 }); await mouse("mouseReleased", sx, sy, { clickCount: 1 });
  await mouse("mousePressed", sx, sy, { clickCount: 2 }); await mouse("mouseReleased", sx, sy, { clickCount: 2 });
  await sleep(400);
  const added = await ev("T()[T().length - 1]");
  check(await ev("T().length") === 10 && await ev("sel") === 9, "double-click added note 10 and selected it");
  check(Math.abs(added.az - spot[0]) <= 5 && Math.abs(added.el - spot[1]) <= 5, `added where clicked: ${added.az},${added.el} vs ${spot}`);
  new_ = sinkLines().slice(n0);
  check(new_.some(l => l.startsWith("PI") && l.split(";").length === 10), "Pi got a 10-point layout");
  check(new_.some(l => l.includes("note 10.0")), "PD got note 10's pitch");
  await shot("3-added");

  // --- release override on the selected note via the panel
  n0 = sinkLines().length;
  await ev(`(() => { const cb = document.querySelector('[data-ov="release_ms"]'); cb.click(); })()`);
  await ev(`(() => { const i = document.querySelector('[data-ovv="release_ms"]'); i.value = 2500; i.dispatchEvent(new Event('change')); })()`);
  await sleep(400);
  check(await ev("T()[9].release_ms") === 2500, "per-note release override set");
  new_ = sinkLines().slice(n0);
  check(new_.some(l => l.endsWith("release_n 10.0 2500.0")), "PD got release_n 10 2500: " + JSON.stringify(new_));

  // --- delete, undo, redo
  await key("Delete", "Delete", 46); await sleep(300);
  check(await ev("T().length") === 9, "Delete removes the selected note");
  await key("z", "KeyZ", 90, 2); await sleep(300);
  check(await ev("T().length") === 10 && await ev("T()[9].release_ms") === 2500, "Ctrl+Z restores it (with its override)");
  await key("y", "KeyY", 89, 2); await sleep(300);
  check(await ev("T().length") === 9, "Ctrl+Y redoes the delete");

  // --- whole-sphere zone slider
  n0 = sinkLines().length;
  await ev(`(() => { const s = document.getElementById('gCone'); s.value = 28; s.dispatchEvent(new Event('input')); s.dispatchEvent(new Event('change')); })()`);
  await sleep(400);
  new_ = sinkLines().slice(n0);
  check(await ev("P.sphere.cone_deg") === 28 && new_.some(l => l.endsWith("cone_deg 28.0")), "zone slider -> Pi cone_deg 28");
  check(!new_.some(l => l.startsWith("PD")), "zone change sends nothing to PD");

  // --- global release -> PD global, then overrides re-sent
  n0 = sinkLines().length;
  await ev(`(() => { const i = document.getElementById('gRelN'); i.value = 900; i.dispatchEvent(new Event('change')); })()`);
  await sleep(400);
  new_ = sinkLines().slice(n0);
  check(new_.length === 1 && new_[0].endsWith("sphere_cfg release 900.0"), "global release: one PD message " + JSON.stringify(new_));

  // --- live data in: aim + sphere values on the live port
  const sock = dgram.createSocket("udp4");
  const vals = new Array(9).fill(0); vals[2] = 0.8;
  for (let k = 0; k < 20; k++) {
    sock.send(osc("/fs/mms/aim", [0, 1, 0.2]), LIVE, "127.0.0.1");
    sock.send(osc("/fs/mms/sphere", vals), LIVE, "127.0.0.1");
    sock.send(osc("/fs/mms/gate", [1]), LIVE, "127.0.0.1");
    await sleep(25);
  }
  await sleep(500);
  check((await ev("document.getElementById('live').textContent")).startsWith("mms"), "live data shown: " + await ev("document.getElementById('live').textContent"));
  check(await ev("curA[2]") > 0.5 && await ev("ray.visible"), "levels + aim ray follow live data");

  // --- recenter: needs a live heading; sends it to the Pi as yaw_offset
  for (let k = 0; k < 5; k++) { sock.send(osc("/fs/mms/heading", [42.5]), LIVE, "127.0.0.1"); await sleep(30); }
  await sleep(400);
  check(!(await ev("document.getElementById('recenter').disabled")), "Recenter enabled once a heading arrives");
  n0 = sinkLines().length;
  await key("r", "KeyR", 82); await sleep(400);
  new_ = sinkLines().slice(n0);
  check(new_.length === 1 && new_[0] === "PI /cmd/pipeline/set EC:47:49:CF:53:C4 quat Alignment yaw_offset 42.5",
    "R recentered: " + JSON.stringify(new_) + " / " + await ev("document.getElementById('status').textContent"));
  await shot("4-live");
  sock.close();

  // --- generate 50 points (dirty -> confirm auto-accepted)
  n0 = sinkLines().length;
  await ev(`document.getElementById('nPoints').value = 50`);
  await ev(`document.getElementById('gen').click()`); await sleep(1500);
  check(await ev("T().length") === 50 && await ev("objs.length") === 50, "generated 50-point dome");
  new_ = sinkLines().slice(n0);
  check(new_.some(l => l.startsWith("PI") && l.split(";").length === 50), "Pi got the 50-point layout");
  check(new_.some(l => l.includes("note 50.0")), "PD got note 50");
  await shot("5-fifty");

  // --- save as, then reopen it
  api.setPrompt("edtest-tmp.json");
  await ev(`document.getElementById('saveAs').click()`); await sleep(600);
  const saved = PRESETS + "/edtest-tmp.json";
  check(existsSync(saved) && JSON.parse(readFileSync(saved, "utf8")).sphere.targets.length === 50, "Save as wrote presets/edtest-tmp.json");
  check(!(await ev("document.getElementById('file').classList.contains('dirty')")), "saved -> not dirty");
  await ev(`(() => { const s = document.getElementById('files'); s.value = 'arc7-d-dorian.json'; s.dispatchEvent(new Event('change')); })()`);
  await sleep(800);
  check(await ev("T().length") === 8 && await ev("FILE") === "arc7-d-dorian.json", "opened arc7 from the list " +
    JSON.stringify(await ev("[FILE, T().length, document.getElementById('status').textContent, document.getElementById('files').value]")));
  await ev(`(() => { const s = document.getElementById('files'); s.value = 'edtest-tmp.json'; s.dispatchEvent(new Event('change')); })()`);
  await sleep(800);
  check(await ev("T().length") === 50, "reopened the saved 50-point preset");
  if (existsSync(saved)) unlinkSync(saved);

  // --- live off: edits stay local
  await ev(`document.getElementById('liveOn').click()`); await sleep(300);
  n0 = sinkLines().length;
  await ev("select(0)"); await key("ArrowUp", "ArrowUp", 38); await sleep(400);
  check(sinkLines().length === n0, "live off: nothing sent");
  // send all forces everything
  await ev(`document.getElementById('sendAll').click()`); await sleep(1200);
  new_ = sinkLines().slice(n0);
  check(new_.filter(l => l.startsWith("PD")).length >= 50 && new_.some(l => l.startsWith("PI")), `Send all re-sent everything (${new_.length} msgs)`);
  await shot("6-final");
}
