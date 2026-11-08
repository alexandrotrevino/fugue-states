// Minimal Chrome DevTools Protocol driver (Node 22: global WebSocket/fetch).
// usage: node cdp.mjs <debug-port> <page-url> <scenario.mjs> <shot-dir>   (normally via run.py)
import { pathToFileURL } from "node:url";
import { writeFileSync } from "node:fs";

const [port, url, scenarioPath, shotDir] = process.argv.slice(2);
const sleep = ms => new Promise(r => setTimeout(r, ms));

let target;
for (let i = 0; i < 50; i++) {
  try { target = (await (await fetch(`http://127.0.0.1:${port}/json/list`)).json()).find(t => t.type === "page"); if (target) break; } catch {}
  await sleep(200);
}
if (!target) { console.log("FAIL no chrome page"); process.exit(1); }
const ws = new WebSocket(target.webSocketDebuggerUrl);
await new Promise(r => ws.addEventListener("open", r));
let id = 0; const pending = new Map(); const problems = [];
ws.addEventListener("message", ev => {
  const m = JSON.parse(ev.data);
  if (m.id && pending.has(m.id)) { pending.get(m.id)(m); pending.delete(m.id); return; }
  if (m.method === "Runtime.exceptionThrown") problems.push("EXCEPTION " + JSON.stringify(m.params.exceptionDetails.exception?.description || m.params.exceptionDetails.text));
  if (m.method === "Runtime.consoleAPICalled" && (m.params.type === "error" || m.params.type === "warning"))
    problems.push("CONSOLE." + m.params.type + " " + m.params.args.map(a => a.value ?? a.description).join(" "));
  if (m.method === "Log.entryAdded" && m.params.entry.level === "error") problems.push("LOG " + m.params.entry.text + " " + (m.params.entry.url || ""));
  if (m.method === "Page.javascriptDialogOpening") { send("Page.handleJavaScriptDialog", { accept: true, promptText: globalThis.__promptText || "" }); }
});
function send(method, params = {}) {
  const i = ++id; ws.send(JSON.stringify({ id: i, method, params }));
  return new Promise(r => pending.set(i, r));
}
async function evaluate(expr) {
  const r = await send("Runtime.evaluate", { expression: expr, awaitPromise: true, returnByValue: true });
  if (r.result?.exceptionDetails) throw new Error("eval failed: " + expr + " -> " + JSON.stringify(r.result.exceptionDetails.exception?.description));
  return r.result?.result?.value;
}
async function mouse(type, x, y, extra = {}) { await send("Input.dispatchMouseEvent", { type, x, y, button: "left", buttons: type === "mouseReleased" ? 0 : 1, clickCount: 1, ...extra }); }
async function key(k, code, vk, mods = 0) {
  await send("Input.dispatchKeyEvent", { type: "keyDown", key: k, code, windowsVirtualKeyCode: vk, modifiers: mods });
  await send("Input.dispatchKeyEvent", { type: "keyUp", key: k, code, windowsVirtualKeyCode: vk, modifiers: mods });
}
async function shot(name) {
  const r = await send("Page.captureScreenshot", { format: "png" });
  writeFileSync(`${shotDir}/${name}.png`, Buffer.from(r.result.data, "base64"));
}
await send("Runtime.enable"); await send("Log.enable"); await send("Page.enable");
await send("Emulation.setDeviceMetricsOverride", { width: 1280, height: 800, deviceScaleFactor: 1, mobile: false });
await send("Page.navigate", { url });
await sleep(2500);
const api = { evaluate, mouse, key, shot, sleep, send, problems, setPrompt: t => { globalThis.__promptText = t; } };
let failed = false;
const check = (ok, what) => { console.log((ok ? "ok   " : "FAIL ") + what); if (!ok) failed = true; };
try {
  const scenario = await import(pathToFileURL(scenarioPath).href);
  await scenario.default(api, check);
} catch (e) { console.log("FAIL scenario threw: " + e.message); failed = true; }
for (const p of problems) console.log("PROBLEM " + p);
if (problems.length) failed = true;
console.log(failed ? "RESULT FAIL" : "RESULT PASS");
ws.close(); process.exit(failed ? 1 : 0);
