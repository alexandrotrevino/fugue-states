# `run_fs.py` usage recipes

Common ways to invoke the Pi-side runtime. The systemd unit
(`deploy/fugue-states.service`) always runs `--mode button-driven`; the
recipes below are for manual iteration during development. For the OSC
remote-control vocabulary spoken by a running process, see
[`c2.md`](c2.md).

`python3 -u run_fs.py --help` lists every flag with auto-generated help
plus an examples block.

## Before you start

Manual runs need the BLE adapter to themselves — stop the systemd service first:

    sudo systemctl stop fugue-states

If you skip this you'll get `libmetawear` "Device or resource busy"
during connect. Re-enable with `sudo systemctl start fugue-states` when
done iterating.

The `-u` (unbuffered stdout) flag matters when running over SSH —
without it, prints don't reach the terminal in real time.

## Run modes

### `--mode pi-driven` (default)

Connect, auto-start every configured sensor, stream for `--stream-duration`
seconds (default 5), then stop and disconnect. Quick smoke checks.

    python3 -u run_fs.py
    python3 -u run_fs.py --stream-duration 30

### `--mode button-driven`

Connect, then idle until a device button event toggles streaming. This
is the production wearable flow. Exits on SIGINT/SIGTERM/SIGHUP or on
`/cmd/shutdown <token>` over OSC.

    python3 -u run_fs.py --mode button-driven

Per-device button gestures:

| Gesture | Effect |
|---|---|
| Double-press | Toggle streaming on this device |
| Long-press (≥ 1 s) | Enter/exit capture mode (requires `--capture-label`) |
| Single-press in capture mode | Open/close a gesture window |

## Trace recording

Every emitted frame (post-pipeline) goes to JSONL — same data the audio
plane sees, in a format `tools/analyze_*.py` and `sense.recorder.replay()`
can consume.

    python3 -u run_fs.py --record
    # → recordings/session-<timestamp>.jsonl

    python3 -u run_fs.py --record-to /tmp/diag.jsonl
    # → /tmp/diag.jsonl

## Gesture capture

Long-press to enter capture mode (LED → blue); single-press to bracket
a labeled window. Each window writes a `_gesture: start/end` marker to
the JSONL with `(label, device, instance)`. OSC emission is muted
during capture so the audio plane stays quiet.

    python3 -u run_fs.py --capture-label wave
    # → recordings/gesture-wave-<timestamp>.jsonl

`--capture-label` implies `--record`. Capture 10–30 instances per
gesture, then check intra-label consistency before training:

    python3 tools/analyze_gestures.py recordings/gesture-wave-*.jsonl

See `tools/analyze_gestures.py --help` for the discrimination metrics
(`bg-min` vs `intra-max` is the structural-cleanliness signal).

## Gesture recognition (runtime)

Load a library of captured templates, run multivariate DTW against a
rolling buffer per device, emit `/<MAC>/gesture/<label>` on match.

    python3 -u run_fs.py --mode button-driven \
        --gesture-library recordings/gesture-wave-*.jsonl,recordings/gesture-roll-*.jsonl

Most useful tuning knobs:

- `--gesture-min-std STD` — skip matching during near-stillness (default 0.3).
- `--gesture-threshold-margin MULT` — auto-threshold = max-intra-pairwise × MULT (default 1.5).
- `--gesture-zscore` — z-score normalize before DTW. Default OFF; raw
  discriminates better when amplitude carries signal (e.g. chops vs.
  waves). Flip on for shape-without-scale gestures.
- `--gesture-debug` — one log line per tick with std + best label + ratio.

The recognizer is inserted before any `Recorder` stage, so adding
`--record` captures both the IMU frames and the trigger events inline —
useful for offline validation.

## Position tracking

Requires `Sensor Fusion` mode in `fs_config.json` with `linear_acc`,
`quaternion`, and `corrected_gyro` in the `outputs` list.

    python3 -u run_fs.py --mode button-driven --position-track

The first ~5 s after streaming starts is cold-start bias calibration —
hold the wearer still while the LED renders **yellow**. After
calibration the LED returns to green and `/<MAC>/position` starts
publishing world-frame meters relative to the calibration pose.

Recalibrate live without restarting (e.g. mid-set if drift accumulates):

    # /cmd/calibrate over OSC — see "Remote control" below
    c.send_message("/cmd/calibrate", [""])  # broadcast, all devices

Tuning:

- `--position-acc-std-threshold MS2` — ZUPT acc-mag std threshold (default 0.15 m/s²).
- `--position-gyro-mag-threshold DEGS` — ZUPT gyro threshold (default 8 deg/s).
- `--position-emit-velocity`, `--position-emit-zupt` — extra outputs;
  default OFF for throughput (each emission is a downstream OSC + JSONL write).
- `--position-debug` — verbose per-tick logging.

Offline drift comparison (raw / bias-subtracted / ZUPT) on a recording:

    python3 tools/analyze_position.py recordings/<your-recording>.jsonl

## Remote control via C2

Once `run_fs.py` is up, drive it via OSC commands to `<pi-ip>:8001`. See
[`c2.md`](c2.md) for the canonical vocabulary. Quick interactive use:

```python
from pythonosc import udp_client
c = udp_client.SimpleUDPClient("192.168.4.100", 8001)

c.send_message("/cmd/status",  [])               # snapshot reply
c.send_message("/cmd/start",   [""])             # broadcast start
c.send_message("/cmd/start",   ["EC:47:..."])    # one device
c.send_message("/cmd/stop",    [""])             # broadcast stop
c.send_message("/cmd/calibrate", [""])           # recalibrate position
c.send_message("/cmd/configure/sensor",
               ["EC:47:...", "Accelerometer", "odr", 50])
c.send_message("/cmd/configure/network",
               ["192.168.4.50", 12345])
c.send_message("/cmd/shutdown", ["abc123"])      # token from /state/heartbeat
```

The Pi pushes `/state/heartbeat`, `/state/<mac>/connected|streaming|calibrating`,
and `/error/<code>` to whatever target is configured in `fs_config.local.json`.
A small pythonosc listener at that target prints them as they arrive — useful
when iterating on a controller.

## Combined recipes

### Capture + recognize on the same stream

Capture labeled windows AND have the recognizer attempt matches on the
same frames — useful for measuring recognizer behavior on ground-truth
captures:

    python3 -u run_fs.py --mode button-driven \
        --capture-label wave \
        --gesture-library recordings/gesture-wave-prior-*.jsonl

### Position tracking + recording for drift analysis

    python3 -u run_fs.py --mode button-driven --position-track --record
    # then offline:
    python3 tools/analyze_position.py recordings/session-*.jsonl

### Throughput diagnostic

If frame rates feel low, isolate firmware-side vs callback-thread side:

    python3 -u run_fs.py --record --stream-duration 10
    python3 tools/check_rates.py recordings/session-*.jsonl
