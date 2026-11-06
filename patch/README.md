# patch/

PlugData / Pure Data side of Fugue States. Everything here is vanilla Pd
(no ELSE / cyclone needed) unless a file says otherwise.

## Receiving sensor data

Three abstractions sit between the OSC data plane (`/<MAC>/<channel>`,
see [`docs/c2.md`](../docs/c2.md)) and your instruments:

| Object | Use |
|---|---|
| `[fs_net 12345]` | Once per patch. Listens on the port and dispatches by device. Outlets: parsed messages (debug), messages/sec. |
| `[fs_device <name> <MAC>]` | Once per sensor. Gives the MAC a short name and prints the channel list the Pi advertises. |
| `[fs_in <name> <channel>]` | Anywhere. Outputs one channel: a float for scalars, a list for vectors. |

```
[fs_net 12345]
[fs_device mmrl E0:2D:64:47:02:F5]

[fs_in mmrl tilt]          [fs_in mmrl gesture/roll]
|                          |
[/ 180]                    (confidence 0..1 on each trigger)
```

Channel names are whatever the Pi pipeline emits: `acc`, `acc_lp`,
`acc_mag`, `tilt`, `gyro`, `gyro_mag`, `mag`, `quat`, `linear_acc`,
`position`, `gesture/<label>`, and any stage outputs added at runtime
(`*_hp`, `*_edge`, `*_scaled`, ...). The console prints the live list
when a device advertises.

`fs_demo.pd` wires both sensors to sliders and number boxes (no sound).

## Control helpers

| Object | Use |
|---|---|
| `[fs_map in_lo in_hi out_lo out_hi]` | Clip + linear map. Either range may be inverted. |
| `[fs_onset threshold rearm refractory_ms]` | Fires once per crossing, outputting the crossing value. `2 1.3 120` suits `acc_mag` flicks. |
| `[fs_quantize root octaves]` | 0..1 → MIDI note on a scale (default major pentatonic). Accepts `scale 0 2 4 ...`. |
| `[fs_smooth up down]` | Asymmetric one-pole per incoming value: fast attack, slow ring-out (e.g. `0.5 0.06`). |

Calibration reference (from the May 2026 recordings): `acc_mag` rests
≈1 g, flicks reach 3–4 g; `gyro_mag` 0–600 °/s while moving, near 0
when still; `tilt` spans ~10–150° in practice.

Sensor-fusion sensors (Pi pipeline: `Orientation` on `quat`,
`Magnitude` on `linear_acc` and `corrected_gyro`; Nov 2026 session):
`elevation` spans about -60..+85°; `corrected_gyro_mag` is deg/s,
0.1–0.5 at rest and up to 1,700 when spun; `linear_acc_mag` is
~0.01 g at rest, 1.8 g p90 and 4.6 g p99 while moving (no 2 g
saturation). During continuous motion `linear_acc_mag` rarely drops
below ~0.5 g, so onset re-arm levels must sit above that. `twist`
wraps at ±180° and natural holds vary, so map it through sin/cos
around a center, never linearly.

## Instruments

Sound and mapping are separate: a **voice** takes plain 0..1 inputs, a
**wrapper** decides which motion drives which input.

| Voice | Inputs |
|---|---|
| `[fs_drone_voice root]` | pitch 0..1 (or `scale ...`), energy 0..1, brightness 0..1 |
| `[fs_pluck_voice root]` | pitch 0..1 (cold, or `scale ...`), velocity 0..1 (plays) |

| Wrapper | Sensor | Plays |
|---|---|---|
| `[fs_drone name root]` | raw IMU | Tilt → pitch; rotation speed → loudness and brightness. Silent when still. |
| `[fs_pluck name root]` | raw IMU | Tilt chooses the note, an `acc_mag` flick plays it. |
| `[fs_drone_point name root]` | fusion | Point up for higher notes; rotation swells it; wrist twist opens/closes the filter. Right inlet: twist center (deg). |
| `[fs_pluck_point name root]` | fusion | Point to choose the note; a gravity-free flick (`linear_acc_mag`) plays it, so tilting never triggers. |
| `[fs_hit name label freq]` | either | Percussive hit on `gesture/<label>`; loudness = match confidence. |
| `[fs_sphere name n1 … n9]` | fusion + `Alignment` | Notes placed on a dome around you (8 in a ring just above the horizon, 1 overhead). Point at one to bring it in, point dead-on and move to recruit overtones (`fs_partials`), leave it to let it ring out. Arm hanging = silence. |

Each outputs a signal (`outlet~`); drones and plucks take `scale ...`
messages on their left inlet. `fs_play.pd` puts them together —
raw-IMU instruments on `mmrl`, fusion instruments on `mms` — with
per-instrument toggles, scale buttons, a "center twist" button and a
master volume.

## Talking back to the Pi (C2)

`[fs_cmd <pi_ipv4> 8001]` (once per patch) opens the control
connection; `[fs_tune <mac> <pipeline> <stage> <param>]` turns each
value it receives into `/cmd/pipeline/set ...` — applied live on the
Pi and persisted to its `fs_config.local.json`. `fs_play.pd` uses this
for **recenter sphere**: it sends the current `heading` as the
`Alignment` stage's `yaw_offset`, making "where I point now" the
front. The Pi's IP is DHCP-assigned — update the `fs_cmd` box if it
moves.

## Developing without the Pi

    python tools/replay_osc.py --synthetic
    python tools/replay_osc.py recordings/<file>.jsonl --loop

Long live sessions are mostly idle; `tools/trim_active.py` cuts one
down to its moving stretches (rotation above 60 deg/s, padded, gaps
collapsed to 1 s) as `<name>-active.jsonl`. Good replay material from
2026-11-06: `live-fusion-20261106-active.jsonl` (both sensors, mms in
fusion mode with the Orientation channels, 3 live `gesture/wave`
triggers) and `live-play-20261106-active.jsonl` (mmrl playing).

Sends to `::1:12345` by default (PD on Windows listens IPv6-only).

## Headless tests

`patch/tests/` holds test patches driven by `tools/pdtest.py` against a
headless vanilla Pd whose version matches PlugData's embedded core
(0.55-2 at the moment):

    <pd>/bin/pd.com -nogui -noprefs -noaudio -stderr -open patch/tests/test_receiver.pd 2> pd.log
    python tools/pdtest.py run receiver --log pd.log

`python tools/pdtest.py list` shows the available tests. Headless tests
check wiring and values only — sound and feel still get checked by ear
in PlugData.

## Layout

- `fs_*.pd` — receiver layer (above).
- Top-level sketches (`spatial_trigger`, `peak_hold`, `wobble`, ...) —
  2024 instrument experiments, not yet wired to the new receiver.
- `legacy/` — the 2024 receive/parse/configure patches, kept for
  reference. They speak the pre-revival OSC vocabulary.
