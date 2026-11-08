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
| `[fs_smooth attack_ms release_ms]` | Asymmetric smoother with time constants in ms, using the real time between values (rate-independent). Inlets 2/3 retune attack/release live. |

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
| `[fs_sphere name n1 n2 …]` | fusion + `Alignment` | Notes on a sphere around you, one voice per target of the Pi's layout — any number up to 64 (`[clone]` of `fs_sphere_voice`; silent voices switch their DSP off). Args are the first notes; the rest default to C4 until a preset sets them. Point at one to bring it in, point dead-on and move to recruit overtones (`fs_partials`). **Motion-gated**: silent while you hold still (e.g. the start pose), fades within ~2 s of stopping. Settings inlet: `release <ms>`, `release_n <i> <ms>`, `attack <ms>`, `attack_n <i> <ms>`, `note <i> <midi>`, `gate <0..1>` (0 = pointing alone). |

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

## Sphere presets

An instrument preset (`presets/*.json`) holds a sphere's whole design:
target positions, zones (per target if wanted), notes, attack/release
and gate. `python tools/preset.py show <file>` prints it;
`python tools/preset.py apply <file>` sends the layout to the Pi (live,
persisted) and notes/envelopes/gate to PD (live; re-apply after
reopening the patch). `--dry-run` shows the messages. The viewer can
draw a preset: `python tools/sphere_viz.py --preset <file>`.

Start a new layout with `python tools/preset.py new <file> --points 20`:
points spread evenly over a `--shape` (`dome`, `sphere` or `ring`),
notes climbing a `--scale` from `--root` (e.g. `A3`) as you point
higher, over `--octaves` (default 3; with more points than notes in that
range, neighbours share a pitch), and a zone sized to the spacing. Up to
64 targets.

## Editing the sphere

`python tools/sphere_edit.py [preset]` opens the editor in the browser
(one shell; it replaces running `preset.py apply` alongside the viewer):

- drag a note across the sphere to move it (5° snapping; hold Shift for
  free placement); double-click empty sphere to add a note there (it
  takes its nearest neighbour's pitch); Del removes the selection
- ↑/↓ change the selected note by a semitone (Shift: an octave); the
  panel edits pitch (`C4`, `F#3` or a MIDI number), position, and the
  note's own zone/attack/release (unchecked = follows the whole sphere)
- whole-sphere zone, gate, attack, release; undo/redo (Ctrl+Z / Ctrl+Y);
  Ctrl+S saves
- open/save presets in `presets/`; "New layout from N points" is
  `preset.py new` (points, shape, scale, root, octaves)

With **live** on, every change is heard at once: layout and zones go to
the Pi, notes/envelopes/gate to PD - only what changed. **Send all**
re-sends everything; use it after reopening `fs_play.pd` (PD forgets
sphere settings) or if you edited with live off. It also shows your
live aim and each note's level like the viewer (switch on **viz** in
`fs_play.pd`). `--no-pi` keeps layout edits local; `--no-pd` leaves
the patch alone.

## Seeing the sphere

`python tools/sphere_viz.py` opens a live 3D view in the browser: the
dome's notes with their cones, glowing by how loudly each sounds, and
your aim as a ray with a trail. Switch on **viz** in `fs_play.pd`
(`[fs_viz mms 12346]` forwards `aim` + `sphere`). Offline, point
`replay_osc.py` straight at it: `--port 12346 --only aim,sphere,pointing`
(older recordings only have `pointing`; the viewer then computes the
alignment itself, without recentering).

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
