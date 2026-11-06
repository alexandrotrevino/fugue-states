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

Calibration reference (from the May 2026 recordings): `acc_mag` rests
≈1 g, flicks reach 3–4 g; `gyro_mag` 0–600 °/s while moving, near 0
when still; `tilt` spans ~10–150° in practice.

## Instruments

| Object | Plays |
|---|---|
| `[fs_drone name root]` | Tilt → pitch (glides between scale notes); rotation speed → loudness and brightness. Silent when still. |
| `[fs_pluck name root]` | Tilt chooses the note, a flick plays it; flick strength → loudness. |
| `[fs_hit name label freq]` | Percussive hit on `gesture/<label>`; loudness = match confidence. |

Each outputs a signal (`outlet~`); the drone and pluck take `scale ...`
messages on their inlet. `fs_play.pd` puts them together for both
sensors with per-instrument toggles, scale buttons and a master volume.

## Developing without the Pi

    python tools/replay_osc.py --synthetic
    python tools/replay_osc.py recordings/<file>.jsonl --loop

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
