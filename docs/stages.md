# Preprocessing Stages Library

The pipeline (`sense/pipeline.py`) ships a library of small, composable
`Stage`s. Each per-(device, sensor) pipeline takes raw IMU frames and
runs them through an ordered chain, ending at a terminal `OscEmit` that
publishes `/<MAC>/<sensor>` over OSC.

This doc is the catalog: what each stage does, what it emits, and the
common composition recipes. For the runtime configuration API
(`/cmd/pipeline/list|inspect|set|add|remove`) see
[c2.md](c2.md#pipeline-configuration).

## Quick reference

| Stage | Input arity | Default output sensor | Tunable params |
|---|---|---|---|
| `LowPass(cutoff_hz, fs)` | any | replaces values (in-place) | `cutoff_hz` |
| `HighPass(cutoff_hz, fs)` | any | `<sensor>_hp` | `cutoff_hz` |
| `Magnitude()` | vec | `<sensor>_mag` (scalar L2) | — |
| `Tilt()` | vec3 (acc) | `tilt` (deg from gravity) | — |
| `Differentiator()` | any | `<sensor>_d` (per-axis dv/dt) | — |
| `EdgeDetector(threshold, ...)` | scalar | `<sensor>_edge` (events, `1.0`) | `threshold`, `hysteresis` |
| `Window(n_samples, stat)` | any | `<sensor>_<stat>` (per-axis) | `stat` |
| `Scale(scale, offset)` | any | `<sensor>_scaled` | `scale`, `offset` |
| `OscEmit(osc_client)` | any | terminal (publishes via UDP) | — |

All stages with `CONSTRUCTION_PARAMS` declared (everything above except
`OscEmit`, `Magnitude`, `Tilt`, `Differentiator` whose constructors
take no scalar params) can be added at runtime via
`/cmd/pipeline/add`. Parameter-less stages get added by name only.

## Default emit mode

Most stages run in **derived** mode by default: the raw frame passes
through unchanged AND a new frame is emitted under a derived sensor
name. Downstream stages see both.

`LowPass` is the exception — it defaults to **in-place** (replaces the
input frame's values, single output). Most callers want "the smoothed
signal IS the signal," so in-place is the right shape.

Every derived-emit stage takes an optional `output_sensor` to override
the default sensor name (e.g.
`HighPass(cutoff_hz=0.5, fs=25, output_sensor="acc_motion")`).

## Per-stage notes

### LowPass

Single-pole IIR. `α = dt / (RC + dt)`, `RC = 1/(2π·cutoff_hz)`. Per-
(device, sensor) state, so one instance can be inserted into a
pipeline that sees multiple sensors. Live retunes smoothly on
`cutoff_hz` change — state stays valid.

### HighPass

Mirror of LowPass with `α = RC / (RC + dt)` and recursion
`y[n] = α(y[n-1] + x[n] - x[n-1])`. First-frame output is zero (no
prior delta seen yet); the filter warms over a few time constants.

### Magnitude

L2 norm: `sqrt(sum(v² for v in values))`. Emits a scalar.

### Tilt

Acc-specific. Emits angle from gravity in degrees. 0° = z-up,
90° = on edge, 180° = z-down. Meaningful only when the device isn't
under strong linear acceleration (motion contaminates the gravity
estimate). For motion-aware tilt, use the fusion `linear_acc` output
(removes gravity).

### Differentiator

Numerical first derivative. First frame for a given (device, sensor)
yields the pass-through but no derivative — the recursion needs two
samples. Subsequent frames divide by the actual `t_recv` delta, so
jitter in the sample rate is reflected in the output (no
fixed-`fs` assumption).

### EdgeDetector

Scalar input only. Vec inputs log a one-shot warning per (device,
sensor) and yield the pass-through with no event — to detect on a
vec, compose `Magnitude` upstream.

`hysteresis` ≥ 0 sets the re-arm gap: after a rising edge, the
detector won't fire again until the signal falls below
`threshold - hysteresis`. Mirror for falling edges. Set `0.0` to
disable hysteresis (every threshold-cross fires).

`direction ∈ {"rising", "falling", "either"}` — `"either"` arms both
directions independently with separate hysteresis re-arm conditions.

### Window

Rolling N-sample deque per (device, sensor). `stat ∈ {mean, std,
max, min, range, sum}`. Output arity matches input arity (per-axis
stat computation). `n_samples` is fixed at construction (not in
TUNABLE_PARAMS — resizing the deque mid-stream is messy state surgery
that the current Tier-1 mutation model doesn't cover).

During warm-up (before the window fills), the stat is computed over
whatever samples have arrived so far — partial-window output is
preferred over silence, because operators usually want to see
*something* during the warm-up window.

### Scale

Stateless affine map: `y = (x - offset) * scale`, per axis. The
canonical "map IMU range to PD/DAW parameter range" stage. Cheap;
compose freely.

## Composition recipes

### Motion vs. gravity split (non-fusion configs)

```
acc → HighPass(cutoff_hz=0.5, fs=25, output_sensor="acc_motion")
    → LowPass(cutoff_hz=0.5, fs=25, output_sensor="acc_gravity")
    → OscEmit
```

Three frames per acc: raw, `acc_motion` (HP — motion-only),
`acc_gravity` (LP — gravity estimate). For full-quality motion
isolation prefer fusion mode's `linear_acc`; this is the non-fusion
fallback.

### Jerk trigger (rate-of-change burst)

```
acc → Magnitude               # acc_mag (scalar)
    → Differentiator          # acc_mag_d (jerk magnitude)
    → EdgeDetector(threshold=15.0, hysteresis=2.0)
                              # acc_mag_d_edge (event on burst)
    → OscEmit
```

PD subscribes to `/<MAC>/acc_mag_d_edge` for one-shot triggers when
jerk crosses 15 m/s³. Hysteresis prevents noise re-triggering.

### Shake detector

```
acc → Magnitude               # acc_mag
    → Window(n_samples=12, stat="std")
                              # acc_mag_std (std over last ~0.5s @ 25Hz)
    → EdgeDetector(threshold=2.0, hysteresis=0.5)
                              # acc_mag_std_edge (event)
    → OscEmit
```

Rolling std crossing a threshold fires when the device transitions
from still to shaken. Stillness gate is the same primitive with the
opposite direction.

### Tilt → normalized 0..1 for a filter cutoff

```
acc → Tilt                                       # tilt (0–180°)
    → Scale(scale=1/90, offset=0, output_sensor="cutoff_norm")
                                                 # cutoff_norm (0–2)
    → OscEmit
```

PD reads `/<MAC>/cutoff_norm` and maps it onto a filter cutoff
range. For a clamped 0..1 a downstream `Clip` stage would help — not
yet in the library; for now choose `scale` conservatively.

### Window mean as a debouncer

```
acc_button_pressed → Window(n_samples=5, stat="mean")
                   → EdgeDetector(threshold=0.5, direction="rising")
                   → OscEmit
```

For a noisy 0/1 signal, the mean of the last 5 frames crossing 0.5
fires only when ≥3 of the last 5 were 1 — a soft majority vote.

## Live configuration

All registered stages can be added/removed/tuned at runtime via the
C2 OSC vocabulary. Example — slide a low-pass cutoff while listening:

```
/cmd/pipeline/set <mac> acc LowPass cutoff_hz 8.0
```

Persistence is debounced (slider drags coalesce into a single write).
For full grammar see [c2.md — Pipeline configuration](c2.md#pipeline-configuration).

## Performance notes

Per-stage timing is in `Pipeline.stats[<StageName>]` (rolling 1024
samples; `count`, `mean_s`, `max_s`). The aggregate budget on the BLE
callback thread should stay well under the inter-frame period at the
configured ODR — at 25 Hz that's 40 ms; the current 3-stage chain
costs ~50 µs end-to-end per frame on the Pi. Recorder write latency
is covered by the `recorder-soak-write-latency` soak in
`sense/stress.py`.

If a new stage you add is heavier than expected (e.g. a Window with
`n_samples` in the thousands), check `Pipeline.stats` before
attributing latency issues to BLE — the in-process pipeline is
usually orders of magnitude faster, but it's worth verifying.
