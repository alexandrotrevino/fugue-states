import argparse
import atexit
import logging
import os
import signal
import sys
import time
from datetime import datetime, timezone
from time import sleep

from sense.c_stderr import reroute_c_stderr_to_log
from sense.c2 import Controller
from sense.fs_setup import read_fugue_states_config, validate_config
from sense.osc import ControlledOSCConnection
from sense.state import MetaWearState
from sense.pipeline import (
    Latch, LatchUpdate, LowPass, Magnitude, Tilt, OscEmit,
    apply_pipeline_overrides,
)
from sense.recorder import Recorder, RecorderSink
from sense.gesture import GestureLibrary, GestureRecognizer
from sense.position import PositionTracker

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    datefmt="%H:%M:%S",
)
# When the controlling SSH session goes away mid-shutdown, stderr writes
# raise BrokenPipeError. Swallow logging errors so they don't abort the
# cleanup work itself.
logging.raiseExceptions = False
# Route libmetawear's C-level BLE noise through a logger at DEBUG so
# INFO-level runs stay clean. Must happen after basicConfig but before
# any libmetawear calls touch fd 2.
reroute_c_stderr_to_log()
log = logging.getLogger("fs.run")

DEFAULT_STREAM_DURATION_S = 5.0
WATCHDOG_TICK_S = 1.0

parser = argparse.ArgumentParser(
    description="Fugue States runtime",
    epilog="""\
Examples:

  Quick smoke check (defaults: pi-driven, 5s duration, no recording):
    python3 -u run_fs.py

  Production wearable flow (what systemd runs at boot):
    python3 -u run_fs.py --mode button-driven

  Record a session for offline analysis:
    python3 -u run_fs.py --record
    python3 -u run_fs.py --record-to /tmp/diag.jsonl

  Capture training data for a new gesture (long-press a button to enter
  capture mode; single-press marks a window. --capture-label implies --record):
    python3 -u run_fs.py --capture-label wave

  Live gesture recognition from captured templates:
    python3 -u run_fs.py --mode button-driven \\
        --gesture-library recordings/gesture-wave-*.jsonl,recordings/gesture-roll-*.jsonl

  Position tracking (requires Sensor Fusion config with linear_acc + quaternion
  + corrected_gyro outputs; first 5s is cold-start bias calibration, LED yellow):
    python3 -u run_fs.py --mode button-driven --position-track

For runtime remote-control commands (start/stop/configure/calibrate/shutdown
over OSC) and combined recipes, see docs/usage.md.
For the C2 OSC vocabulary, see docs/c2.md.
""",
    formatter_class=argparse.RawDescriptionHelpFormatter,
)
parser.add_argument(
    "--mode",
    choices=("pi-driven", "button-driven"),
    default="pi-driven",
    help=(
        "pi-driven (default): connect, auto-start sensors, run for "
        f"{DEFAULT_STREAM_DURATION_S}s, stop, disconnect. "
        "button-driven: connect and wait until SIGINT; the device "
        "button toggles streaming (double-press)."
    ),
)
parser.add_argument(
    "--stream-duration",
    type=float,
    default=DEFAULT_STREAM_DURATION_S,
    help="The number of seconds to run the pi-driven stream.",
)
parser.add_argument(
    "--record",
    action="store_true",
    help="Record every emitted frame to recordings/session-<timestamp>.jsonl.",
)
parser.add_argument(
    "--record-to",
    type=str,
    default=None,
    metavar="PATH",
    help="Record to a specific JSONL path (implies --record).",
)
parser.add_argument(
    "--capture-label",
    type=str,
    default=None,
    metavar="LABEL",
    help=(
        "Enable gesture-capture mode with LABEL as the active gesture name. "
        "Implies --record (auto-targets recordings/gesture-<LABEL>-<ts>.jsonl "
        "if --record-to isn't set). Long-press a device button to enter/exit "
        "capture mode; single-press toggles a labeled gesture window."
    ),
)
parser.add_argument(
    "--gesture-library",
    type=str,
    default=None,
    metavar="PATHS",
    help=(
        "Comma-separated JSONL recording paths to load gesture templates "
        "from (typically the files produced by --capture-label runs). "
        "Each `_gesture` window becomes a multivariate (acc_mag + gyro_mag) "
        "template; per-label thresholds auto-derived from intra-label DTW "
        "distances. A single GestureRecognizer is inserted into every "
        "pipeline producing a feature sensor; emits /<MAC>/gesture/<label> "
        "on match."
    ),
)
parser.add_argument(
    "--gesture-features",
    type=str,
    default=None,
    metavar="LIST",
    help=(
        "Comma-separated scalar sensor names to use as gesture-recognition "
        "features (e.g. 'acc_mag,gyro_mag,tilt'). When omitted, the library "
        "auto-detects from the JSONL — taking the intersection of scalar "
        "streams present in every captured gesture window. Per-tick "
        "recognizer cost scales with len(features) × n_templates; check "
        "Pipeline.stats[GestureRecognizer] after a run to compare."
    ),
)
parser.add_argument(
    "--gesture-min-std",
    type=float,
    default=0.3,
    metavar="STD",
    help=(
        "Minimum buffer std required to attempt a match (default 0.3 "
        "for acc_mag). Skips matching during near-stillness to suppress "
        "noise-amplification false positives from z-score normalization."
    ),
)
parser.add_argument(
    "--gesture-threshold-margin",
    type=float,
    default=1.5,
    metavar="MULT",
    help=(
        "Auto-threshold = max(intra-label pairwise DTW) * MARGIN "
        "(default 1.5). Lower tightens, raising the false-positive bar "
        "at the cost of borderline real gestures."
    ),
)
parser.add_argument(
    "--gesture-band",
    type=int,
    default=10,
    metavar="N",
    help=(
        "Sakoe-Chiba band radius for DTW (default 10). Constrains the "
        "warping path to within N cells of the diagonal — prevents short "
        "templates from warping across long buffers and false-matching."
    ),
)
parser.add_argument(
    "--gesture-psi",
    type=int,
    default=10,
    metavar="N",
    help=(
        "Subsequence relaxation for DTW (default 10). Allows the "
        "template's first/last N samples to match anywhere in the buffer "
        "— for short gestures embedded in a longer streaming window."
    ),
)
parser.add_argument(
    "--gesture-cooldown",
    type=float,
    default=0.2,
    metavar="SECONDS",
    help=(
        "Minimum time between successive triggers (default 0.2s). "
        "Sanity backstop only — the armed-state hysteresis does the "
        "real one-gesture-equals-one-trigger work."
    ),
)
parser.add_argument(
    "--gesture-exit-threshold",
    type=float,
    default=1.2,
    metavar="RATIO",
    help=(
        "Hysteresis exit threshold (default 1.2). After firing, the "
        "recognizer disarms and won't re-fire until best_ratio rises "
        "back above this value — i.e. the buffer has clearly left the "
        "matching valley. Higher = more conservative re-arming."
    ),
)
parser.add_argument(
    "--gesture-zscore",
    action="store_true",
    help=(
        "Z-score normalize templates and runtime buffer before DTW. "
        "Default OFF — analysis on real captures showed raw multivariate "
        "distances discriminate better. Flip on if your gestures rely "
        "on shape independent of amplitude."
    ),
)
parser.add_argument(
    "--gesture-filter-outliers",
    action="store_true",
    help=(
        "Drop outlier templates per label using Median Absolute "
        "Deviation on each template's mean DTW distance to its peers. "
        "Guardrails: skips labels with <5 templates; never drops more "
        "than 20%% of a label's templates (warns instead). All decisions "
        "logged. Default OFF — opt in when training data is suspected "
        "to contain a few bad captures."
    ),
)
parser.add_argument(
    "--gesture-debug",
    action="store_true",
    help=(
        "Verbose per-tick logging from the gesture recognizer — one log "
        "line per tick with std + best label + distance + ratio. Useful "
        "for tuning min_std, band, psi, and threshold_margin."
    ),
)
parser.add_argument(
    "--position-track",
    action="store_true",
    help=(
        "Enable position tracking from sensor-fusion outputs (linear_acc + "
        "quaternion + corrected_gyro). Requires Sensor Fusion mode in the "
        "config with those three outputs enabled. Inserts a single "
        "PositionTracker into the linear_acc/quat/corrected_gyro pipelines; "
        "publishes /<MAC>/position (and /<MAC>/velocity, /<MAC>/zupt) on "
        "every linear_acc tick after a 5s cold-start bias calibration."
    ),
)
parser.add_argument(
    "--position-acc-std-threshold",
    type=float,
    default=0.15,
    metavar="MS2",
    help=(
        "ZUPT threshold: rolling std of acc magnitude (m/s²) below this "
        "AND gyro-magnitude below --position-gyro-mag-threshold means "
        "stationary → velocity reset (default 0.15)."
    ),
)
parser.add_argument(
    "--position-gyro-mag-threshold",
    type=float,
    default=8.0,
    metavar="DEGS",
    help=(
        "ZUPT threshold: gyro magnitude (deg/s) below this AND acc-std "
        "below --position-acc-std-threshold means stationary (default 8)."
    ),
)
parser.add_argument(
    "--position-calibration-samples",
    type=int,
    default=125,
    metavar="N",
    help=(
        "Cold-start bias calibration window (samples; default 125 = ~5s "
        "at 25Hz). Wearer holds still while LED is YELLOW; bias is the "
        "mean of world-frame linear_acc over this window."
    ),
)
parser.add_argument(
    "--position-emit-velocity",
    action="store_true",
    help=(
        "Also publish /<MAC>/velocity (3-axis world-frame m/s). Default "
        "OFF — emitting creates an extra OSC + JSONL write per linear_acc "
        "frame, which can back up the BLE callback thread and drop sample "
        "rate. Turn on for debug/analysis runs."
    ),
)
parser.add_argument(
    "--position-emit-zupt",
    action="store_true",
    help=(
        "Also publish /<MAC>/zupt (1.0 stationary / 0.0 moving). Default "
        "OFF for the same throughput reason as --position-emit-velocity."
    ),
)
parser.add_argument(
    "--position-debug",
    action="store_true",
    help="Verbose per-tick logging from the position tracker.",
)
args = parser.parse_args()

config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "fs_config.json")
config = read_fugue_states_config(config_path)
config = validate_config(config)
assert config["valid"], "Invalid configuration"

network = config["network"]
devices = config["metawear"]["devices"]

log.info("setting up OSC")
osc = ControlledOSCConnection(ip=network["ip"], port=network["port"])

log.info("building device states")
states = [MetaWearState(device_config=d, network_config=network, OSC=osc) for d in devices]

# Testing IMUFrame pipeline elements.
# LowPass with output_sensor="acc_lp" emits both raw and filtered acc,
# so you can compare side-by-side. Downstream Magnitude sees both
# inputs and emits acc_mag + acc_lp_mag automatically.
# Gyro pipeline gets Magnitude so gyro_mag is published and recorded —
# needed as the second feature for multivariate gesture recognition
# (acc_mag is translation, gyro_mag is rotation; together they
# discriminate gestures with similar amplitude profiles).
for s in states:
    s.pipelines["acc"].stages = [
        LowPass(cutoff_hz=5.0, fs=25.0, output_sensor="acc_lp"),
        Magnitude(),
        Tilt(),
        OscEmit(s._osc_client)
    ]
    s.pipelines["gyro"].stages = [
        Magnitude(),
        OscEmit(s._osc_client)
    ]

# --- Gesture recognition (opt-in) ---------------------------------------------
# --gesture-library loads templates from one or more capture-mode JSONL
# recordings, auto-derives per-label thresholds, and inserts a single
# GestureRecognizer instance into every pipeline that produces any of
# the configured feature sensors (default acc_mag + gyro_mag, so the
# acc and gyro pipelines). The recognizer maintains per-(device, sensor)
# buffers, ticks on the primary feature, runs multivariate DTW (with
# Sakoe-Chiba band + subsequence relaxation) every tick, and emits
# /<MAC>/gesture/<label> on match. Inserted BEFORE the recording block
# so any --record run also captures the trigger frames inline (useful
# for offline validation).
if args.gesture_library:
    gesture_paths = [p.strip() for p in args.gesture_library.split(",") if p.strip()]
    explicit_features = None
    if args.gesture_features:
        explicit_features = tuple(
            f.strip() for f in args.gesture_features.split(",") if f.strip()
        )
    library = GestureLibrary.from_files(
        gesture_paths,
        feature_sensors=explicit_features,  # None → auto-detect from JSONL
        threshold_margin=args.gesture_threshold_margin,
        band=args.gesture_band,
        psi=args.gesture_psi,
        zscore=args.gesture_zscore,
        filter_outliers=args.gesture_filter_outliers,
    )
    log.info("loaded gesture library: %d templates across %d label(s): %s "
             "(features=%s, band=%d, psi=%d, zscore=%s, filter_outliers=%s)",
             len(library.templates), len(library.labels), library.labels,
             library.feature_sensors, library.band, library.psi,
             library.zscore, args.gesture_filter_outliers)
    for s in states:
        # Single recognizer instance, inserted into every pipeline
        # whose advertised outputs include any feature sensor. Each
        # pipeline's process() feeds the corresponding per-(device,
        # sensor) buffer; the recognizer only ticks on the primary
        # feature's frames.
        rec = GestureRecognizer(
            library,
            min_std=args.gesture_min_std,
            band=args.gesture_band,
            psi=args.gesture_psi,
            cooldown_s=args.gesture_cooldown,
            exit_threshold=args.gesture_exit_threshold,
            debug=args.gesture_debug,
        )
        feature_set = set(library.feature_sensors)
        inserted_into: list = []
        for pipe_name, pipe in s.pipelines.items():
            advertised = pipe.advertised_outputs(pipe_name)
            if not (advertised & feature_set):
                continue
            insert_at = len(pipe.stages)
            for i, stage in enumerate(pipe.stages):
                if stage.is_terminal:
                    insert_at = i
                    break
            pipe.stages.insert(insert_at, rec)
            inserted_into.append(pipe_name)
        if not inserted_into:
            log.warning("[gesture] no pipeline produces any of %s — "
                        "recognizer not wired (check pipeline composition)",
                        library.feature_sensors)
        else:
            log.info("[gesture] recognizer wired into pipelines: %s",
                     inserted_into)

# --- Position tracking (opt-in, requires sensor-fusion config) ----------------
# PositionTracker is a FusionStage — it reads quat and corrected_gyro
# from a shared Latch (populated by LatchUpdate stages in those pipelines)
# while ticking only on linear_acc frames. One Latch instance is shared
# across all devices; PositionTracker keys reads by the driving frame's
# device so per-device wiring is automatic. Inserted BEFORE the recording
# block so --record runs capture position frames inline.
position_trackers: dict = {}
if args.position_track:
    state_by_addr = {s.address: s for s in states}
    required_inputs = (
        PositionTracker.INPUT_LINEAR_ACC,
        PositionTracker.INPUT_QUAT,
        PositionTracker.INPUT_GYRO,
    )
    pos_latch = Latch()
    for s in states:
        # Validate fusion config: all three required pipelines must
        # actually carry their source sensor (which the validator only
        # records when Sensor Fusion is configured with the right outputs).
        configured = s._configured_pipeline_sources()
        missing = [r for r in required_inputs if r not in configured]
        if missing:
            log.warning(
                "[position] [%s] skipping — missing fusion outputs %s. "
                "Add to fs_config.json under \"Sensor Fusion\" → \"outputs\": "
                "[\"linear_acc\", \"quaternion\", \"corrected_gyro\"]",
                s.address, missing,
            )
            continue
        # LatchUpdate at the head of the quat and corrected_gyro pipelines
        # so the tracker sees raw fusion-output values. Position 0 = before
        # any other stages; explicit per the design contract (placing it
        # after a transform would make the latch reflect transformed values).
        for src in (PositionTracker.INPUT_QUAT, PositionTracker.INPUT_GYRO):
            s.pipelines[src].stages.insert(0, LatchUpdate(pos_latch))
        # Tracker inserted only in linear_acc pipeline, before any
        # terminal stage (OscEmit).
        tracker = PositionTracker(
            latch=pos_latch,
            zupt_acc_std_threshold=args.position_acc_std_threshold,
            zupt_gyro_mag_threshold=args.position_gyro_mag_threshold,
            calibration_samples=args.position_calibration_samples,
            emit_velocity=args.position_emit_velocity,
            emit_zupt=args.position_emit_zupt,
            state_lookup=lambda mac: state_by_addr.get(mac),
            debug=args.position_debug,
        )
        pipe = s.pipelines[PositionTracker.INPUT_LINEAR_ACC]
        insert_at = len(pipe.stages)
        for i, stage in enumerate(pipe.stages):
            if stage.is_terminal:
                insert_at = i
                break
        pipe.stages.insert(insert_at, tracker)
        # Stash by MAC so the C2 controller can dispatch /cmd/calibrate
        # to the right tracker.
        position_trackers[s.address] = tracker
        log.info("[position] [%s] tracker wired (acc_std<%.2f, gyro<%.1f, "
                 "calib=%d samples)",
                 s.address,
                 args.position_acc_std_threshold,
                 args.position_gyro_mag_threshold,
                 args.position_calibration_samples)

# --- Recording (opt-in) -------------------------------------------------------
# --record auto-generates recordings/session-<ts>.jsonl in the project
# dir; --record-to PATH overrides. Recorder is injected into every
# pipeline just before the first terminal stage so the JSONL captures
# exactly what the receiver sees (post-transform). All Recorders share
# one RecorderSink so a session is one file demuxable by device+sensor.
# A `_meta` first line anchors the session in wall-clock time and
# records the configured sensor settings; `_session` start/end markers
# bracket the file for boundary recovery on read.
recorder_sink = None
recorder_path = None
ts = time.strftime("%Y%m%dT%H%M%S")
# --capture-label implies --record. Default file naming distinguishes
# gesture-capture sessions from plain recordings.
record_implied_by_capture = args.capture_label is not None
if args.record_to:
    recorder_path = args.record_to
elif args.record:
    recorder_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "recordings",
        f"session-{ts}.jsonl",
    )
elif record_implied_by_capture:
    recorder_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "recordings",
        f"gesture-{args.capture_label}-{ts}.jsonl",
    )

if recorder_path:
    metadata = {
        "version": 1,
        "session_id": ts,
        "wall_start_iso": datetime.now(timezone.utc).isoformat(),
        "wall_start_epoch": time.time(),
        "mono_start": time.monotonic(),
        "capture_label": args.capture_label,
        "devices": [
            {"address": d["mac"], "name": d["name"], "sensors": d["sensors"]}
            for d in devices
        ],
    }
    recorder_sink = RecorderSink(recorder_path)
    recorder_sink.open(metadata=metadata)
    if args.capture_label:
        recorder_sink.current_label = args.capture_label
        # Wire the sink to each state so long-press / single-press in
        # capture mode can write _gesture markers.
        for s in states:
            s.set_capture_sink(recorder_sink)
    for s in states:
        for pipe in s.pipelines.values():
            insert_at = len(pipe.stages)
            for i, stage in enumerate(pipe.stages):
                if stage.is_terminal:
                    insert_at = i
                    break
            pipe.stages.insert(insert_at, Recorder(recorder_sink))

# Apply persisted operator edits from fs_config.local.json's pipelines section
# (C2 /cmd/pipeline/* writes there). Composition overrides replace the
# constructible chain; tunings setattr params on runtime-dep stages. See
# apply_pipeline_overrides docstring for details.
apply_pipeline_overrides(states, config)

# Announce the OSC addresses each device will publish so receivers
# (PD, recorders) can subscribe without hardcoding. Fires once at
# startup; re-call s.advertise() interactively if pipelines change.
for s in states:
    s.advertise()

# C2 — process-level remote-control surface (docs/c2.md). Owns the
# shutdown token, heartbeat loop, /cmd/* handlers, and snapshot replies.
# Per-device state-change events (/state/<mac>/connected etc.) are
# emitted from MetaWearState directly. The controller's tick() is
# called from both watchdog loops below; should_stop signals a clean
# exit when /cmd/shutdown was accepted.
controller = Controller(
    osc=osc,
    states=states,
    config_path=config_path,
    recorder_path_provider=lambda: recorder_path,
    position_track_enabled=args.position_track,
    position_trackers=position_trackers,
)
controller.install()
controller.announce_initial_state()


def _shutdown_all() -> None:
    # Flush any pending C2 persistence so a /cmd/pipeline/set right
    # before SIGTERM doesn't get lost in the debounce window.
    try:
        controller.flush_persist_now()
    except BaseException:
        log.exception("error flushing c2 persistence on shutdown")
    for s in states:
        try:
            s.shutdown()
        except BaseException:
            log.exception("error during shutdown of %s", s.address)
    try:
        osc.stop_server()
    except BaseException:
        log.exception("error stopping OSC server")
    if recorder_sink is not None:
        try:
            recorder_sink.close()
        except BaseException:
            log.exception("error closing recorder sink")


atexit.register(_shutdown_all)


# mbientlab's MetaWear constructor os.fork()s a BLE worker process per
# device. The fork inherits our signal handlers and run_fs.py state
# (copy-on-write), so each child independently runs cleanup if it
# receives a signal. But a signal sent to *us* (the bash-tracked PID)
# doesn't reach those siblings unless they share a process group.
# Make ourselves a new process group leader so a single signal can fan
# out via os.killpg.
try:
    os.setpgrp()
except OSError:
    pass


def _sigint_handler(signum, frame):
    log.warning("signal %s received, shutting down", signum)
    _shutdown_all()
    # Re-broadcast to the group so any forked BLE workers also exit.
    # Re-installing the default handler on the same signal first means
    # we exit immediately on the broadcast rather than recurse.
    try:
        signal.signal(signum, signal.SIG_DFL)
        os.killpg(os.getpgrp(), signum)
    except BaseException:
        pass
    sys.exit(128 + signum)


signal.signal(signal.SIGINT, _sigint_handler)
# SIGTERM is what systemd sends on `systemctl stop`. Without a handler
# we'd get SIGKILLed after TimeoutStopSec elapses and skip cleanup.
signal.signal(signal.SIGTERM, _sigint_handler)
# SIGHUP fires when our controlling terminal goes away — e.g., the user
# `ssh fugue-pi "..."` (no -t) and Ctrl-Cs the local ssh client. Without
# this, the remote python kept running as a zombie holding port 8001.
if hasattr(signal, "SIGHUP"):
    signal.signal(signal.SIGHUP, _sigint_handler)


if args.mode == "button-driven":
    # Connect each device but DON'T auto-start sensors — the user's
    # button (double-press) will toggle streaming per-device. Run until
    # SIGINT; the watchdog tick still drives stale-stream recovery for
    # whichever devices happen to be streaming.
    log.info("button-driven mode — connecting devices and waiting for button events")
    for s in states:
        try:
            s.connect()
        except BaseException:
            log.exception("failed to connect %s", s.address)
    log.info("ready — double-press a device button to toggle its stream; SIGINT to exit")
    while not controller.should_stop:
        sleep(WATCHDOG_TICK_S)
        for s in states:
            try:
                s.check_and_recover()
            except BaseException:
                log.exception("[%s] check_and_recover raised", s.address)
        controller.tick()
    log.info("controller requested shutdown — exiting button-driven loop")

else:
    # pi-driven (default): auto-start, run for args.stream_duration, stop.
    log.info("starting sensors (duration=%.1fs)", args.stream_duration)
    for s in states:
        try:
            s.start_sensors(s.sensor_config)
        except BaseException:
            log.exception("failed to start sensors on %s", s.address)

    deadline = time.monotonic() + args.stream_duration
    while time.monotonic() < deadline and not controller.should_stop:
        sleep(WATCHDOG_TICK_S)
        for s in states:
            try:
                s.check_and_recover()
            except BaseException:
                log.exception("[%s] check_and_recover raised", s.address)
        controller.tick()

    log.info("stopping sensors")
    for s in states:
        try:
            s.stop_sensors(s.sensor_config)
        except BaseException:
            log.exception("failed to stop sensors on %s", s.address)

    sleep(1.0)

    log.info("disconnecting devices")
    for s in states:
        try:
            s.disconnect()
        except BaseException:
            log.exception("failed to disconnect %s", s.address)
        log.info("[%s] sample report: %s", s.address, s.logger)
        if s.failed:
            log.warning(
                "[%s] %d failure(s) recorded; sources=%s last_error=%r",
                s.address, len(s.failed_sources), s.failed_sources, s.last_error,
            )
