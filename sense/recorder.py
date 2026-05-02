"""
Trace recording and replay for IMU pipelines.

A `Recorder` Stage taps a Pipeline and writes every frame it sees to a
shared `RecorderSink` as JSON Lines. Drop it anywhere in the pipeline:
- just before `OscEmit` captures everything the receiver sees (default
  composition in run_fs.py)
- first position captures raw sensor frames before any transforms
- between two stages captures the boundary

`replay(path, on_frame, speed)` reads a JSONL file back and calls
`on_frame(frame)` per frame. Combine with a Pipeline to tune or debug
stages offline against real recordings — no sensors, no PD,
deterministic.

JSONL schema:
    {"_meta": {...}}                                        # optional, line 1
    {"_session": "start", "t": <mono>}                      # process-start marker
    {"device":"E0:..","sensor":"acc","t_recv":1714..,"values":[...]}
    ...
    {"_session": "end", "t": <mono>}                        # process-end marker

Frame records always have a `device` key; non-frame records (`_meta`,
`_session`, future `_stream`/`_gesture` markers) do not. `replay()`
skips anything without `device`. `read_metadata(path)` returns the
`_meta` dict for callers who want session context without a full read.
"""
import json
import logging
import threading
import time
from pathlib import Path
from typing import Callable, Iterable, Optional, TextIO

from .pipeline import IMUFrame, Stage

log = logging.getLogger("fs.recorder")


class RecorderSink:
    """
    Owner of one JSONL file. Thread-safe — multiple `Recorder` Stages
    (one per pipeline, across all devices) may share a single sink so
    a session is one file with frames demuxable by device + sensor.

    Lifecycle: instantiate, `open()` once, any number of `write(frame)`
    calls from any thread, then `close()` to flush and release.
    """
    def __init__(self, path):
        self.path = Path(path)
        self._fh: Optional[TextIO] = None
        self._lock = threading.Lock()
        self._count = 0
        # Active label for `_gesture` markers. Set by run_fs.py at startup
        # (--capture-label) and/or at runtime via OSC (future). Per-(label,
        # device) instance counter is auto-assigned by mark_gesture_start.
        self.current_label: Optional[str] = None
        self._gesture_instances: dict = {}

    def open(self, metadata: Optional[dict] = None) -> None:
        """
        Open the file for writing. If `metadata` is provided, write it
        as a `{"_meta": ...}` first line so callers can recover session
        context (wall-clock anchor, configured ODRs, FS version, etc.)
        without inspecting frames. A `{"_session": "start", "t": ...}`
        marker is always written after the optional meta line so the
        process-start boundary is recoverable on read.
        """
        if self._fh is not None:
            return
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._fh = self.path.open("w", encoding="utf-8")
        if metadata is not None:
            self._fh.write(
                json.dumps({"_meta": metadata}, separators=(",", ":")) + "\n"
            )
        self._fh.write(
            json.dumps({"_session": "start", "t": time.monotonic()},
                       separators=(",", ":")) + "\n"
        )
        log.info("recording to %s", self.path)

    def write(self, frame: IMUFrame) -> None:
        if self._fh is None:
            return
        record = {
            "device": frame.device,
            "sensor": frame.sensor,
            "t_recv": frame.t_recv,
            "values": list(frame.values),
        }
        line = json.dumps(record, separators=(",", ":"))
        with self._lock:
            # Re-check under lock — close() may have raced ahead.
            if self._fh is None:
                return
            self._fh.write(line + "\n")
            self._count += 1

    def close(self) -> None:
        with self._lock:
            if self._fh is None:
                return
            try:
                self._fh.write(
                    json.dumps({"_session": "end", "t": time.monotonic()},
                               separators=(",", ":")) + "\n"
                )
            except BaseException:
                # Best-effort end marker — don't block close on a write failure.
                pass
            self._fh.flush()
            self._fh.close()
            self._fh = None
        log.info("recording closed: %d frames in %s", self._count, self.path)

    @property
    def frame_count(self) -> int:
        return self._count

    def mark_gesture_start(self, device: str) -> Optional[int]:
        """
        Write a `_gesture: start` marker for `device` under the current
        label. Returns the instance integer to be passed to
        `mark_gesture_end` on close, or None if no label is set or the
        sink isn't open. Auto-increments the per-(label, device) counter.
        """
        label = self.current_label
        if label is None or self._fh is None:
            return None
        key = (label, device)
        instance = self._gesture_instances.get(key, 0)
        self._gesture_instances[key] = instance + 1
        record = {
            "_gesture": "start",
            "label": label,
            "device": device,
            "instance": instance,
            "t": time.monotonic(),
        }
        line = json.dumps(record, separators=(",", ":")) + "\n"
        with self._lock:
            if self._fh is None:
                return None
            self._fh.write(line)
        return instance

    def mark_gesture_end(self, device: str, instance: int) -> None:
        """Write the closing `_gesture: end` marker paired with the
        instance returned by mark_gesture_start. No-op if the sink is
        closed or no label is set."""
        label = self.current_label
        if label is None or self._fh is None:
            return
        record = {
            "_gesture": "end",
            "label": label,
            "device": device,
            "instance": instance,
            "t": time.monotonic(),
        }
        line = json.dumps(record, separators=(",", ":")) + "\n"
        with self._lock:
            if self._fh is None:
                return
            self._fh.write(line)


class Recorder(Stage):
    """
    Pipeline Stage that writes every passing frame to a `RecorderSink`
    and forwards it unchanged. Multiple Recorders sharing one sink is
    expected — each per-sensor pipeline gets its own instance pointing
    at the same file.
    """
    is_terminal = False

    def __init__(self, sink: RecorderSink):
        self.sink = sink

    def process(self, frame: IMUFrame) -> Iterable[IMUFrame]:
        self.sink.write(frame)
        yield frame


def replay(path, on_frame: Callable[[IMUFrame], None], speed: float = 1.0) -> int:
    """
    Read a JSONL recording and call `on_frame(frame)` per frame.

    speed:
      1.0  — real-time (respect inter-frame timestamps)
      0.5  — half speed
      2.0  — double speed
      0.0  — no pacing (push as fast as the loop runs; right for
             offline pipeline tuning)

    Returns the number of frames replayed.
    """
    path = Path(path)
    wall_start = time.monotonic()
    record_start: Optional[float] = None
    n = 0

    with path.open("r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            record = json.loads(line)
            # Skip metadata / session-bracket / future marker records.
            # Frame records always have a "device" key.
            if "device" not in record:
                continue
            frame = IMUFrame(
                device=record["device"],
                sensor=record["sensor"],
                t_recv=record["t_recv"],
                values=tuple(record["values"]),
            )

            if speed > 0.0:
                if record_start is None:
                    record_start = frame.t_recv
                # Wall-clock time at which this frame is "due".
                target = wall_start + (frame.t_recv - record_start) / speed
                delay = target - time.monotonic()
                if delay > 0:
                    time.sleep(delay)

            on_frame(frame)
            n += 1

    log.info("replay: %d frames from %s", n, path)
    return n


def read_metadata(path) -> Optional[dict]:
    """
    Return the `_meta` dict from a recording's first line, or None if
    the file has no metadata header. Reads only the first line.
    """
    path = Path(path)
    with path.open("r", encoding="utf-8") as fh:
        line = fh.readline().strip()
    if not line:
        return None
    record = json.loads(line)
    return record.get("_meta")
