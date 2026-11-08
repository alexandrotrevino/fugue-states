#!/usr/bin/env python3
"""
Instrument presets: one JSON file describes a sphere instrument; this tool
checks it and pushes it to the running system.

    python tools/preset.py show  presets/dome9-a-major-pent.json
    python tools/preset.py apply presets/dome9-a-major-pent.json
    python tools/preset.py apply presets/arc7-d-dorian.json --dry-run
    python tools/preset.py new presets/dome20-c-major.json --points 20 --scale major --root C4

`apply` sends two kinds of messages:

  * to the Pi (C2, udp/8001): the target layout and cones, as live
    `/cmd/pipeline/set <mac> quat Alignment points|cone_deg ...`. The Pi
    applies them immediately and persists them to fs_config.local.json.
  * to PD (fs_net, udp/12345): notes, envelopes and gate, as
    `/<mac>/sphere_cfg ...` messages that [fs_sphere] picks up. PD doesn't
    persist these - re-apply after reopening the patch.

Format (version 1):

    {
      "fugue_preset": 1,
      "name": "dome9-a-major-pent",
      "description": "...",
      "device": {"name": "mms", "mac": "EC:47:49:CF:53:C4"},
      "sphere": {
        "cone_deg": 35,          # default zone (half-angle, degrees)
        "gate": 1.0,             # 1 = sound only while moving, 0 = pointing alone
        "attack_ms": 40,         # default envelope for every target
        "release_ms": 600,
        "targets": [             # 1..64 targets, in fs_sphere voice order
          {"az": 0, "el": 10, "note": 57},
          {"az": 45, "el": 10, "note": 61, "cone_deg": 25, "release_ms": 1500},
          ...
        ]
      }
    }

az is degrees counterclockwise from front (seen from above), el degrees
above the horizon. Per-target cone_deg / attack_ms / release_ms override
the defaults.

`new` writes a starting layout with any number of points, spread evenly
(a golden-angle spiral) over a dome, the whole sphere, or a ring, with
notes climbing a scale from low to high elevation and a cone sized to
the point spacing - a base to edit by hand or in the editor.
"""

import argparse
import json
import math
import os
import sys
import time
from pathlib import Path

MAX_TARGETS = 64         # fs_sphere clones 64 voices
NOTE_NAMES = ["C", "C#", "D", "D#", "E", "F", "F#", "G", "G#", "A", "A#", "B"]


class PresetError(ValueError):
    pass


def note_name(m):
    return "%s%d" % (NOTE_NAMES[m % 12], m // 12 - 1)


def _num(d, key, lo, hi, where, required=True, default=None):
    if key not in d:
        if required:
            raise PresetError("%s: missing %r" % (where, key))
        return default
    v = d[key]
    if isinstance(v, bool) or not isinstance(v, (int, float)):
        raise PresetError("%s: %r must be a number, got %r" % (where, key, v))
    if not lo <= v <= hi:
        raise PresetError("%s: %r must be within %g..%g, got %g" % (where, key, lo, hi, v))
    return float(v)


def load(path):
    """Read and validate a preset; returns a normalized dict."""
    try:
        raw = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, ValueError) as e:
        raise PresetError("%s: %s" % (path, e))
    if raw.get("fugue_preset") != 1:
        raise PresetError("%s: expected \"fugue_preset\": 1" % path)
    dev = raw.get("device") or {}
    if not dev.get("mac") or not dev.get("name"):
        raise PresetError("device needs both \"name\" (PD name) and \"mac\"")
    sp = raw.get("sphere")
    if not isinstance(sp, dict):
        raise PresetError("missing \"sphere\" section")
    out = {
        "name": raw.get("name") or Path(path).stem,
        "description": raw.get("description", ""),
        "device": {"name": str(dev["name"]), "mac": str(dev["mac"])},
        "cone_deg": _num(sp, "cone_deg", 1, 180, "sphere", False, 35.0),
        "gate": _num(sp, "gate", 0, 1, "sphere", False, 1.0),
        "attack_ms": _num(sp, "attack_ms", 0.001, 60000, "sphere", False, 40.0),
        "release_ms": _num(sp, "release_ms", 0.001, 60000, "sphere", False, 600.0),
        "targets": [],
    }
    targets = sp.get("targets")
    if not isinstance(targets, list) or not 1 <= len(targets) <= MAX_TARGETS:
        raise PresetError("sphere.targets must list 1..%d targets" % MAX_TARGETS)
    for i, tg in enumerate(targets, 1):
        where = "target %d" % i
        if not isinstance(tg, dict):
            raise PresetError("%s must be an object" % where)
        note = tg.get("note")
        if isinstance(note, bool) or not isinstance(note, int) or not 0 <= note <= 127:
            raise PresetError("%s: \"note\" must be a MIDI note 0..127, got %r" % (where, note))
        out["targets"].append({
            "az": _num(tg, "az", -360, 360, where),
            "el": _num(tg, "el", -90, 90, where),
            "note": note,
            "cone_deg": _num(tg, "cone_deg", 1, 180, where, False),
            "attack_ms": _num(tg, "attack_ms", 0.001, 60000, where, False),
            "release_ms": _num(tg, "release_ms", 0.001, 60000, where, False),
            "label": str(tg.get("label", "")),
        })
    return out


def _g(v):
    return ("%.3f" % v).rstrip("0").rstrip(".")


def points_spec(p):
    """Alignment `points` string: 'az,el[,cone]; ...'."""
    parts = []
    for t in p["targets"]:
        s = "%s,%s" % (_g(t["az"]), _g(t["el"]))
        if t["cone_deg"] is not None:
            s += ",%s" % _g(t["cone_deg"])
        parts.append(s)
    return "; ".join(parts)


def pi_messages(p):
    mac = p["device"]["mac"]
    base = ["/cmd/pipeline/set", mac, "quat", "Alignment"]
    return [base + ["cone_deg", p["cone_deg"]],
            base + ["points", points_spec(p)]]


def pd_messages(p):
    """Global envelope first (clears any old per-target overrides), then
    per-target notes and overrides, then the gate."""
    addr = "/%s/sphere_cfg" % p["device"]["mac"]
    msgs = [[addr, "attack", p["attack_ms"]], [addr, "release", p["release_ms"]]]
    for i, t in enumerate(p["targets"], 1):
        msgs.append([addr, "note", float(i), float(t["note"])])
        if t["attack_ms"] is not None:
            msgs.append([addr, "attack_n", float(i), t["attack_ms"]])
        if t["release_ms"] is not None:
            msgs.append([addr, "release_n", float(i), t["release_ms"]])
    msgs.append([addr, "gate", p["gate"]])
    return msgs


# --------------------------------------------------------------- generator

SCALES = {
    "major": [0, 2, 4, 5, 7, 9, 11],
    "minor": [0, 2, 3, 5, 7, 8, 10],
    "dorian": [0, 2, 3, 5, 7, 9, 10],
    "mixolydian": [0, 2, 4, 5, 7, 9, 10],
    "lydian": [0, 2, 4, 6, 7, 9, 11],
    "phrygian": [0, 1, 3, 5, 7, 8, 10],
    "pent": [0, 2, 4, 7, 9],
    "minpent": [0, 3, 5, 7, 10],
    "whole": [0, 2, 4, 6, 8, 10],
    "chromatic": list(range(12)),
}
SHAPES = {          # elevation band, degrees
    "dome": (0.0, 90.0),
    "sphere": (-90.0, 90.0),
    "ring": (10.0, 10.0),
}
GOLDEN_DEG = 180.0 * (3.0 - math.sqrt(5.0))   # 137.5...


def parse_note(s):
    """MIDI number or a name like C4, F#3, Bb2 (C4 = 60)."""
    s = str(s).strip()
    if s.lstrip("-").isdigit():
        m = int(s)
    else:
        name = s[:1].upper()
        rest = s[1:]
        acc = 0
        while rest[:1] in ("#", "b"):
            acc += 1 if rest[0] == "#" else -1
            rest = rest[1:]
        if not name or name not in "CDEFGAB" or not rest.lstrip("-").isdigit():
            raise PresetError("bad note %r (use a MIDI number or e.g. C4, F#3, Bb2)" % s)
        m = NOTE_NAMES.index(name) + acc + 12 * (int(rest) + 1)
    if not 0 <= m <= 127:
        raise PresetError("note %r is outside MIDI 0..127" % s)
    return m


def spread_points(n, shape="dome"):
    """n directions (az, el) spread evenly over the shape's band, ordered
    from lowest to highest elevation; the first sits straight ahead."""
    lo, hi = SHAPES[shape]
    if lo == hi:
        return [((360.0 * i / n) % 360.0, lo) for i in range(n)]
    zlo, zhi = math.sin(math.radians(lo)), math.sin(math.radians(hi))
    pts = []
    for i in range(n):
        z = zlo + (zhi - zlo) * (i + 0.5) / n          # equal-area bands
        pts.append(((i * GOLDEN_DEG) % 360.0, math.degrees(math.asin(z))))
    return pts


def auto_cone(n, shape="dome"):
    """Cone half-angle for n evenly spread points: the cap each point owns,
    widened 25% so neighbours crossfade (dome, 9 points -> ~34 deg)."""
    lo, hi = SHAPES[shape]
    if lo == hi:
        cap = 180.0 / n                                  # half the ring gap
    else:
        area = 2 * math.pi * (math.sin(math.radians(hi)) - math.sin(math.radians(lo)))
        cap = math.degrees(math.acos(max(-1.0, 1 - area / n / (2 * math.pi))))
    return max(5.0, min(90.0, round(cap * 1.25)))


def scale_notes(n, root, scale, octaves=3):
    """n notes climbing the scale from root, within `octaves` octaves. With
    more points than scale degrees in that range, neighbours (in elevation
    order) share a degree, so height still maps to pitch."""
    steps = SCALES[scale]
    degrees = min(n, len(steps) * octaves + 1)       # +1: the top octave's root
    out = []
    for i in range(n):
        d = i * degrees // n
        m = root + 12 * (d // len(steps)) + steps[d % len(steps)]
        if m > 127:
            raise PresetError("%s from %s over %d octaves runs past MIDI 127 - pick a lower root"
                              % (scale, note_name(root), octaves))
        out.append(m)
    return out


def generate(n, shape="dome", scale="major", root=60, name=None, cone=None,
             device=("mms", "EC:47:49:CF:53:C4"), octaves=3):
    if not 1 <= n <= MAX_TARGETS:
        raise PresetError("--points must be 1..%d" % MAX_TARGETS)
    if not 1 <= octaves <= 8:
        raise PresetError("--octaves must be 1..8")
    pts = spread_points(n, shape)
    notes = scale_notes(n, root, scale, octaves)
    cone = auto_cone(n, shape) if cone is None else cone
    name = name or "%s%d-%s-%s" % (shape, n, note_name(root).lower().replace("#", "s"), scale)
    return {
        "fugue_preset": 1,
        "name": name,
        "description": "%d points spread over a %s, %s from %s rising with elevation"
                       % (n, shape, scale, note_name(root)),
        "device": {"name": device[0], "mac": device[1]},
        "sphere": {
            "cone_deg": cone, "gate": 1.0, "attack_ms": 40, "release_ms": 600,
            "targets": [{"az": round(az, 1), "el": round(el, 1), "note": m}
                        for (az, el), m in zip(pts, notes)],
        },
    }


def dumps(preset):
    """JSON with one target per line - readable and diff-friendly."""
    sp = dict(preset["sphere"])
    targets = sp.pop("targets")
    head = json.dumps(dict(preset, sphere=dict(sp, targets=[])), indent=2)
    rows = ",\n".join("      " + json.dumps(t) for t in targets)
    return head.replace('"targets": []', '"targets": [\n%s\n    ]' % rows) + "\n"


def _send(host, port, msgs):
    from pythonosc.udp_client import SimpleUDPClient
    client = SimpleUDPClient(host, port)
    for m in msgs:
        client.send_message(m[0], m[1:])
        time.sleep(0.005)  # keep UDP bursts gentle


def cmd_show(args):
    p = load(args.preset)
    print("%s  (%s @ %s)" % (p["name"], p["device"]["name"], p["device"]["mac"]))
    if p["description"]:
        print("  " + p["description"])
    print("  default cone %s deg, gate %s, attack %s ms, release %s ms"
          % (_g(p["cone_deg"]), _g(p["gate"]), _g(p["attack_ms"]), _g(p["release_ms"])))
    print("   #     az     el  note   cone  attack release  label")
    for i, t in enumerate(p["targets"], 1):
        print("  %2d %6s %6s  %-4s %6s %7s %7s  %s" % (
            i, _g(t["az"]), _g(t["el"]), note_name(t["note"]),
            _g(t["cone_deg"]) if t["cone_deg"] is not None else "-",
            _g(t["attack_ms"]) if t["attack_ms"] is not None else "-",
            _g(t["release_ms"]) if t["release_ms"] is not None else "-", t["label"]))
    print("  Alignment points: %s" % points_spec(p))
    return 0


def cmd_apply(args):
    p = load(args.preset)
    targets = []
    if not args.no_pi:
        targets.append(("Pi", args.pi, args.pi_port, pi_messages(p)))
    if not args.no_pd:
        targets.append(("PD", args.pd, args.pd_port, pd_messages(p)))
    for who, host, port, msgs in targets:
        print("%s [%s]:%d - %d message(s)%s" % (who, host, port, len(msgs), " (dry run)" if args.dry_run else ""))
        for m in msgs:
            if args.dry_run or args.verbose:
                print("   ", m[0], *(_g(a) if isinstance(a, float) else a for a in m[1:]))
        if not args.dry_run:
            _send(host, port, msgs)
    if not args.dry_run:
        print("applied %s (%d targets)" % (p["name"], len(p["targets"])))
    return 0


def cmd_new(args):
    root = parse_note(args.root)
    p = generate(args.points, args.shape, args.scale, root, args.name, args.cone,
                 (args.device_name, args.mac), args.octaves)
    out = Path(args.out)
    if out.exists() and not args.force:
        raise PresetError("%s exists (use --force to overwrite)" % out)
    out.write_text(dumps(p), encoding="utf-8")
    load(out)   # round-trip through the validator
    print("wrote %s: %d points over a %s, %s from %s, cone %s deg"
          % (out, args.points, args.shape, args.scale, note_name(root), _g(p["sphere"]["cone_deg"])))
    return 0


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0],
                                 epilog=__doc__.split("\n\n", 1)[1],
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd")
    s = sub.add_parser("show", help="validate a preset and print it as a table")
    s.add_argument("preset")
    a = sub.add_parser("apply", help="push a preset to the Pi and PD")
    a.add_argument("preset")
    a.add_argument("--pi", default=os.environ.get("FS_PI_HOST", "192.168.4.21"),
                   help="Pi IPv4 address (default $FS_PI_HOST or 192.168.4.21)")
    a.add_argument("--pi-port", type=int, default=8001)
    a.add_argument("--pd", default="::1", help="PD host (default ::1 - PD on Windows listens IPv6-only)")
    a.add_argument("--pd-port", type=int, default=12345)
    a.add_argument("--no-pi", action="store_true", help="skip the Pi (layout/cones)")
    a.add_argument("--no-pd", action="store_true", help="skip PD (notes/envelopes/gate)")
    a.add_argument("--dry-run", action="store_true", help="print messages instead of sending")
    a.add_argument("-v", "--verbose", action="store_true", help="print messages as they are sent")
    n = sub.add_parser("new", help="write a starting layout with N evenly spread points")
    n.add_argument("out", help="preset file to write")
    n.add_argument("--points", type=int, default=9, help="number of targets, 1..%d (default 9)" % MAX_TARGETS)
    n.add_argument("--shape", choices=sorted(SHAPES), default="dome",
                   help="dome: horizon to overhead; sphere: everywhere; ring: one ring at 10 deg (default dome)")
    n.add_argument("--scale", choices=sorted(SCALES), default="major")
    n.add_argument("--root", default="C4", help="lowest note: MIDI number or name, e.g. A3 (default C4)")
    n.add_argument("--octaves", type=int, default=3,
                   help="pitch range; extra points share notes with their neighbours (default 3)")
    n.add_argument("--cone", type=float, default=None, help="default zone in degrees (default: sized to the spacing)")
    n.add_argument("--name", default=None)
    n.add_argument("--device-name", default="mms", help="PD device name (default mms)")
    n.add_argument("--mac", default="EC:47:49:CF:53:C4", help="sensor MAC (default: mms)")
    n.add_argument("--force", action="store_true", help="overwrite an existing file")
    args = ap.parse_args(argv)
    try:
        if args.cmd == "show":
            return cmd_show(args)
        if args.cmd == "apply":
            return cmd_apply(args)
        if args.cmd == "new":
            return cmd_new(args)
    except PresetError as e:
        print("preset error: %s" % e, file=sys.stderr)
        return 2
    ap.print_help()
    return 1


if __name__ == "__main__":
    sys.exit(main())
