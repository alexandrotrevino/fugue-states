#!/usr/bin/env python3
"""
Instrument presets: one JSON file describes a sphere instrument; this tool
checks it and pushes it to the running system.

    python tools/preset.py show  presets/dome9-a-major-pent.json
    python tools/preset.py apply presets/dome9-a-major-pent.json
    python tools/preset.py apply presets/arc7-d-dorian.json --dry-run

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
        "targets": [             # 1..9 targets, in fs_sphere voice order
          {"az": 0, "el": 10, "note": 57},
          {"az": 45, "el": 10, "note": 61, "cone_deg": 25, "release_ms": 1500},
          ...
        ]
      }
    }

az is degrees counterclockwise from front (seen from above), el degrees
above the horizon. Per-target cone_deg / attack_ms / release_ms override
the defaults.
"""

import argparse
import json
import os
import sys
import time
from pathlib import Path

MAX_TARGETS = 9          # fs_sphere has nine voices
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
    args = ap.parse_args(argv)
    try:
        if args.cmd == "show":
            return cmd_show(args)
        if args.cmd == "apply":
            return cmd_apply(args)
    except PresetError as e:
        print("preset error: %s" % e, file=sys.stderr)
        return 2
    ap.print_help()
    return 1


if __name__ == "__main__":
    sys.exit(main())
