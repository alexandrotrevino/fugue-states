"""
Configuration loading and validation for Fugue States.

Two entry points callers should know about:

- `read_fugue_states_config(path)`: loads `path` (typically
  `fs_config.json`) and, if a sibling `fs_config.local.json` exists,
  deep-merges it on top. Local file is gitignored — it carries
  per-host overrides (e.g. the OSC target IP for the host that runs
  PD on this LAN) so the committed config can stay generic.

- `validate_config(config)`: single-pass validation that augments
  the merged config with `valid` (top-level bool) and `fusion_outputs`
  (per-device, list of lowercased fusion-output names; empty when
  Sensor Fusion isn't configured). Also normalizes each device's
  `sensors` dict (drops incompatible sensor combinations, renames
  Gyroscope→Gyroscope160 on MMRL, lowercases + dedupes fusion outputs).
"""
import ipaddress
import json
import logging
import os
import re

log = logging.getLogger("fs.config")

ALLOWED_SENSORS = (
    "Accelerometer", "Gyroscope", "Gyroscope160",
    "Magnetometer", "Temperature", "Ambient Light", "Sensor Fusion",
)
NON_FUSION = ("Accelerometer", "Gyroscope", "Magnetometer")
SUPPORTED_DEVICE_NAMES = ("mms", "mmrl")
ALLOWED_FUSION_OUTPUTS = (
    "quaternion", "euler_angle", "linear_acc", "gravity",
    "corrected_acc", "corrected_gyro", "corrected_mag",
)


def _local_override_path(path: str) -> str:
    """`/foo/bar/fs_config.json` -> `/foo/bar/fs_config.local.json`."""
    base, ext = os.path.splitext(path)
    return f"{base}.local{ext}"


def _deep_merge(base: dict, override: dict) -> dict:
    """
    Recursively merge `override` into `base`. Dicts merge key-by-key;
    any other type (including lists) in `override` replaces the base
    value wholesale. Returns a new dict — does not mutate inputs.
    """
    result = dict(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(result.get(key), dict):
            result[key] = _deep_merge(result[key], value)
        else:
            result[key] = value
    return result


def write_local_overrides(base_path: str, override: dict) -> str:
    """
    Persist `override` into the sibling `fs_config.local.json` (deep-
    merged on top of any existing local file). Atomic via tmp+rename so
    a crash mid-write doesn't leave a half-formed JSON file.

    Used by C2 /cmd/configure/* handlers to save accepted reconfigure
    requests so they survive process restarts. Caller is responsible
    for deciding the override shape — typically `{"network": {...}}`
    for /cmd/configure/network or `{"metawear": {"devices": [...]}}`
    for /cmd/configure/sensor.

    Note on list semantics: `_deep_merge` replaces lists wholesale (it
    doesn't merge by index or by key). For per-device sensor changes,
    callers should write the entire devices list — local.json's
    `metawear.devices` then replaces base's, and the operator's local
    file diverges from base for the duration of the reconfigure. That's
    an explicit Pass-2 trade-off; per-device-by-MAC merging is a future
    refactor.

    Returns the local override path.
    """
    local_path = _local_override_path(base_path)
    if os.path.exists(local_path):
        with open(local_path, "r") as f:
            existing = json.load(f)
    else:
        existing = {}
    merged = _deep_merge(existing, override)
    tmp_path = local_path + ".tmp"
    with open(tmp_path, "w") as f:
        json.dump(merged, f, indent=2)
        f.write("\n")
    os.replace(tmp_path, local_path)
    log.info("wrote local overrides to %s", local_path)
    return local_path


def read_fugue_states_config(path) -> dict:
    """
    Load a Fugue States configuration JSON file. Schema:

        {
            "network": {"ip": "<addr>", "port": <int>},
            "metawear": {
                "devices": [
                    {
                        "mac": "<colon-MAC>",
                        "name": "mms" | "mmrl",
                        "ble": "<colon-MAC of host BLE adapter>",
                        "sensors": {
                            "Accelerometer": {"odr": 25, "range": 4.0},
                            "Gyroscope":     {"odr": 25, "range": 1000.0},
                            "Magnetometer":  {"odr": 25},
                            "Temperature":   {"period": 1},
                            "Sensor Fusion": {
                                "mode":      "ndof",
                                "accRange":  4,
                                "gyroRange": 1000,
                                "outputs": [
                                    "quaternion",
                                    "corrected_acc",
                                    "corrected_gyro",
                                    "corrected_mag"
                                ]
                            }
                        }
                    }
                ]
            }
        }

    Sensor Fusion (when present) is exclusive with Accelerometer /
    Gyroscope / Magnetometer; the validator drops the raw sensors and
    keeps Sensor Fusion. The fusion `outputs` list selects any subset
    of {quaternion, euler_angle, linear_acc, gravity, corrected_acc,
    corrected_gyro, corrected_mag} — the API enables and subscribes
    each independently from one fusion run. MMRL devices don't have
    an ambient light sensor; Ambient Light is dropped from MMRL
    configs. MMRL also uses the BMI160 gyroscope, so a `Gyroscope`
    entry on MMRL is renamed to `Gyroscope160` during validation.

    If a sibling `fs_config.local.json` exists alongside `path`, its
    contents are deep-merged on top of the base config. The local
    file is gitignored and carries per-host overrides (e.g. the OSC
    target IP) so the committed config stays generic.
    """
    with open(path, "r") as f:
        config = json.load(f)

    local_path = _local_override_path(path)
    if os.path.exists(local_path):
        log.info("loading local override from %s", local_path)
        with open(local_path, "r") as f:
            local = json.load(f)
        config = _deep_merge(config, local)

    return config


def validate_config(config) -> dict:
    """
    Validate and augment a bundled config in one pass. Logs problems and
    sets `config["valid"]`. Callers are expected to gate on that flag
    (`assert config["valid"]`) before constructing `MetaWearState`s —
    the state class trusts what it gets.
    """
    log.info("validating configuration")
    valid = True

    if "network" not in config:
        log.error("missing 'network' section")
        valid = False
    elif not _validate_network(config["network"]):
        valid = False

    devices = config.get("metawear", {}).get("devices", [])
    if not devices:
        log.error("no devices configured")
        valid = False
    for device in devices:
        if not _validate_device(device):
            valid = False

    config["valid"] = valid
    return config


def _validate_network(network) -> bool:
    valid = True

    if not is_valid_ip(network.get("ip", "")):
        log.error("invalid IP address: %r", network.get("ip"))
        valid = False

    port = network.get("port")
    if not isinstance(port, int):
        try:
            network["port"] = int(port)
        except (TypeError, ValueError):
            log.error("invalid port: %r", port)
            valid = False
    if not is_valid_port(network.get("port", -1)):
        log.error("port out of range: %r", network.get("port"))
        valid = False

    return valid


def _validate_device(device) -> bool:
    valid = True
    sensors = device.setdefault("sensors", {})
    mac = device.get("mac", "")
    name = device.get("name", "").lower()

    if not is_valid_mac(mac):
        log.error("invalid MAC: %r", mac)
        valid = False

    if name not in SUPPORTED_DEVICE_NAMES:
        log.error("unsupported device name %r — only %s",
                  device.get("name"), "/".join(SUPPORTED_DEVICE_NAMES))
        valid = False

    for s in list(sensors.keys()):
        if s not in ALLOWED_SENSORS:
            log.error("[%s] unknown sensor in config: %s", mac, s)
            valid = False

    # Sensor Fusion is exclusive with raw IMU axes.
    if "Sensor Fusion" in sensors:
        for raw in NON_FUSION:
            if raw in sensors:
                log.warning("[%s] dropping %s — incompatible with Sensor Fusion",
                            mac, raw)
                del sensors[raw]

        sf = sensors["Sensor Fusion"]
        raw_outputs = sf.get("outputs")
        if not isinstance(raw_outputs, list) or not raw_outputs:
            log.error("[%s] Sensor Fusion config missing 'outputs' list", mac)
            valid = False
            device["fusion_outputs"] = []
        else:
            normalized = []
            seen = set()
            for o in raw_outputs:
                s = str(o).lower()
                if s not in ALLOWED_FUSION_OUTPUTS:
                    log.error("[%s] Sensor Fusion: unknown output %r — allowed: %s",
                              mac, o, ALLOWED_FUSION_OUTPUTS)
                    valid = False
                elif s not in seen:
                    seen.add(s)
                    normalized.append(s)
            sf["outputs"] = normalized
            device["fusion_outputs"] = normalized
    else:
        device["fusion_outputs"] = []

    # MMRL has no ambient light sensor; its gyro is the BMI160 (Gyroscope160 in our naming).
    if name == "mmrl":
        if "Ambient Light" in sensors:
            log.warning("[%s] MMRL has no ambient light sensor — dropping", mac)
            del sensors["Ambient Light"]
        if "Gyroscope" in sensors:
            sensors["Gyroscope160"] = sensors.pop("Gyroscope")

    return valid


def is_valid_ip(ip) -> bool:
    try:
        ipaddress.ip_address(ip)
        return True
    except (ValueError, TypeError):
        return False


def is_valid_port(port) -> bool:
    try:
        port = int(port)
        return 0 <= port <= 65535
    except (TypeError, ValueError):
        return False


def is_valid_mac(mac) -> bool:
    pattern = r"^([0-9A-Fa-f]{2}[:-]){5}([0-9A-Fa-f]{2})$"
    return bool(re.match(pattern, mac or ""))


def retrieve_default_settings(sensor, parameter):
    defaults = {
        "Accelerometer": {"odr": 25, "range": 16.0},
        "Gyroscope": {"odr": 25, "range": 2000.0},
        "Magnetometer": {"odr": 25},
        "Temperature": {"period": 1},
        "Ambient Light": {"odr": 10},
    }
    try:
        return defaults[sensor][parameter]
    except KeyError:
        return None
