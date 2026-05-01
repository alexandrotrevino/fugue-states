# Project Fugue States

**Fugue States (FS)**

An experiment in mapping motion sensor data to music/sound. 

<a href="https://youtu.be/hfbqUgFDJvE">
  <img src="https://i3.ytimg.com/vi/hfbqUgFDJvE/maxresdefault.jpg" alt="Thumbnail" width="480" height="270">
</a>

**Design**

FS leverages MBIENTLAB/MetaWear sensors (MMS and MMRL) and API.

The FS project contains high level mappings to intialize and access MMS
data:
- Accelerometer
- Gyrometer
- Magnometer
- Temperature

...as well as processed data, sent from the sensor to a client over Bluetooth.

Note: The MBIENTLAB API is only supported in Linux. 

On the sound side, this project is being programmed in [PlugData](https://plugdata.org/), an IDE
for PureData. This is free and open source software, and the developers welcome your support!

### Outline

```
/fugue-states
+ /sense
+ /patch
```

The `sense` directory contains custom abstractions for the configuration
and usage of the MetaWear sensor. These structures call the MBIENTLAB API,
manage the Bluetooth LE (BLE) connection, parse the sensor data, and send
it over network via the Open Sound Control (OSC) protocol. They can also
receive OSC control messages to start and stop data streaming or, in the
future, configure the sensors remotely. 

The `patch` directory contains abstractions in PlugData (PureData) for
receiving sensor data via the BLE/OSC pathway discussed above; parsing
multiplexed sensor data; graphical control interfaces; and mapping data
to arbitrary outputs. Aside from ingesting data, the `patch` aspect of
the project also hopefully explores creative and musical instrumentation
using the input data.

### Running on the Pi at boot (systemd)

To run `run_fs.py` automatically when the Pi powers on, install the
included systemd unit:

```
sudo cp deploy/fugue-states.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now fugue-states
```

The service runs as user `pi` from `/home/pi/fugue-states` in
`--mode button-driven` — devices connect on boot, double-press a
sensor button to start/stop streaming. If your checkout lives
elsewhere, adjust `WorkingDirectory=` in the unit file before copying.

Inspect and control the service:

```
systemctl status fugue-states         # is it running?
journalctl -u fugue-states -f         # follow logs live
sudo systemctl restart fugue-states
sudo systemctl stop fugue-states      # stop until next boot
sudo systemctl disable fugue-states   # don't start at boot
```

For development, stop the service before running `run_fs.py` manually
so the two don't fight over the BLE adapter.

If sensors aren't powered on at boot, the service retries 5 times with
5-second backoff, then goes "failed". Power the sensors on, then
`sudo systemctl restart fugue-states` to wake it back up.

### Roadmap

For a detailed roadmap, see [this page](https://cerulean-comic-604.notion.site/7fee658729f44f1b9ba1b0b9cd5b3802?v=36453209d7764227a6e8888f48866f06).
I will update the repository with more tidbits (diagrams, videos, and usable demonstrations) as they develop.

