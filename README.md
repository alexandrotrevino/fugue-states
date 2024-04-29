# Project Fugue States

**Fugue States (FS)**

An experiment in mapping motion sensor data to music/sound. 

**Design**

FS leverages the MBIENTLAB/MetaWear MMS sensor and API.

The FS project contains high level mappings to intialize and access MMS
data:
- Accelerometer
- Gyrometer
- Magnometer
- Temperature

...as well as processed data sent from the sensor to a client over Bluetooth.

Note: The MBIENTLAB API is only supported in Linux. 

On the sound side, this project is being programmed in PlugData, an IDE
for PureData. 

### Outline

```
/fugue-states
+ /sense
+ /patch
```

The `sense` directory contains custom abstractions for the configuration
and usage of the MetaWear sensor. 

The `patch` directory contains abstractions in PlugData (PureData) for
receiving sensor data via the Open Sound Control (OSC) protocol; parsing
multiplexed sensor data; graphical control interfaces; and mapping data
to arbitrary outputs. 

### Roadmap

For a detailed roadmap, see [this page](https://cerulean-comic-604.notion.site/7fee658729f44f1b9ba1b0b9cd5b3802?v=36453209d7764227a6e8888f48866f06).

