# Project Fugue States

Fugue States (FS)
An experiment in mapping motion sensor data to music/sound. 

Leverages the MBIENTLAB/MetaWear MMS sensor and API.
Mappings for
- Accelerometer
- Gyrometer
- Magnometer
- Temperature
and processed data sent from the sensor to a client over Bluetooth.
The MBIENTLAB API is only supported in Linux. 

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