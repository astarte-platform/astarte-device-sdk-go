# Astarte Go Device SDK

Go Device SDK for [Astarte](https://github.com/astarte-platform). Create Astarte Devices and Simulators with Go.

The SDK simplifies all the low level operations of the [Astarte MQTTv1 protocol](https://docs.astarte-platform.org/latest/080-mqtt-v1-protocol.html) (such as pairing, property sync, data validation, reconnection...) and exposes an high-level API to interact with Astarte from your device.

## Requirements

If you plan to use the default persistent storage (SQLite), you'll have to compile using `CGO_ENABLED=1`, as the `go-sqlite3` driver uses *cgo*.

## Getting started

Have a look at the `examples/` folder for a minimal example showing how to register a Device and send data.