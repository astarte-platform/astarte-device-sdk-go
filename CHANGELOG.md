# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/en/1.0.0/)
and this project adheres to [Semantic Versioning](http://semver.org/spec/v2.0.0.html).

## Unreleased
## Added
- Introduce the `KeepAlive` device option to configure the MQTT connection keepalive. Default to 30s,
  which was the value used before.

## [0.90.2] - 2022-07-04
## Added
- Introduce the `DeviceOptions` struct to expose all possible configuration options for the device.
- Add `NewDeviceWithPersistency` as default constructor.
- Add a `NewDeviceWithOptions` constructor to allow device configurations different from the default one.
- Allow the device to send purge properties messages.

## Changed
- Deprecate the `NewDevice` constructor. `NewDevice` will be removed in the next version.
- Allow to use a db different from SQLite for persistency. CGO is now optional, if SQLite is not used.
- Use ECDSA instead of RSA private keys.
- `AddInterface` does no more perform validation, as this is already done by `astarte-go`.
  The return value of `AddInterface` is always `nil`.
- Update Go to v1.17.

## Fixed
- Do not send data before all Astarte MQTTv1 initialisation messages have been sent, fixing
  a race condition which sometimes resulted in device disconnection.
  
## [0.90.1] - 2022-02-14
## Added
- Allow to ignore client-side SSL errors with the `Device.IgnoreSSLErrors` field. Defaults to `false`.
- Add messages (when retention: stored) and properties persistency.
  This requires a SQLite db and compiling with CGO.
- Add `SetProperty` and `UnsetProperty` methods for setting and unsetting properties on device side.
- Add `GetProperty`, `GetAllPropertiesForInterface` and `GetAllProperties` methods for retrieving properties.
- Handle reception of purge properties messages from Astarte.

## Changed
- Do not redo all initialisation steps of Astarte MQTTv1 protocol at each device reconnection, but only
  if there is no session present on the broker.
- Send properties only if their value has changed.
- Replace eclipse/paho.mqtt.golang dep with ispirata/paho.mqtt.golang, which handles the sessionPresent
  MQTT flag correctly.
- Deprecate `SendIndividualMessageWithTimestamp` and `SendIndividualMessage` methods for sending properties.

## Fixed
- Do not to panic when checking certificate validity.

## [0.90.0] - 2021-08-17
## Added
- First Astarte Device SDK Go release.