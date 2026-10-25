# ADR 0009 — Camera state over MQTT for Home Assistant

- **Status:** Accepted
- **Date:** 2026-10-10
- **Deciders:** Senior architect, project owner (red-teamed before code)
- **Supersedes:** —
- **Superseded by:** —

## Context

Issue #3 parks "MQTT state publishing for Home Assistant". Today the
daemon's state is visible through `/metrics` (Prometheus) and the admin
API (bearer token, loopback by default). Home Assistant users integrate
devices through an MQTT broker and HA's MQTT discovery: a device that
publishes retained discovery documents appears in HA with its entities,
no YAML. Frigate runs alongside HA with the same broker in most installs.

What the daemon can already tell, per camera, from the published
`CameraSnapshot` (read without disturbing the camera tasks): state
(`idle`, `activating`, `live`, `battery-protect`, `failed`), live source
(`motion` / `user-view`), whether the app is viewing, live seconds today
and the daily budget, the last failure. System-wide: whether the Arlo bus
is connected. It also holds each camera's idle thumbnail.

## Decision

An optional, **publish-only** MQTT adapter, off unless `[mqtt]` is
configured:

```toml
[mqtt]
url              = "mqtts://broker.lan:8883"    # or mqtt:// (1883); no user:pass@
username         = "arlo"                        # optional
password_env     = "STREAMER_MQTT_PASSWORD"      # optional
ca_file          = "/etc/arlo-streamer/ca.pem"   # optional, for a private CA
base_topic       = "arlo-camera-streamer"        # default
discovery_prefix = "homeassistant"               # default; "" turns discovery off
thumbnails       = false                         # publish the idle JPEG; default off
```

- **No commands.** The adapter subscribes to nothing. A command topic
  would let any client of a shared HA broker wake a battery camera, and
  a retained `wake` would replay on every restart past the re-activation
  guard (it starts empty at boot). Waking stays on the admin API.
- **No new dependency.** A publish-only client needs CONNECT (with a last
  will and login), PUBLISH, PINGREQ and DISCONNECT, and reads CONNACK and
  PINGRESP. It is written on `mqttbytes` (the MQTT 3.1.1 codec arlo-rs
  already brings) over tokio, with TLS through the workspace's rustls
  and `ring` provider and the system roots, plus `ca_file` (the relay's
  setup). `rumqttc` was the first choice and was rejected: it pins
  `rustls-webpki` 0.102 (advisories fixed only in 0.103), its `mqtts`
  URL path panics on this tree's rustls features, and it has no
  reconnect backoff.
- **Source of state:** the existing `AdminControl` port, read every
  second (`snapshot()` reads the watch channels the orchestrators
  publish) and published on change, compared against what was last
  *written* to the broker, so an update lost to a disconnection is
  written by the next tick. Thumbnails, when on, through
  `AdminControl::thumbnail` every 30 s, at most 512 KiB (a larger one is
  skipped with one warning: EMQX drops a client over its 1 MiB default).
  No new domain port.
- **Topics**, keyed by the device id (stable across renames; validated
  `[A-Za-z0-9_-]`, never an MQTT wildcard or separator):
  - `<base>/status`: `online` (retained) after each connect; `offline`
    as the last will and on a clean stop;
  - `<base>/bridge`: `{"arlo_connected": …, "version": …}`, retained;
  - `<base>/<device_id>/state`: one retained JSON document per camera
    (`stream_name`, `state`, `live`, `live_source`, `user_view`,
    `live_secs_today`, `daily_budget_secs`, `last_failure`);
  - `<base>/<device_id>/thumbnail`: the idle JPEG, retained, only with
    `thumbnails = true`.
- **Discovery** (retained, sent after every connect):
  `<discovery_prefix>/<component>/arlo_streamer_<device_id>/<object>/config`.
  Per camera, one HA device: a `sensor` for the state, `binary_sensor`s
  for live and app view, a `sensor` for live seconds today, and a
  `camera` for the thumbnail when on. The bridge: a `binary_sensor` for
  the Arlo connection. `unique_id` and device identifiers derive from the
  device id only; `availability_topic` is `<base>/status`.
- **Removed cameras:** the ids published are recorded in the state
  directory (`mqtt-published.json`). After a connect, every topic of an
  id no longer configured gets an empty retained message (HA deletes the
  entity), then the record is rewritten.
- **Connection:** QoS 0, clean session, client id
  `arlo-camera-streamer-<base_topic>` (two daemons on one broker need
  different base topics). After each connect: `online`, discovery, the
  removals, the full state. A failed connect or a lost connection waits
  1, 2, 4 … 60 s before the next attempt; the log says so when the
  connection state changes, not on every attempt. Incoming packets are
  read with a 64 KiB cap.
- **Failure isolation:** its own task. A broker that is down or refuses
  the login never stops or delays the daemon, and `/readyz` does not
  depend on it. The URL, credentials and TLS setup are checked before
  the Arlo login (a boot failure after it costs a login).
- **Credentials:** the password from an env var, held as a secret,
  never logged; a URL with `user:pass@` or a query is refused. A
  password over plain `mqtt://` to a host that is not loopback is allowed
  (LAN brokers often lack TLS) with a warning at boot.
- **Shutdown:** the adapter has its own stop signal, given after the
  cameras are released: it publishes `offline` and disconnects.

## Consequences

### Positive

- Cameras appear in Home Assistant with their state and live/app-view
  flags, without YAML; automations can react to a camera going live.
- Nothing on the broker can act on a camera.
- No camera task does any work for it: one snapshot read a second and a
  publish on change.

### Negative / risks

- **About 300 lines of MQTT client of our own** (connect, keep-alive,
  backoff, TLS) to test and maintain, on a codec (`mqttbytes`) released
  last in 2021; MQTT 3.1.1 itself is frozen.
- **Up to one second of delay** between a state change and its message
  (polling, not push). Motion itself is not published (Frigate already
  does that, from the picture).
- **Thumbnails leave the admin token's protection** when turned on:
  readable by any client of the broker, and in clear over `mqtt://`.
  Off by default for that reason.
- **Battery level is not published**: it arrives on the Arlo bus, not in
  the snapshot. A later addition.

## Alternatives considered

- **`rumqttc`**: see above (vulnerable `rustls-webpki` pin, panicking TLS
  path, no backoff).
- **`paho-mqtt`**: wraps the C library (FFI, a C build in the image).
- **Commands (wake / idle buttons)**: dropped, see the Decision. The
  admin API keeps that role, behind its token.
- **A push port from the orchestrators**: lower latency, but a new
  domain port and work on the camera tasks' path for a one-second gain.
- **An MQTT exporter of `/metrics`**: no discovery, a second system to
  run.
