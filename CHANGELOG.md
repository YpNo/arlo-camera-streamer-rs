# Changelog

All notable changes to `arlo-camera-streamer-rs` are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Per-camera bridge from Arlo's event-driven live stream to a persistent
  local RTSP output: idle still frame (last thumbnail over a STANDBY
  overlay), motion-triggered WebRTC ingest through `webrtcbin`, and a
  seamless `input-selector` / `audiomixer` splice so clients never
  reconnect (ADR 0003).
- Per-camera state machine (`Idle`, `Activating`, `Live`, `Cooling`,
  `BatteryProtect`, `Failed`) with debounce, continuous-live cap, daily
  live budget and exponential backoff.
- Arlo adapter on `arlo-rs` 0.2.0: MQTT-over-WebSocket event bus, email /
  push / SMS multi-factor login with a persisted session, `sipInfo` +
  `hmswebsocketproxy` WebRTC signaling, thumbnail fetch.
- Ops surface: Prometheus `/metrics`, `/healthz`, `/readyz`, and a
  bearer-authenticated `/admin` API (state snapshot, manual wake, force
  idle).
- Configurable H.264 encoder (`x264` software, `vaapi` hardware) and ICE
  address-family policy (`dual`, `ipv4`).
- Multi-stage Docker image running as a non-root user.
- Live-loss feedback (ADR 0004): `attach_live` returns a `LiveSession`
  handle; a stall watchdog, the pipeline bus and the WebRTC connection
  state report a dead source and the camera returns to idle within
  `webrtc.live_stall_timeout_secs` (default 10) instead of waiting for
  the debounce or the continuous-live cap. The reason is visible as the
  `live-lost-<reason>` signal on the state-transition metric.
- User views in the Arlo app are observed (ADR 0005): while the camera
  reports `activityState == "userStreamActive"`, motion does not start a
  session (Arlo would refuse it) and is counted as `suppressed-user-view`;
  a session already running continues. An attach refused with Arlo error
  14001 returns to idle as `camera-busy` instead of failing into backoff.
  The admin camera snapshot gains `user_view`.
- Capture aid for the Arlo bus: `RUST_LOG=streamer_infra_arlo::events=debug`
  logs unmapped camera events with their property keys (never values);
  `trace` logs every event the same way.

### Changed
- Toolchain and MSRV raised to 1.98.1 to follow `arlo-rs` 0.2.0, which is
  now consumed from crates.io instead of a sibling checkout.

### Fixed
- Admin wake and force-idle are acknowledged when dequeued instead of after
  the WebRTC negotiation completed, which exceeded the 2 s reply timeout
  and reported a healthy wake as "orchestrator unavailable".
- The session cache directory is created at boot; a missing directory
  silently cost a new OTP on every restart.
- The WebRTC bus-watch thread now exits with its session instead of
  leaking one blocked thread per live session.
- A motion pulse during battery-protect or a failure backoff no longer
  primes a stale hard-cap deadline that cut the next session short.
- The battery-protect wake-up is rounded up to the reset boundary.
- `config/streamer.example.toml` used an MFA kind (`imap`) the parser never
  accepted; the example now parses and a test keeps it that way.
- Docker image: the build stage lacked the BoringSSL toolchain (cmake,
  libclang, nasm) and the runtime lacked `wget`, so the `HEALTHCHECK`
  could never succeed.
- Documentation still described the retired SSE bus and a Chromium
  requirement; both are gone since the move to `arlo-rs` 0.2.0.
