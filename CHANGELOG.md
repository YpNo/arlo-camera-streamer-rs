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
- Per-camera state machine (`Idle`, `Activating`, `Live`,
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
- `list-devices` subcommand: signs in, lists the account's cameras and
  doorbells with their device ids, marks the configured ones, warns about
  configured ids the account does not have, and prints a `[[cameras]]`
  block with a suggested stream name for each unconfigured device. It
  starts no server and completes the MFA pairing for the daemon. `run`
  is the default subcommand, so existing invocations are unchanged.
- The idle still follows Arlo's snapshots: a `presignedLastImageUrl`
  announced on the event bus is cached (in memory, never logged, https
  only, trusted 10 minutes) and used by the thumbnail fetch before the
  device-list query. While the idle image is on screen (idle,
  battery-protect, failed) each new snapshot refreshes it at once; at the
  end of a session the refresh uses the cached snapshot and saves a cloud
  round-trip.
- GStreamer integration tests (`streamer-infra-media/tests/live_session.rs`):
  real live sessions through the production media stack against a local
  `webrtcbin` gateway and an RTSP client, covering the idle → live → idle
  splice on one connection, a client joining mid-session, a stalled
  source and a call lost during setup. CI installs the runtime plugins
  and requires them (`STREAMER_REQUIRE_GST_IT=1`). `RtspServer::bound_port`
  reports the port of an ephemeral bind.
- Capture aid for the Arlo bus: `RUST_LOG=streamer_infra_arlo::events=debug`
  logs unmapped camera events with their property keys (never values);
  `trace` logs every event the same way.

### Changed
- Every motion / audio pulse is logged at `debug`, on the bus
  (`camera trigger pulse`) and in the orchestrator (`trigger pulse` with
  its outcome and when the session will end), so a capture shows how a
  long motion keeps a session alive.
- `CameraState::Live` no longer carries `since_secs`, which was never
  updated (logs showed `since_secs: 0` after minutes of live); the
  debouncer has always timed the session.
- The `Cooling` camera state is gone: it was declared but never entered.
  The admin snapshot loses `cooling_remaining` (never serialized, it was
  always empty) and `streamer_camera_state` loses its always-zero
  `cooling` series.
- Logs go to stderr (stdout is reserved for command output such as
  `list-devices`). Containers and systemd capture both unchanged.
- Toolchain and MSRV raised to 1.98.1 to follow `arlo-rs` 0.2.0, which is
  now consumed from crates.io instead of a sibling checkout.
- `arlo-rs` 0.2.1: Arlo error codes stay structured (error 14001 is read
  from the code, not the message) and `mediaUploadNotification` events are
  decoded instead of logged as dropped.

### Fixed
- A live session started while no RTSP client was connected sent its
  video to a discard sink for its whole duration, even after a client
  connected; a client reconnecting mid-session lost the live video the
  same way (the pumps stopped at the first push into the torn-down
  media). The live video now follows the media that currently exists and
  is spliced in within one keyframe interval of a client connecting.
- Admin wake and force-idle are acknowledged when dequeued instead of after
  the WebRTC negotiation completed, which exceeded the 2 s reply timeout
  and reported a healthy wake as "orchestrator unavailable".
- The session cache directory is created at boot; a missing directory
  silently cost a new OTP on every restart.
- The WebRTC bus-watch thread now exits with its session instead of
  leaking one blocked thread per live session.
- An attach whose WebRTC leg dies before the first video packet (ICE
  failed, pipeline error) fails at once with the detector's reason
  (`live source lost during setup: peer-disconnected`) instead of after
  the 20 s first-RTP timeout. A failed or abandoned attach now stops its
  `webrtcbin` pipeline; it used to stay in `PLAYING` with its bus-watch
  thread blocked.
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
