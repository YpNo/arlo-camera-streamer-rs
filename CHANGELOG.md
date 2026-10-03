# Changelog

All notable changes to `arlo-camera-streamer-rs` are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0] - 2026-10-03

First release: the daemon as validated on the owner's camera and box.

### Security
- The app-view relay verifies the watch-along host's certificate chain
  against the system roots (hostname check waived, since the host is a
  raw IP) or pins a configured fingerprint (`arlo.watch_along_cert_sha256`);
  it used to accept any certificate. Its RTSP parser bounds every text
  line, header count and `Content-Length` from the server.
- A failing thumbnail fetch no longer puts the presigned URL into the
  error text and the warning log.
- The relay accepts the SDP's AAC `config` only as hex, builds the
  appsrc caps with the typed builder rather than from text, refuses
  interleaved channel pairs that overlap, bounds every write to the
  server by the request timeout, and takes its read task down with it
  when the writer is aborted.
- The container image is scanned per platform before any digest is
  tagged; the base images and the BuildKit frontend are pinned by digest.
- The thumbnail fetch treats the storage endpoint as untrusted: `https`
  only (the device-list URL is checked like the bus URL), 10 s request
  and 5 s connect timeouts, at most two redirects, a 2 MiB body cap read
  chunk by chunk, and the bytes must start with the JPEG magic before
  they reach the image loader.
- Idle thumbnails are written to a `thumbnails/` directory beside the
  session cache (created owner-only at boot) instead of the shared
  system temp directory, through a freshly created temp file; another
  user on the host can no longer plant or block them.
- A rejected `/admin/*` request is logged (route only, never the token);
  the WebRTC session id is no longer logged in full.
- Camera ids are validated (`[A-Za-z0-9_-]{1,64}`) wherever they enter:
  the configuration, the `/admin/cameras/{id}` path (`400` otherwise) and
  the event bus, so an id can neither escape the thumbnail directory nor
  forge a log line. Text that arrives from the cloud or a library is
  stripped of control characters and bounded at 256 bytes before it
  reaches the `Failed` state, the admin API or the logs.
- The ops and admin listeners run with a 10 s header-read timeout and a
  cap of 64 open connections each; the RTSP server keeps at most 64
  sessions. `STREAMER_ADMIN_TOKEN` must be at least 16 bytes, is held as
  a secret zeroed on drop and compared with `subtle`. The Arlo and IMAP
  passwords are moved into the client, never copied.
- The app-view relay refuses a plaintext `rtsp://` URL to a remote host
  (the egress token would travel in clear); the redacted URL no longer
  shows user info and the scheme error no longer echoes the input. The
  TURN credential is redacted from `Debug` output.
- The session cache directory is created owner-only (`0700`) and the boot
  fails when that mode cannot be applied; an HLS stream directory must be
  a real directory directly under the configured HLS root.
- The container runs as `10001:10001` (numeric), without `wget` or the
  GStreamer tools; the health check is the binary's own `healthcheck`
  subcommand. Every image digest carries SLSA provenance and an SPDX
  SBOM; the security job scans the repository history with gitleaks.

### Changed
- The configuration is validated at boot: unknown keys are errors
  (`deny_unknown_fields` on every table), duplicate `arlo_device_id` or
  `stream_name` entries are refused, and the cooldown values must be in
  range (1 to 86400 s for `debounce_secs` and `max_continuous_live`, 0 to
  86400 s for `daily_live_budget` and `user_view_probe_secs`).
- A live session is cut the moment the daily budget runs out (the quota
  used to be checked only when motion arrived), and every activation
  charges 15 s of quota on top of the live time. `POST /admin/cameras/{id}/wake`
  answers `429` within 30 s of the previous session's end.

### Fixed
- A camera reported `Offline` while idle entered `Failed` without a
  backoff deadline and stayed there until a restart; every entry into
  `Failed` now arms the backoff, and an `Online` report ends it early.
- `[webrtc] ice_address_family = "ipv4"` panicked on the first activation
  (webrtcbin's property is `ice-agent`, not `ice`).
- A second push to `main` during a release could cancel the image build
  after the tag existed; pushes to `main` are no longer cancelled, the
  image jobs key on "no image for this version yet" so a re-run
  publishes it, and every CI job has a timeout.
- The daemon no longer runs as a zombie: when the Arlo event bus ends, or
  a camera task ends or panics, the system cancels itself, every camera
  releases its session, and the process exits non-zero after the drain
  so a restart policy takes over. A closed event mailbox now runs the
  same release as a shutdown.
- `GET /admin/state` and `GET /admin/cameras/{id}` read a snapshot the
  camera task publishes after every step, instead of asking the task and
  timing out at 2 s with a synthetic `unresponsive` entry while it was
  negotiating WebRTC.
- A failing update of the "live in the Arlo app" notice after the view's
  hold ran out re-fired the loop at CPU speed; it is retried every 30 s.
- A clock stepped backwards across the budget's reset time left the
  quota exhausted for good; the budget now refills on any date change.
- A `%` in `output.hls.dir` reached `splitmuxsink`'s printf pattern
  (`%s` crashed the daemon); it is escaped in the segment pattern.
- A registration that failed after the RTSP mount was installed left the
  mount published and unknown to the registry; it is removed again.
- The live appsrcs queued without bound while the live branch did not
  consume; they now drop past 4 MiB or 2048 packets (`leaky-type`).
- An answer webrtcbin rejects is reported at once with its reason instead
  of as a splice timeout 20 s later; a failed local description is logged.
- The live leg's teardown (the pipeline's `Null` transition) runs off the
  async runtime and outside the multiplexer's lock. Registering a camera
  holds its lock from the check to the insert.
- An admin command no longer waits on a full mailbox; the request is
  answered `503` at once, as documented.
- ICE servers are filtered case-insensitively (`TURNS`, `TCP`), the admin
  `WWW-Authenticate` header is a constant, the configuration file is read
  without blocking the runtime, and a poisoned device-cache lock is
  tolerated instead of panicking.

### Added
- The container image is published to GitHub's registry on every release
  (`ghcr.io/ypno/arlo-camera-streamer-rs:v<version>`, `:latest`,
  `:sha-<commit>`), built per platform on native runners, scanned with
  Trivy before it is tagged; `linux/arm64` is opt-in through the
  `IMAGE_PLATFORMS` repository variable.
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
- `output.video_encoder = "auto"` (new default) probes the host at boot
  and keeps the first working H.264 encoder: NVIDIA (`nvenc`), Intel/AMD
  (`va`, then `vaapi`), Raspberry Pi 4 family (`v4l2`), then software
  `x264`. Explicit names fail the boot when the encoder does not work on
  that host (ADR 0008). The Docker image carries the VA plugin and drivers.
- A live view you start in the Arlo app is relayed, picture and sound,
  to the RTSP and HLS outputs (ADR 0007; the relay's RTSP client tolerates the unframed
  RTCP and keep-alive packets Arlo's server sends between interleaved
  frames by resynchronising on the next frame header, and reports any
  other framing surprise with a hex dump; since the relay keeps the
  camera streaming after the app closes its view, it lets go every
  `cameras.cooldown.user_view_probe_secs` for a few seconds so the
  camera can report `idle`, and resumes otherwise): the daemon fetches the
  view's own RTSPS stream as
  the app identity (`arlo.app_version`) and splices it in like a motion
  session, picture and sound, with no cooldown and no daily-budget charge. It
  ends with the view; a view that cannot be relayed leaves the idle
  frame reading `LIVE IN ARLO APP · <stream>` (ADR 0005) and is retried
  30 s later. The admin snapshot gains `live_source`
  (`motion` / `user-view`).
- HLS output (ADR 0006): with `[output.hls]` set, each camera's RTSP
  output is repackaged without re-encoding into
  `<dir>/<stream_name>/index.m3u8` and segments by a loopback segmenter.
  Retention is bounded (`playlist_length` + 2 segments on disk), the
  directory is cleared of stale files at start and stop, and the
  segmenter restarts with backoff. `[output.dash]` is still parsed but
  ignored with a warning that explains why.
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

### Removed
- The Phase-4 factory-rebind launch builders (`idle_launch_string`,
  `live_launch_string`, `idle_video_desc`, `idle_video_jpeg_desc`,
  `idle_video_synthetic_desc`, `idle_audio_desc`, `live_video_desc`,
  `live_audio_desc`), unused since the persistent splice pipeline.

### Changed
- `WebrtcSignaler` is one call per attempt: `negotiate(camera, &mut dyn
  OfferBuilder)` fetches the call's coordinates, hands the ICE servers
  to the media adapter to build the offer, and carries it to Arlo. The
  separate `ice_servers` step and the per-camera `SipInfo` cache it
  needed are gone.
- Every motion / audio pulse is logged at `debug`, on the bus
  (`camera trigger pulse kind=motion`) and in the orchestrator (`trigger pulse` with
  its outcome and when the session will end), so a capture shows how a
  long motion keeps a session alive. The `idle` report Arlo sends after
  every motion snapshot is logged at `trace` while our session runs.
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
- `arlo-rs` 0.2.2: `get_stream_url_as` and the iOS-app identity the
  app-view relay (ADR 0007) queries the stream with.
- `arlo-rs` 0.2.3: Arlo's error 9261 ("Invalid factor data"), which a
  fresh install's device id gets from the trusted-browser probe, is
  classified as an untrusted browser like 9204 rather than falling
  through as an unknown code.

### Fixed
- A failed attach (no RTP within 20 s, a loss during setup, a refused
  call) left the camera's live sinks armed, so every later attach
  failed with "already in live mode" until a restart. A failed attach
  now releases them.
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

[Unreleased]: https://github.com/YpNo/arlo-camera-streamer-rs/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/YpNo/arlo-camera-streamer-rs/releases/tag/v0.1.0
