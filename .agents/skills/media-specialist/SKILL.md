---
name: media-specialist
description: GStreamer patterns for streamer-infra-media — webrtcbin against Arlo's non-bundled FreeSWITCH gateway, the persistent gst-rtsp-server media with the idle/live splice, live-loss detectors, and the media lifecycle. Use before touching webrtc_pipeline.rs, gst_pipeline.rs, rtsp.rs or pipeline_desc.rs.
---
# Media Specialist Skill

Target: GStreamer 1.26 (Debian trixie, the `rust-build` distrobox and the image); keep
1.22 working (Debian 12). `webrtc_pipeline.rs`, `gst_pipeline.rs` and `rtsp.rs` are
exercised by `tests/live_session.rs` (a local `webrtcbin` gateway + an RTSP client) —
put every decision that can be pure into `live_watch.rs`, `pipeline_desc.rs` or
`splice.rs`, extend the integration test for behaviour a viewer can see, and validate
Arlo-specific behaviour with the `live-validation` skill.

## Integration test harness (`tests/live_session.rs`, `tests/support/`)

- `FakeGateway` implements `WebrtcSignaler` with a second `webrtcbin`: non-bundled,
  audio sendrecv, white `videotestsrc` → H.264 pt 103 through a `valve`
  (`stall_video()` closes it); `hanging_up()` answers then drops the call.
- `RtspProbe` plays the mount over TCP, decodes to GRAY8 and keeps the mean luma: idle is
  black (< 80), live is white (> 180); `interrupted()` records any EOS or error.
- Tests share the default GLib main context through `RtspServer`, so they hold a static
  lock and run one at a time. `STREAMER_REQUIRE_GST_IT=1` turns a missing element into a
  failure (CI); locally the tests skip without the plugins.
- A recorded session cannot be replayed (DTLS keys are per call) — hence a live peer.

## webrtcbin against Arlo (FreeSWITCH, non-bundled)

1. `bundle-policy=none`; apply the answer verbatim. `webrtc-rs` cannot negotiate it.
2. m0 = audio Opus **sendrecv** from a silent `audiotestsrc` chain linked into `sink_%u`
   (request the pad **with caps**: 1.22 returns NULL otherwise). m1 = video H.264
   **recvonly** via `add-transceiver`. Audio first, then video. FreeSWITCH relays video
   only once the audio leg exists.
3. Every src pad goes to an `appsink` (video → `sinks.video`, Opus → `sinks.audio`).
   An unlinked pad fails with `GST_FLOW_NOT_LINKED` on `nicesrc`.
4. Percent-encode TURN userinfo (`pct`); Arlo credentials contain `: / = +`.
   Arlo's TCP TURN is unusable — keep STUN + UDP TURN only.
5. Keyframes: send an upstream `GstForceKeyUnit` with `send_event` on the webrtcbin
   **src** pad, every 3 s. Arlo withholds video until it gets a PLI.
6. Payload types are pinned: H.264 103, Opus 111 (`H264_PT`/`OPUS_PT` must equal
   `LIVE_RTP_H264_PT`/`LIVE_RTP_OPUS_PT` in `pipeline_desc.rs`).
7. Non-trickle: send the offer once `ice-gathering-state` is `complete`.

## `WebrtcLive` lifetime and live-loss detection (ADR 0004)

- `WebrtcLive::owning(pipeline)` is taken on `start`'s first line: every early `?` and
  every cancellation stops the pipeline through `Drop`. Keep `start` **cancel-safe** —
  the multiplexer drops it when a detector fires during setup (`setup_or_loss`).
- `shutdown` is idempotent (`stopped` flag): the multiplexer calls it and `Drop` runs it.
  It aborts tasks, posts `BUS_WATCH_STOP` **before** `set_state(Null)` (a flushing bus
  drops posts and the bus thread would block forever), then goes to `Null`.
- Detectors share one `LiveLossNotifier`, first wins: bus `ERROR`/`EOS`,
  `connection-state`/`ice-connection-state` `failed|closed` (`disconnected` only logs;
  it may recover), and the stall watchdog — spawned **after** the first RTP, never
  counting silence during negotiation (`webrtc.live_stall_timeout_secs`, floor 4 s).
- A loss before the first RTP fails the attach with `MediaError::LostDuringSetup(reason)`;
  no RTP at all is `SpliceTimeout` after 20 s.

## Persistent RTSP media and the splice

- One factory per camera, `shared=true`, `suspend-mode=None`. Never re-bind
  `set_launch` on a running media: it sends EOS to clients.
- **Video**: idle and decoded live both produce raw I420 at identical caps into
  `input-selector name=sel`; one encoder `video_enc` downstream, so clients see one
  SPS/PPS. Force-key-unit the encoder on every flip. Attach flips on the first decoded
  raw buffer at `sel.sink_1`; detach flips synchronously.
- **Audio**: `audiomixer`, never a second `input-selector` (an empty inactive selector
  pad stalls media prepare). Silent bed + live `rtpopusdepay ! opusdec` mixed, all
  pinned to **F32LE** stereo 48 kHz — `avenc_aac` rejects S16LE ("not-negotiated").
- Idle still: `gdkpixbufoverlay name=idle_overlay` is a pass-through until a thumbnail
  file is set; a rebuilt media re-applies it at `media-configure`.
- Pin each branch's final caps in one capsfilter; keep launch strings as pure builders
  in `pipeline_desc.rs` with their constants beside them.

## Media lifecycle vs live session

gst-rtsp-server builds the media on the first client and unprepares it after the last,
so a session can start with no media or outlive it.

- `WiringSlot` holds the **current** media's elements: set at `media-configure`,
  cleared at `unprepared` by media identity (not by "last one wins").
- Pumps (`spawn_live_pump`) look the appsrc up **per buffer** and discard while the slot
  is empty; they never stop on a refused push.
- `live_active` arms the switch on a media configured mid-session, so a reconnecting
  client gets live video within one keyframe interval.
- Locks go through the poison-tolerant `lock()` helper.

## GLib/threading traps

- Never `bus.add_watch_local()` inside `media-configure` ("no default main context");
  use `bus.set_sync_handler` for diagnostics, or a dedicated thread with `iter_timed`
  that exits on EOS or `BUS_WATCH_STOP`.
- Signal closures hold `WeakRef`s to the pipeline/pads, never strong refs.
- `gupnp … 1900: Address already in use` at live start is harmless.
- VAAPI needs `/dev/dri` in the container.
