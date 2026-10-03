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
  (`stall_video()` closes it); `hanging_up()` answers then drops the call; `busy()`
  refuses with `CameraBusy` like Arlo's 14001.
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
  file is set; a rebuilt media re-applies it at `media-configure`. The file lives in
  the registry's `thumbnail_dir` (`GstPipelineRegistry::new` third argument; the bin
  passes `<session_cache_path dir>/thumbnails`, prepared owner-only by
  `prepare_thumbnail_dir` at boot), written through a `create_new` temp file and a
  rename — never the shared temp directory (sweep finding M4). The adapter fetching
  it (`streamer-infra-arlo/thumbnails.rs`) is https-only, bounded (10 s, 2 MiB, two
  redirects) and checks the JPEG magic, so gdk-pixbuf only ever loads our own JPEGs.
- Pin each branch's final caps in one capsfilter; keep launch strings as pure builders
  in `pipeline_desc.rs` with their constants beside them.

## HLS output (ADR 0006)

- `hls.rs`: a per-camera segmenter that is an **RTSP client of the camera's own mount**
  (`RtspServer::loopback_url`) → `rtph264depay ! h264parse` / `rtpmp4adepay ! aacparse`
  → `hlssink2`. Never tap the RTSP media for HLS: it exists only while a client is
  connected. Never re-encode.
- Retention is `max-files = playlist_length + HLS_SEGMENTS_BEYOND_PLAYLIST`; the directory
  is prepared (created, stale files cleared) before the mount is installed, and cleared
  again when the segmenter thread stops. Drop never joins the thread.
- DASH: stock `dashsink` writes only MPEG-TS and never prunes; fMP4 needs gst-plugins-rs.
  Do not "just wire it".

## App-view relay (ADR 0007, `rtsp_relay.rs`)

- Our own RTSP client, not `rtspsrc` (refused at SETUP by Arlo's server, cause unknown).
  OPTIONS → DESCRIBE → one SETUP for the H.264 track (`RTP/AVP/TCP;unicast;interleaved=0-1`)
  → PLAY → two tasks: `read_loop` owns the read half (`$`-framed RTP → PT rewritten to
  `LIVE_RTP_H264_PT` → `LiveSinks.video`; the AAC track's RTP → `LIVE_RTP_AAC_PT` →
  `LiveSinks.aac`, whose caps follow the SDP through `AacFeed::Format` — the SDP-derived
  caps fields must be typed `(string)`, untyped digits parse as ints and the depayloader
  ignores them), `run` owns the write half (`GET_PARAMETER`
  every 25 s, RTCP RR every 5 s, acks of server requests, TEARDOWN). Never read frames
  inside a `select!` with timers: a cancelled read leaves the stream mid-frame.
- **Arlo's server framing (captured 2026-10-01/02):** SETUP answers keep `interleaved=0-1` (video) and `2-3` (audio); once the audio track is set up its RTP arrives framed on channel 2 (PT 98) and nothing is bare any more. Without the audio SETUP:
  the first RTCP SR after PLAY is framed on channel 1, the periodic ones arrive **bare**
  (`80 c8 00 06 …`, no `$` header), and the AAC audio track's RTP arrives bare too
  (`80 80 00 01 … 00 10 0e 00 …`, PT 0, AU-headers-length 16, one AU). `read_unframed`
  skips bare RTCP by its own length, relays a bare `AAC-hbr` packet by its AU size
  (`bare_aac_len`); anything else goes through `resync`, which scans to the next `$`
  header `frame_header_at` trusts (our channel, length ≥ a packet header, version 2,
  the stream's RTP payload type or a known RTCP type) and logs the skipped bytes
  (`unframed bytes skipped`, first four at debug). After `RESYNC_LIMIT` (64 KiB) the
  relay ends with `unexpected byte 0x..` + a hex dump — read that line before touching
  the parser.
- TLS: chain verified against the system roots (`rustls-native-certs`), hostname waived
  (raw-IP host) — `WatchAlongVerifier` maps only `NotValidForName*` to accepted; or a
  SHA-256 certificate pin from `arlo.watch_along_cert_sha256`. Server text is bounded:
  `MAX_LINE_BYTES`, `MAX_HEADERS`, `MAX_MESSAGE_BODY` (`read_text_line`, `body_length`);
  the AAC `config` must be hex (`valid_aac_config`), channel pairs distinct and disjoint
  (`channel_pairs_disjoint`), every write `timed_write`-bounded, the read task held in
  `AbortOnDrop`. AAC caps are built with `gst::Caps::builder`, never parsed from text.
- Both legs (`LiveLeg::{Webrtc, Relay}`) go through `arm_live`; the stall watchdog and
  `report_loss` live in `live_watch.rs`.
- Test source: `Stack::install_source("/mount", launch)` on the crate's own RTSP server.

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
- `GStreamer-WARNING … Sticky event misordering, got 'segment' before 'caps'` on
  `funnelN:src` / `rtpbin0:recv_rtcp_sink_N` is harmless: gst-rtsp-server logs it when a
  **UDP** client joins a media already playing for another client (with HLS on, the
  segmenter is always that first client). It is a GLib `g_warning`, not silenced by
  `GST_DEBUG`; reproduced 2026-09-29 (TCP client, then UDP client). Only TCP-only RTSP
  would avoid it, at the cost of UDP-only clients.
- Encoder backends (ADR 0008, `encoder.rs`): `EncoderBackend::{X264, Va, Vaapi, V4l2, Nvenc}`,
  each segment named `video_enc` and ending in `byte-stream,alignment=au`. `resolve(config)`
  dry-runs (10 frames, 3 s) because element registration lies: the dev box has the VA
  elements and no `/dev/dri`. Adding a backend = enum variant, `elements()`, `segment()`,
  a place in `AUTO_ORDER`, a config name, README table row. Devices: `/dev/dri` (VA),
  `/dev/video11` (Pi 4 V4L2), NVIDIA toolkit (nvenc); the Pi 5 has no H.264 encoder.
