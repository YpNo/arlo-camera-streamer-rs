---
name: media-specialist
description: GStreamer pipeline, webrtcbin, and gst-rtsp-server patterns for the camera streamer.
---
# Media Specialist Skill

## webrtcbin against a non-bundled foreign gateway (e.g. Arlo FreeSWITCH)

1. **`bundle-policy=none`** on webrtcbin — webrtc-rs cannot negotiate non-bundled SDP; only webrtcbin can.
2. **Audio m-line is sendrecv, video is recvonly**. Link an audio-silence chain into `sink_%u` (creates m0 sendrecv); use `add-transceiver` **only** for the recvonly video (m1). Adding audio via `add-transceiver` too produces a duplicate m-line.
3. **Drain unwanted src pads to `fakesink`.** Leaving the audio recv pad unlinked fails with `GST_FLOW_NOT_LINKED` → "Internal data stream error" on `nicesrc`.
4. **Percent-encode TURN userinfo.** Arlo TURN credentials contain `:`, `/`, `=`. Pass raw and `add-turn-server` returns `accepted=false`.
5. **PLI / force-key-unit direction.** Send an upstream `GstForceKeyUnit` via `send_event` on the webrtcbin **src** pad (peer of the appsink sink). `push_event` on that pad, or `send_event` on the *sink* pad, both warn "wrong direction" and never reach rtpbin.

## gst-rtsp-server seamless splice

- One persistent factory per camera. `set_shared(true) + set_suspend_mode(None)` so the media survives client disconnects.
- The `media-configure` signal is the only place to capture named elements (`appsrc`, `input-selector`, encoder) — they don't exist before construction. Store handles behind an `Arc<StdMutex<Option<Wiring>>>` shared between the GLib thread and tokio.
- **Never re-bind `set_launch`** for a running client — it sends EOS to existing clients (Frigate reconnects; VLC just freezes on the old stream).

## Unified-encoder splice pattern (Phase 7 — video)

- Naive H.264-in / H.264-out through the selector makes VLC render the post-splice branch as **black with no text** because its decoder keeps the pre-splice SPS/PPS.
- Fix: both branches produce **raw I420 at identical caps** into the `input-selector`; **one** `x264enc name=video_enc` downstream. Clients see one continuous SPS/PPS.
- Live branch decodes via `avdec_h264 ! videoconvert ! videoscale ! videorate` to normalize to the unified caps.
- Dispatch a downstream `CustomDownstream("GstForceKeyUnit", all-headers=true)` to the encoder on every flip so the next encoded frame is a clean IDR.
- Attach probe waits for **first raw buffer** at `sel.sink_1` (no IDR concept on raw video). Detach flips synchronously — every idle raw frame is complete.

## Opus audio bridging — deferred (Phase 8b)

- Do **not** put a decode chain (`rtpopusdepay ! opusdec` or `rtph264depay ! avdec_h264`) inside the *persistent factory pipeline* fed by an initially-empty `appsrc`. The decoder can't determine its src caps until the first buffer, so `PAUSED` preroll blocks, gst-rtsp-server times out, and the shared media is rebuilt in a loop. Both idle and live become unplayable.
- The correct design: **decode on the webrtcbin side** and push **raw samples** (not RTP) through `LiveRtpSink`. The persistent pipeline's `appsrc` then declares known raw caps (`I420` for video, `S16LE` for audio) at construction time — no decoder-waits-for-data preroll issue.
- Until Phase 8b lands: audio pad on webrtcbin drains to `fakesink`; the persistent pipeline's audio side is silent-AAC linear (`audiotestsrc silence → avenc_aac → rtpmp4apay pay1`) — no `input-selector` on audio.

## Pipeline description hygiene

- Keep launch strings as pure builders in `pipeline_desc.rs`. Unit-testable, greppable, no GStreamer runtime in tests.
- **Pin every branch's final caps identically.** Splitting `format` and `dims/rate` across two capsfilters breaks selector symmetry.
- Constants for the splice contract (`UNIFIED_FPS`, `UNIFIED_GOP`, `UNIFIED_ENCODER_NAME`, `LIVE_RTP_H264_PT`) live next to the builder and are referenced by name from the registry.

## Coverage exclusion

`gst_pipeline.rs`, `rtsp.rs`, `webrtc_pipeline.rs` require live GStreamer and are excluded from unit coverage. Every change here needs a manual live-gate run — mechanical tests will not catch behavior regressions.
