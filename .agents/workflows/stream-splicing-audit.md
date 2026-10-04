# Workflow: Stream Splicing Audit
**Role**: Senior Media Quality Engineer

## Objective
Verify that switching between the idle still and the live Arlo feed stays seamless:
RTSP clients never disconnect, the live picture appears within one keyframe interval,
and the camera returns to idle cleanly.

## Triggers
- Changes to `crates/streamer-infra-media/src/{splice,pipeline_desc,gst_pipeline,webrtc_pipeline,rtsp}.rs`.
- Changes to the orchestrator's attach/detach paths.
- An `arlo-rs` update that touches signaling or the event bus.

## Steps

### 1. Pure checks
```bash
cargo test -p streamer-infra-media --all-features
```
Launch-string builders (`pipeline_desc.rs`), splice logic (`splice.rs`), the loss
rules (`live_watch.rs`) and the multiplexer mapping are unit-tested; the recorded-session
integration test replays a captured WebRTC session through the real pipeline when
GStreamer is present. Add `GST_DEBUG=2` to see pipeline warnings.

### 2. Invariants to re-read in the diff
- Both video branches reach `input-selector` as raw I420 at identical caps; one encoder.
- Audio stays on `audiomixer`, F32LE everywhere.
- `set_launch` is never re-bound on a running media.
- `WebrtcLive::start` stays cancel-safe; `shutdown` stays idempotent.
- The wiring slot follows the current media (`media-configure` / `unprepared`).

### 3. Live gate
Follow the `live-validation` skill. At minimum: a motion session end to end, a VLC
reconnect in the middle of a session, and the return to the idle still.

## Success criteria
- [ ] Trigger to live picture under 5 s (`state transition to=Activating` → `first RTP flowing`).
- [ ] Zero RTSP client disconnects across idle → live → idle.
- [ ] No `ERROR` on either pipeline bus; `webrtcbin bus watch exited` at session end.
- [ ] Idle still refreshed after the session (`thumbnail applied to idle overlay`).
