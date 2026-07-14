# ADR 0003 — Seamless input-selector splice (Idle → Live transition)

- **Status:** Accepted
- **Date:** 2026-07-12
- **Deciders:** Senior architect, project owner
- **Supersedes:** [ADR 0001](./0001-factory-restart-splice.md)
- **Superseded by:** —

## Context

[ADR 0001](./0001-factory-restart-splice.md) chose **factory-restart**
for the v1 Idle → Live splice, accepting a ~1 s client reconnect, and
explicitly flagged the seamless `input-selector` variant for
re-evaluation "if production telemetry shows the reconnect gap is
causing real detection misses."

In practice the reconnect gap was worse than anticipated:

- **VLC (and other plain RTSP clients) do not silently reconnect** the
  way Frigate's go2rtc does — a factory rebind drops them entirely.
  Direct-viewing a camera was therefore broken.
- Every wake-up lost the first ~1 s of the live segment and reset RTP
  continuity.

The seamless variant was implemented (Phases 6–8b). Crucially, the
implementation is **not** the naive "switch between two *encoded*
branches" that ADR 0001 rejected as GOP-sensitive. Instead:

- Both branches are normalized to **raw I420** and fed to a single
  `input-selector`, whose output drives **one shared `x264enc`**
  (the "unified-encoder" design). Clients see one continuous H.264
  stream with stable SPS/PPS across the splice — no decoder re-init,
  no GOP-boundary sensitivity.
- Audio is mixed (silent bed + live Opus) through an `audiomixer`
  rather than a second selector, which would stall gst-rtsp-server's
  media prepare.

## Decision

Use the **seamless `input-selector` splice** as the production splice
strategy. One persistent `RTSPMediaFactory` per camera
(`combined_launch_string`, `suspend-mode=NONE`) hosting:

- an **idle** branch (synthetic STANDBY frame + last-snapshot overlay)
  producing raw I420 → `sel.sink_0`;
- a **live** branch — an `appsrc` fed inbound H.264 RTP by the
  per-camera `webrtcbin` leg, decoded and normalized to raw I420 →
  `sel.sink_1`;
- `input-selector name=sel` → shared `x264enc` → `rtph264pay pay0`;
- an audio path: silent bed + a live-Opus `appsrc` (decoded) mixed by
  `audiomixer` → shared `avenc_aac` → `rtpmp4apay pay1`.

On the first decoded live buffer a pad probe on `sel.sink_1` flips
`active-pad` and fires a force-key-unit at the encoder (fresh IDR right
after the content switch). Detach flips back to `sink_0` with another
force-key-unit. Connected clients never disconnect.

The legacy factory-restart builders (`idle_launch_string` /
`live_launch_string`) remain in `pipeline_desc.rs` but are no longer on
the production path.

## Consequences

### Positive

- **No client reconnect.** VLC *and* Frigate stay connected across
  Idle↔Live; RTSP session, RTP continuity, and TCP baselines survive.
- **Stable codec parameters.** One downstream encoder ⇒ one SPS/PPS,
  so no client-side decoder re-initialization at the splice — the
  concern that made a *plain* `input-selector` risky is neutralized by
  operating on raw frames with a unified encoder.
- **Enables features that need a persistent pipeline** — the idle
  thumbnail overlay (Phase 5) and live audio bridging (Phase 8b) both
  ride on the always-running factory.

### Negative / costs

- **The complexity ADR 0001 warned about is now ours.** Pad-blocking,
  IDR alignment (force-key-unit), the "live branch dies → revert to
  idle without dropping the client" path, and the WebRTC `appsrc`
  pump lifecycle all live in `streamer-infra-media`.
- **Always-on encoding.** The shared `x264enc` runs whenever a client
  is connected, so idle costs nearly as much as live (~0.6 core/camera
  on an i5-6500T). CPU scales with *connected* cameras — see the
  README "Performance" section; hardware encode (VAAPI) is the planned
  mitigation.
- **Version-sensitive prepare.** The persistent pipeline exercises
  gst-rtsp-server media-prepare paths that differ across GStreamer
  versions and are hard to validate off-target (a second audio
  `input-selector` stalled prepare; `audiomixer` was required).

## Alternatives considered

- **Keep factory-restart (ADR 0001).** Rejected: breaks VLC and loses
  the first ~1 s every wake-up.
- **Switch between two *encoded* branches with `input-selector`.**
  Rejected: GOP/SPS-PPS-sensitive; the raw + unified-encoder design
  avoids it.
- **`audiomixer` vs a second `input-selector` for audio.** The second
  selector stalled gst-rtsp-server prepare (empty inactive live pad →
  `pay1` caps never resolve); `audiomixer` always produces from the
  silent bed and was adopted.
