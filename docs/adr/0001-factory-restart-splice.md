# ADR 0001 — Factory-restart splice (Idle → Live transition)

- **Status:** Accepted
- **Date:** 2026-05-09
- **Deciders:** Senior architect, project owner
- **Supersedes:** —
- **Superseded by:** —

## Context

When a camera transitions from `Idle` to `Live`, the embedded RTSP
server has to swap the source pipeline that the
`gst-rtsp-server::RTSPMediaFactory` references — from a synthetic /
JPEG-still source to the real H.264/H.265 stream coming back from
Arlo. There are two well-known strategies in GStreamer for this:

1. **Factory restart.** Tear the factory down, build a new one with
   the live launch string, and re-mount it on the same path. Existing
   clients (Frigate's go2rtc, in our case) get an RTSP `TEARDOWN` and
   reconnect within ~1 second.
2. **Seamless splice via `input-selector`.** Keep one persistent
   pipeline that contains both branches behind an `input-selector`,
   wait for the live branch to emit an IDR frame, then atomically
   switch the pad. No client reconnect.

Strategy 2 is the architecturally "correct" answer and was the
original target. We dropped it in v1 for the reasons below.

## Decision

We use **factory-restart** for the v1 splice.

The orchestrator side-effect on `Idle → Activating → Live`:

```
clear_live_source(camera) → install_factory(idle launch)         (idle path)
set_live_source(camera, source) → install_factory(live launch)   (live path)
```

`gst-rtsp-server` handles the client teardown + remount transparently;
clients reconnect on their own RTSP retry loop.

## Consequences

### Positive

- **Massively simpler to reason about.** The factory's launch string
  is a single immutable description per state — there is no shared
  state, no IDR-detection on the live appsrc, no race between the
  selector switch and the encoder's first keyframe.
- **Robust under upstream errors.** If the live source dies, the
  factory is rebuilt with the idle string and clients reconnect
  cleanly. The seamless-splice variant has to handle "live branch
  dies mid-stream → switch back to idle without dropping the client",
  which is an entire additional state machine.
- **No GStreamer pad-blocking gymnastics.** Pad blocking the live
  side until an IDR is available is the source of most "I tested it
  and the first 200 ms is grey frames" forum posts.
- **Predictable on every camera.** H.264 / H.265 / variable GOP / B-
  frame configurations all behave the same way under factory-restart.
  `input-selector` is GOP-sensitive.

### Negative

- **~1 s reconnect gap visible to clients.** Frigate's go2rtc
  reconnects within its default retry window, so the practical effect
  is "Frigate sees a brief feed interruption every time the camera
  wakes up". Detection misses the first ~1 s of the live segment;
  recording sees a small gap.
- **Per-client state lost.** RTSP session id, TCP timestamp baselines,
  RTP sequence continuity — all reset. A naive client that doesn't
  reconnect will hang.
- **Not invisible to monitoring.** Frigate logs the reconnect at INFO,
  which can drown legitimate signals if a camera is flapping.

## Alternatives considered

### Seamless `input-selector` splice

Rejected for v1. Re-evaluate if production telemetry shows the
reconnect gap is causing real detection misses (the ADR can be
flipped to "Superseded" with a follow-up implementation). The seam
already exists in the multiplexer adapter (`PipelineRegistry` trait
+ keyframe-watch scaffolding in `splice.rs`), so the migration is
local to `streamer-infra-media`.

### MediaMTX sidecar

Rejected at the architecture level (see ADR 0002 by implication).
Adding a process boundary for splicing buys nothing the embedded
GStreamer pipeline can't do, at the cost of a second daemon to
deploy, monitor, and restart.

## Telemetry to revisit

- `streamer_splice_attempts_total{outcome="success"}` rate.
- `streamer_splice_latency_ms` p95 — should remain comfortably below
  the cooldown debounce.
- Frigate's own "stream reconnect" counter (or the ffmpeg `_metrics`
  plumbing equivalent).

If p95 splice latency exceeds 2 s or Frigate reconnect noise becomes
operationally painful, revisit and implement the seamless variant.
