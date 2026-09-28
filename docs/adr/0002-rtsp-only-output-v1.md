# ADR 0002 — RTSP-only output for v1

- **Status:** Superseded by 0006 (HLS)
- **Date:** 2026-05-09
- **Deciders:** Senior architect, project owner
- **Supersedes:** —
- **Superseded by:** 0006 for HLS (DASH remains out, for the reasons
  recorded there)

## Context

The original architecture brief listed three output protocols:

- **RTSP** — primary path consumed by Frigate / go2rtc / VLC.
- **HLS** — secondary, useful for browser clients and remote viewing.
- **DASH** — secondary, redundant in practice with HLS for our use case.

The `OutputConfig` struct in `streamer-domain` already declares
optional `[output.hls]` and `[output.dash]` blocks, and the
GStreamer pipeline-description builder in
`streamer-infra-media/src/pipeline_desc.rs` already emits HLS/DASH
branch strings from `build_output_branches`. The wiring into the
embedded RTSP server, however, is intentionally **not** finished in
v1.

## Decision

**v1 ships RTSP only.** HLS and DASH config blocks are accepted by
the deserializer but the media adapter logs a warning and ignores
them — Frigate doesn't need them, and the time spent making the
secondary sinks production-worthy is better spent hardening the
primary path.

The pipeline-description builder retains the HLS/DASH branch shapes
so the eventual wire-up is one editing pass on `gst_pipeline.rs` and
`multiplexer.rs`, not a redesign.

## Consequences

### Positive

- **Smaller v1 surface area.** Fewer GStreamer plugins to depend on,
  fewer codepaths to test, fewer config combinations to support.
- **Nothing to break.** HLS playlists and DASH manifests have their
  own retention / segment-rotation semantics that interact with
  Frigate's recording and the bridge's idle/live splicing in
  non-trivial ways. Excluding them removes an entire failure mode.
- **Deployment simplification.** A single TCP port (`8554`) plus the
  ops + admin sockets is the entire externally-facing surface.

### Negative

- **No browser-direct viewing.** Operators who want to peek at a
  camera from a browser need to put go2rtc / nginx-rtmp / a VLC
  bridge in front of the daemon.
- **Config schema accepts blocks it ignores.** `[output.hls]` and
  `[output.dash]` parse successfully but the runtime emits a warning
  log line. This is a deliberate compatibility hedge for v2 — old
  configs continue to load.

## Alternatives considered

### Ship HLS in v1

Rejected. HLS via GStreamer's `hlssink2` works, but segment retention,
playlist sizing, and the directory-cleanup-on-shutdown semantics need
their own design pass. Doing it badly is worse than not doing it.

### Drop the config blocks entirely

Rejected. Removing the blocks now and re-adding them in v2 would be
a breaking change for anyone who copy-pasted the example file
ahead of time.

## Re-evaluation triggers

- A user asks for browser-direct viewing without a sidecar.
- Frigate gains direct HLS ingestion that's measurably better than
  its current RTSP path.
- The HLS or DASH branch in the pipeline-description builder ever
  needs to change for an unrelated reason — at that point we wire it
  up properly rather than leave it half-baked.
