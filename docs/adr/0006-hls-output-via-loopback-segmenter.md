# ADR 0006 — HLS output through a loopback segmenter; DASH stays out

- **Status:** Accepted
- **Date:** 2026-09-28
- **Deciders:** Senior architect, project owner
- **Supersedes:** 0002 (RTSP-only output for v1), for HLS
- **Superseded by:** —

## Context

ADR 0002 shipped RTSP only and kept the `[output.hls]` / `[output.dash]`
blocks as accepted-but-ignored config, deferring three questions:
segment retention, playlist sizing, and directory cleanup. The owner
asked for both outputs on 2026-09-28.

Two facts shape the design:

1. **gst-rtsp-server owns the camera's pipeline.** The persistent
   idle/live splice (ADR 0003) lives inside the RTSP media, which the
   server builds when the first client connects and unprepares after
   the last one leaves. There is no always-running pipeline to hang a
   `hlssink2` branch on.
2. **GStreamer's DASH support is not usable on stock distributions.**
   `dashsink` (plugins-bad) can mux fragments with `mpegtsmux` only: its
   `mp4` muxer is documented as "deprecated, non-functional" and
   `dashmp4` needs `dashmp4mux` from gst-plugins-rs, which Debian and
   Ubuntu do not package. Browser DASH players (dash.js) reject MPEG-TS
   fragments, and `dashsink` has no retention setting, so fragments
   accumulate forever.

## Decision

**HLS is produced by a per-camera segmenter that is an RTSP client of
the camera's own mount**, over loopback:

```text
rtspsrc rtsp://127.0.0.1:<port>/<stream>
  ├─ rtph264depay ! h264parse ─┐
  └─ rtpmp4adepay ! aacparse ──┴→ hlssink2 (segments + index.m3u8)
```

- **No re-encoding.** The segmenter repackages the RTSP output's H.264
  and AAC. Segments cut at the encoder's keyframes (every 2 s).
- **One splice for every output.** HLS shows exactly what RTSP clients
  see, idle still and live alike; nothing is duplicated.
- **Retention**: `hlssink2 max-files = playlist_length + 2`, so a player
  still fetching the oldest listed segment never gets a 404.
  `segment_secs` is raised to 1 and `playlist_length` to 3 (RFC 8216
  §6.3.3) when configured lower.
- **Directory**: files go to `<dir>/<stream_name>/`. The directory is
  created at registration; our playlist and `segment-NNNNN.ts` files are
  removed when the segmenter starts and when it stops, so a player never
  sees a frozen playlist from an earlier run. Other files are untouched.
  A directory that cannot be created fails the camera's registration at
  boot.
- **Supervision**: the segmenter runs on its own thread (a `GLib` bus
  loop, off the tokio runtime). An error or end-of-stream rebuilds it
  after a backoff that doubles from 1 s to 30 s and resets after a
  healthy minute.
- **Serving** stays outside the daemon: point a web server (nginx,
  Caddy, go2rtc) at the directory. The externally exposed surface is
  unchanged: RTSP plus the ops and admin sockets.

**DASH stays unsupported.** `[output.dash]` still parses, so existing
configs load; the daemon warns that it is ignored and why.

## Consequences

### Positive

- Browser-direct viewing through any static file server.
- The idle/live splice, keyframe handling and live-loss paths are
  reused as they are; HLS needs no new media logic.
- With HLS on, the RTSP media is always built, so a live session is
  always spliced immediately, even before a real client connects.

### Negative

- **CPU**: with HLS on, the camera's encoder runs continuously (the
  segmenter keeps the media alive), where RTSP alone only encodes while
  someone watches.
- One loopback RTSP connection per camera, and one more thread.
- HLS latency is roughly two to three segments behind RTSP, as with any
  HLS.

## Alternatives considered

- **A `tee` inside the RTSP media.** Rejected: the media exists only
  while a client is connected, so HLS would stop whenever nobody
  watches over RTSP.
- **A second, always-on copy of the splice pipeline for HLS.** Rejected:
  two encoders per camera, and every splice change made twice.
- **DASH with MPEG-TS fragments and our own pruning.** Rejected: the
  result would not play in the browsers DASH is for.
- **DASH through gst-plugins-rs.** Possible once the distributions
  package it, or with a self-built plugin in the image; re-evaluate
  then.

## Re-evaluation triggers

- Debian and Ubuntu ship `dashmp4mux` / `cmafmux` (gst-plugins-rs), or
  the image builds them.
- The continuous encode cost matters on the target box.
