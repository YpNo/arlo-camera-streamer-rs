# ADR 0008 — Choosing the H.264 encoder per host

- **Status:** Accepted
- **Date:** 2026-10-03
- **Deciders:** Senior architect, project owner
- **Supersedes:** —
- **Superseded by:** —

## Context

The unified pipeline (ADR 0003) runs one H.264 encoder per connected
camera, all the time, so the encoder sets the per-camera CPU cost: about
0.6 core with software `x264enc` at 720p15 (README, Performance). The
daemon is meant for small always-on boxes, and they differ:

| Host | Hardware H.264 encoder | GStreamer element |
|---|---|---|
| Intel / AMD mini PC or barebone | yes (QuickSync / VCN) | `vah264enc` (`va` plugin, GStreamer ≥ 1.22), older `vaapih264enc` |
| NVIDIA GPU | yes (NVENC) | `nvh264enc` |
| Raspberry Pi 4, Zero 2 W, CM4 | yes (V4L2 M2M, `/dev/video11`) | `v4l2h264enc` |
| Raspberry Pi 5 | **no** (HEVC decoder only) | — |
| Anything else | — | `x264enc` |

Until now the configuration offered `x264` or `vaapi`, defaulting to
software, and a wrong choice surfaced as a broken media at the first
client connection. A Google Coral USB accelerator, often found on the
same box for Frigate's object detection, encodes no video and plays no
part here.

## Decision

`output.video_encoder` accepts `auto` (new default), `x264`, `va`,
`vaapi`, `v4l2` and `nvenc`. The domain enum records the owner's choice;
the media crate's `encoder::resolve` turns it into a concrete backend at
boot, after `gstreamer::init()`:

- **`auto`** tries, in order, `nvenc`, `va`, `vaapi`, `v4l2`, `x264`,
  and takes the first whose elements are registered **and** whose dry
  run succeeds: ten test frames encoded through the backend's segment,
  three seconds at most. Registration alone is not enough — a VAAPI
  plugin without a render node loads fine and fails only when a pipeline
  starts, which is exactly what the dev box without `/dev/dri` shows.
- **An explicit name** is dry-run too and fails the boot when it does
  not work, with the reason, rather than failing at the first client.
- Every backend's segment names its encoder `video_enc`, so the splice's
  force-key-unit keeps working, and ends in the same
  `byte-stream,alignment=au` caps, so the downstream `h264parse !
  rtph264pay` is unchanged. The chosen backend is logged at `info`
  (`video encoder (auto) encoder=…`).

Deployment: the Docker image carries the VA plugin and drivers (Intel
driver on amd64 only); the host passes the device it has (`/dev/dri`
for VA, `/dev/video11` for V4L2, the NVIDIA container toolkit for
NVENC). The Raspberry Pi 5 ends up on x264 by design.

## Consequences

### Positive

- One configuration works on every supported box; a GPU is used when
  present and the daemon says which encoder it runs.
- A misconfigured host fails at boot with a message, not at 3 a.m. when
  Frigate reconnects.

### Negative / risks

- The dry run adds up to a few seconds to the boot on hosts where
  hardware plugins are installed but unusable (each skipped backend
  costs its timeout).
- The hardware segments are written from the elements' documented
  properties; only `x264` and `vaapi` have run against a real camera so
  far. The handoff records which backends were seen working live.
- `auto` can pick a backend the owner did not expect (an NVIDIA card in
  a box meant to use its iGPU); the explicit names exist for that.

## Alternatives considered

- **Keep `x264` as the default and document the GPU names.** Rejected:
  the people this daemon targets run it on whatever box they have, and
  the software default silently wastes the GPU they paid for.
- **Element registration as the only check.** Rejected by the dev box:
  the VA elements are registered there and no render node exists.
- **Intel QSV (`qsvh264enc`) beside VA.** Not added: one Intel path
  (`va`, with `vaapi` for older stacks) is enough, and QSV needs the
  oneVPL runtime in the image.
