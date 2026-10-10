# ADR 0010 — The idle picture is drawn once a second, not every frame

- **Status:** Accepted
- **Date:** 2026-10-11
- **Deciders:** Senior architect, project owner (red-teamed before code)
- **Supersedes:** —
- **Superseded by:** —

## Context

Every camera's persistent pipeline (ADR 0003) has an idle branch that
drew the STANDBY screen 15 times a second:
`videotestsrc pattern=black ! textoverlay (caption) ! gdkpixbufoverlay
(snapshot, scaled to fill) ! videoconvert`. Frigate keeps one RTSP client
on each camera around the clock, so that branch runs all the time.

Measured on the reference i5-6500T (GStreamer 1.26, one camera, 20 s at
15 fps): **37–56 % of a core** (the spread is the machine's other load),
blending the full-frame snapshot into every frame being most of it; x264
adds about 8 % on that nearly static picture. Three idle cameras with
Frigate attached cost about 2 cores, on a GPU-encoding box too. The
picture changes when a snapshot arrives or the caption changes, a few
times an hour.

The red-team of a first proposal also found that the snapshot, drawn
after the caption and scaled to the whole frame, **covered the caption**:
once a snapshot existed, neither STANDBY nor LIVE IN ARLO APP showed.

## Decision

- **Draw once a second, repeat to 15.** The idle source runs at
  `framerate=1/1` (`IDLE_FPS`), and `imagefreeze is-live=true
  allow-replace=true` sits before the branch's final unified caps
  (I420, 1280×720, 15/1): it repeats each drawing, live-timestamped, and
  takes the next one when it comes. The selector and the encoder see
  15 fps on both sides, as before; latency stays that of the branch
  (`imagefreeze` answers the latency query itself).
- **Snapshot under the caption:** `gdkpixbufoverlay` before
  `textoverlay`.
- **Nothing else moves:** the media hook, `refresh_thumbnail`,
  `set_idle_caption`, the wiring, the live branch, the splice and its
  force-key-unit, audio, HLS.
- **The `JpegStill` idle kind goes:** nothing ever passed it a JPEG
  (dead since the snapshot overlay), and a media built with it would
  never have prerolled.
- `imagefreeze` joins the boot element check; `jpegdec` leaves it.

## Consequences

### Positive

- The idle branch costs **3 %** of a core instead of 37–56 %; with x264,
  14 % instead of 45 % per connected camera on the reference box.
- The caption shows over the snapshot.
- A new snapshot shows within one drawing (measured 133 ms after the
  overlay's `location` changes); a new caption within a second at most.

### Negative / risks

- A caption change can take up to a second to show (it used to be one
  frame).
- Relies on `imagefreeze`'s live mode and `allow-replace`, in
  gst-plugins-good since 1.18.

## Alternatives considered

- **Compose the frame once per change** in a one-shot pipeline and feed
  it through `appsrc ! imagefreeze` (the first proposal): 0.6 % instead
  of 3 %, but a new subsystem: a frame cache, pushes from the media hook,
  a media that cannot preroll without one, races between composing and a
  rebuilt media, compose failures needing a fallback. The red-team
  showed the smaller change gets about 93 % of the saving with none of
  that.
- **Lower the idle frame rate without `imagefreeze`**: the selector and
  the encoder would see 1 fps on one side and 15 on the other.
- **Compose in Rust** (decode, scale, draw text): a JPEG decoder and a
  font rasterizer as new dependencies.
