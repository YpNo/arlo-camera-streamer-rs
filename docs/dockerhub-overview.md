<!-- Docker Hub repository overview for docker.io/ypno/arlo-camera-streamer-rs.
     Paste the part below the line into Hub → Repository → Overview (Markdown,
     absolute links only). Short description (100 chars max):
     "A Rust daemon that bridges battery-powered Arlo cameras to a 24/7 NVR via RTSP" -->
---

![arlo-camera-streamer](https://raw.githubusercontent.com/YpNo/arlo-camera-streamer-rs/main/docs/banner.jpeg)

# arlo-camera-streamer

A Rust daemon that bridges battery-powered Arlo cameras to a 24/7 NVR
(Frigate, ZoneMinder, Shinobi, Home Assistant) over RTSP, and optionally HLS,
**without draining the camera battery**.

An NVR expects a continuous stream. Arlo battery cameras sleep by design; a
continuous pull would flatten them in a day. This daemon decouples the two:
the RTSP output is always up and shows a still frame (the camera's last
snapshot) while the camera sleeps, and splices the camera's real video in,
on the same connection, when Arlo reports motion or when you open a live
view in the Arlo app. Your NVR sees one uninterrupted stream per camera.

| Phase | Your NVR sees | The camera |
|---|---|---|
| Idle | Last snapshot with a standby overlay, 1 fps | Sleeping |
| Motion | The live H.264 feed, until the motion stops plus a debounce | Awake |
| Viewed in the Arlo app | The app's own stream, picture and sound | Awake, because of you |
| Daily budget spent | Back to the idle frame | Sleeping, protected |

Source, full documentation, changelog and issues:
**https://github.com/YpNo/arlo-camera-streamer-rs**

## Tags

| Tag | Meaning |
|---|---|
| `v<version>` (e.g. `v0.1.0`) | A release. **Pin this one.** |
| `latest` | The newest release. |
| `sha-<commit>` | The exact commit a release was built from. |

Every tag is also published to GitHub's registry as
`ghcr.io/ypno/arlo-camera-streamer-rs`, byte for byte the same image. Each
digest is scanned with Trivy before it is tagged and carries SLSA provenance
and an SPDX SBOM. Platforms: `linux/amd64` and `linux/arm64` (Raspberry Pi 4
and 5, Apple silicon hosts) in one manifest list; `docker pull` picks yours.

## What is in the image

- The `arlo-camera-streamer` binary, statically configured by one TOML file.
- GStreamer 1.22 with the plugins the pipelines need: base, good, bad, ugly,
  libav, `gst-rtsp-server`, libnice for WebRTC, VA-API drivers for Intel/AMD
  hardware encoding.
- `tini` as PID 1. No shell tools: the health check is the binary's own
  `healthcheck` subcommand.
- Runs as uid/gid `10001`, non-root, with a `HEALTHCHECK` built in.

## Quick start

**1. Configuration.** Copy the example and fill in the `[arlo]` section
(account email, the *name* of the env var holding the password, the second
factor). Secrets never go in the file.

```
https://github.com/YpNo/arlo-camera-streamer-rs/blob/main/config/streamer.example.toml
```

**2. State directory.** The session token and the idle thumbnails live under
`/var/lib/arlo-streamer`. On a bind mount it must belong to the image's user:

```bash
sudo mkdir -p /var/lib/arlo-streamer && sudo chown -R 10001:10001 /var/lib/arlo-streamer
```

**3. First login and device ids.** One interactive run signs in, completes
the second-factor pairing and prints a ready-to-paste `[[cameras]]` block per
camera. Keep the same state volume for the daemon afterwards; `-it` matters
only when the one-time code is typed on stdin.

```bash
docker run --rm -it -e ARLO_PASSWORD -e ARLO_IMAP_PASSWORD \
  -v /etc/arlo-streamer:/etc/arlo-streamer:ro \
  -v /var/lib/arlo-streamer:/var/lib/arlo-streamer \
  ypno/arlo-camera-streamer-rs:v0.1.0 list-devices --config /etc/arlo-streamer/streamer.toml
```

**4. Run the daemon.**

```bash
docker run -d --name arlo-camera-streamer --restart on-failure --stop-timeout 30 \
  -p 8554:8554 \
  -v /etc/arlo-streamer:/etc/arlo-streamer:ro \
  -v /var/lib/arlo-streamer:/var/lib/arlo-streamer \
  -e ARLO_PASSWORD -e ARLO_IMAP_PASSWORD -e STREAMER_ADMIN_TOKEN \
  ypno/arlo-camera-streamer-rs:v0.1.0
```

**5. Watch.** `rtsp://<host>:8554/<stream_name>` in VLC or as a Frigate input.
The idle frame shows at once; walk in front of the camera and the picture
goes live within a few seconds.

### Environment variables

| Variable | Purpose |
|---|---|
| `ARLO_PASSWORD` (or the name set in `arlo.password_env`) | **Required.** Arlo cloud password. |
| `ARLO_IMAP_PASSWORD` (or the name set in `arlo.mfa.password_env`) | IMAP password, for the email second factor read from a mailbox. |
| `STREAMER_ADMIN_TOKEN` | **Required** by the daemon. Bearer token for the `/admin/*` API, 16 bytes or more (`openssl rand -hex 32`). |
| `RUST_LOG` | Optional log filter. The image sets `info,arlo_camera_streamer=info`. |

### Ports

| Port | Purpose |
|---|---|
| `8554/tcp` | RTSP output, one mount per camera. |
| `9090/tcp` | `/metrics` (Prometheus), `/healthz`, `/readyz`. |
| `9091/tcp` | `/admin/*` (bearer token): state, manual wake, force idle. |

The metrics and admin listeners bind `127.0.0.1` *inside the container* by
default, so publishing their ports alone exposes nothing. To reach them from
outside, set `output.metrics_bind = "0.0.0.0:9090"` and
`output.admin_bind = "0.0.0.0:9091"` in the config, and keep the admin port
off any untrusted network.

### Hardware encoding

The daemon re-encodes one H.264 stream per camera. With the default
`video_encoder = "auto"` it probes the host at start and takes the first
backend that works; give the container the device:

| Hardware | Add to `docker run` |
|---|---|
| Intel / AMD GPU | `--device /dev/dri` |
| Raspberry Pi 4 / Zero 2 / CM4 | `--device /dev/video11` |
| NVIDIA | `--gpus all` (NVIDIA container toolkit) |
| none | nothing: x264 in software, about 0.6 CPU core per connected camera |

### HLS output

Set `[output.hls] dir = "/var/lib/arlo-streamer/hls"` and the daemon writes
`<dir>/<stream_name>/index.m3u8` plus segments, served by any static web
server. HLS keeps each camera's encoder running whenever the segmenter is
up.

## Operational notes

- Disable the Arlo app's own motion **recording** for the cameras exposed
  here; Arlo's cloud recording competes with the live stream.
- Start with a `daily_live_budget` of 300 to 600 seconds per camera until you
  have measured the battery impact; `0` means no cap.
- The process exits non-zero when it stops on its own (the Arlo event bus
  ended, or a camera task failed) after releasing every live session: run it
  under `--restart on-failure`.
- Logs go to stderr; `docker logs` shows them. The project README documents
  every `RUST_LOG` target and the `GST_DEBUG` categories for the RTSP server.

## Security

- Secrets come from environment variables only and are never logged;
  presigned URLs, session ids and tokens are redacted by design.
- Thumbnails are fetched over HTTPS only with timeouts and a size cap, and
  written owner-only beside the session cache.
- Report a vulnerability as described in
  https://github.com/YpNo/arlo-camera-streamer-rs/blob/main/SECURITY.md.

## License

MIT. Not affiliated with Arlo Technologies; Arlo is a trademark of its owner.
