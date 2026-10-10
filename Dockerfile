# syntax=docker/dockerfile:1.7@sha256:a57df69d0ea827fb7266491f2813635de6f17269be881f696fbfdf2d83dda33e

# ----------------------------------------------------------------------
# Stage 1 — build
#
# Debian 13 (trixie) in both stages, so the GStreamer the binary is built
# against is the one it runs with (1.26, as on the development box and in
# the live gates). Bookworm left regular security support on 2026-07-12.
# The build stage carries the dev headers; runtime carries only the .so
# files.
# ----------------------------------------------------------------------
FROM rust:1.99.0-slim-trixie@sha256:24e632c09342c20abf8312cf4f61430a911c01ed3a5e4c02b87292b1c39c5273 AS builder

# System packages required to build the gstreamer-rs crates against
# system GStreamer, plus what arlo-rs's transport needs: `wreq` links
# BoringSSL (`btls-sys`), whose build.rs runs `git init` in its source
# tree, then cmake and bindgen (libclang), and assembles with nasm.
# Base images are pinned by digest (Renovate bumps them); the apt packages
# are not version-pinned, so two builds of one commit can differ by a
# Debian point update. Rebuild on a cadence or pin sources.list to a
# snapshot.debian.org date if byte-identical rebuilds ever matter.
RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential \
        pkg-config \
        git \
        cmake \
        libclang-dev \
        nasm \
        libssl-dev \
        libgstreamer1.0-dev \
        libgstreamer-plugins-base1.0-dev \
        libgstreamer-plugins-bad1.0-dev \
        libgstrtspserver-1.0-dev \
        libglib2.0-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build
ENV CARGO_TERM_COLOR=never

# 1) The dependencies, in a layer of their own: the manifests, the lockfile
#    and the toolchain pin, with empty placeholder sources, built in release
#    mode. That layer (crates downloaded, BoringSSL and GStreamer bindings
#    compiled) only changes with Cargo.lock, so CI restores it from its
#    layer cache and a release compiles our crates only. No cache mount
#    here: a mount is not part of the layer, so a fresh CI runner would
#    start from nothing. `--locked` blocks Cargo.lock churn during deploy.
COPY rust-toolchain.toml Cargo.toml Cargo.lock ./
COPY crates/streamer-app/Cargo.toml         crates/streamer-app/Cargo.toml
COPY crates/streamer-bin/Cargo.toml         crates/streamer-bin/Cargo.toml
COPY crates/streamer-domain/Cargo.toml      crates/streamer-domain/Cargo.toml
COPY crates/streamer-infra-arlo/Cargo.toml  crates/streamer-infra-arlo/Cargo.toml
COPY crates/streamer-infra-media/Cargo.toml crates/streamer-infra-media/Cargo.toml
COPY crates/streamer-infra-ops/Cargo.toml   crates/streamer-infra-ops/Cargo.toml
RUN for crate in crates/*/; do mkdir -p "${crate}src" && touch "${crate}src/lib.rs"; done \
 && echo 'fn main() {}' > crates/streamer-bin/src/main.rs \
 && cargo build --release --locked --package arlo-camera-streamer \
 && rm -rf crates/*/src

# 2) Our sources. Their timestamps may predate the placeholder build above,
#    and cargo decides what to rebuild by timestamp: touch them, or the
#    placeholder binary would ship.
COPY crates/ crates/
RUN find crates -name '*.rs' -exec touch {} + \
 && cargo build --release --locked --package arlo-camera-streamer

# ----------------------------------------------------------------------
# Stage 2 — runtime
#
# Distroless was rejected: GStreamer plugins require a glibc + dynamic
# loader runtime that distroless does not ship in a usable form. We get
# the same isolation by running as a non-root user and stripping the
# binary at build time.
# ----------------------------------------------------------------------
FROM debian:trixie-slim@sha256:a29215f6a35e51e22adffa17f89e9d2ef06214e64a2bad10d765c46aea49f11f AS runtime

# GHCR links the package to the repository through this label; the
# release workflow adds version, revision and dates.
LABEL org.opencontainers.image.source="https://github.com/YpNo/arlo-camera-streamer-rs" \
      org.opencontainers.image.licenses="MIT"

# Runtime libraries: GStreamer base + plugins required by the idle and
# live pipelines (videotestsrc, jpegdec, x264enc, h264parse, h265parse,
# rtspserver), `gstreamer1.0-x` for the pango plugin (the idle caption's
# `textoverlay`: not in plugins-base on Debian; the daemon refuses to
# start without it), the WebRTC transport (`gstreamer1.0-nice` = libnice ICE,
# required by webrtcbin for live streaming). arlo-rs no longer needs a
# browser: its default transport is a Chrome-impersonating HTTP client
# (verified live 2026-09-25). Bring `tini` as PID 1 so signals propagate
# cleanly.
#
# `apt-get upgrade` first: the pinned base only gains Debian's security
# fixes when Docker rebuilds it, every few weeks, and the release gate
# refuses a CRITICAL vulnerability that already has a fixed package
# (perl-base, 2026-10-06: fixed in the security archive, not yet in the
# newest slim image). The digest still pins everything else.
RUN apt-get update && apt-get upgrade -y --no-install-recommends \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        tini \
        gstreamer1.0-plugins-base \
        gstreamer1.0-x \
        gstreamer1.0-plugins-good \
        gstreamer1.0-plugins-bad \
        gstreamer1.0-plugins-ugly \
        gstreamer1.0-libav \
        gstreamer1.0-rtsp \
        gstreamer1.0-nice \
        gstreamer1.0-vaapi \
        mesa-va-drivers \
    # GPU encoding (ADR 0008): the `va` plugin is in plugins-bad, `vaapi`
    # above, the V4L2 encoder in plugins-good, `nvh264enc` in plugins-bad
    # (its libraries come from the NVIDIA container toolkit at run time).
    # The Intel media driver exists on amd64 only.
    && if [ "$(dpkg --print-architecture)" = "amd64" ]; then \
         apt-get install -y --no-install-recommends intel-media-va-driver; \
       fi \
    && rm -rf /var/lib/apt/lists/*

# Non-root user: uid 10001 keeps us out of the typical host uid space.
# The setuid/setgid bits go too (su, passwd, mount, …): the daemon needs
# none of them, and each is a way back to root for a local exploit when
# the container runs without `no-new-privileges`.
# Default mount points (overridable at runtime):
# - /etc/arlo-streamer/streamer.toml — config (read-only)
# - /var/lib/arlo-streamer            — session cache + thumbnails
# - /var/lib/arlo-streamer/hls         — HLS output (when [output.hls] is set)
RUN groupadd --system --gid 10001 streamer \
 && useradd  --system --uid 10001 --gid streamer --create-home --shell /usr/sbin/nologin streamer \
 && find / -xdev -perm /6000 -type f -exec chmod a-s {} + \
 && mkdir -p /etc/arlo-streamer /var/lib/arlo-streamer/hls \
 && chown -R streamer:streamer /var/lib/arlo-streamer

WORKDIR /app
# Last, as it changes with every commit; the mode is set in the copy itself
# (a `RUN chmod` after it stored the 27 MB binary a second time).
COPY --from=builder --chmod=0755 /build/target/release/arlo-camera-streamer /usr/local/bin/arlo-camera-streamer

# Numeric so the runtime can verify the image runs unprivileged without
# resolving the name (Kubernetes `runAsNonRoot`, Docker Scout).
USER 10001:10001

# Default ports:
# - 8554/tcp  RTSP
# - 9090/tcp  /metrics, /healthz, /readyz
# - 9091/tcp  /admin (bearer-token-auth required)
EXPOSE 8554/tcp 9090/tcp 9091/tcp

# Healthcheck: the binary's own `healthcheck` subcommand asks the
# liveness endpoint (read from the config's `metrics_bind`), so the image
# needs neither a shell nor wget/curl.
HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
    CMD ["/usr/local/bin/arlo-camera-streamer", "healthcheck", "--config", "/etc/arlo-streamer/streamer.toml"]

ENV RUST_LOG=info,arlo_camera_streamer=info

# tini forwards SIGTERM; the daemon handles it and drains (detach,
# release every Arlo session) before exiting 0.
ENTRYPOINT ["/usr/bin/tini", "--", "/usr/local/bin/arlo-camera-streamer"]
CMD ["--config", "/etc/arlo-streamer/streamer.toml"]
