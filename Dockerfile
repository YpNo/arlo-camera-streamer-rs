# syntax=docker/dockerfile:1.7

# ----------------------------------------------------------------------
# Stage 1 — build
#
# Pinned to debian:bookworm-slim so the GStreamer ABI matches the runtime
# stage exactly (debian:bookworm-slim ships GStreamer 1.22). The build
# stage carries the dev headers; runtime carries only the .so files.
# ----------------------------------------------------------------------
FROM rust:1.95-slim-bookworm AS builder

# System packages required to build the gstreamer-rs crates against
# system GStreamer. Pinned via debian's own version selection — apt is
# deterministic per snapshot.
RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential \
        pkg-config \
        libssl-dev \
        libgstreamer1.0-dev \
        libgstreamer-plugins-base1.0-dev \
        libgstrtspserver-1.0-dev \
        libglib2.0-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build

# 1) Copy the workspace manifests first to maximize layer cache hits on
#    iterative source-only changes.
COPY Cargo.toml Cargo.lock ./
COPY crates/streamer-app/Cargo.toml         crates/streamer-app/Cargo.toml
COPY crates/streamer-bin/Cargo.toml         crates/streamer-bin/Cargo.toml
COPY crates/streamer-domain/Cargo.toml      crates/streamer-domain/Cargo.toml
COPY crates/streamer-infra-arlo/Cargo.toml  crates/streamer-infra-arlo/Cargo.toml
COPY crates/streamer-infra-media/Cargo.toml crates/streamer-infra-media/Cargo.toml
COPY crates/streamer-infra-ops/Cargo.toml   crates/streamer-infra-ops/Cargo.toml
COPY rust-toolchain.toml clippy.toml deny.toml rustfmt.toml ./

# 2) Copy the actual source.
COPY crates/ crates/

# 3) Build the release binary.
#    `--locked` blocks accidental Cargo.lock churn during deploy.
ENV CARGO_TERM_COLOR=never
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/build/target \
    cargo build --release --locked --package arlo-camera-streamer \
 && cp /build/target/release/arlo-camera-streamer /tmp/arlo-camera-streamer

# ----------------------------------------------------------------------
# Stage 2 — runtime
#
# Distroless was rejected: GStreamer plugins require a glibc + dynamic
# loader runtime that distroless does not ship in a usable form. We get
# the same isolation by running as a non-root user and stripping the
# binary at build time.
# ----------------------------------------------------------------------
FROM debian:bookworm-slim AS runtime

# Runtime libraries: GStreamer base + plugins required by the idle and
# live pipelines (videotestsrc, jpegdec, x264enc, h264parse, h265parse,
# rtspserver). Bring `tini` as PID 1 so signals propagate cleanly.
RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates \
        tini \
        gstreamer1.0-plugins-base \
        gstreamer1.0-plugins-good \
        gstreamer1.0-plugins-bad \
        gstreamer1.0-plugins-ugly \
        gstreamer1.0-libav \
        gstreamer1.0-rtsp \
        gstreamer1.0-tools \
    && rm -rf /var/lib/apt/lists/*

# Non-root user: uid 10001 keeps us out of the typical host uid space.
RUN groupadd --system --gid 10001 streamer \
 && useradd  --system --uid 10001 --gid streamer --create-home --shell /usr/sbin/nologin streamer

WORKDIR /app
COPY --from=builder /tmp/arlo-camera-streamer /usr/local/bin/arlo-camera-streamer
RUN chmod +x /usr/local/bin/arlo-camera-streamer

# Default mount points (overridable at runtime):
# - /etc/arlo-streamer/streamer.toml — config (read-only)
# - /var/lib/arlo-streamer            — session cache + thumbnails
# - /var/lib/arlo-streamer/hls,/dash  — segment output (when enabled)
RUN mkdir -p /etc/arlo-streamer /var/lib/arlo-streamer/hls /var/lib/arlo-streamer/dash \
 && chown -R streamer:streamer /var/lib/arlo-streamer

USER streamer

# Default ports:
# - 8554/tcp  RTSP
# - 9090/tcp  /metrics, /healthz, /readyz
# - 9091/tcp  /admin (bearer-token-auth required)
EXPOSE 8554/tcp 9090/tcp 9091/tcp

# Healthcheck talks to the public liveness endpoint with a 5s budget.
HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
    CMD ["sh", "-c", "wget -qO- http://127.0.0.1:9090/healthz | grep -q ok || exit 1"]

ENV RUST_LOG=info,arlo_camera_streamer=info

# tini handles SIGTERM correctly; the daemon's tokio runtime then drains.
ENTRYPOINT ["/usr/bin/tini", "--", "/usr/local/bin/arlo-camera-streamer"]
CMD ["--config", "/etc/arlo-streamer/streamer.toml"]
