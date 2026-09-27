# CONTEXT — arlo-camera-streamer-rs

Profile: `service` (a daemon that runs unattended next to an NVR). Full
rigor: boundaries, failure-path tests, ADRs, CI.

## Stack
- Rust 1.98.1 (pinned in `rust-toolchain.toml` and `mise.toml`; follows
  `arlo-rs` 0.2.0's MSRV), edition 2024, tokio, GStreamer 1.22+ via
  gstreamer-rs 0.25, axum 0.8, prometheus 0.14.
- Six-crate workspace, hexagonal: `streamer-domain` (ports + types),
  `streamer-app` (orchestrator), `streamer-infra-arlo`,
  `streamer-infra-media` (GStreamer), `streamer-infra-ops`, `streamer-bin`.
- `arlo-rs` comes from crates.io. Local override: uncomment
  `[patch.crates-io]` at the end of `Cargo.toml` (never commit it on).

## Verified commands (2026-09-27)
On this workstation every cargo command runs inside the `rust-build`
distrobox (BoringSSL under `wreq` needs libclang there):

```bash
# one-shot form (mise exec selects the pinned toolchain inside the container)
distrobox enter rust-build -- bash -lc 'cd ~/workspace/arlo-camera-streamer/arlo-camera-streamer-rs && LIBCLANG_PATH=/usr/lib/llvm-19/lib mise exec -- cargo test -p streamer-domain -p streamer-app -p streamer-infra-arlo -p streamer-infra-ops --all-features'

# interactive form
distrobox enter rust-build
export LIBCLANG_PATH=/usr/lib/llvm-19/lib
P="-p streamer-domain -p streamer-app -p streamer-infra-arlo -p streamer-infra-ops"
alias cargo='mise exec -- cargo'
```

| Purpose | Command |
|---|---|
| format | `cargo fmt --all` (works on the host too) |
| lint | `cargo clippy $P --all-targets --all-features -- -D warnings` |
| test (focused) | `cargo test -p <crate> --all-features <filter>` |
| test (local full) | `cargo test $P --all-features` |
| docs | `RUSTDOCFLAGS="-D warnings" cargo doc $P --no-deps --all-features` |
| security | `cargo audit && cargo deny check` (host is fine) |
| image | `podman build -t arlo-camera-streamer:local .` (needs ~10 GB free) |
| release binary (Linux box with GStreamer dev headers) | `cargo build --release --locked --package arlo-camera-streamer` → `target/release/arlo-camera-streamer` |

## Runtime constraints
- `streamer-infra-media` and `streamer-bin` **do not build here**: neither
  the host nor the container has GStreamer dev headers. CI
  (`ubuntu-latest`) and the Frigate box are the only executors. Keep
  GStreamer wiring thin and the decision logic in pure modules.
- Root disk is small; `target/` directories of the sibling projects grow
  to tens of GB. Clean them before an image build.
- Coverage gate: 85 % via tarpaulin, media crate excluded (`ci.yml`).
- Battery rule: every `Live` exit pairs `detach_live` with
  `WebrtcSignaler::teardown`.

## Dependencies
Checked against crates.io on 2026-09-27: all within one minor of latest
(`mockall` 0.15 and `rstest` 0.27 pending, dev-only). `cargo audit` and
`cargo deny` clean.
