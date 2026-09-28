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
- `arlo-rs` 0.2.1 comes from crates.io. Local override: uncomment
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
P="--workspace"   # the four-crate subset is no longer needed
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
- Since 2026-09-27 the `rust-build` container has the GStreamer 1.26 dev
  and runtime packages (Debian trixie), so the **whole workspace builds and
  tests there** (`cargo test --workspace --all-features` inside the
  container). The host still has none. Pipeline behaviour needs a camera.
- Root disk is small; `target/` directories of the sibling projects grow
  to tens of GB. Clean them before an image build.
- Coverage gate: 86 % via tarpaulin (measured 88.31 % on 2026-09-28), GStreamer-bound
  files and the binary excluded (`ci.yml`). Raise deliberately.
- Battery rule: every `Live` exit pairs `detach_live` with
  `WebrtcSignaler::teardown`.
- One stream per camera (ADR 0005): while the user watches in the Arlo app,
  never attach; Arlo 14001 maps to `DomainError::CameraBusy` (no backoff).
- Never poll arlo-rs `get_stream_url`: on an idle camera it wakes it.
- The orchestrator's clock is `tokio::time::Instant` (virtual under
  `start_paused` tests); never reintroduce `std::time::Instant::now()` there.

## Dependencies
Checked against crates.io on 2026-09-27: all within one minor of latest
(`mockall` 0.15 and `rstest` 0.27 pending, dev-only). `cargo audit` and
`cargo deny` clean.
