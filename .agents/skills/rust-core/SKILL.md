---
name: rust-core
description: Rust workspace conventions for arlo-camera-streamer-rs — build environment, hexagonal crate boundaries, error handling, async time, testing and the quality gates CI enforces. Use for any Rust change, dependency bump or new module.
---
# Rust Core Skill

## Build environment

Every cargo command runs in the `rust-build` distrobox (libclang for BoringSSL,
GStreamer 1.26 dev headers) through mise for the pinned toolchain (1.98.1):

```bash
distrobox enter rust-build -- bash -lc 'cd ~/workspace/arlo-camera-streamer/arlo-camera-streamer-rs && LIBCLANG_PATH=/usr/lib/llvm-19/lib mise exec -- cargo test --workspace --all-features'
```

The first media-crate build is slow; run long builds in the background and wait for
completion rather than a short timeout. `cargo audit` / `cargo deny check` work on the host.
The root disk is small: sibling `target/` dirs reach tens of GB.

## Gates (all must pass before a commit)

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps --all-features
```

CI is staged (format → clippy → test → coverage/Sonar, doc beside test; docs-only
changes and Renovate's PRs run less — see the comment atop `ci.yml`). It adds tarpaulin
with `--fail-under 86` (the binary and the infra-arlo network wrappers
are excluded in `ci.yml`; the GStreamer files count through `tests/live_session.rs`),
audit/deny, Sonar, GitGuardian. Clippy is pedantic: functions over
100 lines fail (`too_many_lines`) — extract a helper rather than `allow`. An `allow` needs a
`reason` or an adjacent comment. Rustdoc links to private items fail the doc job.

## Hexagonal boundaries

- `streamer-domain`: pure types and port traits (`port.rs` is the source of truth). No
  tokio runtime, no GStreamer, no arlo-rs.
- `streamer-app`: orchestration against ports only; actors are `pub(crate)`.
- `streamer-infra-*`: adapters named `*Adapter`; internals private, exported through
  `lib.rs`. Adapter errors (`MediaError`, arlo errors) map into `DomainError` at the edge.
- `streamer-bin`: composition root and CLI (clap `run` / `list-devices`).

## Code rules

- No `unwrap()`/`expect()` outside tests; `thiserror` in libraries.
- No blocking I/O on the runtime (`tokio::fs`, `spawn_blocking`, or a dedicated thread
  for GLib bus loops).
- Named constants for every timeout and magic value, next to their use.
- Async time in orchestration code comes from tokio (`tokio::time::Instant::now()`), so
  `#[tokio::test(start_paused = true)]` drives it. Never mix std and tokio clocks in one
  deadline computation.
- A future that callers may race or cancel must release its resources on drop (RAII
  owner created first, as `WebrtcLive::owning` does).
- `tokio::select!` drops the losing futures. Never read a framed stream (RTSP interleaved,
  length-prefixed records) in a `select!` arm beside timers: a tick mid-frame desyncs the
  stream. Give the read half its own task and keep timers with the write half
  (`rtsp_relay.rs`, 2026-10-01).
- An arm that sleeps until a deadline must either move the deadline or change the state
  when it fires. A past instant whose condition stays true spins the loop; under a paused
  clock the test hangs instead of failing (`user_view_notice_expiry`, 2026-10-02).
- Never log secrets, presigned URLs, egress tokens or PII; redact in `Debug` impls.
  Secrets held for the process lifetime are `secrecy::SecretString` (admin token).
- Text from the network or a library becomes a `DomainError` through
  `DomainError::adapter_transport` / `sanitize_reason` (no control characters, 256
  bytes), never through `AdapterTransport(format!(..))` directly.
- Ids from a trust boundary (config, HTTP path, event bus) go through `CameraId::parse`
  (`[A-Za-z0-9_-]{1,64}`); `CameraId::new` is for trusted values and tests only.
- HTTP listeners go through `streamer_infra_ops::serve::serve` (HTTP/1 only, header-read
  timeout, connection cap), never bare `axum::serve`.

## Testing

- TDD: write the test, watch it fail for the right reason, then implement. A test that
  can hang (a race that never resolves) is bounded with `tokio::time::timeout` on the
  paused clock so it fails in virtual time.
- Unit tests in-file under `#[cfg(test)] mod tests`; names
  `<unit>_<scenario>_<expectedOutcome>`; `rstest` for tables, hand-written doubles over
  `mockall` for ports with state (the doubles must keep what they mint, e.g. notifiers).
- Pure logic lives outside the GStreamer-bound files so it can be covered.

## Dependencies

- Check the latest version upstream (`cargo search`, crates.io), maintenance status and
  `cargo audit` before adding; justify every new crate. Report staleness, don't upgrade
  silently.
- `arlo-rs` is the sibling repo (`../arlo-rs`, released through release-plz). Protocol
  fixes go there first; a commented `[patch.crates-io]` block at the end of `Cargo.toml`
  points at the local checkout for testing before a release.
