# Contributing to arlo-camera-streamer-rs

Thanks for helping. The bar is "production daemon that runs unattended next
to someone's NVR": every change must keep the battery-safety invariants and
the zero-warning gates intact.

## Non-negotiable rules

1. **Battery first.** Every path that enters the `Live` state must pair with
   a `WebrtcSignaler::teardown` on *every* exit (cooldown, cap, failure,
   shutdown). A leaked session keeps the camera awake.
2. **No `unwrap()` / `expect()` outside tests.** Use `Result` with
   `thiserror` in libraries and `anyhow` in the binary.
3. **No blocking I/O on the tokio runtime.** `tokio::fs` or `spawn_blocking`.
4. **Hexagonal integrity.** `streamer-domain` has no I/O and defines the
   ports; adapters implement them; `streamer-bin` is the only place that
   wires concrete types together.
5. **A new dependency needs a justification in the PR** (why stdlib or an
   existing dependency does not do the job) and must pass `cargo deny`.

## Development workflow

- Toolchain: `rust-toolchain.toml` (1.98.1, follows `arlo-rs`). `mise.toml`
  pins the same version plus cmake for the BoringSSL build.
- Gates, all of which CI runs on every PR:

  ```bash
  cargo fmt --all -- --check
  cargo clippy --workspace --all-targets --all-features -- -D warnings
  cargo test --workspace --all-features
  cargo doc --workspace --no-deps --all-features   # RUSTDOCFLAGS=-D warnings
  cargo deny check
  ```

- `streamer-infra-media` needs the GStreamer development headers. Without
  them, build and test the other crates with `-p` and let CI cover the media
  crate.
- Coverage is gated at 85 % (tarpaulin, media crate excluded). New behavior
  ships with tests; failure paths are tested, not only happy paths.
- Test names follow `<unit>_<scenario>_<expectedOutcome>`.

## Branches, commits, changelog

- Branches: `feat/<topic>`, `fix/<topic>`, `chore/<topic>`, `docs/<topic>`,
  `security/<topic>`.
- Commits follow [Conventional Commits 1.0](https://www.conventionalcommits.org/en/v1.0.0/):
  `type(scope): summary`, body explains *why*. Types: `feat`, `fix`, `docs`,
  `refactor`, `test`, `build`, `ci`, `chore`, `security`. Scopes are the
  crate short names: `domain`, `app`, `infra-arlo`, `infra-media`,
  `infra-ops`, `bin`; omit the scope for repo-wide changes.
- Every user-visible change adds a line under `[Unreleased]` in
  `CHANGELOG.md` (Keep a Changelog headings).
- Architectural decisions that a future reader might reverse get an ADR in
  `docs/adr/`.

## Pull requests

- One concern per PR, opened as a draft early for anything larger than a
  fix.
- Self-review the diff, link the issue, include test evidence for behavior
  changes and the relevant `gst-launch` or log excerpt for pipeline changes.
- Pipeline changes cannot be fully validated on macOS or without a camera:
  say what was tested live and what was not.

## Security

- Never commit credentials, session caches, HAR captures or logs. Secrets
  reach the daemon only through environment variables named in the config.
- Report vulnerabilities as described in `SECURITY.md`.
