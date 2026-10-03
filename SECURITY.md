# Security Policy

## Reporting a vulnerability

Please do **not** open a public issue for a security problem. Write to
**ypno+security@gmail.com** with a description, steps to reproduce and the
impact you see. You will get an acknowledgement within 48 hours and a
timeline for the fix; the fix ships as a new release and a changelog entry
that credits you if you wish.

## Supported versions

Only the latest release receives security fixes. Run the image tag of the
newest release (`ghcr.io/ypno/arlo-camera-streamer-rs:v<version>`).

## What this daemon handles

- **Arlo credentials**: the account password and the IMAP password are
  read from environment variables at boot and never written to disk or
  logs. The session cache (`arlo.session_cache_path`) holds the Arlo
  token, cookies and the paired device id, mode `0600`; treat it as a
  credential.
- **Presigned URLs, egress tokens, session ids**: never logged; `Debug`
  and `Display` implementations redact them.
- **The admin endpoint** refuses to start without a non-empty
  `STREAMER_ADMIN_TOKEN` and requires it as a bearer token. Bind it and
  `/metrics` to a trusted interface.
- **RTSP and HLS outputs** are unauthenticated and unencrypted by design
  (Frigate consumes them on the LAN). Bind them to a trusted interface or
  front them with a proxy.
- **TLS to Arlo's watch-along host** (ADR 0007) runs without certificate
  validation, because Arlo hands out a raw IP no certificate can match;
  the per-session egress token in the URL is the access control, as it is
  for the mobile app. All other Arlo traffic is validated.

## Code and supply chain

- `unsafe_code` is forbidden in every crate of this workspace. The
  GStreamer bindings and the BoringSSL used by `arlo-rs`'s HTTP client
  are native code behind audited Rust wrappers.
- CI runs `cargo deny` (advisories, licences, duplicate crates) on every
  change and weekly, `cargo audit` locally before releases, and Trivy on
  the container image before any tag points at it. Dependencies and base
  images are kept current by Renovate, with actions and images pinned to
  digests.
- The image runs as an unprivileged user (uid 10001) with `tini` as PID 1.
