---
name: arlo-orchestrator
description: Application-layer camera state machine wiring MQTT events to the media multiplexer and WebRTC signaler.
---
# Arlo Orchestrator Skill

## Camera state machine

`Idle → Activating → Live { since_secs } → Idle`. Repeated `MotionDetected` while `Live` **extends the cooldown**; it never re-enters `Activating` mid-session.

- `Idle → Activating` on `MotionDetected`: call `signaler.ice_servers(camera)` → `media.attach_live(camera, signaler)`.
- `Activating → Live { since_secs: 0 }` on `LiveAttached`.
- `Activating → Failed` on any error from `ice_servers` / `attach_live` / `negotiate` — **always pair with `signaler.teardown(camera)`** on that path.
- `Live → Idle` on `CooldownExpired`: call `media.detach_live(camera)` **and** `signaler.teardown(camera)` — always paired, in that order.

## WebrtcSignaler contract

Every implementation must guarantee:

1. `ice_servers(camera)` caches the SipInfo internally so a following `negotiate(camera, offer)` reuses the same SIP endpoint (no duplicate `sipInfo` HTTP).
2. `negotiate(camera, offer)` returns the SDP answer **verbatim** — no bundle-munging (webrtcbin needs the non-bundled shape).
3. `teardown(camera)` disconnects the signaling socket **and** clears the SipInfo cache so the next attach re-fetches fresh ICE.
4. All three methods are safe to call concurrently for different cameras.

## Two-level attach seam

- `MediaMultiplexer::attach_live(camera, &dyn WebrtcSignaler)` is the *domain port*.
- `PipelineRegistry::attach_live_sink(camera) → LiveRtpSink` is the *internal* seam: the registry owns the live ingestion path; the multiplexer just carries the returned sink to `WebrtcLive::start`.
- **Never spawn a loopback/bridge in the multiplexer** — the registry decides how bytes become a stream. This is the mistake Phase 6.3 undid.

## Failure paths

- `SpliceTimeout` (no first RTP within 20 s) surfaces as `MediaError::SpliceTimeout` → `DomainError::AdapterTransport`. On failure, drop the returned sink so the registry's pump exits.
- The orchestrator must survive `attach_live` errors — transition to `Failed`, wait for the next `MotionDetected`, retry. Do not panic.
- `WebrtcSignaler::teardown` must be idempotent — the orchestrator calls it on both `Failed` and `Live → Idle`.

## Instrumentation

- Every state transition logs `state transition from=X to=Y signal=Z`.
- Every registry method has `#[instrument(fields(camera = %camera))]`.
- Use `debug!` for lifecycle detail (`live ingestion armed`, `input-selector flipped …`); reserve `info!` for state transitions and shutdown.
