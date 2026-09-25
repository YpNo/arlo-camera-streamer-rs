# Hexagonal Architecture & Core Principles
**Role**: Senior Rust Architect

## Architectural Guidelines

- **Domain Layer (`streamer-domain`)**:
    - Must be free of I/O and external transport dependencies.
    - Contains: Camera entities, Stream session models, and Output Port definitions (`ArloEventSource`, `MediaMultiplexer`).

- **Application Layer (`streamer-app`)**:
    - Orchestrates logic using Ports (Traits).
    - Contains: `StreamerSystem`, per-camera `CameraActor`, and lifecycle management (Idle -> Live transition logic).

- **Infrastructure Layer (`streamer-infra-*`)**:
    - Implementation of Output Ports using specialized crates.
    - **`streamer-infra-arlo`**: Adapter for `arlo-rs` protocol library.
    - **`streamer-infra-media`**: GStreamer pipelines, RTSP server, and splicing engine.
    - **`streamer-infra-ops`**: Prometheus metrics and axum health checks.

- **Error Handling**:
    - Use `thiserror` for all library/domain errors in crates.
    - Use `anyhow` strictly in `streamer-bin` and integration tests.

## Specialized Expertise (Agent Skills)

When working in this codebase, the following specialized skills are activated:
- **`rust-core`**: Governs hexagonal boilerplate, crate management, and instrumentation.
- **`media-specialist`**: Governs GStreamer pipeline construction, muxing, and RTSP protocol fidelity.
- **`arlo-orchestrator`**: Governs Arlo event bus integration and stream request lifecycle.

## Coding Style & Safety

- **Instrumentation**: Use the `tracing` crate. Apply `#[tracing::instrument]` to all critical async paths.
- **Defensive Coding**: Avoid `unwrap()`. Use `.expect("SAFETY: <reason>")` for cases where failure is logically impossible.
- **Async Runtime**: Strictly use `tokio`. Avoid blocking calls in async contexts without `spawn_blocking`.
