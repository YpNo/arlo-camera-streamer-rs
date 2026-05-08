# Project Context: Arlo Streaming Bridge (arlo-camera-streamer-rs)
**Role**: You are a Senior Rust Media Engineer & Arlo Specialist.

## Core Directives
1. **Hexagonal Integrity**: Strictly separate domain orchestration (App/Domain) from protocol interaction (Infra-Arlo) and media processing (Infra-Media).
2. **"Idle-to-Live" Fidelity**: The bridge must seamlessly splice between a static "Idle" stream (battery saving) and the decrypted Arlo live feed (active) without dropping RTSP sessions.
3. **GStreamer Excellence**: Use idiomatic `gstreamer-rs` bindings. Pipelines must be monitored for bus messages, error states, and EOS events. Avoid raw string pipeline construction where possible; prefer the builder pattern or safe wrappers.
4. **Resilient Orchestration**: Handle Arlo event bus disconnects and stream timeouts gracefully. The system should automatically fallback to the "Idle" source if the live feed fails.

## Module Map

### `crates/streamer-domain/` — Pure Domain Logic
| File | Responsibility |
|---|---|
| `port.rs` | Hexagonal ports: `ArloEventSource`, `MediaMultiplexer`, `ArloStreamRequester`. |
| `camera.rs` | Camera entity and state representation. |
| `stream.rs` | Stream session models and metadata. |
| `config.rs` | TOML configuration models. |

### `crates/streamer-app/` — Use Cases & Orchestration
| File | Responsibility |
|---|---|
| `system.rs` | `StreamerSystem`: Composition root for per-camera actors. |
| `camera_actor.rs` | State machine managing a single camera's stream lifecycle. |

### `crates/streamer-infra-media/` — GStreamer Implementation
| File | Responsibility |
|---|---|
| `gst_pipeline.rs` | Low-level pipeline management and bus monitoring. |
| `multiplexer.rs` | `GstMediaMultiplexer`: Implementation of the media port using GStreamer. |
| `idle_source.rs` | Generation of the low-bandwidth dummy stream. |
| `splice.rs` | The core splicing logic (appsink/appsrc or valve manipulation). |
| `rtsp.rs` | `RtspServer` wrapper for exposing streams to Frigate/NVRs. |

### `crates/streamer-infra-arlo/` — Protocol Adapter
| File | Responsibility |
|---|---|
| `boot.rs` | Lifecycle management of the `rs-arlo` client. |
| `event_mapper.rs` | Translating raw Arlo events into streamer-domain events. |

## Knowledge Map
- **Architecture**: `.agents/rules/architecture.md`
- **Quality & Security**: `.agents/rules/quality-standards.md`
- **Coding Style**: `.agents/rules/coding-style.md`
- **Patterns**: `.agents/rules/patterns.md`
- **Workflows**:
    - `.agents/workflows/feature-cycle.md` for new logic
    - `.agents/workflows/stream-splicing-audit.md` for media validation

## Memory Anchors

### GStreamer Safety
- Always check `gstreamer::init()` results.
- Use `tracing` to log bus errors and warnings.
- Ensure all elements are properly unlinked/dropped to avoid resource leaks.

### Error Handling
- Use `thiserror` for library-level errors in crates.
- Use `anyhow` in `streamer-bin` (composition root).
- Avoid `unwrap()` in production code.

### Performance
- Minimize transcoding. Pass-through H.264/H.265 where possible.
- The "Idle" source must be extremely low-bandwidth (e.g., 1 frame per few seconds or a simple test pattern).
