# Project Context: arlo-camera-streamer-rs

This document provides a high-level overview of the project for AI Agents to understand its purpose, architecture, and core mechanics.

## 🎯 Purpose
The **Arlo Streaming Bridge** is a specialized media proxy designed to bridge Arlo battery-powered cameras to standard Network Video Recorders (NVRs) like Frigate. Its primary goal is to provide a continuous RTSP stream for each camera without draining their batteries by constantly pulling live video.

## 🚀 Core Innovation: "Idle-to-Live" Splicing
Unlike traditional proxies that keep a live stream open (which kills Arlo batteries in hours), this bridge uses a **dual-source splicing engine**:
1.  **Idle Mode**: When no motion is detected or no live view is requested, the bridge serves a locally-generated, ultra-low-bandwidth dummy stream (e.g., a static image or a single frame every few seconds).
2.  **Live Mode**: When a motion event occurs or a manual stream is requested, the bridge triggers the Arlo API (via `arlo-rs`), receives the encrypted H.264/H.265 feed, and **splices** it into the existing RTSP session.
3.  **Transparency**: The RTSP client (e.g., Frigate) never sees a disconnect; the bridge handles the transition between the dummy source and the real Arlo source seamlessly.

## 🏗️ Architecture
The project follows a **Hexagonal Architecture** (Ports and Adapters) to isolate media processing and protocol logic:

-   **`streamer-domain`**: Pure domain logic. Defines camera entities, stream states, and ports (traits) for event sources and media multiplexers.
-   **`streamer-app`**: Use-case orchestration. Implements the `CameraActor` state machine that manages the lifecycle of a single camera's stream.
-   **`streamer-infra-media`**: GStreamer-based implementation of the media port. Handles pipeline construction, RTSP serving, and the low-level splicing logic.
-   **`streamer-infra-arlo`**: Adapter for the `arlo-rs` library. Maps Arlo SSE events to domain events and handles stream requests.
-   **`streamer-infra-ops`**: Operational concerns like Prometheus metrics and health checks.
-   **`streamer-bin`**: The composition root that boots the system and CLI.

## 🛠️ Technology Stack
-   **Language**: Rust (Edition 2024).
-   **Media Engine**: GStreamer (via `gstreamer-rs` bindings).
-   **Async Runtime**: Tokio.
-   **Protocol Support**: RTSP (outbound), Arlo SSE/REST (inbound).
-   **Instrumentation**: Comprehensive `tracing` spans and Prometheus metrics.

## 🔑 Key Concepts for AI Agents
-   **No Transcoding**: The bridge aims for zero-transcode pass-through of H.264/H.265 to minimize CPU usage.
-   **Event-Driven**: The system is reactive to Arlo's event bus.
-   **Resilience**: The bridge is designed to recover from Arlo session timeouts and GStreamer bus errors automatically.
-   **Hexagonal Boundaries**: Never leak GStreamer types into the domain or app layers; use the ports defined in `streamer-domain`.
