# Arlo to Frigate Streaming Bridge (arlo-streamer-rs)

## Executive Summary

This document outlines the architecture and development plan for arlo-streamer-rs, a Rust-based daemon that bridges the Arlo camera ecosystem with Frigate. It leverages the highly capable arlo-rs library to securely authenticate, listen to events, and decrypt media, while exposing standard video streams (RTSP, HLS, DASH) for NVR consumption.

## Architectural Analysis & Challenges (The "Challenge")

As a Senior Architect, I must push back on a few naive assumptions typically made when integrating battery-powered IoT cameras with continuous-recording NVRs like Frigate.

### Challenge 1: The Battery Drain Paradox (Continuous NVR vs. Sleepy Camera)

The Problem: Frigate expects a 24/7 continuous stream (typically RTSP) to perform its object detection. If we expose a direct RTSP proxy to Arlo, Frigate will keep the stream open indefinitely. For battery-operated Arlo cameras, this will drain the battery in less than 24 hours.
The Solution (Stream Splicing / Fallback Multiplexer): Our bridge must decouple Frigate's pull from Arlo's push.

- Idle State: The Rust app serves a locally generated 1 FPS dummy stream (e.g., a black frame, or the last detected snapshot) to Frigate. This keeps Frigate's Ffmpeg process alive without waking the camera.

- Active State: When the arlo-rs SSE Event Bus emits a Motion or Audio event, the app triggers a stream request, intercepts the encrypted chunks, decrypts them via arlo-rs::client::library, and dynamically splices the live H.264/H.265 frames into the continuous pipeline.

- Cooldown: Once the event completes, the pipeline seamlessly transitions back to the dummy frame.

### Challenge 2: Multi-Protocol Output (RTSP/HLS/DASH) Complexity

The Problem: Writing RTSP, HLS, and DASH servers from scratch in Rust is a massive, error-prone undertaking that distracts from the core integration logic.
The Solution (GStreamer-based Native Integration):
We should not reinvent the media serving wheel, but we also want to keep the architecture as native and single-process as possible.

- Instead of relying on an external sidecar daemon like MediaMTX, we will embed an RTSP server directly into our Rust application using `gstreamer-rs` and `gstreamer-rtsp-server-rs`.
- GStreamer can auto-detect the incoming codec (H.264/H.265) and package it natively into an RTSP stream without requiring `ffmpeg` or re-encoding. This ensures high performance and low latency.

### Challenge 3: Encryption & Local Hub vs. Cloud

The Problem: As noted in the arlo-rs context, Arlo encrypts media chunks, and fetching them involves complex S3 chunking or local RATLS.

The Solution: We must rigorously use arlo-rs's Media Library for decryption and abstract the source. The streamer should not care if the chunk came from LocalHubClient (LAN) or CloudScraperTransport (WAN) — it only expects raw decrypted frames.

## Proposed System Architecture

Components

### Arlo Controller (arlo-rs wrapper):

- Manages the 6-step MFA OAuth ceremony via MfaHandler.

- Maintains session via SessionToken snapshotting.

### Event Orchestrator:

- Hooks into the arlo-rs SSE EventBus.

- Maps Arlo device IDs to local Frigate stream identities.

### Stream Pipeline (The Multiplexer via GStreamer):

- DummySrc: A GStreamer element (e.g., `videotestsrc` or `appsrc` with a static frame) generating idle frames at 1 FPS.

- LiveSrc: `appsrc` element receiving decrypted byte arrays from `arlo-rs::client::library`.

- SwitchingLogic: GStreamer `input-selector` or dynamic pad linking to safely switch streams without dropping sequence numbers or breaking NAL units.

### Output Muxer (Embedded RTSP Server):

- Packages the raw H.264/H.265 stream into RTP and serves it natively via the embedded GStreamer RTSP Server.

## Development Plan & Milestones

Phase 1: Foundation & Event Bridging

- Task 1.1: Initialize the application using tokio and tracing. Setup configuration parsing (mapping Arlo Device IDs to friendly names).

- Task 1.2: Integrate arlo-rs initialization. Hook up ImapMfaHandler or StdinMfaHandler for automated headless login.

- Task 1.3: Subscribe to the arlo-rs SSE EventBus. Create a reactive loop that logs ConnectionState and Motion events to standard out.

Phase 2: Decryption & Media Ingestion
- Task 2.1: On motion event, trigger get_stream via arlo-rs.

- Task 2.2: Pipe the encrypted S3 chunks/Local Hub stream through arlo-rs::client::library.

- Task 2.3: Buffer and identify the NAL units (Network Abstraction Layer) of the decrypted H.264/H.265 stream.

Phase 3: The Stream Splicer (The Hard Part)
- Task 3.1: Implement the "Idle Frame Generator". This can be done by generating an empty H.264 frame loop.

- Task 3.2: Build the dynamic switch. Ensure that when switching from Idle -> Live, an IDR frame (Keyframe) is requested or waited for, so Frigate doesn't receive garbled P-frames without a reference.

- Task 3.3: Output the seamless byte stream to a named pipe (FIFO) or local UDP socket.

Phase 4: Output & Multi-Protocol Serving
- Task 4.1: Integrate `gstreamer-rtsp-server-rs`.

- Task 4.2: Configure the GStreamer pipeline to expose the RTSP endpoint:

rtsp://localhost:8554/{camera_name}

- Task 4.3: Document the Frigate go2rtc and cameras: yaml configuration mapping. Since Frigate handles RTSP seamlessly, we only need to expose the RTSP endpoint.

Phase 5: Hardening & Containerization
- Task 5.1: Implement auto-reconnect logic for when arlo-rs Pinger fails or session expires.
- Task 5.2: Create a minimal Dockerfile multi-stage build (bringing in rs-cloudscraper dependencies and GStreamer runtime libraries).
- Task 5.3: Finalize CI/CD pipelines ensuring no unsafe code and passing cargo clippy.

## Summary of Frigate Configuration Target
Once deployed, the ideal Frigate configuration consuming this bridge will look like:

```yaml
cameras:
  front_door_arlo:
    ffmpeg:
      inputs:
        - path: rtsp://127.0.0.1:8554/front_door # Provided by our Rust Bridge
          roles:
            - detect
            - record
    detect:
      enabled: True
      fps: 5 # Arlo streams are low FPS, optimize Frigate config
```

# Thoughts

I've thought about something : How the user can get the device IDs at the first time ? It seems to be unconfortable for the user to find the information. Otherwise, we should giving a way to show him up.

Find a way to simplify this botleneck.

2/ Generate a dedicated skill for the Arlo's OTP protocol
3/ Check the config example
4/ Explain the reverse engineering of Arlo's API from HAR files and the output logs. What have you learned from it ?