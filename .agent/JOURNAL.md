# JOURNAL — arlo-camera-streamer-rs

Append-only. Newest at the bottom.

## 2026-09-27 — CI unblock, docs hygiene, ADR 0004 live-loss feedback
Changed:  toolchain 1.98.1 everywhere; `arlo-rs` from crates.io; Dockerfile
          + CI native deps (git, libclang, nasm, plugins-bad dev, wget);
          example config fixed + parse test; SSE/Chromium wording purged;
          CHANGELOG, CONTRIBUTING, HANDOFF rewritten; ADR 0004:
          `LiveSession`/`LiveLossNotifier`, `StateTransition::LiveLost`,
          orchestrator select arm + invariant, `live_watch.rs`, webrtcbin
          detectors, `webrtc.live_stall_timeout_secs`.
Why:      CI had never run and would have failed on toolchain + path dep;
          a dead live source froze the RTSP output for up to 300 s, and the
          coming ManualStream feature makes that the default case.
Tests:    fmt / clippy -D warnings / rustdoc -D warnings clean on the four
          buildable crates inside the distrobox; see the session report for
          the test count. Media crate: unit tests for `live_watch` written,
          not runnable here; webrtcbin wiring blind until CI.
Open:     PR to `main` not yet created (push from this shell is blocked:
          no SSH agent, gh token lacks `workflow`). Item 4 (ManualStream:
          capture the MQTT event first) and item 5 (`list-devices`) remain.
          Follow-up from ADR 0004: race first-RTP wait against early loss.

## 2026-09-27 (evening) — first live runs, three fixes from the logs
Changed:  boot.rs creates the session cache directory (owner-only) so the
          trusted-browser pairing survives restarts; WebrtcLive::shutdown
          posts a bus application message so the bus-watch thread exits;
          admin ForceIdle/ManualWake acknowledge before acting (a wake's
          WebRTC negotiation exceeded the 2 s admin reply timeout and was
          reported as "orchestrator unavailable"). CI: doc links, tarpaulin
          file exclusions, gate 80 (measured 82), Sonar config + skip.
Why:      The user's first two live runs (GStreamer 1.26, Debian trixie)
          showed all three; the ADR 0004 stall path itself passed:
          live-lost-rtp-stalled 4.0 s after the last packet, then detach.
Tests:    Whole workspace now builds in the rust-build container (GStreamer
          dev packages installed): clippy/rustdoc -D warnings clean, 299
          tests green (app 96, domain 43, infra-arlo 38, infra-media 84,
          infra-ops 38).
Open:     Gap recorded in HANDOFF §5: a live session started with no RTSP
          client keeps discarding RTP after a client connects (media is
          rebuilt but pumps are not re-armed); gst-rtsp-server unprepares
          the media ~10 s after the last client leaves. Item 4
          (ManualStream capture switch) and item 5 (list-devices) remain.
