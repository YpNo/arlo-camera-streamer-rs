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
