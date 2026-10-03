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

## 2026-09-27 (night) — ADR 0005 manual-stream piggy-back
Changed:  CameraEvent::{ManualStream, ManualStreamEnded} (activityState
          userStreamActive / idle), StateTransition::{ManualStreamDetected,
          ManualStreamEnded}, LiveTrigger field on the orchestrator, debouncer
          Held state, budget never charged for manual sessions, activation from
          BatteryProtect with a manual-only re-check on exit, 5 s echo guard,
          capture logging in events.rs, admin snapshot `trigger`, ManualWake
          through the same guard as motion, orchestrator clock = tokio Instant.
Why:      Owner wants Frigate to see the feed when they watch in the app; the
          camera is awake anyway so it must be free; must end when the app
          closes (battery). Wire signal is pyaarlo's vocabulary — unverified.
Tests:    343 workspace tests green in the container; clippy/doc/fmt clean.
          App suite dropped from ~4-10 s to 2 s (no more real-time spinning
          on paused-clock deadlines).
Open:     LIVE CAPTURE of the ADR 0005 checklist from the phone (debug target
          streamer_infra_arlo::events). If key/value differ, edit the three
          constants in event_mapper.rs. Item 5 list-devices remains. Deferred
          wiring gap (no client at wake) still open.

## 2026-09-28 — ADR 0005 revised: observe user views, never compete
Changed:  Removed the piggy-back session (LiveTrigger, debouncer Held state,
          manual transitions, echo guard, BatteryProtect activation). Added
          user_view_until flag (120 s hold), MotionOutcome::SuppressedUserView,
          DomainError::CameraBusy (Arlo 14001, both message and structured
          forms), StateTransition::CameraBusy (Activating → Idle, no backoff),
          snapshot `user_view`. arlo-rs branch feat/stream-peek-probe:
          event-driven probe with a 3-variant manifest test, `data.error`
          kept structured, events without `action` decoded.
Why:      Captures: our leg refused while the app streams (14001); the
          watch-along DASH URL answers 502 from Arlo's ALB for every client
          identity; `get` on idle cameras wakes them. Owner: one session only.
Tests:    Workspace 330 green in the container (app 110, domain 43,
          infra-arlo 55, media 84, ops 38); clippy/doc clean. arlo-rs 342.
Open:     arlo-rs branch to merge + release 0.2.1, then bump the streamer and
          drop the 14001 message fallback. Item 5 list-devices. Deferred
          wiring gap (session with no RTSP client).

## 2026-09-28 — step 5: list-devices subcommand
Changed:  streamer-bin: clap subcommands `run` (default) and `list-devices`;
          list_devices.rs renders table / unknown-id warnings / [[cameras]]
          snippets (unique suggested stream names). infra-arlo: discovery.rs
          (`discover_devices`, streamable = camera|doorbell|arloq). domain:
          `DiscoveredDevice`, `StreamName::suggest`. Tracing to stderr.
Why:      Finding opaque device ids was the worst first-run step; running
          it once also completes MFA pairing for the daemon.
Tests:    Workspace 349 green in the container; clippy/doc/fmt clean; `--help`
          checked. Not yet run against the live account.
Open:     Live run of `list-devices`; arlo-rs 0.2.1 bump; deferred wiring gap.

## 2026-09-28 — live video follows the RTSP media lifecycle
Changed:  gst_pipeline.rs: WiringSlot holds the current media's wiring (set at
          media-configure, cleared at unprepared by media identity); pumps
          look it up per buffer, discard while empty, survive push errors;
          `live_active` flag arms the switch on a media built mid-session;
          `expect()` on locks replaced by poison-tolerant `lock()`.
Why:      Owner's VLC test: motion before connecting VLC showed only the
          still for the whole session (camera streamed into a discard pump).
Tests:    Workspace 349 green, clippy/doc clean, release build OK. Pipeline
          behaviour needs the live VLC check (motion first, then connect).
Open:     VLC re-test; arlo-rs 0.2.1 bump (next commit).

## 2026-09-28 — live-wiring fix validated
Tests:    Owner's VLC run: session attached to an existing media; VLC closed
          (media unprepared 14:54:28) and reopened mid-session (14:54:32):
          "media built during a live session; live switch armed", live video
          shown. Connect-after-motion uses the same hook path.
Open:     Optional: idle thumbnail from the bus `presignedLastImageUrl`.

## 2026-09-28 — idle still from bus snapshots
Changed:  CameraEvent::SnapshotAvailable (URL-free); infra-arlo
          snapshot_cache.rs (per-camera presigned URL, https only, 10 min,
          redacted Debug); mapper `snapshot_url`; events adapter records;
          thumbnail adapter uses the cached URL first, forgets it on
          failure, falls back to the device list; orchestrator refreshes
          on SnapshotAvailable only in Idle/BatteryProtect/Failed.
Why:      Newer idle image (motion snapshot), fewer get_devices calls, and
          a still that stays current while motion is suppressed.
Tests:    Workspace 361 green; clippy/doc clean. Live check pending.
Open:     Live check (snapshot line + refreshed still while idle).

## 2026-09-28 — snapshot path validated live; WebrtcLive shutdown made idempotent
Tests:    Owner's run: end of session → `idle snapshot fetched source="bus"`
          then `thumbnail applied to idle overlay` (no device-list call);
          mid-session reconnect: one "push refused" per pump, new media armed
          and flipped to live 1.2 s later; `webrtcbin bus watch exited`.
Changed:  WebrtcLive::shutdown runs once (explicit call + Drop used to post a
          second stop message to a flushing bus: harmless debug noise).
Open:     Idle-still refresh during a user view: unit-tested, not seen live.

## 2026-09-28 — early loss fails the attach with its reason
Changed:  live_watch::setup_or_loss races WebrtcLive::start against the
          LiveSession (biased: a finished setup wins and keeps the report);
          MediaError::LostDuringSetup(reason). WebrtcLive owns the pipeline
          from start's first line, so every error or cancellation stops it
          (a failed attach used to leave it PLAYING with a blocked bus-watch
          thread). SDP exchange extracted; offer timeout named.
Why:      ICE failure or a bus error during setup reported nothing useful:
          the attach waited out the 20 s first-RTP timeout.
Tests:    Five setup_or_loss tests on the paused clock (red first: the two
          loss cases hung until bounded), error mapping test.
Open:     Not provoked live (needs an ICE failure, e.g. blocked UDP).

## 2026-09-28 — skills refreshed; GStreamer integration tests; Cooling removed
Changed:  .agents skills rewritten from the code (orchestrator, media, rust-core) +
          new live-validation skill; splicing audit workflow fixed; arlo-rs
          protocol-specialist gains the live-proven facts (branch
          docs/skills-arlo-live-facts). tests/live_session.rs: FakeGateway
          (second webrtcbin) + RtspProbe (mean luma) — idle→live→idle on one
          client, mid-session join, stall → RtpStalled, hang-up → setup loss
          (peer-disconnected at 14.7 s, libnice timer). CI installs runtime
          plugins, STREAMER_REQUIRE_GST_IT=1. Cooling state removed.
Why:      The GStreamer-bound files had no automated test; a recording cannot be
          replayed (per-call DTLS), a local peer can. Cooling had no producer.
Tests:    Integration suite 4/4, stable over 5 runs (~21 s).
Open:     Cooldown on motionDetected:false needs a capture of how Arlo repeats
          motionDetected during sustained motion.

## 2026-09-28 — motion cadence captured; since_secs removed; pulses logged
Tests:    Owner capture, 2 min of motion: Arlo repeats fullFrameSnapshot →
          motionDetected true → false (~5 s) every ~10 s (max gap 13 s); one
          session 20:46:04 → 20:50:01, ended debounce (owner's config 120 s)
          after the last pulse. No behaviour change needed: `false` ends a pulse.
Changed:  Live is a unit variant (since_secs never updated); motion/audio
          pulses logged at debug in the events adapter and the orchestrator
          (outcome + session_ends_in_ms). Hard cap kept at 300 s (owner).

## 2026-09-28 — pulse logging validated live
Tests:    Owner run, 69f679c: 12 pulses 20:59:59 → 21:02:12, each `camera trigger
          pulse` + `trigger pulse outcome=absorbed session_ends_in_ms=119999`
          (cooldown restarts, owner's debounce 120 s); session ended exactly
          120 s after the last pulse (21:04:12.52, CooldownExpired), under the
          300 s cap. Each pulse cycle also brings SnapshotAvailable and an
          `activityState: idle` report (logged "camera idle report without a
          known user view"): harmless while no user view is tracked.

## 2026-09-28 — backlog: legacy builders, dev-deps, one negotiate call, HLS
Changed:  Phase-4 builders deleted; PT constants shared. mockall 0.15 / rstest 0.27.
          WebrtcSignaler::negotiate(camera, &mut dyn OfferBuilder) replaces
          ice_servers + negotiate (no SipInfo cache; crate::ice tested);
          MediaError::Signaling keeps CameraBusy. Found + fixed: a failed attach
          never released the registry's live sinks → every later attach refused
          ("already in live mode"). HLS via loopback RTSP segmenter (hls.rs,
          ADR 0006); DASH unsupported (stock dashsink: TS only, no pruning).
Tests:    Integration suite 6/6 (refused attach then attach; HLS live segment
          luma > 180, retention, cleanup), stable ×3 (~28 s).
Open:     HLS not yet run on the Frigate box.

## 2026-09-29 — app live view: every route closed
Tests:    Owner runs, app view open each time, app never affected:
          startUserStream accepted, URL in the POST reply (not the bus) =
          watch-along DASH → 502; its sipCallInfo used for our WebRTC leg →
          gateway `NO_ROUTE_DESTINATION`. With 14001 (sipInfo) and 502
          (get_stream_url) that closes all four routes. ADR 0005 addendum.
Found:    arlo-rs force_start_stream ignored the POST reply's URL (always
          timed out); fixed on its own branch.
Kept:     local probe branches arlo-rs probe/force-start-during-view,
          streamer probe/join-user-view (never push: [patch.crates-io]).

## 2026-09-30 — the app's live view is reachable; relay module (ADR 0007 step 1)
Found:    Arlo answers the `get` stream query per User-Agent (pyaarlo's
          `user_agent` option; owner's pyaarlo PR #166). As the iOS app
          identity, during an app view, it returns the view's own
          `rtsps://<ip>:443/vzmodulelive/…?egressToken=…&watchalong=true`.
          A raw RTSP client plays it (OPTIONS/DESCRIBE/SETUP/PLAY 200, RTP
          flowing, app unaffected); rtspsrc gets 403 at SETUP for a reason
          not identified (not the token: SETUP without it is accepted).
          TLS: raw IP, certificate cannot match → validation off.
Changed:  arlo-rs branch fix/force-start-stream-reply-url: force_start_stream
          reply URL fix + get_stream_url_as / ios_app_user_agent + probe
          (PR-ready, 349 tests). Streamer: WatchAlongUrl (redacted),
          UserViewSource port, MediaMultiplexer::attach_user_view,
          rtsp_relay.rs (own RTSP client → LiveSinks.video, PT rewritten to
          103, keep-alive, RTCP RR, shared stall watchdog), LiveLeg enum,
          arm_live shared by both legs; tokio net/io-util made explicit.
Tests:    Relay pure pieces unit-tested; integration: relayed white source
          reaches the client, source end reported; 10/10 ×3.
Open:     Orchestrator wiring + Arlo adapter (UserViewSource) + ADR 0007;
          Arlo's H.264 may lack in-band SPS/PPS (SDP carries sprop) —
          check at the live gate, inject via appsrc caps if black.

## 2026-10-01 — ADR 0007: the app's view relayed (orchestrator + adapter)
Changed:  Signals UserViewStarted/Ended/Unavailable, LiveSource tag,
          CameraSnapshot.live_source, ArloConfig.app_version (6.46.0),
          ArloUserViewSourceAdapter (get_stream_url_as as the iOS app),
          orchestrator relay flow (no debounce, no budget, 30 s retry guard,
          motion absorbed during a relay), main.rs arlo_adapters() helper,
          `[patch.crates-io] arlo-rs = ../arlo-rs` ON (owner's request;
          remove when 0.2.2 ships).
Tests:    App 120 (5 new relay tests; 4 ADR-0005 tests retargeted), domain
          57, infra-arlo 75, media 97 + 10 integration, ops 38; clippy/doc
          clean; release built.
Open:     Live gate of the relay; SPS/PPS in-band question; audio.

## 2026-10-02 — Relay gated live; probe; MFA login skill; ADR cleanup
Changed:  rtsp_relay.rs: read half in its own task (cancel-safety), bare RTCP
          skipped by its length, resync on Arlo's unframed packets (the AAC
          audio RTP, PT 0, arrives unframed on the same connection), desync
          hex report; relay capped then probed (`user_view_probe_secs`, 60 s,
          `UserViewProbe` signal, 5 s grace for the camera's `idle`); notice
          kept through a relay. New skill `arlo-mfa-login` (code-read facts:
          trusted-browser pairing in the cache file, IMAP/push/stdin paths,
          log lines, troubleshooting). rust-core: select! lessons.
          ADR 0005 trimmed to its rules, its dead-end table moved to 0007.
Tests:    App 126, domain 57, infra-arlo 75, media 104 + 10 integration,
          ops 38; gates green. Live: relay shows the app's view in VLC;
          probe stop path proven (idle 340 ms after TEARDOWN).
Open:     Probe resume path only unit-tested; audio relay; arlo-rs 0.2.2
          release then drop `[patch.crates-io]`; MFA skill not yet run by
          the owner on a fresh cache.

## 2026-10-02 — Audio relay (ADR 0007), CI staged, arlo-rs 0.2.2
Changed:  Third audiomixer input (`live_aac_rtp_src`, rtpmp4gdepay →
          avdec_aac) fed by `LiveAacSink` (format first, then RTP; the pump
          applies the caps per media; SDP fields typed `(string)`). The relay
          parses the AAC track, SETUPs it on channels 2-3, routes its frames,
          and relays Arlo's bare AAC packets by their AU size. Both repos'
          CI staged; streamer release job folded into ci.yml; arlo-rs 0.2.2
          from crates.io, patch block off.
Tests:    Media 110 unit + 11 integration (new: tone through the relay heard
          by the probe's `level`); workspace green; release built.
Open:     — (audio heard in VLC and the probe's resume path seen live on 2026-10-02; grace cut to 2 s).

## 2026-10-03 — ADR 0008: encoder selection; MFA skill validated
Changed:  `VideoEncoder::{Auto (default), X264, Va, Vaapi, V4l2, Nvenc}`;
          media `encoder.rs` (`EncoderBackend`, segments, `resolve` with a
          dry run per backend); main resolves at boot; Dockerfile carries
          vaapi + VA drivers; README/config/ADR 0008. MFA skill corrected
          from a fresh login (9261 for a new device id → arlo-rs
          fix/untrusted-9261). arlo-rs refactor/examples-common.
Tests:    Media 113 unit + 13 integration (auto resolves on a box with VA
          elements but no /dev/dri → x264; explicit unavailable refused);
          domain 57; workspace green; release built.
Open:     Hardware backends (va, v4l2, nvenc) unseen on hardware; Frigate
          box deployment pending.

## 2026-10-03 — Release 0.1.0 preparation
Changed:  ci.yml: `release` exposes version/created; `image` (per-platform,
          native runners, push by digest) and `image-publish` (Trivy
          report + CRITICAL gate, manifest tags v<version>/latest/sha-*);
          Sonar action v8.3.0; Dockerfile OCI labels; Renovate pins image
          digests. README Docker sections reference the GHCR image;
          CHANGELOG cut to 0.1.0 with link refs; SECURITY.md rewritten
          (was another project's); licence "MIT" to match the file; bin
          description without DASH; stale "video only" wording removed;
          Cargo.lock refreshed. arlo-rs: refactor + 9261 fix cherry-picked
          onto ci/stage-the-pipeline (PR #35) for one 0.2.3 release.
Tests:    Workspace green on the refreshed lockfile; actionlint clean.
Open:     First image build runs on the merge; arm64 leg needs the
          IMAGE_PLATFORMS variable and a billed arm64 runner.

## 2026-10-03 — Docs restructure
Changed:  `docs/adr/HANDOFF.md` → `.agent/HANDOFF.md` (an AI handoff, not an
          ADR); its status table → `docs/VALIDATION.md` (human validation
          record, live gates by feature); `docs/adr/README.md` index;
          ADR 0005 renamed to its title (`user-views-observe-never-compete`);
          README ADR list uniform; CLAUDE.md points at index, record, memory.

## 2026-10-03 — Security sweep: items 1 and 2 fixed
Changed:  Relay TLS: chain verification via system roots with hostname waiver
          or a certificate pin (`RelayTls`, `arlo.watch_along_cert_sha256`);
          bounded RTSP lines/headers/Content-Length. thumbnails: errors
          without the presigned URL (`without_url`). Orchestrator: backoff
          armed on every entry into Failed, Online ends it. webrtc: `ice-agent`.
          `.security/` ignored. Sweep report in .security/report-2026-10-03.md
          (48 findings; 29 confirmed; items 3–9 open).
Tests:    Media 118 + 13, app 128, infra-arlo 76; clippy/doc/deny clean.
Open:     Live gate of the TLS chain (trusted or pin); remaining sweep items.

## 2026-10-03 — Security sweep items 3 and 4
Changed:  Config: `deny_unknown_fields` on every table, `StreamerConfig::validate`
          (duplicate ids/stream names, cooldown ranges) called by the loader
          and `StreamerSystem::spawn`; `TimeDelta::try_seconds` for the budget.
          Budget: `remaining()`/`charge()`; live deadline = min(debouncer,
          budget); `handle_deadline` emits BudgetExhausted mid-session;
          15 s activation surcharge; manual wake refused within 30 s of the
          last session (`WakeOutcome`, `AdminError::RateLimited`, HTTP 429).
Tests:    App 133, domain 59, ops 38 (+429 mapping untested at HTTP level: the
          actor mapping is), workspace green, release built.
Open:     Sweep items 5–9.

## 2026-10-03 — Security sweep items 5 and 6
Changed:  Relay: hex-only AAC `config`, typed caps builder, distinct/disjoint
          channel pairs, `timed_write` on every server write, `AbortOnDrop`
          read task. CI: cancel-in-progress for PRs only, `image_missing`
          output (docker manifest inspect) gates the image jobs, Trivy per
          platform inside `image`, timeouts on every job. Dockerfile: base
          images and frontend pinned by digest (Renovate maintains them).
Tests:    Media 119 + 13; workspace green; actionlint clean; release built.
Open:     Sweep items 7–9.

## 2026-10-03 — Security sweep items 7 and 8
Changed:  Thumbnails: `thumbnails::http_client` (https only, 10 s/5 s timeouts,
          2 redirects), body capped at 2 MiB chunk by chunk, JPEG magic check,
          device-list URL through the shared `is_https_with_host`; files in
          `<session cache dir>/thumbnails` (0700, `prepare_thumbnail_dir`),
          `create_new` temp + rename. Supervision: router cancels the shared
          token on an upstream end, orchestrator runs `handle_shutdown` on a
          closed mailbox, `supervised()` wraps every task (panic/early exit →
          cancel), main exits non-zero when the system stopped itself.
          Admin snapshots on a `watch` channel (no `AdminCommand::Snapshot`,
          no `unresponsive` stub). Notice update failure retried every 30 s.
          401s logged with the route; session id logged as a 4-char tail.
Tests:    New: thumbnails fetch (loopback server), supervised ×3, ended bus,
          router cancel, channel-close release, notice retry, snapshot watch.
Open:     Sweep item 9 (hygiene batch); live gate of the TLS chain.

