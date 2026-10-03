# Validation record

What has run against a real Arlo camera and the owner's box, what is only
covered by tests, and the evidence for each. The ADRs record decisions;
this file records whether reality agreed. Update it after every live gate
(the `live-validation` skill says which log lines prove each path).

## Live gates by feature

| Feature | Last live gate | Evidence |
|---|---|---|
| Motion session (ADR 0003, 0004) | 2026-09-28 | motion → live → cooldown exactly `debounce_secs` after the last pulse; stall path `live-lost-rtp-stalled` 4 s after the last packet (2026-09-27). |
| App-view relay, picture (ADR 0007) | 2026-10-01 | VLC shows the app's view; SPS/PPS in-band; Arlo's bare RTCP and audio packets handled. |
| App-view relay, sound (ADR 0007) | 2026-10-02 | AAC track set up on channels 2-3, framed by the server; the owner heard the room in VLC. |
| Relay probe, stop and resume (ADR 0007) | 2026-10-02 | release at 60 s; `idle` 0.3–0.8 s after `TEARDOWN` when the app had closed; relayed again 0.5 s after the grace otherwise. |
| HLS output (ADR 0006) | 2026-10-02 | idle → relayed view → idle in VLC over HLS; the VLC error seen once on 2026-09-30 did not recur. |
| MFA login, IMAP factor | 2026-10-03 | fresh cache path, 13 s to `authentication complete`, file mode 0600, code 9261 on the trusted-browser probe of a new device id. |
| Encoder `x264` (ADR 0008) | 2026-10-03 | every gate above; `auto` resolves to it on a box with VA elements and no render node. |
| Encoder `vaapi` | before 2026-09-28 | ran on the owner's Intel box in an earlier phase. |
| Encoders `va`, `v4l2`, `nvenc` | **never** | written from element documentation; first run on the Frigate box. |
| Several cameras at once | **never** | per-camera actors, unit-tested only. |
| H.265 camera | **never** | code path present, the owner's camera sends H.264. |
| Container image from the release workflow | **never** | first build on the merge that releases 0.1.0. |
| `linux/arm64` image | **never** | opt-in through `IMAGE_PLATFORMS`; needs a Pi to prove. |

## Status detail

| Item | Notes |
|---|---|
| Coverage gate | 86 % (measured 88.25 % on 2026-09-28, GStreamer files counted since the integration tests). Still excluded: `streamer-bin` and the infra-arlo network wrappers (`boot`, `events`, `stream_requester`, `thumbnails`). |
| Release 0.1.0 | Prepared 2026-10-03: changelog cut, lockfile refreshed (166 crates), licence metadata aligned to the MIT file, SECURITY.md rewritten for this project, image publishing added to `ci.yml` (GHCR, per-version, Trivy-gated, arm64 opt-in via `IMAGE_PLATFORMS`). The tag and the image appear on the merge to `main`. |
| Encoder selection | ADR 0008 (2026-10-03): `encoder::resolve` with a dry run per backend; integration-tested on the dev box (VA elements present, no render node → x264). Hardware backends `va`, `v4l2`, `nvenc` written from element docs, not yet run on hardware; `vaapi` ran before 2026-09-28. |
| MFA login skill | `.agents/skills/arlo-mfa-login`, validated 2026-10-03 on a fresh cache path (IMAP factor, 13 s, file 0600; Arlo answers 9261 "Invalid factor data" to the trusted-browser probe of a new device id). |
| App-view relay | Built and **gated live 2026-10-01** (ADR 0007): VLC shows the relayed view, SPS/PPS arrive in-band (no caps work needed), the hard cap ends the relay at 300 s and the camera idles within a second of our TEARDOWN. The relay probes every `user_view_probe_secs` (60 s): release, 5 s for the camera's `idle` report, resume if none — so the camera stops within a minute of the app and a view may last longer than the cap. Gated 2026-10-02 for the stop path (release at 60 s, `idle` 340 ms later, no re-attach); the resume path gated 2026-10-02 (no `idle` in the grace → relayed again, Live 0.5 s later); grace cut to 2 s since the report takes 0.3–0.8 s. Audio relayed since 2026-10-02 (AAC track `SETUP` + bare-packet fallback, third mixer input; gated 2026-10-02: with the track set up Arlo frames the audio on channel 2, no bare packets; the owner heard the room in VLC). |
| HLS / DASH | HLS wired 2026-09-28 (ADR 0006, `hls.rs`), integration-tested (live picture in a segment, retention, cleanup); run on the owner's box 2026-10-02 (idle → relayed view → idle over HLS in VLC); a VLC connection error seen once on 2026-09-30 with HLS on did not recur and is closed. DASH unsupported: parsed, warned, ignored. |
| Integration test vs a real gateway | `tests/live_session.rs` (2026-09-28) replaces the planned recorded-session fixture: DTLS keys are per call, so a recording cannot be replayed; a local `webrtcbin` answers instead. It proves negotiation, the splice seen by a client, mid-session join, stall and setup loss. Arlo-specific quirks (TURN, SDP shape drift) still need the Frigate box. |
| Live session vs RTSP client lifecycle | **Fixed 2026-09-28.** The pumps push into the *current* media's appsrc (slot set at `media-configure`, cleared at `unprepared`), discard while none exists, and a media built during a live session gets its switch armed. Validated live 2026-09-28: VLC disconnected and reconnected mid-session and got the live video back ("media built during a live session; live switch armed"). |
| Dependency currency | `mockall` 0.15 and `rstest` 0.27 adopted 2026-09-28; everything else within one minor of latest on 2026-09-27. |

---

## Rules

- Never poll `get_stream_url` (action `get`): it wakes idle cameras.
- Never paste presigned URLs, egress tokens, cookies or credentials into
  this file or an issue; quote log lines with their keys only.
