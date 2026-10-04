# Architecture decision records

One file per decision, numbered in order, never rewritten once accepted:
a superseded ADR keeps its text and points at its successor, so the
history of why stays readable. Whether a decision held up against the
real camera is recorded in [`../VALIDATION.md`](../VALIDATION.md).

| ADR | Title | Status | Date |
|---|---|---|---|
| [0001](./0001-factory-restart-splice.md) | Factory-restart splice (Idle → Live) | superseded by 0003 | 2026-05-09 |
| [0002](./0002-rtsp-only-output-v1.md) | RTSP-only output for v1 | superseded by 0006 for HLS; DASH still out | 2026-05-09 |
| [0003](./0003-seamless-input-selector-splice.md) | Seamless `input-selector` splice in one persistent pipeline | accepted | 2026-07-12 |
| [0004](./0004-live-lost-feedback.md) | Live-loss feedback from the media adapter to the orchestrator | accepted | 2026-09-27 |
| [0005](./0005-user-views-observe-never-compete.md) | User views in the Arlo app: observe, never compete | accepted for its rules; superseded by 0007 for what the NVR shows | 2026-09-27 |
| [0006](./0006-hls-output-via-loopback-segmenter.md) | HLS output through a loopback segmenter; DASH stays out | accepted | 2026-09-28 |
| [0007](./0007-relay-the-users-app-view.md) | Relay the user's live view from the Arlo app, picture and sound | accepted | 2026-10-01 |
| [0008](./0008-video-encoder-selection.md) | Choosing the H.264 encoder per host (`auto`) | accepted | 2026-10-03 |

## Writing one

Copy the header block of the latest ADR (status, date, deciders,
supersedes, superseded by), then Context, Decision, Consequences
(positive, negative / risks), Alternatives considered. Facts from a live
capture carry their date. Link the ADR from the README's Architecture
section and add a row here.
