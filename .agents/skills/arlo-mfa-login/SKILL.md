---
name: arlo-mfa-login
description: First login and second-factor pairing of the streamer with the Arlo cloud — choosing the factor, the one interactive `list-devices` run, what the log must say, and the symptom→cause→fix table for a daemon that asks for a code again. Use on a new machine or container, after an Arlo password change, when a restart asks for an OTP, or when switching the second factor.
---
# Arlo MFA Login Skill

The daemon logs in once. arlo-rs keeps three things in `arlo.session_cache_path`
(`session.json`, mode 0600, parent directory created owner-only by the streamer): the
token, the cookie jar, and the paired **device id**. The token expires; the pairing is
what makes later logins silent (Arlo's "trusted browser": `getFactorId` with
`factorType: BROWSER` succeeds for a paired device id + cookies, and `startAuth` on it
returns a full token with no OTP). Deleting or corrupting the file costs one more OTP.

**The agent never runs the login.** The tool shell has no terminal, no second factor and
no mailbox; it prepares the commands and reads the log the owner pastes back. Facts below
come from `crates/streamer-infra-arlo/src/boot.rs` and arlo-rs `client/auth/{flow,push}.rs`,
`client/auth_imap.rs`, `client/mfa.rs` (read 2026-10-02) and were checked against a fresh
IMAP login on 2026-10-03 (13 s from cold start to `authentication complete`).

## Choosing the factor (`[arlo.mfa] kind`)

| `kind` | Headless | Needs | How the code arrives |
|---|---|---|---|
| `email` + `host`/`provider` + `user` + `password_env` | yes | an IMAP mailbox receiving Arlo's mails | arlo-rs polls the inbox: baseline of unseen mail (45 s budget), then every 5 s for 90 s; only mail whose `From` is an `arlo.com` address counts, newest first; the 6-digit code is taken from the `<h1>`, a bare digit line, or a loose 6-digit match. Gmail needs the mailbox session refreshed, which the code does. |
| `push` | yes | the Arlo app signed in on a phone, **push set as the account's primary factor** | `startAuth` with an empty factor type dispatches the primary factor; `finishAuth` is polled every `poll_interval_secs` (3) until approval or `timeout_secs` (120). |
| `email` without IMAP fields, `sms` | no | a terminal | `MFA challenge dispatched via EMAIL.` / `Enter the OTP:` on stdin. |

The streamer maps `kind` to arlo-rs's preferred factor (`EMAIL`, `SMS`, `PUSH`); a factor the
account does not have fails with `Preferred 2FA method '…' is not registered on the account`.

## Pre-flight

- `arlo.session_cache_path` is on a persistent volume (Docker: `/var/lib/arlo-streamer`,
  owned by uid 10001). A fresh container directory means a new pairing, hence an OTP.
- The env vars named in `arlo.password_env` and `arlo.mfa.password_env` are set in the
  shell that runs the command (`ARLO_PASSWORD`, `ARLO_IMAP_PASSWORD` by convention).
- IMAP: the host answers on port 993 (`openssl s_client -connect host:993 -quiet </dev/null`).
- Push: the Arlo app is signed in and push is the primary factor in the account settings.

## The login: one interactive `list-devices` run

It signs in, completes the pairing, prints the account's cameras, starts no server, needs no
admin token. Logs go to stderr, the report to stdout. Run it where the daemon will run:

```bash
export ARLO_PASSWORD='…' ARLO_IMAP_PASSWORD='…'
arlo-camera-streamer list-devices --config /etc/arlo-streamer/streamer.toml
```

```bash
distrobox enter rust-build -- bash -lc 'cd ~/workspace/arlo-camera-streamer/arlo-camera-streamer-rs && ARLO_PASSWORD="…" ./target/release/arlo-camera-streamer list-devices --config config/streamer.toml'
```

```bash
docker run --rm -it -e ARLO_PASSWORD -e ARLO_IMAP_PASSWORD \
  -v /srv/arlo-streamer/streamer.toml:/etc/arlo-streamer/streamer.toml:ro \
  -v arlo-streamer-state:/var/lib/arlo-streamer \
  <image> list-devices --config /etc/arlo-streamer/streamer.toml
```

`-it` matters for the stdin kinds only; keep the same volume for the daemon afterwards.

## What the log must say

| Path | Lines, in order |
|---|---|
| Cache valid | `arlo-rs session restored from cache` — nothing else; the daemon starts. |
| Cold start | `no valid cached session — running MFA cold-start` (or, for an expired token, `Cached token rejected by Arlo; re-authenticating`) → `Preparing MFA handler before OTP dispatch` → one of the rows below → `Browser paired with Arlo; future logins can skip the OTP` → `arlo-rs authentication complete`. First time on a machine: `created session cache directory` comes first. |
| Paired device | `Trusted browser accepted by Arlo — no OTP required`; no pairing line. |
| Not paired yet | `Browser not trusted by Arlo; running the OTP ceremony error=API Error [400/9261]: Invalid factor data` (9261 on a fresh device id; 9204 on a known but untrusted one), then the factor's own lines. |
| Email over IMAP | `Awaiting OTP from MFA handler provider=EMAIL`, ~10 s of silence at `info` (the inbox poll logs at `debug`: `Captured IMAP baseline`; a skipped unrelated mail logs `ignoring unseen mail: From address is not Arlo's` at `warn`), then the pairing line. |
| Push | `Awaiting push approval in the Arlo mobile app` → approve on the phone → the pairing line. |
| Stdin | `Awaiting OTP from MFA handler provider=…`, then the prompt on the terminal; type the code. |
| Transient | `Cached token could not be validated; keeping it for a retry`: network, 5xx or 429. The token is kept, no OTP is spent; retry later. |

**Verification:** `ls -l <session_cache_path>` shows `-rw-------`; a second `list-devices`
or the daemon's start logs `arlo-rs session restored from cache` and asks for nothing.

## While the daemon runs

The streamer authenticates at boot only. A session Arlo invalidates mid-run (password
change, session revoked in the app) is not renewed in place: Arlo calls start failing with
re-authentication errors until the daemon is restarted. The restart takes the trusted path
and needs no OTP unless the pairing was revoked too. How long Arlo keeps a pairing is not
documented; keep the cache file across upgrades and restarts.

## Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| A code on every restart | the cache file is not where the daemon looks, or its directory vanishes (ephemeral container storage) | mount a persistent volume at the directory of `session_cache_path`; compare the path in the log line `created session cache directory` with the mount |
| `session cache not written; the trusted-browser pairing will not survive a restart` | directory not writable by the daemon's user (uid 10001 in Docker) | fix ownership of the directory; the file is written by temp-file + rename inside it |
| `session cache is corrupt and will be replaced; the trusted-browser pairing is lost` | truncated or edited file | nothing to do; the next login re-pairs (one OTP) |
| `could not restrict session cache directory to owner-only` | filesystem without POSIX modes | harmless on a private volume |
| IMAP: no code within 90 s | mail not from `arlo.com`, wrong mailbox, provider blocks app passwords | check the mail's `From`, use an app password, prefer `host` over `provider` |
| `push approval not granted within 120s` | phone not reached, push not the primary factor | approve faster, raise `timeout_secs`, or set push as primary in the Arlo app |
| `Due to multiple attempts account is locked` (Arlo 9017) | too many login attempts | wait 5 minutes; never retry in a loop |
| `Preferred 2FA method … is not registered` | `kind` does not match the account's factors | add the factor in the Arlo app or change `kind` |
| Login works but the daemon later logs re-authentication errors | session invalidated by Arlo | restart the daemon |

## Safety

- Never paste an OTP, the session file, a cookie, or a password into the chat, an issue or
  a log; the owner types the code on their own terminal.
- `session.json` and `streamer.toml` never go into git; passwords live in env vars only.
- Never script retries of the login: Arlo locks the account for 5 minutes after repeated
  attempts, and each attempt may spend an OTP.
- No `get`/`startStream` probes while reasoning about login problems: they wake cameras.
