//! Per-camera state-machine orchestrator.
//!
//! One [`CameraOrchestrator`] runs as a single tokio task. It owns:
//!
//! - The camera's [`CameraState`] (the canonical authority).
//! - A [`MotionDebouncer`] for monotonic Live → Idle deadlines.
//! - A [`LiveBudgetTracker`] for daily wall-clock quota.
//! - `Arc<dyn Trait>` handles to the three Arlo + media ports.
//!
//! Inputs: a [`mpsc::Receiver<CameraEvent>`] fed by the [`EventRouter`].
//! Output: side-effects on the [`MediaMultiplexer`] and graceful
//! shutdown on cancellation.
//!
//! # Two-phase activation
//!
//! `Idle → Activating → Live` is asynchronous: the pure
//! [`transition`](crate::transition()) reducer flips the state to
//! `Activating`, then the orchestrator drives `attach_live` (which
//! owns the WebRTC offer/answer round-trip via the `WebrtcSignaler`)
//! and emits a follow-up `LiveAttached` signal that flips
//! to `Live`. If it fails the orchestrator emits
//! `Failure(reason)` instead, which puts us in `Failed { retries }`
//! with an exponential backoff timer.
//!
//! Side-effects happen in `apply_state_change` *after* the state is
//! committed; follow-up signals are queued so a single
//! `process_signals` loop drains them in FIFO order. This avoids
//! mid-flight state divergence between the recursive call returning
//! and the outer arm completing.
//!
//! # Live-loss feedback (ADR 0004)
//!
//! `attach_live` returns a [`LiveSession`] handle that resolves when
//! the media adapter detects the source died (no RTP, pipeline error,
//! end-of-stream, peer disconnected). The orchestrator holds it exactly
//! while the camera is `Live` (invariant enforced in
//! `process_signals`), awaits it as one `select!` arm, and turns a
//! resolution into `StateTransition::LiveLost`, which reuses the
//! ordinary `Live → Idle` exit (detach + teardown + thumbnail refresh).
//! Dropping the handle on every live exit is what makes late reports
//! from a finished session unobservable — no generation counters.
//!
//! # User views in the Arlo app (ADR 0005)
//!
//! Arlo serves one live transport per camera: while the user watches in
//! the mobile app (RTSP), our WebRTC session is refused (Arlo 14001) and
//! the app's stream cannot be joined. User views are therefore only
//! *observed*: `ManualStream` marks the camera as viewed for
//! `USER_VIEW_HOLD` (refreshed by every report), `ManualStreamEnded`
//! clears it, and meanwhile a motion pulse does not start a session — it
//! is recorded as `suppressed-user-view` instead of failing into backoff.
//! A session already running is left to end normally. A view the bus
//! did not report surfaces as the attach failing with `CameraBusy`, which
//! returns to `Idle` without backoff and marks the camera as viewed.
//!
//! [`EventRouter`]: crate::router::EventRouter

// `tokio::select!` arms with terminal `None` paths are clearer as
// `match` than as `let-else` because the surrounding macro already
// scopes the bindings. Keep the readability-over-clippy call here.
#![allow(clippy::single_match_else)]
// Many local bindings shadow others (`tx`/`rx`, `from`/`to`, `signal`)
// — the alternative would be longer names like `event_tx_sender` that
// add noise without clarity.
#![allow(clippy::similar_names)]

use std::collections::VecDeque;
use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::{Local, NaiveDateTime};
use tokio::select;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, instrument, trace, warn};

use streamer_domain::camera::CameraId;
use streamer_domain::config::CameraConfig;
use streamer_domain::error::DomainError;
use streamer_domain::event::CameraEvent;
use streamer_domain::metrics::{BudgetDecision, MotionOutcome, SpliceOutcome};
use streamer_domain::port::{
    ArloThumbnailSource, MediaMultiplexer, MetricsRecorder, UserViewSource, WebrtcSignaler,
};
use streamer_domain::state::{CameraState, LiveLossReason, LiveSource, StateTransition};
use streamer_domain::stream::LiveSession;

use crate::budget::{BudgetVerdict, LiveBudgetTracker};
use crate::debouncer::{DebouncerVerdict, MotionDebouncer};
#[cfg(test)]
use crate::metrics_noop::NoopRecorder;
use crate::transition::transition;

/// How long one `ManualStream` report keeps motion activations paused
/// when no `idle` report follows, so a lost MQTT event cannot pause
/// motion for good. Every report refreshes it; a view that outlives it
/// is caught again by the attach failing with `CameraBusy`.
const USER_VIEW_HOLD: Duration = Duration::from_secs(120);

/// After a relay of the user's view failed or was lost, how long the
/// repeated `userStreamActive` reports (one every snapshot, ~10 s) are
/// left alone before another relay is tried (ADR 0007).
const USER_VIEW_RETRY: Duration = Duration::from_secs(30);
/// After a probe released the stream, how long the camera gets to report
/// `idle` before the view is taken to go on and the relay resumes. The
/// report came 0.34 to 0.8 s after our `TEARDOWN` over four live gates
/// (2026-10-01/02); the grace is what viewers see as the gap.
const USER_VIEW_PROBE_GRACE: Duration = Duration::from_secs(2);

/// Per-camera state-machine task.
pub struct CameraOrchestrator {
    camera_id: CameraId,
    state: CameraState,
    debouncer: MotionDebouncer,
    budget: LiveBudgetTracker,
    /// Set when in [`CameraState::Failed`]; cleared on transition out.
    failed_deadline: Option<Instant>,
    /// Handle to the attached live session (ADR 0004).
    ///
    /// Invariant: `Some` iff `state` is `Live`. Set by
    /// `start_activation`, cleared in `process_signals` the moment the
    /// state leaves live, so a report from a finished session can never
    /// be observed.
    live: Option<LiveSession>,
    /// Until when the user is considered to be watching the camera in
    /// the Arlo app (ADR 0005); `None` when no view is known.
    user_view_until: Option<Instant>,
    /// Whether the idle frame currently shows the "live in the Arlo app"
    /// notice; kept in step with [`Self::user_view_active`] by
    /// [`Self::sync_user_view_notice`].
    user_view_notice: bool,
    /// What the current `Activating` / `Live` session shows (ADR 0007);
    /// `None` outside a session.
    session_source: Option<LiveSource>,
    /// Until when a failed or lost relay of the user's view is not
    /// retried, so the repeated view reports cannot hammer Arlo.
    user_view_retry_after: Option<Instant>,
    /// How long a relay of the user's view may run: our RTSP session
    /// keeps the camera streaming for Arlo's backend even after the app
    /// closes its view, so a relay is capped like a motion session
    /// (`max_continuous_live`).
    relay_cap: Duration,
    /// When the running relay releases the stream — the next probe, or
    /// the cap when probing is off; `Some` iff live with a
    /// [`LiveSource::UserView`] session.
    relay_deadline: Option<Instant>,
    /// How often a relay lets go of the stream to learn whether the app
    /// still views (`user_view_probe_secs`); `None` never probes.
    probe_interval: Option<Duration>,
    /// Until when, after a probe, the camera's `idle` report is awaited
    /// in `Idle`; `None` outside a probe.
    probe_until: Option<Instant>,
    signaler: Arc<dyn WebrtcSignaler>,
    thumbnails: Arc<dyn ArloThumbnailSource>,
    media: Arc<dyn MediaMultiplexer>,
    user_views: Arc<dyn UserViewSource>,
    metrics: Arc<dyn MetricsRecorder>,
    events: mpsc::Receiver<CameraEvent>,
    /// Inbound admin commands (e.g. force-idle, manual-wake). The
    /// admin actor in `crate::admin` enqueues here.
    admin: mpsc::Receiver<crate::admin::AdminCommand>,
    shutdown: CancellationToken,
}

impl CameraOrchestrator {
    /// Construct an orchestrator. Anchors the budget tracker at the
    /// current wall-clock so a same-day restart doesn't re-grant the
    /// quota.
    ///
    /// `metrics` is optional from the caller's perspective — pass
    /// [`Arc::new(NoopRecorder)`](crate::metrics_noop::NoopRecorder)
    /// in tests or when observability is disabled.
    ///
    /// `admin` is the inbound mailbox for
    /// [`AdminCommand`](crate::admin::AdminCommand)s dispatched by
    /// [`AdminControlActor`](crate::admin::AdminControlActor). Pass a
    /// never-resolving receiver if admin commands are not used.
    ///
    /// # Errors
    ///
    /// Returns [`DomainError::InvalidConfig`] when
    /// [`LiveBudgetTracker::new`] rejects the camera's `budget_reset`
    /// or `daily_live_budget` values.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        config: &CameraConfig,
        signaler: Arc<dyn WebrtcSignaler>,
        thumbnails: Arc<dyn ArloThumbnailSource>,
        media: Arc<dyn MediaMultiplexer>,
        user_views: Arc<dyn UserViewSource>,
        metrics: Arc<dyn MetricsRecorder>,
        events: mpsc::Receiver<CameraEvent>,
        admin: mpsc::Receiver<crate::admin::AdminCommand>,
        shutdown: CancellationToken,
    ) -> Result<Self, DomainError> {
        let now = Local::now().naive_local();
        let debouncer = MotionDebouncer::new(&config.cooldown);
        let budget = LiveBudgetTracker::new(&config.cooldown, now)?;
        Ok(Self {
            camera_id: config.arlo_device_id.clone(),
            state: CameraState::Idle,
            debouncer,
            budget,
            failed_deadline: None,
            live: None,
            user_view_until: None,
            user_view_notice: false,
            session_source: None,
            user_view_retry_after: None,
            relay_cap: Duration::from_secs(config.cooldown.max_continuous_live),
            relay_deadline: None,
            probe_interval: (config.cooldown.user_view_probe_secs > 0)
                .then(|| Duration::from_secs(config.cooldown.user_view_probe_secs)),
            probe_until: None,
            signaler,
            thumbnails,
            media,
            user_views,
            metrics,
            events,
            admin,
            shutdown,
        })
    }

    /// Convenience constructor for tests / minimal callers — wires a
    /// [`NoopRecorder`] and a closed admin mailbox.
    ///
    /// # Errors
    ///
    /// Forwarded from [`Self::new`].
    #[cfg(test)]
    pub fn new_minimal(
        config: &CameraConfig,
        signaler: Arc<dyn WebrtcSignaler>,
        thumbnails: Arc<dyn ArloThumbnailSource>,
        media: Arc<dyn MediaMultiplexer>,
        user_views: Arc<dyn UserViewSource>,
        events: mpsc::Receiver<CameraEvent>,
        shutdown: CancellationToken,
    ) -> Result<Self, DomainError> {
        let (_admin_tx, admin_rx) = mpsc::channel(1);
        Self::new(
            config,
            signaler,
            thumbnails,
            media,
            user_views,
            Arc::new(NoopRecorder),
            events,
            admin_rx,
            shutdown,
        )
    }

    /// Run the orchestrator until cancelled or the event channel closes.
    ///
    /// On entry: registers the camera with the [`MediaMultiplexer`] so
    /// the idle pipeline is up before any motion arrives.
    /// On exit: detaches any in-flight live source for clean shutdown.
    #[instrument(skip(self), fields(camera = %self.camera_id))]
    pub async fn run(mut self) {
        if let Err(e) = self.media.register(&self.camera_id).await {
            warn!(error = %e, "media register failed; continuing — pipeline may be missing");
        }
        // Prime the idle overlay with the camera's last snapshot so the
        // first STANDBY view already shows a thumbnail rather than the
        // black frame (which otherwise appears until the first live
        // session ends). Applied to the pipeline at `media-configure`.
        self.refresh_idle_thumbnail().await;

        // Tracks whether the admin mailbox is still alive. Once it
        // closes (e.g. tests that drop the sender immediately), we
        // skip the admin arm to avoid a busy `Some(None)` loop.
        let mut admin_open = true;

        loop {
            let deadline = self.next_deadline();
            let notice_expiry = self.user_view_notice_expiry();

            select! {
                biased;

                () = self.shutdown.cancelled() => {
                    info!("shutdown requested");
                    self.handle_shutdown().await;
                    return;
                }
                cmd = recv_admin(&mut self.admin, admin_open) => match cmd {
                    Some(cmd) => self.handle_admin(cmd).await,
                    None => {
                        debug!("admin channel closed; admin endpoints disabled");
                        admin_open = false;
                    }
                },
                reason = live_lost(self.live.as_mut()) => {
                    // Take the handle first: a resolved handle resolves
                    // again immediately (as `AdapterDropped`) and would
                    // busy-loop the select — same rule as `admin_open`.
                    self.live = None;
                    if let Err(e) = self.handle_live_lost(reason).await {
                        warn!(error = %e, "live-lost handling failed");
                    }
                }
                event = self.events.recv() => match event {
                    Some(event) => {
                        if let Err(e) = self.handle_event(event).await {
                            warn!(error = %e, "event handling failed");
                        }
                    }
                    None => {
                        warn!("event channel closed; exiting");
                        return;
                    }
                },
                () = maybe_sleep_until(deadline) => {
                    if let Err(e) = self.handle_deadline().await {
                        warn!(error = %e, "deadline handling failed");
                    }
                }
                // A user view whose `idle` report never came: its hold
                // runs out, so the notice must go.
                () = maybe_sleep_until(notice_expiry) => {
                    self.sync_user_view_notice().await;
                }
            }
        }
    }

    /// Apply an inbound admin command and reply on its oneshot.
    ///
    /// Errors during reply (e.g. caller dropped the future) are logged
    /// but never surfaced — admin requests are inherently best-effort.
    async fn handle_admin(&mut self, cmd: crate::admin::AdminCommand) {
        use crate::admin::AdminCommand;
        match cmd {
            AdminCommand::Snapshot { reply } => {
                let snap = self.snapshot();
                if reply.send(snap).is_err() {
                    debug!("admin snapshot reply dropped");
                }
            }
            // Mutating commands are acknowledged *before* they are
            // applied: the HTTP layer answers 202 Accepted, and a wake
            // spends several seconds in WebRTC negotiation — longer
            // than the admin reply timeout, which would report a
            // perfectly healthy wake as "orchestrator unavailable".
            AdminCommand::ForceIdle { reply } => {
                if reply.send(()).is_err() {
                    debug!("admin force-idle reply dropped");
                }
                if matches!(self.state, CameraState::Live | CameraState::Activating) {
                    let signals = VecDeque::from([StateTransition::CooldownExpired]);
                    if let Err(e) = self.process_signals(signals).await {
                        warn!(error = %e, "force-idle failed");
                    }
                }
            }
            AdminCommand::ManualWake { reply } => {
                if reply.send(()).is_err() {
                    debug!("admin manual-wake reply dropped");
                }
                // Same guards as a real pulse: absorbed during a manual
                // session, no stale debouncer priming in BatteryProtect /
                // Failed, budget checked.
                let Some(signal) = self.motion_signal("admin-wake") else {
                    return;
                };
                if let Err(e) = self.process_signals(VecDeque::from([signal])).await {
                    warn!(error = %e, "manual-wake failed");
                }
            }
        }
    }

    /// Build a [`CameraSnapshot`](streamer_domain::admin::CameraSnapshot)
    /// from the current orchestrator state.
    fn snapshot(&self) -> streamer_domain::admin::CameraSnapshot {
        use streamer_domain::admin::CameraSnapshot;
        let state_label = match &self.state {
            CameraState::Idle => "idle",
            CameraState::Activating => "activating",
            CameraState::Live => "live",
            CameraState::BatteryProtect { .. } => "battery-protect",
            CameraState::Failed { .. } => "failed",
        };
        let (last_failure, retries) = match &self.state {
            CameraState::Failed { reason, retries } => (Some(reason.clone()), *retries),
            _ => (None, 0),
        };
        CameraSnapshot {
            id: self.camera_id.clone(),
            stream_name: self.stream_name(),
            state: state_label.to_string(),
            live_secs_today: self.budget.spent_secs_today(),
            daily_budget_secs: self.budget.daily_budget_secs(),
            live_source: (self.state == CameraState::Live)
                .then_some(self.session_source)
                .flatten()
                .map(|s| s.as_label().to_string()),
            last_failure,
            retries,
            user_view: self.user_view_active(),
        }
    }

    /// Stream name is captured from config at construction; rebuilt
    /// here from `camera_id` because we don't currently store it.
    /// Kept as a separate function so the small workaround is visible.
    fn stream_name(&self) -> streamer_domain::camera::StreamName {
        // Best-effort fallback when the camera id is also a valid
        // stream name — otherwise the snapshot still serializes but
        // the stream name is left as the camera id. The orchestrator
        // does not strictly own the stream name today; future work can
        // pass it in via the config.
        streamer_domain::camera::StreamName::parse(self.camera_id.as_str()).unwrap_or_else(|_| {
            streamer_domain::camera::StreamName::parse("unknown")
                .expect("'unknown' is a valid stream name")
        })
    }

    fn next_deadline(&self) -> Option<Instant> {
        match &self.state {
            CameraState::Idle => self.probe_until,
            CameraState::Activating => None,
            CameraState::Live if self.session_source == Some(LiveSource::UserView) => {
                self.relay_deadline
            }
            CameraState::Live => self.debouncer.next_deadline(now()),
            CameraState::Failed { .. } => self.failed_deadline,
            CameraState::BatteryProtect { .. } => {
                let wall = Local::now().naive_local();
                let next_reset = self.budget.next_reset(wall);
                // Round *up*: firing before the wall-clock boundary would
                // poll the tracker on the old day and bounce straight
                // back into BatteryProtect.
                let delta_ms = (next_reset - wall).num_milliseconds().max(0);
                Some(now() + Duration::from_millis(u64::try_from(delta_ms).unwrap_or(0) + 1))
            }
        }
    }

    async fn handle_event(&mut self, event: CameraEvent) -> Result<(), DomainError> {
        let signal = match event {
            CameraEvent::Motion { .. } => {
                let Some(signal) = self.motion_signal("motion") else {
                    return Ok(());
                };
                signal
            }
            CameraEvent::Audio { .. } => {
                let Some(signal) = self.motion_signal("audio") else {
                    return Ok(());
                };
                signal
            }
            CameraEvent::ManualStream { .. } => {
                self.on_user_view_started();
                self.sync_user_view_notice().await;
                let Some(signal) = self.user_view_relay_signal() else {
                    return Ok(());
                };
                signal
            }
            CameraEvent::ManualStreamEnded { .. } => {
                self.on_user_view_ended();
                self.sync_user_view_notice().await;
                if self.session_source != Some(LiveSource::UserView) {
                    return Ok(());
                }
                StateTransition::UserViewEnded
            }
            CameraEvent::SnapshotAvailable { .. } => {
                self.on_snapshot_available().await;
                return Ok(());
            }
            // A camera back online while in `Failed` ends the backoff
            // early; otherwise the report changes nothing.
            CameraEvent::Online { .. } => {
                if matches!(self.state, CameraState::Failed { .. }) {
                    info!("camera back online; leaving the failure backoff early");
                    StateTransition::BackoffElapsed
                } else {
                    debug!("camera online");
                    return Ok(());
                }
            }
            CameraEvent::Offline { .. } => {
                StateTransition::Failure("camera went offline".to_string())
            }
        };
        self.process_signals(VecDeque::from([signal])).await
    }

    /// Turn a motion / audio pulse (or an admin wake) into a signal, or
    /// `None` when it must not touch the machine: while the user watches
    /// in the Arlo app, Arlo would refuse our session, so none is started
    /// — a session already running is still extended as usual.
    ///
    /// `trigger` names the source (`motion`, `audio`, `admin-wake`) for
    /// the debug line every pulse gets: Arlo repeats a motion pulse about
    /// every 10 s while motion lasts, and each one restarts the cooldown.
    fn motion_signal(&mut self, trigger: &'static str) -> Option<StateTransition> {
        let in_session = matches!(self.state, CameraState::Live);
        if self.user_view_active() && !in_session {
            debug!(
                trigger,
                "pulse while the user watches in the Arlo app; not activating"
            );
            self.metrics
                .record_motion(&self.camera_id, MotionOutcome::SuppressedUserView);
            return None;
        }
        // Prime the debouncer only where a session can start or extend;
        // a pulse in BatteryProtect / Failed would leave a stale
        // `live_since` that trips the hard cap right after the next attach.
        // A relay of the user's view has no cooldown: it ends with the
        // view, so a pulse during it must not start the debouncer.
        let relaying = self.session_source == Some(LiveSource::UserView);
        if (in_session && !relaying) || self.state == CameraState::Idle {
            self.debouncer.on_motion(now());
        }
        let signal = self.intercept_budget(StateTransition::MotionDetected);
        let outcome = classify_motion(&self.state, &signal);
        let at = now();
        let session_ends_in_ms = self
            .debouncer
            .next_deadline(at)
            .map(|deadline| deadline.saturating_duration_since(at).as_millis());
        debug!(
            trigger,
            outcome = outcome.as_label(),
            ?session_ends_in_ms,
            "trigger pulse"
        );
        self.metrics.record_motion(&self.camera_id, outcome);
        Some(signal)
    }

    /// Arlo took a fresh snapshot. Refresh the idle still now only when it
    /// is what clients see (idle, battery-protect, failed); during a
    /// session the end-of-session refresh picks the same snapshot up from
    /// the adapter's cache.
    async fn on_snapshot_available(&self) {
        if matches!(
            self.state,
            CameraState::Idle | CameraState::BatteryProtect { .. } | CameraState::Failed { .. }
        ) {
            debug!("new snapshot; refreshing the idle still");
            self.refresh_idle_thumbnail().await;
        } else {
            debug!(state = ?self.state, "new snapshot during a session; refreshed when it ends");
        }
    }

    fn user_view_active(&self) -> bool {
        self.user_view_until.is_some_and(|until| now() < until)
            || self.probe_until.is_some_and(|until| now() < until)
            || self.session_source == Some(LiveSource::UserView)
    }

    /// When the shown notice must be re-checked: the end of the hold.
    /// When the shown notice may stop being wanted by time alone. A relay
    /// session keeps it wanted until a transition re-syncs it, so no
    /// instant is returned then — a past instant would spin the loop.
    fn user_view_notice_expiry(&self) -> Option<Instant> {
        if !self.user_view_notice || self.session_source == Some(LiveSource::UserView) {
            return None;
        }
        self.user_view_until.max(self.probe_until)
    }

    /// Make the idle frame's notice match the user-view flag. Best-effort:
    /// a failure is logged and retried at the next change.
    async fn sync_user_view_notice(&mut self) {
        let wanted = self.user_view_active();
        if wanted == self.user_view_notice {
            return;
        }
        match self
            .media
            .set_user_view_notice(&self.camera_id, wanted)
            .await
        {
            Ok(()) => self.user_view_notice = wanted,
            Err(e) => warn!(error = %e, shown = wanted, "user-view notice not updated"),
        }
    }

    /// Mark the camera as viewed in the Arlo app, or refresh the mark.
    fn on_user_view_started(&mut self) {
        if !self.user_view_active() {
            info!(state = ?self.state, "user is watching in the Arlo app; motion activations paused");
        }
        self.user_view_until = Some(now() + USER_VIEW_HOLD);
    }

    /// The camera went idle. Ends a known user view; otherwise (after a
    /// motion recording or our own session) there is nothing to do.
    /// During our own session Arlo reports `idle` after every motion
    /// snapshot, about every 10 s, so that case stays at `trace`.
    fn on_user_view_ended(&mut self) {
        let probing = self.probe_until.take().is_some();
        if self.user_view_until.take().is_some() || probing {
            info!(
                after_probe = probing,
                "user view in the Arlo app ended; motion activations resumed"
            );
        } else if self.state == CameraState::Live {
            trace!("camera idle report during our session (after a motion snapshot)");
        } else {
            debug!(state = ?self.state, "camera idle report without a known user view");
        }
    }

    /// The media adapter reported the attached source dead. Route it
    /// through the reducer as `LiveLost`; the `Live → Idle` side effects
    /// (detach, teardown, thumbnail refresh) are the ordinary ones.
    async fn handle_live_lost(&mut self, reason: LiveLossReason) -> Result<(), DomainError> {
        warn!(
            reason = reason.as_label(),
            "live source lost; returning to idle"
        );
        if self.session_source == Some(LiveSource::UserView) {
            self.user_view_retry_after = Some(now() + USER_VIEW_RETRY);
        }
        self.process_signals(VecDeque::from([StateTransition::LiveLost(reason)]))
            .await
    }

    /// If the daily budget is exhausted, divert `MotionDetected` into
    /// `BudgetExhausted` so the orchestrator never tries to wake the
    /// camera in the first place. Side-effect: emits a budget-decision
    /// metric.
    fn intercept_budget(&mut self, signal: StateTransition) -> StateTransition {
        if !matches!(signal, StateTransition::MotionDetected) {
            return signal;
        }
        match self.budget.poll(Local::now().naive_local()) {
            BudgetVerdict::Exhausted => {
                self.metrics
                    .record_budget(&self.camera_id, BudgetDecision::Denied);
                StateTransition::BudgetExhausted
            }
            BudgetVerdict::Unlimited | BudgetVerdict::Available { .. } => {
                self.metrics
                    .record_budget(&self.camera_id, BudgetDecision::Granted);
                signal
            }
        }
    }

    /// Why a relay lets go of the stream: to probe for the view's end
    /// (the camera only reports `idle` once released), or because the
    /// cap is reached when probing is off.
    fn relay_release_signal(&self) -> StateTransition {
        if let Some(every) = self.probe_interval {
            info!(
                every_secs = every.as_secs(),
                grace_secs = USER_VIEW_PROBE_GRACE.as_secs(),
                "releasing the relayed view to learn whether the app still views"
            );
            StateTransition::UserViewProbe
        } else {
            info!(
                cap_secs = self.relay_cap.as_secs(),
                "relay of the user's view reached the hard cap; releasing the camera"
            );
            StateTransition::MaxLiveExceeded
        }
    }

    async fn handle_deadline(&mut self) -> Result<(), DomainError> {
        let signal = match &self.state {
            CameraState::Idle => {
                if self.probe_until.is_none_or(|at| now() < at) {
                    return Ok(());
                }
                self.probe_until = None;
                let Some(signal) = self.user_view_relay_signal() else {
                    return Ok(());
                };
                info!("no idle report after the probe; the app still views, relaying again");
                signal
            }
            CameraState::Live if self.session_source == Some(LiveSource::UserView) => {
                if self.relay_deadline.is_none_or(|at| now() < at) {
                    return Ok(());
                }
                self.relay_release_signal()
            }
            CameraState::Live => match self.debouncer.poll(now()) {
                DebouncerVerdict::DebounceExpired => StateTransition::CooldownExpired,
                DebouncerVerdict::MaxLiveExceeded => StateTransition::MaxLiveExceeded,
                DebouncerVerdict::KeepLive | DebouncerVerdict::Idle => return Ok(()),
            },
            CameraState::Failed { .. } => StateTransition::BackoffElapsed,
            CameraState::BatteryProtect { .. } => StateTransition::BudgetReset,
            CameraState::Activating => return Ok(()),
        };
        self.process_signals(VecDeque::from([signal])).await
    }

    async fn process_signals(
        &mut self,
        mut signals: VecDeque<StateTransition>,
    ) -> Result<(), DomainError> {
        while let Some(signal) = signals.pop_front() {
            let from = self.state.clone();
            let to = transition(&from, &signal);
            if to == from {
                debug!(?signal, ?from, "no-op transition");
                continue;
            }
            info!(?from, ?to, ?signal, "state transition");
            self.state = to.clone();
            // ADR 0004 invariant: the session handle lives exactly as
            // long as the camera is live. Dropping it here, before any
            // side effect awaits, makes a late report unobservable.
            if !matches!(to, CameraState::Live) {
                self.live = None;
                self.relay_deadline = None;
            }
            // Every entry into `Failed` arms the backoff, whatever the
            // previous state: an `Offline` report while idle used to enter
            // `Failed` with no deadline and stay there until a restart.
            if let CameraState::Failed { retries, .. } = &to {
                self.failed_deadline = Some(now() + backoff_duration(*retries));
            }
            self.probe_until = match (&to, &signal) {
                (CameraState::Idle, StateTransition::UserViewProbe) => {
                    Some(now() + USER_VIEW_PROBE_GRACE)
                }
                (CameraState::Idle, _) => self.probe_until,
                _ => None,
            };
            self.session_source = match (&to, &signal) {
                (CameraState::Activating, StateTransition::UserViewStarted) => {
                    Some(LiveSource::UserView)
                }
                (CameraState::Activating, _) => Some(LiveSource::Motion),
                (CameraState::Live, _) => self.session_source,
                _ => None,
            };
            self.metrics
                .record_state_change(&self.camera_id, &from, &to, signal_label(&signal));
            // Failures: increment retry counter on Failed entry.
            if let CameraState::Failed { retries, .. } = &to {
                self.metrics.record_failure(&self.camera_id, *retries);
            }
            // Budget reset: surface the reset decision for completeness.
            if matches!(signal, StateTransition::BudgetReset) {
                self.metrics
                    .record_budget(&self.camera_id, BudgetDecision::Reset);
            }
            let follow_ups = self.apply_state_change(&from, &to).await?;
            for f in follow_ups {
                signals.push_back(f);
            }
        }
        Ok(())
    }

    async fn apply_state_change(
        &mut self,
        from: &CameraState,
        to: &CameraState,
    ) -> Result<Vec<StateTransition>, DomainError> {
        let mut follow_ups = Vec::new();
        match (from, to) {
            // Cold-start activation: kick off the async stream request.
            (CameraState::Idle, CameraState::Activating) => {
                let follow = if self.session_source == Some(LiveSource::UserView) {
                    self.start_user_view_relay().await
                } else {
                    self.start_activation().await
                };
                follow_ups.extend(follow);
            }
            // Activation succeeded. A relay of the user's view has no
            // cooldown and costs no battery of ours: it is not debounced
            // and not charged to the daily budget.
            (CameraState::Activating, CameraState::Live) => {
                if self.session_source == Some(LiveSource::UserView) {
                    let segment = self.probe_interval.unwrap_or(self.relay_cap);
                    self.relay_deadline = Some(now() + segment);
                } else {
                    self.debouncer.on_live_attached(now());
                    self.budget.on_live_started(Local::now().naive_local());
                }
            }
            // Attach refused because the user watches in the Arlo app:
            // release whatever the attempt left behind, no backoff.
            (CameraState::Activating, CameraState::Idle) => {
                let _ = self.media.detach_live(&self.camera_id).await;
                self.stop_arlo_live().await;
                self.debouncer.on_idle();
            }
            // Cooldown / max-live / loss → idle, or budget exhausted
            // mid-session → battery-protect: release the session either way.
            (CameraState::Live, CameraState::Idle | CameraState::BatteryProtect { .. }) => {
                self.detach_and_refresh().await;
            }
            // Anywhere → Failed (transient error).
            (CameraState::Activating | CameraState::Live, CameraState::Failed { .. }) => {
                let _ = self.media.detach_live(&self.camera_id).await;
                self.stop_arlo_live().await;
                self.budget.on_live_ended(Local::now().naive_local());
                self.debouncer.on_idle();
            }
            // Failed → Idle (backoff elapsed; ready to retry).
            (CameraState::Failed { .. }, CameraState::Idle) => {
                self.failed_deadline = None;
            }
            // BatteryProtect → Idle (budget reset).
            (CameraState::BatteryProtect { .. }, CameraState::Idle) => {
                debug!("budget reset; ready to wake on motion");
            }
            // No-op transitions (already filtered by process_signals).
            _ => {}
        }
        Ok(follow_ups)
    }

    /// Bring up a fresh live session. The media adapter owns the
    /// WebRTC offer/answer round-trip (via `signaler`); the orchestrator
    /// only drives the splice and keeps teardown symmetric.
    async fn start_activation(&mut self) -> Vec<StateTransition> {
        let started = now();
        match self
            .media
            .attach_live(&self.camera_id, self.signaler.as_ref())
            .await
        {
            Ok(session) => {
                self.live = Some(session);
                self.metrics.record_splice(
                    &self.camera_id,
                    SpliceOutcome::Success,
                    elapsed_ms(started),
                );
                vec![StateTransition::LiveAttached]
            }
            Err(DomainError::CameraBusy(reason)) => {
                info!(%reason, "camera busy with a user view in the Arlo app; not activating");
                self.user_view_until = Some(now() + USER_VIEW_HOLD);
                self.sync_user_view_notice().await;
                vec![StateTransition::CameraBusy]
            }
            Err(e) => {
                let latency = elapsed_ms(started);
                warn!(error = %e, latency_ms = latency, "attach_live failed");
                self.metrics
                    .record_splice(&self.camera_id, SpliceOutcome::AttachFailed, latency);
                vec![StateTransition::Failure(e.to_string())]
            }
        }
    }

    /// Whether a report of the user's view should start a relay now: only
    /// from `Idle`, and not within [`USER_VIEW_RETRY`] of a failed or lost
    /// relay (the reports repeat about every 10 s while the view runs).
    fn user_view_relay_signal(&self) -> Option<StateTransition> {
        if self.state != CameraState::Idle {
            return None;
        }
        if self
            .user_view_retry_after
            .is_some_and(|until| now() < until)
        {
            debug!("user view reported again within the relay retry guard; not retrying yet");
            return None;
        }
        Some(StateTransition::UserViewStarted)
    }

    /// Relay the user's view in the Arlo app (ADR 0007): fetch its
    /// watch-along stream and attach it as the live source. A failure
    /// returns to idle without backoff and arms the retry guard; the idle
    /// frame keeps saying where the live picture is.
    async fn start_user_view_relay(&mut self) -> Vec<StateTransition> {
        let started = now();
        let attached = match self.user_views.watch_along_url(&self.camera_id).await {
            Ok(url) => self.media.attach_user_view(&self.camera_id, &url).await,
            Err(e) => Err(e),
        };
        match attached {
            Ok(session) => {
                self.live = Some(session);
                self.metrics.record_splice(
                    &self.camera_id,
                    SpliceOutcome::Success,
                    elapsed_ms(started),
                );
                info!("relaying the user's live view from the Arlo app");
                vec![StateTransition::LiveAttached]
            }
            Err(e) => {
                let latency = elapsed_ms(started);
                warn!(error = %e, latency_ms = latency, "user view not relayed; idle frame keeps the notice");
                self.metrics
                    .record_splice(&self.camera_id, SpliceOutcome::AttachFailed, latency);
                self.user_view_retry_after = Some(now() + USER_VIEW_RETRY);
                vec![StateTransition::UserViewUnavailable]
            }
        }
    }

    /// Detach the live source and refresh the idle thumbnail.
    /// Best-effort: thumbnail-fetch failures are logged but never
    /// propagated — the synthetic black-frame fallback in the media
    /// adapter handles the missing-image case.
    async fn detach_and_refresh(&mut self) {
        if let Err(e) = self.media.detach_live(&self.camera_id).await {
            warn!(error = %e, "detach_live failed");
        }
        self.stop_arlo_live().await;
        if self.session_source != Some(LiveSource::UserView) {
            self.budget.on_live_ended(Local::now().naive_local());
        }
        self.debouncer.on_idle();
        self.refresh_idle_thumbnail().await;
        self.sync_user_view_notice().await;
    }

    /// Fetch the camera's latest snapshot and hand it to the media
    /// adapter so the idle STANDBY screen shows it. Best-effort:
    /// failures are logged, never propagated — the synthetic
    /// black-frame fallback covers the missing-image case. Called both
    /// on startup (so the first idle view already shows a thumbnail)
    /// and after every live session ends.
    async fn refresh_idle_thumbnail(&self) {
        match self.thumbnails.last_thumbnail(&self.camera_id).await {
            Ok(Some(jpeg)) => {
                if let Err(e) = self.media.refresh_thumbnail(&self.camera_id, jpeg).await {
                    warn!(error = %e, "refresh_thumbnail failed");
                }
            }
            Ok(None) => debug!("no thumbnail available; idle frame stays synthetic"),
            Err(e) => warn!(error = %e, "thumbnail fetch failed"),
        }
    }

    async fn handle_shutdown(&mut self) {
        // The task ends here; drop the session handle so a report that
        // races the shutdown is never acted upon.
        self.live = None;
        if matches!(self.state, CameraState::Live) {
            if let Err(e) = self.media.detach_live(&self.camera_id).await {
                warn!(error = %e, "detach on shutdown failed");
            }
            self.budget.on_live_ended(Local::now().naive_local());
        }
        // Always release any upstream session (also covers `Activating`
        // mid-negotiation) — idempotent; a leaked WebRTC session keeps
        // the camera streaming and drains its battery.
        self.stop_arlo_live().await;
    }

    /// Best-effort release of the upstream Arlo signaling session.
    /// Paired with every `media.detach_live` so no live exit — cooldown,
    /// max-live, failure, or shutdown — can leave a WebRTC session
    /// draining the camera battery.
    async fn stop_arlo_live(&self) {
        if let Err(e) = self.signaler.teardown(&self.camera_id).await {
            warn!(error = %e, "teardown failed (upstream session may linger)");
        }
    }
}

/// The orchestrator's monotonic clock. Taken from tokio so that a paused
/// test clock (`start_paused`) drives every deadline, guard and debounce
/// computation consistently; in production it is the plain monotonic
/// clock.
fn now() -> Instant {
    tokio::time::Instant::now().into_std()
}

/// Sleep until `deadline` or block forever when `None`. Used inside
/// `tokio::select!` so the deadline arm can be short-circuited when
/// the orchestrator has nothing to wait for. Converts the
/// [`std::time::Instant`] used by the debouncer into the
/// [`tokio::time::Instant`] required by [`tokio::time::sleep_until`]
/// so a paused tokio clock (in tests) drives the wake-up.
async fn maybe_sleep_until(deadline: Option<Instant>) {
    match deadline {
        Some(d) => tokio::time::sleep_until(tokio::time::Instant::from_std(d)).await,
        None => std::future::pending::<()>().await,
    }
}

/// Await the live session's loss report, or block forever when there is
/// no session — so the `select!` arm never fires (and never busy-loops)
/// in `Idle` / `Activating` / `BatteryProtect` / `Failed`.
async fn live_lost(session: Option<&mut LiveSession>) -> LiveLossReason {
    match session {
        Some(s) => s.lost().await,
        None => std::future::pending::<LiveLossReason>().await,
    }
}

/// `mpsc::Receiver::recv()` if the channel is still considered open;
/// otherwise block forever. Lets the `tokio::select!` admin arm be
/// disabled at runtime by flipping `open` to `false` once the receiver
/// has reported `None`.
async fn recv_admin(
    rx: &mut mpsc::Receiver<crate::admin::AdminCommand>,
    open: bool,
) -> Option<crate::admin::AdminCommand> {
    if open {
        rx.recv().await
    } else {
        std::future::pending::<Option<crate::admin::AdminCommand>>().await
    }
}

/// Convert a [`StateTransition`] to a stable kebab-case label for
/// metrics. Short names keep Prometheus label cardinality bounded.
fn signal_label(s: &StateTransition) -> &'static str {
    match s {
        StateTransition::MotionDetected => "motion-detected",
        StateTransition::LiveAttached => "live-attached",
        StateTransition::LiveReady => "live-ready",
        StateTransition::CooldownExpired => "cooldown-expired",
        StateTransition::MaxLiveExceeded => "max-live-exceeded",
        StateTransition::BudgetExhausted => "budget-exhausted",
        StateTransition::BudgetReset => "budget-reset",
        StateTransition::Failure(_) => "failure",
        StateTransition::BackoffElapsed => "backoff-elapsed",
        StateTransition::LiveLost(reason) => reason.signal_label(),
        StateTransition::CameraBusy => "camera-busy",
        StateTransition::UserViewStarted => "user-view-started",
        StateTransition::UserViewEnded => "user-view-ended",
        StateTransition::UserViewUnavailable => "user-view-unavailable",
        StateTransition::UserViewProbe => "user-view-probe",
    }
}

/// Classify a fresh motion event given the *current* state and the
/// computed signal so the recorder gets a meaningful outcome:
/// - in `Failed`: suppressed
/// - signal diverted to `BudgetExhausted`: budget-exhausted
/// - already live: absorbed (debouncer handles it)
/// - else: triggered
fn classify_motion(state: &CameraState, signal: &StateTransition) -> MotionOutcome {
    if matches!(signal, StateTransition::BudgetExhausted) {
        return MotionOutcome::BudgetExhausted;
    }
    match state {
        CameraState::Failed { .. } => MotionOutcome::SuppressedFailed,
        CameraState::Live => MotionOutcome::Absorbed,
        _ => MotionOutcome::Triggered,
    }
}

/// Wall-clock milliseconds elapsed since `start`. Saturates to
/// `u64::MAX` instead of panicking on overflow.
fn elapsed_ms(start: Instant) -> u64 {
    u64::try_from(start.elapsed().as_millis()).unwrap_or(u64::MAX)
}

/// Exponential backoff capped at 60 s.
fn backoff_duration(retries: u32) -> Duration {
    let secs = match retries {
        0 => 1,
        1 => 5,
        2 => 30,
        _ => 60,
    };
    Duration::from_secs(secs)
}

/// Convenience: wall-clock now used by callers.
#[must_use]
pub fn now_local() -> NaiveDateTime {
    Local::now().naive_local()
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use bytes::Bytes;
    use futures::stream::BoxStream;
    use streamer_domain::config::CooldownConfig;
    use streamer_domain::event::ConnectionStatus;
    use streamer_domain::port::ArloEventSource;
    use streamer_domain::stream::SignalingAnswer;
    use tokio::sync::Mutex;

    // ---------- Hand-written test doubles ----------
    //
    // We don't use mockall here because we need fine-grained control
    // over which port returns which value across multiple awaits, and
    // mockall's expectation API gets noisy fast for state machines.
    // Hand-written doubles keep tests readable.

    #[derive(Default)]
    struct StubSignaler {
        responses: Mutex<VecDeque<Result<SignalingAnswer, DomainError>>>,
        calls: Mutex<u32>,
        stops: Mutex<u32>,
    }

    impl StubSignaler {
        async fn push_response(&self, r: Result<SignalingAnswer, DomainError>) {
            self.responses.lock().await.push_back(r);
        }
        fn with_responses(rs: Vec<Result<SignalingAnswer, DomainError>>) -> Arc<Self> {
            Arc::new(Self {
                responses: Mutex::new(VecDeque::from(rs)),
                calls: Mutex::new(0),
                stops: Mutex::new(0),
            })
        }
        /// Number of `negotiate` calls.
        async fn call_count(&self) -> u32 {
            *self.calls.lock().await
        }
        /// Number of `teardown` calls.
        async fn stop_count(&self) -> u32 {
            *self.stops.lock().await
        }
    }

    #[async_trait]
    impl WebrtcSignaler for StubSignaler {
        async fn negotiate(
            &self,
            _camera: &CameraId,
            offer: &mut dyn streamer_domain::port::OfferBuilder,
        ) -> Result<SignalingAnswer, DomainError> {
            *self.calls.lock().await += 1;
            offer.build_offer(&[]).await?;
            self.responses
                .lock()
                .await
                .pop_front()
                .unwrap_or_else(|| Ok(ok_answer()))
        }
        async fn teardown(&self, _camera: &CameraId) -> Result<(), DomainError> {
            *self.stops.lock().await += 1;
            Ok(())
        }
    }

    /// The media half of a negotiation, for doubles that need no SDP.
    struct StaticOffer;

    #[async_trait]
    impl streamer_domain::port::OfferBuilder for StaticOffer {
        async fn build_offer(
            &mut self,
            _ice_servers: &[streamer_domain::stream::IceServer],
        ) -> Result<String, DomainError> {
            Ok("offer".to_string())
        }
    }

    #[derive(Default)]
    struct StubThumbnails;

    #[async_trait]
    impl ArloThumbnailSource for StubThumbnails {
        async fn last_thumbnail(&self, _camera: &CameraId) -> Result<Option<Bytes>, DomainError> {
            Ok(None)
        }
    }

    #[derive(Default)]
    struct RecordingMedia {
        events: Mutex<Vec<MediaCall>>,
        /// One notifier per `attach_live`, in order. Retained on purpose:
        /// a double that dropped it would resolve the session as
        /// `AdapterDropped` and flip the orchestrator to Idle at once.
        notifiers: Mutex<Vec<streamer_domain::stream::LiveLossNotifier>>,
        /// When `false`, notifiers are dropped immediately — simulates an
        /// adapter that vanishes under a live session.
        retain_notifiers: std::sync::atomic::AtomicBool,
        /// `set_user_view_notice` calls, in order.
        notices: Mutex<Vec<bool>>,
    }

    #[derive(Debug, PartialEq, Eq)]
    enum MediaCall {
        Register,
        AttachLive(String),
        AttachUserView,
        DetachLive,
        RefreshThumbnail,
    }

    /// `UserViewSource` double: scripted answers, calls counted.
    struct StubUserViews {
        responses: Mutex<VecDeque<Result<streamer_domain::stream::WatchAlongUrl, DomainError>>>,
        calls: Mutex<u32>,
    }

    impl StubUserViews {
        fn with_responses(
            responses: Vec<Result<streamer_domain::stream::WatchAlongUrl, DomainError>>,
        ) -> Arc<Self> {
            Arc::new(Self {
                responses: Mutex::new(responses.into()),
                calls: Mutex::new(0),
            })
        }
        /// Always hands out a relayable URL.
        fn always() -> Arc<Self> {
            Self::with_responses(vec![])
        }
        async fn call_count(&self) -> u32 {
            *self.calls.lock().await
        }
    }

    fn view_url() -> streamer_domain::stream::WatchAlongUrl {
        streamer_domain::stream::WatchAlongUrl::parse("rtsps://1.2.3.4:443/live/x?t=1").unwrap()
    }

    #[async_trait]
    impl UserViewSource for StubUserViews {
        async fn watch_along_url(
            &self,
            _camera: &CameraId,
        ) -> Result<streamer_domain::stream::WatchAlongUrl, DomainError> {
            *self.calls.lock().await += 1;
            self.responses
                .lock()
                .await
                .pop_front()
                .unwrap_or_else(|| Ok(view_url()))
        }
    }

    impl RecordingMedia {
        fn new() -> Arc<Self> {
            let m = Self::default();
            m.retain_notifiers
                .store(true, std::sync::atomic::Ordering::Relaxed);
            Arc::new(m)
        }
        fn dropping_notifiers() -> Arc<Self> {
            Arc::new(Self::default())
        }
        async fn calls(&self) -> Vec<MediaCall> {
            std::mem::take(&mut *self.events.lock().await)
        }
        async fn notices(&self) -> Vec<bool> {
            self.notices.lock().await.clone()
        }
        /// Report the `idx`-th session (0-based, in attach order) lost.
        /// Returns what the notifier returned (`false` = unobservable).
        async fn fail_live(&self, idx: usize, reason: LiveLossReason) -> bool {
            let notifiers = self.notifiers.lock().await;
            match notifiers.get(idx) {
                Some(n) => n.notify(reason),
                None => panic!("no session #{idx} attached"),
            }
        }
    }

    #[async_trait]
    impl MediaMultiplexer for RecordingMedia {
        async fn register(&self, _camera: &CameraId) -> Result<(), DomainError> {
            self.events.lock().await.push(MediaCall::Register);
            Ok(())
        }
        async fn attach_live(
            &self,
            camera: &CameraId,
            signaler: &dyn WebrtcSignaler,
        ) -> Result<LiveSession, DomainError> {
            // Mirror the real adapter: the media leg owns the WebRTC
            // offer/answer round-trip. Surfacing the negotiate error as
            // an attach failure is exactly the production contract.
            let answer = signaler.negotiate(camera, &mut StaticOffer).await?;
            self.events
                .lock()
                .await
                .push(MediaCall::AttachLive(answer.session_id));
            let (session, notifier) = LiveSession::new();
            if self
                .retain_notifiers
                .load(std::sync::atomic::Ordering::Relaxed)
            {
                self.notifiers.lock().await.push(notifier);
            }
            Ok(session)
        }
        async fn detach_live(&self, _camera: &CameraId) -> Result<(), DomainError> {
            self.events.lock().await.push(MediaCall::DetachLive);
            Ok(())
        }
        async fn refresh_thumbnail(
            &self,
            _camera: &CameraId,
            _jpeg: Bytes,
        ) -> Result<(), DomainError> {
            self.events.lock().await.push(MediaCall::RefreshThumbnail);
            Ok(())
        }
        async fn set_user_view_notice(
            &self,
            _camera: &CameraId,
            shown: bool,
        ) -> Result<(), DomainError> {
            self.notices.lock().await.push(shown);
            Ok(())
        }
        async fn attach_user_view(
            &self,
            _camera: &CameraId,
            _url: &streamer_domain::stream::WatchAlongUrl,
        ) -> Result<LiveSession, DomainError> {
            self.events.lock().await.push(MediaCall::AttachUserView);
            let (session, notifier) = LiveSession::new();
            if self
                .retain_notifiers
                .load(std::sync::atomic::Ordering::Relaxed)
            {
                self.notifiers.lock().await.push(notifier);
            }
            Ok(session)
        }
    }

    /// Unused but required by some test scaffolding.
    #[allow(dead_code)]
    struct StubEventSource;

    #[async_trait]
    impl ArloEventSource for StubEventSource {
        async fn subscribe(&self) -> Result<BoxStream<'static, CameraEvent>, DomainError> {
            Ok(Box::pin(futures::stream::empty()))
        }
        async fn connection_status(
            &self,
        ) -> Result<BoxStream<'static, ConnectionStatus>, DomainError> {
            Ok(Box::pin(futures::stream::empty()))
        }
    }

    fn camera_cfg(debounce: u64, max: u64) -> CameraConfig {
        camera_cfg_probing(debounce, max, 0)
    }

    fn camera_cfg_probing(debounce: u64, max: u64, probe: u64) -> CameraConfig {
        CameraConfig {
            arlo_device_id: CameraId::new("CAM"),
            stream_name: streamer_domain::camera::StreamName::parse("cam").unwrap(),
            codec_hint: None,
            cooldown: CooldownConfig {
                debounce_secs: debounce,
                max_continuous_live: max,
                daily_live_budget: 0,
                budget_reset: "00:00".to_string(),
                user_view_probe_secs: probe,
            },
        }
    }

    fn build(
        cfg: &CameraConfig,
        sr: Arc<dyn WebrtcSignaler>,
        media: Arc<dyn MediaMultiplexer>,
    ) -> (
        CameraOrchestrator,
        mpsc::Sender<CameraEvent>,
        CancellationToken,
    ) {
        build_with_views(cfg, sr, media, StubUserViews::always())
    }

    fn build_with_views(
        cfg: &CameraConfig,
        sr: Arc<dyn WebrtcSignaler>,
        media: Arc<dyn MediaMultiplexer>,
        views: Arc<dyn UserViewSource>,
    ) -> (
        CameraOrchestrator,
        mpsc::Sender<CameraEvent>,
        CancellationToken,
    ) {
        let (tx, rx) = mpsc::channel(32);
        let token = CancellationToken::new();
        let orch = CameraOrchestrator::new_minimal(
            cfg,
            sr,
            Arc::new(StubThumbnails),
            media,
            views,
            rx,
            token.clone(),
        )
        .expect("budget config valid");
        (orch, tx, token)
    }

    fn ok_answer() -> SignalingAnswer {
        SignalingAnswer {
            answer_sdp: "v=0\r\n".to_string(),
            session_id: "sess-test".to_string(),
        }
    }

    // ---------- Happy path ----------

    #[tokio::test(start_paused = true)]
    async fn motion_drives_idle_to_live_to_idle() {
        let cfg = camera_cfg(2, 60);
        let sr = StubSignaler::with_responses(vec![Ok(ok_answer())]);
        let media = RecordingMedia::new();
        let (orch, tx, token) = build(&cfg, sr.clone(), media.clone());

        let handle = tokio::spawn(orch.run());

        tx.send(CameraEvent::Motion {
            device_id: CameraId::new("CAM"),
        })
        .await
        .unwrap();

        // Yield enough times for activation to run + state to settle.
        tokio::time::sleep(Duration::from_millis(10)).await;
        // Still live: the session handle must not have resolved on its
        // own (guards the "doubles retain the notifier" rule).
        let before = media.calls().await;
        assert!(before.iter().any(|c| matches!(c, MediaCall::AttachLive(_))));
        assert!(!before.contains(&MediaCall::DetachLive));

        // Advance past the debounce deadline (debounce=2s).
        tokio::time::advance(Duration::from_secs(3)).await;
        tokio::time::sleep(Duration::from_millis(10)).await;

        token.cancel();
        handle.await.unwrap();

        let calls = media.calls().await;
        assert!(before.contains(&MediaCall::Register));
        assert!(calls.contains(&MediaCall::DetachLive));
        // Teardown is symmetric: every detach pairs with a teardown.
        assert!(
            sr.stop_count().await >= 1,
            "teardown must pair with detach on the Live→Idle exit"
        );
        assert_eq!(sr.call_count().await, 1);
    }

    // ---------- Live-loss feedback (ADR 0004) ----------

    async fn drive_to_live(tx: &mpsc::Sender<CameraEvent>) {
        tx.send(CameraEvent::Motion {
            device_id: CameraId::new("CAM"),
        })
        .await
        .unwrap();
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    #[tokio::test(start_paused = true)]
    async fn live_lost_in_live_detaches_and_tears_down_before_debounce() {
        // debounce=60s: without the feedback path the output would stay
        // on the dead pad for a minute.
        let cfg = camera_cfg(60, 300);
        let sr = StubSignaler::with_responses(vec![Ok(ok_answer())]);
        let media = RecordingMedia::new();
        let (orch, tx, token) = build(&cfg, sr.clone(), media.clone());
        let handle = tokio::spawn(orch.run());

        drive_to_live(&tx).await;
        assert!(!media.calls().await.contains(&MediaCall::DetachLive));

        tokio::time::advance(Duration::from_secs(1)).await;
        assert!(media.fail_live(0, LiveLossReason::RtpStalled).await);
        tokio::time::sleep(Duration::from_millis(10)).await;

        // `StubThumbnails` yields no image, so the idle refresh after the
        // detach makes no media call; the detach itself is the proof.
        let calls = media.calls().await;
        assert_eq!(
            calls,
            vec![MediaCall::DetachLive],
            "loss must detach well before the debounce"
        );
        assert_eq!(
            sr.stop_count().await,
            1,
            "teardown pairs with the loss exit"
        );

        token.cancel();
        handle.await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn live_lost_after_cooldown_exit_is_unobservable() {
        let cfg = camera_cfg(2, 60);
        let sr = StubSignaler::with_responses(vec![Ok(ok_answer())]);
        let media = RecordingMedia::new();
        let (orch, tx, token) = build(&cfg, sr.clone(), media.clone());
        let handle = tokio::spawn(orch.run());

        drive_to_live(&tx).await;
        tokio::time::advance(Duration::from_secs(3)).await;
        tokio::time::sleep(Duration::from_millis(10)).await;
        let calls = media.calls().await;
        assert_eq!(
            calls
                .iter()
                .filter(|c| **c == MediaCall::DetachLive)
                .count(),
            1
        );

        // The orchestrator dropped the handle on the cooldown exit: the
        // late report has nowhere to go and nothing else happens.
        assert!(!media.fail_live(0, LiveLossReason::EndOfStream).await);
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(media.calls().await.is_empty());
        assert_eq!(sr.stop_count().await, 1);

        token.cancel();
        handle.await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn stale_loss_does_not_kill_the_next_session() {
        let cfg = camera_cfg(2, 60);
        let sr = StubSignaler::with_responses(vec![Ok(ok_answer()), Ok(ok_answer())]);
        let media = RecordingMedia::new();
        let (orch, tx, token) = build(&cfg, sr.clone(), media.clone());
        let handle = tokio::spawn(orch.run());

        // Session 1 → cooldown → Idle.
        drive_to_live(&tx).await;
        tokio::time::advance(Duration::from_secs(3)).await;
        tokio::time::sleep(Duration::from_millis(10)).await;
        // Session 2.
        drive_to_live(&tx).await;
        media.calls().await; // drain

        assert!(!media.fail_live(0, LiveLossReason::PeerDisconnected).await);
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(
            media.calls().await.is_empty(),
            "session 1's notifier must not touch session 2"
        );

        assert!(media.fail_live(1, LiveLossReason::PeerDisconnected).await);
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(media.calls().await.contains(&MediaCall::DetachLive));
        assert_eq!(sr.stop_count().await, 2);

        token.cancel();
        handle.await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn adapter_dropping_its_notifier_returns_camera_to_idle() {
        let cfg = camera_cfg(60, 300);
        let sr = StubSignaler::with_responses(vec![Ok(ok_answer())]);
        let media = RecordingMedia::dropping_notifiers();
        let (orch, tx, token) = build(&cfg, sr.clone(), media.clone());
        let handle = tokio::spawn(orch.run());

        drive_to_live(&tx).await;
        tokio::time::sleep(Duration::from_millis(10)).await;

        // Fail closed: no report, but the notifier is gone → Idle.
        let calls = media.calls().await;
        assert!(calls.contains(&MediaCall::DetachLive));
        assert_eq!(sr.stop_count().await, 1);

        token.cancel();
        handle.await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn idle_orchestrator_makes_no_media_calls_over_a_long_wait() {
        // The live-lost arm must be pending, not firing, when idle.
        let cfg = camera_cfg(60, 300);
        let sr = StubSignaler::with_responses(vec![]);
        let media = RecordingMedia::new();
        let (orch, _tx, token) = build(&cfg, sr, media.clone());
        let handle = tokio::spawn(orch.run());

        tokio::time::sleep(Duration::from_millis(10)).await;
        tokio::time::advance(Duration::from_hours(6)).await;
        tokio::time::sleep(Duration::from_millis(10)).await;

        token.cancel();
        handle.await.unwrap();
        assert_eq!(media.calls().await, vec![MediaCall::Register]);
    }

    #[test]
    fn signal_label_live_lost_carries_the_reason() {
        assert_eq!(
            signal_label(&StateTransition::LiveLost(LiveLossReason::RtpStalled)),
            "live-lost-rtp-stalled"
        );
    }

    // ---------- Snapshots → idle still ----------

    /// Thumbnail source that always has an image and counts its calls.
    #[derive(Default)]
    struct CountingThumbnails {
        calls: std::sync::atomic::AtomicU32,
    }

    impl CountingThumbnails {
        fn count(&self) -> u32 {
            self.calls.load(std::sync::atomic::Ordering::Relaxed)
        }
    }

    #[async_trait]
    impl ArloThumbnailSource for CountingThumbnails {
        async fn last_thumbnail(&self, _camera: &CameraId) -> Result<Option<Bytes>, DomainError> {
            self.calls
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            Ok(Some(Bytes::from_static(b"jpeg")))
        }
    }

    fn snapshot() -> CameraEvent {
        CameraEvent::SnapshotAvailable {
            device_id: CameraId::new("CAM"),
        }
    }

    #[tokio::test(start_paused = true)]
    async fn snapshot_refreshes_the_idle_still_only_when_it_is_visible() {
        let cfg = camera_cfg(2, 300);
        let sr = StubSignaler::with_responses(vec![Ok(ok_answer())]);
        let media = RecordingMedia::new();
        let thumbs = Arc::new(CountingThumbnails::default());
        let (tx, rx) = mpsc::channel(32);
        let token = CancellationToken::new();
        let orch = CameraOrchestrator::new_minimal(
            &cfg,
            sr,
            thumbs.clone(),
            media.clone(),
            StubUserViews::always(),
            rx,
            token.clone(),
        )
        .unwrap();
        let handle = tokio::spawn(orch.run());
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert_eq!(thumbs.count(), 1, "startup primes the idle still");

        // Idle: a new snapshot refreshes the still at once.
        send(&tx, snapshot()).await;
        assert_eq!(thumbs.count(), 2);
        assert_eq!(
            media
                .calls()
                .await
                .iter()
                .filter(|c| **c == MediaCall::RefreshThumbnail)
                .count(),
            2
        );

        // Live: skipped, the session end refreshes instead.
        send(&tx, motion()).await;
        send(&tx, snapshot()).await;
        assert_eq!(
            thumbs.count(),
            2,
            "no refresh while the live video is on screen"
        );
        tokio::time::advance(Duration::from_secs(3)).await;
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert_eq!(thumbs.count(), 3, "the session end refreshes the still");

        token.cancel();
        handle.await.unwrap();
    }

    // ---------- User views in the Arlo app (ADR 0005) ----------

    async fn send(tx: &mpsc::Sender<CameraEvent>, event: CameraEvent) {
        tx.send(event).await.unwrap();
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    fn manual() -> CameraEvent {
        CameraEvent::ManualStream {
            device_id: CameraId::new("CAM"),
        }
    }

    fn manual_ended() -> CameraEvent {
        CameraEvent::ManualStreamEnded {
            device_id: CameraId::new("CAM"),
        }
    }

    fn motion() -> CameraEvent {
        CameraEvent::Motion {
            device_id: CameraId::new("CAM"),
        }
    }

    fn offline() -> CameraEvent {
        CameraEvent::Offline {
            device_id: CameraId::new("CAM"),
        }
    }

    fn online() -> CameraEvent {
        CameraEvent::Online {
            device_id: CameraId::new("CAM"),
        }
    }

    fn busy() -> DomainError {
        DomainError::CameraBusy("RTSP Streaming in progress".to_string())
    }

    #[tokio::test(start_paused = true)]
    async fn user_view_pauses_motion_activation_until_it_ends() {
        let cfg = camera_cfg(60, 300);
        let sr = StubSignaler::with_responses(vec![Ok(ok_answer())]);
        let media = RecordingMedia::new();
        let (orch, tx, token) = build(&cfg, sr.clone(), media.clone());
        let handle = tokio::spawn(orch.run());

        send(&tx, manual()).await;
        send(&tx, motion()).await;
        assert_eq!(
            sr.call_count().await,
            0,
            "no WebRTC attach while the user watches"
        );
        assert_eq!(
            media.calls().await,
            vec![MediaCall::Register, MediaCall::AttachUserView],
            "the view is relayed instead"
        );

        send(&tx, manual_ended()).await;
        send(&tx, motion()).await;
        assert_eq!(
            sr.call_count().await,
            1,
            "motion activates once the view ended"
        );

        token.cancel();
        handle.await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn user_view_never_triggers_a_webrtc_attach() {
        let cfg = camera_cfg(60, 300);
        let sr = StubSignaler::with_responses(vec![]);
        let media = RecordingMedia::new();
        let (orch, tx, token) = build(&cfg, sr.clone(), media.clone());
        let handle = tokio::spawn(orch.run());

        send(&tx, manual()).await;
        send(&tx, manual_ended()).await;
        assert_eq!(sr.call_count().await, 0, "Arlo would refuse it (14001)");
        assert_eq!(
            media.calls().await,
            vec![
                MediaCall::Register,
                MediaCall::AttachUserView,
                MediaCall::DetachLive
            ]
        );

        token.cancel();
        handle.await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn user_view_leaves_a_running_motion_session_alone_then_blocks_the_next() {
        let cfg = camera_cfg(2, 300);
        let sr = StubSignaler::with_responses(vec![Ok(ok_answer()), Ok(ok_answer())]);
        let media = RecordingMedia::new();
        let (orch, tx, token) = build(&cfg, sr.clone(), media.clone());
        let handle = tokio::spawn(orch.run());

        send(&tx, motion()).await;
        media.calls().await;
        send(&tx, manual()).await;
        assert!(
            media.calls().await.is_empty(),
            "a user view must not end our running session"
        );
        // The session ends on its own debounce …
        tokio::time::advance(Duration::from_secs(3)).await;
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(media.calls().await.contains(&MediaCall::DetachLive));
        // … and while the user still watches, motion does not re-activate.
        send(&tx, motion()).await;
        assert_eq!(sr.call_count().await, 1);

        token.cancel();
        handle.await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn user_view_hold_expires_without_an_idle_report() {
        // The view cannot be relayed here (no stream handed out), so the
        // camera stays idle with the notice; the hold alone must not pause
        // motion for good.
        let cfg = camera_cfg(60, 300);
        let sr = StubSignaler::with_responses(vec![Ok(ok_answer())]);
        let media = RecordingMedia::new();
        let views = StubUserViews::with_responses(vec![Err(DomainError::AdapterTransport(
            "no stream".into(),
        ))]);
        let (orch, tx, token) = build_with_views(&cfg, sr.clone(), media.clone(), views);
        let handle = tokio::spawn(orch.run());

        send(&tx, manual()).await;
        tokio::time::advance(USER_VIEW_HOLD + Duration::from_secs(1)).await;
        send(&tx, motion()).await;
        assert_eq!(
            sr.call_count().await,
            1,
            "a lost idle report must not pause motion for good"
        );

        token.cancel();
        handle.await.unwrap();
    }

    // ---------- Relay of the user's view (ADR 0007) ----------

    #[tokio::test(start_paused = true)]
    async fn user_view_in_idle_relays_the_view_until_it_ends() {
        let cfg = camera_cfg(60, 300);
        let sr = StubSignaler::with_responses(vec![]);
        let media = RecordingMedia::new();
        let views = StubUserViews::always();
        let (orch, tx, token) = build_with_views(&cfg, sr.clone(), media.clone(), views.clone());
        let handle = tokio::spawn(orch.run());

        send(&tx, manual()).await;
        assert_eq!(
            media.calls().await,
            vec![MediaCall::Register, MediaCall::AttachUserView]
        );
        assert_eq!(views.call_count().await, 1);
        assert_eq!(sr.call_count().await, 0, "no WebRTC call for a relay");
        // A repeated view report changes nothing while relaying.
        send(&tx, manual()).await;
        assert!(media.calls().await.is_empty());
        assert_eq!(views.call_count().await, 1);

        send(&tx, manual_ended()).await;
        let calls = media.calls().await;
        assert_eq!(calls[0], MediaCall::DetachLive, "{calls:?}");
        assert_eq!(
            sr.stop_count().await,
            1,
            "teardown stays paired with detach"
        );

        token.cancel();
        handle.await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn user_view_relay_ends_at_the_hard_cap_and_is_not_resumed() {
        let cfg = camera_cfg(60, 300);
        let sr = StubSignaler::with_responses(vec![]);
        let media = RecordingMedia::new();
        let views = StubUserViews::always();
        let (orch, tx, token) = build_with_views(&cfg, sr.clone(), media.clone(), views.clone());
        let handle = tokio::spawn(orch.run());

        send(&tx, manual()).await;
        assert_eq!(
            media.calls().await,
            vec![MediaCall::Register, MediaCall::AttachUserView]
        );
        tokio::time::advance(Duration::from_secs(299)).await;
        tokio::task::yield_now().await;
        assert!(
            media.calls().await.is_empty(),
            "still relaying before the cap"
        );

        tokio::time::advance(Duration::from_secs(2)).await;
        tokio::task::yield_now().await;
        let calls = media.calls().await;
        assert_eq!(calls, vec![MediaCall::DetachLive], "{calls:?}");
        assert_eq!(sr.stop_count().await, 1, "teardown paired with detach");
        assert_eq!(
            views.call_count().await,
            1,
            "no new query without a new view report"
        );

        // The camera idles once we let go: the view's end is a no-op now.
        send(&tx, manual_ended()).await;
        assert!(media.calls().await.is_empty());

        token.cancel();
        handle.await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn user_view_relay_probe_relays_again_when_no_idle_report_comes() {
        let cfg = camera_cfg_probing(60, 300, 60);
        let sr = StubSignaler::with_responses(vec![]);
        let media = RecordingMedia::new();
        let views = StubUserViews::always();
        let (orch, tx, token) = build_with_views(&cfg, sr.clone(), media.clone(), views.clone());
        let handle = tokio::spawn(orch.run());

        send(&tx, manual()).await;
        assert_eq!(
            media.calls().await,
            vec![MediaCall::Register, MediaCall::AttachUserView]
        );
        tokio::time::advance(Duration::from_secs(61)).await;
        tokio::task::yield_now().await;
        assert_eq!(
            media.calls().await,
            vec![MediaCall::DetachLive],
            "probe lets go"
        );
        assert_eq!(sr.stop_count().await, 1);

        // No idle report within the grace: the view goes on.
        tokio::time::advance(USER_VIEW_PROBE_GRACE + Duration::from_secs(1)).await;
        tokio::task::yield_now().await;
        assert_eq!(media.calls().await, vec![MediaCall::AttachUserView]);
        assert_eq!(views.call_count().await, 2);

        // The next segment probes again, so the relay never runs past the cap unchecked.
        tokio::time::advance(Duration::from_secs(61)).await;
        tokio::task::yield_now().await;
        assert_eq!(media.calls().await, vec![MediaCall::DetachLive]);

        token.cancel();
        handle.await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn user_view_relay_probe_stops_when_the_camera_reports_idle() {
        let cfg = camera_cfg_probing(60, 300, 60);
        let sr = StubSignaler::with_responses(vec![]);
        let media = RecordingMedia::new();
        let views = StubUserViews::always();
        let (orch, tx, token) = build_with_views(&cfg, sr.clone(), media.clone(), views.clone());
        let handle = tokio::spawn(orch.run());

        send(&tx, manual()).await;
        tokio::time::advance(Duration::from_secs(61)).await;
        tokio::task::yield_now().await;
        assert!(media.calls().await.contains(&MediaCall::DetachLive));

        assert_eq!(
            media.notices().await,
            vec![true],
            "the still says where the view is while the probe listens"
        );

        // The camera idles once released: the app had closed its view.
        send(&tx, manual_ended()).await;
        tokio::time::advance(USER_VIEW_PROBE_GRACE + Duration::from_secs(1)).await;
        tokio::task::yield_now().await;
        assert!(media.calls().await.is_empty(), "no relay without a view");
        assert_eq!(views.call_count().await, 1);
        assert_eq!(
            media.notices().await,
            vec![true, false],
            "notice down with the view"
        );

        // A new view in the app relays as usual.
        send(&tx, manual()).await;
        assert_eq!(media.calls().await, vec![MediaCall::AttachUserView]);

        token.cancel();
        handle.await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn motion_during_a_relay_neither_starts_a_cooldown_nor_a_second_session() {
        let cfg = camera_cfg(2, 300);
        let sr = StubSignaler::with_responses(vec![]);
        let media = RecordingMedia::new();
        let (orch, tx, token) = build(&cfg, sr.clone(), media.clone());
        let handle = tokio::spawn(orch.run());

        send(&tx, manual()).await;
        media.calls().await;
        send(&tx, motion()).await;
        tokio::time::advance(Duration::from_secs(10)).await;
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(
            media.calls().await.is_empty(),
            "a motion pulse must not end the relay through the debounce"
        );
        assert_eq!(sr.call_count().await, 0);

        send(&tx, manual_ended()).await;
        assert!(media.calls().await.contains(&MediaCall::DetachLive));
        // The view is over: the next pulse is an ordinary motion session.
        sr.push_response(Ok(ok_answer())).await;
        send(&tx, motion()).await;
        assert_eq!(sr.call_count().await, 1);

        token.cancel();
        handle.await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn relay_failure_returns_to_idle_without_backoff_and_guards_retries() {
        let cfg = camera_cfg(60, 300);
        let media = RecordingMedia::new();
        let views = StubUserViews::with_responses(vec![Err(DomainError::AdapterTransport(
            "no stream".into(),
        ))]);
        let (orch, tx, token) = build_with_views(
            &cfg,
            StubSignaler::with_responses(vec![]),
            media.clone(),
            views.clone(),
        );
        let handle = tokio::spawn(orch.run());

        send(&tx, manual()).await;
        assert_eq!(views.call_count().await, 1);
        assert_eq!(
            media.notices().await,
            vec![true],
            "the idle frame keeps the notice"
        );
        // Within the guard: the repeated report is left alone.
        send(&tx, manual()).await;
        assert_eq!(views.call_count().await, 1);
        // Past it: the next report tries again, and this time it works.
        tokio::time::advance(USER_VIEW_RETRY + Duration::from_secs(1)).await;
        send(&tx, manual()).await;
        assert_eq!(views.call_count().await, 2);
        assert!(media.calls().await.contains(&MediaCall::AttachUserView));

        token.cancel();
        handle.await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn relay_loss_returns_to_idle_and_is_not_retried_at_once() {
        let cfg = camera_cfg(60, 300);
        let media = RecordingMedia::new();
        let views = StubUserViews::always();
        let (orch, tx, token) = build_with_views(
            &cfg,
            StubSignaler::with_responses(vec![]),
            media.clone(),
            views.clone(),
        );
        let handle = tokio::spawn(orch.run());

        send(&tx, manual()).await;
        media.calls().await;
        assert!(media.fail_live(0, LiveLossReason::EndOfStream).await);
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(media.calls().await.contains(&MediaCall::DetachLive));
        send(&tx, manual()).await;
        assert_eq!(
            views.call_count().await,
            1,
            "lost relay not retried within the guard"
        );

        token.cancel();
        handle.await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn user_view_during_a_motion_session_does_not_replace_it() {
        let cfg = camera_cfg(60, 300);
        let sr = StubSignaler::with_responses(vec![Ok(ok_answer())]);
        let media = RecordingMedia::new();
        let views = StubUserViews::always();
        let (orch, tx, token) = build_with_views(&cfg, sr, media.clone(), views.clone());
        let handle = tokio::spawn(orch.run());

        send(&tx, motion()).await;
        media.calls().await;
        send(&tx, manual()).await;
        assert!(media.calls().await.is_empty());
        assert_eq!(views.call_count().await, 0);

        token.cancel();
        handle.await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn user_view_notice_follows_the_app_view() {
        let cfg = camera_cfg(60, 300);
        let media = RecordingMedia::new();
        let (orch, tx, token) = build(&cfg, StubSignaler::with_responses(vec![]), media.clone());
        let handle = tokio::spawn(orch.run());

        send(&tx, manual()).await;
        send(&tx, manual()).await; // a repeated report changes nothing
        assert_eq!(media.notices().await, vec![true]);
        send(&tx, manual_ended()).await;
        assert_eq!(media.notices().await, vec![true, false]);

        token.cancel();
        handle.await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn user_view_notice_clears_when_the_hold_expires() {
        // A view the daemon could not relay: the notice lives on the hold alone.
        let cfg = camera_cfg(60, 300);
        let media = RecordingMedia::new();
        let views = StubUserViews::with_responses(vec![Err(DomainError::AdapterTransport(
            "no stream".into(),
        ))]);
        let (orch, tx, token) = build_with_views(
            &cfg,
            StubSignaler::with_responses(vec![]),
            media.clone(),
            views,
        );
        let handle = tokio::spawn(orch.run());

        send(&tx, manual()).await;
        tokio::time::advance(USER_VIEW_HOLD.saturating_sub(Duration::from_secs(1))).await;
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert_eq!(media.notices().await, vec![true], "still inside the hold");
        tokio::time::advance(Duration::from_secs(2)).await;
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert_eq!(
            media.notices().await,
            vec![true, false],
            "a lost idle report must not leave the notice up"
        );

        token.cancel();
        handle.await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn user_view_notice_is_shown_when_an_attach_is_refused_as_busy() {
        let cfg = camera_cfg(60, 300);
        let media = RecordingMedia::new();
        let sr = StubSignaler::with_responses(vec![Err(busy())]);
        let (orch, tx, token) = build(&cfg, sr, media.clone());
        let handle = tokio::spawn(orch.run());

        send(&tx, motion()).await;
        assert_eq!(media.notices().await, vec![true]);

        token.cancel();
        handle.await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn camera_busy_attach_returns_to_idle_without_backoff() {
        // The bus missed the user view: the attach itself is refused.
        let cfg = camera_cfg(60, 300);
        let sr = StubSignaler::with_responses(vec![Err(busy()), Ok(ok_answer())]);
        let media = RecordingMedia::new();
        let (orch, tx, token) = build(&cfg, sr.clone(), media.clone());
        let handle = tokio::spawn(orch.run());

        send(&tx, motion()).await;
        assert_eq!(sr.call_count().await, 1);
        assert_eq!(
            sr.stop_count().await,
            1,
            "a refused attach still releases the signaling"
        );
        // The refusal marked the camera as viewed: motion is paused …
        send(&tx, motion()).await;
        assert_eq!(sr.call_count().await, 1);
        // … and once the view ends, the next pulse activates at once —
        // no Failed backoff to wait out.
        send(&tx, manual_ended()).await;
        send(&tx, motion()).await;
        assert_eq!(sr.call_count().await, 2);
        assert!(
            media
                .calls()
                .await
                .iter()
                .any(|c| matches!(c, MediaCall::AttachLive(_)))
        );

        token.cancel();
        handle.await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn motion_pulse_in_failed_state_does_not_leave_a_stale_cap() {
        // First attach fails (backoff 1 s); a pulse during Failed must not
        // prime `live_since`, or the next session would trip the 2 s cap
        // right after attaching.
        let cfg = camera_cfg(60, 2);
        let sr = StubSignaler::with_responses(vec![
            Err(DomainError::AdapterTransport("boom".to_string())),
            Ok(ok_answer()),
        ]);
        let media = RecordingMedia::new();
        let (orch, tx, token) = build(&cfg, sr.clone(), media.clone());
        let handle = tokio::spawn(orch.run());

        send(&tx, motion()).await; // → Failed
        send(&tx, motion()).await; // pulse while Failed
        tokio::time::advance(Duration::from_secs(3)).await; // backoff elapsed → Idle
        tokio::time::sleep(Duration::from_millis(10)).await;
        media.calls().await;

        send(&tx, motion()).await; // attaches
        tokio::time::advance(Duration::from_millis(500)).await;
        tokio::time::sleep(Duration::from_millis(10)).await;
        let calls = media.calls().await;
        assert!(calls.iter().any(|c| matches!(c, MediaCall::AttachLive(_))));
        assert!(
            !calls.contains(&MediaCall::DetachLive),
            "stale live_since would have tripped the cap immediately"
        );

        token.cancel();
        handle.await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn snapshot_reports_user_view() {
        let cfg = camera_cfg(60, 300);
        let sr = StubSignaler::with_responses(vec![]);
        let media = RecordingMedia::new();
        let (tx, rx) = mpsc::channel(32);
        let (admin_tx, admin_rx) = mpsc::channel(4);
        let token = CancellationToken::new();
        let orch = CameraOrchestrator::new(
            &cfg,
            sr,
            Arc::new(StubThumbnails),
            media,
            StubUserViews::always(),
            Arc::new(NoopRecorder),
            rx,
            admin_rx,
            token.clone(),
        )
        .unwrap();
        let handle = tokio::spawn(orch.run());

        let snap = |admin_tx: mpsc::Sender<crate::admin::AdminCommand>| async move {
            let (reply, rx) = tokio::sync::oneshot::channel();
            admin_tx
                .send(crate::admin::AdminCommand::Snapshot { reply })
                .await
                .unwrap();
            rx.await.unwrap()
        };
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(!snap(admin_tx.clone()).await.user_view);
        send(&tx, manual()).await;
        let viewed = snap(admin_tx.clone()).await;
        assert!(viewed.user_view);
        assert_eq!(viewed.state, "live", "the view is relayed");
        assert_eq!(viewed.live_source.as_deref(), Some("user-view"));
        send(&tx, manual_ended()).await;
        let after = snap(admin_tx).await;
        assert!(!after.user_view);
        assert_eq!(after.state, "idle");
        assert_eq!(after.live_source, None);

        token.cancel();
        handle.await.unwrap();
    }

    #[test]
    fn signal_label_covers_camera_busy() {
        assert_eq!(signal_label(&StateTransition::CameraBusy), "camera-busy");
        assert_eq!(
            signal_label(&StateTransition::UserViewStarted),
            "user-view-started"
        );
        assert_eq!(
            signal_label(&StateTransition::UserViewEnded),
            "user-view-ended"
        );
        assert_eq!(
            signal_label(&StateTransition::UserViewUnavailable),
            "user-view-unavailable"
        );
    }

    // ---------- Failure path ----------

    #[tokio::test(start_paused = true)]
    async fn negotiate_failure_enters_failed_state() {
        let cfg = camera_cfg(2, 60);
        let sr = StubSignaler::with_responses(vec![Err(DomainError::AdapterTransport(
            "auth lost".to_string(),
        ))]);
        let media = RecordingMedia::new();
        let (orch, tx, token) = build(&cfg, sr.clone(), media.clone());

        let handle = tokio::spawn(orch.run());

        tx.send(CameraEvent::Motion {
            device_id: CameraId::new("CAM"),
        })
        .await
        .unwrap();

        tokio::time::sleep(Duration::from_millis(20)).await;
        token.cancel();
        handle.await.unwrap();

        // negotiate was called once (inside attach_live); no AttachLive
        // was recorded since negotiation failed.
        assert_eq!(sr.call_count().await, 1);
        // The Failed exit must still release the upstream session
        // (battery safety) even though negotiate errored.
        assert!(
            sr.stop_count().await >= 1,
            "teardown must run on the Failed exit"
        );
        let calls = media.calls().await;
        assert!(!calls.iter().any(|c| matches!(c, MediaCall::AttachLive(_))));
    }

    // ---------- Sustained motion + max-live cap ----------

    #[tokio::test(start_paused = true)]
    async fn max_live_cap_forces_return_to_idle_under_sustained_motion() {
        // debounce=10s (long), max=2s (short). The cap should win.
        let cfg = camera_cfg(10, 2);
        let sr = StubSignaler::with_responses(vec![Ok(ok_answer())]);
        let media = RecordingMedia::new();
        let (orch, tx, token) = build(&cfg, sr.clone(), media.clone());

        let handle = tokio::spawn(orch.run());

        // Initial motion → activation.
        tx.send(CameraEvent::Motion {
            device_id: CameraId::new("CAM"),
        })
        .await
        .unwrap();
        tokio::time::sleep(Duration::from_millis(10)).await;

        // Advance past the max-live cap.
        tokio::time::advance(Duration::from_secs(3)).await;
        tokio::time::sleep(Duration::from_millis(10)).await;

        token.cancel();
        handle.await.unwrap();

        let calls = media.calls().await;
        assert!(calls.contains(&MediaCall::DetachLive));
    }

    // ---------- Shutdown ----------

    #[tokio::test(start_paused = true)]
    async fn shutdown_during_idle_exits_cleanly() {
        let cfg = camera_cfg(60, 300);
        let sr = StubSignaler::with_responses(vec![]);
        let media = RecordingMedia::new();
        let (orch, _tx, token) = build(&cfg, sr, media.clone());

        let handle = tokio::spawn(orch.run());
        tokio::time::sleep(Duration::from_millis(10)).await;
        token.cancel();
        handle.await.unwrap();

        let calls = media.calls().await;
        // Register on entry, no live work, no detach (we were idle).
        assert_eq!(calls, vec![MediaCall::Register]);
    }

    // ---------- Backoff timing ----------

    #[test]
    fn backoff_duration_progresses() {
        assert_eq!(backoff_duration(0), Duration::from_secs(1));
        assert_eq!(backoff_duration(1), Duration::from_secs(5));
        assert_eq!(backoff_duration(2), Duration::from_secs(30));
        assert_eq!(backoff_duration(3), Duration::from_mins(1));
        assert_eq!(backoff_duration(99), Duration::from_mins(1));
    }

    // ---------- Online events ignored ----------

    #[tokio::test(start_paused = true)]
    async fn offline_while_idle_recovers_after_the_backoff() {
        let cfg = camera_cfg(60, 300);
        let sr = StubSignaler::with_responses(vec![]);
        let media = RecordingMedia::new();
        let (orch, tx, token) = build(&cfg, sr.clone(), media.clone());
        let handle = tokio::spawn(orch.run());

        send(&tx, offline()).await;
        send(&tx, motion()).await;
        assert_eq!(
            sr.call_count().await,
            0,
            "motion is suppressed while Failed"
        );

        // The first backoff is 1 s; after it the camera is Idle again.
        tokio::time::advance(backoff_duration(0) + Duration::from_millis(100)).await;
        tokio::task::yield_now().await;
        send(&tx, motion()).await;
        assert_eq!(
            sr.call_count().await,
            1,
            "a motion after the backoff activates"
        );

        token.cancel();
        handle.await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn offline_then_online_returns_to_idle_at_once() {
        let cfg = camera_cfg(60, 300);
        let sr = StubSignaler::with_responses(vec![]);
        let media = RecordingMedia::new();
        let (orch, tx, token) = build(&cfg, sr.clone(), media.clone());
        let handle = tokio::spawn(orch.run());

        send(&tx, offline()).await;
        send(&tx, online()).await;
        send(&tx, motion()).await;
        assert_eq!(sr.call_count().await, 1, "online ends the backoff early");

        token.cancel();
        handle.await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn online_event_does_not_drive_state() {
        let cfg = camera_cfg(60, 300);
        let sr = StubSignaler::with_responses(vec![]);
        let media = RecordingMedia::new();
        let (orch, tx, token) = build(&cfg, sr.clone(), media.clone());

        let handle = tokio::spawn(orch.run());

        tx.send(CameraEvent::Online {
            device_id: CameraId::new("CAM"),
        })
        .await
        .unwrap();

        tokio::time::sleep(Duration::from_millis(20)).await;
        token.cancel();
        handle.await.unwrap();

        // negotiate was never called.
        assert_eq!(sr.call_count().await, 0);
    }

    // ---------- now_local just returns naive local time ----------

    #[test]
    fn now_local_returns_naive_local_time() {
        let _ = now_local();
    }
}
