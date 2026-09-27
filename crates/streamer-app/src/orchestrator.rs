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
//! while the camera is `Live`/`Cooling` (invariant enforced in
//! `process_signals`), awaits it as one `select!` arm, and turns a
//! resolution into `StateTransition::LiveLost`, which reuses the
//! ordinary `Live → Idle` exit (detach + teardown + thumbnail refresh).
//! Dropping the handle on every live exit is what makes late reports
//! from a finished session unobservable — no generation counters.
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
use tracing::{debug, info, instrument, warn};

use streamer_domain::camera::CameraId;
use streamer_domain::config::CameraConfig;
use streamer_domain::error::DomainError;
use streamer_domain::event::CameraEvent;
use streamer_domain::metrics::{BudgetDecision, MotionOutcome, SpliceOutcome};
use streamer_domain::port::{
    ArloThumbnailSource, MediaMultiplexer, MetricsRecorder, WebrtcSignaler,
};
use streamer_domain::state::{CameraState, LiveLossReason, StateTransition};
use streamer_domain::stream::LiveSession;

use crate::budget::{BudgetVerdict, LiveBudgetTracker};
use crate::debouncer::{DebouncerVerdict, MotionDebouncer};
#[cfg(test)]
use crate::metrics_noop::NoopRecorder;
use crate::transition::transition;

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
    /// Invariant: `Some` iff `state` is `Live` or `Cooling`. Set by
    /// `start_activation`, cleared in `process_signals` the moment the
    /// state leaves live, so a report from a finished session can never
    /// be observed.
    live: Option<LiveSession>,
    signaler: Arc<dyn WebrtcSignaler>,
    thumbnails: Arc<dyn ArloThumbnailSource>,
    media: Arc<dyn MediaMultiplexer>,
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
            signaler,
            thumbnails,
            media,
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
        events: mpsc::Receiver<CameraEvent>,
        shutdown: CancellationToken,
    ) -> Result<Self, DomainError> {
        let (_admin_tx, admin_rx) = mpsc::channel(1);
        Self::new(
            config,
            signaler,
            thumbnails,
            media,
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
                if matches!(
                    self.state,
                    CameraState::Live { .. }
                        | CameraState::Cooling { .. }
                        | CameraState::Activating
                ) {
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
                let signal = self.intercept_budget(StateTransition::MotionDetected);
                self.debouncer.on_motion(Instant::now());
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
            CameraState::Live { .. } => "live",
            CameraState::Cooling { .. } => "cooling",
            CameraState::BatteryProtect { .. } => "battery-protect",
            CameraState::Failed { .. } => "failed",
        };
        let cooling_remaining = match &self.state {
            CameraState::Cooling { remaining } => Some(*remaining),
            _ => None,
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
            cooling_remaining,
            last_failure,
            retries,
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
            CameraState::Idle | CameraState::Activating => None,
            CameraState::Live { .. } | CameraState::Cooling { .. } => {
                self.debouncer.next_deadline(Instant::now())
            }
            CameraState::Failed { .. } => self.failed_deadline,
            CameraState::BatteryProtect { .. } => {
                let next_reset = self.budget.next_reset(Local::now().naive_local());
                let now = Local::now().naive_local();
                let delta = (next_reset - now).num_seconds().max(0);
                Some(Instant::now() + Duration::from_secs(u64::try_from(delta).unwrap_or(0)))
            }
        }
    }

    async fn handle_event(&mut self, event: CameraEvent) -> Result<(), DomainError> {
        let signal = match event {
            CameraEvent::Motion { .. } | CameraEvent::Audio { .. } => {
                self.debouncer.on_motion(Instant::now());
                let signal = self.intercept_budget(StateTransition::MotionDetected);
                self.metrics
                    .record_motion(&self.camera_id, classify_motion(&self.state, &signal));
                signal
            }
            CameraEvent::Online { .. } => {
                debug!("camera online");
                return Ok(());
            }
            CameraEvent::Offline { .. } => {
                StateTransition::Failure("camera went offline".to_string())
            }
        };
        self.process_signals(VecDeque::from([signal])).await
    }

    /// The media adapter reported the attached source dead. Route it
    /// through the reducer as `LiveLost`; the `Live → Idle` side effects
    /// (detach, teardown, thumbnail refresh) are the ordinary ones.
    async fn handle_live_lost(&mut self, reason: LiveLossReason) -> Result<(), DomainError> {
        warn!(
            reason = reason.as_label(),
            "live source lost; returning to idle"
        );
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

    async fn handle_deadline(&mut self) -> Result<(), DomainError> {
        let signal = match &self.state {
            CameraState::Live { .. } | CameraState::Cooling { .. } => {
                match self.debouncer.poll(Instant::now()) {
                    DebouncerVerdict::DebounceExpired => StateTransition::CooldownExpired,
                    DebouncerVerdict::MaxLiveExceeded => StateTransition::MaxLiveExceeded,
                    DebouncerVerdict::KeepLive | DebouncerVerdict::Idle => return Ok(()),
                }
            }
            CameraState::Failed { .. } => StateTransition::BackoffElapsed,
            CameraState::BatteryProtect { .. } => StateTransition::BudgetReset,
            _ => return Ok(()),
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
            if !matches!(to, CameraState::Live { .. } | CameraState::Cooling { .. }) {
                self.live = None;
            }
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
                follow_ups.extend(self.start_activation().await);
            }
            // Activation succeeded.
            (CameraState::Activating, CameraState::Live { .. }) => {
                let now = Instant::now();
                self.debouncer.on_live_attached(now);
                self.budget.on_live_started(Local::now().naive_local());
            }
            // Cooldown / max-live / mid-Live failure → return to idle (or beyond).
            (CameraState::Live { .. } | CameraState::Cooling { .. }, CameraState::Idle) => {
                self.detach_and_refresh().await;
            }
            // Live → BatteryProtect (budget exhausted mid-session).
            (
                CameraState::Live { .. } | CameraState::Cooling { .. },
                CameraState::BatteryProtect { .. },
            ) => {
                self.detach_and_refresh().await;
                self.budget.on_live_ended(Local::now().naive_local());
                self.debouncer.on_idle();
            }
            // Anywhere → Failed (transient error).
            (
                CameraState::Activating | CameraState::Live { .. } | CameraState::Cooling { .. },
                CameraState::Failed { retries, .. },
            ) => {
                let _ = self.media.detach_live(&self.camera_id).await;
                self.stop_arlo_live().await;
                self.budget.on_live_ended(Local::now().naive_local());
                self.debouncer.on_idle();
                self.failed_deadline = Some(Instant::now() + backoff_duration(*retries));
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
        let started = Instant::now();
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
            Err(e) => {
                let latency = elapsed_ms(started);
                warn!(error = %e, latency_ms = latency, "attach_live failed");
                self.metrics
                    .record_splice(&self.camera_id, SpliceOutcome::AttachFailed, latency);
                vec![StateTransition::Failure(e.to_string())]
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
        self.budget.on_live_ended(Local::now().naive_local());
        self.debouncer.on_idle();
        self.refresh_idle_thumbnail().await;
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
        if matches!(
            self.state,
            CameraState::Live { .. } | CameraState::Cooling { .. }
        ) {
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
    }
}

/// Classify a fresh motion event given the *current* state and the
/// computed signal so the recorder gets a meaningful outcome:
/// - in `Failed`: suppressed
/// - signal diverted to `BudgetExhausted`: budget-exhausted
/// - already live/cooling: absorbed (debouncer handles it)
/// - else: triggered
fn classify_motion(state: &CameraState, signal: &StateTransition) -> MotionOutcome {
    if matches!(signal, StateTransition::BudgetExhausted) {
        return MotionOutcome::BudgetExhausted;
    }
    match state {
        CameraState::Failed { .. } => MotionOutcome::SuppressedFailed,
        CameraState::Live { .. } | CameraState::Cooling { .. } => MotionOutcome::Absorbed,
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
        async fn ice_servers(
            &self,
            _camera: &CameraId,
        ) -> Result<Vec<streamer_domain::stream::IceServer>, DomainError> {
            Ok(vec![])
        }
        async fn negotiate(
            &self,
            _camera: &CameraId,
            _offer_sdp: String,
        ) -> Result<SignalingAnswer, DomainError> {
            *self.calls.lock().await += 1;
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
    }

    #[derive(Debug, PartialEq, Eq)]
    enum MediaCall {
        Register,
        AttachLive(String),
        DetachLive,
        RefreshThumbnail,
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
            let answer = signaler.negotiate(camera, "offer".to_string()).await?;
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
        CameraConfig {
            arlo_device_id: CameraId::new("CAM"),
            stream_name: streamer_domain::camera::StreamName::parse("cam").unwrap(),
            codec_hint: None,
            cooldown: CooldownConfig {
                debounce_secs: debounce,
                max_continuous_live: max,
                daily_live_budget: 0,
                budget_reset: "00:00".to_string(),
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
        let (tx, rx) = mpsc::channel(32);
        let token = CancellationToken::new();
        let orch = CameraOrchestrator::new_minimal(
            cfg,
            sr,
            Arc::new(StubThumbnails),
            media,
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
