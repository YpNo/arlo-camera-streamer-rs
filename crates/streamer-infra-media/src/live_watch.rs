//! Pure building blocks of the live-loss detector (ADR 0004).
//!
//! The GStreamer-bound wiring in [`crate::webrtc_pipeline`] cannot run
//! on a workstation without GStreamer, so the decision logic lives here
//! with no `gstreamer` import and full unit coverage:
//!
//! - [`RtpActivity`] is the monotonic "last inbound video packet" clock,
//!   written from the `appsink` streaming thread with a single atomic
//!   store and read by the stall watchdog task.
//! - [`stall_verdict`] is the rule that turns a silence duration into a
//!   [`LiveLossReason::RtpStalled`], or not.
//! - [`setup_or_loss`] races the attach setup against the session's
//!   loss signal, so a detector that fires before the first packet
//!   fails the attach with its reason.
//!
//! Reporting goes straight through the domain's
//! [`LiveLossNotifier`](streamer_domain::stream::LiveLossNotifier), which
//! is already cloneable and first-wins; no adapter-side wrapper.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use streamer_domain::state::LiveLossReason;
use streamer_domain::stream::LiveSession;

use crate::error::MediaError;

/// Monotonic record of the most recent inbound video RTP packet.
///
/// Stores milliseconds since a fixed [`Instant`] anchor in an
/// `AtomicU64`, so the streaming-thread hot path never takes a lock and
/// wall-clock steps (NTP on a cold-booted Pi) cannot fake a stall.
#[derive(Debug)]
pub struct RtpActivity {
    anchor: Instant,
    last_ms: AtomicU64,
}

impl RtpActivity {
    /// Start the clock at `now`; counts as the last activity until the
    /// first [`touch`](Self::touch).
    #[must_use]
    pub fn new(now: Instant) -> Self {
        Self {
            anchor: now,
            last_ms: AtomicU64::new(0),
        }
    }

    /// Record an inbound packet at `now`.
    pub fn touch(&self, now: Instant) {
        self.last_ms
            .store(self.millis_since_anchor(now), Ordering::Relaxed);
    }

    /// How long the source has been silent as of `now`. Zero when `now`
    /// precedes the last recorded packet (clock reads from two threads
    /// may interleave).
    #[must_use]
    pub fn silent_for(&self, now: Instant) -> Duration {
        let now_ms = self.millis_since_anchor(now);
        let last_ms = self.last_ms.load(Ordering::Relaxed);
        Duration::from_millis(now_ms.saturating_sub(last_ms))
    }

    fn millis_since_anchor(&self, now: Instant) -> u64 {
        u64::try_from(now.saturating_duration_since(self.anchor).as_millis()).unwrap_or(u64::MAX)
    }
}

/// The stall rule: a source silent for at least `timeout` is lost.
#[must_use]
pub fn stall_verdict(silent_for: Duration, timeout: Duration) -> Option<LiveLossReason> {
    (silent_for >= timeout).then_some(LiveLossReason::RtpStalled)
}

/// Drive the attach `setup` to completion unless `session` reports a
/// loss first.
///
/// The detectors hold the session's notifier from the moment the
/// pipeline exists, so an ICE failure or a bus error during setup
/// resolves `session` while `setup` still waits for its first packet.
/// That report wins and `setup` is dropped, which must release whatever
/// it built (`WebrtcLive` shuts its pipeline down on drop).
///
/// When both are ready, `setup`'s outcome wins: a session that came up
/// keeps its report, and the orchestrator reads it as an ordinary loss.
///
/// # Errors
///
/// `setup`'s own error, or [`MediaError::LostDuringSetup`] carrying the
/// detector's reason.
pub async fn setup_or_loss<T>(
    setup: impl Future<Output = Result<T, MediaError>>,
    session: &mut LiveSession,
) -> Result<T, MediaError> {
    tokio::select! {
        biased;
        outcome = setup => outcome,
        reason = session.lost() => Err(MediaError::LostDuringSetup(reason)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::AtomicBool;

    /// Bounds a race under test: with the paused clock an unanswered
    /// wait fails in virtual time instead of hanging the suite.
    async fn bounded<T>(f: impl Future<Output = T>) -> T {
        tokio::time::timeout(Duration::from_secs(60), f)
            .await
            .expect("race did not resolve")
    }

    /// Sets its flag when dropped: proves a cancelled setup released
    /// what it held.
    struct DropFlag(Arc<AtomicBool>);

    impl Drop for DropFlag {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn setup_or_loss_without_loss_returns_setup_result() {
        let (mut session, _notifier) = LiveSession::new();
        let out = setup_or_loss(async { Ok::<_, MediaError>(7) }, &mut session).await;
        assert_eq!(out.ok(), Some(7));
    }

    #[tokio::test(start_paused = true)]
    async fn setup_or_loss_setup_error_passes_through() {
        let (mut session, _notifier) = LiveSession::new();
        let out = setup_or_loss(
            async { Err::<(), _>(MediaError::SpliceTimeout { timeout_secs: 20 }) },
            &mut session,
        )
        .await;
        assert!(matches!(
            out,
            Err(MediaError::SpliceTimeout { timeout_secs: 20 })
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn setup_or_loss_early_loss_fails_with_reason_and_drops_setup() {
        let (mut session, notifier) = LiveSession::new();
        let dropped = Arc::new(AtomicBool::new(false));
        let guard = DropFlag(dropped.clone());
        let setup = async move {
            let _guard = guard;
            std::future::pending::<Result<(), MediaError>>().await
        };
        notifier.notify(LiveLossReason::PeerDisconnected);
        let out = bounded(setup_or_loss(setup, &mut session)).await;
        assert!(matches!(
            out,
            Err(MediaError::LostDuringSetup(
                LiveLossReason::PeerDisconnected
            ))
        ));
        assert!(
            dropped.load(Ordering::SeqCst),
            "the cancelled setup must be dropped"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn setup_or_loss_loss_mid_setup_interrupts_the_wait() {
        let (mut session, notifier) = LiveSession::new();
        let setup = async {
            tokio::time::sleep(Duration::from_secs(3600)).await;
            Ok::<_, MediaError>(())
        };
        let report = async {
            tokio::task::yield_now().await;
            notifier.notify(LiveLossReason::PipelineError);
        };
        let (out, ()) =
            bounded(async { tokio::join!(setup_or_loss(setup, &mut session), report) }).await;
        assert!(matches!(
            out,
            Err(MediaError::LostDuringSetup(LiveLossReason::PipelineError))
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn setup_or_loss_both_ready_prefers_setup_and_keeps_the_report() {
        let (mut session, notifier) = LiveSession::new();
        notifier.notify(LiveLossReason::RtpStalled);
        let out = setup_or_loss(async { Ok::<_, MediaError>(()) }, &mut session).await;
        assert!(out.is_ok());
        assert_eq!(session.lost().await, LiveLossReason::RtpStalled);
    }

    fn t0() -> Instant {
        Instant::now()
    }

    #[test]
    fn rtp_activity_starts_silent_from_its_anchor() {
        let start = t0();
        let a = RtpActivity::new(start);
        assert_eq!(
            a.silent_for(start + Duration::from_millis(1500)),
            Duration::from_millis(1500)
        );
    }

    #[test]
    fn rtp_activity_touch_resets_silence() {
        let start = t0();
        let a = RtpActivity::new(start);
        a.touch(start + Duration::from_secs(5));
        assert_eq!(
            a.silent_for(start + Duration::from_secs(5) + Duration::from_millis(40)),
            Duration::from_millis(40)
        );
    }

    #[test]
    fn rtp_activity_silence_is_zero_when_now_precedes_last_touch() {
        let start = t0();
        let a = RtpActivity::new(start);
        a.touch(start + Duration::from_secs(2));
        assert_eq!(a.silent_for(start + Duration::from_secs(1)), Duration::ZERO);
    }

    #[test]
    fn stall_verdict_below_timeout_is_healthy() {
        assert_eq!(
            stall_verdict(Duration::from_millis(9999), Duration::from_secs(10)),
            None
        );
    }

    #[test]
    fn stall_verdict_at_timeout_boundary_is_stalled() {
        assert_eq!(
            stall_verdict(Duration::from_secs(10), Duration::from_secs(10)),
            Some(LiveLossReason::RtpStalled)
        );
        assert_eq!(
            stall_verdict(Duration::from_secs(11), Duration::from_secs(10)),
            Some(LiveLossReason::RtpStalled)
        );
    }
}
