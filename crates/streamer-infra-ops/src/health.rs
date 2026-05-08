//! Lock-free liveness + readiness state shared with the HTTP handlers.
//!
//! Both flags are [`std::sync::atomic::AtomicBool`] so handlers and
//! background tasks can read/write without contending on a mutex.
//!
//! ## Liveness vs readiness (Kubernetes conventions)
//!
//! - `/healthz` (liveness): the process is alive and the event loop
//!   is running. Returns `200` for the whole process lifetime.
//!   A failed liveness probe should restart the pod.
//! - `/readyz` (readiness): the process can serve traffic. Returns
//!   `200` only when [`Readiness::is_ready`] is true (started **and**
//!   Arlo bus connected). A failed readiness probe should remove the
//!   pod from load-balancer rotation but **not** restart it.

use std::sync::atomic::{AtomicBool, Ordering};

/// Shared readiness state. Cheap to clone (it's a single
/// [`std::sync::Arc`]).
#[derive(Debug, Default)]
pub struct Readiness {
    started: AtomicBool,
    arlo_connected: AtomicBool,
}

impl Readiness {
    /// Construct fresh state — both flags `false`.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Mark the system as started. Called by the composition root
    /// after `StreamerSystem::spawn` resolves.
    pub fn mark_started(&self) {
        self.started.store(true, Ordering::SeqCst);
    }

    /// Update the Arlo-bus connectivity flag. Driven by the
    /// `connection_status` watch task.
    pub fn set_arlo_connected(&self, connected: bool) {
        self.arlo_connected.store(connected, Ordering::SeqCst);
    }

    /// Has the system finished `spawn`?
    #[must_use]
    pub fn is_started(&self) -> bool {
        self.started.load(Ordering::SeqCst)
    }

    /// Is the Arlo bus currently connected?
    #[must_use]
    pub fn is_arlo_connected(&self) -> bool {
        self.arlo_connected.load(Ordering::SeqCst)
    }

    /// `true` when the system is started **and** the Arlo bus is
    /// connected — i.e. the daemon is actively serving streams.
    #[must_use]
    pub fn is_ready(&self) -> bool {
        self.is_started() && self.is_arlo_connected()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fresh_readiness_is_not_ready() {
        let r = Readiness::new();
        assert!(!r.is_started());
        assert!(!r.is_arlo_connected());
        assert!(!r.is_ready());
    }

    #[test]
    fn started_alone_is_not_ready() {
        let r = Readiness::new();
        r.mark_started();
        assert!(r.is_started());
        assert!(!r.is_ready());
    }

    #[test]
    fn arlo_connected_alone_is_not_ready() {
        let r = Readiness::new();
        r.set_arlo_connected(true);
        assert!(r.is_arlo_connected());
        assert!(!r.is_ready());
    }

    #[test]
    fn both_started_and_connected_is_ready() {
        let r = Readiness::new();
        r.mark_started();
        r.set_arlo_connected(true);
        assert!(r.is_ready());
    }

    #[test]
    fn arlo_disconnect_drops_readiness_back() {
        let r = Readiness::new();
        r.mark_started();
        r.set_arlo_connected(true);
        assert!(r.is_ready());
        r.set_arlo_connected(false);
        assert!(!r.is_ready());
    }

    #[test]
    fn started_is_sticky_does_not_revert() {
        // We deliberately do not provide an `unmark_started` API; the
        // intent is "started" reflects spawn completion (one-shot).
        let r = Readiness::new();
        r.mark_started();
        assert!(r.is_started());
        // Calling again is a no-op.
        r.mark_started();
        assert!(r.is_started());
    }
}
