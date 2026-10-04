//! The process signals that ask the daemon to stop gracefully.
//!
//! SIGINT (Ctrl-C) and SIGTERM (`docker stop`, `compose down`, systemd)
//! both run the drain, which detaches every live source and releases the
//! Arlo sessions. Without a SIGTERM handler the default action killed the
//! process outright and a camera kept streaming on battery until Arlo
//! timed the session out.
//!
//! The handlers are installed once, early, so a signal that arrives while
//! the daemon boots is held until the main loop waits for it.

use anyhow::{Context, Result};

/// Installed shutdown-signal handlers.
pub struct ShutdownSignals {
    #[cfg(unix)]
    interrupt: tokio::signal::unix::Signal,
    #[cfg(unix)]
    terminate: tokio::signal::unix::Signal,
}

impl ShutdownSignals {
    /// Install the handlers. From here on the signals no longer kill the
    /// process; [`Self::recv`] reports them.
    ///
    /// # Errors
    ///
    /// Returns an error when a handler cannot be registered: the daemon
    /// would otherwise run with no way to stop it cleanly.
    #[cfg(unix)]
    pub fn install() -> Result<Self> {
        use tokio::signal::unix::{SignalKind, signal};
        Ok(Self {
            interrupt: signal(SignalKind::interrupt()).context("failed to handle SIGINT")?,
            terminate: signal(SignalKind::terminate()).context("failed to handle SIGTERM")?,
        })
    }

    /// Install the handlers (Ctrl-C only outside Unix).
    ///
    /// # Errors
    ///
    /// Never on this platform; the signature matches the Unix one.
    #[cfg(not(unix))]
    #[allow(clippy::unnecessary_wraps)]
    pub fn install() -> Result<Self> {
        Ok(Self {})
    }

    /// Wait for the next shutdown signal and return its name for the log.
    #[cfg(unix)]
    pub async fn recv(&mut self) -> &'static str {
        // `recv` yields `None` only once the runtime is shutting down,
        // which is a reason to stop as good as the signal itself.
        tokio::select! {
            _ = self.interrupt.recv() => "SIGINT",
            _ = self.terminate.recv() => "SIGTERM",
        }
    }

    /// Wait for Ctrl-C and return its name for the log.
    #[cfg(not(unix))]
    pub async fn recv(&mut self) -> &'static str {
        // A registration error is reported as a stop request: the drain
        // still releases every session.
        let _ = tokio::signal::ctrl_c().await;
        "Ctrl-C"
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::time::Duration;

    /// The handler must catch SIGTERM: without it the signal kills the
    /// test process, which is exactly the bug it guards against.
    #[tokio::test]
    async fn recv_with_sigterm_reports_it_instead_of_dying() {
        let mut signals = ShutdownSignals::install().unwrap();
        let status = std::process::Command::new("kill")
            .args(["-TERM", &std::process::id().to_string()])
            .status()
            .unwrap();
        assert!(status.success());
        let name = tokio::time::timeout(Duration::from_secs(5), signals.recv())
            .await
            .unwrap();
        assert_eq!(name, "SIGTERM");
    }
}
