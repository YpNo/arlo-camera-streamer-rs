//! A pause between failed Arlo logins that survives restarts.
//!
//! A container restart policy turns any boot failure into a login per
//! restart, and Arlo's Cloudflare edge answers a burst of logins with a
//! rate-limit block (error 1015) that each new attempt prolongs. The
//! failures are therefore recorded in the state directory, beside the
//! session cache: the next boot waits out [`delay_after`] before it logs in
//! again, however often the process was restarted meanwhile. A successful
//! login clears the record; a rate-limited one jumps straight to a long
//! pause.
//!
//! The record holds a count and a Unix timestamp, nothing about the account.

use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use tracing::warn;

/// The record's file name, beside the session cache.
pub const LOGIN_BACKOFF_FILE: &str = "login-backoff.json";

/// Pause before the next login after 1, 2, 3 and 4+ failures in a row.
const DELAYS: [Duration; 4] = [
    Duration::from_mins(1),
    Duration::from_mins(5),
    Duration::from_mins(15),
    Duration::from_hours(1),
];
/// A rate-limited failure counts at least this many, so the next pause is
/// already the 15-minute one: Cloudflare's block outlasts the short ones.
const RATE_LIMITED_FAILURES: u32 = 3;

/// The pause a boot waits after `failures` failed logins in a row.
#[must_use]
pub fn delay_after(failures: u32) -> Duration {
    match failures {
        0 => Duration::ZERO,
        n => {
            DELAYS[usize::try_from(n - 1)
                .unwrap_or(usize::MAX)
                .min(DELAYS.len() - 1)]
        }
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
struct Record {
    failures: u32,
    last_failure_unix: u64,
}

impl Record {
    /// What is left of the pause at `now`. A last failure in the future (a
    /// clock stepped back) restarts the pause from `now`.
    fn remaining(&self, now: SystemTime) -> Duration {
        let delay = delay_after(self.failures);
        let last = UNIX_EPOCH + Duration::from_secs(self.last_failure_unix);
        match now.duration_since(last) {
            Ok(elapsed) => delay.saturating_sub(elapsed),
            Err(_) => delay,
        }
    }
}

/// The persisted login back-off of one state directory.
#[derive(Debug, Clone)]
pub struct LoginBackoff {
    path: PathBuf,
}

impl LoginBackoff {
    /// The back-off kept beside `session_cache_path`.
    #[must_use]
    pub fn beside(session_cache_path: &Path) -> Self {
        let dir = session_cache_path.parent().unwrap_or_else(|| Path::new(""));
        Self {
            path: dir.join(LOGIN_BACKOFF_FILE),
        }
    }

    /// How long to wait at `now` before the next login. A missing or
    /// unreadable record means no wait (an unreadable one is reported).
    pub async fn remaining(&self, now: SystemTime) -> Duration {
        self.load().await.remaining(now)
    }

    /// Record one more failed login at `now` and return the pause before
    /// the next one.
    ///
    /// # Errors
    ///
    /// The I/O error when the record cannot be written; the caller then
    /// waits the pause in memory before exiting.
    pub async fn record_failure(
        &self,
        now: SystemTime,
        rate_limited: bool,
    ) -> Result<Duration, std::io::Error> {
        let mut record = self.load().await;
        record.failures = record.failures.saturating_add(1);
        if rate_limited {
            record.failures = record.failures.max(RATE_LIMITED_FAILURES);
        }
        record.last_failure_unix = now
            .duration_since(UNIX_EPOCH)
            .map_or(0, |since| since.as_secs());
        let json = serde_json::to_vec(&record).map_err(std::io::Error::other)?;
        tokio::fs::write(&self.path, json).await?;
        Ok(delay_after(record.failures))
    }

    /// Forget the failures after a successful login.
    pub async fn clear(&self) {
        match tokio::fs::remove_file(&self.path).await {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => {
                warn!(path = %self.path.display(), error = %e, "login back-off record not removed");
            }
        }
    }

    async fn load(&self) -> Record {
        let bytes = match tokio::fs::read(&self.path).await {
            Ok(bytes) => bytes,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Record::default(),
            Err(e) => {
                warn!(path = %self.path.display(), error = %e, "login back-off record unreadable; not waiting");
                return Record::default();
            }
        };
        serde_json::from_slice(&bytes).unwrap_or_else(|e| {
            warn!(path = %self.path.display(), error = %e, "login back-off record malformed; not waiting");
            Record::default()
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scratch(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("login-backoff-{name}-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn at(secs: u64) -> SystemTime {
        UNIX_EPOCH + Duration::from_secs(secs)
    }

    #[test]
    fn delay_after_grows_then_stays_at_an_hour() {
        assert_eq!(delay_after(0), Duration::ZERO);
        assert_eq!(delay_after(1), Duration::from_mins(1));
        assert_eq!(delay_after(2), Duration::from_mins(5));
        assert_eq!(delay_after(3), Duration::from_mins(15));
        assert_eq!(delay_after(4), Duration::from_hours(1));
        assert_eq!(delay_after(u32::MAX), Duration::from_hours(1));
    }

    /// The pause holds across restarts: a new process reads the record.
    #[tokio::test]
    async fn a_failure_makes_the_next_boot_wait_until_it_has_passed() {
        let dir = scratch("restart");
        let backoff = LoginBackoff::beside(&dir.join("session.json"));
        assert_eq!(backoff.remaining(at(1_000)).await, Duration::ZERO);

        backoff.record_failure(at(1_000), false).await.unwrap();
        backoff.record_failure(at(1_010), false).await.unwrap();

        let restarted = LoginBackoff::beside(&dir.join("session.json"));
        assert_eq!(
            restarted.remaining(at(1_070)).await,
            Duration::from_secs(240)
        );
        assert_eq!(restarted.remaining(at(1_310)).await, Duration::ZERO);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn a_rate_limited_failure_waits_at_least_fifteen_minutes() {
        let dir = scratch("rate");
        let backoff = LoginBackoff::beside(&dir.join("session.json"));

        let pause = backoff.record_failure(at(5_000), true).await.unwrap();

        assert_eq!(pause, Duration::from_mins(15));
        assert_eq!(backoff.remaining(at(5_000)).await, Duration::from_mins(15));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn a_successful_login_clears_the_record() {
        let dir = scratch("clear");
        let backoff = LoginBackoff::beside(&dir.join("session.json"));
        backoff.record_failure(at(100), false).await.unwrap();

        backoff.clear().await;
        backoff.clear().await;

        assert_eq!(backoff.remaining(at(100)).await, Duration::ZERO);
        std::fs::remove_dir_all(&dir).ok();
    }

    /// A clock stepped back must not cancel the pause.
    #[test]
    fn a_failure_in_the_future_restarts_the_pause_from_now() {
        let record = Record {
            failures: 2,
            last_failure_unix: 10_000,
        };
        assert_eq!(record.remaining(at(9_000)), Duration::from_mins(5));
    }

    #[tokio::test]
    async fn a_malformed_record_means_no_wait() {
        let dir = scratch("malformed");
        std::fs::write(dir.join(LOGIN_BACKOFF_FILE), b"not json").unwrap();
        let backoff = LoginBackoff::beside(&dir.join("session.json"));
        assert_eq!(backoff.remaining(at(0)).await, Duration::ZERO);
        std::fs::remove_dir_all(&dir).ok();
    }
}
