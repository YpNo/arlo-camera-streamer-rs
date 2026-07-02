//! Boot the [`ArloClient`] from a domain [`ArloConfig`] and return a
//! shared, authenticated handle.
//!
//! Boot sequence:
//!
//! 1. **Build** the client via [`ArloClient::builder`], pointing at
//!    the configured `session_cache_path`. rs-arlo auto-loads any
//!    existing token snapshot on disk.
//! 2. **Resolve secrets** from the env vars named in
//!    `ArloConfig::password_env` and `EmailMfaConfig::password_env`.
//!    Missing env vars are surfaced as [`DomainError::InvalidConfig`].
//! 3. **Authenticate** if [`ArloClient::is_authenticated`] is `false`
//!    (cold start or expired session) using the configured MFA
//!    strategy: `MfaConfig::Push` (poll for mobile-app approval),
//!    `MfaConfig::Email` with IMAP (headless inbox polling), or a
//!    stdin OTP prompt (`MfaConfig::Email` without IMAP, or
//!    `MfaConfig::Sms`).
//! 4. Wrap the authenticated client in [`std::sync::Arc`] and return.
//!
//! After this returns, the `Arc<ArloClient>` is shared by the three
//! port adapters; only `&self` methods are called from there on, so no
//! `Mutex` / `RwLock` is required.
//!
//! [`EmailMfaConfig`]: streamer_domain::config::EmailMfaConfig
//! [`MfaConfig`]: streamer_domain::config::MfaConfig

use std::env;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use rs_arlo::client::ArloClient;
use rs_arlo::config as rs_config;
use tracing::{info, warn};

use streamer_domain::config::{ArloConfig, EmailMfaConfig, MfaConfig};
use streamer_domain::error::DomainError;

use crate::error::arlo_to_domain;

/// Resolved secrets pulled from env vars during boot. Held only for
/// the duration of the auth handshake; never persisted by this crate.
struct ResolvedSecrets {
    arlo_password: String,
    /// `Some` when MFA is `Email` and IMAP fields are configured.
    imap_password: Option<String>,
}

/// Boot an authenticated [`ArloClient`] from a domain [`ArloConfig`].
///
/// # Errors
///
/// - [`DomainError::InvalidConfig`] when a required env var is missing
///   or `session_cache_path` is not valid UTF-8.
/// - [`DomainError::AdapterTransport`] when the rs-arlo build /
///   authentication call fails (network, credentials rejected,
///   IMAP unreachable).
pub async fn boot(config: &ArloConfig) -> Result<Arc<ArloClient>, DomainError> {
    let session_cache_path = utf8_path(&config.session_cache_path)?;

    let mut client = ArloClient::builder()
        .headless(true)
        .session_cache(session_cache_path.clone())
        .build()
        .await
        .map_err(arlo_to_domain)?;

    if client.is_authenticated() {
        info!("rs-arlo session restored from cache");
        return Ok(Arc::new(client));
    }

    warn!("no valid cached session — running MFA cold-start");
    let secrets = resolve_secrets(config)?;
    let rs_config = build_rs_arlo_config(config, &secrets, session_cache_path);

    match &config.mfa {
        MfaConfig::Push(push) => {
            client
                .authenticate_with_push(
                    &rs_config,
                    Duration::from_secs(push.poll_interval_secs),
                    Duration::from_secs(push.timeout_secs),
                )
                .await
                .map_err(arlo_to_domain)?;
        }
        MfaConfig::Email(email) if email_uses_imap(email) => {
            client
                .authenticate_with_imap(&rs_config)
                .await
                .map_err(arlo_to_domain)?;
        }
        // Email-without-IMAP and SMS both resolve the OTP interactively.
        MfaConfig::Email(_) | MfaConfig::Sms => {
            client
                .authenticate_with_handler(&rs_config, rs_arlo::client::mfa::StdinMfaHandler)
                .await
                .map_err(arlo_to_domain)?;
        }
    }

    info!("rs-arlo authentication complete");
    Ok(Arc::new(client))
}

fn utf8_path(p: &Path) -> Result<String, DomainError> {
    p.to_str().map(String::from).ok_or_else(|| {
        DomainError::InvalidConfig(format!("path is not valid UTF-8: {}", p.display()))
    })
}

fn resolve_secrets(config: &ArloConfig) -> Result<ResolvedSecrets, DomainError> {
    let arlo_password = read_env(&config.password_env)?;
    let imap_password = match &config.mfa {
        MfaConfig::Email(email) => {
            if let Some(env_name) = &email.password_env {
                Some(read_env(env_name)?)
            } else {
                None
            }
        }
        MfaConfig::Sms | MfaConfig::Push(_) => None,
    };
    Ok(ResolvedSecrets {
        arlo_password,
        imap_password,
    })
}

fn read_env(name: &str) -> Result<String, DomainError> {
    env::var(name)
        .map_err(|_| DomainError::InvalidConfig(format!("env var {name} required but not set")))
}

/// IMAP auto-retrieval is viable only when the mailbox is locatable
/// (an explicit `host` *or* a `provider` shortcut) **and** a user and
/// password env var are configured. Any other shape falls back to a
/// stdin OTP prompt. Single source of truth for the `boot` dispatch
/// and the `build_rs_arlo_config` assembly so the two can't diverge.
fn email_uses_imap(email: &EmailMfaConfig) -> bool {
    (email.host.is_some() || email.provider.is_some())
        && email.user.is_some()
        && email.password_env.is_some()
}

/// Pure (no I/O) assembly of an `rs_arlo::config::ArloConfig` from
/// the domain config + already-resolved secrets + the UTF-8'd
/// session-cache path. Split out from [`boot`] so it's easy to test.
fn build_rs_arlo_config(
    cfg: &ArloConfig,
    secrets: &ResolvedSecrets,
    session_cache_path: String,
) -> rs_config::ArloConfig {
    let credentials = rs_config::CredentialsConfig {
        email: Some(cfg.email.clone()),
        password: Some(secrets.arlo_password.clone()),
    };

    let mfa = match (&cfg.mfa, secrets.imap_password.as_ref()) {
        (MfaConfig::Email(email), Some(pw)) if email_uses_imap(email) => rs_config::MfaConfig {
            preferred_method: Some("email".to_string()),
            imap: Some(rs_config::ImapConfig {
                enabled: Some(true),
                provider: email.provider.clone(),
                host: email.host.clone(),
                port: Some(email.port),
                username: email.user.clone(),
                password: Some(pw.clone()),
                delete_after_read: Some(false),
            }),
        },
        (MfaConfig::Email(_), _) => rs_config::MfaConfig {
            preferred_method: Some("email".to_string()),
            imap: None,
        },
        (MfaConfig::Sms, _) => rs_config::MfaConfig {
            preferred_method: Some("sms".to_string()),
            imap: None,
        },
        (MfaConfig::Push(_), _) => rs_config::MfaConfig {
            preferred_method: Some("push".to_string()),
            imap: None,
        },
    };

    rs_config::ArloConfig {
        credentials: Some(credentials),
        mfa: Some(mfa),
        client: Some(rs_config::ClientConfig {
            debug_mode: Some(false),
            user_agent: None,
            session_cache_path: Some(session_cache_path),
            headless: Some(true),
            upstream_proxy: None,
            // Pin to the modern v3 API. rs-arlo auto-falls-back to
            // Legacy at runtime if the v2 device endpoints 403/404.
            api_version: Some(rs_config::ApiVersion::V3),
        }),
        streaming: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use streamer_domain::config::{EmailMfaConfig, PushMfaConfig};

    fn cfg_email_imap() -> ArloConfig {
        ArloConfig {
            email: "owner@example.com".to_string(),
            password_env: "ARLO_PASSWORD".to_string(),
            session_cache_path: PathBuf::from("/tmp/session.json"),
            mfa: MfaConfig::Email(EmailMfaConfig {
                host: Some("imap.example.com".to_string()),
                provider: None,
                user: Some("u@example.com".to_string()),
                password_env: Some("ARLO_IMAP_PASSWORD".to_string()),
                port: 993,
            }),
        }
    }

    fn secrets_imap() -> ResolvedSecrets {
        ResolvedSecrets {
            arlo_password: "arlo_pw".to_string(),
            imap_password: Some("imap_pw".to_string()),
        }
    }

    fn secrets_stdin() -> ResolvedSecrets {
        ResolvedSecrets {
            arlo_password: "arlo_pw".to_string(),
            imap_password: None,
        }
    }

    #[test]
    fn utf8_path_round_trips_normal_path() {
        let p = PathBuf::from("/var/lib/arlo-streamer/session.json");
        let s = utf8_path(&p).expect("ok");
        assert_eq!(s, "/var/lib/arlo-streamer/session.json");
    }

    #[test]
    fn build_rs_arlo_config_email_imap_assembles_correctly() {
        let rs_cfg = build_rs_arlo_config(
            &cfg_email_imap(),
            &secrets_imap(),
            "/tmp/session.json".to_string(),
        );

        let creds = rs_cfg.credentials.expect("credentials present");
        assert_eq!(creds.email.as_deref(), Some("owner@example.com"));
        assert_eq!(creds.password.as_deref(), Some("arlo_pw"));

        let mfa = rs_cfg.mfa.expect("mfa present");
        assert_eq!(mfa.preferred_method.as_deref(), Some("email"));

        let imap = mfa.imap.expect("imap present");
        assert_eq!(imap.host.as_deref(), Some("imap.example.com"));
        assert_eq!(imap.provider, None);
        assert_eq!(imap.port, Some(993));
        assert_eq!(imap.username.as_deref(), Some("u@example.com"));
        assert_eq!(imap.password.as_deref(), Some("imap_pw"));
        assert_eq!(imap.enabled, Some(true));

        let client = rs_cfg.client.expect("client section present");
        assert_eq!(
            client.session_cache_path.as_deref(),
            Some("/tmp/session.json")
        );
        assert_eq!(client.headless, Some(true));
        assert_eq!(client.api_version, Some(rs_config::ApiVersion::V3));
    }

    #[test]
    fn build_rs_arlo_config_sms_omits_imap() {
        let mut c = cfg_email_imap();
        c.mfa = MfaConfig::Sms;
        let rs_cfg = build_rs_arlo_config(&c, &secrets_stdin(), "/tmp/session.json".to_string());

        let mfa = rs_cfg.mfa.expect("mfa present");
        assert_eq!(mfa.preferred_method.as_deref(), Some("sms"));
        assert!(mfa.imap.is_none());
    }

    #[test]
    fn build_rs_arlo_config_push_sets_preferred_method_and_omits_imap() {
        let mut c = cfg_email_imap();
        c.mfa = MfaConfig::Push(PushMfaConfig::default());
        let rs_cfg = build_rs_arlo_config(&c, &secrets_stdin(), "/tmp/session.json".to_string());

        let mfa = rs_cfg.mfa.expect("mfa present");
        assert_eq!(mfa.preferred_method.as_deref(), Some("push"));
        assert!(mfa.imap.is_none());
    }

    #[test]
    fn build_rs_arlo_config_email_stdin_omits_imap() {
        let mut c = cfg_email_imap();
        c.mfa = MfaConfig::Email(EmailMfaConfig {
            host: None,
            provider: None,
            user: None,
            password_env: None,
            port: 993,
        });
        let rs_cfg = build_rs_arlo_config(&c, &secrets_stdin(), "/tmp/session.json".to_string());

        let mfa = rs_cfg.mfa.expect("mfa present");
        assert_eq!(mfa.preferred_method.as_deref(), Some("email"));
        assert!(mfa.imap.is_none());
    }

    #[test]
    fn build_rs_arlo_config_email_provider_shortcut_assembles_imap() {
        let mut c = cfg_email_imap();
        c.mfa = MfaConfig::Email(EmailMfaConfig {
            host: None,
            provider: Some("gmail".to_string()),
            user: Some("u@example.com".to_string()),
            password_env: Some("ARLO_IMAP_PASSWORD".to_string()),
            port: 993,
        });
        let rs_cfg = build_rs_arlo_config(&c, &secrets_imap(), "/tmp/session.json".to_string());

        let mfa = rs_cfg.mfa.expect("mfa present");
        assert_eq!(mfa.preferred_method.as_deref(), Some("email"));
        let imap = mfa.imap.expect("imap present (provider shortcut)");
        assert_eq!(imap.provider.as_deref(), Some("gmail"));
        assert_eq!(imap.host, None);
        assert_eq!(imap.username.as_deref(), Some("u@example.com"));
        assert_eq!(imap.enabled, Some(true));
    }

    #[test]
    fn email_uses_imap_matrix() {
        let base = EmailMfaConfig {
            host: Some("h".to_string()),
            provider: None,
            user: Some("u".to_string()),
            password_env: Some("PW".to_string()),
            port: 993,
        };
        // host-only and provider-only both qualify.
        assert!(email_uses_imap(&base));
        assert!(email_uses_imap(&EmailMfaConfig {
            host: None,
            provider: Some("gmail".to_string()),
            ..base.clone()
        }));
        // Missing mailbox locator, user, or password each disqualify.
        assert!(!email_uses_imap(&EmailMfaConfig {
            host: None,
            provider: None,
            ..base.clone()
        }));
        assert!(!email_uses_imap(&EmailMfaConfig {
            user: None,
            ..base.clone()
        }));
        assert!(!email_uses_imap(&EmailMfaConfig {
            password_env: None,
            ..base.clone()
        }));
    }

    #[test]
    fn build_rs_arlo_config_propagates_session_path() {
        let rs_cfg = build_rs_arlo_config(
            &cfg_email_imap(),
            &secrets_imap(),
            "/etc/arlo-streamer/session.json".to_string(),
        );
        let client = rs_cfg.client.expect("client section present");
        assert_eq!(
            client.session_cache_path.as_deref(),
            Some("/etc/arlo-streamer/session.json")
        );
    }
}
