//! Boot the [`ArloClient`] from a domain [`ArloConfig`] and return a
//! shared, authenticated handle.
//!
//! Boot sequence:
//!
//! 1. **Build** the client via [`ArloClient::builder`], pointing at
//!    the configured `session_cache_path`. rs-arlo auto-loads any
//!    existing token snapshot on disk.
//! 2. **Resolve secrets** from the env vars named in
//!    `ArloConfig::password_env` and `ImapMfaConfig::password_env`.
//!    Missing env vars are surfaced as [`DomainError::InvalidConfig`].
//! 3. **Authenticate** if [`ArloClient::is_authenticated`] is `false`
//!    (cold start or expired session): drive the configured MFA
//!    handler — `MfaConfig::Imap` for headless production,
//!    `MfaConfig::Stdin` for first-time setup.
//! 4. Wrap the authenticated client in [`std::sync::Arc`] and return.
//!
//! After this returns, the `Arc<ArloClient>` is shared by the three
//! port adapters; only `&self` methods are called from there on, so no
//! `Mutex` / `RwLock` is required.
//!
//! [`ImapMfaConfig`]: streamer_domain::config::ImapMfaConfig
//! [`MfaConfig`]: streamer_domain::config::MfaConfig

use std::env;
use std::path::Path;
use std::sync::Arc;

use rs_arlo::client::ArloClient;
use rs_arlo::config as rs_config;
use tracing::{info, warn};

use streamer_domain::config::{ArloConfig, MfaConfig};
use streamer_domain::error::DomainError;

use crate::error::arlo_to_domain;

/// Resolved secrets pulled from env vars during boot. Held only for
/// the duration of the auth handshake; never persisted by this crate.
struct ResolvedSecrets {
    arlo_password: String,
    /// `Some` when MFA is `Imap`; `None` for `Stdin`.
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
        MfaConfig::Imap(_) => {
            client
                .authenticate_with_imap(&rs_config)
                .await
                .map_err(arlo_to_domain)?;
        }
        MfaConfig::Stdin => {
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
        MfaConfig::Imap(imap) => Some(read_env(&imap.password_env)?),
        MfaConfig::Stdin => None,
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
        (MfaConfig::Imap(imap), Some(pw)) => rs_config::MfaConfig {
            preferred_method: Some("imap".to_string()),
            imap: Some(rs_config::ImapConfig {
                enabled: Some(true),
                provider: None,
                host: Some(imap.host.clone()),
                port: Some(imap.port),
                username: Some(imap.user.clone()),
                password: Some(pw.clone()),
                delete_after_read: Some(false),
            }),
        },
        // `MfaConfig::Stdin`, or the (Imap, None) shape that
        // [`resolve_secrets`] never produces. The latter is
        // statically unreachable but we keep the arm explicit
        // rather than `unreachable!()` so a future refactor that
        // breaks the invariant fails closed.
        _ => rs_config::MfaConfig {
            preferred_method: Some("stdin".to_string()),
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
        }),
        streaming: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use streamer_domain::config::ImapMfaConfig;

    fn cfg() -> ArloConfig {
        ArloConfig {
            email: "owner@example.com".to_string(),
            password_env: "ARLO_PASSWORD".to_string(),
            session_cache_path: PathBuf::from("/tmp/session.json"),
            mfa: MfaConfig::Imap(ImapMfaConfig {
                host: "imap.example.com".to_string(),
                user: "u@example.com".to_string(),
                password_env: "ARLO_IMAP_PASSWORD".to_string(),
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
    fn build_rs_arlo_config_imap_assembles_correctly() {
        let rs_cfg = build_rs_arlo_config(&cfg(), &secrets_imap(), "/tmp/session.json".to_string());

        let creds = rs_cfg.credentials.expect("credentials present");
        assert_eq!(creds.email.as_deref(), Some("owner@example.com"));
        assert_eq!(creds.password.as_deref(), Some("arlo_pw"));

        let mfa = rs_cfg.mfa.expect("mfa present");
        assert_eq!(mfa.preferred_method.as_deref(), Some("imap"));

        let imap = mfa.imap.expect("imap present");
        assert_eq!(imap.host.as_deref(), Some("imap.example.com"));
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
    }

    #[test]
    fn build_rs_arlo_config_stdin_omits_imap() {
        let mut c = cfg();
        c.mfa = MfaConfig::Stdin;
        let rs_cfg = build_rs_arlo_config(&c, &secrets_stdin(), "/tmp/session.json".to_string());

        let mfa = rs_cfg.mfa.expect("mfa present");
        assert_eq!(mfa.preferred_method.as_deref(), Some("stdin"));
        assert!(mfa.imap.is_none());
    }

    #[test]
    fn build_rs_arlo_config_propagates_session_path() {
        let rs_cfg = build_rs_arlo_config(
            &cfg(),
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
