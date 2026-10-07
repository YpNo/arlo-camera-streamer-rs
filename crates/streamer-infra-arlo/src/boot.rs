//! Boot the [`ArloClient`] from a domain [`ArloConfig`] and return a
//! shared, authenticated handle.
//!
//! Boot sequence:
//!
//! 1. **Build** the client via [`ArloClient::builder`], pointing at
//!    the configured `session_cache_path`. arlo-rs auto-loads any
//!    existing token snapshot on disk. The cache's parent directory is
//!    created first (owner-only): arlo-rs writes the file with a
//!    temp-file + rename inside that directory and never creates it,
//!    so a missing directory silently costs an OTP on every restart.
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

use arlo_rs::client::ArloClient;
use arlo_rs::config as rs_config;
use tracing::{info, warn};

/// Owner-only mode for the directory holding the session token.
#[cfg(unix)]
const SESSION_CACHE_DIR_MODE: u32 = 0o700;

use arlo_rs::secrecy::SecretString;
use streamer_domain::config::{ArloConfig, EmailMfaConfig, MfaConfig};
use streamer_domain::error::DomainError;

use crate::error::arlo_to_domain;

/// Resolved secrets pulled from env vars during boot. Held only for
/// the duration of the auth handshake; never persisted by this crate.
struct ResolvedSecrets {
    arlo_password: SecretString,
    /// `Some` when MFA is `Email` and IMAP fields are configured.
    imap_password: Option<SecretString>,
}

/// Boot an authenticated [`ArloClient`] from a domain [`ArloConfig`].
///
/// # Errors
///
/// - [`DomainError::InvalidConfig`] when a required env var is missing
///   or `session_cache_path` is not valid UTF-8.
/// - [`DomainError::AdapterTransport`] when the arlo-rs build /
///   authentication call fails (network, credentials rejected,
///   IMAP unreachable).
pub async fn boot(config: &ArloConfig) -> Result<Arc<ArloClient>, DomainError> {
    let session_cache_path = utf8_path(&config.session_cache_path)?;
    ensure_cache_dir(&config.session_cache_path).await?;

    let mut client = ArloClient::builder()
        .headless(true)
        .session_cache(session_cache_path.clone())
        .build()
        .await
        .map_err(arlo_to_domain)?;

    if client.is_authenticated() {
        info!("arlo-rs session restored from cache");
        return Ok(Arc::new(client));
    }

    warn!("no valid cached session — running MFA cold-start");
    let secrets = resolve_secrets(config)?;
    let rs_config = build_arlo_rs_config(config, secrets, session_cache_path);

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
        MfaConfig::Email(_) | MfaConfig::Sms(_) => {
            client
                .authenticate_with_handler(&rs_config, arlo_rs::client::mfa::StdinMfaHandler)
                .await
                .map_err(arlo_to_domain)?;
        }
    }

    info!("arlo-rs authentication complete");
    Ok(Arc::new(client))
}

/// Create the session cache's parent directory when it is missing, with
/// owner-only permissions (the file inside carries the Arlo token and
/// cookie jar). An existing directory is left untouched, whatever its
/// mode — it may be a shared system path the deployer manages.
///
/// # Errors
///
/// [`DomainError::InvalidConfig`] when the directory cannot be created
/// (permissions, a file in the way): the daemon would otherwise run but
/// re-ask the second factor on every restart, which is a configuration
/// problem worth failing on at boot.
async fn ensure_cache_dir(cache_path: &Path) -> Result<(), DomainError> {
    let Some(parent) = cache_path.parent().filter(|p| !p.as_os_str().is_empty()) else {
        return Ok(());
    };
    if let Ok(meta) = tokio::fs::metadata(parent).await {
        if meta.is_dir() {
            return Ok(());
        }
        return Err(DomainError::InvalidConfig(format!(
            "session_cache_path directory {} exists but is not a directory",
            parent.display()
        )));
    }
    let mut builder = tokio::fs::DirBuilder::new();
    builder.recursive(true);
    #[cfg(unix)]
    builder.mode(SESSION_CACHE_DIR_MODE);
    builder.create(parent).await.map_err(|e| {
        DomainError::InvalidConfig(format!(
            "session_cache_path directory {} cannot be created: {e}",
            parent.display()
        ))
    })?;
    // `recursive` creation applies the mode to the leaf only through the
    // umask; set it explicitly, and refuse to go on with a world-readable
    // token directory — the rationale above is the daemon's, not a hint.
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        tokio::fs::set_permissions(
            parent,
            std::fs::Permissions::from_mode(SESSION_CACHE_DIR_MODE),
        )
        .await
        .map_err(|e| {
            DomainError::InvalidConfig(format!(
                "session_cache_path directory {} cannot be restricted to the owner: {e}",
                parent.display()
            ))
        })?;
    }
    info!(path = %parent.display(), "created session cache directory");
    Ok(())
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
        MfaConfig::Sms(_) | MfaConfig::Push(_) => None,
    };
    Ok(ResolvedSecrets {
        arlo_password,
        imap_password,
    })
}

/// Read a secret from the environment straight into a [`SecretString`]
/// (zeroed on drop); the plain `String` lives only inside this call.
fn read_env(name: &str) -> Result<SecretString, DomainError> {
    env::var(name)
        .map(SecretString::from)
        .map_err(|_| DomainError::InvalidConfig(format!("env var {name} required but not set")))
}

/// IMAP auto-retrieval is viable only when the mailbox is locatable
/// (an explicit `host` *or* a `provider` shortcut) **and** a user and
/// password env var are configured. Any other shape falls back to a
/// stdin OTP prompt. Single source of truth for the `boot` dispatch
/// and the `build_arlo_rs_config` assembly so the two can't diverge.
fn email_uses_imap(email: &EmailMfaConfig) -> bool {
    (email.host.is_some() || email.provider.is_some())
        && email.user.is_some()
        && email.password_env.is_some()
}

/// Pure (no I/O) assembly of an `arlo_rs::config::ArloConfig` from
/// the domain config + already-resolved secrets + the UTF-8'd
/// session-cache path. Split out from [`boot`] so it's easy to test.
fn build_arlo_rs_config(
    cfg: &ArloConfig,
    secrets: ResolvedSecrets,
    session_cache_path: String,
) -> rs_config::ArloConfig {
    // The secrets are moved, never cloned: one copy in memory, dropped
    // (and zeroed) with the arlo-rs config.
    let ResolvedSecrets {
        arlo_password,
        imap_password,
    } = secrets;
    let credentials = rs_config::CredentialsConfig {
        email: Some(cfg.email.clone()),
        password: Some(arlo_password),
    };

    let mfa = match (&cfg.mfa, imap_password) {
        (MfaConfig::Email(email), Some(pw)) if email_uses_imap(email) => rs_config::MfaConfig {
            preferred_method: Some("email".to_string()),
            imap: Some(rs_config::ImapConfig {
                enabled: Some(true),
                provider: email.provider.clone(),
                host: email.host.clone(),
                port: Some(email.port),
                username: email.user.clone(),
                password: Some(pw),
                delete_after_read: Some(false),
            }),
        },
        (MfaConfig::Email(_), _) => rs_config::MfaConfig {
            preferred_method: Some("email".to_string()),
            imap: None,
        },
        (MfaConfig::Sms(_), _) => rs_config::MfaConfig {
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
            // Default transport is the browser-less `wreq` client; opt-in only.
            use_browser: None,
            upstream_proxy: None,
            // Pin to the modern v3 API. arlo-rs auto-falls-back to
            // Legacy at runtime if the v2 device endpoints 403/404.
            api_version: Some(rs_config::ApiVersion::V3),
        }),
        streaming: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arlo_rs::secrecy::ExposeSecret;
    use std::path::PathBuf;
    use streamer_domain::config::{EmailMfaConfig, PushMfaConfig};

    fn cfg_email_imap() -> ArloConfig {
        ArloConfig {
            email: "owner@example.com".to_string(),
            password_env: "ARLO_PASSWORD".to_string(),
            session_cache_path: PathBuf::from("/tmp/session.json"),
            app_version: "6.46.0".to_string(),
            watch_along_cert_sha256: None,
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
            arlo_password: SecretString::from("arlo_pw"),
            imap_password: Some(SecretString::from("imap_pw")),
        }
    }

    fn secrets_stdin() -> ResolvedSecrets {
        ResolvedSecrets {
            arlo_password: SecretString::from("arlo_pw"),
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
    fn build_arlo_rs_config_email_imap_assembles_correctly() {
        let rs_cfg = build_arlo_rs_config(
            &cfg_email_imap(),
            secrets_imap(),
            "/tmp/session.json".to_string(),
        );

        let creds = rs_cfg.credentials.expect("credentials present");
        assert_eq!(creds.email.as_deref(), Some("owner@example.com"));
        assert_eq!(
            creds.password.as_ref().map(ExposeSecret::expose_secret),
            Some("arlo_pw")
        );

        let mfa = rs_cfg.mfa.expect("mfa present");
        assert_eq!(mfa.preferred_method.as_deref(), Some("email"));

        let imap = mfa.imap.expect("imap present");
        assert_eq!(imap.host.as_deref(), Some("imap.example.com"));
        assert_eq!(imap.provider, None);
        assert_eq!(imap.port, Some(993));
        assert_eq!(imap.username.as_deref(), Some("u@example.com"));
        assert_eq!(
            imap.password.as_ref().map(ExposeSecret::expose_secret),
            Some("imap_pw")
        );
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
    fn build_arlo_rs_config_sms_omits_imap() {
        let mut c = cfg_email_imap();
        c.mfa = MfaConfig::Sms(streamer_domain::config::SmsMfaConfig::default());
        let rs_cfg = build_arlo_rs_config(&c, secrets_stdin(), "/tmp/session.json".to_string());

        let mfa = rs_cfg.mfa.expect("mfa present");
        assert_eq!(mfa.preferred_method.as_deref(), Some("sms"));
        assert!(mfa.imap.is_none());
    }

    #[test]
    fn build_arlo_rs_config_push_sets_preferred_method_and_omits_imap() {
        let mut c = cfg_email_imap();
        c.mfa = MfaConfig::Push(PushMfaConfig::default());
        let rs_cfg = build_arlo_rs_config(&c, secrets_stdin(), "/tmp/session.json".to_string());

        let mfa = rs_cfg.mfa.expect("mfa present");
        assert_eq!(mfa.preferred_method.as_deref(), Some("push"));
        assert!(mfa.imap.is_none());
    }

    #[test]
    fn build_arlo_rs_config_email_stdin_omits_imap() {
        let mut c = cfg_email_imap();
        c.mfa = MfaConfig::Email(EmailMfaConfig {
            host: None,
            provider: None,
            user: None,
            password_env: None,
            port: 993,
        });
        let rs_cfg = build_arlo_rs_config(&c, secrets_stdin(), "/tmp/session.json".to_string());

        let mfa = rs_cfg.mfa.expect("mfa present");
        assert_eq!(mfa.preferred_method.as_deref(), Some("email"));
        assert!(mfa.imap.is_none());
    }

    #[test]
    fn build_arlo_rs_config_email_provider_shortcut_assembles_imap() {
        let mut c = cfg_email_imap();
        c.mfa = MfaConfig::Email(EmailMfaConfig {
            host: None,
            provider: Some("gmail".to_string()),
            user: Some("u@example.com".to_string()),
            password_env: Some("ARLO_IMAP_PASSWORD".to_string()),
            port: 993,
        });
        let rs_cfg = build_arlo_rs_config(&c, secrets_imap(), "/tmp/session.json".to_string());

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
    fn build_arlo_rs_config_propagates_session_path() {
        let rs_cfg = build_arlo_rs_config(
            &cfg_email_imap(),
            secrets_imap(),
            "/etc/arlo-streamer/session.json".to_string(),
        );
        let client = rs_cfg.client.expect("client section present");
        assert_eq!(
            client.session_cache_path.as_deref(),
            Some("/etc/arlo-streamer/session.json")
        );
    }

    // ---------- session cache directory ----------

    fn scratch_dir(tag: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!("streamer-boot-{tag}-{}", std::process::id()))
    }

    #[tokio::test]
    async fn ensure_cache_dir_creates_missing_nested_parent_owner_only() {
        let root = scratch_dir("nested");
        let cache = root.join("deeper").join("session.json");
        ensure_cache_dir(&cache).await.expect("directory created");
        let meta = std::fs::metadata(cache.parent().unwrap()).expect("exists");
        assert!(meta.is_dir());
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(meta.permissions().mode() & 0o777, 0o700);
        }
        std::fs::remove_dir_all(&root).ok();
    }

    #[tokio::test]
    async fn ensure_cache_dir_leaves_existing_directory_and_mode_alone() {
        let root = scratch_dir("existing");
        std::fs::create_dir_all(&root).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o755)).unwrap();
        }
        ensure_cache_dir(&root.join("session.json"))
            .await
            .expect("existing directory is fine");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(&root).unwrap().permissions().mode() & 0o777;
            assert_eq!(mode, 0o755, "an existing directory must keep its mode");
        }
        std::fs::remove_dir_all(&root).ok();
    }

    #[tokio::test]
    async fn ensure_cache_dir_fails_when_a_file_blocks_the_directory() {
        let root = scratch_dir("blocked");
        std::fs::create_dir_all(&root).unwrap();
        let blocker = root.join("not-a-dir");
        std::fs::write(&blocker, b"x").unwrap();
        let err = ensure_cache_dir(&blocker.join("session.json"))
            .await
            .expect_err("a file in the way must fail");
        assert!(matches!(err, DomainError::InvalidConfig(_)));
        std::fs::remove_dir_all(&root).ok();
    }

    #[tokio::test]
    async fn ensure_cache_dir_accepts_bare_filename() {
        ensure_cache_dir(Path::new("session.json"))
            .await
            .expect("no parent means nothing to create");
    }
}
