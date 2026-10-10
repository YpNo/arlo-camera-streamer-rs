//! Camera state over MQTT for Home Assistant (ADR 0009). Publish-only:
//! nothing on the broker can act on a camera.
//!
//! [`MqttSetup::new`] checks the settings and builds the TLS setup at boot,
//! before the Arlo login; [`MqttSetup::spawn`] runs the bridge once the
//! system is up. The bridge reads the [`AdminControl`] snapshot every
//! second (the watch channels the camera tasks publish; no camera task is
//! asked anything) and publishes what changed. On every connect it
//! publishes `online`, the discovery documents, the removal of cameras no
//! longer served and the full state; a lost or refused connection is
//! retried after 1, 2, 4 … 60 s. A broker that is down never stops or
//! slows the daemon.

mod messages;
mod record;
mod session;

use std::collections::{BTreeSet, HashSet};
use std::net::IpAddr;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use secrecy::SecretString;
use tokio::task::JoinHandle;
use tokio::time::{MissedTickBehavior, interval};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use streamer_domain::camera::CameraId;
use streamer_domain::config::MqttConfig;
use streamer_domain::port::AdminControl;

use messages::{Layout, Message, OFFLINE, ONLINE, Written};
use record::PublishedRecord;
pub use record::RECORD_FILE;
use session::{ConnectParams, KEEP_ALIVE_SECS, TcpBroker};

/// How often the snapshot is read.
const STATE_PERIOD: Duration = Duration::from_secs(1);
/// How often the idle pictures are checked, when published.
const THUMBNAIL_PERIOD: Duration = Duration::from_secs(30);
/// Largest idle picture published: EMQX drops a client over its 1 MiB
/// default packet size.
const MAX_THUMBNAIL_BYTES: usize = 512 * 1024;
/// First and longest pause before reconnecting.
const FIRST_RETRY: Duration = Duration::from_secs(1);
const LONGEST_RETRY: Duration = Duration::from_secs(60);

/// Why the broker could not be reached or used. Never carries a password.
#[derive(Debug, thiserror::Error)]
pub enum MqttError {
    /// The `[mqtt]` settings.
    #[error("configuration: {0}")]
    Config(String),
    /// TCP: refused, reset, closed.
    #[error("connection: {0}")]
    Io(String),
    /// TLS setup or handshake.
    #[error("TLS: {0}")]
    Tls(String),
    /// The broker's CONNACK refusal (bad credentials, not authorized, …).
    #[error("refused by the broker: {0}")]
    Refused(String),
    /// A packet that could not be encoded or was not expected.
    #[error("protocol: {0}")]
    Protocol(String),
    /// No answer within the step's time.
    #[error("no answer in time: {0}")]
    Timeout(&'static str),
    /// The daemon's own state could not be read.
    #[error("daemon state: {0}")]
    Source(String),
}

/// Opens connections to the broker.
#[async_trait]
pub(crate) trait Broker: Send + Sync {
    async fn connect(&self) -> Result<Box<dyn Session>, MqttError>;
}

/// One accepted connection.
#[async_trait]
pub(crate) trait Session: Send {
    /// Publish `message`, retained, at `QoS` 0.
    async fn publish(&mut self, message: &Message) -> Result<(), MqttError>;
    /// Ping the broker and wait for its answer.
    async fn keep_alive(&mut self) -> Result<(), MqttError>;
    /// Close cleanly; best effort.
    async fn disconnect(&mut self);
}

/// The MQTT adapter, checked at boot and started once the system runs.
pub struct MqttSetup {
    broker: Box<dyn Broker>,
    layout: Layout,
    thumbnails: bool,
    record: PublishedRecord,
    version: String,
}

impl MqttSetup {
    /// Check `config` and build the connection setup. `password` comes from
    /// the env var `config.password_env` names; `ca_pem` is the content of
    /// `config.ca_file`; the published-cameras record goes in `state_dir`.
    ///
    /// # Errors
    ///
    /// [`MqttError::Config`] for a bad URL, [`MqttError::Tls`] when the TLS
    /// setup cannot be built (no root certificate, an unreadable CA).
    pub fn new(
        config: &MqttConfig,
        password: Option<SecretString>,
        ca_pem: Option<&[u8]>,
        state_dir: &Path,
        version: &str,
    ) -> Result<Self, MqttError> {
        let endpoint = config
            .endpoint()
            .map_err(|e| MqttError::Config(e.to_string()))?;
        let tls = endpoint
            .tls
            .then(|| session::tls_config(ca_pem))
            .transpose()?;
        if !endpoint.tls && password.is_some() && !is_loopback(&endpoint.host) {
            warn!(
                host = %endpoint.host,
                "the MQTT password goes in clear over mqtt://; use mqtts:// if the broker offers it"
            );
        }
        let layout = Layout::new(config);
        let params = ConnectParams {
            client_id: layout.client_id(),
            username: config.username.clone(),
            password,
            will_topic: layout.status(),
            will_payload: OFFLINE,
        };
        Ok(Self {
            broker: Box::new(TcpBroker::new(endpoint, tls, params)?),
            layout,
            thumbnails: config.thumbnails,
            record: PublishedRecord::in_dir(state_dir),
            version: version.to_string(),
        })
    }

    /// Run the bridge until `stop` is cancelled: then it publishes
    /// `offline` and disconnects. Cancel it after the cameras are released.
    #[must_use]
    pub fn spawn(self, admin: Arc<dyn AdminControl>, stop: CancellationToken) -> JoinHandle<()> {
        let bridge = Bridge {
            broker: self.broker,
            layout: self.layout,
            thumbnails: self.thumbnails,
            record: self.record,
            version: self.version,
            admin,
            written: Written::default(),
            cameras: Vec::new(),
            oversized: HashSet::new(),
        };
        tokio::spawn(bridge.run(stop))
    }
}

fn is_loopback(host: &str) -> bool {
    host == "localhost" || host.parse::<IpAddr>().is_ok_and(|ip| ip.is_loopback())
}

/// The pause before the next connection attempt: doubling, capped.
#[derive(Debug)]
struct Backoff(Duration);

impl Backoff {
    const fn new() -> Self {
        Self(FIRST_RETRY)
    }

    fn next(&mut self) -> Duration {
        let wait = self.0;
        self.0 = (self.0 * 2).min(LONGEST_RETRY);
        wait
    }
}

struct Bridge {
    broker: Box<dyn Broker>,
    layout: Layout,
    thumbnails: bool,
    record: PublishedRecord,
    version: String,
    admin: Arc<dyn AdminControl>,
    written: Written,
    /// The cameras of the last snapshot read at connect.
    cameras: Vec<CameraId>,
    /// Cameras whose picture was too big, warned about once.
    oversized: HashSet<CameraId>,
}

impl Bridge {
    async fn run(mut self, stop: CancellationToken) {
        let mut backoff = Backoff::new();
        let mut failing: Option<String> = None;
        loop {
            let attempt = tokio::select! {
                biased;
                () = stop.cancelled() => return,
                attempt = self.broker.connect() => attempt,
            };
            match attempt {
                Ok(mut session) => {
                    info!(
                        broker_was_unavailable = failing.is_some(),
                        "MQTT broker connected"
                    );
                    failing = None;
                    backoff = Backoff::new();
                    let served = self.serve(session.as_mut(), &stop).await;
                    session.disconnect().await;
                    match served {
                        Ok(()) => return,
                        Err(e) => report(&mut failing, &e),
                    }
                }
                Err(e) => report(&mut failing, &e),
            }
            let wait = backoff.next();
            debug!(wait_secs = wait.as_secs(), "MQTT: next attempt");
            tokio::select! {
                biased;
                () = stop.cancelled() => return,
                () = tokio::time::sleep(wait) => {}
            }
        }
    }

    /// One connection: announce, then publish what changes until `stop`
    /// (`offline`, then `Ok`) or a failure.
    async fn serve(
        &mut self,
        session: &mut dyn Session,
        stop: &CancellationToken,
    ) -> Result<(), MqttError> {
        self.written.clear();
        self.announce(session).await?;
        let mut state = interval(STATE_PERIOD);
        let mut ping = interval(Duration::from_secs(u64::from(KEEP_ALIVE_SECS / 2)));
        let mut pictures = interval(THUMBNAIL_PERIOD);
        for timer in [&mut state, &mut ping, &mut pictures] {
            timer.set_missed_tick_behavior(MissedTickBehavior::Skip);
            timer.tick().await;
        }
        loop {
            tokio::select! {
                biased;
                () = stop.cancelled() => {
                    return session.publish(&messages::status(&self.layout, OFFLINE)).await;
                }
                _ = ping.tick() => session.keep_alive().await?,
                _ = state.tick() => self.publish_state(session).await?,
                _ = pictures.tick(), if self.thumbnails => self.publish_thumbnails(session).await?,
            }
        }
    }

    /// After a connect: `online`, discovery, the removal of cameras no
    /// longer served, then the full state.
    async fn announce(&mut self, session: &mut dyn Session) -> Result<(), MqttError> {
        let snapshot = self
            .admin
            .snapshot()
            .await
            .map_err(|e| MqttError::Source(e.to_string()))?;
        self.send(session, messages::status(&self.layout, ONLINE))
            .await?;
        for message in messages::bridge_discovery(&self.layout, &self.version) {
            self.send(session, message).await?;
        }
        for camera in &snapshot.cameras {
            for message in messages::camera_discovery(&self.layout, camera, self.thumbnails) {
                self.send(session, message).await?;
            }
        }
        self.cameras = snapshot.cameras.iter().map(|c| c.id.clone()).collect();
        self.remove_departed(session).await?;
        self.publish_state(session).await?;
        if self.thumbnails {
            self.publish_thumbnails(session).await?;
        }
        Ok(())
    }

    /// Clear every camera published before but no longer served, then
    /// record the ones served now.
    async fn remove_departed(&mut self, session: &mut dyn Session) -> Result<(), MqttError> {
        let current: BTreeSet<CameraId> = self.cameras.iter().cloned().collect();
        for gone in self.record.load().await.difference(&current) {
            info!(camera = %gone, "MQTT: clearing a camera no longer served");
            for message in messages::removal(&self.layout, gone) {
                self.send(session, message).await?;
            }
        }
        if let Err(e) = self.record.save(&current).await {
            warn!(error = %e, "MQTT published record not saved");
        }
        Ok(())
    }

    async fn publish_state(&mut self, session: &mut dyn Session) -> Result<(), MqttError> {
        let snapshot = match self.admin.snapshot().await {
            Ok(snapshot) => snapshot,
            Err(e) => {
                debug!(error = %e, "MQTT: no snapshot this tick");
                return Ok(());
            }
        };
        let bridge = messages::bridge_state(&self.layout, snapshot.arlo_connected, &self.version);
        self.send(session, bridge).await?;
        for camera in &snapshot.cameras {
            self.send(session, messages::camera_state(&self.layout, camera))
                .await?;
        }
        Ok(())
    }

    async fn publish_thumbnails(&mut self, session: &mut dyn Session) -> Result<(), MqttError> {
        for camera in self.cameras.clone() {
            let Ok(Some(jpeg)) = self.admin.thumbnail(&camera).await else {
                continue;
            };
            if jpeg.len() > MAX_THUMBNAIL_BYTES {
                if self.oversized.insert(camera.clone()) {
                    warn!(
                        %camera,
                        bytes = jpeg.len(),
                        "idle picture too big for MQTT ({MAX_THUMBNAIL_BYTES} bytes at most); not published"
                    );
                }
                continue;
            }
            self.send(session, messages::thumbnail(&self.layout, &camera, jpeg))
                .await?;
        }
        Ok(())
    }

    /// Publish `message` when it differs from what was last written.
    async fn send(&mut self, session: &mut dyn Session, message: Message) -> Result<(), MqttError> {
        if self.written.is_due(&message) {
            session.publish(&message).await?;
            self.written.record(&message);
        }
        Ok(())
    }
}

/// Warn when the broker's trouble starts or changes; repeats go to debug.
fn report(failing: &mut Option<String>, e: &MqttError) {
    let text = e.to_string();
    if failing.as_deref() == Some(text.as_str()) {
        debug!(error = %text, "MQTT broker still unavailable");
    } else {
        warn!(error = %text, "MQTT broker unavailable; retrying with a growing pause (state publishing paused)");
    }
    *failing = Some(text);
}

#[cfg(test)]
mod tests;
