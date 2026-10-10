//! The bridge against a scripted broker (paused clock), and the real
//! session against a minimal broker on loopback.

use std::collections::{HashMap, VecDeque};
use std::sync::Mutex;

use bytes::{Bytes, BytesMut};
use mqttbytes::v4::{ConnAck, ConnectReturnCode, Packet, PingResp};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::time::Instant;

use super::*;
use streamer_domain::admin::{AdminError, CameraSnapshot, SystemSnapshot};
use streamer_domain::camera::StreamName;
use streamer_domain::config::MqttEndpoint;

// ---------------------------------------------------------------- fakes

#[derive(Debug, Clone, PartialEq, Eq)]
enum Event {
    Connect,
    Publish(String, Bytes),
    Ping,
    Disconnect,
}

/// How the next connection attempt goes.
#[derive(Debug, Clone, Copy)]
enum Plan {
    Refuse,
    /// Accepted; publishes fail after this many succeeded.
    Accept(usize),
}

#[derive(Default)]
struct Log {
    events: Mutex<Vec<(Instant, Event)>>,
}

impl Log {
    fn push(&self, event: Event) {
        self.events.lock().unwrap().push((Instant::now(), event));
    }

    fn events(&self) -> Vec<Event> {
        self.events
            .lock()
            .unwrap()
            .iter()
            .map(|(_, e)| e.clone())
            .collect()
    }

    fn connect_times(&self) -> Vec<Instant> {
        let events = self.events.lock().unwrap();
        events
            .iter()
            .filter(|(_, e)| *e == Event::Connect)
            .map(|(at, _)| *at)
            .collect()
    }

    fn published(&self) -> Vec<(String, Bytes)> {
        self.events()
            .into_iter()
            .filter_map(|e| match e {
                Event::Publish(topic, payload) => Some((topic, payload)),
                _ => None,
            })
            .collect()
    }
}

struct FakeBroker {
    plans: Mutex<VecDeque<Plan>>,
    log: Arc<Log>,
}

#[async_trait]
impl Broker for FakeBroker {
    async fn connect(&self) -> Result<Box<dyn Session>, MqttError> {
        let plan = self
            .plans
            .lock()
            .unwrap()
            .pop_front()
            .unwrap_or(Plan::Accept(usize::MAX));
        self.log.push(Event::Connect);
        match plan {
            Plan::Refuse => Err(MqttError::Io("connection refused".into())),
            Plan::Accept(budget) => Ok(Box::new(FakeSession {
                budget,
                log: self.log.clone(),
            })),
        }
    }
}

struct FakeSession {
    budget: usize,
    log: Arc<Log>,
}

#[async_trait]
impl Session for FakeSession {
    async fn publish(&mut self, message: &Message) -> Result<(), MqttError> {
        if self.budget == 0 {
            return Err(MqttError::Io("connection reset".into()));
        }
        self.budget -= 1;
        self.log.push(Event::Publish(
            message.topic.clone(),
            message.payload.clone(),
        ));
        Ok(())
    }

    async fn keep_alive(&mut self) -> Result<(), MqttError> {
        self.log.push(Event::Ping);
        Ok(())
    }

    async fn disconnect(&mut self) {
        self.log.push(Event::Disconnect);
    }
}

#[derive(Default)]
struct FakeAdmin {
    states: Mutex<HashMap<String, String>>,
    thumbnails: Mutex<HashMap<CameraId, Bytes>>,
}

impl FakeAdmin {
    fn with(cameras: &[&str]) -> Arc<Self> {
        let admin = Self::default();
        for id in cameras {
            admin.set(id, "idle");
        }
        Arc::new(admin)
    }

    fn set(&self, camera: &str, state: &str) {
        self.states
            .lock()
            .unwrap()
            .insert(camera.to_string(), state.to_string());
    }

    fn snapshot_of(id: &str, state: &str) -> CameraSnapshot {
        CameraSnapshot {
            id: CameraId::new(id),
            stream_name: StreamName::parse(id.to_lowercase()).unwrap(),
            state: state.to_string(),
            live_secs_today: 0,
            daily_budget_secs: 0,
            live_source: None,
            last_failure: None,
            retries: 0,
            user_view: false,
        }
    }
}

#[async_trait]
impl AdminControl for FakeAdmin {
    async fn snapshot(&self) -> Result<SystemSnapshot, AdminError> {
        let states = self.states.lock().unwrap();
        let mut cameras: Vec<CameraSnapshot> = states
            .iter()
            .map(|(id, state)| Self::snapshot_of(id, state))
            .collect();
        cameras.sort_by(|a, b| a.id.cmp(&b.id));
        Ok(SystemSnapshot {
            version: "test".into(),
            uptime_secs: 1,
            arlo_connected: true,
            cameras,
        })
    }
    async fn camera_snapshot(&self, camera: &CameraId) -> Result<CameraSnapshot, AdminError> {
        Err(AdminError::UnknownCamera(camera.clone()))
    }
    async fn force_idle(&self, _camera: &CameraId) -> Result<(), AdminError> {
        unreachable!("the MQTT bridge never acts on a camera")
    }
    async fn thumbnail(&self, camera: &CameraId) -> Result<Option<Bytes>, AdminError> {
        Ok(self.thumbnails.lock().unwrap().get(camera).cloned())
    }
    async fn manual_wake(&self, _camera: &CameraId) -> Result<(), AdminError> {
        unreachable!("the MQTT bridge never acts on a camera")
    }
}

fn config(thumbnails: bool) -> MqttConfig {
    MqttConfig {
        url: "mqtt://broker".into(),
        username: None,
        password_env: None,
        ca_file: None,
        base_topic: "arlo".into(),
        discovery_prefix: "homeassistant".into(),
        thumbnails,
    }
}

fn scratch(name: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("mqtt-bridge-{name}-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// The bridge on a fake broker that follows `plans`; returns the log, the
/// stop token and the task.
fn start(
    plans: &[Plan],
    admin: Arc<FakeAdmin>,
    thumbnails: bool,
    dir: &Path,
) -> (Arc<Log>, CancellationToken, JoinHandle<()>) {
    let log = Arc::new(Log::default());
    let setup = MqttSetup {
        broker: Box::new(FakeBroker {
            plans: Mutex::new(plans.iter().copied().collect()),
            log: log.clone(),
        }),
        layout: Layout::new(&config(thumbnails)),
        thumbnails,
        record: PublishedRecord::in_dir(dir),
        version: "0.3.0".into(),
    };
    let stop = CancellationToken::new();
    let task = setup.spawn(admin, stop.clone());
    (log, stop, task)
}

async fn stop_and_join(stop: &CancellationToken, task: JoinHandle<()>) {
    stop.cancel();
    tokio::time::timeout(Duration::from_secs(5), task)
        .await
        .expect("the bridge stops promptly")
        .unwrap();
}

fn topics(published: &[(String, Bytes)]) -> Vec<&str> {
    published.iter().map(|(t, _)| t.as_str()).collect()
}

// ---------------------------------------------------------------- bridge

#[tokio::test(start_paused = true)]
async fn a_connection_announces_online_discovery_then_state_and_ends_offline() {
    let dir = scratch("announce");
    let (log, stop, task) = start(&[], FakeAdmin::with(&["CAM1"]), false, &dir);

    tokio::time::sleep(Duration::from_secs(3)).await;
    stop_and_join(&stop, task).await;

    let events = log.events();
    assert_eq!(events.first(), Some(&Event::Connect));
    assert_eq!(events.last(), Some(&Event::Disconnect));
    let published = log.published();
    let topics = topics(&published);
    assert_eq!(topics[0], "arlo/status");
    assert_eq!(published[0].1, Bytes::from_static(b"online"));
    assert_eq!(
        topics[1..6],
        [
            "homeassistant/binary_sensor/arlo_streamer_bridge_arlo/arlo_connected/config",
            "homeassistant/sensor/arlo_streamer_CAM1/state/config",
            "homeassistant/binary_sensor/arlo_streamer_CAM1/live/config",
            "homeassistant/binary_sensor/arlo_streamer_CAM1/user_view/config",
            "homeassistant/sensor/arlo_streamer_CAM1/live_secs_today/config",
        ]
    );
    assert_eq!(topics[6..8], ["arlo/bridge", "arlo/CAM1/state"]);
    // Three more ticks of an unchanged state wrote nothing more.
    assert_eq!(topics[8..], ["arlo/status"]);
    assert_eq!(published[8].1, Bytes::from_static(b"offline"));
    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test(start_paused = true)]
async fn a_state_change_is_published_once() {
    let dir = scratch("change");
    let admin = FakeAdmin::with(&["CAM1"]);
    let (log, stop, task) = start(&[], admin.clone(), false, &dir);
    tokio::time::sleep(Duration::from_millis(1500)).await;

    admin.set("CAM1", "live");
    tokio::time::sleep(Duration::from_secs(3)).await;
    stop_and_join(&stop, task).await;

    let states: Vec<serde_json::Value> = log
        .published()
        .into_iter()
        .filter(|(t, _)| t == "arlo/CAM1/state")
        .map(|(_, p)| serde_json::from_slice(&p).unwrap())
        .collect();
    assert_eq!(states.len(), 2, "{states:?}");
    assert_eq!(states[0]["state"], "idle");
    assert_eq!(states[1]["state"], "live");
    assert_eq!(states[1]["live"], true);
    std::fs::remove_dir_all(&dir).ok();
}

/// A broker restarted without persistence lost the retained messages: a
/// new connection writes everything again.
#[tokio::test(start_paused = true)]
async fn a_lost_connection_reconnects_and_publishes_everything_again() {
    let dir = scratch("reconnect");
    let admin = FakeAdmin::with(&["CAM1"]);
    // The first session takes the announce (8 messages), then fails.
    let (log, stop, task) = start(&[Plan::Accept(8)], admin.clone(), false, &dir);
    tokio::time::sleep(Duration::from_millis(1500)).await;
    admin.set("CAM1", "live");

    tokio::time::sleep(Duration::from_secs(5)).await;
    stop_and_join(&stop, task).await;

    let events = log.events();
    let connects = events.iter().filter(|e| **e == Event::Connect).count();
    assert_eq!(connects, 2, "{events:?}");
    let published = log.published();
    let onlines = published
        .iter()
        .filter(|(t, p)| t == "arlo/status" && p.as_ref() == b"online")
        .count();
    assert_eq!(onlines, 2, "online again after the reconnect");
    let last_state = published
        .iter()
        .rev()
        .find(|(t, _)| t == "arlo/CAM1/state")
        .map(|(_, p)| serde_json::from_slice::<serde_json::Value>(p).unwrap())
        .unwrap();
    assert_eq!(
        last_state["state"], "live",
        "the update lost with the first connection"
    );
    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test(start_paused = true)]
async fn an_unreachable_broker_is_retried_after_1_2_then_4_seconds() {
    let dir = scratch("backoff");
    let (log, stop, task) = start(
        &[Plan::Refuse, Plan::Refuse, Plan::Refuse],
        FakeAdmin::with(&["CAM1"]),
        false,
        &dir,
    );
    tokio::time::sleep(Duration::from_secs(10)).await;
    stop_and_join(&stop, task).await;

    let at = log.connect_times();
    assert_eq!(at.len(), 4, "three refusals then a connection");
    let gaps: Vec<u64> = at.windows(2).map(|w| (w[1] - w[0]).as_secs()).collect();
    assert_eq!(gaps, [1, 2, 4]);
    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test(start_paused = true)]
async fn stopping_while_the_broker_is_unreachable_returns_at_once() {
    let dir = scratch("stop-unreachable");
    let (log, stop, task) = start(&[Plan::Refuse; 10], FakeAdmin::with(&["CAM1"]), false, &dir);
    tokio::time::sleep(Duration::from_millis(500)).await;
    stop_and_join(&stop, task).await;
    assert_eq!(log.published(), []);
    std::fs::remove_dir_all(&dir).ok();
}

/// A camera dropped from the configuration does not stay in Home
/// Assistant with its last state frozen.
#[tokio::test(start_paused = true)]
async fn a_camera_no_longer_served_is_cleared_and_the_record_updated() {
    let dir = scratch("removal");
    let record = PublishedRecord::in_dir(&dir);
    let before: BTreeSet<CameraId> = [CameraId::new("CAM1"), CameraId::new("OLD")].into();
    record.save(&before).await.unwrap();
    let (log, stop, task) = start(&[], FakeAdmin::with(&["CAM1"]), false, &dir);

    tokio::time::sleep(Duration::from_secs(2)).await;
    stop_and_join(&stop, task).await;

    let cleared: Vec<String> = log
        .published()
        .into_iter()
        .filter(|(_, p)| p.is_empty())
        .map(|(t, _)| t)
        .collect();
    assert!(
        cleared.contains(&"arlo/OLD/state".to_string()),
        "{cleared:?}"
    );
    assert!(cleared.contains(&"homeassistant/sensor/arlo_streamer_OLD/state/config".to_string()));
    assert!(cleared.iter().all(|t| !t.contains("CAM1")), "{cleared:?}");
    let after = record.load().await;
    assert_eq!(after, [CameraId::new("CAM1")].into());
    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test(start_paused = true)]
async fn thumbnails_are_published_only_when_on_and_small_enough() {
    for thumbnails in [false, true] {
        let dir = scratch(&format!("thumbs-{thumbnails}"));
        let admin = FakeAdmin::with(&["CAM1", "BIG"]);
        admin
            .thumbnails
            .lock()
            .unwrap()
            .insert(CameraId::new("CAM1"), Bytes::from_static(b"\xff\xd8jpeg"));
        admin.thumbnails.lock().unwrap().insert(
            CameraId::new("BIG"),
            Bytes::from(vec![0; MAX_THUMBNAIL_BYTES + 1]),
        );
        let (log, stop, task) = start(&[], admin, thumbnails, &dir);

        tokio::time::sleep(Duration::from_secs(2)).await;
        stop_and_join(&stop, task).await;

        let published = log.published();
        let pictures: Vec<&(String, Bytes)> = published
            .iter()
            .filter(|(t, _)| t.ends_with("/thumbnail"))
            .collect();
        if thumbnails {
            assert_eq!(pictures.len(), 1, "the small one only");
            assert_eq!(pictures[0].0, "arlo/CAM1/thumbnail");
            assert!(
                topics(&published)
                    .contains(&"homeassistant/camera/arlo_streamer_CAM1/idle_picture/config")
            );
        } else {
            assert_eq!(pictures.len(), 0, "no picture with thumbnails off");
            assert!(
                !topics(&published)
                    .iter()
                    .any(|t| t.contains("idle_picture"))
            );
        }
        std::fs::remove_dir_all(&dir).ok();
    }
}

#[test]
fn backoff_doubles_to_a_minute() {
    let mut backoff = Backoff::new();
    let waits: Vec<u64> = (0..8).map(|_| backoff.next().as_secs()).collect();
    assert_eq!(waits, [1, 2, 4, 8, 16, 32, 60, 60]);
}

// ---------------------------------------------------------------- session

/// A broker that answers CONNECT with `code` and PINGREQ with PINGRESP,
/// and returns every packet it read.
async fn minimal_broker(code: ConnectReturnCode) -> (u16, JoinHandle<Vec<Packet>>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let task = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut buffer = BytesMut::new();
        let mut packets = Vec::new();
        loop {
            match mqttbytes::v4::read(&mut buffer, 1 << 20) {
                Ok(packet) => {
                    let mut reply = BytesMut::new();
                    match &packet {
                        Packet::Connect(_) => {
                            ConnAck::new(code, false).write(&mut reply).unwrap();
                        }
                        Packet::PingReq => {
                            PingResp.write(&mut reply).unwrap();
                        }
                        _ => {}
                    }
                    stream.write_all(&reply).await.unwrap();
                    packets.push(packet);
                }
                Err(mqttbytes::Error::InsufficientBytes(_)) => {
                    if stream.read_buf(&mut buffer).await.unwrap_or(0) == 0 {
                        return packets;
                    }
                }
                Err(e) => panic!("bad packet: {e}"),
            }
        }
    });
    (port, task)
}

fn tcp_broker(port: u16, password: &str) -> TcpBroker {
    TcpBroker::new(
        MqttEndpoint {
            tls: false,
            host: "127.0.0.1".into(),
            port,
        },
        None,
        ConnectParams {
            client_id: "arlo-camera-streamer-arlo".into(),
            username: Some("arlo".into()),
            password: Some(SecretString::from(password.to_string())),
            will_topic: "arlo/status".into(),
            will_payload: OFFLINE,
        },
    )
    .unwrap()
}

#[tokio::test]
async fn the_session_speaks_mqtt_3_1_1_with_a_retained_will_and_qos_0() {
    let (port, broker) = minimal_broker(ConnectReturnCode::Success).await;
    let mut session = tcp_broker(port, "s3cret")
        .connect()
        .await
        .expect("accepted");
    session
        .publish(&Message {
            topic: "arlo/CAM1/state".into(),
            payload: Bytes::from_static(b"{}"),
        })
        .await
        .unwrap();
    session.keep_alive().await.expect("PINGRESP");
    session.disconnect().await;

    let packets = broker.await.unwrap();
    let Packet::Connect(connect) = &packets[0] else {
        panic!("first packet: {:?}", packets[0]);
    };
    assert_eq!(connect.client_id, "arlo-camera-streamer-arlo");
    assert_eq!(connect.keep_alive, KEEP_ALIVE_SECS);
    assert!(connect.clean_session, "no session kept across connects");
    let will = connect.last_will.as_ref().expect("a last will");
    assert_eq!(
        (will.topic.as_str(), will.message.as_ref(), will.retain),
        ("arlo/status", &b"offline"[..], true)
    );
    let login = connect.login.as_ref().expect("a login");
    assert_eq!(
        (login.username.as_str(), login.password.as_str()),
        ("arlo", "s3cret")
    );
    let Packet::Publish(publish) = &packets[1] else {
        panic!("second packet: {:?}", packets[1]);
    };
    assert_eq!(publish.topic, "arlo/CAM1/state");
    assert!(publish.retain);
    assert_eq!(publish.qos, mqttbytes::QoS::AtMostOnce);
    assert!(matches!(packets[2], Packet::PingReq));
    assert!(matches!(packets[3], Packet::Disconnect));
}

#[tokio::test]
async fn a_refused_login_is_reported_without_the_password() {
    let (port, _broker) = minimal_broker(ConnectReturnCode::BadUserNamePassword).await;

    let err = tcp_broker(port, "s3cret")
        .connect()
        .await
        .err()
        .expect("refused");

    let text = err.to_string();
    assert!(matches!(err, MqttError::Refused(_)), "{text}");
    assert!(text.contains("BadUserNamePassword"), "{text}");
    assert!(!text.contains("s3cret"), "{text}");
}

#[test]
fn mqtts_needs_a_tls_setup_and_a_ca_file_needs_a_certificate() {
    let endpoint = MqttEndpoint {
        tls: true,
        host: "broker.lan".into(),
        port: 8883,
    };
    let params = || ConnectParams {
        client_id: "c".into(),
        username: None,
        password: None,
        will_topic: "s".into(),
        will_payload: OFFLINE,
    };
    assert!(matches!(
        TcpBroker::new(endpoint, None, params()),
        Err(MqttError::Tls(_))
    ));
    assert!(matches!(
        session::tls_config(Some(b"no certificate here")),
        Err(MqttError::Tls(_))
    ));
}

#[test]
fn setup_refuses_a_bad_url_and_accepts_a_plain_one() {
    let dir = scratch("setup");
    let mut bad = config(false);
    bad.url = "http://broker".into();
    assert!(matches!(
        MqttSetup::new(&bad, None, None, &dir, "0.3.0"),
        Err(MqttError::Config(_))
    ));
    assert!(MqttSetup::new(&config(false), None, None, &dir, "0.3.0").is_ok());
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn only_loopback_hosts_count_as_local() {
    for (host, local) in [
        ("localhost", true),
        ("127.0.0.1", true),
        ("::1", true),
        ("192.168.1.10", false),
        ("broker.lan", false),
    ] {
        assert_eq!(is_loopback(host), local, "{host}");
    }
}

/// Against a real broker, on demand:
/// `MQTT_IT_URL=mqtt://127.0.0.1:1883 cargo test -p streamer-infra-ops -- --ignored real_broker`
#[tokio::test]
#[ignore = "needs a broker at MQTT_IT_URL"]
async fn real_broker_receives_the_announce_and_the_will() {
    let Ok(url) = std::env::var("MQTT_IT_URL") else {
        return;
    };
    let dir = scratch("real-broker");
    let mut settings = config(true);
    settings.url = url;
    let setup = MqttSetup::new(&settings, None, None, &dir, "0.3.0").expect("setup");
    let admin = FakeAdmin::with(&["CAM1"]);
    admin
        .thumbnails
        .lock()
        .unwrap()
        .insert(CameraId::new("CAM1"), Bytes::from_static(b"\xff\xd8jpeg"));
    let stop = CancellationToken::new();
    let task = setup.spawn(admin.clone(), stop.clone());
    tokio::time::sleep(Duration::from_secs(2)).await;
    admin.set("CAM1", "live");
    tokio::time::sleep(Duration::from_secs(2)).await;
    stop_and_join(&stop, task).await;
    std::fs::remove_dir_all(&dir).ok();
}
