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
    /// Accepted; publishes go out, a ping is never answered (the
    /// connection died with the packets unread).
    Deaf,
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
                deaf: false,
                log: self.log.clone(),
            })),
            Plan::Deaf => Ok(Box::new(FakeSession {
                budget: usize::MAX,
                deaf: true,
                log: self.log.clone(),
            })),
        }
    }
}

struct FakeSession {
    budget: usize,
    deaf: bool,
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
        if self.deaf {
            return Err(MqttError::Timeout("PINGRESP"));
        }
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
    record
        .save(&Layout::new(&config(false)), &before)
        .await
        .unwrap();
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
    assert_eq!(after.cameras, [CameraId::new("CAM1")].into());
    std::fs::remove_dir_all(&dir).ok();
}

/// The removals are `QoS` 0: the record forgot the departed camera as soon
/// as they were written, so a connection lost before the broker read them
/// left its topics for good (third sweep, S3-Z6). The record changes only
/// once a ping came back after them.
#[tokio::test(start_paused = true)]
async fn the_record_keeps_a_departed_camera_until_the_broker_confirms_its_removal() {
    let dir = scratch("removal-unconfirmed");
    let record = PublishedRecord::in_dir(&dir);
    let before: BTreeSet<CameraId> = [CameraId::new("CAM1"), CameraId::new("OLD")].into();
    record
        .save(&Layout::new(&config(false)), &before)
        .await
        .unwrap();
    let plans = [Plan::Deaf, Plan::Refuse, Plan::Refuse, Plan::Refuse];
    let (log, stop, task) = start(&plans, FakeAdmin::with(&["CAM1"]), false, &dir);

    tokio::time::sleep(Duration::from_secs(3)).await;
    stop_and_join(&stop, task).await;

    assert!(
        log.published()
            .iter()
            .any(|(t, p)| t == "arlo/OLD/state" && p.is_empty()),
        "the removal went out"
    );
    assert_eq!(
        record.load().await.cameras,
        before,
        "but was never confirmed"
    );
    std::fs::remove_dir_all(&dir).ok();
}

/// After a change of `base_topic`, what was published under the old one
/// is cleared: the old bridge device would stay in Home Assistant, and
/// the old state topics on the broker.
#[tokio::test(start_paused = true)]
async fn a_new_base_topic_clears_what_the_old_one_published() {
    let dir = scratch("relayout");
    let mut old = config(false);
    old.base_topic = "old".into();
    PublishedRecord::in_dir(&dir)
        .save(&Layout::new(&old), &[CameraId::new("CAM1")].into())
        .await
        .unwrap();
    let (log, stop, task) = start(&[], FakeAdmin::with(&["CAM1"]), false, &dir);

    tokio::time::sleep(Duration::from_secs(2)).await;
    stop_and_join(&stop, task).await;

    let published = log.published();
    let cleared: Vec<&str> = published
        .iter()
        .filter(|(_, p)| p.is_empty())
        .map(|(t, _)| t.as_str())
        .collect();
    for topic in [
        "old/status",
        "old/bridge",
        "old/CAM1/state",
        "old/CAM1/thumbnail",
        "homeassistant/binary_sensor/arlo_streamer_bridge_old/arlo_connected/config",
    ] {
        assert!(cleared.contains(&topic), "{topic} not cleared: {cleared:?}");
    }
    // The camera's config topic is the same under both base topics: it is
    // cleared first, then announced again, and stays announced.
    let config_topic = "homeassistant/sensor/arlo_streamer_CAM1/state/config";
    let last = published
        .iter()
        .rev()
        .find(|(t, _)| t == config_topic)
        .expect("announced");
    assert!(!last.1.is_empty(), "the new config is not cleared after");
    assert!(cleared.contains(&config_topic));
    assert_eq!(
        PublishedRecord::in_dir(&dir).load().await.layout,
        Some(Layout::new(&config(false)))
    );
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

/// A broker that accepts the connection, then stops reading, as a host
/// that vanished without closing the connection: writes pile up in the
/// socket buffers until they block.
async fn deaf_broker() -> (u16, JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let task = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut buffer = BytesMut::new();
        loop {
            match mqttbytes::v4::read(&mut buffer, 1 << 20) {
                Ok(_connect) => break,
                Err(mqttbytes::Error::InsufficientBytes(_)) => {
                    stream.read_buf(&mut buffer).await.unwrap();
                }
                Err(e) => panic!("bad packet: {e}"),
            }
        }
        let mut reply = BytesMut::new();
        ConnAck::new(ConnectReturnCode::Success, false)
            .write(&mut reply)
            .unwrap();
        stream.write_all(&reply).await.unwrap();
        // Hold the connection open and read nothing more.
        std::future::pending::<()>().await;
    });
    (port, task)
}

/// A write that cannot complete fails within the step timeout: it used to
/// wait for the kernel to give up on TCP, about 15 minutes during which
/// no keep-alive could run and nothing was published.
#[tokio::test(start_paused = true)]
async fn a_write_the_broker_never_reads_times_out() {
    let (port, broker) = deaf_broker().await;
    let mut session = tcp_broker(port, "s3cret")
        .connect()
        .await
        .expect("accepted");
    let big = Message {
        topic: "arlo/CAM1/thumbnail".into(),
        payload: Bytes::from(vec![0u8; 1 << 20]),
    };
    let mut outcome = Ok(());
    for _ in 0..256 {
        outcome = tokio::time::timeout(Duration::from_secs(600), session.publish(&big))
            .await
            .expect("the session must give up on its own");
        if outcome.is_err() {
            break;
        }
    }
    assert!(matches!(outcome, Err(MqttError::Timeout(_))), "{outcome:?}");
    broker.abort();
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

/// A broker that answers the n-th packet it reads with `replies[n]`, and
/// closes the connection on the first packet it has no reply for.
async fn scripted_broker(replies: &'static [&'static [u8]]) -> u16 {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut buffer = BytesMut::new();
        for index in 0..=replies.len() {
            loop {
                match mqttbytes::v4::read(&mut buffer, 1 << 20) {
                    Ok(_) => break,
                    Err(mqttbytes::Error::InsufficientBytes(_)) => {
                        if stream.read_buf(&mut buffer).await.unwrap_or(0) == 0 {
                            return;
                        }
                    }
                    Err(e) => panic!("bad packet: {e}"),
                }
            }
            match replies.get(index) {
                Some(reply) => stream.write_all(reply).await.unwrap(),
                None => return,
            }
        }
    });
    port
}

const CONNACK_ACCEPTED: &[u8] = &[0x20, 0x02, 0x00, 0x00];

async fn connect_error(replies: &'static [&'static [u8]]) -> MqttError {
    let port = scripted_broker(replies).await;
    tcp_broker(port, "s3cret")
        .connect()
        .await
        .err()
        .expect("the connect must fail")
}

#[tokio::test]
async fn a_broker_that_closes_after_the_connect_is_reported_as_closed() {
    let err = connect_error(&[]).await;
    assert!(
        matches!(&err, MqttError::Io(text) if text.contains("closed the connection")),
        "{err}"
    );
}

/// Only CONNACK and PINGRESP are expected: a longer packet is refused from
/// its length, before its body is read.
#[tokio::test]
async fn a_packet_longer_than_a_connack_is_refused_unread() {
    // CONNACK type, remaining length 200 on two bytes.
    let err = connect_error(&[&[0x20, 0xC8, 0x01]]).await;
    assert!(
        matches!(&err, MqttError::Protocol(text) if text.contains("200-byte")),
        "{err}"
    );
}

#[tokio::test]
async fn a_remaining_length_over_four_bytes_is_refused() {
    let err = connect_error(&[&[0x20, 0xFF, 0xFF, 0xFF, 0xFF, 0x01]]).await;
    assert!(matches!(err, MqttError::Protocol(_)), "{err}");
}

#[tokio::test]
async fn a_pingresp_in_place_of_the_connack_is_refused() {
    let err = connect_error(&[&[0xD0, 0x00]]).await;
    assert!(
        matches!(&err, MqttError::Protocol(text) if text.contains("CONNACK")),
        "{err}"
    );
}

#[tokio::test]
async fn a_connack_in_place_of_the_pingresp_is_refused() {
    let port = scripted_broker(&[CONNACK_ACCEPTED, CONNACK_ACCEPTED]).await;
    let mut session = tcp_broker(port, "s3cret")
        .connect()
        .await
        .expect("accepted");
    let err = session.keep_alive().await.expect_err("no PINGRESP");
    assert!(
        matches!(&err, MqttError::Protocol(text) if text.contains("PINGRESP")),
        "{err}"
    );
}

fn tls_endpoint(host: &str, port: u16) -> MqttEndpoint {
    MqttEndpoint {
        tls: true,
        host: host.into(),
        port,
    }
}

fn anonymous() -> ConnectParams {
    ConnectParams {
        client_id: "c".into(),
        username: None,
        password: None,
        will_topic: "s".into(),
        will_payload: OFFLINE,
    }
}

/// A listener that is no TLS server: the handshake fails as a TLS error,
/// before any CONNECT (and its password) is written.
#[tokio::test]
async fn mqtts_to_a_server_without_tls_fails_the_handshake() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        drop(stream);
    });
    let tls = session::tls_config(None).expect("the system roots");
    let broker = TcpBroker::new(tls_endpoint("127.0.0.1", port), Some(tls), anonymous())
        .expect("an IP is a TLS server name");

    let err = broker.connect().await.err().expect("no TLS server");

    assert!(matches!(err, MqttError::Tls(_)), "{err}");
}

#[test]
fn mqtts_needs_a_host_that_is_a_tls_server_name() {
    let tls = session::tls_config(None).expect("the system roots");
    assert!(matches!(
        TcpBroker::new(tls_endpoint("not a host", 8883), Some(tls), anonymous()),
        Err(MqttError::Tls(_))
    ));
}

/// A CA bundle in PEM gives a TLS setup; the system's own bundle serves as
/// one (Debian and Ubuntu, where the tests run).
#[test]
fn a_ca_file_with_certificates_gives_a_tls_setup() {
    let pem = std::fs::read("/etc/ssl/certs/ca-certificates.crt").expect("the system CA bundle");
    assert!(session::tls_config(Some(&pem)).is_ok());
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
