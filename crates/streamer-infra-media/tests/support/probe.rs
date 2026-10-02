//! An RTSP client that watches what a viewer would see.
//!
//! [`RtspProbe`] plays the camera's mount over TCP, decodes the video
//! to 8-bit grey and keeps the mean brightness of the latest frame.
//! The idle screen is black with a small caption and the fake gateway
//! sends white, so brightness alone tells idle from live. It also
//! records whether the client ever saw end-of-stream or an error: a
//! seamless splice shows neither. The audio track is decoded through a
//! `level` element so a test can tell the silent bed from relayed sound.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread::JoinHandle;
use std::time::Duration;

use gstreamer as gst;
use gstreamer::prelude::*;
use gstreamer_app as gst_app;

/// `level` reports silence as a very negative dB value; anything above
/// this is sound.
const AUDIBLE_DB: f64 = -40.0;

#[derive(Default)]
struct Seen {
    frames: AtomicU64,
    mean_luma: AtomicU64,
    /// Latest RMS level of the audio track in dB, stored as the f64's bits.
    audio_rms_bits: AtomicU64,
    audio_windows: AtomicU64,
    eos: AtomicBool,
    error: AtomicBool,
}

/// See the module docs.
pub struct RtspProbe {
    pipeline: gst::Pipeline,
    seen: Arc<Seen>,
    bus_thread: Option<JoinHandle<()>>,
}

impl RtspProbe {
    /// Connect to `url` over TCP (interleaved) and start playing.
    pub fn connect(url: &str) -> Self {
        Self::connect_over(url, "tcp")
    }

    /// Connect to `url` with the given `rtspsrc` `protocols` (`tcp`,
    /// `udp`) and start playing. VLC and many cameras default to UDP.
    pub fn connect_over(url: &str, protocols: &str) -> Self {
        let launch = format!(
            "rtspsrc name=src location={url} protocols={protocols} latency=0 \
             src. ! rtph264depay ! h264parse ! avdec_h264 ! videoconvert \
             ! video/x-raw,format=GRAY8 \
             ! appsink name=frames sync=false max-buffers=1 drop=true \
             src. ! rtpmp4adepay ! aacparse ! avdec_aac ! audioconvert \
             ! level interval=100000000 ! fakesink sync=false"
        );
        let pipeline = gst::parse::launch(&launch)
            .expect("probe launch")
            .downcast::<gst::Pipeline>()
            .expect("probe pipeline");
        let seen = Arc::new(Seen::default());
        let appsink = pipeline
            .by_name("frames")
            .expect("frames appsink")
            .downcast::<gst_app::AppSink>()
            .expect("appsink type");
        let frame_seen = seen.clone();
        appsink.set_callbacks(
            gst_app::AppSinkCallbacks::builder()
                .new_sample(move |sink| {
                    let sample = sink.pull_sample().map_err(|_| gst::FlowError::Eos)?;
                    let buffer = sample.buffer().ok_or(gst::FlowError::Error)?;
                    let map = buffer.map_readable().map_err(|_| gst::FlowError::Error)?;
                    frame_seen
                        .mean_luma
                        .store(mean(map.as_slice()), Ordering::Relaxed);
                    frame_seen.frames.fetch_add(1, Ordering::Relaxed);
                    Ok(gst::FlowSuccess::Ok)
                })
                .build(),
        );
        let bus_thread = Some(watch_bus(&pipeline, seen.clone()));
        pipeline
            .set_state(gst::State::Playing)
            .expect("probe → Playing");
        Self {
            pipeline,
            seen,
            bus_thread,
        }
    }

    /// Frames decoded so far.
    pub fn frames(&self) -> u64 {
        self.seen.frames.load(Ordering::Relaxed)
    }

    /// Mean brightness (0–255) of the latest frame.
    pub fn mean_luma(&self) -> u64 {
        self.seen.mean_luma.load(Ordering::Relaxed)
    }

    /// Latest RMS level of the audio track in dB (`level` element);
    /// silence reads far below [`AUDIBLE_DB`].
    pub fn audio_rms_db(&self) -> f64 {
        f64::from_bits(self.seen.audio_rms_bits.load(Ordering::Relaxed))
    }

    /// Whether the latest audio window carried sound rather than the
    /// silent bed. `false` until the first window was measured.
    pub fn hears_sound(&self) -> bool {
        self.seen.audio_windows.load(Ordering::Relaxed) > 0 && self.audio_rms_db() > AUDIBLE_DB
    }

    /// Whether the client saw end-of-stream or an error at any point.
    pub fn interrupted(&self) -> bool {
        self.seen.eos.load(Ordering::Relaxed) || self.seen.error.load(Ordering::Relaxed)
    }
}

impl Drop for RtspProbe {
    fn drop(&mut self) {
        if let Some(bus) = self.pipeline.bus() {
            let _ = bus.post(gst::message::Application::new(gst::Structure::new_empty(
                "probe-stop",
            )));
        }
        let _ = self.pipeline.set_state(gst::State::Null);
        if let Some(thread) = self.bus_thread.take() {
            let _ = thread.join();
        }
    }
}

/// The loudest channel's RMS (dB) of a `level` message, if it is one.
fn level_rms_db(s: &gst::StructureRef) -> Option<f64> {
    if s.name() != "level" {
        return None;
    }
    let rms = s.get::<gst::glib::ValueArray>("rms").ok()?;
    rms.iter()
        .filter_map(|v| v.get::<f64>().ok())
        .fold(None, |max: Option<f64>, db| {
            Some(max.map_or(db, |m| m.max(db)))
        })
}

fn mean(frame: &[u8]) -> u64 {
    if frame.is_empty() {
        return 0;
    }
    let sum: u64 = frame.iter().map(|&b| u64::from(b)).sum();
    sum / frame.len() as u64
}

fn watch_bus(pipeline: &gst::Pipeline, seen: Arc<Seen>) -> JoinHandle<()> {
    let bus = pipeline.bus().expect("probe bus");
    std::thread::spawn(move || {
        loop {
            let Some(msg) = bus.timed_pop(gst::ClockTime::from_mseconds(200)) else {
                continue;
            };
            match msg.view() {
                gst::MessageView::Eos(_) => seen.eos.store(true, Ordering::Relaxed),
                gst::MessageView::Error(e) => {
                    eprintln!("probe error: {} ({:?})", e.error(), e.debug());
                    seen.error.store(true, Ordering::Relaxed);
                }
                gst::MessageView::Application(app)
                    if app.structure().is_some_and(|s| s.name() == "probe-stop") =>
                {
                    return;
                }
                gst::MessageView::Element(el) => {
                    if let Some(db) = el.structure().and_then(level_rms_db) {
                        seen.audio_rms_bits.store(db.to_bits(), Ordering::Relaxed);
                        seen.audio_windows.fetch_add(1, Ordering::Relaxed);
                    }
                }
                _ => {}
            }
        }
    })
}

/// Poll `condition` every 100 ms until it holds or `within` elapses.
pub async fn eventually(within: Duration, mut condition: impl FnMut() -> bool) -> bool {
    let deadline = tokio::time::Instant::now() + within;
    while tokio::time::Instant::now() < deadline {
        if condition() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    condition()
}
