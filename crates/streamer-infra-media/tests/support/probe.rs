//! An RTSP client that watches what a viewer would see.
//!
//! [`RtspProbe`] plays the camera's mount over TCP, decodes the video
//! to 8-bit grey and keeps the mean brightness of the latest frame.
//! The idle screen is black with a small caption and the fake gateway
//! sends white, so brightness alone tells idle from live. It also
//! records whether the client ever saw end-of-stream or an error: a
//! seamless splice shows neither.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread::JoinHandle;
use std::time::Duration;

use gstreamer as gst;
use gstreamer::prelude::*;
use gstreamer_app as gst_app;

#[derive(Default)]
struct Seen {
    frames: AtomicU64,
    mean_luma: AtomicU64,
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
    /// Connect to `url` and start playing.
    pub fn connect(url: &str) -> Self {
        let launch = format!(
            "rtspsrc location={url} protocols=tcp latency=0 \
             ! rtph264depay ! h264parse ! avdec_h264 ! videoconvert \
             ! video/x-raw,format=GRAY8 \
             ! appsink name=frames sync=false max-buffers=1 drop=true"
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
