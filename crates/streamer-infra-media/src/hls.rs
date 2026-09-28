//! HLS output (ADR 0006): a per-camera segmenter that plays the
//! camera's own RTSP mount over loopback and writes `hlssink2`
//! segments, without re-encoding.
//!
//! Reading the RTSP output instead of tapping the persistent pipeline
//! keeps one idle/live splice for every output: gst-rtsp-server owns
//! that pipeline and builds it only while a client is connected, and
//! the segmenter is such a client. The side effect is deliberate: with
//! HLS enabled, the camera's media stays built, so its encoder runs
//! continuously.
//!
//! The segmenter runs on its own thread (a `GLib` bus loop, never on the
//! tokio runtime). A pipeline that errors or ends — the RTSP server
//! restarting, a rebuilt media — is rebuilt after a backoff that doubles
//! up to [`MAX_BACKOFF`]. The per-stream directory is cleared of our
//! playlist and segments when the segmenter starts and when it stops,
//! so a player never sees a frozen playlist from a previous run.

use std::path::Path;
use std::sync::mpsc::{self, RecvTimeoutError};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use gstreamer as gst;
use gstreamer::prelude::*;
use tracing::{debug, info, warn};

use streamer_domain::camera::CameraId;

use crate::error::MediaError;
use crate::pipeline_desc::{
    HLS_PLAYLIST_FILE, HLS_SEGMENT_PREFIX, HLS_SEGMENT_SUFFIX, HlsBranchConfig,
    hls_segmenter_launch,
};

/// First restart delay after the segmenter pipeline stops.
const MIN_BACKOFF: Duration = Duration::from_secs(1);
/// Longest restart delay.
const MAX_BACKOFF: Duration = Duration::from_secs(30);
/// A run that lasted this long resets the backoff.
const HEALTHY_RUN: Duration = Duration::from_secs(60);
/// Application message that ends the segmenter's bus loop.
const STOP_MESSAGE: &str = "streamer-hls-stop";

type CurrentPipeline = Arc<Mutex<Option<gst::Pipeline>>>;

/// A running HLS segmenter. Dropping it stops the pipeline; the thread
/// then clears the directory and exits (it is not joined, so a drop on
/// the runtime never blocks).
pub(crate) struct HlsSegmenter {
    stop: mpsc::Sender<()>,
    current: CurrentPipeline,
}

impl HlsSegmenter {
    /// Start segmenting `url` into `hls`. The directory must already
    /// exist ([`prepare_dir`]).
    ///
    /// # Errors
    ///
    /// [`MediaError::Pipeline`] when the thread cannot be spawned.
    pub(crate) fn start(
        camera: CameraId,
        url: String,
        hls: HlsBranchConfig,
    ) -> Result<Self, MediaError> {
        let (stop, stop_rx) = mpsc::channel();
        let current: CurrentPipeline = Arc::new(Mutex::new(None));
        let thread_current = current.clone();
        std::thread::Builder::new()
            .name(format!("hls-{camera}"))
            .spawn(move || supervise(&camera, &url, &hls, &thread_current, &stop_rx))
            .map_err(|e| MediaError::Pipeline(format!("spawn HLS segmenter: {e}")))?;
        Ok(Self { stop, current })
    }
}

impl Drop for HlsSegmenter {
    fn drop(&mut self) {
        // Order matters with `run_once`: it stores the pipeline, then
        // checks the channel, so one of the two signals always lands.
        let _ = self.stop.send(());
        if let Some(pipeline) = lock(&self.current).as_ref() {
            post_stop(pipeline);
        }
    }
}

/// Create the per-stream directory and clear what a previous run left.
///
/// # Errors
///
/// [`MediaError::Pipeline`] when the directory cannot be created or read.
pub(crate) fn prepare_dir(hls: &HlsBranchConfig) -> Result<(), MediaError> {
    std::fs::create_dir_all(&hls.dir)
        .map_err(|e| MediaError::Pipeline(format!("create HLS dir {}: {e}", hls.dir)))?;
    clear_dir(Path::new(&hls.dir))
}

/// Remove our playlist and segments from `dir`; other files stay.
///
/// # Errors
///
/// [`MediaError::Pipeline`] when `dir` cannot be read.
pub(crate) fn clear_dir(dir: &Path) -> Result<(), MediaError> {
    let entries = std::fs::read_dir(dir)
        .map_err(|e| MediaError::Pipeline(format!("read HLS dir {}: {e}", dir.display())))?;
    for entry in entries.flatten() {
        let name = entry.file_name();
        if is_hls_output(&name.to_string_lossy())
            && let Err(e) = std::fs::remove_file(entry.path())
        {
            debug!(file = %entry.path().display(), error = %e, "HLS file not removed");
        }
    }
    Ok(())
}

/// Whether `name` is a file the segmenter writes.
fn is_hls_output(name: &str) -> bool {
    name == HLS_PLAYLIST_FILE
        || name
            .strip_prefix(HLS_SEGMENT_PREFIX)
            .and_then(|rest| rest.strip_suffix(HLS_SEGMENT_SUFFIX))
            .is_some_and(|index| !index.is_empty() && index.bytes().all(|b| b.is_ascii_digit()))
}

/// The delay after `previous` when the next run fails quickly too.
fn next_backoff(previous: Duration) -> Duration {
    (previous * 2).min(MAX_BACKOFF)
}

enum RunEnd {
    Stopped,
    Failed(String),
}

fn supervise(
    camera: &CameraId,
    url: &str,
    hls: &HlsBranchConfig,
    current: &CurrentPipeline,
    stop: &mpsc::Receiver<()>,
) {
    info!(%camera, playlist = %hls.playlist_location, "HLS segmenter started");
    let mut backoff = MIN_BACKOFF;
    loop {
        let started = Instant::now();
        let RunEnd::Failed(reason) = run_once(url, hls, current, stop) else {
            break;
        };
        let ran = started.elapsed();
        if ran >= HEALTHY_RUN {
            backoff = MIN_BACKOFF;
        }
        warn!(%camera, %reason, retry_in_secs = backoff.as_secs(), "HLS segmenter stopped; restarting");
        match stop.recv_timeout(backoff) {
            Err(RecvTimeoutError::Timeout) => {}
            Ok(()) | Err(RecvTimeoutError::Disconnected) => break,
        }
        backoff = next_backoff(backoff);
    }
    if let Err(e) = clear_dir(Path::new(&hls.dir)) {
        debug!(%camera, error = %e, "HLS directory not cleared on stop");
    }
    info!(%camera, "HLS segmenter stopped");
}

fn run_once(
    url: &str,
    hls: &HlsBranchConfig,
    current: &CurrentPipeline,
    stop: &mpsc::Receiver<()>,
) -> RunEnd {
    let pipeline = match build(url, hls) {
        Ok(p) => p,
        Err(e) => return RunEnd::Failed(e.to_string()),
    };
    *lock(current) = Some(pipeline.clone());
    let end = if stop.try_recv().is_ok() {
        RunEnd::Stopped
    } else {
        play_until_end(&pipeline)
    };
    lock(current).take();
    let _ = pipeline.set_state(gst::State::Null);
    end
}

fn build(url: &str, hls: &HlsBranchConfig) -> Result<gst::Pipeline, MediaError> {
    gst::parse::launch(&hls_segmenter_launch(url, hls))
        .map_err(|e| MediaError::Pipeline(format!("HLS segmenter launch: {e}")))?
        .downcast::<gst::Pipeline>()
        .map_err(|_| MediaError::Pipeline("HLS segmenter launch is not a pipeline".into()))
}

fn play_until_end(pipeline: &gst::Pipeline) -> RunEnd {
    let Some(bus) = pipeline.bus() else {
        return RunEnd::Failed("segmenter pipeline has no bus".into());
    };
    if let Err(e) = pipeline.set_state(gst::State::Playing) {
        return RunEnd::Failed(format!("segmenter → Playing: {e}"));
    }
    for msg in bus.iter_timed(gst::ClockTime::NONE) {
        match msg.view() {
            gst::MessageView::Error(e) => return RunEnd::Failed(e.error().to_string()),
            gst::MessageView::Eos(_) => return RunEnd::Failed("end of stream".into()),
            gst::MessageView::Application(app)
                if app.structure().is_some_and(|s| s.name() == STOP_MESSAGE) =>
            {
                return RunEnd::Stopped;
            }
            _ => {}
        }
    }
    RunEnd::Failed("bus closed".into())
}

fn post_stop(pipeline: &gst::Pipeline) {
    if let Some(bus) = pipeline.bus() {
        let stop = gst::message::Application::new(gst::Structure::new_empty(STOP_MESSAGE));
        if let Err(e) = bus.post(stop) {
            debug!(error = %e, "HLS stop message not posted");
        }
    }
}

fn lock(current: &CurrentPipeline) -> std::sync::MutexGuard<'_, Option<gst::Pipeline>> {
    current.lock().unwrap_or_else(PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn is_hls_output_matches_our_playlist_and_numbered_segments_only() {
        assert!(is_hls_output("index.m3u8"));
        assert!(is_hls_output("segment-00042.ts"));
        assert!(!is_hls_output("segment-.ts"));
        assert!(!is_hls_output("segment-00042.ts.bak"));
        assert!(!is_hls_output("segment-abc.ts"));
        assert!(!is_hls_output("notes.txt"));
    }

    #[test]
    fn next_backoff_doubles_up_to_the_cap() {
        assert_eq!(next_backoff(MIN_BACKOFF), Duration::from_secs(2));
        assert_eq!(next_backoff(Duration::from_secs(20)), MAX_BACKOFF);
        assert_eq!(next_backoff(MAX_BACKOFF), MAX_BACKOFF);
    }

    #[test]
    fn prepare_dir_creates_the_dir_and_clears_only_our_files() {
        let root = std::env::temp_dir().join(format!("hls-prepare-{}", std::process::id()));
        let dir = root.join("front_door");
        let hls = HlsBranchConfig {
            dir: dir.to_string_lossy().into_owned(),
            playlist_location: String::new(),
            segment_location: String::new(),
            target_duration: 2,
            playlist_length: 3,
            max_files: 5,
        };
        prepare_dir(&hls).expect("create");
        for f in ["index.m3u8", "segment-00001.ts", "keep.txt"] {
            std::fs::write(dir.join(f), b"x").expect("write");
        }
        prepare_dir(&hls).expect("clear");
        let mut left: Vec<String> = std::fs::read_dir(&dir)
            .expect("read")
            .flatten()
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .collect();
        left.sort();
        assert_eq!(left, ["keep.txt"]);
        std::fs::remove_dir_all(root).expect("cleanup");
    }
}
