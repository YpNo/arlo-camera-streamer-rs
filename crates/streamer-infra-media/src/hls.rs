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
/// [`MediaError::Pipeline`] when the directory cannot be created or
/// read, is not a real directory (a symlink planted at the path would
/// make `clear_dir` delete elsewhere), or does not sit directly under
/// the configured HLS root, or when it or the root is not the daemon's
/// own or is writable by others.
pub(crate) fn prepare_dir(hls: &HlsBranchConfig) -> Result<(), MediaError> {
    let dir = Path::new(&hls.dir);
    create_private_dir(dir)
        .map_err(|e| MediaError::Pipeline(format!("create HLS dir {}: {e}", hls.dir)))?;
    validate_dir(hls)?;
    clear_dir(dir)
}

/// Mode of the directories the daemon creates for HLS: the segments are
/// read by a server sharing the daemon's group at most.
#[cfg(unix)]
const HLS_DIR_MODE: u32 = 0o750;

#[cfg(unix)]
fn create_private_dir(dir: &Path) -> std::io::Result<()> {
    use std::os::unix::fs::DirBuilderExt;
    std::fs::DirBuilder::new()
        .recursive(true)
        .mode(HLS_DIR_MODE)
        .create(dir)
}

#[cfg(not(unix))]
fn create_private_dir(dir: &Path) -> std::io::Result<()> {
    std::fs::create_dir_all(dir)
}

/// The directory is the daemon's own and nobody else can write in it: the
/// segmenter opens predictable file names, which would follow a symlink
/// someone else planted (third sweep, S3-L9).
#[cfg(unix)]
fn check_private(what: &str, path: &Path) -> Result<(), MediaError> {
    use std::os::unix::fs::MetadataExt;
    let meta = std::fs::metadata(path)
        .map_err(|e| MediaError::Pipeline(format!("stat {what} {}: {e}", path.display())))?;
    // The owner of /proc/self is the process's effective uid.
    let ours = std::fs::metadata("/proc/self")
        .map_err(|e| MediaError::Pipeline(format!("cannot tell the daemon's uid: {e}")))?
        .uid();
    if meta.uid() != ours {
        return Err(MediaError::Pipeline(format!(
            "{what} {} belongs to uid {}, not to the daemon (uid {ours})",
            path.display(),
            meta.uid()
        )));
    }
    if meta.mode() & 0o022 != 0 {
        return Err(MediaError::Pipeline(format!(
            "{what} {} is writable by others (mode {:o}); make it the daemon's only",
            path.display(),
            meta.mode() & 0o777
        )));
    }
    Ok(())
}

#[cfg(not(unix))]
fn check_private(_what: &str, _path: &Path) -> Result<(), MediaError> {
    Ok(())
}

/// The HLS dir is a real directory (not a symlink) directly under the HLS
/// root. Checked before every clear, not only at preparation: a symlink
/// swapped in while the segmenter ran would otherwise be followed.
///
/// # Errors
///
/// [`MediaError::Pipeline`] naming what is wrong.
fn validate_dir(hls: &HlsBranchConfig) -> Result<(), MediaError> {
    let dir = Path::new(&hls.dir);
    let meta = std::fs::symlink_metadata(dir)
        .map_err(|e| MediaError::Pipeline(format!("stat HLS dir {}: {e}", hls.dir)))?;
    if !meta.file_type().is_dir() {
        return Err(MediaError::Pipeline(format!(
            "HLS dir {} is not a directory (a symlink or a file is in the way)",
            hls.dir
        )));
    }
    let root = std::fs::canonicalize(&hls.root)
        .map_err(|e| MediaError::Pipeline(format!("resolve HLS root {}: {e}", hls.root)))?;
    let real = std::fs::canonicalize(dir)
        .map_err(|e| MediaError::Pipeline(format!("resolve HLS dir {}: {e}", hls.dir)))?;
    if real.parent() != Some(root.as_path()) {
        return Err(MediaError::Pipeline(format!(
            "HLS dir {} is not directly under the HLS root {}",
            hls.dir, hls.root
        )));
    }
    check_private("HLS root", &root)?;
    check_private("HLS dir", &real)
}

/// The stop-time clear, after the same checks as [`prepare_dir`].
///
/// # Errors
///
/// [`MediaError::Pipeline`] when the dir is no longer a real directory
/// directly under the root, or cannot be read.
fn clear_on_stop(hls: &HlsBranchConfig) -> Result<(), MediaError> {
    validate_dir(hls)?;
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
    if let Err(e) = clear_on_stop(hls) {
        warn!(%camera, error = %e, "HLS directory not cleared on stop");
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
        std::fs::create_dir_all(&root).expect("root");
        let hls = HlsBranchConfig {
            root: root.to_string_lossy().into_owned(),
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

    #[cfg(unix)]
    #[test]
    fn prepare_dir_refuses_a_symlink_and_a_dir_outside_the_root() {
        let base = std::env::temp_dir().join(format!("hls-prepare-sym-{}", std::process::id()));
        let root = base.join("root");
        let elsewhere = base.join("elsewhere");
        std::fs::create_dir_all(&root).expect("root");
        std::fs::create_dir_all(&elsewhere).expect("elsewhere");
        std::os::unix::fs::symlink(&elsewhere, root.join("front_door")).expect("symlink");
        let hls = |dir: &Path| HlsBranchConfig {
            root: root.to_string_lossy().into_owned(),
            dir: dir.to_string_lossy().into_owned(),
            playlist_location: String::new(),
            segment_location: String::new(),
            target_duration: 2,
            playlist_length: 3,
            max_files: 5,
        };
        let err = prepare_dir(&hls(&root.join("front_door"))).unwrap_err();
        assert!(err.to_string().contains("not a directory"), "{err}");
        let err = prepare_dir(&hls(&elsewhere.join("deeper"))).unwrap_err();
        assert!(err.to_string().contains("not directly under"), "{err}");
        std::fs::remove_dir_all(base).expect("cleanup");
    }

    /// A segment file opened by name follows a symlink planted in a
    /// directory others can write: the stream dir and the root must be
    /// writable by the daemon only (third sweep, S3-L9).
    #[cfg(unix)]
    #[test]
    fn prepare_dir_refuses_a_dir_or_root_others_can_write() {
        use std::os::unix::fs::PermissionsExt;
        let base = std::env::temp_dir().join(format!("hls-perms-{}", std::process::id()));
        let root = base.join("root");
        let dir = root.join("front_door");
        std::fs::create_dir_all(&dir).expect("dir");
        let hls = HlsBranchConfig {
            root: root.to_string_lossy().into_owned(),
            dir: dir.to_string_lossy().into_owned(),
            playlist_location: String::new(),
            segment_location: String::new(),
            target_duration: 2,
            playlist_length: 3,
            max_files: 5,
        };
        let mode = |path: &Path, mode: u32| {
            std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode)).expect("chmod");
        };
        mode(&dir, 0o770);
        let err = prepare_dir(&hls).unwrap_err();
        assert!(err.to_string().contains("writable by others"), "{err}");
        mode(&dir, 0o750);
        mode(&root, 0o777);
        let err = prepare_dir(&hls).unwrap_err();
        assert!(err.to_string().contains("writable by others"), "{err}");
        mode(&root, 0o755);
        prepare_dir(&hls).expect("private again");
        std::fs::remove_dir_all(base).expect("cleanup");
    }

    /// A missing stream dir is created private.
    #[cfg(unix)]
    #[test]
    fn prepare_dir_creates_the_dir_owner_only() {
        use std::os::unix::fs::PermissionsExt;
        let root = std::env::temp_dir().join(format!("hls-mode-{}", std::process::id()));
        std::fs::create_dir_all(&root).expect("root");
        let dir = root.join("front_door");
        let hls = HlsBranchConfig {
            root: root.to_string_lossy().into_owned(),
            dir: dir.to_string_lossy().into_owned(),
            playlist_location: String::new(),
            segment_location: String::new(),
            target_duration: 2,
            playlist_length: 3,
            max_files: 5,
        };
        prepare_dir(&hls).expect("create");
        let mode = std::fs::metadata(&dir).expect("stat").permissions().mode();
        assert_eq!(mode & 0o777, 0o750, "{mode:o}");
        std::fs::remove_dir_all(root).expect("cleanup");
    }

    /// The stop-time clear followed a symlink swapped in after the
    /// preparation checks, and removed files outside the HLS root.
    #[test]
    fn clear_on_stop_refuses_a_dir_swapped_for_a_symlink() {
        let base = std::env::temp_dir().join(format!("hls-stop-swap-{}", std::process::id()));
        let root = base.join("root");
        let elsewhere = base.join("elsewhere");
        let dir = root.join("front_door");
        std::fs::create_dir_all(&dir).expect("dir");
        std::fs::create_dir_all(&elsewhere).expect("elsewhere");
        let hls = HlsBranchConfig {
            root: root.to_string_lossy().into_owned(),
            dir: dir.to_string_lossy().into_owned(),
            playlist_location: String::new(),
            segment_location: String::new(),
            target_duration: 2,
            playlist_length: 3,
            max_files: 5,
        };
        prepare_dir(&hls).expect("prepared while sound");
        std::fs::remove_dir(&dir).expect("swap: remove");
        std::os::unix::fs::symlink(&elsewhere, &dir).expect("swap: symlink");
        let victim = elsewhere.join(HLS_PLAYLIST_FILE);
        std::fs::write(&victim, "#EXTM3U").expect("victim");

        assert!(clear_on_stop(&hls).is_err());
        assert!(victim.exists(), "nothing outside the root is removed");
        std::fs::remove_dir_all(base).expect("cleanup");
    }
}
