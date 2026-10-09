//! The GStreamer elements the daemon's pipelines are built from, checked
//! once at boot.
//!
//! A missing element fails silently at run time: the RTSP factory treats
//! an unknown element in its launch string as a recoverable parse error,
//! builds what it can, and the media is torn down the moment a client
//! connects, with nothing in the daemon's log but the teardown (the pango
//! plugin, `textoverlay`, absent from the image until 0.2.1). [`check`]
//! turns that into a boot failure naming the elements, before the Arlo
//! login. The H.264 encoder is not listed: [`crate::encoder::resolve`]
//! probes it.

use gstreamer as gst;

use crate::error::MediaError;

/// Elements of the RTSP mount's launch string
/// ([`crate::pipeline_desc::combined_launch_string`], both idle kinds) and
/// of the WebRTC live leg ([`crate::webrtc_pipeline`]), with the ICE
/// transport `webrtcbin` loads at run time and the elements gst-rtsp-server
/// adds per client.
pub const ALWAYS: &[&str] = &[
    // Idle producers.
    "videotestsrc",
    "textoverlay",
    "gdkpixbufoverlay",
    "jpegdec",
    // Shared video path.
    "appsrc",
    "capsfilter",
    "videoconvert",
    "videoscale",
    "videorate",
    "queue",
    "input-selector",
    "h264parse",
    "rtph264pay",
    // Live video decode.
    "rtph264depay",
    "avdec_h264",
    // Audio: idle bed, live Opus, relayed AAC, the mix and its encoder.
    "audiotestsrc",
    "audioconvert",
    "audioresample",
    "rtpopusdepay",
    "opusdec",
    "rtpmp4gdepay",
    "aacparse",
    "avdec_aac",
    "audiomixer",
    "avenc_aac",
    "rtpmp4apay",
    // WebRTC live leg.
    "webrtcbin",
    "opusenc",
    "rtpopuspay",
    "nicesrc",
    "nicesink",
    // Created by gst-rtsp-server for each client.
    "rtpbin",
    "udpsrc",
    "udpsink",
];

/// Elements of the HLS segmenter
/// ([`crate::pipeline_desc::hls_segmenter_launch`]), needed only with
/// `[output.hls]`.
pub const HLS: &[&str] = &[
    "rtspsrc",
    "rtph264depay",
    "h264parse",
    "rtpmp4adepay",
    "aacparse",
    "queue",
    "hlssink2",
    // Created inside `hlssink2`.
    "splitmuxsink",
    "mpegtsmux",
];

/// Fail when an element the configured outputs need is not registered.
/// Call after `gstreamer::init()`.
///
/// # Errors
///
/// [`MediaError::Pipeline`] naming every missing element.
pub fn check(hls: bool) -> Result<(), MediaError> {
    let hls_elements: &[&str] = if hls { HLS } else { &[] };
    let missing = missing(ALWAYS.iter().chain(hls_elements));
    if missing.is_empty() {
        return Ok(());
    }
    Err(MediaError::Pipeline(format!(
        "missing GStreamer element(s): {} (install the GStreamer plugin packages \
         that provide them; the container image ships them all)",
        missing.join(", ")
    )))
}

/// The names in `elements` with no registered factory, each once.
fn missing<'a>(elements: impl IntoIterator<Item = &'a &'a str>) -> Vec<&'a str> {
    let mut missing: Vec<&str> = Vec::new();
    for &name in elements {
        if gst::ElementFactory::find(name).is_none() && !missing.contains(&name) {
            missing.push(name);
        }
    }
    missing
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::encoder::EncoderBackend;
    use crate::idle_source::IdleKind;
    use crate::pipeline_desc::{HlsBranchConfig, combined_launch_string, hls_segmenter_launch};
    use gstreamer::prelude::*;
    use streamer_domain::camera::StreamName;

    /// Set to `1` in CI: a host without the plugins fails instead of
    /// skipping.
    const REQUIRE_ENV: &str = "STREAMER_REQUIRE_GST_IT";

    /// Every element factory `launch` instantiates, or `None` (skip) when
    /// this host cannot build it.
    fn factories_of(launch: &str) -> Option<Vec<String>> {
        gst::init().ok()?;
        let parsed = match gst::parse::launch(launch) {
            Ok(parsed) => parsed,
            Err(e) => {
                assert!(
                    std::env::var(REQUIRE_ENV).as_deref() != Ok("1"),
                    "cannot build the launch string here: {e}"
                );
                eprintln!("skipping: cannot build the launch string here: {e}");
                return None;
            }
        };
        let bin = parsed.dynamic_cast::<gst::Bin>().ok()?;
        let names = bin
            .iterate_recurse()
            .into_iter()
            .filter_map(Result::ok)
            .filter_map(|element| element.factory())
            .map(|factory| factory.name().to_string())
            .collect();
        Some(names)
    }

    fn assert_listed(factories: &[String], listed: &[&str]) {
        let encoder = EncoderBackend::X264.elements();
        let unlisted: Vec<&String> = factories
            .iter()
            .filter(|f| !listed.contains(&f.as_str()) && !encoder.contains(&f.as_str()))
            .collect();
        assert!(unlisted.is_empty(), "not in the boot check: {unlisted:?}");
    }

    #[test]
    fn always_lists_every_element_of_the_rtsp_launch_string() {
        let stream_name = StreamName::parse("front_door").unwrap();
        let idles = [
            IdleKind::Synthetic {
                stream_name,
                overlay: "STANDBY".into(),
            },
            IdleKind::JpegStill {
                jpeg: bytes::Bytes::from_static(b"\xff\xd8"),
            },
        ];
        for idle in &idles {
            let Some(factories) = factories_of(&combined_launch_string(idle, EncoderBackend::X264))
            else {
                return;
            };
            assert_listed(&factories, ALWAYS);
        }
    }

    #[test]
    fn hls_lists_every_element_of_the_segmenter() {
        let hls = HlsBranchConfig {
            root: "/tmp/hls".into(),
            dir: "/tmp/hls/front_door".into(),
            playlist_location: "/tmp/hls/front_door/index.m3u8".into(),
            segment_location: "/tmp/hls/front_door/segment%05d.ts".into(),
            target_duration: 2,
            playlist_length: 5,
            max_files: 7,
        };
        let launch = hls_segmenter_launch("rtsp://127.0.0.1:8554/front_door", &hls);
        let Some(factories) = factories_of(&launch) else {
            return;
        };
        assert_listed(&factories, HLS);
    }

    #[test]
    fn missing_names_each_unregistered_element_once() {
        gst::init().unwrap();
        let wanted = [
            "queue",
            "no-such-element",
            "no-such-element",
            "other-missing",
        ];
        assert_eq!(
            missing(wanted.iter()),
            vec!["no-such-element", "other-missing"]
        );
    }
}
