//! [`LiveRtpSink`] — per-camera byte conduit for inbound H.264 RTP
//! emitted by the WebRTC ingestion ([`crate::webrtc_pipeline`]) and
//! consumed by whatever feeds the live branch of the camera's media
//! pipeline.
//!
//! Phase 6.1 introduced this seam between WebRTC ingestion and live
//! consumer; Phase 6.3 wires the paired receiver into the per-camera
//! persistent factory pipeline's `appsrc` (inside
//! [`crate::gst_pipeline::GstPipelineRegistry`]), enabling the
//! IDR-aligned idle↔live splice. The public surface here (`push`)
//! stayed stable across that swap.

use bytes::Bytes;
use tokio::sync::mpsc;

/// Bound on the RTP channel — preserved from Phase 4's in-tree
/// `RTP_CHANNEL_CAPACITY`. A full channel drops; the GStreamer
/// streaming thread must never stall on a slow consumer.
pub const LIVE_RTP_CAPACITY: usize = 512;

/// Best-effort RTP-byte sink. Cloneable so the appsink callback can
/// own a private handle.
#[derive(Clone)]
pub struct LiveRtpSink {
    tx: mpsc::Sender<Bytes>,
}

impl LiveRtpSink {
    /// Build a sink + its paired receiver. The caller wires the
    /// receiver into the consumer (loopback today, `appsrc`-pump after
    /// Phase 6).
    pub fn new() -> (Self, mpsc::Receiver<Bytes>) {
        let (tx, rx) = mpsc::channel(LIVE_RTP_CAPACITY);
        (Self { tx }, rx)
    }

    /// Push one RTP buffer. Returns `false` when the buffer was dropped
    /// (channel full or closed). Callers ignore — the keyframe pump
    /// (PLI every 3 s) recovers playback, and back-pressuring the
    /// GStreamer streaming thread would stall the whole pipeline.
    pub fn push(&self, rtp: Bytes) -> bool {
        self.tx.try_send(rtp).is_ok()
    }
}

/// Paired video + audio RTP sinks handed to [`crate::webrtc_pipeline`]
/// (Phase 8b). The WebRTC recv branch pushes inbound H.264 RTP into
/// `video` and inbound Opus RTP into `audio`; the registry drains each
/// receiver into its matching `appsrc` on the persistent pipeline.
#[derive(Clone)]
pub struct LiveSinks {
    /// Inbound H.264 RTP → the persistent pipeline's `live_rtp_src`.
    pub video: LiveRtpSink,
    /// Inbound Opus RTP → the persistent pipeline's `live_audio_rtp_src`.
    pub audio: LiveRtpSink,
}

/// The receiver halves paired with a [`LiveSinks`], one per media.
pub struct LiveSinkReceivers {
    /// Drains into the video live `appsrc`.
    pub video: mpsc::Receiver<Bytes>,
    /// Drains into the audio live `appsrc`.
    pub audio: mpsc::Receiver<Bytes>,
}

impl LiveSinks {
    /// Build the video+audio sink pair and their paired receivers.
    #[must_use]
    pub fn new() -> (Self, LiveSinkReceivers) {
        let (video, video_rx) = LiveRtpSink::new();
        let (audio, audio_rx) = LiveRtpSink::new();
        (
            Self { video, audio },
            LiveSinkReceivers {
                video: video_rx,
                audio: audio_rx,
            },
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn push_routes_bytes_to_paired_receiver() {
        let (sink, mut rx) = LiveRtpSink::new();
        assert!(sink.push(Bytes::from_static(b"rtp")));
        let got = rx.try_recv().expect("delivered");
        assert_eq!(&got[..], b"rtp");
    }

    #[test]
    fn push_returns_false_when_receiver_is_dropped() {
        let (sink, rx) = LiveRtpSink::new();
        drop(rx);
        assert!(!sink.push(Bytes::from_static(b"rtp")));
    }

    #[test]
    fn push_returns_false_when_channel_is_full() {
        // Capacity-1 sink so the second push must drop, not block —
        // documenting that we use `try_send`, not `send.await`.
        let (tx, _rx) = mpsc::channel::<Bytes>(1);
        let sink = LiveRtpSink { tx };
        assert!(sink.push(Bytes::from_static(b"a")));
        assert!(!sink.push(Bytes::from_static(b"b")));
    }
}
