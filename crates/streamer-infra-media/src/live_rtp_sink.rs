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

/// How a relayed AAC audio stream is packetised (RFC 3640 `AAC-hbr`),
/// read from the stream's SDP (`a=rtpmap` + `a=fmtp`). It becomes the
/// caps of the pipeline's AAC `appsrc`, so the depayloader and decoder
/// know the sampling rate, channel count and `AudioSpecificConfig`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AacRtpFormat {
    /// RTP clock rate, the AAC sampling rate.
    pub clock_rate: u32,
    /// Audio channels (`encoding-params`).
    pub channels: u32,
    /// `AudioSpecificConfig` as hex (`config=`).
    pub config: String,
    /// AU-header size field width (`sizelength`); 13 for `AAC-hbr`.
    pub size_length: u32,
    /// AU-header index field width (`indexlength`); 3 for `AAC-hbr`.
    pub index_length: u32,
    /// AU-header index-delta width (`indexdeltalength`); 3 for `AAC-hbr`.
    pub index_delta_length: u32,
}

impl AacRtpFormat {
    /// Caps of an RTP `appsrc` that feeds `rtpmp4gdepay` with payload
    /// type `pt`.
    #[must_use]
    pub fn caps_string(&self, pt: i32) -> String {
        // The SDP-derived fields are strings in GStreamer's RTP caps
        // (`rtpmp4gdepay` reads them with `gst_structure_get_string`);
        // untyped digits would parse as integers and be ignored.
        format!(
            "application/x-rtp,media=audio,encoding-name=MPEG4-GENERIC,\
clock-rate={},encoding-params=(string){},payload={pt},mode=(string)AAC-hbr,config=(string){},\
sizelength=(string){},indexlength=(string){},indexdeltalength=(string){}",
            self.clock_rate,
            self.channels,
            self.config,
            self.size_length,
            self.index_length,
            self.index_delta_length
        )
    }
}

/// What the AAC audio sink carries: the stream's format first, then its
/// RTP packets.
#[derive(Clone, Debug)]
pub enum AacFeed {
    /// The format the following packets are in; sets the `appsrc` caps.
    Format(AacRtpFormat),
    /// One AAC RTP packet.
    Rtp(Bytes),
}

/// Best-effort sink for a relayed AAC audio stream (ADR 0007): the
/// format is announced once, the packets follow. Same drop-when-full
/// contract as [`LiveRtpSink`].
#[derive(Clone)]
pub struct LiveAacSink {
    tx: mpsc::Sender<AacFeed>,
}

impl LiveAacSink {
    /// Build a sink + its paired receiver.
    pub fn new() -> (Self, mpsc::Receiver<AacFeed>) {
        let (tx, rx) = mpsc::channel(LIVE_RTP_CAPACITY);
        (Self { tx }, rx)
    }

    /// Announce the format of the packets to come. `false` when dropped.
    pub fn configure(&self, format: AacRtpFormat) -> bool {
        self.tx.try_send(AacFeed::Format(format)).is_ok()
    }

    /// Push one AAC RTP packet. `false` when dropped (full or closed).
    pub fn push(&self, rtp: Bytes) -> bool {
        self.tx.try_send(AacFeed::Rtp(rtp)).is_ok()
    }
}

/// The live sinks of one camera (Phase 8b, ADR 0007). The WebRTC recv
/// branch pushes inbound H.264 RTP into `video` and Opus RTP into
/// `audio`; the app-view relay pushes H.264 into `video` and AAC into
/// `aac`. The registry drains each receiver into its matching `appsrc`
/// on the persistent pipeline.
#[derive(Clone)]
pub struct LiveSinks {
    /// Inbound H.264 RTP → the persistent pipeline's `live_rtp_src`.
    pub video: LiveRtpSink,
    /// Inbound Opus RTP → the persistent pipeline's `live_audio_rtp_src`.
    pub audio: LiveRtpSink,
    /// Relayed AAC RTP → the persistent pipeline's `live_aac_rtp_src`.
    pub aac: LiveAacSink,
}

/// The receiver halves paired with a [`LiveSinks`], one per media.
pub struct LiveSinkReceivers {
    /// Drains into the video live `appsrc`.
    pub video: mpsc::Receiver<Bytes>,
    /// Drains into the Opus audio live `appsrc`.
    pub audio: mpsc::Receiver<Bytes>,
    /// Drains into the AAC audio live `appsrc`.
    pub aac: mpsc::Receiver<AacFeed>,
}

impl LiveSinks {
    /// Build the sinks and their paired receivers.
    #[must_use]
    pub fn new() -> (Self, LiveSinkReceivers) {
        let (video, video_rx) = LiveRtpSink::new();
        let (audio, audio_rx) = LiveRtpSink::new();
        let (aac, aac_rx) = LiveAacSink::new();
        (
            Self { video, audio, aac },
            LiveSinkReceivers {
                video: video_rx,
                audio: audio_rx,
                aac: aac_rx,
            },
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn aac_sink_announces_the_format_before_the_packets() {
        let (sink, mut rx) = LiveAacSink::new();
        let format = AacRtpFormat {
            clock_rate: 16000,
            channels: 1,
            config: "1408".into(),
            size_length: 13,
            index_length: 3,
            index_delta_length: 3,
        };
        assert!(sink.configure(format.clone()));
        assert!(sink.push(Bytes::from_static(&[0x80, 0x62])));
        assert!(matches!(rx.try_recv(), Ok(AacFeed::Format(f)) if f == format));
        assert!(matches!(rx.try_recv(), Ok(AacFeed::Rtp(b)) if b.len() == 2));
        drop(rx);
        assert!(!sink.push(Bytes::new()));
    }

    #[test]
    fn aac_caps_string_carries_every_rfc3640_field() {
        let caps = AacRtpFormat {
            clock_rate: 16000,
            channels: 1,
            config: "1408".into(),
            size_length: 13,
            index_length: 3,
            index_delta_length: 3,
        }
        .caps_string(98);
        assert_eq!(
            caps,
            "application/x-rtp,media=audio,encoding-name=MPEG4-GENERIC,clock-rate=16000,\
encoding-params=(string)1,payload=98,mode=(string)AAC-hbr,config=(string)1408,\
sizelength=(string)13,indexlength=(string)3,indexdeltalength=(string)3"
        );
    }

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
