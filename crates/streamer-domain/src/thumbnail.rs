//! What an idle still may be: a JPEG whose declared size is bounded.
//!
//! gdk-pixbuf decodes a JPEG at the size its frame header declares, so a
//! few kilobytes of header can ask for a gigabyte of pixels (a 2.6 KB file
//! declaring 12000×12000 peaked near 1 GB). The bytes come from Arlo's
//! storage endpoint, so the header is read and bounded before anything
//! decodes them.

use thiserror::Error;

/// Largest width or height accepted for a thumbnail, in pixels. Arlo's
/// snapshots are at most 4K (3840×2160); the bound leaves room above that
/// and caps a decode at 64 MiB of RGBA.
pub const MAX_THUMBNAIL_DIMENSION: u16 = 4096;

const SOI: [u8; 2] = [0xFF, 0xD8];
const MARKER_PREFIX: u8 = 0xFF;
/// Start of scan: entropy-coded data follows, no frame header after it.
const SOS: u8 = 0xDA;
const EOI: u8 = 0xD9;
/// Markers that carry no length field: TEM and RST0–RST7.
const TEM: u8 = 0x01;
const RST_FIRST: u8 = 0xD0;
const RST_LAST: u8 = 0xD7;
/// Bytes of a frame header before the dimensions: length (2), precision (1).
const SOF_HEIGHT_OFFSET: usize = 3;

/// Declared size of a JPEG frame, in pixels.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct JpegSize {
    /// Columns.
    pub width: u16,
    /// Rows.
    pub height: u16,
}

/// Why a thumbnail was refused.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ThumbnailError {
    /// The bytes do not start with the JPEG start-of-image marker.
    #[error("not a JPEG")]
    NotJpeg,
    /// The bytes end inside a marker segment, before any frame header.
    #[error("JPEG truncated before its frame header")]
    Truncated,
    /// Image data (or the end) comes before any frame header, or a
    /// segment is malformed.
    #[error("JPEG has no frame header before its image data")]
    NoFrameHeader,
    /// A side exceeds [`MAX_THUMBNAIL_DIMENSION`].
    #[error("JPEG declares {width}x{height}; the limit is {MAX_THUMBNAIL_DIMENSION} per side")]
    TooLarge {
        /// Declared columns.
        width: u16,
        /// Declared rows.
        height: u16,
    },
    /// A side is zero (height defined later by a DNL marker included).
    #[error("JPEG declares an empty frame ({width}x{height})")]
    Empty {
        /// Declared columns.
        width: u16,
        /// Declared rows.
        height: u16,
    },
}

/// Check that `jpeg` is a JPEG whose frame is non-empty and at most
/// [`MAX_THUMBNAIL_DIMENSION`] on each side, by walking its marker
/// segments up to the first frame header. Nothing is decoded.
///
/// # Errors
///
/// A [`ThumbnailError`] naming the first thing wrong with the header.
pub fn check_thumbnail(jpeg: &[u8]) -> Result<JpegSize, ThumbnailError> {
    if !jpeg.starts_with(&SOI) {
        return Err(ThumbnailError::NotJpeg);
    }
    let size = frame_size(jpeg)?;
    let JpegSize { width, height } = size;
    if width == 0 || height == 0 {
        return Err(ThumbnailError::Empty { width, height });
    }
    if width > MAX_THUMBNAIL_DIMENSION || height > MAX_THUMBNAIL_DIMENSION {
        return Err(ThumbnailError::TooLarge { width, height });
    }
    Ok(size)
}

/// The dimensions in the first `SOFn` segment after the SOI marker.
fn frame_size(jpeg: &[u8]) -> Result<JpegSize, ThumbnailError> {
    let mut at = SOI.len();
    loop {
        if *jpeg.get(at).ok_or(ThumbnailError::Truncated)? != MARKER_PREFIX {
            return Err(ThumbnailError::NoFrameHeader);
        }
        // Any number of 0xFF fill bytes may precede the marker code.
        while jpeg.get(at) == Some(&MARKER_PREFIX) {
            at += 1;
        }
        let marker = *jpeg.get(at).ok_or(ThumbnailError::Truncated)?;
        at += 1;
        match marker {
            TEM | RST_FIRST..=RST_LAST => continue,
            SOS | EOI => return Err(ThumbnailError::NoFrameHeader),
            _ => {}
        }
        let length = usize::from(read_u16(jpeg, at)?);
        if is_frame_header(marker) {
            return Ok(JpegSize {
                height: read_u16(jpeg, at + SOF_HEIGHT_OFFSET)?,
                width: read_u16(jpeg, at + SOF_HEIGHT_OFFSET + 2)?,
            });
        }
        // The length counts its own two bytes; less is malformed.
        if length < 2 {
            return Err(ThumbnailError::NoFrameHeader);
        }
        at += length;
    }
}

/// SOF0–SOF15, except DHT (C4), JPG (C8) and DAC (CC), which share the range.
fn is_frame_header(marker: u8) -> bool {
    matches!(marker, 0xC0..=0xCF) && !matches!(marker, 0xC4 | 0xC8 | 0xCC)
}

fn read_u16(bytes: &[u8], at: usize) -> Result<u16, ThumbnailError> {
    match bytes.get(at..at + 2) {
        Some(&[hi, lo]) => Ok(u16::from_be_bytes([hi, lo])),
        _ => Err(ThumbnailError::Truncated),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// SOI, an APP0 segment, then a baseline frame header declaring
    /// `width`×`height` — what a crafted thumbnail needs to look real.
    fn jpeg_header(width: u16, height: u16) -> Vec<u8> {
        let mut v = vec![0xFF, 0xD8];
        v.extend([0xFF, 0xE0, 0x00, 0x10]);
        v.extend(b"JFIF\0");
        v.extend([0x01, 0x01, 0x00, 0x00, 0x01, 0x00, 0x01, 0x00, 0x00]);
        v.extend([0xFF, 0xC0, 0x00, 0x11, 0x08]);
        v.extend(height.to_be_bytes());
        v.extend(width.to_be_bytes());
        v.extend([0x03, 0x01, 0x22, 0x00, 0x02, 0x11, 0x01, 0x03, 0x11, 0x01]);
        v.extend([0xFF, 0xDA, 0x00, 0x02]);
        v
    }

    #[test]
    fn check_thumbnail_with_a_4k_snapshot_returns_its_size() {
        assert_eq!(
            check_thumbnail(&jpeg_header(3840, 2160)),
            Ok(JpegSize {
                width: 3840,
                height: 2160
            })
        );
    }

    #[test]
    fn check_thumbnail_at_the_limit_is_accepted() {
        let max = MAX_THUMBNAIL_DIMENSION;
        assert!(check_thumbnail(&jpeg_header(max, max)).is_ok());
    }

    #[test]
    fn check_thumbnail_with_a_huge_declared_frame_is_refused() {
        assert_eq!(
            check_thumbnail(&jpeg_header(12000, 12000)),
            Err(ThumbnailError::TooLarge {
                width: 12000,
                height: 12000
            })
        );
        assert!(matches!(
            check_thumbnail(&jpeg_header(MAX_THUMBNAIL_DIMENSION + 1, 10)),
            Err(ThumbnailError::TooLarge { .. })
        ));
    }

    #[test]
    fn check_thumbnail_with_an_empty_frame_is_refused() {
        assert!(matches!(
            check_thumbnail(&jpeg_header(640, 0)),
            Err(ThumbnailError::Empty { .. })
        ));
    }

    #[test]
    fn check_thumbnail_reads_a_progressive_frame_after_fill_bytes() {
        let mut v = jpeg_header(800, 600);
        // SOF2 (progressive) behind two fill bytes instead of SOF0.
        let sof = v.iter().position(|&b| b == 0xC0).unwrap();
        v[sof] = 0xC2;
        v.splice(sof - 1..sof - 1, [0xFF, 0xFF]);
        assert_eq!(
            check_thumbnail(&v),
            Ok(JpegSize {
                width: 800,
                height: 600
            })
        );
    }

    #[test]
    fn check_thumbnail_skips_a_huffman_table_that_shares_the_sof_range() {
        let mut v = vec![0xFF, 0xD8, 0xFF, 0xC4, 0x00, 0x04, 0x00, 0x00];
        v.extend(&jpeg_header(320, 240)[2..]);
        assert_eq!(
            check_thumbnail(&v).map(|s| (s.width, s.height)),
            Ok((320, 240))
        );
    }

    #[test]
    fn check_thumbnail_with_bytes_that_are_not_a_jpeg_is_refused() {
        assert_eq!(
            check_thumbnail(b"<html></html>"),
            Err(ThumbnailError::NotJpeg)
        );
        assert_eq!(check_thumbnail(&[]), Err(ThumbnailError::NotJpeg));
    }

    #[test]
    fn check_thumbnail_without_a_frame_header_is_refused() {
        assert_eq!(
            check_thumbnail(&[0xFF, 0xD8, 0xFF, 0xDA, 0x00, 0x02]),
            Err(ThumbnailError::NoFrameHeader)
        );
        assert_eq!(
            check_thumbnail(&[0xFF, 0xD8, 0x12, 0x34]),
            Err(ThumbnailError::NoFrameHeader)
        );
        // A segment length below its own two bytes would loop forever.
        assert_eq!(
            check_thumbnail(&[0xFF, 0xD8, 0xFF, 0xE0, 0x00, 0x00, 0xFF, 0xC0]),
            Err(ThumbnailError::NoFrameHeader)
        );
    }

    #[test]
    fn check_thumbnail_truncated_anywhere_is_refused_without_panicking() {
        let full = jpeg_header(640, 480);
        let sof_end = full.iter().position(|&b| b == 0xC0).unwrap() + 8;
        for cut in 2..sof_end {
            assert!(
                check_thumbnail(&full[..cut]).is_err(),
                "a header cut at {cut} bytes must be refused"
            );
        }
    }
}
