//! The MQTT 3.1.1 packets a publish-only client needs, encoded and decoded
//! here instead of by an unmaintained codec (third security sweep,
//! S3-L6): CONNECT (with a retained last will and a login), PUBLISH at
//! `QoS` 0, PINGREQ and DISCONNECT out; CONNACK and PINGRESP in, nothing
//! else. The CONNECT carrying the password is built in a buffer zeroed on
//! drop (S3-L7).

use secrecy::{ExposeSecret, SecretString};
use zeroize::Zeroizing;

/// The four-byte fixed body of a CONNACK, two bytes after its header.
const CONNACK: u8 = 0x20;
const PINGRESP: u8 = 0xD0;
/// PINGREQ and DISCONNECT: a fixed header and no body.
pub(crate) const PINGREQ: [u8; 2] = [0xC0, 0x00];
pub(crate) const DISCONNECT: [u8; 2] = [0xE0, 0x00];

/// The largest remaining length MQTT can express (four length bytes).
const MAX_REMAINING: usize = 268_435_455;

/// What the CONNECT says.
pub(crate) struct ConnectFields<'a> {
    pub client_id: &'a str,
    pub keep_alive_secs: u16,
    pub will_topic: &'a str,
    pub will_payload: &'a [u8],
    pub username: Option<&'a str>,
    pub password: Option<&'a SecretString>,
}

/// A packet the broker may send this client.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Incoming {
    /// CONNACK: the return code, 0 for accepted.
    ConnAck {
        code: u8,
    },
    PingResp,
}

/// Why bytes could not be encoded or decoded.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub(crate) enum CodecError {
    #[error("{0} is longer than MQTT allows")]
    TooLong(&'static str),
    #[error("unexpected packet from the broker (type {0:#04x}, {1} bytes)")]
    Unexpected(u8, usize),
}

/// The name of a CONNACK refusal code, as the specification names it.
pub(crate) fn refusal_name(code: u8) -> &'static str {
    match code {
        1 => "UnacceptableProtocolVersion",
        2 => "IdentifierRejected",
        3 => "ServiceUnavailable",
        4 => "BadUserNamePassword",
        5 => "NotAuthorized",
        _ => "UnknownRefusal",
    }
}

/// CONNECT: clean session, a retained `QoS` 0 last will, a login.
///
/// # Errors
///
/// [`CodecError::TooLong`] when a field does not fit its length prefix.
pub(crate) fn connect(fields: &ConnectFields<'_>) -> Result<Zeroizing<Vec<u8>>, CodecError> {
    let mut body = Zeroizing::new(Vec::with_capacity(64));
    put_str(&mut body, "MQTT", "protocol name")?;
    body.push(4); // protocol level: 3.1.1
    let mut flags = 0x02 | 0x04 | 0x20; // clean session, will, will retain
    if fields.username.is_some() {
        flags |= 0x80;
        if fields.password.is_some() {
            flags |= 0x40;
        }
    }
    body.push(flags);
    body.extend_from_slice(&fields.keep_alive_secs.to_be_bytes());
    put_str(&mut body, fields.client_id, "client id")?;
    put_str(&mut body, fields.will_topic, "will topic")?;
    put_bytes(&mut body, fields.will_payload, "will payload")?;
    if let Some(username) = fields.username {
        put_str(&mut body, username, "username")?;
        if let Some(password) = fields.password {
            put_bytes(&mut body, password.expose_secret().as_bytes(), "password")?;
        }
    }
    let mut packet = Zeroizing::new(Vec::with_capacity(body.len() + 5));
    packet.push(0x10);
    put_remaining(&mut packet, body.len())?;
    packet.extend_from_slice(&body);
    Ok(packet)
}

/// PUBLISH at `QoS` 0, retained.
///
/// # Errors
///
/// [`CodecError::TooLong`] when the topic or the packet is too long.
pub(crate) fn publish_retained(topic: &str, payload: &[u8]) -> Result<Vec<u8>, CodecError> {
    let topic_len = u16::try_from(topic.len()).map_err(|_| CodecError::TooLong("topic"))?;
    let remaining = 2 + usize::from(topic_len) + payload.len();
    let mut packet = Vec::with_capacity(remaining + 5);
    packet.push(0x30 | 0x01); // PUBLISH, QoS 0, retain
    put_remaining(&mut packet, remaining)?;
    packet.extend_from_slice(&topic_len.to_be_bytes());
    packet.extend_from_slice(topic.as_bytes());
    packet.extend_from_slice(payload);
    Ok(packet)
}

/// The packet a fixed header (`header`, the remaining length) and `body`
/// make: a CONNACK or a PINGRESP, nothing else.
///
/// # Errors
///
/// [`CodecError::Unexpected`] for any other packet or shape.
pub(crate) fn incoming(header: u8, body: &[u8]) -> Result<Incoming, CodecError> {
    match (header, body) {
        (CONNACK, [_session_present, code]) => Ok(Incoming::ConnAck { code: *code }),
        (PINGRESP, []) => Ok(Incoming::PingResp),
        _ => Err(CodecError::Unexpected(header, body.len())),
    }
}

/// Decode one byte of a remaining length: `Ok(Some(total))` once complete,
/// `Ok(None)` when more bytes follow.
///
/// # Errors
///
/// [`CodecError::TooLong`] past the four bytes MQTT allows.
pub(crate) fn remaining_step(
    total: &mut usize,
    index: usize,
    byte: u8,
) -> Result<Option<usize>, CodecError> {
    if index >= 4 {
        return Err(CodecError::TooLong("remaining length"));
    }
    *total += usize::from(byte & 0x7F) << (7 * index);
    Ok((byte & 0x80 == 0).then_some(*total))
}

fn put_remaining(out: &mut Vec<u8>, mut len: usize) -> Result<(), CodecError> {
    if len > MAX_REMAINING {
        return Err(CodecError::TooLong("packet"));
    }
    loop {
        // Seven bits at a time; the cast keeps the low byte on purpose.
        #[allow(clippy::cast_possible_truncation)]
        let mut byte = (len % 128) as u8;
        len /= 128;
        if len > 0 {
            byte |= 0x80;
        }
        out.push(byte);
        if len == 0 {
            return Ok(());
        }
    }
}

fn put_str(out: &mut Vec<u8>, value: &str, what: &'static str) -> Result<(), CodecError> {
    put_bytes(out, value.as_bytes(), what)
}

fn put_bytes(out: &mut Vec<u8>, value: &[u8], what: &'static str) -> Result<(), CodecError> {
    let len = u16::try_from(value.len()).map_err(|_| CodecError::TooLong(what))?;
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(value);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;
    use mqttbytes::v4::{Packet, read};

    /// mqttbytes, an independent decoder, reads what this codec writes.
    fn decoded(bytes: &[u8]) -> Packet {
        let mut buffer = BytesMut::from(bytes);
        let packet = read(&mut buffer, 1 << 24).expect("a valid packet");
        assert!(buffer.is_empty(), "exactly one packet");
        packet
    }

    #[test]
    fn connect_carries_the_will_and_the_login() {
        let password = SecretString::from("s3cret".to_string());
        let bytes = connect(&ConnectFields {
            client_id: "arlo-camera-streamer-arlo",
            keep_alive_secs: 30,
            will_topic: "arlo/status",
            will_payload: b"offline",
            username: Some("arlo"),
            password: Some(&password),
        })
        .unwrap();
        let Packet::Connect(connect) = decoded(&bytes) else {
            panic!("not a CONNECT");
        };
        assert_eq!(connect.client_id, "arlo-camera-streamer-arlo");
        assert_eq!(connect.keep_alive, 30);
        assert!(connect.clean_session);
        let will = connect.last_will.expect("a will");
        assert_eq!(
            (will.topic.as_str(), will.message.as_ref(), will.retain),
            ("arlo/status", &b"offline"[..], true)
        );
        assert_eq!(will.qos, mqttbytes::QoS::AtMostOnce);
        let login = connect.login.expect("a login");
        assert_eq!(
            (login.username.as_str(), login.password.as_str()),
            ("arlo", "s3cret")
        );
    }

    #[test]
    fn connect_without_a_login_has_none() {
        let bytes = connect(&ConnectFields {
            client_id: "c",
            keep_alive_secs: 30,
            will_topic: "s",
            will_payload: b"offline",
            username: None,
            password: None,
        })
        .unwrap();
        let Packet::Connect(connect) = decoded(&bytes) else {
            panic!("not a CONNECT");
        };
        assert!(connect.login.is_none());
    }

    /// The remaining length takes one to four bytes: check each boundary.
    #[test]
    fn publish_lengths_round_trip_across_the_length_boundaries() {
        for payload_len in [0, 100, 125, 126, 16_380, 16_381, 600_000] {
            let payload = vec![0xAB; payload_len];
            let bytes = publish_retained("a/b", &payload).unwrap();
            let Packet::Publish(publish) = decoded(&bytes) else {
                panic!("not a PUBLISH");
            };
            assert_eq!(publish.topic, "a/b");
            assert!(publish.retain);
            assert_eq!(publish.qos, mqttbytes::QoS::AtMostOnce);
            assert_eq!(publish.payload.len(), payload_len, "payload {payload_len}");
        }
    }

    #[test]
    fn pingreq_and_disconnect_are_what_mqttbytes_reads() {
        assert!(matches!(decoded(&PINGREQ), Packet::PingReq));
        assert!(matches!(decoded(&DISCONNECT), Packet::Disconnect));
    }

    #[test]
    fn only_connack_and_pingresp_are_accepted_from_the_broker() {
        assert_eq!(incoming(0x20, &[0, 0]), Ok(Incoming::ConnAck { code: 0 }));
        assert_eq!(incoming(0x20, &[0, 4]), Ok(Incoming::ConnAck { code: 4 }));
        assert_eq!(incoming(0xD0, &[]), Ok(Incoming::PingResp));
        for (header, body) in [
            (0x30, &b"\x00\x01ax"[..]), // a PUBLISH
            (0x20, &[0][..]),           // a short CONNACK
            (0x20, &[0, 0, 0][..]),     // a long one
            (0xD0, &[1][..]),           // a PINGRESP with a body
            (0x90, &[0, 1, 0][..]),     // a SUBACK
        ] {
            assert!(incoming(header, body).is_err(), "{header:#04x} {body:?}");
        }
    }

    #[test]
    fn remaining_length_decodes_up_to_four_bytes_and_no_more() {
        let decode = |bytes: &[u8]| {
            let mut total = 0;
            for (i, b) in bytes.iter().enumerate() {
                if let Some(done) = remaining_step(&mut total, i, *b)? {
                    return Ok(Some(done));
                }
            }
            Ok(None)
        };
        assert_eq!(decode(&[0x00]), Ok(Some(0)));
        assert_eq!(decode(&[0x7F]), Ok(Some(127)));
        assert_eq!(decode(&[0x80, 0x01]), Ok(Some(128)));
        assert_eq!(decode(&[0xFF, 0xFF, 0xFF, 0x7F]), Ok(Some(MAX_REMAINING)));
        assert_eq!(
            decode(&[0xFF, 0xFF, 0xFF, 0xFF, 0x01]),
            Err(CodecError::TooLong("remaining length"))
        );
    }

    #[test]
    fn a_field_past_its_length_prefix_is_refused() {
        let long = "x".repeat(70_000);
        assert_eq!(
            publish_retained(&long, b""),
            Err(CodecError::TooLong("topic"))
        );
        let refused = connect(&ConnectFields {
            client_id: &long,
            keep_alive_secs: 30,
            will_topic: "s",
            will_payload: b"",
            username: None,
            password: None,
        });
        assert_eq!(refused.err(), Some(CodecError::TooLong("client id")));
    }

    #[test]
    fn refusal_codes_have_their_specification_names() {
        assert_eq!(refusal_name(1), "UnacceptableProtocolVersion");
        assert_eq!(refusal_name(2), "IdentifierRejected");
        assert_eq!(refusal_name(3), "ServiceUnavailable");
        assert_eq!(refusal_name(4), "BadUserNamePassword");
        assert_eq!(refusal_name(5), "NotAuthorized");
        assert_eq!(refusal_name(9), "UnknownRefusal");
    }
}
