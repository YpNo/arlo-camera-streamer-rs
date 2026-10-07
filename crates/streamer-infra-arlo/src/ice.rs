//! Translation of Arlo's `sipInfo` ICE list into the domain's
//! [`IceServer`] vocabulary.
//!
//! Arlo lists STUN, UDP TURN and TCP TURN. libnice + Arlo's gateway
//! only ever connect over UDP TURN; the TCP TURN negotiates flakily
//! (proven live), so TURN is kept only over UDP (an allow-list: a
//! deny-list of `tcp` let `"tcp "` or `tls` through). STUN entries never
//! carry credentials, even if the payload has some. The URL is rebuilt
//! from the validated parts (arlo-rs checks the type, an Arlo host and a
//! numeric port), never with the cloud's raw transport string.

use arlo_rs::models::sip::IceServers;

use streamer_domain::stream::IceServer;

const TURN: &str = "turn";
const UDP: &str = "udp";

/// `turn` and `turns`, whatever the case: both carry credentials and
/// both are refused over TCP.
fn is_turn(kind: &str) -> bool {
    // `get`, never a slice: `kind` is cloud input and a byte index inside
    // a multibyte character would panic the negotiation.
    kind.get(..TURN.len())
        .is_some_and(|head| head.eq_ignore_ascii_case(TURN))
}

/// Whether a TURN entry may be used: no transport stated (UDP by
/// default) or exactly `udp`.
fn is_udp_or_default(transport: Option<&str>) -> bool {
    transport.is_none_or(|t| t.eq_ignore_ascii_case(UDP))
}

/// The usable ICE servers of one call, in Arlo's order.
#[must_use]
pub fn usable_ice_servers(servers: &IceServers) -> Vec<IceServer> {
    servers
        .data
        .iter()
        .filter(|s| !is_turn(&s.kind) || is_udp_or_default(s.transport.as_deref()))
        .map(|s| {
            let is_turn = is_turn(&s.kind);
            let kind = s.kind.to_ascii_lowercase();
            let url = match s.transport.as_deref() {
                Some(_) if is_turn => format!("{kind}:{}:{}?transport={UDP}", s.domain, s.port),
                _ => format!("{kind}:{}:{}", s.domain, s.port),
            };
            IceServer {
                url,
                username: if is_turn { s.username.clone() } else { None },
                credential: if is_turn { s.credential.clone() } else { None },
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn servers(json: &str) -> IceServers {
        serde_json::from_str(json).expect("ice servers json")
    }

    #[test]
    fn usable_ice_servers_drops_tcp_turn_and_keeps_order() {
        let list = servers(
            r#"{"data":[
                {"type":"stun","domain":"stun.example","port":"19302"},
                {"type":"turn","domain":"turn.example","port":"443","transport":"tcp",
                 "username":"u","credential":"c"},
                {"type":"turn","domain":"turn.example","port":"3478","transport":"udp",
                 "username":"u","credential":"c"}
            ]}"#,
        );
        let urls: Vec<String> = usable_ice_servers(&list)
            .into_iter()
            .map(|s| s.url)
            .collect();
        assert_eq!(
            urls,
            [
                "stun:stun.example:19302",
                "turn:turn.example:3478?transport=udp"
            ]
        );
    }

    #[test]
    fn usable_ice_servers_keeps_turn_credentials_only() {
        let list = servers(
            r#"{"data":[
                {"type":"stun","domain":"stun.example","port":"19302",
                 "username":"leak","credential":"leak"},
                {"type":"TURN","domain":"turn.example","port":"3478","transport":"udp",
                 "username":"u","credential":"c"}
            ]}"#,
        );
        let out = usable_ice_servers(&list);
        assert_eq!(
            (out[0].username.as_deref(), out[0].credential.as_deref()),
            (None, None)
        );
        assert_eq!(
            (out[1].username.as_deref(), out[1].credential.as_deref()),
            (Some("u"), Some("c"))
        );
    }

    #[test]
    fn usable_ice_servers_empty_list_is_empty() {
        assert_eq!(usable_ice_servers(&servers(r#"{"data":[]}"#)).len(), 0);
    }

    #[test]
    fn is_turn_never_panics_on_non_ascii_kinds() {
        assert!(is_turn("turn"));
        assert!(is_turn("TURNS"));
        assert!(!is_turn("stun"));
        assert!(!is_turn("tur"));
        // Byte 4 falls inside the second `\u{e9}`: a slice would panic.
        assert!(!is_turn("a\u{e9}\u{e9}"));
        assert!(!is_turn("\u{e9}\u{e9}\u{e9}"));
    }

    #[test]
    fn usable_ice_servers_matches_kind_and_transport_case_insensitively() {
        let list = servers(
            r#"{"data":[
                {"type":"TURNS","domain":"turn.example","port":"5349","transport":"TCP",
                 "username":"u","credential":"c"},
                {"type":"Turn","domain":"turn.example","port":"3478","transport":"UDP",
                 "username":"u","credential":"c"}
            ]}"#,
        );
        let kept = usable_ice_servers(&list);
        assert_eq!(kept.len(), 1, "{kept:?}");
        assert!(kept[0].url.contains("3478"));
        assert_eq!(kept[0].credential.as_deref(), Some("c"));
    }

    /// Only an exact `tcp` used to be dropped: `"tcp "` or `tls` passed,
    /// and went raw into the TURN URL.
    #[test]
    fn usable_ice_servers_keeps_turn_only_over_udp_and_rebuilds_the_url() {
        let list = servers(
            r#"{"data":[
                {"type":"turn","domain":"turn.example","port":"443","transport":"tcp ",
                 "username":"u","credential":"c"},
                {"type":"turn","domain":"turn.example","port":"443","transport":"tls",
                 "username":"u","credential":"c"},
                {"type":"turn","domain":"turn.example","port":"3478","transport":"Udp",
                 "username":"u","credential":"c"},
                {"type":"turn","domain":"turn.example","port":"3479",
                 "username":"u","credential":"c"},
                {"type":"stun","domain":"stun.example","port":"19302","transport":"x&y"}
            ]}"#,
        );
        let urls: Vec<String> = usable_ice_servers(&list)
            .into_iter()
            .map(|s| s.url)
            .collect();
        assert_eq!(
            urls,
            vec![
                "turn:turn.example:3478?transport=udp",
                "turn:turn.example:3479",
                "stun:stun.example:19302",
            ]
        );
    }
}
