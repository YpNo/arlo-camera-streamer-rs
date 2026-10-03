//! Translation of Arlo's `sipInfo` ICE list into the domain's
//! [`IceServer`] vocabulary.
//!
//! Arlo lists STUN, UDP TURN and TCP TURN. libnice + Arlo's gateway
//! only ever connect over UDP TURN; the TCP TURN negotiates flakily
//! (proven live), so it is dropped here. STUN entries never carry
//! credentials, even if the payload has some.

use arlo_rs::models::sip::IceServers;

use streamer_domain::stream::IceServer;

const TURN: &str = "turn";
const TCP: &str = "tcp";

/// `turn` and `turns`, whatever the case: both carry credentials and
/// both are refused over TCP.
fn is_turn(kind: &str) -> bool {
    kind.len() >= TURN.len() && kind[..TURN.len()].eq_ignore_ascii_case(TURN)
}

fn is_tcp(transport: Option<&str>) -> bool {
    transport.is_some_and(|t| t.eq_ignore_ascii_case(TCP))
}

/// The usable ICE servers of one call, in Arlo's order.
#[must_use]
pub fn usable_ice_servers(servers: &IceServers) -> Vec<IceServer> {
    servers
        .data
        .iter()
        .filter(|s| !(is_turn(&s.kind) && is_tcp(s.transport.as_deref())))
        .map(|s| {
            let is_turn = is_turn(&s.kind);
            IceServer {
                url: s.url(),
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
        assert!(usable_ice_servers(&servers(r#"{"data":[]}"#)).is_empty());
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
}
