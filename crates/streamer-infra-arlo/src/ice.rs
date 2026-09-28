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

/// The usable ICE servers of one call, in Arlo's order.
#[must_use]
pub fn usable_ice_servers(servers: &IceServers) -> Vec<IceServer> {
    servers
        .data
        .iter()
        .filter(|s| !(s.kind.eq_ignore_ascii_case(TURN) && s.transport.as_deref() == Some(TCP)))
        .map(|s| {
            let is_turn = s.kind.eq_ignore_ascii_case(TURN);
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
}
