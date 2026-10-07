use std::net::SocketAddr;
use thiserror::Error;

/// Errors that can occur when starting or running the ops HTTP server.
#[derive(Error, Debug)]
pub enum OpsError {
    /// Server failed to bind to the requested port.
    #[error("failed to bind {addr}: {source}")]
    Bind {
        /// The address that we attempted to bind to.
        addr: SocketAddr,
        /// The underlying I/O error.
        #[source]
        source: std::io::Error,
    },

    /// Server encountered an error while serving requests.
    #[error("ops server encountered a fatal error: {0}")]
    Serve(#[source] std::io::Error),
}
