//! Live user-local IPC client and server adapters.

mod client;
mod client_error;
mod server;
mod stream;
mod wire;

pub use client::IpcClient;
pub use server::{IpcServer, IpcServerConfig, SameUserPeerAuthorizer};
pub use stream::{
    GeneratedStreamItem, IpcStreamBroker, IpcStreamCancellation, LogStream, StatusStream,
    StatusStreamUpdate, StreamBrokerError,
};

#[cfg(test)]
mod tests;
