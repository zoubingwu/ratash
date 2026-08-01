//! Privileged CoreRuntime IPC client and server adapters.

mod authorization;
mod client;
mod error;
mod ingress;
mod server;
mod socket;
mod wire;

pub use authorization::{CoreServicePeerAuthorizer, CoreServicePeerIdentity};
pub use client::CoreServiceClient;
pub use server::{CoreServiceServer, CoreServiceServerConfig};

pub const CORE_SERVICE_IPC_PROTOCOL_VERSION: u16 = 1;
