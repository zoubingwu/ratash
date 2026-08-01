//! Kernel-derived peer identity and authorization boundary.

use std::io;
use std::os::unix::net::UnixStream;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CoreServicePeerIdentity {
    uid: u32,
    pid: u32,
    audit_token: Option<[u32; 8]>,
}

impl CoreServicePeerIdentity {
    #[must_use]
    pub const fn uid(&self) -> u32 {
        self.uid
    }

    #[must_use]
    pub const fn pid(&self) -> u32 {
        self.pid
    }

    #[must_use]
    pub const fn audit_token(&self) -> Option<&[u32; 8]> {
        self.audit_token.as_ref()
    }
}

pub trait CoreServicePeerAuthorizer: Send + Sync {
    fn authorize(&self, peer: &CoreServicePeerIdentity) -> io::Result<()>;
}

#[derive(Clone, Copy, Debug, Default)]
pub(super) struct AcceptPeerIdentity;

impl CoreServicePeerAuthorizer for AcceptPeerIdentity {
    fn authorize(&self, _peer: &CoreServicePeerIdentity) -> io::Result<()> {
        Ok(())
    }
}

#[cfg(any(
    target_os = "macos",
    target_os = "ios",
    target_os = "watchos",
    target_os = "tvos",
    target_os = "visionos"
))]
pub(super) fn peer_identity(peer: &UnixStream) -> nix::Result<CoreServicePeerIdentity> {
    let uid = nix::sys::socket::getsockopt(peer, nix::sys::socket::sockopt::LocalPeerCred)?.uid();
    let pid = nix::sys::socket::getsockopt(peer, nix::sys::socket::sockopt::LocalPeerPid)?;
    let pid = u32::try_from(pid).map_err(|_| nix::errno::Errno::EINVAL)?;
    let audit_token =
        nix::sys::socket::getsockopt(peer, nix::sys::socket::sockopt::LocalPeerToken)?.val;
    Ok(CoreServicePeerIdentity {
        uid,
        pid,
        audit_token: Some(audit_token),
    })
}

#[cfg(any(target_os = "linux", target_os = "android"))]
pub(super) fn peer_identity(peer: &UnixStream) -> nix::Result<CoreServicePeerIdentity> {
    let credentials =
        nix::sys::socket::getsockopt(peer, nix::sys::socket::sockopt::PeerCredentials)?;
    let pid = u32::try_from(credentials.pid()).map_err(|_| nix::errno::Errno::EINVAL)?;
    Ok(CoreServicePeerIdentity {
        uid: credentials.uid(),
        pid,
        audit_token: None,
    })
}

#[cfg(not(any(
    target_os = "macos",
    target_os = "ios",
    target_os = "watchos",
    target_os = "tvos",
    target_os = "visionos",
    target_os = "linux",
    target_os = "android"
)))]
pub(super) fn peer_identity(_peer: &UnixStream) -> nix::Result<CoreServicePeerIdentity> {
    Err(nix::errno::Errno::ENOTSUP)
}
