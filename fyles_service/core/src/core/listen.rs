use std::fmt;
use std::io;
use std::str::FromStr;

use thiserror::Error;

const MIN_LISTEN_PORT: u16 = 1024;

/// A port the node may listen on for peers, over TCP and QUIC alike.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ListenPort(u16);

impl ListenPort {
    pub fn get(self) -> u16 {
        self.0
    }
}

impl fmt::Display for ListenPort {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[error("{0} is not a port peers can be served on, which needs 1024 to 65535")]
pub struct InvalidListenPort(pub u32);

impl TryFrom<u16> for ListenPort {
    type Error = InvalidListenPort;

    fn try_from(port: u16) -> Result<Self, Self::Error> {
        if port >= MIN_LISTEN_PORT {
            Ok(ListenPort(port))
        } else {
            Err(InvalidListenPort(port.into()))
        }
    }
}

impl FromStr for ListenPort {
    type Err = String;

    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        let port: u32 = raw
            .parse()
            .map_err(|_| format!("{raw} is not a port number"))?;
        ListenPort::try_from(port).map_err(|e| e.to_string())
    }
}

impl TryFrom<u32> for ListenPort {
    type Error = InvalidListenPort;

    fn try_from(port: u32) -> Result<Self, Self::Error> {
        u16::try_from(port)
            .map_err(|_| InvalidListenPort(port))
            .and_then(ListenPort::try_from)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ListenFailure {
    #[error("the port is in use")]
    AddressInUse,
    #[error("the system denied access to the port")]
    AccessDenied,
    #[error("{0}")]
    Other(String),
}

impl From<io::Error> for ListenFailure {
    fn from(e: io::Error) -> Self {
        match e.kind() {
            io::ErrorKind::AddrInUse => ListenFailure::AddressInUse,
            io::ErrorKind::PermissionDenied => ListenFailure::AccessDenied,
            _ => ListenFailure::Other(e.to_string()),
        }
    }
}

/// The node listens on [`Self::active`] because [`Self::configured`], the port stored for the user,
/// failed. The two never coincide.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TemporaryPort {
    configured: ListenPort,
    failure: ListenFailure,
    active: ListenPort,
}

impl TemporaryPort {
    pub fn new(configured: ListenPort, failure: ListenFailure, active: ListenPort) -> Option<Self> {
        (active != configured).then_some(TemporaryPort {
            configured,
            failure,
            active,
        })
    }

    pub fn configured(&self) -> ListenPort {
        self.configured
    }

    pub fn failure(&self) -> &ListenFailure {
        &self.failure
    }

    pub fn active(&self) -> ListenPort {
        self.active
    }
}

/// Who decides the port peers reach the node on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ListenPortChoice {
    /// The user, in the app. The port is stored in the database.
    Stored,
    /// The daemon's `--listen-port`, so that the system's firewall and the node agree.
    Pinned(ListenPort),
}

#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[error("the listen port is pinned to {0} by the daemon's --listen-port")]
pub struct ListenPortPinned(pub ListenPort);

impl ListenPortChoice {
    pub fn allow_change(self) -> Result<(), ListenPortPinned> {
        match self {
            ListenPortChoice::Stored => Ok(()),
            ListenPortChoice::Pinned(port) => Err(ListenPortPinned(port)),
        }
    }

    pub fn set_by(self) -> PortSetBy {
        match self {
            ListenPortChoice::Stored => PortSetBy::User,
            ListenPortChoice::Pinned(_) => PortSetBy::System,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PortSetBy {
    User,
    System,
}

/// The listen status together with who decides the port, as the clients are told it. The port
/// itself is in the status.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListenReport {
    /// `None` until the node first reported ready.
    pub status: Option<ListenStatus>,
    pub set_by: PortSetBy,
}

/// Where the node accepts peers, against the port configured for the user.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ListenStatus {
    Listening(ListenPort),
    Temporary(TemporaryPort),
    NotListening {
        configured: Option<ListenPort>,
        failure: ListenFailure,
    },
}

impl ListenStatus {
    pub fn active_port(&self) -> Option<ListenPort> {
        match self {
            ListenStatus::Listening(port) => Some(*port),
            ListenStatus::Temporary(temporary) => Some(temporary.active()),
            ListenStatus::NotListening { .. } => None,
        }
    }

    pub fn is_degraded(&self) -> bool {
        !matches!(self, ListenStatus::Listening(_))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn privileged_ports_are_rejected() {
        assert_eq!(ListenPort::try_from(1023u16), Err(InvalidListenPort(1023)));
        assert_eq!(ListenPort::try_from(1024u16).map(ListenPort::get), Ok(1024));
    }

    #[test]
    fn the_configured_port_is_never_temporary() {
        let port = ListenPort::try_from(24256u16).unwrap();
        assert_eq!(
            TemporaryPort::new(port, ListenFailure::AddressInUse, port),
            None
        );
    }

    #[test]
    fn a_port_parses_from_its_digits_only() {
        assert_eq!(
            "24301".parse::<ListenPort>().map(ListenPort::get),
            Ok(24301)
        );
        assert!("80".parse::<ListenPort>().is_err());
        assert!("http".parse::<ListenPort>().is_err());
    }

    #[test]
    fn only_a_stored_port_can_be_changed() {
        let port = ListenPort::try_from(24256u16).unwrap();
        assert_eq!(ListenPortChoice::Stored.allow_change(), Ok(()));
        assert_eq!(
            ListenPortChoice::Pinned(port).allow_change(),
            Err(ListenPortPinned(port))
        );
    }

    #[test]
    fn values_beyond_u16_are_rejected() {
        assert_eq!(
            ListenPort::try_from(65536u32),
            Err(InvalidListenPort(65536))
        );
        assert_eq!(
            ListenPort::try_from(65535u32).map(ListenPort::get),
            Ok(65535)
        );
    }
}
