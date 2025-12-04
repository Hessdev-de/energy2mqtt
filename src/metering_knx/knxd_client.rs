//! KNX Client using knx_rust TunnelConnection
//!
//! This module provides a wrapper around knx_rust for KNX/IP UDP tunneling.

use super::group_address::GroupAddress;
use knx_rust::group_event::{GroupEvent, GroupEventType};
use knx_rust::tunnel_connection::{TunnelConnection, TunnelConnectionConfig};
use log::{debug, info, warn};
use std::net::IpAddr;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tokio::net::UdpSocket;
use tokio::sync::Mutex;
use tokio::time::{timeout, Instant};

#[derive(Error, Debug)]
pub enum KnxError {
    #[error("Connection failed: {0}")]
    ConnectionFailed(String),

    #[error("Connection closed")]
    ConnectionClosed,

    #[error("Read timeout")]
    ReadTimeout,

    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),

    #[error("Invalid address: {0}")]
    InvalidAddress(String),

    #[error("Internal library error (sequence overflow), reconnect required")]
    SequenceOverflow,
}

/// KNX Client wrapper around knx_rust TunnelConnection
pub struct KnxClient {
    host: String,
    port: u16,
    socket: Option<UdpSocket>,
    tunnel: Option<Arc<Mutex<TunnelConnection>>>,
    read_timeout: Duration,
}

impl KnxClient {
    /// Create a new KNX client
    pub fn new(host: &str, port: u16) -> Self {
        Self {
            host: host.to_string(),
            port,
            socket: None,
            tunnel: None,
            read_timeout: Duration::from_secs(30),
        }
    }

    /// Set the read timeout
    pub fn set_read_timeout(&mut self, duration: Duration) {
        self.read_timeout = duration;
    }

    /// Check if connected
    pub fn is_connected(&self) -> bool {
        self.tunnel.is_some()
    }

    /// Connect to KNX/IP gateway
    pub async fn connect(&mut self) -> Result<(), KnxError> {
        let remote_addr: std::net::SocketAddr = format!("{}:{}", self.host, self.port)
            .parse()
            .map_err(|e| KnxError::InvalidAddress(format!("{}", e)))?;

        info!("Connecting to KNX/IP gateway at {}", remote_addr);

        let socket = UdpSocket::bind("0.0.0.0:0").await?;
        socket.connect(remote_addr).await?;

        let local_addr = socket.local_addr()?;
        info!("Local UDP socket bound to {}", local_addr);

        let ipv4 = match local_addr.ip() {
            IpAddr::V4(ip) => ip.octets(),
            IpAddr::V6(_) => return Err(KnxError::InvalidAddress("IPv6 not supported".to_string())),
        };

        let tunnel = Arc::new(Mutex::new(TunnelConnection::new(
            ipv4,
            local_addr.port(),
            TunnelConnectionConfig::default(),
        )));

        // Send initial outbound data to establish connection
        {
            let mut tunnel_lock = tunnel.lock().await;
            while let Some(data) = tunnel_lock.get_outbound_data() {
                socket.send(data).await?;
            }
        }

        // Wait for connect response
        let mut buf = [0u8; 1024];
        let connect_timeout = Duration::from_secs(10);

        let connected = loop {
            tokio::select! {
                result = timeout(connect_timeout, socket.recv(&mut buf)) => {
                    match result {
                        Ok(Ok(len)) => {
                            let mut tunnel_lock = tunnel.lock().await;
                            tunnel_lock.handle_inbound_message(&buf[..len]);

                            // Send any response data
                            while let Some(data) = tunnel_lock.get_outbound_data() {
                                socket.send(data).await?;
                            }

                            if tunnel_lock.connected() {
                                break true;
                            }
                        }
                        Ok(Err(e)) => return Err(KnxError::IoError(e)),
                        Err(_) => return Err(KnxError::ConnectionFailed("Connection timeout".to_string())),
                    }
                }
                _ = tokio::time::sleep(Duration::from_millis(100)) => {
                    let mut tunnel_lock = tunnel.lock().await;
                    tunnel_lock.handle_time_events();
                    while let Some(data) = tunnel_lock.get_outbound_data() {
                        socket.send(data).await?;
                    }
                }
            }
        };

        if !connected {
            return Err(KnxError::ConnectionFailed("Failed to establish tunnel".to_string()));
        }

        info!("KNX/IP tunnel connection established");
        self.socket = Some(socket);
        self.tunnel = Some(tunnel);
        Ok(())
    }

    /// Disconnect from KNX/IP gateway
    pub async fn disconnect(&mut self) {
        if let (Some(socket), Some(tunnel)) = (&self.socket, &self.tunnel) {
            // The tunnel will send disconnect on drop, but let's be explicit
            let mut tunnel_lock = tunnel.lock().await;
            // Flush any remaining outbound data
            while let Some(data) = tunnel_lock.get_outbound_data() {
                let _ = socket.send(data).await;
            }
        }
        self.socket = None;
        self.tunnel = None;
        info!("Disconnected from KNX/IP gateway");
    }

    /// Send a group value read request
    pub async fn send_read_request(&self, group: GroupAddress) -> Result<(), KnxError> {
        let (socket, tunnel) = self.get_socket_and_tunnel()?;

        debug!("Sending read request to {}", group);

        let mut tunnel_lock = tunnel.lock().await;

        // Catch potential overflow panic in knx-rust library
        let event = GroupEvent {
            event_type: GroupEventType::GroupValueRead,
            address: group.to_u16(),
            data: vec![],
        };
        let send_result = catch_unwind(AssertUnwindSafe(|| {
            tunnel_lock.send(event);
        }));

        if send_result.is_err() {
            warn!("KNX tunnel send panicked (likely sequence overflow), reconnection required");
            return Err(KnxError::SequenceOverflow);
        }

        // Send outbound data
        while let Some(data) = tunnel_lock.get_outbound_data() {
            socket.send(data).await?;
        }

        Ok(())
    }

    /// Send a group value write
    pub async fn send_write(&self, group: GroupAddress, value: &[u8]) -> Result<(), KnxError> {
        let (socket, tunnel) = self.get_socket_and_tunnel()?;

        debug!("Sending write to {}: {:02X?}", group, value);

        let mut tunnel_lock = tunnel.lock().await;

        // Catch potential overflow panic in knx-rust library
        let event = GroupEvent {
            event_type: GroupEventType::GroupValueWrite,
            address: group.to_u16(),
            data: value.to_vec(),
        };
        let send_result = catch_unwind(AssertUnwindSafe(|| {
            tunnel_lock.send(event);
        }));

        if send_result.is_err() {
            warn!("KNX tunnel send panicked (likely sequence overflow), reconnection required");
            return Err(KnxError::SequenceOverflow);
        }

        // Send outbound data
        while let Some(data) = tunnel_lock.get_outbound_data() {
            socket.send(data).await?;
        }

        Ok(())
    }

    /// Receive next group event (blocking with timeout)
    pub async fn recv_group_event(&self) -> Result<ReceivedGroupEvent, KnxError> {
        let (socket, tunnel) = self.get_socket_and_tunnel()?;
        let mut buf = [0u8; 1024];

        loop {
            let next_timeout = {
                let tunnel_lock = tunnel.lock().await;
                Instant::from(tunnel_lock.get_next_time_event())
            };

            tokio::select! {
                result = timeout(self.read_timeout, socket.recv(&mut buf)) => {
                    match result {
                        Ok(Ok(len)) => {
                            let mut tunnel_lock = tunnel.lock().await;
                            if let Some(event) = tunnel_lock.handle_inbound_message(&buf[..len]) {
                                // Send any ACKs
                                while let Some(data) = tunnel_lock.get_outbound_data() {
                                    socket.send(data).await?;
                                }

                                // Convert to our event type
                                let ga = GroupAddress::from_u16(event.address);
                                return Ok(ReceivedGroupEvent {
                                    address: ga,
                                    event_type: event.event_type,
                                    data: event.data,
                                });
                            }

                            // Send any outbound data (ACKs, etc.)
                            while let Some(data) = tunnel_lock.get_outbound_data() {
                                socket.send(data).await?;
                            }
                        }
                        Ok(Err(e)) => return Err(KnxError::IoError(e)),
                        Err(_) => return Err(KnxError::ReadTimeout),
                    }
                }
                _ = tokio::time::sleep_until(next_timeout) => {
                    let mut tunnel_lock = tunnel.lock().await;
                    tunnel_lock.handle_time_events();

                    // Check if still connected
                    if !tunnel_lock.connected() {
                        return Err(KnxError::ConnectionClosed);
                    }

                    // Send any keepalive data
                    while let Some(data) = tunnel_lock.get_outbound_data() {
                        socket.send(data).await?;
                    }
                }
            }
        }
    }

    fn get_socket_and_tunnel(&self) -> Result<(&UdpSocket, &Arc<Mutex<TunnelConnection>>), KnxError> {
        match (&self.socket, &self.tunnel) {
            (Some(socket), Some(tunnel)) => Ok((socket, tunnel)),
            _ => Err(KnxError::ConnectionFailed("Not connected".to_string())),
        }
    }
}

/// A received group event from the KNX bus
#[derive(Debug, Clone)]
pub struct ReceivedGroupEvent {
    pub address: GroupAddress,
    pub event_type: GroupEventType,
    pub data: Vec<u8>,
}

impl ReceivedGroupEvent {
    /// Check if this is a read request
    pub fn is_read(&self) -> bool {
        self.event_type == GroupEventType::GroupValueRead
    }

    /// Check if this is a write
    pub fn is_write(&self) -> bool {
        self.event_type == GroupEventType::GroupValueWrite
    }

    /// Check if this is a response
    pub fn is_response(&self) -> bool {
        self.event_type == GroupEventType::GroupValueResponse
    }
}

impl GroupAddress {
    /// Convert from u16 (for knx_rust compatibility)
    pub fn from_u16(value: u16) -> Self {
        Self::from_bytes(value.to_be_bytes())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_client_new() {
        let client = KnxClient::new("192.168.1.10", 3671);
        assert_eq!(client.host, "192.168.1.10");
        assert_eq!(client.port, 3671);
        assert!(!client.is_connected());
    }

    #[test]
    fn test_group_address_from_u16() {
        let ga = GroupAddress::from_u16(0x0A03); // 1/2/3
        assert_eq!(ga.main, 1);
        assert_eq!(ga.middle, 2);
        assert_eq!(ga.sub, 3);
    }
}
