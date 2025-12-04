//! KNX Telegram structures
//!
//! This module defines the structure of KNX telegrams as received from KNXD.

use super::group_address::{GroupAddress, PhysicalAddress};
use std::time::{SystemTime, UNIX_EPOCH};

/// APCI (Application Protocol Control Information) - defines the telegram type
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KnxApci {
    /// A_GroupValue_Read - Request to read a group value
    GroupValueRead,
    /// A_GroupValue_Response - Response to a read request
    GroupValueResponse,
    /// A_GroupValue_Write - Write a value to a group
    GroupValueWrite,
    /// Unknown APCI value
    Unknown(u8),
}

impl KnxApci {
    /// Parse APCI from the APCI byte (lower 2 bits of TPCI/APCI byte + data[0] upper bits)
    ///
    /// APCI is encoded in bits:
    /// - GroupValueRead:     0000 00 (0x00)
    /// - GroupValueResponse: 0001 xx (0x40-0x7F with 6-bit data)
    /// - GroupValueWrite:    0010 xx (0x80-0xBF with 6-bit data)
    pub fn from_apci_byte(apci: u8) -> Self {
        match apci & 0xC0 {
            0x00 => KnxApci::GroupValueRead,
            0x40 => KnxApci::GroupValueResponse,
            0x80 => KnxApci::GroupValueWrite,
            _ => KnxApci::Unknown(apci),
        }
    }

    /// Check if this APCI carries data (Response or Write)
    pub fn has_data(&self) -> bool {
        matches!(self, KnxApci::GroupValueResponse | KnxApci::GroupValueWrite)
    }

    /// Get the APCI byte value for encoding
    pub fn to_apci_byte(&self) -> u8 {
        match self {
            KnxApci::GroupValueRead => 0x00,
            KnxApci::GroupValueResponse => 0x40,
            KnxApci::GroupValueWrite => 0x80,
            KnxApci::Unknown(v) => *v,
        }
    }
}

/// A KNX telegram received from the bus via KNXD
#[derive(Debug, Clone)]
pub struct KnxTelegram {
    /// Source physical address of the sending device
    pub source: PhysicalAddress,
    /// Destination group address
    pub destination: GroupAddress,
    /// APCI - type of telegram
    pub apci: KnxApci,
    /// Data payload (may be empty for GroupValueRead)
    pub data: Vec<u8>,
    /// Timestamp when the telegram was received (Unix timestamp in milliseconds)
    pub timestamp_ms: u64,
}

impl KnxTelegram {
    /// Create a new telegram with current timestamp
    pub fn new(
        source: PhysicalAddress,
        destination: GroupAddress,
        apci: KnxApci,
        data: Vec<u8>,
    ) -> Self {
        let timestamp_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);

        Self {
            source,
            destination,
            apci,
            data,
            timestamp_ms,
        }
    }

    /// Parse a telegram from KNXD group packet format
    ///
    /// KNXD group packet format (after 4-byte header):
    /// - Bytes 0-1: Source physical address (big-endian)
    /// - Bytes 2-3: Destination group address (big-endian)
    /// - Byte 4: APCI (and possibly first 6 bits of data for small payloads)
    /// - Bytes 5+: Additional data (if any)
    pub fn from_knxd_packet(data: &[u8]) -> Option<Self> {
        if data.len() < 5 {
            return None;
        }

        let source = PhysicalAddress::from_bytes([data[0], data[1]]);
        let destination = GroupAddress::from_bytes([data[2], data[3]]);
        let apci = KnxApci::from_apci_byte(data[4]);

        // Extract data payload
        let payload = if data.len() > 5 {
            // Data is in bytes 5+
            // For small values (<=6 bits), data might be in lower bits of byte 4
            let mut payload = Vec::with_capacity(data.len() - 4);

            // If APCI has data, the lower 6 bits of byte 4 might contain data
            if apci.has_data() {
                // First byte might contain 6-bit data in lower bits
                payload.push(data[4] & 0x3F);
            }

            // Append remaining bytes
            payload.extend_from_slice(&data[5..]);
            payload
        } else if apci.has_data() {
            // Only the APCI byte, data is in lower 6 bits
            vec![data[4] & 0x3F]
        } else {
            Vec::new()
        };

        Some(Self::new(source, destination, apci, payload))
    }

    /// Check if this is a read request
    pub fn is_read(&self) -> bool {
        self.apci == KnxApci::GroupValueRead
    }

    /// Check if this is a response
    pub fn is_response(&self) -> bool {
        self.apci == KnxApci::GroupValueResponse
    }

    /// Check if this is a write
    pub fn is_write(&self) -> bool {
        self.apci == KnxApci::GroupValueWrite
    }

    /// Get data as a single byte (for DPT1, DPT5, etc.)
    pub fn data_as_u8(&self) -> Option<u8> {
        self.data.first().copied()
    }

    /// Get data as two bytes (for DPT7, DPT8, DPT9, etc.)
    pub fn data_as_u16(&self) -> Option<u16> {
        if self.data.len() >= 2 {
            Some(u16::from_be_bytes([self.data[0], self.data[1]]))
        } else {
            None
        }
    }

    /// Get data as four bytes (for DPT12, DPT13, DPT14, etc.)
    pub fn data_as_u32(&self) -> Option<u32> {
        if self.data.len() >= 4 {
            Some(u32::from_be_bytes([
                self.data[0],
                self.data[1],
                self.data[2],
                self.data[3],
            ]))
        } else {
            None
        }
    }

    /// Get data as IEEE 754 float (for DPT14)
    pub fn data_as_f32(&self) -> Option<f32> {
        self.data_as_u32().map(f32::from_bits)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_apci_from_byte_read() {
        assert_eq!(KnxApci::from_apci_byte(0x00), KnxApci::GroupValueRead);
    }

    #[test]
    fn test_apci_from_byte_response() {
        // Response with various data values in lower bits
        assert_eq!(KnxApci::from_apci_byte(0x40), KnxApci::GroupValueResponse);
        assert_eq!(KnxApci::from_apci_byte(0x41), KnxApci::GroupValueResponse);
        assert_eq!(KnxApci::from_apci_byte(0x7F), KnxApci::GroupValueResponse);
    }

    #[test]
    fn test_apci_from_byte_write() {
        // Write with various data values in lower bits
        assert_eq!(KnxApci::from_apci_byte(0x80), KnxApci::GroupValueWrite);
        assert_eq!(KnxApci::from_apci_byte(0x81), KnxApci::GroupValueWrite);
        assert_eq!(KnxApci::from_apci_byte(0xBF), KnxApci::GroupValueWrite);
    }

    #[test]
    fn test_apci_has_data() {
        assert!(!KnxApci::GroupValueRead.has_data());
        assert!(KnxApci::GroupValueResponse.has_data());
        assert!(KnxApci::GroupValueWrite.has_data());
    }

    #[test]
    fn test_telegram_from_knxd_packet_read() {
        // Source: 1.2.3, Dest: 1/2/3, APCI: Read
        let packet = vec![
            0x12, 0x03, // Source: 1.2.3
            0x0A, 0x03, // Dest: 1/2/3
            0x00, // APCI: Read
        ];

        let telegram = KnxTelegram::from_knxd_packet(&packet).unwrap();
        assert_eq!(telegram.source.area, 1);
        assert_eq!(telegram.source.line, 2);
        assert_eq!(telegram.source.device, 3);
        assert_eq!(telegram.destination.main, 1);
        assert_eq!(telegram.destination.middle, 2);
        assert_eq!(telegram.destination.sub, 3);
        assert!(telegram.is_read());
        assert!(telegram.data.is_empty());
    }

    #[test]
    fn test_telegram_from_knxd_packet_write_small() {
        // Source: 1.2.3, Dest: 1/2/3, APCI: Write, Data: 1 (in APCI byte)
        let packet = vec![
            0x12, 0x03, // Source: 1.2.3
            0x0A, 0x03, // Dest: 1/2/3
            0x81, // APCI: Write + data=1
        ];

        let telegram = KnxTelegram::from_knxd_packet(&packet).unwrap();
        assert!(telegram.is_write());
        assert_eq!(telegram.data, vec![0x01]);
    }

    #[test]
    fn test_telegram_from_knxd_packet_write_large() {
        // Source: 1.2.3, Dest: 1/2/3, APCI: Write, Data: 4 bytes (DPT14)
        let packet = vec![
            0x12, 0x03, // Source: 1.2.3
            0x0A, 0x03, // Dest: 1/2/3
            0x80, // APCI: Write
            0x41, 0x20, 0x00, 0x00, // Data: 10.0 as f32
        ];

        let telegram = KnxTelegram::from_knxd_packet(&packet).unwrap();
        assert!(telegram.is_write());
        assert_eq!(telegram.data.len(), 5); // APCI lower bits + 4 bytes
    }

    #[test]
    fn test_telegram_from_knxd_packet_too_short() {
        let packet = vec![0x12, 0x03, 0x0A, 0x03]; // Missing APCI byte
        assert!(KnxTelegram::from_knxd_packet(&packet).is_none());
    }

    #[test]
    fn test_telegram_data_as_u8() {
        let telegram = KnxTelegram::new(
            PhysicalAddress::from_bytes([0x12, 0x03]),
            GroupAddress::from_bytes([0x0A, 0x03]),
            KnxApci::GroupValueWrite,
            vec![0x42],
        );
        assert_eq!(telegram.data_as_u8(), Some(0x42));
    }

    #[test]
    fn test_telegram_data_as_u16() {
        let telegram = KnxTelegram::new(
            PhysicalAddress::from_bytes([0x12, 0x03]),
            GroupAddress::from_bytes([0x0A, 0x03]),
            KnxApci::GroupValueWrite,
            vec![0x12, 0x34],
        );
        assert_eq!(telegram.data_as_u16(), Some(0x1234));
    }

    #[test]
    fn test_telegram_data_as_u32() {
        let telegram = KnxTelegram::new(
            PhysicalAddress::from_bytes([0x12, 0x03]),
            GroupAddress::from_bytes([0x0A, 0x03]),
            KnxApci::GroupValueWrite,
            vec![0x12, 0x34, 0x56, 0x78],
        );
        assert_eq!(telegram.data_as_u32(), Some(0x12345678));
    }

    #[test]
    fn test_telegram_data_as_f32() {
        // 10.0 as IEEE 754 float = 0x41200000
        let telegram = KnxTelegram::new(
            PhysicalAddress::from_bytes([0x12, 0x03]),
            GroupAddress::from_bytes([0x0A, 0x03]),
            KnxApci::GroupValueWrite,
            vec![0x41, 0x20, 0x00, 0x00],
        );
        assert_eq!(telegram.data_as_f32(), Some(10.0));
    }
}
