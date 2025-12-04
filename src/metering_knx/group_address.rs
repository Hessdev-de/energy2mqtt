//! KNX Group Address and Physical Address handling
//!
//! KNX uses two types of addresses:
//! - Physical Address: Identifies a specific device (Area.Line.Device, e.g., 1.2.3)
//! - Group Address: Used for communication (Main/Middle/Sub, e.g., 1/2/3)

use std::fmt;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum AddressError {
    #[error("Invalid address format: {0}")]
    InvalidFormat(String),
    #[error("Address component out of range: {0}")]
    OutOfRange(String),
}

/// KNX Group Address in 3-level format (Main/Middle/Sub)
///
/// Binary layout: MMMMM MMM SSSSSSSS (16 bits total)
/// - Main: 5 bits (0-31)
/// - Middle: 3 bits (0-7)
/// - Sub: 8 bits (0-255)
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct GroupAddress {
    pub main: u8,
    pub middle: u8,
    pub sub: u8,
}

impl GroupAddress {
    /// Create a new group address
    pub fn new(main: u8, middle: u8, sub: u8) -> Result<Self, AddressError> {
        if main > 31 {
            return Err(AddressError::OutOfRange(format!(
                "Main group {} exceeds maximum 31",
                main
            )));
        }
        if middle > 7 {
            return Err(AddressError::OutOfRange(format!(
                "Middle group {} exceeds maximum 7",
                middle
            )));
        }
        Ok(Self { main, middle, sub })
    }

    /// Parse from string in "main/middle/sub" format (e.g., "1/2/3")
    pub fn from_str(s: &str) -> Result<Self, AddressError> {
        let parts: Vec<&str> = s.split('/').collect();
        if parts.len() != 3 {
            return Err(AddressError::InvalidFormat(format!(
                "Expected 3 parts separated by '/', got {}",
                parts.len()
            )));
        }

        let main: u8 = parts[0]
            .parse()
            .map_err(|_| AddressError::InvalidFormat(format!("Invalid main group: {}", parts[0])))?;
        let middle: u8 = parts[1]
            .parse()
            .map_err(|_| AddressError::InvalidFormat(format!("Invalid middle group: {}", parts[1])))?;
        let sub: u8 = parts[2]
            .parse()
            .map_err(|_| AddressError::InvalidFormat(format!("Invalid sub group: {}", parts[2])))?;

        Self::new(main, middle, sub)
    }

    /// Parse from 2-byte raw format (big-endian)
    pub fn from_bytes(bytes: [u8; 2]) -> Self {
        let raw = u16::from_be_bytes(bytes);
        Self {
            main: ((raw >> 11) & 0x1F) as u8,
            middle: ((raw >> 8) & 0x07) as u8,
            sub: (raw & 0xFF) as u8,
        }
    }

    /// Convert to 2-byte raw format (big-endian)
    pub fn to_bytes(&self) -> [u8; 2] {
        let raw: u16 = ((self.main as u16) << 11) | ((self.middle as u16) << 8) | (self.sub as u16);
        raw.to_be_bytes()
    }

    /// Convert to u16 for use as map key or comparison
    pub fn to_u16(&self) -> u16 {
        u16::from_be_bytes(self.to_bytes())
    }

    pub fn to_string(&self) -> String {
        format!("{}/{}/{}", self.main, self.middle, self.sub)
    }
}

impl fmt::Display for GroupAddress {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}/{}", self.main, self.middle, self.sub)
    }
}

impl fmt::Debug for GroupAddress {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "GroupAddress({})", self)
    }
}

/// KNX Physical Address (Area.Line.Device)
///
/// Binary layout: AAAA LLLL DDDDDDDD (16 bits total)
/// - Area: 4 bits (0-15)
/// - Line: 4 bits (0-15)
/// - Device: 8 bits (0-255)
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct PhysicalAddress {
    pub area: u8,
    pub line: u8,
    pub device: u8,
}

impl PhysicalAddress {
    /// Create a new physical address
    pub fn new(area: u8, line: u8, device: u8) -> Result<Self, AddressError> {
        if area > 15 {
            return Err(AddressError::OutOfRange(format!(
                "Area {} exceeds maximum 15",
                area
            )));
        }
        if line > 15 {
            return Err(AddressError::OutOfRange(format!(
                "Line {} exceeds maximum 15",
                line
            )));
        }
        Ok(Self { area, line, device })
    }

    /// Parse from string in "area.line.device" format (e.g., "1.2.3")
    pub fn from_str(s: &str) -> Result<Self, AddressError> {
        let parts: Vec<&str> = s.split('.').collect();
        if parts.len() != 3 {
            return Err(AddressError::InvalidFormat(format!(
                "Expected 3 parts separated by '.', got {}",
                parts.len()
            )));
        }

        let area: u8 = parts[0]
            .parse()
            .map_err(|_| AddressError::InvalidFormat(format!("Invalid area: {}", parts[0])))?;
        let line: u8 = parts[1]
            .parse()
            .map_err(|_| AddressError::InvalidFormat(format!("Invalid line: {}", parts[1])))?;
        let device: u8 = parts[2]
            .parse()
            .map_err(|_| AddressError::InvalidFormat(format!("Invalid device: {}", parts[2])))?;

        Self::new(area, line, device)
    }

    /// Parse from 2-byte raw format (big-endian)
    pub fn from_bytes(bytes: [u8; 2]) -> Self {
        Self {
            area: (bytes[0] >> 4) & 0x0F,
            line: bytes[0] & 0x0F,
            device: bytes[1],
        }
    }

    /// Convert to 2-byte raw format (big-endian)
    pub fn to_bytes(&self) -> [u8; 2] {
        [((self.area & 0x0F) << 4) | (self.line & 0x0F), self.device]
    }
}

impl fmt::Display for PhysicalAddress {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}.{}", self.area, self.line, self.device)
    }
}

impl fmt::Debug for PhysicalAddress {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "PhysicalAddress({})", self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_group_address_new_valid() {
        let ga = GroupAddress::new(1, 2, 3).unwrap();
        assert_eq!(ga.main, 1);
        assert_eq!(ga.middle, 2);
        assert_eq!(ga.sub, 3);
    }

    #[test]
    fn test_group_address_new_max_values() {
        let ga = GroupAddress::new(31, 7, 255).unwrap();
        assert_eq!(ga.main, 31);
        assert_eq!(ga.middle, 7);
        assert_eq!(ga.sub, 255);
    }

    #[test]
    fn test_group_address_new_main_out_of_range() {
        let result = GroupAddress::new(32, 0, 0);
        assert!(result.is_err());
    }

    #[test]
    fn test_group_address_new_middle_out_of_range() {
        let result = GroupAddress::new(0, 8, 0);
        assert!(result.is_err());
    }

    #[test]
    fn test_group_address_from_str() {
        let ga = GroupAddress::from_str("1/2/3").unwrap();
        assert_eq!(ga.main, 1);
        assert_eq!(ga.middle, 2);
        assert_eq!(ga.sub, 3);
    }

    #[test]
    fn test_group_address_from_str_max() {
        let ga = GroupAddress::from_str("31/7/255").unwrap();
        assert_eq!(ga.main, 31);
        assert_eq!(ga.middle, 7);
        assert_eq!(ga.sub, 255);
    }

    #[test]
    fn test_group_address_from_str_invalid_format() {
        assert!(GroupAddress::from_str("1/2").is_err());
        assert!(GroupAddress::from_str("1/2/3/4").is_err());
        assert!(GroupAddress::from_str("1.2.3").is_err());
    }

    #[test]
    fn test_group_address_from_str_invalid_values() {
        assert!(GroupAddress::from_str("32/0/0").is_err());
        assert!(GroupAddress::from_str("0/8/0").is_err());
        assert!(GroupAddress::from_str("abc/0/0").is_err());
    }

    #[test]
    fn test_group_address_to_bytes() {
        let ga = GroupAddress::new(1, 2, 3).unwrap();
        let bytes = ga.to_bytes();
        // 1/2/3 = 00001 010 00000011 = 0x0A03
        assert_eq!(bytes, [0x0A, 0x03]);
    }

    #[test]
    fn test_group_address_from_bytes() {
        let ga = GroupAddress::from_bytes([0x0A, 0x03]);
        assert_eq!(ga.main, 1);
        assert_eq!(ga.middle, 2);
        assert_eq!(ga.sub, 3);
    }

    #[test]
    fn test_group_address_roundtrip() {
        let original = GroupAddress::new(15, 5, 128).unwrap();
        let bytes = original.to_bytes();
        let restored = GroupAddress::from_bytes(bytes);
        assert_eq!(original, restored);
    }

    #[test]
    fn test_group_address_display() {
        let ga = GroupAddress::new(1, 2, 3).unwrap();
        assert_eq!(format!("{}", ga), "1/2/3");
    }

    #[test]
    fn test_physical_address_new_valid() {
        let pa = PhysicalAddress::new(1, 2, 3).unwrap();
        assert_eq!(pa.area, 1);
        assert_eq!(pa.line, 2);
        assert_eq!(pa.device, 3);
    }

    #[test]
    fn test_physical_address_new_max_values() {
        let pa = PhysicalAddress::new(15, 15, 255).unwrap();
        assert_eq!(pa.area, 15);
        assert_eq!(pa.line, 15);
        assert_eq!(pa.device, 255);
    }

    #[test]
    fn test_physical_address_new_out_of_range() {
        assert!(PhysicalAddress::new(16, 0, 0).is_err());
        assert!(PhysicalAddress::new(0, 16, 0).is_err());
    }

    #[test]
    fn test_physical_address_from_str() {
        let pa = PhysicalAddress::from_str("1.2.3").unwrap();
        assert_eq!(pa.area, 1);
        assert_eq!(pa.line, 2);
        assert_eq!(pa.device, 3);
    }

    #[test]
    fn test_physical_address_to_bytes() {
        let pa = PhysicalAddress::new(1, 2, 3).unwrap();
        let bytes = pa.to_bytes();
        // 1.2.3 = 0001 0010 00000011 = 0x1203
        assert_eq!(bytes, [0x12, 0x03]);
    }

    #[test]
    fn test_physical_address_from_bytes() {
        let pa = PhysicalAddress::from_bytes([0x12, 0x03]);
        assert_eq!(pa.area, 1);
        assert_eq!(pa.line, 2);
        assert_eq!(pa.device, 3);
    }

    #[test]
    fn test_physical_address_roundtrip() {
        let original = PhysicalAddress::new(10, 5, 200).unwrap();
        let bytes = original.to_bytes();
        let restored = PhysicalAddress::from_bytes(bytes);
        assert_eq!(original, restored);
    }

    #[test]
    fn test_physical_address_display() {
        let pa = PhysicalAddress::new(1, 2, 3).unwrap();
        assert_eq!(format!("{}", pa), "1.2.3");
    }
}
