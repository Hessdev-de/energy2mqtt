use lazy_static::lazy_static;
use std::sync::Mutex;
use crate::{models::DeviceProtocol, mqtt::{SubscribeData, Transmission}, MeteringData};
use log::{debug, error, info, warn};
use tokio::sync::mpsc::Sender;
use thiserror::Error;
use std::collections::HashMap;

pub mod utils;
pub mod structs;
pub mod obis_parser;
pub mod meter_definitions;

pub struct Iec62056Manager {
    sender: Sender<Transmission>,
}

lazy_static! {
    static ref DEVICE_CONFIGS: Mutex<HashMap<String, Iec62056Config>> = Mutex::new(HashMap::new());
}

impl Iec62056Manager {
    pub fn new(sender: Sender<Transmission>) -> Self {
        Self { sender }
    }

    pub async fn start_thread(&mut self) {
        info!("Starting IEC 62056-21 thread");
        
        let (sender, mut receiver) = tokio::sync::mpsc::channel(10);

        let register = Transmission::Subscribe(SubscribeData {
            topic: "iec62056_input".to_string(),
            sender,
        });

        let _ = self.sender.send(register).await;

        info!("Starting IEC 62056-21 waiting for messages");
        while let Some(message) = receiver.recv().await {
            debug!("Received IEC 62056-21 message: {}", message);
            
            match parse_iec62056_telegram(&message) {
                Ok(metering_data) => {
                    let _ = self.sender.send(Transmission::Metering(metering_data)).await;
                }
                Err(e) => {
                    error!("IEC 62056-21 telegram parse error: {:?}", e);
                }
            }
        }
    }
}

#[derive(Error, Debug)]
pub enum Iec62056ParseError {
    #[error("Invalid telegram format")]
    InvalidFormat,
    #[error("Unsupported protocol mode")]
    UnsupportedMode,
    #[error("Invalid OBIS code format")]
    InvalidObisCode,
    #[error("Checksum verification failed")]
    ChecksumFailed,
    #[error("Device not configured")]
    DeviceNotConfigured,
    #[error("Missing identification line")]
    MissingIdentification,
    #[error("Invalid data line format")]
    InvalidDataLine,
}

#[derive(Debug, Clone)]
pub struct Iec62056Config {
    pub id: String,
    pub name: String,
    pub manufacturer: String,
    pub model: String,
    pub mode: ProtocolMode,
    pub baud_rate: u32,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ProtocolMode {
    ModeA,
    ModeB, 
    ModeC,
    ModeD,
}

fn parse_iec62056_telegram(telegram: &str) -> Result<MeteringData, Iec62056ParseError> {
    let lines: Vec<&str> = telegram.lines().collect();
    
    if lines.is_empty() {
        return Err(Iec62056ParseError::InvalidFormat);
    }

    // Parse identification line (must start with '/')
    let identification_line = lines.first()
        .ok_or(Iec62056ParseError::MissingIdentification)?;
    
    if !identification_line.starts_with('/') {
        return Err(Iec62056ParseError::MissingIdentification);
    }

    let device_info = utils::parse_identification_line(identification_line)?;
    debug!("Parsed device info: {:?}", device_info);

    // Create metering data object
    let mut mr = MeteringData::new().unwrap();
    mr.protocol = DeviceProtocol::IEC62056;
    mr.meter_name = device_info.full_id.clone();

    let mut protocol_map = serde_json::Map::new();
    protocol_map.insert("type".to_string(), "iec62056".into());
    protocol_map.insert("manufacturer".to_string(), device_info.manufacturer.clone().into());
    protocol_map.insert("identification".to_string(), device_info.identification.clone().into());
    protocol_map.insert("mode".to_string(), device_info.mode.clone().into());

    // Parse data lines (OBIS codes and values)
    let mut has_data = false;
    for line in &lines[1..] {
        if line.trim().is_empty() {
            continue;
        }
        
        // Check for end of telegram
        if line.starts_with('!') {
            debug!("End of telegram found");
            break;
        }

        // Parse OBIS data line
        match obis_parser::parse_obis_line(line) {
            Ok(obis_data) => {
                let code_clone = obis_data.code.clone();
                mr.metered_values.insert(obis_data.code, obis_data.value.into());
                if let Some(unit) = obis_data.unit {
                    mr.metered_values.insert(format!("{}_unit", code_clone), unit.into());
                }
                has_data = true;
            }
            Err(e) => {
                warn!("Failed to parse OBIS line '{}': {:?}", line, e);
            }
        }
    }

    if !has_data {
        warn!("No valid OBIS data found in telegram");
    }

    mr.metered_values.insert("proto".to_string(), protocol_map.into());
    Ok(mr)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_basic_telegram() {
        let telegram = r"/ELS5\@V5.3
1-0:1.8.1(000123.456*kWh)
1-0:1.8.2(000234.567*kWh)
1-0:15.7.0(001.234*kW)
!";
        
        let result = parse_iec62056_telegram(telegram);
        assert!(result.is_ok());
        let metering_data = result.unwrap();
        assert_eq!(metering_data.protocol, DeviceProtocol::IEC62056);
    }

    #[test]
    fn test_parse_invalid_telegram() {
        let telegram = "invalid telegram format";
        let result = parse_iec62056_telegram(telegram);
        assert!(result.is_err());
    }
}