use crate::{models::DeviceProtocol, mqtt::{SubscribeData, Transmission, MeteringData, TranmissionValueType}};
use log::{debug, error, info};
use std::collections::HashMap;
use tokio::sync::mpsc::Sender;

pub mod structs;
pub mod parser;
pub mod utils;
pub mod meter_definitions;

use structs::*;
use parser::*;
use utils::*;

#[derive(Debug)]
pub enum SmlError {
    InvalidMessage,
    ParseError(String),
    MqttError(String),
    ConfigError(String),
}

pub struct SmlManager {
    sender: Sender<Transmission>,
    device_definitions: HashMap<String, MeterDefinition>,
}

impl SmlManager {
    pub fn new(sender: Sender<Transmission>) -> Self {
        Self {
            sender,
            device_definitions: meter_definitions::get_supported_meters(),
        }
    }

    pub async fn start_thread(&mut self) {
        info!("Starting SML thread");
        
        // Subscribe to SML input topic
        let (sender, mut receiver) = tokio::sync::mpsc::channel(10);
        let register = Transmission::Subscribe(SubscribeData {
            topic: "sml_input".to_string(),
            sender,
        });
        
        let _ = self.sender.send(register).await;
        
        info!("Starting SML waiting for messages");
        while let Some(payload_hex) = receiver.recv().await {
            let payload = match hex::decode(&payload_hex) {
                Ok(data) => data,
                Err(_) => {
                    error!("Non hex string received: {}", payload_hex);
                    continue;
                }
            };
            
            self.handle_sml_message(&payload).await;
        }
    }

    async fn handle_sml_message(&self, payload: &[u8]) {
        debug!("Received SML message with {} bytes", payload.len());
        
        match parse_sml_message(payload) {
            Ok(sml_file) => {
                info!("Successfully parsed SML message with {} entries", sml_file.messages.len());
                
                // Process each SML message in the file
                for message in &sml_file.messages {
                    if let Some(get_list_response) = &message.message_body.get_list_response {
                        self.process_get_list_response(get_list_response, &message.client_id).await;
                    }
                }
            }
            Err(e) => {
                error!("Failed to parse SML message: {:?}", e);
            }
        }
    }

    async fn process_get_list_response(&self, response: &SmlGetListResponse, _client_id: &Option<Vec<u8>>) {
        let server_id = response.server_id.as_ref()
            .map(|id| hex::encode(id))
            .unwrap_or_else(|| "unknown".to_string());
        
        debug!("Processing GetList response from server: {}", server_id);

        // Identify meter type based on server ID or other characteristics
        let meter_type = self.identify_meter_type(&server_id, &response.val_list);
        
        // Convert SML entries to metered values
        let mut metered_values = serde_json::Map::new();
        
        for entry in &response.val_list {
            if let Some(obis_code) = &entry.obis_code {
                let obis_str = format_obis_code(obis_code);
                
                if let Some(value) = &entry.value {
                    let (mut value_str, unit) = parse_sml_value(value);
                    
                    // Apply scaler and unit if present
                    if entry.scaler.is_some() || entry.unit.is_some() {
                        let (scaled_value, final_unit) = apply_scaler_and_unit(&value_str, entry.scaler, entry.unit);
                        value_str = scaled_value;
                        if let Some(u) = final_unit {
                            value_str = format!("{} {}", value_str, u);
                        }
                    } else if let Some(u) = unit {
                        value_str = format!("{} {}", value_str, u);
                    }
                    
                    // Map to field name if we have a meter definition
                    let field_name = if let Some(field_name) = self.get_field_mapping(&meter_type, &obis_str) {
                        field_name
                    } else {
                        // Use OBIS code as field name if no mapping available
                        obis_str
                    };
                    
                    metered_values.insert(field_name, serde_json::Value::String(value_str));
                }
            }
        }

        // Create and publish MeteringData
        let current_time = crate::get_unix_ts();
        let metering_data = MeteringData {
            id: format!("sml-{}", server_id),
            meter_name: format!("SML-{}", server_id),
            tenant: "default".to_string(),
            protocol: DeviceProtocol::SML,
            transmission_time: current_time,
            transmission_type: TranmissionValueType::Now,
            metered_time: current_time,
            metered_values,
        };

        // Send metering data through the transmission channel
        if let Err(e) = self.sender.send(Transmission::Metering(metering_data)).await {
            error!("Failed to send SML metering data: {}", e);
        } else {
            debug!("Successfully sent SML metering data for server: {}", server_id);
        }
    }

    fn identify_meter_type(&self, server_id: &str, val_list: &[SmlListEntry]) -> MeterType {
        // Try to identify meter type based on server ID patterns
        for (pattern, meter_def) in &self.device_definitions {
            if server_id.contains(pattern) {
                return meter_def.meter_type.clone();
            }
        }
        
        // Fallback: try to identify based on available OBIS codes
        let obis_codes: Vec<String> = val_list.iter()
            .filter_map(|entry| entry.obis_code.as_ref())
            .map(|code| format_obis_code(code))
            .collect();
        
        // EMH meters typically have specific OBIS patterns
        if obis_codes.iter().any(|code| code.starts_with("129-129:")) {
            return MeterType::EMH;
        }
        
        // Iskraemeco meters have different patterns
        if obis_codes.iter().any(|code| code.starts_with("1-0:0.0.0")) {
            return MeterType::Iskraemeco;
        }
        
        MeterType::Generic
    }

    fn get_field_mapping(&self, meter_type: &MeterType, obis_code: &str) -> Option<String> {
        // Look up field mapping in meter definitions
        for meter_def in self.device_definitions.values() {
            if meter_def.meter_type == *meter_type {
                if let Some(field_name) = meter_def.obis_mapping.get(obis_code) {
                    return Some(field_name.clone());
                }
            }
        }
        
        // Fallback to standard OBIS mappings
        match obis_code {
            "1-0:1.8.0" => Some("total_energy_consumed".to_string()),
            "1-0:2.8.0" => Some("total_energy_delivered".to_string()),
            "1-0:16.7.0" => Some("current_power".to_string()),
            "1-0:32.7.0" => Some("voltage_l1".to_string()),
            "1-0:52.7.0" => Some("voltage_l2".to_string()),
            "1-0:72.7.0" => Some("voltage_l3".to_string()),
            "1-0:31.7.0" => Some("current_l1".to_string()),
            "1-0:51.7.0" => Some("current_l2".to_string()),
            "1-0:71.7.0" => Some("current_l3".to_string()),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sml_manager_creation() {
        let (tx, _rx) = tokio::sync::mpsc::channel(10);
        let manager = SmlManager::new(tx);
        assert!(!manager.device_definitions.is_empty());
    }

    #[test]
    fn test_meter_type_identification() {
        let (tx, _rx) = tokio::sync::mpsc::channel(10);
        let manager = SmlManager::new(tx);
        
        // Test EMH identification
        let emh_server_id = "EMH0123456789";
        let empty_list = Vec::new();
        let _meter_type = manager.identify_meter_type(emh_server_id, &empty_list);
        // This would depend on the actual meter definitions loaded
        
        // Test generic fallback
        let unknown_server_id = "UNKNOWN123";
        let meter_type = manager.identify_meter_type(unknown_server_id, &empty_list);
        assert_eq!(meter_type, MeterType::Generic);
    }
}