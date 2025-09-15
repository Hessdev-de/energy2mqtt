use super::structs::*;
use log::debug;
use chrono;

pub fn format_obis_code(obis_bytes: &[u8]) -> String {
    if obis_bytes.len() == 6 {
        if let Some(obis) = SmlObisCode::from_bytes(obis_bytes) {
            return obis.to_string();
        }
    }
    
    // Fallback: format as hex
    hex::encode(obis_bytes)
}

pub fn parse_sml_value(value: &SmlValue) -> (String, Option<String>) {
    match value {
        SmlValue::Bool(b) => (b.to_string(), None),
        SmlValue::Int8(i) => (i.to_string(), None),
        SmlValue::Int16(i) => (i.to_string(), None),
        SmlValue::Int32(i) => (i.to_string(), None),
        SmlValue::Int64(i) => (i.to_string(), None),
        SmlValue::UInt8(u) => (u.to_string(), None),
        SmlValue::UInt16(u) => (u.to_string(), None),
        SmlValue::UInt32(u) => (u.to_string(), None),
        SmlValue::UInt64(u) => (u.to_string(), None),
        SmlValue::OctetString(bytes) => {
            // Try to decode as UTF-8 string first
            if let Ok(string) = String::from_utf8(bytes.clone()) {
                if string.chars().all(|c| c.is_ascii_graphic() || c.is_whitespace()) {
                    return (string, None);
                }
            }
            // Otherwise format as hex
            (hex::encode(bytes), None)
        },
        SmlValue::List(values) => {
            let formatted: Vec<String> = values.iter()
                .map(|v| parse_sml_value(v).0)
                .collect();
            (format!("[{}]", formatted.join(", ")), None)
        }
    }
}

pub fn apply_scaler_and_unit(value_str: &str, scaler: Option<i8>, unit: Option<u8>) -> (String, Option<String>) {
    // Parse the numeric value
    if let Ok(mut value) = value_str.parse::<f64>() {
        // Apply scaler if present
        if let Some(s) = scaler {
            let scale_factor = 10_f64.powi(s as i32);
            value *= scale_factor;
        }
        
        // Format with appropriate precision
        let formatted_value = if value.fract() == 0.0 && value.abs() < 1e15 {
            format!("{:.0}", value)
        } else {
            format!("{:.6}", value).trim_end_matches('0').trim_end_matches('.').to_string()
        };
        
        // Get unit name
        let unit_name = unit.and_then(|u| get_sml_unit_name(u).map(|s| s.to_string()));
        
        (formatted_value, unit_name)
    } else {
        // If not numeric, return as-is
        let unit_name = unit.and_then(|u| get_sml_unit_name(u).map(|s| s.to_string()));
        (value_str.to_string(), unit_name)
    }
}

pub fn extract_server_id_info(server_id: &[u8]) -> ServerIdInfo {
    let hex_id = hex::encode(server_id);
    
    // Try to identify manufacturer based on server ID patterns
    let manufacturer = identify_manufacturer_from_server_id(&hex_id);
    
    ServerIdInfo {
        hex_id: hex_id.clone(),
        manufacturer,
        raw_bytes: server_id.to_vec(),
    }
}

pub fn identify_manufacturer_from_server_id(server_id: &str) -> String {
    let id_upper = server_id.to_uppercase();
    
    // Common manufacturer patterns in SML server IDs
    if id_upper.starts_with("EMH") || id_upper.contains("EMH") {
        return "EMH".to_string();
    }
    
    if id_upper.starts_with("ISK") || id_upper.contains("ISK") {
        return "Iskraemeco".to_string();
    }
    
    if id_upper.starts_with("EAS") || id_upper.contains("EAS") {
        return "EasyMeter".to_string();
    }
    
    if id_upper.starts_with("ITR") || id_upper.contains("ITR") {
        return "Itron".to_string();
    }
    
    // Check for specific patterns
    if server_id.len() >= 10 {
        if id_upper.starts_with("1E") {
            return "EMH".to_string();
        }
        if id_upper.starts_with("1I") {
            return "Iskraemeco".to_string();
        }
        if id_upper.starts_with("1S") {
            return "Siemens".to_string();
        }
        if id_upper.starts_with("1L") {
            return "Landis+Gyr".to_string();
        }
    }
    
    "Unknown".to_string()
}

#[derive(Debug, Clone)]
pub struct ServerIdInfo {
    pub hex_id: String,
    pub manufacturer: String,
    pub raw_bytes: Vec<u8>,
}

pub fn validate_sml_checksum(data: &[u8]) -> bool {
    if data.len() < 4 {
        return false;
    }
    
    // Find the checksum position (typically last 2 bytes before end sequence)
    let end_pos = data.len() - 4; // Account for end sequence
    if end_pos < 2 {
        return false;
    }
    
    let checksum_pos = end_pos - 2;
    let expected_crc = u16::from_be_bytes([data[checksum_pos], data[checksum_pos + 1]]);
    
    // Calculate CRC16 over the data (excluding checksum and end sequence)
    let calculated_crc = calculate_crc16(&data[0..checksum_pos]);
    
    debug!("SML checksum validation: expected=0x{:04X}, calculated=0x{:04X}", 
           expected_crc, calculated_crc);
    
    expected_crc == calculated_crc
}

fn calculate_crc16(data: &[u8]) -> u16 {
    // CRC16-CCITT implementation (polynomial 0x1021)
    let mut crc: u16 = 0xFFFF;
    
    for &byte in data {
        crc ^= (byte as u16) << 8;
        for _ in 0..8 {
            if crc & 0x8000 != 0 {
                crc = (crc << 1) ^ 0x1021;
            } else {
                crc <<= 1;
            }
        }
    }
    
    crc
}

pub fn format_timestamp(timestamp: Option<u32>) -> Option<String> {
    timestamp.map(|ts| {
        // SML timestamps are typically seconds since epoch
        let datetime = chrono::DateTime::from_timestamp(ts as i64, 0);
        datetime.map(|dt| dt.format("%Y-%m-%d %H:%M:%S UTC").to_string())
               .unwrap_or_else(|| format!("Invalid timestamp: {}", ts))
    })
}

pub fn get_common_sml_obis_mappings() -> std::collections::HashMap<String, String> {
    let mut map = std::collections::HashMap::new();
    
    // Standard electricity OBIS codes commonly found in SML
    map.insert("1-0:1.8.0".to_string(), "total_energy_consumed".to_string());
    map.insert("1-0:1.8.1".to_string(), "energy_consumed_t1".to_string());
    map.insert("1-0:1.8.2".to_string(), "energy_consumed_t2".to_string());
    map.insert("1-0:2.8.0".to_string(), "total_energy_delivered".to_string());
    map.insert("1-0:2.8.1".to_string(), "energy_delivered_t1".to_string());
    map.insert("1-0:2.8.2".to_string(), "energy_delivered_t2".to_string());
    
    // Power values
    map.insert("1-0:16.7.0".to_string(), "current_power".to_string());
    map.insert("1-0:36.7.0".to_string(), "reactive_power".to_string());
    map.insert("1-0:21.7.0".to_string(), "active_power_l1".to_string());
    map.insert("1-0:41.7.0".to_string(), "active_power_l2".to_string());
    map.insert("1-0:61.7.0".to_string(), "active_power_l3".to_string());
    
    // Voltage values
    map.insert("1-0:32.7.0".to_string(), "voltage_l1".to_string());
    map.insert("1-0:52.7.0".to_string(), "voltage_l2".to_string());
    map.insert("1-0:72.7.0".to_string(), "voltage_l3".to_string());
    
    // Current values
    map.insert("1-0:31.7.0".to_string(), "current_l1".to_string());
    map.insert("1-0:51.7.0".to_string(), "current_l2".to_string());
    map.insert("1-0:71.7.0".to_string(), "current_l3".to_string());
    
    // Frequency and other
    map.insert("1-0:14.7.0".to_string(), "frequency".to_string());
    map.insert("1-0:13.7.0".to_string(), "power_factor".to_string());
    
    // Device identification
    map.insert("129-129:199.130.3".to_string(), "manufacturer".to_string());
    map.insert("1-0:0.0.0".to_string(), "device_id".to_string());
    map.insert("1-0:0.0.9".to_string(), "timestamp".to_string());
    
    map
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_obis_code() {
        let obis_bytes = [0x01, 0x00, 0x01, 0x08, 0x00, 0xFF];
        let formatted = format_obis_code(&obis_bytes);
        assert_eq!(formatted, "1-0:1.8.0.255");
    }

    #[test]
    fn test_parse_sml_value() {
        let value = SmlValue::UInt32(12345);
        let (value_str, unit) = parse_sml_value(&value);
        assert_eq!(value_str, "12345");
        assert_eq!(unit, None);
    }

    #[test]
    fn test_apply_scaler_and_unit() {
        let (result, unit) = apply_scaler_and_unit("12345", Some(-2), Some(30)); // 30 = Watt
        assert_eq!(result, "123.45");
        assert_eq!(unit, Some("W".to_string()));
    }

    #[test]
    fn test_identify_manufacturer() {
        assert_eq!(identify_manufacturer_from_server_id("EMH12345"), "EMH");
        assert_eq!(identify_manufacturer_from_server_id("ISK67890"), "Iskraemeco");
        assert_eq!(identify_manufacturer_from_server_id("1E2D3F4A567890"), "EMH"); // Should match prefix "1E"
        assert_eq!(identify_manufacturer_from_server_id("UNKNOWN"), "Unknown");
    }

    #[test]
    fn test_crc16_calculation() {
        let data = [0x1B, 0x1B, 0x1B, 0x1B, 0x01, 0x01, 0x01, 0x01];
        let crc = calculate_crc16(&data);
        // This should produce a specific CRC16 value
        assert_ne!(crc, 0);
    }
}