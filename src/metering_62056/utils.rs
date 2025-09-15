use super::{structs::DeviceIdentification, Iec62056ParseError};
use log::debug;

pub fn parse_identification_line(line: &str) -> Result<DeviceIdentification, Iec62056ParseError> {
    // Example formats:
    // /ELS5\@V5.3
    // /ISK5MT382-1000
    // /EBZ5DD3BL10-112 (EBZ meter example)
    
    if !line.starts_with('/') {
        return Err(Iec62056ParseError::MissingIdentification);
    }

    let content = &line[1..]; // Remove leading '/'
    
    // Parse manufacturer (first 3 characters typically)
    let manufacturer = if content.len() >= 3 {
        content[..3].to_string()
    } else {
        return Err(Iec62056ParseError::InvalidFormat);
    };

    // The rest is identification/model info
    let identification = content.to_string();
    
    // Determine protocol mode from identification string
    let mode = determine_protocol_mode(&identification);

    debug!("Parsed identification - Manufacturer: {}, ID: {}, Mode: {}", 
           manufacturer, identification, mode);

    Ok(DeviceIdentification {
        manufacturer: manufacturer.clone(),
        identification: identification.clone(),
        mode,
        full_id: format!("{}{}", manufacturer, identification),
    })
}

fn determine_protocol_mode(identification: &str) -> String {
    // Mode determination logic based on identification string patterns
    // This can be enhanced based on specific meter requirements
    
    if identification.contains("@") {
        "C".to_string() // Mode C typical for bidirectional communication
    } else if identification.len() > 10 {
        "D".to_string() // Mode D for push telegrams
    } else {
        "A".to_string() // Default to Mode A
    }
}

pub fn calculate_checksum(data: &str) -> String {
    // Calculate CRC16 checksum for IEC 62056-21
    // This is a simplified implementation - real CRC16 calculation should be used
    let mut checksum: u16 = 0;
    
    for byte in data.bytes() {
        checksum = checksum.wrapping_add(byte as u16);
    }
    
    format!("{:04X}", checksum & 0xFFFF)
}

pub fn verify_checksum(telegram: &str, provided_checksum: &str) -> bool {
    let calculated = calculate_checksum(telegram);
    calculated == provided_checksum
}

pub fn get_meter_type_from_manufacturer(manufacturer: &str) -> super::structs::MeterType {
    match manufacturer.to_uppercase().as_str() {
        "ESY" | "EAS" => super::structs::MeterType::EasyMeter,
        "EBZ" => super::structs::MeterType::EBZ,
        _ => super::structs::MeterType::Generic,
    }
}

pub fn extract_numeric_value(value_str: &str) -> Option<f64> {
    // Extract numeric value from strings like "000123.456" or "123.456*kWh"
    let cleaned = value_str
        .chars()
        .take_while(|c| c.is_ascii_digit() || *c == '.' || *c == '-' || *c == '+')
        .collect::<String>();
    
    cleaned.parse::<f64>().ok()
}

pub fn extract_unit(value_str: &str) -> Option<String> {
    // Extract unit from strings like "123.456*kWh"
    if let Some(star_pos) = value_str.find('*') {
        let unit = &value_str[star_pos + 1..];
        // Remove any trailing characters that are not part of the unit
        let unit = unit.chars()
            .take_while(|c| c.is_alphabetic() || *c == '/')
            .collect::<String>();
        if !unit.is_empty() {
            Some(unit)
        } else {
            None
        }
    } else {
        None
    }
}

pub fn normalize_obis_code(code: &str) -> String {
    // Normalize OBIS codes to standard format
    // Remove any whitespace and ensure proper formatting
    code.trim().replace(" ", "")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_identification_line() {
        let line = "/ELS5\\@V5.3";
        let result = parse_identification_line(line);
        assert!(result.is_ok());
        let device_info = result.unwrap();
        assert_eq!(device_info.manufacturer, "ELS");
        assert_eq!(device_info.mode, "C");
    }

    #[test]
    fn test_extract_numeric_value() {
        assert_eq!(extract_numeric_value("000123.456"), Some(123.456));
        assert_eq!(extract_numeric_value("123.456*kWh"), Some(123.456));
        assert_eq!(extract_numeric_value("-12.34"), Some(-12.34));
    }

    #[test]
    fn test_extract_unit() {
        assert_eq!(extract_unit("123.456*kWh"), Some("kWh".to_string()));
        assert_eq!(extract_unit("123.456*V"), Some("V".to_string()));
        assert_eq!(extract_unit("123.456"), None);
    }

    #[test]
    fn test_normalize_obis_code() {
        assert_eq!(normalize_obis_code("1-0:1.8.1"), "1-0:1.8.1");
        assert_eq!(normalize_obis_code(" 1-0:1.8.1 "), "1-0:1.8.1");
    }
}