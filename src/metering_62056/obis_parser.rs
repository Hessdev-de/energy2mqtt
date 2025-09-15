use super::Iec62056ParseError;
use crate::obis_utils::{self, ObisData};
use log::debug;

pub fn parse_obis_line(line: &str) -> Result<ObisData, Iec62056ParseError> {
    // Example formats:
    // 1-0:1.8.1(000123.456*kWh)
    // 1-0:15.7.0(001.234*kW)
    // 0-0:1.0.0(210101120000W)
    
    let line = line.trim();
    
    // Find the opening parenthesis
    let paren_start = line.find('(')
        .ok_or(Iec62056ParseError::InvalidDataLine)?;
    
    // Find the closing parenthesis
    let paren_end = line.rfind(')')
        .ok_or(Iec62056ParseError::InvalidDataLine)?;
    
    if paren_start >= paren_end {
        return Err(Iec62056ParseError::InvalidDataLine);
    }
    
    // Extract OBIS code (before opening parenthesis)
    let obis_code = obis_utils::normalize_obis_code(&line[..paren_start]);
    
    // Extract value content (between parentheses)
    let value_content = &line[paren_start + 1..paren_end];
    
    // Parse value and unit
    let unit = obis_utils::extract_unit(value_content);
    let value = value_content.to_string();
    
    debug!("Parsed OBIS line - Code: {}, Value: {}, Unit: {:?}", 
           obis_code, value, unit);
    
    Ok(ObisData {
        code: obis_code,
        value,
        unit,
    })
}

pub fn get_obis_description(obis_code: &str) -> Option<&'static str> {
    obis_utils::get_obis_description(obis_code)
}

pub fn get_easymeter_obis_mapping() -> std::collections::HashMap<&'static str, &'static str> {
    obis_utils::get_easymeter_obis_mapping()
}

pub fn get_ebz_obis_mapping() -> std::collections::HashMap<&'static str, &'static str> {
    obis_utils::get_ebz_obis_mapping()
}

pub fn validate_obis_code(code: &str) -> bool {
    obis_utils::validate_obis_code(code)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_obis_line() {
        let line = "1-0:1.8.1(000123.456*kWh)";
        let result = parse_obis_line(line);
        assert!(result.is_ok());
        let obis_data = result.unwrap();
        assert_eq!(obis_data.code, "1-0:1.8.1");
        assert_eq!(obis_data.value, "000123.456*kWh");
        assert_eq!(obis_data.unit, Some("kWh".to_string()));
    }

    #[test]
    fn test_validate_obis_code() {
        assert!(validate_obis_code("1-0:1.8.1"));
        assert!(validate_obis_code("0-0:1.0.0"));
        assert!(validate_obis_code("1-0:15.7.0"));
        assert!(!validate_obis_code("invalid"));
        assert!(!validate_obis_code("1:2.3.4"));
        assert!(!validate_obis_code("1-0:1.8"));
    }

    #[test]
    fn test_get_obis_description() {
        assert_eq!(get_obis_description("1-0:1.8.1"), Some("Active energy + (tariff 1)"));
        assert_eq!(get_obis_description("1-0:15.7.0"), Some("Absolute active instantaneous power"));
        assert_eq!(get_obis_description("nonexistent"), None);
    }
}