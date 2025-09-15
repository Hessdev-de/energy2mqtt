use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct ObisData {
    pub code: String,
    pub value: String,
    pub unit: Option<String>,
}

pub fn get_obis_description(obis_code: &str) -> Option<&'static str> {
    let descriptions = get_standard_obis_descriptions();
    descriptions.get(obis_code).copied()
}

pub fn get_standard_obis_descriptions() -> HashMap<&'static str, &'static str> {
    let mut map = HashMap::new();
    
    // Energy values
    map.insert("1-0:1.8.0", "Active energy + (total)");
    map.insert("1-0:1.8.1", "Active energy + (tariff 1)");
    map.insert("1-0:1.8.2", "Active energy + (tariff 2)");
    map.insert("1-0:2.8.0", "Active energy - (total)");
    map.insert("1-0:2.8.1", "Active energy - (tariff 1)");
    map.insert("1-0:2.8.2", "Active energy - (tariff 2)");
    map.insert("1-0:15.8.0", "Absolute active energy total");
    
    // Power values
    map.insert("1-0:1.7.0", "Active power + (total)");
    map.insert("1-0:2.7.0", "Active power - (total)");
    map.insert("1-0:15.7.0", "Absolute active instantaneous power");
    map.insert("1-0:21.7.0", "Active power + (L1)");
    map.insert("1-0:41.7.0", "Active power + (L2)");
    map.insert("1-0:61.7.0", "Active power + (L3)");
    
    // Voltage values
    map.insert("1-0:32.7.0", "Voltage (L1)");
    map.insert("1-0:52.7.0", "Voltage (L2)");
    map.insert("1-0:72.7.0", "Voltage (L3)");
    
    // Current values
    map.insert("1-0:31.7.0", "Current (L1)");
    map.insert("1-0:51.7.0", "Current (L2)");
    map.insert("1-0:71.7.0", "Current (L3)");
    
    // Reactive energy
    map.insert("1-0:3.8.0", "Reactive energy + (total)");
    map.insert("1-0:4.8.0", "Reactive energy - (total)");
    
    // Reactive power  
    map.insert("1-0:3.7.0", "Reactive power + (total)");
    map.insert("1-0:4.7.0", "Reactive power - (total)");
    
    // Timestamp and identification
    map.insert("0-0:1.0.0", "Date and time");
    map.insert("0-0:0.0.0", "Device ID");
    map.insert("0-0:0.0.1", "Device ID 1");
    map.insert("0-0:0.2.0", "Firmware version");
    
    // Frequency
    map.insert("1-0:14.7.0", "Supply frequency");
    
    // EasyMeter specific OBIS codes
    map.insert("1-0:0.0.0", "Equipment identifier");
    map.insert("1-0:0.0.9", "Date and time");
    map.insert("1-0:32.32.0", "Number of voltage sags (L1)");
    map.insert("1-0:52.32.0", "Number of voltage sags (L2)");
    map.insert("1-0:72.32.0", "Number of voltage sags (L3)");
    
    // EBZ specific OBIS codes  
    map.insert("1-0:16.7.0", "Sum active instantaneous power");
    map.insert("1-0:36.7.0", "Sum reactive instantaneous power");
    map.insert("1-0:13.7.0", "Power factor");
    
    map
}

pub fn get_easymeter_obis_mapping() -> HashMap<&'static str, &'static str> {
    let mut map = HashMap::new();
    
    // EasyMeter Q3D specific mappings
    map.insert("1-0:1.8.0", "total_energy_consumed");
    map.insert("1-0:2.8.0", "total_energy_delivered");
    map.insert("1-0:15.7.0", "current_power");
    map.insert("1-0:32.7.0", "voltage_l1");
    map.insert("1-0:52.7.0", "voltage_l2");
    map.insert("1-0:72.7.0", "voltage_l3");
    map.insert("1-0:31.7.0", "current_l1");
    map.insert("1-0:51.7.0", "current_l2");
    map.insert("1-0:71.7.0", "current_l3");
    map.insert("0-0:1.0.0", "timestamp");
    
    map
}

pub fn get_ebz_obis_mapping() -> HashMap<&'static str, &'static str> {
    let mut map = HashMap::new();
    
    // EBZ DD3 specific mappings
    map.insert("1-0:1.8.1", "energy_consumed_t1");
    map.insert("1-0:1.8.2", "energy_consumed_t2");
    map.insert("1-0:2.8.1", "energy_delivered_t1");
    map.insert("1-0:2.8.2", "energy_delivered_t2");
    map.insert("1-0:16.7.0", "sum_active_power");
    map.insert("1-0:36.7.0", "sum_reactive_power");
    map.insert("1-0:21.7.0", "active_power_l1");
    map.insert("1-0:41.7.0", "active_power_l2");
    map.insert("1-0:61.7.0", "active_power_l3");
    map.insert("1-0:32.7.0", "voltage_l1");
    map.insert("1-0:52.7.0", "voltage_l2");
    map.insert("1-0:72.7.0", "voltage_l3");
    map.insert("1-0:31.7.0", "current_l1");
    map.insert("1-0:51.7.0", "current_l2");
    map.insert("1-0:71.7.0", "current_l3");
    map.insert("1-0:13.7.0", "power_factor");
    map.insert("1-0:14.7.0", "frequency");
    map.insert("0-0:1.0.0", "timestamp");
    
    map
}

pub fn validate_obis_code(code: &str) -> bool {
    // OBIS code format: A-B:C.D.E*F
    // A: Medium (0=abstract, 1=electricity, 6=heat, 7=gas, 8=water)
    // B: Channel (0-15)
    // C: Physical value (1-255) 
    // D: Processing method (0-255)
    // E: Tariff/Time (0-255)
    // F: Storage (optional, 0-255)
    
    let parts: Vec<&str> = code.split(':').collect();
    if parts.len() != 2 {
        return false;
    }
    
    // Check A-B part
    let ab_parts: Vec<&str> = parts[0].split('-').collect();
    if ab_parts.len() != 2 {
        return false;
    }
    
    // Check C.D.E part (and optional *F)
    let cde_part = parts[1];
    let cde_parts: Vec<&str> = if cde_part.contains('*') {
        cde_part.split('*').next().unwrap_or("").split('.').collect()
    } else {
        cde_part.split('.').collect()
    };
    
    if cde_parts.len() != 3 {
        return false;
    }
    
    // Validate that all parts are numeric
    for part in ab_parts.iter().chain(cde_parts.iter()) {
        if part.parse::<u8>().is_err() {
            return false;
        }
    }
    
    true
}

pub fn normalize_obis_code(code: &str) -> String {
    code.trim().to_string()
}

pub fn extract_unit(value_content: &str) -> Option<String> {
    if let Some(star_pos) = value_content.rfind('*') {
        let unit = &value_content[star_pos + 1..];
        if !unit.is_empty() {
            return Some(unit.to_string());
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

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

    #[test]
    fn test_extract_unit() {
        assert_eq!(extract_unit("123.456*kWh"), Some("kWh".to_string()));
        assert_eq!(extract_unit("1.234*kW"), Some("kW".to_string()));
        assert_eq!(extract_unit("123456"), None);
        assert_eq!(extract_unit("123*"), None);
    }

    #[test]
    fn test_normalize_obis_code() {
        assert_eq!(normalize_obis_code("  1-0:1.8.1  "), "1-0:1.8.1");
        assert_eq!(normalize_obis_code("1-0:15.7.0"), "1-0:15.7.0");
    }
}