use super::structs::{MeterDefinition, MeterType};
use std::collections::HashMap;

pub fn get_supported_meters() -> HashMap<String, MeterDefinition> {
    let mut meters = HashMap::new();
    
    // EMH ED300L - Popular smart meter with SML support
    meters.insert("EMH".to_string(), MeterDefinition {
        meter_type: MeterType::EMH,
        manufacturer_codes: vec!["EMH".to_string(), "1E".to_string()],
        supported_obis_codes: vec![
            "1-0:1.8.0".to_string(),   // Active energy + total
            "1-0:2.8.0".to_string(),   // Active energy - total
            "1-0:16.7.0".to_string(),  // Active power total
            "1-0:32.7.0".to_string(),  // Voltage L1
            "1-0:52.7.0".to_string(),  // Voltage L2
            "1-0:72.7.0".to_string(),  // Voltage L3
            "1-0:31.7.0".to_string(),  // Current L1
            "1-0:51.7.0".to_string(),  // Current L2
            "1-0:71.7.0".to_string(),  // Current L3
            "1-0:14.7.0".to_string(),  // Frequency
            "129-129:199.130.3".to_string(), // Manufacturer
            "1-0:0.0.0".to_string(),   // Device ID
            "1-0:0.0.9".to_string(),   // Timestamp
        ],
        obis_mapping: get_emh_obis_mapping(),
        description: "EMH ED300L Smart Meter".to_string(),
    });
    
    // Iskraemeco MT175/MT631
    meters.insert("Iskraemeco".to_string(), MeterDefinition {
        meter_type: MeterType::Iskraemeco,
        manufacturer_codes: vec!["ISK".to_string(), "1I".to_string()],
        supported_obis_codes: vec![
            "1-0:1.8.0".to_string(),   // Active energy + total
            "1-0:1.8.1".to_string(),   // Active energy + tariff 1
            "1-0:1.8.2".to_string(),   // Active energy + tariff 2
            "1-0:2.8.0".to_string(),   // Active energy - total
            "1-0:2.8.1".to_string(),   // Active energy - tariff 1
            "1-0:2.8.2".to_string(),   // Active energy - tariff 2
            "1-0:16.7.0".to_string(),  // Active power total
            "1-0:36.7.0".to_string(),  // Reactive power total
            "1-0:32.7.0".to_string(),  // Voltage L1
            "1-0:52.7.0".to_string(),  // Voltage L2
            "1-0:72.7.0".to_string(),  // Voltage L3
            "1-0:31.7.0".to_string(),  // Current L1
            "1-0:51.7.0".to_string(),  // Current L2
            "1-0:71.7.0".to_string(),  // Current L3
            "1-0:13.7.0".to_string(),  // Power factor
            "1-0:14.7.0".to_string(),  // Frequency
            "1-0:0.0.0".to_string(),   // Device ID
            "1-0:0.0.9".to_string(),   // Timestamp
        ],
        obis_mapping: get_iskraemeco_obis_mapping(),
        description: "Iskraemeco MT175/MT631 Smart Meter".to_string(),
    });
    
    // Itron OpenWay 3.HZ
    meters.insert("Itron".to_string(), MeterDefinition {
        meter_type: MeterType::Itron,
        manufacturer_codes: vec!["ITR".to_string(), "ITO".to_string()],
        supported_obis_codes: vec![
            "1-0:1.8.0".to_string(),   // Active energy + total
            "1-0:2.8.0".to_string(),   // Active energy - total
            "1-0:3.8.0".to_string(),   // Reactive energy + total
            "1-0:4.8.0".to_string(),   // Reactive energy - total
            "1-0:9.8.0".to_string(),   // Apparent energy + total
            "1-0:10.8.0".to_string(),  // Apparent energy - total
            "1-0:16.7.0".to_string(),  // Active power total
            "1-0:36.7.0".to_string(),  // Reactive power total
            "1-0:21.7.0".to_string(),  // Active power L1
            "1-0:41.7.0".to_string(),  // Active power L2
            "1-0:61.7.0".to_string(),  // Active power L3
            "1-0:32.7.0".to_string(),  // Voltage L1
            "1-0:52.7.0".to_string(),  // Voltage L2
            "1-0:72.7.0".to_string(),  // Voltage L3
            "1-0:31.7.0".to_string(),  // Current L1
            "1-0:51.7.0".to_string(),  // Current L2
            "1-0:71.7.0".to_string(),  // Current L3
            "1-0:14.7.0".to_string(),  // Frequency
            "1-0:0.0.0".to_string(),   // Device ID
        ],
        obis_mapping: get_itron_obis_mapping(),
        description: "Itron OpenWay 3.HZ Smart Meter".to_string(),
    });
    
    // EasyMeter (if they support SML - some newer models do)
    meters.insert("EasyMeter".to_string(), MeterDefinition {
        meter_type: MeterType::EasyMeter,
        manufacturer_codes: vec!["EAS".to_string(), "ESY".to_string()],
        supported_obis_codes: vec![
            "1-0:1.8.0".to_string(),   // Active energy + total
            "1-0:2.8.0".to_string(),   // Active energy - total
            "1-0:16.7.0".to_string(),  // Active power total
            "1-0:32.7.0".to_string(),  // Voltage L1
            "1-0:52.7.0".to_string(),  // Voltage L2
            "1-0:72.7.0".to_string(),  // Voltage L3
            "1-0:31.7.0".to_string(),  // Current L1
            "1-0:51.7.0".to_string(),  // Current L2
            "1-0:71.7.0".to_string(),  // Current L3
            "1-0:14.7.0".to_string(),  // Frequency
            "1-0:0.0.0".to_string(),   // Equipment identifier
            "1-0:0.0.9".to_string(),   // Date and time
        ],
        obis_mapping: get_easymeter_obis_mapping(),
        description: "EasyMeter Smart Meter (SML variant)".to_string(),
    });
    
    // Generic SML meter for unknown devices
    meters.insert("Generic".to_string(), MeterDefinition {
        meter_type: MeterType::Generic,
        manufacturer_codes: vec!["UNK".to_string(), "GEN".to_string()],
        supported_obis_codes: vec![
            "1-0:1.8.0".to_string(),   // Basic energy readings
            "1-0:2.8.0".to_string(),
            "1-0:16.7.0".to_string(),  // Basic power reading
            "1-0:0.0.0".to_string(),   // Device identification
        ],
        obis_mapping: get_generic_obis_mapping(),
        description: "Generic SML Smart Meter".to_string(),
    });
    
    meters
}

fn get_emh_obis_mapping() -> HashMap<String, String> {
    let mut map = HashMap::new();
    
    // Energy values
    map.insert("1-0:1.8.0".to_string(), "total_energy_consumed".to_string());
    map.insert("1-0:2.8.0".to_string(), "total_energy_delivered".to_string());
    
    // Power values
    map.insert("1-0:16.7.0".to_string(), "current_power".to_string());
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
    
    // Other measurements
    map.insert("1-0:14.7.0".to_string(), "frequency".to_string());
    
    // Device info
    map.insert("129-129:199.130.3".to_string(), "manufacturer".to_string());
    map.insert("1-0:0.0.0".to_string(), "device_id".to_string());
    map.insert("1-0:0.0.9".to_string(), "timestamp".to_string());
    
    map
}

fn get_iskraemeco_obis_mapping() -> HashMap<String, String> {
    let mut map = HashMap::new();
    
    // Energy values with tariffs
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
    
    // Power quality
    map.insert("1-0:13.7.0".to_string(), "power_factor".to_string());
    map.insert("1-0:14.7.0".to_string(), "frequency".to_string());
    
    // Device info
    map.insert("1-0:0.0.0".to_string(), "device_id".to_string());
    map.insert("1-0:0.0.9".to_string(), "timestamp".to_string());
    
    map
}

fn get_itron_obis_mapping() -> HashMap<String, String> {
    let mut map = HashMap::new();
    
    // Energy values (active, reactive, apparent)
    map.insert("1-0:1.8.0".to_string(), "total_energy_consumed".to_string());
    map.insert("1-0:2.8.0".to_string(), "total_energy_delivered".to_string());
    map.insert("1-0:3.8.0".to_string(), "reactive_energy_consumed".to_string());
    map.insert("1-0:4.8.0".to_string(), "reactive_energy_delivered".to_string());
    map.insert("1-0:9.8.0".to_string(), "apparent_energy_consumed".to_string());
    map.insert("1-0:10.8.0".to_string(), "apparent_energy_delivered".to_string());
    
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
    
    // Power quality
    map.insert("1-0:14.7.0".to_string(), "frequency".to_string());
    
    // Device info
    map.insert("1-0:0.0.0".to_string(), "device_id".to_string());
    
    map
}

fn get_easymeter_obis_mapping() -> HashMap<String, String> {
    let mut map = HashMap::new();
    
    // Energy values
    map.insert("1-0:1.8.0".to_string(), "total_energy_consumed".to_string());
    map.insert("1-0:2.8.0".to_string(), "total_energy_delivered".to_string());
    
    // Power values
    map.insert("1-0:16.7.0".to_string(), "current_power".to_string());
    
    // Voltage values
    map.insert("1-0:32.7.0".to_string(), "voltage_l1".to_string());
    map.insert("1-0:52.7.0".to_string(), "voltage_l2".to_string());
    map.insert("1-0:72.7.0".to_string(), "voltage_l3".to_string());
    
    // Current values
    map.insert("1-0:31.7.0".to_string(), "current_l1".to_string());
    map.insert("1-0:51.7.0".to_string(), "current_l2".to_string());
    map.insert("1-0:71.7.0".to_string(), "current_l3".to_string());
    
    // Power quality
    map.insert("1-0:14.7.0".to_string(), "frequency".to_string());
    
    // Device info
    map.insert("1-0:0.0.0".to_string(), "device_id".to_string());
    map.insert("1-0:0.0.9".to_string(), "timestamp".to_string());
    
    map
}

fn get_generic_obis_mapping() -> HashMap<String, String> {
    let mut map = HashMap::new();
    
    // Basic energy and power readings that most meters support
    map.insert("1-0:1.8.0".to_string(), "total_energy_consumed".to_string());
    map.insert("1-0:2.8.0".to_string(), "total_energy_delivered".to_string());
    map.insert("1-0:16.7.0".to_string(), "current_power".to_string());
    map.insert("1-0:0.0.0".to_string(), "device_id".to_string());
    
    map
}

// Helper function to get meter by manufacturer code
pub fn get_meter_by_manufacturer(manufacturer_code: &str) -> Option<MeterDefinition> {
    let meters = get_supported_meters();
    
    for meter_def in meters.values() {
        if meter_def.manufacturer_codes.iter().any(|code| code == manufacturer_code) {
            return Some(meter_def.clone());
        }
    }
    
    None
}

// Helper function to get all supported OBIS codes across all meters
pub fn get_all_supported_obis_codes() -> Vec<String> {
    let meters = get_supported_meters();
    let mut all_codes = std::collections::HashSet::new();
    
    for meter_def in meters.values() {
        for code in &meter_def.supported_obis_codes {
            all_codes.insert(code.clone());
        }
    }
    
    all_codes.into_iter().collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_supported_meters() {
        let meters = get_supported_meters();
        assert!(!meters.is_empty());
        assert!(meters.contains_key("EMH"));
        assert!(meters.contains_key("Iskraemeco"));
        assert!(meters.contains_key("Generic"));
    }

    #[test]
    fn test_meter_definitions() {
        let meters = get_supported_meters();
        let emh = meters.get("EMH").unwrap();
        
        assert_eq!(emh.meter_type, MeterType::EMH);
        assert!(!emh.supported_obis_codes.is_empty());
        assert!(!emh.obis_mapping.is_empty());
        assert!(emh.obis_mapping.contains_key("1-0:1.8.0"));
    }

    #[test]
    fn test_get_meter_by_manufacturer() {
        let meter = get_meter_by_manufacturer("EMH");
        assert!(meter.is_some());
        assert_eq!(meter.unwrap().meter_type, MeterType::EMH);
        
        let unknown = get_meter_by_manufacturer("UNKNOWN");
        assert!(unknown.is_none());
    }

    #[test]
    fn test_get_all_supported_obis_codes() {
        let codes = get_all_supported_obis_codes();
        assert!(!codes.is_empty());
        assert!(codes.contains(&"1-0:1.8.0".to_string()));
        assert!(codes.contains(&"1-0:16.7.0".to_string()));
    }
}