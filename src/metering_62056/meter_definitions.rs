use super::{structs::{MeterDefinition, MeterType}, ProtocolMode};
use std::collections::HashMap;

pub fn get_easymeter_definition() -> MeterDefinition {
    let mut obis_mapping = HashMap::new();
    
    // EasyMeter Q3D OBIS code mappings
    obis_mapping.insert("1-0:1.8.0".to_string(), "total_energy_consumed".to_string());
    obis_mapping.insert("1-0:2.8.0".to_string(), "total_energy_delivered".to_string());
    obis_mapping.insert("1-0:1.8.1".to_string(), "energy_consumed_t1".to_string());
    obis_mapping.insert("1-0:1.8.2".to_string(), "energy_consumed_t2".to_string());
    obis_mapping.insert("1-0:2.8.1".to_string(), "energy_delivered_t1".to_string());
    obis_mapping.insert("1-0:2.8.2".to_string(), "energy_delivered_t2".to_string());
    obis_mapping.insert("1-0:15.7.0".to_string(), "current_power".to_string());
    obis_mapping.insert("1-0:32.7.0".to_string(), "voltage_l1".to_string());
    obis_mapping.insert("1-0:52.7.0".to_string(), "voltage_l2".to_string());
    obis_mapping.insert("1-0:72.7.0".to_string(), "voltage_l3".to_string());
    obis_mapping.insert("1-0:31.7.0".to_string(), "current_l1".to_string());
    obis_mapping.insert("1-0:51.7.0".to_string(), "current_l2".to_string());
    obis_mapping.insert("1-0:71.7.0".to_string(), "current_l3".to_string());
    obis_mapping.insert("1-0:14.7.0".to_string(), "frequency".to_string());
    obis_mapping.insert("0-0:1.0.0".to_string(), "timestamp".to_string());
    obis_mapping.insert("1-0:0.0.0".to_string(), "equipment_identifier".to_string());
    
    // EasyMeter specific codes
    obis_mapping.insert("1-0:32.32.0".to_string(), "voltage_sags_l1".to_string());
    obis_mapping.insert("1-0:52.32.0".to_string(), "voltage_sags_l2".to_string());
    obis_mapping.insert("1-0:72.32.0".to_string(), "voltage_sags_l3".to_string());

    MeterDefinition {
        meter_type: MeterType::EasyMeter,
        manufacturer_code: "ESY".to_string(),
        supported_modes: vec![ProtocolMode::ModeC, ProtocolMode::ModeD],
        default_baud_rate: 9600,
        obis_mapping,
    }
}

pub fn get_ebz_definition() -> MeterDefinition {
    let mut obis_mapping = HashMap::new();
    
    // EBZ DD3 OBIS code mappings
    obis_mapping.insert("1-0:1.8.1".to_string(), "energy_consumed_t1".to_string());
    obis_mapping.insert("1-0:1.8.2".to_string(), "energy_consumed_t2".to_string());
    obis_mapping.insert("1-0:2.8.1".to_string(), "energy_delivered_t1".to_string());
    obis_mapping.insert("1-0:2.8.2".to_string(), "energy_delivered_t2".to_string());
    obis_mapping.insert("1-0:15.8.0".to_string(), "absolute_energy_total".to_string());
    
    // Power measurements
    obis_mapping.insert("1-0:16.7.0".to_string(), "sum_active_power".to_string());
    obis_mapping.insert("1-0:36.7.0".to_string(), "sum_reactive_power".to_string());
    obis_mapping.insert("1-0:21.7.0".to_string(), "active_power_l1".to_string());
    obis_mapping.insert("1-0:41.7.0".to_string(), "active_power_l2".to_string());
    obis_mapping.insert("1-0:61.7.0".to_string(), "active_power_l3".to_string());
    
    // Voltage measurements
    obis_mapping.insert("1-0:32.7.0".to_string(), "voltage_l1".to_string());
    obis_mapping.insert("1-0:52.7.0".to_string(), "voltage_l2".to_string());
    obis_mapping.insert("1-0:72.7.0".to_string(), "voltage_l3".to_string());
    
    // Current measurements
    obis_mapping.insert("1-0:31.7.0".to_string(), "current_l1".to_string());
    obis_mapping.insert("1-0:51.7.0".to_string(), "current_l2".to_string());
    obis_mapping.insert("1-0:71.7.0".to_string(), "current_l3".to_string());
    
    // Power factor and frequency
    obis_mapping.insert("1-0:13.7.0".to_string(), "power_factor".to_string());
    obis_mapping.insert("1-0:14.7.0".to_string(), "frequency".to_string());
    
    // Timestamp and identification
    obis_mapping.insert("0-0:1.0.0".to_string(), "timestamp".to_string());
    obis_mapping.insert("0-0:0.0.0".to_string(), "device_id".to_string());

    MeterDefinition {
        meter_type: MeterType::EBZ,
        manufacturer_code: "EBZ".to_string(),
        supported_modes: vec![ProtocolMode::ModeC, ProtocolMode::ModeD],
        default_baud_rate: 9600,
        obis_mapping,
    }
}

pub fn get_meter_definition_by_manufacturer(manufacturer: &str) -> Option<MeterDefinition> {
    match manufacturer.to_uppercase().as_str() {
        "ESY" | "EAS" => Some(get_easymeter_definition()),
        "EBZ" => Some(get_ebz_definition()),
        _ => None,
    }
}

pub fn create_example_telegrams() -> HashMap<String, String> {
    let mut examples = HashMap::new();
    
    // EasyMeter Q3D example telegram (Mode D)
    let easymeter_telegram = r"/ESY5Q3D\@V5.3
0-0:1.0.0(210101120000W)
1-0:1.8.0(000123.456*kWh)
1-0:2.8.0(000012.345*kWh)
1-0:15.7.0(001.234*kW)
1-0:32.7.0(230.5*V)
1-0:52.7.0(231.2*V)
1-0:72.7.0(229.8*V)
1-0:31.7.0(05.34*A)
1-0:51.7.0(04.89*A)
1-0:71.7.0(05.12*A)
1-0:14.7.0(50.0*Hz)
!";
    examples.insert("EasyMeter_Q3D".to_string(), easymeter_telegram.to_string());

    // EBZ DD3 example telegram (Mode C)
    let ebz_telegram = r"/EBZ5DD3BL10-112
0-0:1.0.0(210101120000W)
1-0:1.8.1(000234.567*kWh)
1-0:1.8.2(000123.456*kWh)
1-0:2.8.1(000012.345*kWh)
1-0:2.8.2(000001.234*kWh)
1-0:16.7.0(001.500*kW)
1-0:36.7.0(000.250*kvar)
1-0:21.7.0(000.500*kW)
1-0:41.7.0(000.480*kW)
1-0:61.7.0(000.520*kW)
1-0:32.7.0(230.1*V)
1-0:52.7.0(231.5*V)
1-0:72.7.0(229.3*V)
1-0:31.7.0(02.17*A)
1-0:51.7.0(02.09*A)
1-0:71.7.0(02.24*A)
1-0:13.7.0(0.95)
1-0:14.7.0(50.0*Hz)
!";
    examples.insert("EBZ_DD3".to_string(), ebz_telegram.to_string());

    examples
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_easymeter_definition() {
        let definition = get_easymeter_definition();
        assert_eq!(definition.meter_type, MeterType::EasyMeter);
        assert_eq!(definition.manufacturer_code, "ESY");
        assert_eq!(definition.default_baud_rate, 9600);
        assert!(definition.obis_mapping.contains_key("1-0:1.8.0"));
        assert!(definition.supported_modes.contains(&ProtocolMode::ModeD));
    }

    #[test]
    fn test_ebz_definition() {
        let definition = get_ebz_definition();
        assert_eq!(definition.meter_type, MeterType::EBZ);
        assert_eq!(definition.manufacturer_code, "EBZ");
        assert_eq!(definition.default_baud_rate, 9600);
        assert!(definition.obis_mapping.contains_key("1-0:16.7.0"));
        assert!(definition.supported_modes.contains(&ProtocolMode::ModeC));
    }

    #[test]
    fn test_get_meter_definition_by_manufacturer() {
        assert!(get_meter_definition_by_manufacturer("ESY").is_some());
        assert!(get_meter_definition_by_manufacturer("EBZ").is_some());
        assert!(get_meter_definition_by_manufacturer("UNKNOWN").is_none());
    }

    #[test]
    fn test_example_telegrams() {
        let examples = create_example_telegrams();
        assert!(examples.contains_key("EasyMeter_Q3D"));
        assert!(examples.contains_key("EBZ_DD3"));
        
        let easymeter_example = examples.get("EasyMeter_Q3D").unwrap();
        assert!(easymeter_example.contains("/ESY5Q3D"));
        assert!(easymeter_example.contains("1-0:1.8.0"));
        
        let ebz_example = examples.get("EBZ_DD3").unwrap();
        assert!(ebz_example.contains("/EBZ5DD3BL10-112"));
        assert!(ebz_example.contains("1-0:16.7.0"));
    }
}