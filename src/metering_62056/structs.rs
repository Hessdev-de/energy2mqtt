#[derive(Debug, Clone)]
pub struct DeviceIdentification {
    pub manufacturer: String,
    pub identification: String,
    pub mode: String,
    pub full_id: String,
}


use crate::obis_utils::ObisData;

#[derive(Debug, Clone)]
pub struct Iec62056Telegram {
    pub identification: DeviceIdentification,
    pub data_objects: Vec<ObisData>,
    pub checksum: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum MeterType {
    EasyMeter,
    EBZ,
    Generic,
}

#[derive(Debug, Clone)]
pub struct MeterDefinition {
    pub meter_type: MeterType,
    pub manufacturer_code: String,
    pub supported_modes: Vec<super::ProtocolMode>,
    pub default_baud_rate: u32,
    pub obis_mapping: std::collections::HashMap<String, String>,
}