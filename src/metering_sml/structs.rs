use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq)]
pub enum MeterType {
    EMH,           // EMH meters (ED300L, etc.)
    Iskraemeco,    // Iskraemeco MT175/MT631
    EasyMeter,     // EasyMeter (if they support SML)
    Itron,         // Itron OpenWay 3.HZ
    Generic,       // Unknown/Generic meters
}

#[derive(Debug, Clone)]
pub struct MeterDefinition {
    pub meter_type: MeterType,
    pub manufacturer_codes: Vec<String>,
    pub supported_obis_codes: Vec<String>,
    pub obis_mapping: HashMap<String, String>,
    pub description: String,
}

// SML Protocol Data Structures

#[derive(Debug, Clone)]
pub struct SmlFile {
    pub messages: Vec<SmlMessage>,
}

#[derive(Debug, Clone)]
pub struct SmlMessage {
    pub transaction_id: Vec<u8>,
    pub group_no: u8,
    pub abort_on_error: u8,
    pub message_body: SmlMessageBody,
    pub crc: Option<u16>,
    pub end_of_message: u8,
    pub client_id: Option<Vec<u8>>,
}

#[derive(Debug, Clone)]
pub struct SmlMessageBody {
    pub msg_type: u16,
    pub get_list_response: Option<SmlGetListResponse>,
    pub get_proc_parameter_response: Option<SmlGetProcParameterResponse>,
    pub attention_response: Option<SmlAttentionMessage>,
}

#[derive(Debug, Clone)]
pub struct SmlGetListResponse {
    pub client_id: Option<Vec<u8>>,
    pub server_id: Option<Vec<u8>>,
    pub list_name: Option<Vec<u8>>,
    pub act_sensor_time: Option<u32>,
    pub val_list: Vec<SmlListEntry>,
    pub list_signature: Option<Vec<u8>>,
    pub act_gateway_time: Option<u32>,
}

#[derive(Debug, Clone)]
pub struct SmlListEntry {
    pub obis_code: Option<Vec<u8>>,
    pub status: Option<u64>,
    pub val_time: Option<u32>,
    pub unit: Option<u8>,
    pub scaler: Option<i8>,
    pub value: Option<SmlValue>,
    pub value_signature: Option<Vec<u8>>,
}

#[derive(Debug, Clone)]
pub enum SmlValue {
    Bool(bool),
    Int8(i8),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    UInt8(u8),
    UInt16(u16),
    UInt32(u32),
    UInt64(u64),
    OctetString(Vec<u8>),
    List(Vec<SmlValue>),
}

#[derive(Debug, Clone)]
pub struct SmlGetProcParameterResponse {
    pub server_id: Option<Vec<u8>>,
    pub parameter_tree_path: Vec<u8>,
    pub parameter_tree: Option<SmlTree>,
}

#[derive(Debug, Clone)]
pub struct SmlTree {
    pub parameter_name: Option<Vec<u8>>,
    pub parameter_value: Option<SmlValue>,
    pub child_list: Option<Vec<SmlTree>>,
}

#[derive(Debug, Clone)]
pub struct SmlAttentionMessage {
    pub server_id: Option<Vec<u8>>,
    pub attention_no: Vec<u8>,
    pub attention_msg: Option<Vec<u8>>,
    pub attention_details: Option<SmlTree>,
}

// SML-specific OBIS code format (6 bytes)
#[derive(Debug, Clone)]
pub struct SmlObisCode {
    pub medium: u8,      // 0: abstract, 1: electricity, etc.
    pub channel: u8,     // 0-255
    pub indicator: u8,   // Physical value indicator
    pub mode: u8,        // Processing method
    pub tariff: u8,      // Tariff/time
    pub previous: u8,    // Historical value indicator
}

impl SmlObisCode {
    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        if bytes.len() == 6 {
            Some(SmlObisCode {
                medium: bytes[0],
                channel: bytes[1], 
                indicator: bytes[2],
                mode: bytes[3],
                tariff: bytes[4],
                previous: bytes[5],
            })
        } else {
            None
        }
    }
    
    pub fn to_string(&self) -> String {
        format!("{}-{}:{}.{}.{}.{}", 
            self.medium, self.channel, 
            self.indicator, self.mode, 
            self.tariff, self.previous)
    }
}

// Unit mappings for SML values
pub fn get_sml_unit_name(unit_code: u8) -> Option<&'static str> {
    match unit_code {
        1 => Some("a"),      // year
        2 => Some("mo"),     // month  
        3 => Some("wk"),     // week
        4 => Some("d"),      // day
        5 => Some("h"),      // hour
        6 => Some("min"),    // minute
        7 => Some("s"),      // second
        8 => Some("°"),      // degree
        9 => Some("°C"),     // degree celsius
        10 => Some("K"),     // kelvin
        11 => Some("m"),     // meter
        12 => Some("dm"),    // decimeter
        13 => Some("cm"),    // centimeter
        14 => Some("mm"),    // millimeter
        15 => Some("km"),    // kilometer
        16 => Some("m²"),    // square meter
        17 => Some("m³"),    // cubic meter
        18 => Some("l"),     // liter
        19 => Some("kg"),    // kilogram
        20 => Some("g"),     // gram
        21 => Some("t"),     // ton
        22 => Some("N"),     // newton
        23 => Some("Pa"),    // pascal
        24 => Some("bar"),   // bar
        25 => Some("J"),     // joule
        26 => Some("kJ"),    // kilojoule
        27 => Some("Wh"),    // watt hour
        28 => Some("kWh"),   // kilowatt hour
        29 => Some("MWh"),   // megawatt hour
        30 => Some("W"),     // watt
        31 => Some("kW"),    // kilowatt
        32 => Some("MW"),    // megawatt
        33 => Some("var"),   // volt ampere reactive
        34 => Some("kvar"),  // kilovolt ampere reactive
        35 => Some("VA"),    // volt ampere
        36 => Some("kVA"),   // kilovolt ampere
        37 => Some("V"),     // volt
        38 => Some("mV"),    // millivolt
        39 => Some("kV"),    // kilovolt
        40 => Some("A"),     // ampere
        41 => Some("mA"),    // milliampere
        42 => Some("kA"),    // kiloampere
        43 => Some("Ω"),     // ohm
        44 => Some("mΩ"),    // milliohm
        45 => Some("kΩ"),    // kiloohm
        46 => Some("F"),     // farad
        47 => Some("C"),     // coulomb
        48 => Some("Hz"),    // hertz
        49 => Some("kHz"),   // kilohertz
        50 => Some("MHz"),   // megahertz
        51 => Some("1/h"),   // per hour
        52 => Some("1/d"),   // per day
        53 => Some("1/wk"),  // per week
        54 => Some("1/mo"),  // per month
        55 => Some("1/a"),   // per year
        _ => None,
    }
}