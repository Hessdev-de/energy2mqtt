use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use chrono::{DateTime, Utc};
use uuid::Uuid;


/// Represents a device type in the system
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum DeviceType {
    Sensor,
    Actuator,
    Gateway,
    Controller,
    ModbusTcp,
    ModbusRtu,
    ModbusRtuOverTcp
}

impl DeviceType {
    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "Sensor" => Some(DeviceType::Sensor),
            "Actuator" => Some(DeviceType::Actuator),
            "Gateway" => Some(DeviceType::Gateway),
            "Controller" => Some(DeviceType::Controller),
            "ModbusTcp" => Some(DeviceType::ModbusTcp),
            "ModbusRtu" => Some(DeviceType::ModbusRtu),
            "ModbusRtuOverTcp" => Some(DeviceType::ModbusRtu),
            _ => None,
        }
    }

    pub fn to_string(&self) -> String {
        match self {
            DeviceType::Sensor => "Sensor".to_string(),
            DeviceType::Actuator => "Actuator".to_string(),
            DeviceType::Gateway => "Gateway".to_string(),
            DeviceType::Controller => "Controller".to_string(),
            DeviceType::ModbusTcp => "ModbusTcp".to_string(),
            DeviceType::ModbusRtu => "ModbusRtu".to_string(),
            DeviceType::ModbusRtuOverTcp => "ModbusRtuOverTcp".to_string(),
        }
    }
}

/// Represents the status of a device
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum DeviceStatus {
    Online,
    Offline,
    Maintenance,
    Unknown,
}

impl DeviceStatus {
    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "Online" => Some(DeviceStatus::Online),
            "Offline" => Some(DeviceStatus::Offline),
            "Maintenance" => Some(DeviceStatus::Maintenance),
            "Unknown" => Some(DeviceStatus::Unknown),
            _ => None,
        }
    }

    pub fn to_string(&self) -> String {
        match self {
            DeviceStatus::Online => "Online".to_string(),
            DeviceStatus::Offline => "Offline".to_string(),
            DeviceStatus::Maintenance => "Maintenance".to_string(),
            DeviceStatus::Unknown => "Unknown".to_string(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum DeviceProtocol {
    Unknown,
    ModbusTCP,
    ModbusRTU,
    OMS,
    MBUS,
    LoRaWAN,
    NBIoT,
    Tibber,
    IEC62056,
    SML,
    Victron,
    KNX
}

impl DeviceProtocol {
    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "Modbus TCP" => Some(DeviceProtocol::ModbusTCP),
            "Modbus RTU" => Some(DeviceProtocol::ModbusRTU),
            "OMS" => Some(DeviceProtocol::OMS),
            "M-Bus" => Some(DeviceProtocol::MBUS),
            "LoRaWAN" => Some(DeviceProtocol::LoRaWAN),
            "NBIoT" => Some(DeviceProtocol::NBIoT),
            "Tibber" => Some(DeviceProtocol::Tibber),
            "IEC 62056-21" => Some(DeviceProtocol::IEC62056),
            "SML" => Some(DeviceProtocol::SML),
            "Victron" => Some(DeviceProtocol::Victron),
            "KNX" => Some(DeviceProtocol::KNX),
            _ => Some(DeviceProtocol::Unknown),
        }
    }

    pub fn to_string(&self) -> String {
        match self {
            DeviceProtocol::ModbusTCP => "Modbus TCP".to_string(),
            DeviceProtocol::ModbusRTU => "Modbus RTU".to_string(),
            DeviceProtocol::OMS => "OMS".to_string(),
            DeviceProtocol::MBUS => "M-Bus".to_string(),
            DeviceProtocol::LoRaWAN => "LoRaWAN".to_string(),
            DeviceProtocol::NBIoT => "NBIoT".to_string(),
            DeviceProtocol::Unknown => "Unknown".to_string(),
            DeviceProtocol::Tibber => "Tibber".to_string(),
            DeviceProtocol::IEC62056 => "IEC 62056-21".to_string(),
            DeviceProtocol::SML => "SML".to_string(),
            DeviceProtocol::Victron => "Victron".to_string(),
            DeviceProtocol::KNX => "KNX".to_string(),
        }
    }
}

/// Represents a device in the system
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Device {
    /// Unique identifier for the device
    pub id: String,
    /// Human-readable name of the device
    pub name: String,
    /// Type of device
    pub device_type: DeviceType,
    /// Current status of the device
    pub status: DeviceStatus,
    /// IP address of the device
    pub device_protocol: String,
    /// Last time the device was seen online
    pub last_seen: DateTime<Utc>,
    /// Additional device-specific parameters
    pub parameters: HashMap<String, String>,
}


impl Device {
    /// Create a new device with default parameters
    pub fn new(name: String, device_type: DeviceType, protcol: String) -> Self {
        Device {
            id: Uuid::new_v4().to_string(),
            name,
            device_type,
            status: DeviceStatus::Offline,
            device_protocol: protcol,
            last_seen: Utc::now(),
            parameters: HashMap::new(),
        }
    }

    /// Update the device status
    pub fn update_status(&mut self, status: DeviceStatus) {
        self.status = status.clone();
        if self.status == DeviceStatus::Online {
            self.last_seen = Utc::now();
        }
    }

    /// Set a device parameter
    pub fn set_parameter(&mut self, key: String, value: String) {
        self.parameters.insert(key, value);
    }

    /// Get a device parameter
    pub fn get_parameter(&self, key: &str) -> Option<&String> {
        self.parameters.get(key)
    }
}