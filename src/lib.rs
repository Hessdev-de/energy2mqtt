//! Device Manager library for managing IoT devices
//!
//! This library provides functionality for storing, retrieving, and configuring
//! IoT devices with a SQLite-based persistence layer.

pub mod device_manager;
pub mod models;
pub mod api;
pub mod mqtt;
pub mod metering_modbus;
pub mod config;
pub mod metering_oms;
pub mod metering_62056;
pub mod metering_sml;
pub mod metering_victron;
pub mod metering_zennerdatahub;
pub mod metering_knx;
pub mod obis_utils;
pub mod storage;
pub mod task_monitor;
pub mod discovered_devices;

// Re-export common types for easier access
pub use models::{Device, DeviceType, DeviceStatus};
pub use device_manager::DeviceManager;
pub use mqtt::{CALLBACKS, MeteringData};
pub use metering_modbus::ModbusManger;
pub use api::ApiManager;
pub use config::CONFIG;
pub use metering_oms::OmsManager;
pub use metering_62056::Iec62056Manager;
pub use metering_sml::SmlManager;
pub use metering_victron::VictronManager;
pub use metering_zennerdatahub::ZennerDatahubManager;
pub use metering_knx::KnxManager;
pub use storage::StoredData;
pub use task_monitor::{TaskMonitor, TaskInfo, TaskStatus};
pub use discovered_devices::{init_discovered_devices, get_discovered_devices, DiscoveredDevice, DiscoveredDeviceUpdate};

pub fn get_unix_ts() -> u64 {
    return std::time::SystemTime::now().duration_since(std::time::SystemTime::UNIX_EPOCH).unwrap().as_secs();
}

pub fn get_id(protocol: String, meter_name: &String) -> String {
    return format!("{}-{}-{:?}", protocol, meter_name, get_unix_ts());
}