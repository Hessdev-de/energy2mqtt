//! Device Manager library for managing IoT devices
//!
//! This library provides functionality for storing, retrieving, and configuring
//! IoT devices with a SQLite-based persistence layer.

/// Application version - set via BUILD_VERSION env var at compile time,
/// falls back to "local" if not set
pub const VERSION: &str = match option_env!("BUILD_VERSION") {
    Some(v) => v,
    None => "local",
};

pub mod device_manager;
pub mod models;
#[cfg(feature = "api")]
pub mod api;
pub mod mqtt;
#[cfg(feature = "modbus")]
pub mod metering_modbus;
pub mod config;
#[cfg(feature = "oms")]
pub mod metering_oms;
#[cfg(feature = "iec62056")]
pub mod metering_62056;
#[cfg(feature = "sml")]
pub mod metering_sml;
#[cfg(feature = "victron")]
pub mod metering_victron;
#[cfg(feature = "zenner-datahub")]
pub mod metering_zennerdatahub;
#[cfg(feature = "knx")]
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
pub use config::CONFIG;
pub use storage::StoredData;
pub use task_monitor::{TaskMonitor, TaskInfo, TaskStatus};
pub use discovered_devices::{init_discovered_devices, get_discovered_devices, DiscoveredDevice, DiscoveredDeviceUpdate};

#[cfg(feature = "api")]
pub use api::ApiManager;
#[cfg(feature = "oms")]
pub use metering_oms::OmsManager;
#[cfg(feature = "iec62056")]
pub use metering_62056::Iec62056Manager;
#[cfg(feature = "sml")]
pub use metering_sml::SmlManager;
#[cfg(feature = "victron")]
pub use metering_victron::VictronManager;
#[cfg(feature = "zenner-datahub")]
pub use metering_zennerdatahub::ZennerDatahubManager;
#[cfg(feature = "knx")]
pub use metering_knx::KnxManager;

pub fn get_unix_ts() -> u64 {
    return std::time::SystemTime::now().duration_since(std::time::SystemTime::UNIX_EPOCH).unwrap().as_secs();
}

pub fn get_id(protocol: String, meter_name: &String) -> String {
    return format!("{}-{}-{:?}", protocol, meter_name, get_unix_ts());
}