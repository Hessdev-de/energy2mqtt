use lazy_static::lazy_static;
use log::{debug, error, info};
use serde::{Deserialize, Serialize};
use serde_yml;
#[cfg(feature = "api")]
use utoipa::ToSchema;
use std::error::Error;
use std::fs::{self, File};
use std::path::Path;
use std::io::prelude::*;
use std::sync::RwLock;

fn httpd_enabled_default() -> bool { return true }
fn httpd_port_default() -> u16 { return 8240 }

#[derive(Deserialize, Serialize, Clone)]
#[cfg_attr(feature = "api", derive(ToSchema))]
pub struct HttpdConfig {
    #[serde(default="httpd_enabled_default")]
    pub enabled: bool,
    #[serde(default="httpd_port_default")]
    pub port: u16
}

fn mqtt_client_name_default() -> String { return "energy2mqtt".to_string() }
fn mqtt_client_user_default() -> String { return "energy2mqtt".to_string() }
fn mqtt_client_pass_default() -> String { return "energy2mqtt".to_string() }
fn mqtt_discovery_version_default() -> u32 { 1 }

/// Current discovery format version
/// Version 1: Flat topic structure (homeassistant/sensor/e2m_proto_device_sensor/config)
/// Version 2: Hierarchical topics + availability (homeassistant/sensor/e2m_proto_device/sensor/config)
pub const MQTT_DISCOVERY_VERSION_CURRENT: u32 = 2;

#[derive(Deserialize, Serialize, Clone)]
#[cfg_attr(feature = "api", derive(ToSchema))]
pub struct MqttConfig {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub pass: String,
    pub ha_enabled: bool,
    #[serde(default="mqtt_client_name_default")]
    pub client_name: String,
    /// Discovery format version - used to trigger cleanup when format changes
    #[serde(default="mqtt_discovery_version_default")]
    pub discovery_version: u32,
}

fn db_dbtype_default() -> String {return "sqlite".to_string() }
fn db_uri_default() -> String { return "config/devices.db".to_string() }

#[derive(Deserialize, Serialize, Clone)]
#[cfg_attr(feature = "api", derive(ToSchema))]
pub struct DatabaseConfig {
    #[serde(default="db_dbtype_default")]
    pub dbtype: String,
    #[serde(default="db_uri_default")]
    pub uri: String,
}

fn discovered_devices_path_default() -> String { "discovered_devices.yaml".to_string() }

#[derive(Deserialize, Serialize, Clone)]
#[cfg_attr(feature = "api", derive(ToSchema))]
pub struct StorageConfig {
    /// Path to the discovered devices file
    #[serde(default="discovered_devices_path_default")]
    pub discovered_devices_path: String,
}

fn storage_default() -> StorageConfig {
    StorageConfig {
        discovered_devices_path: discovered_devices_path_default(),
    }
}

#[derive(Deserialize, Serialize, Clone)]
#[cfg_attr(feature = "api", derive(ToSchema))]
pub struct ModbusDeviceConfig {
    pub name: String,
    pub meter: String,
    pub slave_id: u8,
    pub read_interval: u32,
}

#[derive(Deserialize, Serialize, Clone, PartialEq)]
#[cfg_attr(feature = "api", derive(ToSchema))]
pub enum ModbusProtoConfig {
    TCP,
    RTU,
    RTUoverTCP
}

fn modbus_hubs_devices_default() -> Vec<ModbusDeviceConfig> { return Vec::new() }
fn modbus_hub_connection_timeout_default() -> u64 { 10 }
fn modbus_hub_read_timeout_default() -> u64 { 5 }

#[derive(Deserialize, Serialize, Clone)]
#[cfg_attr(feature = "api", derive(ToSchema))]
pub struct ModbusHubConfig
{
    pub name: String,
    pub host: String,
    pub port: u16,
    pub proto: ModbusProtoConfig,
    #[serde(default="modbus_hub_connection_timeout_default")]
    pub connection_timeout: u64,  // Connection timeout in seconds
    #[serde(default="modbus_hub_read_timeout_default")]
    pub read_timeout: u64,        // Read/write timeout in seconds
    #[serde(default="modbus_hubs_devices_default")]
    pub devices: Vec<ModbusDeviceConfig>
}


fn modbus_hubs_default() -> Vec<ModbusHubConfig> { return Vec::new() }
#[derive(Deserialize, Serialize, Clone)]
#[cfg_attr(feature = "api", derive(ToSchema))]
pub struct ModbusConfig {
    #[serde(default="modbus_hubs_default")]
    pub hubs: Vec<ModbusHubConfig>,
}

#[derive(Deserialize, Serialize, Clone, PartialEq)]
#[cfg_attr(feature = "api", derive(ToSchema))]
pub enum ConfigOperation {
    ADD,
    DELETE,
    CHANGE
}

#[derive(Deserialize, Serialize, Clone)]
#[cfg_attr(feature = "api", derive(ToSchema))]
pub struct ConfigChange {
    pub operation: ConfigOperation,
    pub base: String, /* This is like mqtt, modbus and so on */
}
#[derive(Clone)]
pub struct Callbacks {
    sender: tokio::sync::broadcast::Sender<ConfigChange>,
}

#[derive(Deserialize, Serialize, Clone)]
#[cfg_attr(feature = "api", derive(ToSchema))]
pub struct TibberConfig {
    pub name: String,
    pub account_token: String,
}

#[derive(Deserialize, Serialize, Clone)]
#[cfg_attr(feature = "api", derive(ToSchema))]
pub struct OmsConfig {
    pub name: String,
    pub id: String,
    pub key: String,
}

/// Configuration for a single Victron cluster
#[derive(Deserialize, Serialize, Clone, Debug)]
#[cfg_attr(feature = "api", derive(ToSchema))]
pub struct VictronClusterConfig {
    #[serde(default = "victron_cluster_enabled_default")]
    pub enabled: bool,
}

fn victron_cluster_enabled_default() -> bool { true }

/// Battery cluster options
#[derive(Deserialize, Serialize, Clone, Debug)]
#[cfg_attr(feature = "api", derive(ToSchema))]
pub struct VictronBatteryClusterConfig {
    #[serde(default = "victron_cluster_enabled_default")]
    pub enabled: bool,
    /// Include Pylontech cell-level data (min/max cell voltage/temperature)
    #[serde(default = "victron_cluster_enabled_default")]
    pub include_cell_data: bool,
}

fn victron_battery_cluster_default() -> VictronBatteryClusterConfig {
    VictronBatteryClusterConfig {
        enabled: true,
        include_cell_data: true,
    }
}

/// All Victron export clusters
#[derive(Deserialize, Serialize, Clone, Debug)]
#[cfg_attr(feature = "api", derive(ToSchema))]
pub struct VictronClustersConfig {
    /// Grid import/export energy and power (essential for HA Energy Dashboard)
    #[serde(default = "victron_cluster_grid_default")]
    pub grid_metering: VictronClusterConfig,

    /// Battery SoC, power, temperature
    #[serde(default = "victron_battery_cluster_default")]
    pub battery: VictronBatteryClusterConfig,

    /// PV yield and charger status
    #[serde(default = "victron_cluster_solar_default")]
    pub solar: VictronClusterConfig,

    /// Detailed energy flow through inverter (advanced)
    #[serde(default = "victron_cluster_inverter_default")]
    pub inverter_flow: VictronClusterConfig,

    /// Total PV, consumption, system state
    #[serde(default = "victron_cluster_system_default")]
    pub system_overview: VictronClusterConfig,

    /// Per-phase voltage, current, power (advanced)
    #[serde(default = "victron_cluster_phase_default")]
    pub phase_details: VictronClusterConfig,

    /// Temperature and tank sensors
    #[serde(default = "victron_cluster_env_default")]
    pub environment: VictronClusterConfig,

    /// Error codes, alarms, device status
    #[serde(default = "victron_cluster_diag_default")]
    pub diagnostics: VictronClusterConfig,
}

fn victron_cluster_grid_default() -> VictronClusterConfig {
    VictronClusterConfig { enabled: true }
}

fn victron_cluster_solar_default() -> VictronClusterConfig {
    VictronClusterConfig { enabled: true }
}

fn victron_cluster_inverter_default() -> VictronClusterConfig {
    VictronClusterConfig { enabled: false }  // Advanced, disabled by default
}

fn victron_cluster_system_default() -> VictronClusterConfig {
    VictronClusterConfig { enabled: true }
}

fn victron_cluster_phase_default() -> VictronClusterConfig {
    VictronClusterConfig { enabled: false }  // Advanced, disabled by default
}

fn victron_cluster_env_default() -> VictronClusterConfig {
    VictronClusterConfig { enabled: false }  // Optional, disabled by default
}

fn victron_cluster_diag_default() -> VictronClusterConfig {
    VictronClusterConfig { enabled: false }  // Optional, disabled by default
}

fn victron_clusters_default() -> VictronClustersConfig {
    VictronClustersConfig {
        grid_metering: victron_cluster_grid_default(),
        battery: victron_battery_cluster_default(),
        solar: victron_cluster_solar_default(),
        inverter_flow: victron_cluster_inverter_default(),
        system_overview: victron_cluster_system_default(),
        phase_details: victron_cluster_phase_default(),
        environment: victron_cluster_env_default(),
        diagnostics: victron_cluster_diag_default(),
    }
}

#[derive(Deserialize, Serialize, Clone)]
#[cfg_attr(feature = "api", derive(ToSchema))]
pub struct VictronConfig {
    pub name: String,
    #[serde(default="mqtt_client_name_default")]
    pub client_name: String,
    pub broker_host: String,
    #[serde(default = "victron_broker_port_default")]
    pub broker_port: u16,
    #[serde(default = "victron_update_interval_default")]
    pub update_interval: u64,
    #[serde(default = "victron_enabled_default")]
    pub enabled: bool,
    /// Export clusters - control which data is exported to Home Assistant
    #[serde(default = "victron_clusters_default")]
    pub clusters: VictronClustersConfig,
}

fn victron_broker_port_default() -> u16 { 1883 }
fn victron_update_interval_default() -> u64 { 10 }
fn victron_enabled_default() -> bool { true }

#[derive(Deserialize, Serialize, Clone, PartialEq, Debug)]
#[cfg_attr(feature = "api", derive(ToSchema))]
pub enum KnxDatapointType {
    ActiveEnergyWh,     // DPT 13.010
    ActiveEnergyKwh,    // DPT 13.013
    ReactiveEnergyVah,  // DPT 13.012
    ReactiveEnergyKvarh, // DPT 13.015
    ApparentEnergyKvah, // DPT 13.014
    VoltageV,           // DPT 9.020 (millivolts -> volts)
    CurrentA,           // DPT 9.021 (milliamps -> amps)
    PowerW,             // DPT 14.056
    PowerKw,            // DPT 9.024
    PowerDensityWm2,    // DPT 9.022
    Switch,             // DPT 1.001 for on/off switches
}

fn knx_phase_energy_type_default() -> KnxDatapointType { KnxDatapointType::ActiveEnergyKwh }
fn knx_phase_power_type_default() -> KnxDatapointType { KnxDatapointType::PowerW }
fn knx_phase_voltage_type_default() -> KnxDatapointType { KnxDatapointType::VoltageV }
fn knx_phase_current_type_default() -> KnxDatapointType { KnxDatapointType::CurrentA }
fn knx_adapter_read_delay_default() -> u64 { 100 }

#[derive(Deserialize, Serialize, Clone)]
#[cfg_attr(feature = "api", derive(ToSchema))]
pub struct KnxPhaseConfig {
    pub name: String,
    pub voltage_ga: Option<String>,     // Group address for voltage
    pub current_ga: Option<String>,     // Group address for current
    pub power_ga: Option<String>,       // Group address for power
    pub energy_ga: Option<String>,      // Group address for energy
    #[serde(default="knx_phase_energy_type_default")]
    pub energy_type: KnxDatapointType,  // Type of energy measurement
    #[serde(default="knx_phase_power_type_default")]
    pub power_type: KnxDatapointType,   // Type of power measurement
    #[serde(default="knx_phase_voltage_type_default")]
    pub voltage_type: KnxDatapointType, // Type of voltage measurement
    #[serde(default="knx_phase_current_type_default")]
    pub current_type: KnxDatapointType, // Type of current measurement
    /// Group address for phase switch control (write)
    pub switch_ga: Option<String>,
    /// Group address for phase switch state feedback (read)
    pub switch_state_ga: Option<String>,
}

fn knx_meter_enabled_default() -> bool { true }
fn knx_meter_read_interval_default() -> u64 { 60 }
fn knx_meter_phases_default() -> Vec<KnxPhaseConfig> { Vec::new() }
fn knx_meter_calculate_totals_default() -> bool { true }

#[derive(Deserialize, Serialize, Clone)]
#[cfg_attr(feature = "api", derive(ToSchema))]
pub struct KnxMeterConfig {
    pub name: String,
    #[serde(default="knx_meter_enabled_default")]
    pub enabled: bool,
    #[serde(default="knx_meter_read_interval_default")]
    pub read_interval: u64,             // Seconds between reads

    /// Optional manufacturer for HA discovery (e.g., "ABB", "Siemens")
    pub manufacturer: Option<String>,
    /// Optional model for HA discovery (e.g., "Energy Meter EM/S")
    pub model: Option<String>,

    // Multi-phase configuration
    #[serde(default="knx_meter_phases_default")]
    pub phases: Vec<KnxPhaseConfig>,    // 1-3 phases

    // Single-meter configuration (alternative to phases)
    pub voltage_ga: Option<String>,
    pub current_ga: Option<String>,
    pub power_ga: Option<String>,
    pub energy_ga: Option<String>,
    #[serde(default="knx_phase_voltage_type_default")]
    pub voltage_type: KnxDatapointType,
    #[serde(default="knx_phase_current_type_default")]
    pub current_type: KnxDatapointType,
    #[serde(default="knx_phase_power_type_default")]
    pub power_type: KnxDatapointType,
    #[serde(default="knx_phase_energy_type_default")]
    pub energy_type: KnxDatapointType,

    // Total values (read from bus or calculated from phases)
    pub total_energy_ga: Option<String>,
    pub total_power_ga: Option<String>,
    pub total_current_ga: Option<String>,

    /// If true and phases are set, calculate *_all values by summing phase values
    #[serde(default="knx_meter_calculate_totals_default")]
    pub calculate_totals: bool,

    /// Global switch group address (for single-phase or combined control)
    pub switch_ga: Option<String>,
    /// Global switch state feedback address
    pub switch_state_ga: Option<String>,
}

fn knx_switch_enabled_default() -> bool { true }
fn knx_switch_expose_to_ha_default() -> bool { true }

#[derive(Deserialize, Serialize, Clone)]
#[cfg_attr(feature = "api", derive(ToSchema))]
pub struct KnxSwitchConfig {
    pub name: String,
    #[serde(default="knx_switch_enabled_default")]
    pub enabled: bool,
    pub group_address: String,          // Group address for switch control
    pub state_address: Option<String>,  // Optional state feedback address
    #[serde(default="knx_switch_expose_to_ha_default")]
    pub expose_to_ha: bool,             // Expose to Home Assistant
}

fn knx_adapter_port_default() -> u16 { 3671 }
fn knx_adapter_enabled_default() -> bool { true }
fn knx_adapter_connection_timeout_default() -> u64 { 10 }
fn knx_adapter_read_timeout_default() -> u64 { 5 }
fn knx_adapter_meters_default() -> Vec<KnxMeterConfig> { Vec::new() }
fn knx_adapter_switches_default() -> Vec<KnxSwitchConfig> { Vec::new() }
fn knx_adapter_poll_groups_default() -> Vec<KnxPollGroupConfig> { Vec::new() }
fn knx_poll_interval_default() -> u64 { 60 }
fn knx_poll_enabled_default() -> bool { true }

/// Configuration for a group address to be actively polled
#[derive(Deserialize, Serialize, Clone)]
#[cfg_attr(feature = "api", derive(ToSchema))]
pub struct KnxPollGroupConfig {
    /// Group address to poll (e.g., "1/2/3")
    pub group_address: String,
    /// Friendly name for this value
    pub name: String,
    /// Datapoint type for parsing the response
    pub dpt: KnxDatapointType,
    /// Polling interval in seconds
    #[serde(default = "knx_poll_interval_default")]
    pub poll_interval: u64,
    /// Whether this poll is enabled
    #[serde(default = "knx_poll_enabled_default")]
    pub enabled: bool,
    /// Optional meter name to associate this value with
    pub meter_name: Option<String>,
    /// Field name for the value (defaults to name if not set)
    pub field_name: Option<String>,
}

#[derive(Deserialize, Serialize, Clone)]
#[cfg_attr(feature = "api", derive(ToSchema))]
pub struct KnxAdapterConfig {
    pub name: String,
    pub host: String,
    #[serde(default="knx_adapter_port_default")]
    pub port: u16,
    #[serde(default="knx_adapter_enabled_default")]
    pub enabled: bool,
    #[serde(default="knx_adapter_connection_timeout_default")]
    pub connection_timeout: u64,        // Connection timeout in seconds
    #[serde(default="knx_adapter_read_timeout_default")]
    pub read_timeout: u64,              // Read timeout in seconds
    /// How long to wait after sending read requests before reading cache (default: 3 seconds)
    pub response_wait: Option<u64>,
    /// Delay between read requests in milliseconds (default: 100)
    #[serde(default="knx_adapter_read_delay_default")]
    pub read_delay_ms: u64,
    #[serde(default="knx_adapter_meters_default")]
    pub meters: Vec<KnxMeterConfig>,
    #[serde(default="knx_adapter_switches_default")]
    pub switches: Vec<KnxSwitchConfig>,
    /// Group addresses to actively poll
    #[serde(default="knx_adapter_poll_groups_default")]
    pub poll_groups: Vec<KnxPollGroupConfig>,
}

#[derive(Deserialize, Serialize, Clone)]
#[cfg_attr(feature = "api", derive(ToSchema))]
pub struct ZennerDatahubConfig {
    pub name: String,
    #[serde(default="mqtt_client_name_default")]
    pub client_name: String,
    pub broker_host: String,
    pub broker_port: u16,
    #[serde(default="mqtt_client_user_default")]
    pub broker_user: String,
    #[serde(default="mqtt_client_pass_default")]
    pub broker_pass: String,
    pub update_interval: u64,
    pub enabled: bool,
    pub base_topic: String,
}

fn httpd_default() -> HttpdConfig { return  HttpdConfig{ enabled: httpd_enabled_default(), port: httpd_port_default() }}
fn db_default() -> DatabaseConfig { return DatabaseConfig { dbtype: db_dbtype_default(), uri: db_uri_default() }}
fn modbus_default() -> ModbusConfig { return ModbusConfig { hubs: Vec::new() }}
fn tibber_default() -> Vec<TibberConfig> { return Vec::new(); }
fn oms_default() -> Vec<OmsConfig> { return Vec::new(); }
fn victron_default() -> Vec<VictronConfig> { return Vec::new(); }
fn knx_default() -> Vec<KnxAdapterConfig> { return Vec::new(); }
fn zridh_default() -> Vec<ZennerDatahubConfig> { return Vec::new(); }

#[derive(Deserialize, Serialize, Clone)]
#[cfg_attr(feature = "api", derive(ToSchema))]
pub struct Config {
    #[serde(default="httpd_default")]
    pub httpd: HttpdConfig,
    pub mqtt: MqttConfig,
    #[serde(default="db_default")]
    pub db: DatabaseConfig,
    #[serde(default="storage_default")]
    pub storage: StorageConfig,
    #[serde(default="modbus_default")]
    pub modbus: ModbusConfig,
    #[serde(default="tibber_default")]
    pub tibber: Vec<TibberConfig>,
    #[serde(default="oms_default")]
    pub oms: Vec<OmsConfig>,
    #[serde(default="victron_default")]
    pub victron: Vec<VictronConfig>,
    #[serde(default="knx_default")]
    pub knx: Vec<KnxAdapterConfig>,
    #[serde(default="zridh_default")]
    pub zenner_datahub: Vec<ZennerDatahubConfig>,
}

pub struct ConfigHolder {
    pub config: Config,
    pub callbacks: Callbacks,
    pub dirty: bool,
    pub lock: RwLock<bool>,
    pub base_path: String,
}

pub enum ConfigBases {
    Httpd(HttpdConfig),
    Mqtt(MqttConfig),
    Modbus(ModbusConfig),
    Tibber(Vec<TibberConfig>),
    Oms(Vec<OmsConfig>),
    Victron(Vec<VictronConfig>),
    Knx(Vec<KnxAdapterConfig>),
    ZRIDH(Vec<ZennerDatahubConfig>),
}

/// Status of the configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ConfigStatus {
    Valid,
    Missing,
    Invalid(String),
}

impl ConfigHolder {
    /// Try to load config, returning status and optional holder
    pub fn try_load() -> (ConfigStatus, Option<Self>) {
        let bpath = "config/".to_string();

        // Load config from config/e2m.yaml only
        let file = match File::open("config/e2m.yaml") {
            Ok(f) => {
                info!("Config file config/e2m.yaml loaded");
                f
            },
            Err(_) => {
                info!("No config file found at config/e2m.yaml");
                return (ConfigStatus::Missing, None);
            }
        };

        let mut file = file;
        let mut contents = String::new();
        if let Err(e) = file.read_to_string(&mut contents) {
            error!("Config could not be read: {}", e);
            return (ConfigStatus::Invalid(format!("Unable to read config file: {}", e)), None);
        }

        match serde_yml::from_str::<Config>(&contents) {
            Ok(c) => {
                let (s, _) = tokio::sync::broadcast::channel(100);
                (ConfigStatus::Valid, Some(ConfigHolder {
                    config: c,
                    callbacks: Callbacks { sender: s },
                    dirty: false,
                    lock: RwLock::new(true),
                    base_path: bpath,
                }))
            },
            Err(e) => {
                error!("Config could not be parsed: {}", e);
                (ConfigStatus::Invalid(format!("Unable to parse config file: {}", e)), None)
            }
        }
    }

    pub fn load() -> Self {
        let (_status, holder) = Self::try_load();
        match holder {
            Some(h) => h,
            None => {
                if cfg!(feature = "api") {
                    panic!("Configuration is invalid AND api is disabled, bailing out!")
                } else {
                    info!("Loading the config failed, using default config");
                }

                // Create a default config holder with placeholder MQTT settings
                // This allows the app to start and show the setup wizard
                info!("Creating default config holder for setup wizard");
                let default_config = Config {
                    httpd: httpd_default(),
                    mqtt: MqttConfig {
                        host: "".to_string(),
                        port: 1883,
                        user: "".to_string(),
                        pass: "".to_string(),
                        ha_enabled: true,
                        client_name: mqtt_client_name_default(),
                        discovery_version: MQTT_DISCOVERY_VERSION_CURRENT,
                    },
                    db: db_default(),
                    storage: storage_default(),
                    modbus: modbus_default(),
                    tibber: tibber_default(),
                    oms: oms_default(),
                    victron: victron_default(),
                    knx: knx_default(),
                    zenner_datahub: zridh_default(),
                };
                let (s, _) = tokio::sync::broadcast::channel(100);
                ConfigHolder {
                    config: default_config,
                    callbacks: Callbacks { sender: s },
                    dirty: false,
                    lock: RwLock::new(true),
                    base_path: "config/".to_string(),
                }
            }
        }
    }

    /// Check if the configuration is valid (has MQTT host configured)
    pub fn is_configured(&self) -> bool {
        !self.config.mqtt.host.is_empty()
    }

    /// Get the current config status
    pub fn get_status() -> ConfigStatus {
        let (status, _) = Self::try_load();
        status
    }

    /// Create initial config file with MQTT settings
    /// Note: base_path should always be "config/" - config files are always stored in config/
    pub fn create_initial_config(mqtt_config: MqttConfig, base_path: &str) -> Result<(), String> {
        let config = Config {
            httpd: httpd_default(),
            mqtt: mqtt_config,
            db: db_default(),
            storage: storage_default(),
            modbus: modbus_default(),
            tibber: tibber_default(),
            oms: oms_default(),
            victron: victron_default(),
            knx: knx_default(),
            zenner_datahub: zridh_default(),
        };

        let yaml = serde_yml::to_string(&config)
            .map_err(|e| format!("Failed to serialize config: {}", e))?;

        // Ensure config directory exists
        let dir_path = base_path.trim_end_matches('/');
        fs::create_dir_all(dir_path)
            .map_err(|e| format!("Failed to create config directory: {}", e))?;

        let config_path = format!("{}/e2m.yaml", dir_path);

        fs::write(&config_path, yaml.as_bytes())
            .map_err(|e| format!("Failed to write config file: {}", e))?;

        info!("Initial config created at {}", config_path);
        Ok(())
    }

    pub fn save(&mut self) {
        /* No need to write config if it's not dirty */
        if !self.dirty {
            debug!("Who ever called me, the config is not dirty");
            return;
        }

        let base = Path::new(&self.base_path);
        let config_path = base.join("e2m.yaml");
        let backup_path = base.join("backup.yaml");

        match fs::copy(&config_path, &backup_path) {
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                // First save, no existing config to backup - proceed anyway
                debug!("No existing config to backup, proceeding with first save");
            }
            Err(e) => {
                error!("Backing up config failed: {e}, not replacing it");
                return;
            }
        }

        let x = serde_yml::to_string(&self.config).unwrap();
        match fs::write(&config_path, x.as_bytes()) {
            Ok(_) => { info!("New Config written"); self.dirty = false; }
            Err(e) => { error!("Error writing config {e:?}"); }
        }
    }

    pub fn get_change_receiver(&self) -> tokio::sync::broadcast::Receiver<ConfigChange> {
        return self.callbacks.sender.subscribe();
    }

    pub fn is_dirty(&self) -> bool {
        return self.dirty;
    }

    pub fn update_config(&mut self, operation: ConfigOperation, new_data: ConfigBases) {
        // First we need to get a write lock
        //let _lock = self.lock.write().unwrap();
        let base: &str;

        match new_data {
            ConfigBases::Httpd(httpd_config) => {
                self.config.httpd = httpd_config;
                base = "httpd";
            },
            ConfigBases::Mqtt(mqtt_config) => {
                self.config.mqtt = mqtt_config;
                base = "mqtt";
            },
            ConfigBases::Modbus(modbus_config) => {
                self.config.modbus = modbus_config;
                base = "modbus";
            },
            ConfigBases::Tibber(tibber_configs) => {
                self.config.tibber = tibber_configs;
                base = "tibber";
            },
            ConfigBases::Oms(oms_configs) => {
                self.config.oms = oms_configs;
                base = "oms";
            },
            ConfigBases::Victron(victron_configs) => {
                self.config.victron = victron_configs;
                base = "victron";
            },
            ConfigBases::Knx(knx_configs) => {
                self.config.knx = knx_configs;
                base = "knx";
            }
            ConfigBases::ZRIDH(zridh_config) => {
                self.config.zenner_datahub = zridh_config;
                base = "zridh";
            }
        }

        self.dirty = true;

        let _ = self.callbacks.sender.send(ConfigChange { operation: operation, base: base.to_string()});
    }

    pub fn get_copy(&self, base: &str) -> Result<ConfigBases, Box<dyn Error>> {
        /* Lock against modifications during copy */
        let _lock = self.lock.read().unwrap();

        match base {
            "httpd" => { return Ok(ConfigBases::Httpd(self.config.httpd.clone())) },
            "mqtt" => { return Ok(ConfigBases::Mqtt(self.config.mqtt.clone())) },
            "modbus" => { return Ok(ConfigBases::Modbus(self.config.modbus.clone())) },
            "tibber" => { return Ok(ConfigBases::Tibber(self.config.tibber.clone())) },
            "oms" => { return Ok(ConfigBases::Oms(self.config.oms.clone())) },
            "victron" => { return Ok(ConfigBases::Victron(self.config.victron.clone())) },
            "knx" => { return Ok(ConfigBases::Knx(self.config.knx.clone())) },
            "zridh" => { return Ok(ConfigBases::ZRIDH(self.config.zenner_datahub.clone())) },
            _ => { Err("Type not known")? }
        }
    }

    pub fn get_complete_config(&self) -> Config {
        /* Lock against modifications during copy */
        //let _lock = self.lock.read().unwrap();
        return self.config.clone();
    }
}

lazy_static! {
    pub static ref CONFIG: RwLock<ConfigHolder> = RwLock::new(ConfigHolder::load());
}

#[macro_export]
macro_rules! get_config_or_panic {
    ($base: expr, $pat: path) => {
        {
            let c = CONFIG.read().unwrap().get_copy($base).unwrap();
            if let $pat(a) = c { // #1
                a
            } else {
                panic!(
                    "mismatch variant when cast to {}", 
                    stringify!($pat)); // #2
            }
        }
    };
}
