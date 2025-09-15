use lazy_static::lazy_static;
use log::{debug, error, info};
use serde::{Deserialize, Serialize};
use serde_yml;
use utoipa::ToSchema;
use std::error::Error;
use std::fs::{self, File};
use std::io::prelude::*;
use std::sync::RwLock;

fn httpd_enabled_default() -> bool { return true }
fn httpd_port_default() -> u16 { return 8240 }

#[derive(Deserialize, Serialize, Clone)]
pub struct HttpdConfig {
    #[serde(default="httpd_enabled_default")]
    pub enabled: bool,
    #[serde(default="httpd_port_default")]
    pub port: u16
}

fn mqtt_client_name_default() -> String { return "energy2mqtt".to_string() }

#[derive(Deserialize, Serialize, Clone)]
pub struct MqttConfig {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub pass: String,
    pub ha_enabled: bool,
    #[serde(default="mqtt_client_name_default")]
    pub client_name: String,
}

fn db_dbtype_default() -> String {return "sqlite".to_string() }
fn db_uri_default() -> String { return "devices.db".to_string() }

#[derive(Deserialize, Serialize, Clone)]
pub struct DatabaseConfig {
    #[serde(default="db_dbtype_default")]
    pub dbtype: String,
    #[serde(default="db_uri_default")]
    pub uri: String,
}

#[derive(Deserialize, Serialize, Clone, ToSchema)]
pub struct ModbusDeviceConfig {
    pub name: String,
    pub meter: String,
    pub slave_id: u8,
    pub read_interval: u32,
}

#[derive(Deserialize, Serialize, Clone, PartialEq, ToSchema)]
pub enum ModbusProtoConfig {
    TCP,
    RTU,
    RTUoverTCP
}

fn modbus_hubs_devices_default() -> Vec<ModbusDeviceConfig> { return Vec::new() }
#[derive(Deserialize, Serialize, Clone, ToSchema)]
pub struct ModbusHubConfig
{
    pub name: String,
    pub host: String,
    pub port: u16,
    pub proto: ModbusProtoConfig,
    #[serde(default="modbus_hubs_devices_default")]
    pub devices: Vec<ModbusDeviceConfig>
}


fn modbus_hubs_default() -> Vec<ModbusHubConfig> { return Vec::new() }
#[derive(Deserialize, Serialize, Clone)]
pub struct ModbusConfig {
    #[serde(default="modbus_hubs_default")]
    pub hubs: Vec<ModbusHubConfig>,
}

#[derive(Deserialize, Serialize, Clone, PartialEq)]
pub enum ConfigOperation {
    ADD,
    DELETE,
    CHANGE
}

#[derive(Deserialize, Serialize, Clone)]
pub struct ConfigChange {
    pub operation: ConfigOperation,
    pub base: String, /* This is like mqtt, modbus and so on */
}
#[derive(Clone)]
pub struct Callbacks {
    sender: tokio::sync::broadcast::Sender<ConfigChange>,
}

#[derive(Deserialize, Serialize, Clone)]
pub struct TibberConfig {
    pub name: String,
    pub account_token: String,
}

#[derive(Deserialize, Serialize, Clone)]
pub struct OmsConfig {
    pub name: String,
    pub id: String,
    pub key: String,
}

fn victron_client_name_default() -> String {
    return "energy2mqtt".to_string();
}

#[derive(Deserialize, Serialize, Clone, ToSchema)]
pub struct VictronConfig {
    pub name: String,
    #[serde(default="victron_client_name_default")]
    pub client_name: String,
    pub broker_host: String,
    pub broker_port: u16,
    pub update_interval: u64,
    pub enabled: bool,
}

#[derive(Deserialize, Serialize, Clone, ToSchema, PartialEq)]
pub enum KnxConnectionType {
    TcpDirect,          // Direct TCP connection to KNX/IP interface
    UdpTunneling,       // UDP tunneling via KNXd daemon
}

#[derive(Deserialize, Serialize, Clone, ToSchema, PartialEq, Debug)]
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

#[derive(Deserialize, Serialize, Clone, ToSchema)]
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
}

fn knx_meter_enabled_default() -> bool { true }
fn knx_meter_read_interval_default() -> u64 { 60 }
fn knx_meter_phases_default() -> Vec<KnxPhaseConfig> { Vec::new() }

#[derive(Deserialize, Serialize, Clone, ToSchema)]
pub struct KnxMeterConfig {
    pub name: String,
    #[serde(default="knx_meter_enabled_default")]
    pub enabled: bool,
    #[serde(default="knx_meter_read_interval_default")]
    pub read_interval: u64,             // Seconds between reads
    #[serde(default="knx_meter_phases_default")]
    pub phases: Vec<KnxPhaseConfig>,    // 1-3 phases
    pub total_energy_ga: Option<String>, // Optional total energy group address
    pub total_power_ga: Option<String>,  // Optional total power group address
}

fn knx_switch_enabled_default() -> bool { true }
fn knx_switch_expose_to_ha_default() -> bool { true }

#[derive(Deserialize, Serialize, Clone, ToSchema)]
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
fn knx_adapter_connection_type_default() -> KnxConnectionType { KnxConnectionType::TcpDirect }

#[derive(Deserialize, Serialize, Clone, ToSchema)]
pub struct KnxAdapterConfig {
    pub name: String,
    pub host: String,
    #[serde(default="knx_adapter_port_default")]
    pub port: u16,
    #[serde(default="knx_adapter_enabled_default")]
    pub enabled: bool,
    #[serde(default="knx_adapter_connection_type_default")]
    pub connection_type: KnxConnectionType, // Connection method
    #[serde(default="knx_adapter_connection_timeout_default")]
    pub connection_timeout: u64,        // Connection timeout in seconds
    #[serde(default="knx_adapter_read_timeout_default")]
    pub read_timeout: u64,              // Read timeout in seconds
    #[serde(default="knx_adapter_meters_default")]
    pub meters: Vec<KnxMeterConfig>,
    #[serde(default="knx_adapter_switches_default")]
    pub switches: Vec<KnxSwitchConfig>,
}

fn httpd_default() -> HttpdConfig { return  HttpdConfig{ enabled: httpd_enabled_default(), port: httpd_port_default() }}
fn db_default() -> DatabaseConfig { return DatabaseConfig { dbtype: db_dbtype_default(), uri: db_uri_default() }}
fn modbus_default() -> ModbusConfig { return ModbusConfig { hubs: Vec::new() }}
fn tibber_default() -> Vec<TibberConfig> { return Vec::new(); }
fn oms_default() -> Vec<OmsConfig> { return Vec::new(); }
fn victron_default() -> Vec<VictronConfig> { return Vec::new(); }
fn knx_default() -> Vec<KnxAdapterConfig> { return Vec::new(); }
#[derive(Deserialize, Serialize, Clone)]
pub struct Config {
    #[serde(default="httpd_default")]
    pub httpd: HttpdConfig,
    pub mqtt: MqttConfig,
    #[serde(default="db_default")]
    pub db: DatabaseConfig,
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
}

impl ConfigHolder {
    pub fn load() -> Self {

        let mut bpath = "config/".to_string();
        /* Check for the two paths of the config file */
        let mut file = File::open("config/e2m.yaml");
        if file.is_err() {
            file = Ok(File::open("e2m.yaml").expect("Unable to read the config on config/e2m.yaml or e2m.yaml"));
            bpath = "".to_string();
        }

        let mut file = file.unwrap();

        let mut contents = String::new();
        file.read_to_string(&mut contents).expect("Unable to read config file");
        let c: Config =  serde_yml::from_str(&contents).expect("Unable to parse config file");
        let (s, _) = tokio::sync::broadcast::channel(100);
        return ConfigHolder { 
            config: c,
            callbacks: Callbacks { sender: s },
            dirty: false,
            lock: RwLock::new(true),
            base_path: bpath,
        }
    }

    pub fn save(&mut self) {
        /* No need to write config if it's not dirty */
        if !self.dirty {
            debug!("Who ever called me, the config is not dirty");
            return;
        }

        let config_path = format!("{:?}/e2m.yaml", self.base_path);
        let backup_path = format!("{:?}/backup.yaml", self.base_path);
        
        if fs::copy(config_path.clone(), backup_path).is_err() {
            error!("Backing up config failed, not replacing it");
        } else {
            let x = serde_yml::to_string(&self.config).unwrap();
            match fs::write(config_path, x.as_bytes()) {
                Ok(_) => { info!("New Config written"); self.dirty = false; }
                Err(e) => { error!("Error writing config {e:?}"); }
            }
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
        let base: &str ;

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
