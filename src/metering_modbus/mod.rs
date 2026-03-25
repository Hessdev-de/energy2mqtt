use std::collections::HashMap;
use std::time::Duration;
use crate::mqtt::SubscribeData;
use crate::{config::{ConfigBases, ConfigChange, ConfigOperation, ModbusConfig, ModbusDeviceConfig, ModbusHubConfig, ModbusProtoConfig}, metering_modbus::registers::Register, models::DeviceProtocol, mqtt::{home_assistant::HaSensor, Transmission, publish_protocol_count}, task_monitor::TaskMonitor, CONFIG};
use log::{debug, error, info, warn};
use rmodbus::ModbusProto;
use serde::Deserialize;
use tokio::{net::TcpStream, sync::mpsc::Sender};
pub mod registers;
pub mod read_device_parms;
pub mod set_device_parms;
pub mod ha_config;
pub mod utils;

/// Errors that can occur during Modbus communication
#[derive(Debug)]
pub enum ModbusError {
    ConnectionFailed(String),
    ConnectionTimeout(u64),
    ConnectionClosed,
    ReadTimeout(u64),
    WriteTimeout(u64),
    WriteFailed(String),
    ProtocolError(String),
    IoError(std::io::Error),
}

impl std::fmt::Display for ModbusError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ModbusError::ConnectionFailed(msg) => write!(f, "Connection failed: {}", msg),
            ModbusError::ConnectionTimeout(secs) => write!(f, "Connection timeout after {}s", secs),
            ModbusError::ConnectionClosed => write!(f, "Connection closed by server"),
            ModbusError::ReadTimeout(secs) => write!(f, "Read timeout after {}s", secs),
            ModbusError::WriteTimeout(secs) => write!(f, "Write timeout after {}s", secs),
            ModbusError::WriteFailed(msg) => write!(f, "Write failed: {}", msg),
            ModbusError::ProtocolError(msg) => write!(f, "Protocol error: {}", msg),
            ModbusError::IoError(e) => write!(f, "IO error: {}", e),
        }
    }
}

impl std::error::Error for ModbusError {}

/// Connection state for a Modbus hub - lives in task scope across read cycles
pub struct HubConnectionState {
    stream: Option<TcpStream>,
    connection_timeout: Duration,
    read_timeout: Duration,
    consecutive_failures: u32,
}

impl HubConnectionState {
    fn new(config: &ModbusHubConfig) -> Self {
        Self {
            stream: None,
            connection_timeout: Duration::from_secs(config.connection_timeout),
            read_timeout: Duration::from_secs(config.read_timeout),
            consecutive_failures: 0,
        }
    }

    fn clear_connection(&mut self) {
        self.stream = None;
    }

    fn record_success(&mut self) {
        self.consecutive_failures = 0;
    }

    fn record_failure(&mut self) {
        self.consecutive_failures += 1;
    }
}


pub struct ModbusManger {
    sender: Sender<Transmission>,
    config_change: tokio::sync::broadcast::Receiver<ConfigChange>,
    task_monitor: TaskMonitor,
    config: ModbusConfig,
}

#[derive(Clone)]
pub struct ModbusDevice {
    config: ModbusDeviceConfig,
    waits_till_read: u32,
    cur_waits: u32,
    registers: Vec<registers::Register>,
    device_class: String,
}

#[derive(Deserialize)]
struct ModbusMqttCommand {
    function: String,
    device: String,
    registers: HashMap<String, Vec<u8>>
}

#[derive(Clone)]
pub struct ModbusHub
{
    config: ModbusHubConfig,
    devices: Vec<ModbusDevice>,
}

impl ModbusManger
{
    pub fn new(sender: Sender<Transmission>) -> Self {
        let config: ModbusConfig = crate::get_config_or_panic!("modbus", ConfigBases::Modbus);

        ModbusManger {
            sender: sender.clone(),
            config_change: CONFIG.read().unwrap().get_change_receiver(),
            task_monitor: TaskMonitor::with_mqtt("modbus", sender),
            config,
        }
    }

    pub async fn start_thread(&mut self) {

        /* There may be not config to start with, so sleep until there is  */
        if self.config.hubs.len() == 0 {
            info!("No Modbus devices found, waiting for a config change to wake me up");
            loop {
                let change = self.config_change.recv().await.unwrap();
                if change.operation != ConfigOperation::ADD || change.base != "modbus" {
                    continue;
                }

                /* we need to read the config now as this change is about our part of the code */
                break;
            }
        }

        info!("Started Modbus configuration, but waiting some seconds for futher updates");
        let _ = tokio::time::sleep(Duration::from_secs(5)).await;

        /* we need to restart the threads on every config change for now, because it's much simpler :) */
        loop {
            let mut device_count : u32 = 0;
            self.config = crate::get_config_or_panic!("modbus", ConfigBases::Modbus);

            /* Read config of all modbus devices */
            for config_hub  in self.config.hubs.iter() {
                device_count += config_hub.devices.len() as u32;

                let hub_sender = self.sender.clone();
                /* Sender and Receiver for our Callbacks */
                let (sender, mut write_receiver) = tokio::sync::mpsc::channel(10);
                let mut hub = ModbusHub {
                    config: config_hub.clone(),
                    devices: {
                        let mut devs: Vec<ModbusDevice> = Vec::new();
                        for dev in config_hub.devices.iter() {
                            let (regs, manu, model) = registers::get_registers(&dev.meter);
                            let r = regs.clone();
                            let d = ModbusDevice {
                                config: dev.clone(),
                                waits_till_read: 1,
                                cur_waits: 0,
                                registers: regs,
                                device_class: "sensor".to_string(),
                            };
                            devs.push(d);

                            /* Register with Home Assistant using individual entity discovery */
                            let mut discover = HaSensor::new(
                                format!("{:?}", DeviceProtocol::ModbusTCP),
                                dev.name.clone(),
                                Some(manu),
                                Some(model)
                            );

                            /* Subscribe to our set topic for RAW transmission of data to registers */
                            let _ = hub_sender.send(Transmission::Subscribe(SubscribeData {
                                                            topic: format!("energy2mqtt/set/modbus/{}/{}", &config_hub.name, &dev.name),
                                                            sender: sender.clone() })).await;

                            /* Add everything to home assistant if needed */
                            for reg in r {
                                let _ = ha_config::get_cmp_from_reg(reg.clone(), &mut discover, &sender, &hub_sender, &config_hub.name, &dev.name).await;
                            }

                            let _ = hub_sender.send(Transmission::AutoDiscovery2(discover)).await;
                        }
                        devs
                    }
                };

                /* Find the sleeptime of this hub, do not use a too small value as it may halt the application  */
                let mut hub_inveral_sec: u32 = 60;
                for device in hub.devices.iter() {
                    hub_inveral_sec = std::cmp::min(hub_inveral_sec, device.config.read_interval);
                }

                /*
                 * Now check again to round the read intervals
                 */
                for device in hub.devices.iter_mut() {
                    /* Round up based on the hubs read interval */
                    device.waits_till_read = device.config.read_interval / hub_inveral_sec;

                    let new_sec = device.waits_till_read * hub_inveral_sec;
                    if new_sec != device.config.read_interval {
                        /* Print a warning if the readouts changed */
                        warn!("Device {} will be read every {} seconds instead of {} seconds because of your config",
                                device.config.name, new_sec, device.config.read_interval);
                    }
                }


                let hub_name_for_task = config_hub.name.clone();
                self.task_monitor.spawn(
                    &format!("hub_{}", hub_name_for_task),
                    "modbus_hub",
                    async move {
                        let hub_delay = Duration::from_secs(hub_inveral_sec as u64);
                        let socket_addr = format!("{}:{}", hub.config.host, hub.config.port);

                        let mut proto = ModbusProto::TcpUdp;
                        /* if we use RTUoverTCP we need to add all of those fancy CRC stuff */
                        if hub.config.proto == ModbusProtoConfig::RTUoverTCP {
                            proto = ModbusProto::Rtu;
                        }

                        // Create connection state that persists across read cycles
                        let mut conn_state = HubConnectionState::new(&hub.config);

                        loop {
                            tokio::select! {
                                /* Now sleep for one tick of hub_inveral_sec */
                                _ =  tokio::time::sleep(hub_delay) => {
                                    /* Increment wait counters for all devices */
                                    for device in hub.devices.iter_mut() {
                                        device.cur_waits += 1;
                                    }
                                },
                                /* We got a write command, we may miss a beat but that is ok */
                                Some((topic, payload)) = write_receiver.recv() => {
                                    if topic.starts_with("energy2mqtt/set/modbus") {
                                        /* Check which device we need to call out to */
                                        let command: ModbusMqttCommand = match serde_json::from_slice(payload.as_bytes()) {
                                            Ok(d) => d,
                                            Err(e) => {
                                                error!("Malformed JSON received: {:?} -> {}", e, payload);
                                                continue;
                                            }
                                        };

                                        if command.function != "modbus_set" {
                                            error!("Function {} is unknown for {}", command.function, topic);
                                            continue;
                                        }

                                        /* Find the device to use */
                                        for device in hub.devices.iter_mut() {
                                            if device.config.name != command.device {
                                                debug!("{} != {}", device.config.name, command.device);
                                                continue;
                                            }

                                            /* We found our device */
                                            set_device_parms::set(&socket_addr, &hub.config.name, &command.registers, device, proto, &mut conn_state).await;
                                            debug!("Hub {} Device {} will now be read because the configuration changed", hub.config.name, device.config.name);
                                            device.cur_waits = device.waits_till_read + 10;
                                        }

                                    } else if topic.starts_with("energy2mqtt/cmds/modbus") {
                                        //"energy2mqtt/cmds/modbus/{}/{}/{}"
                                        /* Get the correct device to run */
                                        let (topic, register) = topic.rsplit_once('/').unwrap();
                                        let (_, name) = topic.rsplit_once('/').unwrap();

                                        for device in hub.devices.iter_mut() {
                                            if device.config.name == name {
                                                for reg in &device.registers {
                                                    if let Register::Modbus(r) = &reg {
                                                        
                                                        if r.name != register {
                                                            continue;
                                                        }

                                                        let value = utils::get_data_vec(&reg, &payload);
                                                        if value.is_empty() {
                                                            error!("Register {}: conversation failed, skipping", r.name);
                                                            continue;
                                                        }

                                                        info!("WRITING {} -> {} -> {:?}", r.register, payload, value);

                                                        /* Write our register */
                                                        set_device_parms::write_register(device, proto, &mut conn_state, reg.clone(), value).await;
                                                        debug!("Hub {} Device {} will now be read because the configuration changed", hub.config.name, device.config.name);
                                                        device.cur_waits = device.waits_till_read + 10;
                                                    }
                                                }
                                                break;
                                            }
                                        }
                                    }
                                }

                            }


                            /* Read all devices that are due using persistent connection
                            * This is critical for RTU over TCP where converters typically
                            * only support one active connection at a time */
                            read_device_parms::read_hub_devices(
                                &socket_addr,
                                &mut hub.devices,
                                &hub.config.name,
                                proto,
                                &hub_sender,
                                &mut conn_state,
                            ).await;


                            // Log connection health if there are failures
                            if conn_state.consecutive_failures > 0 {
                                warn!("Hub {}: {} consecutive failures",
                                    hub.config.name, conn_state.consecutive_failures);
                            }
                        }
                    }
                ).await;
            } /* loop per config hub */

            // Publish device count to MQTT
            publish_protocol_count(&self.sender, "modbus", device_count).await;

            info!(
                "Modbus activated with {} hubs and {} devices, waiting for config changes",
                self.config.hubs.len(),
                device_count
            );

            // Wait for config changes while periodically checking task health
            loop {
                tokio::select! {
                    // Check for config changes
                    change_result = self.config_change.recv() => {
                        match change_result {
                            Ok(change) if change.base == "modbus" => {
                                info!("Modbus config change detected, restarting tasks");
                                break;
                            }
                            Ok(_) => continue,
                            Err(e) => {
                                error!("Config change receiver error: {:?}", e);
                                continue;
                            }
                        }
                    }
                    // Periodic health check every 30 seconds
                    _ = tokio::time::sleep(Duration::from_secs(30)) => {
                        let crashed = self.task_monitor.check_all_tasks().await;
                        if !crashed.is_empty() {
                            warn!(
                                "Modbus: {} task(s) crashed! Tasks: {:?}",
                                crashed.len(),
                                crashed.iter().map(|(name, _, _)| name.clone()).collect::<Vec<_>>()
                            );
                        }

                        let running = self.task_monitor.running_count().await;
                        let total = self.task_monitor.task_count().await;
                        debug!("Modbus task health: {}/{} tasks running", running, total);
                    }
                }
            }

            /* We are woken up because some of our config changed, so stop the threads and start over */
            info!("Modbus is stopping all hub tasks");
            self.task_monitor.clear_all().await;
        }
    }
}