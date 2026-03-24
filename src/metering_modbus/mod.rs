use std::collections::HashMap;
use std::time::{Duration, SystemTime};
use crate::mqtt::{PublishData, SubscribeData};
use crate::{config::{ConfigBases, ConfigChange, ConfigOperation, ModbusConfig, ModbusDeviceConfig, ModbusHubConfig, ModbusProtoConfig}, metering_modbus::registers::Register, models::DeviceProtocol, mqtt::{home_assistant::{HaSensor, HaComponent2}, Transmission, publish_protocol_count}, task_monitor::TaskMonitor, MeteringData, CONFIG};
use evalexpr::{ContextWithMutableVariables, DefaultNumericTypes, HashMapContext};
use log::{debug, error, info, warn};
use rmodbus::{client::ModbusRequest, guess_response_frame_len, ModbusProto};
use serde::{Deserialize, Serialize};
use tokio::{io::{AsyncReadExt, AsyncWriteExt}, net::TcpStream, sync::mpsc::Sender, time::timeout};
pub mod registers;
pub mod set_device_parms;
pub mod ha_config;

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

                                    /* Read all devices that are due using persistent connection
                                    * This is critical for RTU over TCP where converters typically
                                    * only support one active connection at a time */
                                    read_hub_devices(
                                        &socket_addr,
                                        &mut hub.devices,
                                        &hub.config.name,
                                        proto,
                                        &hub_sender,
                                        &mut conn_state,
                                    ).await;
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
                                        for device in hub.devices.iter() {
                                            if device.config.name != command.device {
                                                debug!("{} != {}", device.config.name, command.device);
                                                continue;
                                            }

                                            /* We found our device */
                                            set_device_parms::set(&socket_addr, &hub.config.name, &command.registers, device, proto, &mut conn_state).await;
                                        }

                                    } else if topic.starts_with("energy2mqtt/cmds/modbus") {
                                        //"energy2mqtt/cmds/modbus/{}/{}/{}"
                                        /* Get the correct device to run */
                                        let (topic, register) = topic.rsplit_once('/').unwrap();
                                        let (_, name) = topic.rsplit_once('/').unwrap();

                                        for device in hub.devices.iter() {
                                            if device.config.name == name {
                                                for reg in &device.registers {
                                                    if let Register::Modbus(r) = &reg {
                                                        
                                                        if r.name != register {
                                                            continue;
                                                        }

                                                        let value: Vec<u8> = match r.format {
                                                            registers::ModbusRegisterFormat::Int16 => {
                                                                let d: Result<i16, _> = payload.parse();
                                                                if d.is_err() {
                                                                    error!("Can not parse register {} to {payload}", r.register);
                                                                    break;
                                                                }

                                                                d.unwrap().to_be_bytes().to_vec()
                                                            },
                                                            registers::ModbusRegisterFormat::UInt16 => {
                                                                let d: Result<u16, _> = payload.parse();
                                                                if d.is_err() {
                                                                    error!("Can not parse register {} to {payload}", r.register);
                                                                    break;
                                                                }

                                                                d.unwrap().to_be_bytes().to_vec() 
                                                            },
                                                            registers::ModbusRegisterFormat::Int32 => {
                                                                let d: Result<i32, _> = payload.parse();
                                                                if d.is_err() {
                                                                    error!("Can not parse register {} to {payload}", r.register);
                                                                    break;
                                                                }

                                                                d.unwrap().to_be_bytes().to_vec() 
                                                            },
                                                            registers::ModbusRegisterFormat::UInt32 => {
                                                                let d: Result<u32, _> = payload.parse();
                                                                if d.is_err() {
                                                                    error!("Can not parse register {} to {payload}", r.register);
                                                                    break;
                                                                }

                                                                d.unwrap().to_be_bytes().to_vec() 
                                                            },
                                                            registers::ModbusRegisterFormat::Coil => {
                                                                let d: Result<u8, _> = payload.parse();
                                                                if d.is_err() {
                                                                    error!("Can not parse register {} to {payload}", r.register);
                                                                    break;
                                                                }

                                                                d.unwrap().to_be_bytes().to_vec()
                                                            }
                                                            _ => {
                                                                error!("Unable to set f32, SunSSF or String");
                                                                break;
                                                            }
                                                        };

                                                        info!("WRITING {} -> {} -> {:?}", r.register, payload, value);

                                                        /* Write our register */
                                                        set_device_parms::write_register(device, proto, &mut conn_state, reg.clone(), value).await;
                                                    }
                                                }
                                                break;
                                            }
                                        }
                                    }
                                }

                            }

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

/// Establish a connection to a Modbus hub with timeout
async fn connect_to_hub(
    socket_addr: &str,
    connection_timeout: Duration,
) -> Result<TcpStream, ModbusError> {
    match timeout(connection_timeout, TcpStream::connect(socket_addr)).await {
        Ok(Ok(stream)) => {
            let _ = stream.set_nodelay(true);
            Ok(stream)
        }
        Ok(Err(e)) => Err(ModbusError::ConnectionFailed(format!(
            "Failed to connect to {}: {}", socket_addr, e
        ))),
        Err(_) => Err(ModbusError::ConnectionTimeout(connection_timeout.as_secs())),
    }
}

/// Read all devices on a hub using persistent connection
/// Connection is kept alive across read cycles; only reconnects on failure
/// This is important for RTU over TCP where many converters only support one connection
async fn read_hub_devices(
    socket_addr: &str,
    devices: &mut [ModbusDevice],
    hub_name: &str,
    proto: ModbusProto,
    hub_sender: &Sender<Transmission>,
    conn_state: &mut HubConnectionState,
) {
    // Collect indices of devices that need to be read this cycle
    let devices_to_read: Vec<usize> = devices.iter()
        .enumerate()
        .filter(|(_, d)| d.cur_waits >= d.waits_till_read)
        .map(|(i, _)| i)
        .collect();

    if devices_to_read.is_empty() {
        return;
    }

    // Ensure we have a connection (reuse existing or establish new)
    if conn_state.stream.is_none() {
        match connect_to_hub_with_retry(
            socket_addr,
            hub_name,
            conn_state.connection_timeout
        ).await {
            Ok(s) => {
                info!("Hub {}: Connection established", hub_name);
                conn_state.stream = Some(s);
                conn_state.consecutive_failures = 0;
            }
            Err(e) => {
                error!("Hub {}: Failed to establish connection after retries: {:?}", hub_name, e);
                conn_state.record_failure();
                return;
            }
        }
    }

    // Read all devices using the persistent connection
    for idx in devices_to_read {
        let device = &mut devices[idx];
        device.cur_waits = 0;

        debug!("Hub {} Device {} start reading", hub_name, device.config.name);

        // Get mutable reference to stream
        let stream = conn_state.stream.as_mut().unwrap();

        match read_device_registers(
            stream,
            device,
            hub_name,
            proto,
            hub_sender,
            conn_state.read_timeout
        ).await {
            Ok(_) => {
                debug!("Hub {} Device {} done reading", hub_name, device.config.name);
                conn_state.record_success();
            }
            Err(e) => {
                error!("Hub {} Device {} read failed: {:?}", hub_name, device.config.name, e);
                conn_state.record_failure();

                // Close the broken connection
                conn_state.clear_connection();

                // Try to reconnect for remaining devices
                match connect_to_hub_with_retry(
                    socket_addr,
                    hub_name,
                    conn_state.connection_timeout
                ).await {
                    Ok(new_stream) => {
                        conn_state.stream = Some(new_stream);
                        warn!("Hub {}: Reconnected after device {} failure", hub_name, device.config.name);
                    }
                    Err(reconnect_err) => {
                        error!("Hub {}: Failed to reconnect: {:?}. Skipping remaining devices.", hub_name, reconnect_err);
                        break;
                    }
                }
            }
        }
    }
    // Connection is NOT dropped here - it persists for the next cycle
}

/// Connect to hub with retry logic
async fn connect_to_hub_with_retry(
    socket_addr: &str,
    hub_name: &str,
    connection_timeout: Duration,
) -> Result<TcpStream, ModbusError> {
    const MAX_RETRIES: u32 = 3;
    let mut retries = 0;

    loop {
        match connect_to_hub(socket_addr, connection_timeout).await {
            Ok(stream) => return Ok(stream),
            Err(e) if retries < MAX_RETRIES => {
                warn!("Hub {}: Connection error, retrying ({}/{}): {:?}",
                      hub_name, retries + 1, MAX_RETRIES, e);
                retries += 1;
                tokio::time::sleep(Duration::from_secs(1)).await;
                continue;
            }
            Err(e) => return Err(e),
        }
    }
}

/* Helper defition for the RAW export */
#[derive(Serialize)]
struct E2MRegister {
    //r#type: u32,
    address: i32,
    data: Vec<u8>
}

#[derive(Serialize)]
struct E2MRawData {
    hub: String,
    device: String,
    registers: Vec<E2MRegister>,
}

/// Read registers from a single device using an existing connection
async fn read_device_registers(
    stream: &mut TcpStream,
    device: &mut ModbusDevice,
    hub_name: &str,
    proto: ModbusProto,
    hub_sender: &Sender<Transmission>,
    read_timeout: Duration,
) -> Result<(), ModbusError> {
    let mut meter_data = MeteringData::new().unwrap();
    meter_data.meter_name = device.config.name.clone();
    meter_data.protocol = DeviceProtocol::ModbusTCP;
    meter_data.id = get_id("modbus".to_string(), &device.config.name);
    meter_data.transmission_time = get_unix_ts();
    meter_data.metered_time = meter_data.transmission_time;

    let mut context = HashMapContext::<DefaultNumericTypes>::new();
    // Store SunSpec scale factors for later application
    let mut scale_factors: HashMap<String, i16> = HashMap::new();

    /* We want to send the raw data to mqtt */
    let mut raw_data = E2MRawData {
        hub: hub_name.to_string(),
        device: device.config.name.clone(),
        registers: Vec::new()
    };

    for reg in &device.registers {
        let reg = match reg {
            Register::Template(_) => continue,
            Register::Modbus(modbus_register) => modbus_register,
        };

        debug!("Hub {} Device {} Register {} start reading", hub_name, device.config.name, reg.name);

        let mut mreq = ModbusRequest::new(device.config.slave_id, proto);
        let mut request = Vec::new();

        match reg.input_type {
            registers::ModbusRegisterType::Holding => {
                mreq.generate_get_holdings(reg.register, reg.length, &mut request).unwrap();
            }
            registers::ModbusRegisterType::Input => {
                mreq.generate_get_inputs(reg.register, reg.length, &mut request).unwrap();
            }
            registers::ModbusRegisterType::Coil => {
                mreq.generate_get_coils(reg.register, reg.length, &mut request).unwrap();
            }
        }

        // Write request with timeout
        match timeout(read_timeout, stream.write_all(&request)).await {
            Ok(Ok(_)) => {}
            Ok(Err(e)) => {
                return Err(ModbusError::WriteFailed(format!(
                    "Failed to write request for register {}: {}", reg.name, e
                )));
            }
            Err(_) => {
                return Err(ModbusError::WriteTimeout(read_timeout.as_secs()));
            }
        }

        // Read response header with timeout
        let mut buf = [0u8; 6];
        let bytes_read = match timeout(read_timeout, stream.read(&mut buf)).await {
            Ok(Ok(n)) => n,
            Ok(Err(e)) => {
                return Err(ModbusError::IoError(e));
            }
            Err(_) => {
                return Err(ModbusError::ReadTimeout(read_timeout.as_secs()));
            }
        };

        if bytes_read == 0 {
            return Err(ModbusError::ConnectionClosed);
        }

        let mut response = Vec::new();
        response.extend_from_slice(&buf[..bytes_read]);

        let len = guess_response_frame_len(&buf, proto)
            .map_err(|e| ModbusError::ProtocolError(format!(
                "Failed to determine response length for register {}: {:?}", reg.name, e
            )))?;

        if len as usize > bytes_read {
            let mut rest = vec![0u8; len as usize - bytes_read];

            // Read rest of response with timeout
            let rest_bytes = match timeout(read_timeout, stream.read(&mut rest)).await {
                Ok(Ok(n)) => n,
                Ok(Err(e)) => {
                    return Err(ModbusError::IoError(e));
                }
                Err(_) => {
                    return Err(ModbusError::ReadTimeout(read_timeout.as_secs()));
                }
            };

            if rest_bytes == 0 {
                return Err(ModbusError::ConnectionClosed);
            }

            response.extend(&rest[..rest_bytes]);
        }

        // Process the response - use f64 to handle all numeric types
        let parsed_value: Result<f64, String>;
        let mut string_value: Option<String> = None;

        raw_data.registers.push( E2MRegister { address: reg.register as i32, data: response.clone() });

        match reg.format {
            registers::ModbusRegisterFormat::Coil => {
                let mut data = Vec::new();
                match mreq.parse_bool(&response, &mut data) {
                    Ok(()) => {
                        parsed_value = Ok(match data[0] { 
                            true => 1,
                            false => 0
                        } as f64);
                    }
                    Err(e) => {
                        parsed_value = Err(format!("{:?}", e));
                    },
                }
            }
            registers::ModbusRegisterFormat::Int32 => {
                let mut data = Vec::new();
                match mreq.parse_i16(&response, &mut data) {
                    Err(e) => {
                        parsed_value = Err(format!("{:?}", e));
                    }
                    Ok(()) => {
                        let v = ((data[0] as i32) << 16) | (data[1] as u16 as i32);
                        parsed_value = Ok(v as f64);
                    }
                }
            },
            registers::ModbusRegisterFormat::Int16 => {
                let mut data = Vec::new();
                match mreq.parse_i16(&response, &mut data) {
                    Err(e) => {
                        parsed_value = Err(format!("{:?}", e));
                    }
                    Ok(()) => {
                        parsed_value = Ok(data[0] as f64);
                    }
                }
            },
            registers::ModbusRegisterFormat::UInt32 => {
                let mut data = Vec::new();
                match mreq.parse_u16(&response, &mut data) {
                    Err(e) => {
                        parsed_value = Err(format!("{:?}", e));
                    }
                    Ok(()) => {
                        let v = ((data[0] as u32) << 16) | (data[1] as u32);
                        parsed_value = Ok(v as f64);
                    }
                }
            },
            registers::ModbusRegisterFormat::UInt16 => {
                let mut data = Vec::new();
                match mreq.parse_u16(&response, &mut data) {
                    Err(e) => {
                        parsed_value = Err(format!("{:?}", e));
                    }
                    Ok(()) => {
                        parsed_value = Ok(data[0] as f64);
                    }
                }
            },
            registers::ModbusRegisterFormat::Float32 => {
                let mut data = Vec::new();
                match mreq.parse_u16(&response, &mut data) {
                    Err(e) => {
                        parsed_value = Err(format!("{:?}", e));
                    }
                    Ok(()) => {
                        // IEEE 754 float32: combine two u16 registers (big-endian)
                        let bits = ((data[0] as u32) << 16) | (data[1] as u32);
                        let v = f32::from_bits(bits);
                        parsed_value = Ok(v as f64);
                    }
                }
            },
            registers::ModbusRegisterFormat::String => {
                let mut data = Vec::new();
                match mreq.parse_u16(&response, &mut data) {
                    Err(e) => {
                        parsed_value = Err(format!("{:?}", e));
                    }
                    Ok(()) => {
                        // Convert u16 registers to string (2 chars per register, big-endian)
                        let mut chars = Vec::new();
                        for word in &data {
                            let hi = ((*word >> 8) & 0xFF) as u8;
                            let lo = (*word & 0xFF) as u8;
                            if hi != 0 { chars.push(hi); }
                            if lo != 0 { chars.push(lo); }
                        }
                        string_value = Some(String::from_utf8_lossy(&chars).trim_end_matches('\0').trim().to_string());
                        parsed_value = Ok(0.0); // Placeholder, we use string_value
                    }
                }
            },
            registers::ModbusRegisterFormat::SunSSF => {
                // SunSpec scale factor: int16 representing power of 10 exponent
                let mut data = Vec::new();
                match mreq.parse_i16(&response, &mut data) {
                    Err(e) => {
                        parsed_value = Err(format!("{:?}", e));
                    }
                    Ok(()) => {
                        let sf = data[0];
                        scale_factors.insert(reg.name.clone(), sf);
                        debug!("Hub {} Device {}: Stored scale factor {} = {}",
                               hub_name, device.config.name, reg.name, sf);
                        parsed_value = Ok(sf as f64);
                    }
                }
            },
        }

        if let Err(e) = &parsed_value {
            error!("Error getting response for register {}: {}", reg.name, e);
            continue;
        }

        // Handle string values separately
        if let Some(s) = string_value {
            meter_data.metered_values.insert(reg.name.clone(), serde_json::Value::from(s.clone()));
            let _ = context.set_value(reg.name.clone(), evalexpr::Value::String(s));
            continue;
        }

        // For SunSSF, don't add to metered_values (they're internal scale factors)
        if reg.format == registers::ModbusRegisterFormat::SunSSF {
            continue;
        }

        let raw_value = parsed_value.unwrap();

        // Apply scale factor: either from referenced SunSSF register or from static scaler
        let scaled_value = if let Some(ref sf_name) = reg.scale_factor {
            if let Some(&sf) = scale_factors.get(sf_name) {
                // SunSpec scale factor: value * 10^sf
                raw_value * 10_f64.powi(sf as i32)
            } else {
                warn!("Hub {} Device {}: Scale factor {} not found for register {}, using raw value",
                      hub_name, device.config.name, sf_name, reg.name);
                raw_value * reg.scaler as f64
            }
        } else {
            // Use static scaler
            raw_value * reg.scaler as f64
        };

        {
            let v = scaled_value;
            let mut value = serde_json::Value::from(v);

            let mut found = false;
            for mapping in reg.mappings.iter() {
                info!("mapping {:?} with {:?}", mapping.data, v);
                if mapping.data == format!("{:?}", v) {
                    value = mapping.mapping.clone();
                    found = true;
                    break;
                }
            }

            if !found {
                for mapping in reg.mappings.iter() {
                    if mapping.data == "_" {
                        value = mapping.mapping.clone();
                        break;
                    }
                }
            }

            meter_data.metered_values.insert(reg.name.clone(), value);
            let _ = context.set_value(reg.name.clone(), evalexpr::Value::Float(v as f64));
        }
    }

    // Calculate template registers if needed
    for reg in &device.registers {
        let reg = match reg {
            Register::Template(template_register) => template_register,
            Register::Modbus(_) => continue,
        };

        let value = match evalexpr::eval_float_with_context(&reg.value, &context) {
            Ok(r) => r,
            Err(e) => {
                error!("Failed to evaluate: {e:?}");
                0.0
            },
        };
        meter_data.metered_values.insert(reg.name.clone(), serde_json::Value::from(value));
    }

    let _ = hub_sender.send(Transmission::Metering(meter_data)).await;

    /* Now publish the raw data. In that mode we work as transparent bridge for the data to flow */
    let p = PublishData {
        topic: format!("energy2mqtt/raw/modbus/{}", hub_name),
        payload: serde_json::to_string(&raw_data).unwrap_or("{}".to_string()),
        qos: 1,
        retain: false,
    };

    let _ = hub_sender.send(Transmission::Publish(p)).await;

    Ok(())
}

fn get_unix_ts() -> u64 {
    return SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_secs();
}

fn get_id(protocol: String, meter_name: &String) -> String {
    return format!("{}-{}-{:?}", protocol, meter_name, get_unix_ts());
}
