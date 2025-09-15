use std::time::{Duration, SystemTime};
use crate::{config::{ConfigBases, ConfigChange, ConfigOperation, ModbusConfig, ModbusDeviceConfig, ModbusHubConfig, ModbusProtoConfig}, metering_modbus::registers::Register, models::DeviceProtocol, mqtt::{ha_interface::{HaComponent, HaDiscover}, Transmission, publish_protocol_count}, MeteringData, CONFIG};
use evalexpr::{ContextWithMutableVariables, DefaultNumericTypes, HashMapContext};
use log::{debug, error, info, warn};
use rmodbus::{client::ModbusRequest, guess_response_frame_len, ModbusProto};
use tokio::{io::{AsyncReadExt, AsyncWriteExt}, net::TcpStream, sync::mpsc::Sender, task::JoinHandle};
pub mod registers;


pub struct ModbusManger {
    sender: Sender<Transmission>,
    config_change: tokio::sync::broadcast::Receiver<ConfigChange>,
    threads: Vec<JoinHandle<()>>,
    config: ModbusConfig,
}

#[derive(Clone)]
pub struct ModbusDevice {
    config: ModbusDeviceConfig,
    waits_till_read: u32,
    cur_waits: u32,
    registers: Vec<registers::Register>
}

#[derive(Clone)]
pub struct ModbusHub
{
    config: ModbusHubConfig,
    devices: Vec<ModbusDevice>,
}

impl ModbusManger
{
    pub fn new(sender: Sender<Transmission>, ) -> Self {
        let config: ModbusConfig = crate::get_config_or_panic!("modbus", ConfigBases::Modbus);

        return ModbusManger {
            sender: sender,
            config_change: CONFIG.read().unwrap().get_change_receiver(),
            threads: Vec::new(),
            config: config,
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
                            };
                            devs.push(d);

                            /* Register with Home Assistant */
                            let mut discover =  HaDiscover::new(dev.name.clone(), manu, model, format!("{:?}", DeviceProtocol::ModbusTCP));
                            for reg in r {
                            
                                let (platform,name, device_class, unit_of_measurement,  state_class) = match reg {
                                    registers::Register::Template(register) => (
                                        register.platform,
                                        register.name,
                                        register.device_class,
                                        register.unit_of_measurement,
                                        register.state_class,
                                    ),
                                    registers::Register::Modbus(register) => (
                                        register.platform,
                                        register.name,
                                        register.device_class,
                                        register.unit_of_measurement,
                                        register.state_class,
                                    ),
                                };

                                let cmp = HaComponent::new(
                                    platform,
                                    dev.name.clone(),
                                    device_class.clone(),
                                    unit_of_measurement.clone(),
                                    format!("{:?}", DeviceProtocol::ModbusTCP),
                                    name.clone(),
                                    state_class.clone(),
                                );

                                discover.cmps.insert(name.clone(),serde_json::to_value(cmp).unwrap());
                            }
                            let _ = hub_sender.send(Transmission::AutoDiscovery(discover)).await;
                        }
                        devs
                    }
                };
              
                /* Find the sleeptime of this hub, do not use a too small value as it may halt the application  */
                let mut hub_inveral_sec: u32 = 60;
                for device in hub.devices.iter() {
                    hub_inveral_sec = std::cmp::min(hub_inveral_sec, device.config.read_interval);
                }

                /* No check again to round the read intervals */
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

                
                let join: JoinHandle<()> = tokio::spawn(async move {
                    let hub_delay = Duration::from_secs(hub_inveral_sec as u64);
                    let socket_addr = format!("{}:{}", hub.config.host, hub.config.port);

                    let mut proto = ModbusProto::TcpUdp;
                    /* if we use RTUoverTCP we need to add all of those fancy CRC stuff */
                    if hub.config.proto == ModbusProtoConfig::RTUoverTCP {
                        proto = ModbusProto::Rtu;
                    }

                    loop {

                        /* Now sleep for one tick of hub_inveral_sec */
                        tokio::time::sleep(hub_delay).await;
    
                        for device in hub.devices.iter_mut() {
                            device.cur_waits += 1;
    
                            if device.cur_waits == device.waits_till_read {
                                debug!("Hub {} Device {} start reading", hub.config.name, device.config.name);
                                device.cur_waits = 0;
                                
                                if let Err(e) = read_device_with_retry(&socket_addr, device, &hub.config.name, proto, &hub_sender).await {
                                    error!("Failed to read device {} after retries: {:?}", device.config.name, e);
                                }
                                
                                debug!("Hub {} Device {} done reading", hub.config.name, device.config.name);
                            }
                        }
                    }
                });
                
                self.threads.push(join);
            } /* loop per config hub */

            // Publish device count to MQTT
            publish_protocol_count(&self.sender, "modbus", device_count).await;
            
            info!("Modbus activated with {} hubs and {} devices, waiting for the rest of the system to become ready", self.config.hubs.len(), device_count);
            
            debug!("Now waiting for futher config changes");
            loop {
                let change = self.config_change.recv().await.unwrap();
                if change.base == "modbus" {
                    break;
                }
            }

            /* We are waken up because some of our config changed so stop the threads and start over */
            info!("Modbus is stopping threads");
            for thread in self.threads.iter() {
                thread.abort();
            }

            self.threads.clear();
        }

    }
}

async fn read_device_with_retry(
    socket_addr: &str,
    device: &mut ModbusDevice,
    hub_name: &str,
    proto: ModbusProto,
    hub_sender: &Sender<Transmission>
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    const MAX_RETRIES: u32 = 3;
    let mut retries = 0;
    
    loop {
        match read_device_registers(socket_addr, device, hub_name, proto, hub_sender).await {
            Ok(_) => return Ok(()),
            Err(e) if retries < MAX_RETRIES => {
                warn!("Connection error for device {}, retrying ({}/{}): {:?}", 
                      device.config.name, retries + 1, MAX_RETRIES, e);
                retries += 1;
                tokio::time::sleep(Duration::from_secs(1)).await;
                continue;
            }
            Err(e) => return Err(e),
        }
    }
}

async fn read_device_registers(
    socket_addr: &str,
    device: &mut ModbusDevice,
    hub_name: &str,
    proto: ModbusProto,
    hub_sender: &Sender<Transmission>
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Establish fresh connection for each device read
    let stream = TcpStream::connect(socket_addr).await
        .map_err(|e| format!("Failed to connect to {}: {}", socket_addr, e))?;
    let mut stream = stream;
    let _ = stream.set_nodelay(true);
    
    let mut meter_data = MeteringData::new().unwrap();
    meter_data.meter_name = device.config.name.clone();
    meter_data.protocol = DeviceProtocol::ModbusTCP;
    meter_data.id = get_id("modbus".to_string(), &device.config.name);
    meter_data.transmission_time = get_unix_ts();
    meter_data.metered_time = meter_data.transmission_time;

    let mut context = HashMapContext::<DefaultNumericTypes>::new();

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
        
        // Write request with proper error handling
        stream.write_all(&request).await
            .map_err(|e| format!("Failed to write request for register {}: {}", reg.name, e))?;
       
        // Read response with proper error handling
        let mut buf = [0u8; 6];
        let bytes_read = stream.read(&mut buf).await
            .map_err(|e| format!("Failed to read response header for register {}: {}", reg.name, e))?;
        
        if bytes_read == 0 {
            return Err(format!("Connection closed while reading register {}", reg.name).into());
        }

        let mut response = Vec::new();
        response.extend_from_slice(&buf[..bytes_read]);

        let len = guess_response_frame_len(&buf, proto)
            .map_err(|e| format!("Failed to determine response length for register {}: {:?}", reg.name, e))?;
        
        if len as usize > bytes_read {
            let mut rest = vec![0u8; len as usize - bytes_read];
            let rest_bytes = stream.read(&mut rest).await
                .map_err(|e| format!("Failed to read response body for register {}: {}", reg.name, e))?;
            
            if rest_bytes == 0 {
                return Err(format!("Connection closed while reading register {} body", reg.name).into());
            }
            
            response.extend(&rest[..rest_bytes]);
        }

        // Process the response
        let mut v: u32 = 0;
        let ok: bool;

        match reg.format {
            registers::ModbusRegisterFormat::Int32 => { 
                let mut data = Vec::new();
                let d = mreq.parse_u16(&response, &mut data);
                match d {
                    Err(e) => { 
                        error!("Error getting response for register {}: {:?}", reg.name, e); 
                        ok = false;
                    }
                    Ok(()) => {
                        v = u32::from(data[0]) << 16 | u32::from(data[1]);
                        ok = true;
                    }
                }    
            },
            registers::ModbusRegisterFormat::Int16 => { 
                let mut data = Vec::new();
                let d = mreq.parse_u16(&response, &mut data);
                match d {
                    Err(e) => { 
                        error!("Error getting response for register {}: {:?}", reg.name, e); 
                        ok = false;
                    }
                    Ok(()) => {
                        v = u32::from(data[0]);
                        ok = true;
                    }
                }    
            },
        }

        if ok {
            let v = (v as f32 * reg.scaler).round();
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
    Ok(())
}

fn get_unix_ts() -> u64 {
    return SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_secs();
}

fn get_id(protocol: String, meter_name: &String) -> String {
    return format!("{}-{}-{:?}", protocol, meter_name, get_unix_ts());
}