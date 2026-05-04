
use evalexpr::{ContextWithMutableVariables, DefaultNumericTypes, HashMapContext};
use log::{debug, error, info, warn};
use rmodbus::{client::ModbusRequest, guess_response_frame_len, ModbusProto};
use crate::{metering_modbus::{HubConnectionState, ModbusDevice, ModbusError, registers}, mqtt::{PublishData, Transmission}};
use serde::Serialize;
use tokio::{io::{AsyncReadExt, AsyncWriteExt}, net::TcpStream, sync::mpsc::Sender, time::timeout};
use std::collections::HashMap;
use std::time::Duration;
use crate::{metering_modbus::registers::Register, models::DeviceProtocol, MeteringData};


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
pub async fn read_hub_devices(
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
pub async fn connect_to_hub_with_retry(
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
pub async fn read_device_registers(
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
    meter_data.id = crate::get_id("modbus".to_string(), &device.config.name);
    meter_data.transmission_time = crate::get_unix_ts();
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
                        if data.len() < 2 {
                            error!("Register {} is malformed, length is less then INT32 type", reg.name);
                            parsed_value = Err(format!("Register {} is malformed, length is less then INT32", reg.name))
                        } else {
                            let v = ((data[0] as i32) << 16) | (data[1] as u16 as i32);
                            parsed_value = Ok(v as f64);
                        }
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
                        if data.len() < 2 {
                            error!("Register {} is malformed, length is less then UInt32 type", reg.name);
                            parsed_value = Err(format!("Register {} is malformed, length is less then UInt32f", reg.name))
                        } else {
                            let v = ((data[0] as u32) << 16) | (data[1] as u32);
                            parsed_value = Ok(v as f64);
                        }
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
                        if data.len() < 2 {
                            error!("Register {} is malformed, length is less then Float32 type", reg.name);
                            parsed_value = Err(format!("Register {} is malformed, length is less then Float32", reg.name))
                        } else {
                            // IEEE 754 float32: combine two u16 registers (big-endian)
                            let bits = ((data[0] as u32) << 16) | (data[1] as u32);
                            let v = f32::from_bits(bits);
                            parsed_value = Ok(v as f64);
                        }
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
        let _ = context.set_value(reg.name.clone(), evalexpr::Value::Float(value as f64));
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
