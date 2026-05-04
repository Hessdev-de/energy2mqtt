use std::{collections::HashMap, time::Duration};
use log::{debug, error, info};
use rmodbus::{ModbusProto, client::ModbusRequest, guess_response_frame_len};
use tokio::{io::{AsyncReadExt, AsyncWriteExt}, net::TcpStream, time::timeout};

use crate::metering_modbus::{HubConnectionState, ModbusDevice, ModbusError, read_device_parms, registers::{ModbusRegisterType, Register}};

pub async fn set(
    socket_addr: &str,
    hub_name: &str,
    registers: &HashMap<String, Vec<u8>>,
    device: &ModbusDevice,
    proto: ModbusProto,
    //hub_sender: &Sender<Transmission>,
    conn_state: &mut HubConnectionState,
){
    // Ensure we have a connection (reuse existing or establish new)
    if conn_state.stream.is_none() {
        match read_device_parms::connect_to_hub_with_retry(
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

    // Get mutable reference to stream
    let stream = conn_state.stream.as_mut().unwrap();

    for (str_addr, value) in registers {
        
        let address: u16 = str_addr.parse().unwrap_or(0);

        for needle in &device.registers {
            
            if let super::registers::Register::Modbus(r) = needle {
                debug!("Searching with register ... {}", r.register);
                if r.register != address {
                    continue;
                }

                debug!("Got register to write ... {address}");

                let mut mreq = ModbusRequest::new(device.config.slave_id, proto);
                let value_u16 = match value.len() {
                    1 => value[0] as u16,
                    2 => u16::from_be_bytes([value[0], value[1]]),
                    _ => {
                        error!("Wrong number of bytes to write");
                        continue;
                    }
                };

                let mut request = Vec::new();

                if let Err(e) = match r.input_type {
                    ModbusRegisterType::Holding => mreq.generate_set_holding(address, value_u16, &mut request),
                    ModbusRegisterType::Coil => mreq.generate_set_coil(address, value[0], &mut request),
                    ModbusRegisterType::Input => {
                        error!("Trying to set input register on hub {} device {} address {address}", hub_name, device.config.name);
                        continue;
                    }
                } {
                    error!("Can not build request for {address} on {}: {e:?}", device.config.name);
                    continue;
                }

                match write_single_register(stream, request, proto).await {
                    Ok(_) => info!("Written register {} on Device {} of Hub {}", address, device.config.name, hub_name),
                    Err(e) => error!("Writing register {} on Device {} of Hub {} failed: {e:?}", address, device.config.name, hub_name),
                }

            }
        }
    }
}

pub async fn write_register(device: &ModbusDevice, proto: ModbusProto,  conn_state: &mut HubConnectionState, register: Register, value: Vec<u8> ) {
    if let Register::Modbus(r) = register {

        let address = r.register;

        let mut mreq = ModbusRequest::new(device.config.slave_id, proto);
        let value_u16 = match value.len() {
            1 => value[0] as u16,
            2 => u16::from_be_bytes([value[0], value[1]]),
            _ => {
                error!("Wrong number of bytes to write");
                return;
            }
        };

        let mut request = Vec::new();

        if let Err(e) = match r.input_type {
            ModbusRegisterType::Holding => mreq.generate_set_holding(address, value_u16, &mut request),
            ModbusRegisterType::Coil => mreq.generate_set_coil(address, value[0], &mut request),
            ModbusRegisterType::Input => {
                error!("Trying to set input register on device {} address {address}", device.config.name);
                return;
            }
        } {
            error!("Can not build request for {address} on {}: {e:?}", device.config.name);
            return;
        }

        if conn_state.stream.is_none() {
            error!("Write before connection");
            return;
        }
        
        /* Get our stream to write to */
        let stream = conn_state.stream.as_mut().unwrap();

        match write_single_register(stream, request, proto).await {
            Ok(_) => info!("Written register {} on Device {}", address, device.config.name),
            Err(e) => error!("Writing register {} on Device {} failed: {e:?}", address, device.config.name),
        }
    }
}

async fn write_single_register(stream: &mut TcpStream, request: Vec<u8>, proto: ModbusProto) -> Result<(), ModbusError> {

    let modbus_timeout = Duration::from_millis(1000);

    // Write request with timeout
    match timeout(modbus_timeout, stream.write_all(&request)).await {
        Ok(Ok(_)) => {}
        Ok(Err(e)) => {
            return Err(ModbusError::WriteFailed(format!(
                "Failed to write request {request:?}, {e}"
            )));
        }
        Err(_) => {
            return Err(ModbusError::WriteTimeout(modbus_timeout.as_secs()));
        }
    }

    // Read response header with timeout
    let mut buf = [0u8; 6];
    let bytes_read = match timeout(modbus_timeout, stream.read(&mut buf)).await {
        Ok(Ok(n)) => n,
        Ok(Err(e)) => {
            return Err(ModbusError::IoError(e));
        }
        Err(_) => {
            return Err(ModbusError::ReadTimeout(modbus_timeout.as_secs()));
        }
    };

    if bytes_read == 0 {
        return Err(ModbusError::ConnectionClosed);
    }

    let mut response = Vec::new();
    response.extend_from_slice(&buf[..bytes_read]);

    let len = guess_response_frame_len(&buf, proto)
        .map_err(|e| ModbusError::ProtocolError(format!(
            "Failed to determine response length for register{e:?}"
        )))?;

    if len as usize > bytes_read {
        let mut rest = vec![0u8; len as usize - bytes_read];

        // Read rest of response with timeout
        let rest_bytes = match timeout(modbus_timeout, stream.read(&mut rest)).await {
            Ok(Ok(n)) => n,
            Ok(Err(e)) => {
                return Err(ModbusError::IoError(e));
            }
            Err(_) => {
                return Err(ModbusError::ReadTimeout(modbus_timeout.as_secs()));
            }
        };

        if rest_bytes == 0 {
            return Err(ModbusError::ConnectionClosed);
        }

        response.extend(&rest[..rest_bytes]);
    }

    Ok(())
}