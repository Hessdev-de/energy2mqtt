use log::error;

use crate::metering_modbus::registers::{self, Register};

pub fn get_data_vec(register_enum: &Register, payload: &String) -> Vec<u8> {

    let mut value = Vec::new();

    if let Register::Modbus(reg) = register_enum { 
        match reg.format {
            registers::ModbusRegisterFormat::Int16 => {
                let d: Result<i16, _> = payload.parse();
                if let Ok(d) = d {
                    value = d.to_be_bytes().to_vec();
                }
            },
            registers::ModbusRegisterFormat::UInt16 => {
                let d: Result<u16, _> = payload.parse();
                if let Ok(d) = d {
                    value = d.to_be_bytes().to_vec();
                }
            },
            registers::ModbusRegisterFormat::Int32 => {
                let d: Result<i32, _> = payload.parse();
                if let Ok(d) = d {
                    value = d.to_be_bytes().to_vec();
                }
            },
            registers::ModbusRegisterFormat::UInt32 => {
                let d: Result<u32, _> = payload.parse();
                if let Ok(d) = d {
                    value = d.to_be_bytes().to_vec();
                }
            },
            registers::ModbusRegisterFormat::Coil => {
                let d: Result<u8, _> = payload.parse();
                if let Ok(d) = d {
                    value = d.to_be_bytes().to_vec();
                }
            }
            _ => {
                error!("Unable to set f32, SunSSF or String");
            }
        };
    }
    value
}