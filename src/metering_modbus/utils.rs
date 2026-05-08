use log::error;

use crate::metering_modbus::registers::{self, Register, Endianess};

macro_rules! handle_endianess {
    ($input:expr, $endianess:expr) => {
        if $endianess == Endianess::Little {
            $input.to_le_bytes().to_vec()
        } else {
            $input.to_be_bytes().to_vec()
        }
    };
}

pub fn get_data_vec(register_enum: &Register, payload: &String) -> Vec<u8> {
    let mut value = Vec::new();

    let mut input = payload.clone();
    /* Our input may be a float with .0 because of the way serde is used */
    if payload.contains(".") {
        let fields: Vec<&str> = payload.split(".").collect();
        input = fields[0].to_string();
    }

    if let Register::Modbus(reg) = register_enum {
        match reg.format {
            registers::ModbusRegisterFormat::Int16 => {
                let d: Result<i16, _> = input.parse();
                if let Ok(d) = d {
                    let d = (d as f32 / reg.scaler) as i16;
                    value = handle_endianess!(d, reg.endianess);
                }
            },
            registers::ModbusRegisterFormat::UInt16 => {
                let d: Result<u16, _> = input.parse();
                if let Ok(d) = d {
                    let d = (d as f32 / reg.scaler) as u16;
                    value = handle_endianess!(d, reg.endianess);
                }
            },
            registers::ModbusRegisterFormat::Int32 => {
                let d: Result<i32, _> = input.parse();
                if let Ok(d) = d {
                    let d = (d as f32 / reg.scaler) as i32;
                    value = handle_endianess!(d, reg.endianess);
                }
            },
            registers::ModbusRegisterFormat::UInt32 => {
                let d: Result<u32, _> = input.parse();
                if let Ok(d) = d {
                    let d = (d as f32 / reg.scaler) as u32;
                    value = handle_endianess!(d, reg.endianess);
                }
            },
            registers::ModbusRegisterFormat::Coil => {
                let d: Result<u8, _> = input.parse();
                if let Ok(d) = d {
                    value = handle_endianess!(d, reg.endianess);
                }
            }
            _ => {
                error!("Unable to set f32, SunSSF or String");
            }
        };
    }
    value
}

pub fn round_number(number: f64, precision: u32) -> f64 {
    let scaler = i32::pow(10, precision) as f64;
    return (number * scaler).round() / scaler;
}