use std::fs::File;
use std::io::prelude::*;
use log::{error, info};
use serde::Deserialize;
use serde_yml;

use crate::mqtt::ha_interface::HAPlatform;

#[derive(Clone, PartialEq, Deserialize)]
pub enum ModbusRegisterType {
    Holding,
    Input,
    Coil
}
#[derive(Clone, PartialEq, Deserialize)]
pub enum ModbusRegisterFormat {
    Int16,
    Int32
}

#[derive(Clone, PartialEq, Deserialize)]
pub struct Mapping { 
    pub data: String,
    pub mapping: serde_json::Value
}

fn default_scaler() -> f32 {
    1.0
}
fn default_none_str() -> String {
    "NONE".to_string()
}

#[derive(Clone, PartialEq, Deserialize)]
pub struct ModbusRegister {
    pub name: String,
    pub input_type: ModbusRegisterType,
    pub register: u16,
    pub length: u16,
    pub format: ModbusRegisterFormat,
    #[serde(default="default_scaler")]
    pub scaler: f32,
    #[serde(default="default_none_str")]
    pub unit_of_measurement: String,
    #[serde(default="default_none_str")]
    pub device_class: String,
    #[serde(default="default_none_str")]
    pub state_class: String,
    #[serde(default)]
    pub platform: HAPlatform,
    #[serde(default)]
    pub mappings: Vec<Mapping>,
}

#[derive(Deserialize, Clone)]
pub struct TemplateRegister {
    pub name: String,
    pub value: String,
    pub unit_of_measurement: String,
    pub device_class: String,
    pub state_class: String,
    #[serde(default)]
    pub platform: HAPlatform,
}

#[derive(Clone)]
pub enum Register {
    Template(TemplateRegister),
    Modbus(ModbusRegister)
}

#[derive(Deserialize)]
pub struct ModbusRegisterFile {
    manufacturer: String,
    model: String,
    registers: Vec<ModbusRegister>,
    #[serde(default)]
    templates: Vec<TemplateRegister>
}

fn parse_registers(file: &mut File)  -> (Vec<Register>, String, String) {
    let mut regs = Vec::new();

    let mut contents = String::new();
    let _ = file.read_to_string(&mut contents);

    let whole_file = match serde_yml::from_str(&contents) {
        Ok(d) => d,
        Err(e) => {
            error!("Failed to parse: {e:?}");
            ModbusRegisterFile{
                registers: Vec::new(),
                templates: Vec::new(),
                manufacturer: "fault".to_string(),
                model: "0.0.0".to_string()
            }
        },
    };
    
    if whole_file.registers.len() == 0 {
        error!("Loading the yaml description seems to be failing, because we got no registers");
    }

    for reg in whole_file.registers {
        regs.push(Register::Modbus(reg));
    }

    for temp in whole_file.templates {
        regs.push(Register::Template(temp));
    }

    return (regs, whole_file.manufacturer, whole_file.model);
}

pub fn get_registers(model: &String) -> (Vec<Register>, String, String) {

    /* user specified definitions are used first */
    let filename = format!("config/modbus/{}.yaml", model);
    let mut file = File::open(filename);
    if file.is_err() {
        let filename = format!("defs/modbus/{}.yaml", model);
        file = File::open(filename);
        if file.is_err() {
            error!("Meter definition of {model} not found");
            return (Vec::new(), "".to_string(), "".to_string());
        } else {
            info!("Loading definition of {model}");
        }
    } else {
        info!("Using user provided definition of {model}");
    }

    let mut file = file.unwrap();
    return parse_registers(&mut file);
}