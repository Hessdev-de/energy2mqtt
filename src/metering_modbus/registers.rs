use std::fs::File;
use std::io::prelude::*;
use log::{error, info};
use serde::Deserialize;
use serde_yml;

#[derive(Clone, PartialEq, Deserialize)]
pub enum ModbusRegisterType {
    Holding,
    Input,
    Coil
}
#[derive(Clone, PartialEq, Deserialize)]
pub enum ModbusRegisterFormat {
    Int16,
    Int32,
    UInt16,
    UInt32,
    Float32,
    String,
    /// SunSpec scale factor - int16 used as power of 10 exponent
    SunSSF,
    Coil,
}

#[derive(Clone, PartialEq, Deserialize)]
pub enum Endianess {
    Big,
    Little,
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
fn default_platform() -> String {
    "sensor".to_string()
}
fn default_precision() -> u32 {
    3
}
fn default_endianess() -> Endianess {
    Endianess::Big
}

#[derive(Clone, PartialEq, Deserialize)]
pub struct ModbusRegister {
    pub name: String,
    pub input_type: ModbusRegisterType,
    pub register: u16,
    pub length: u16,
    pub format: ModbusRegisterFormat,
    #[serde(default="default_endianess")]
    pub endianess: Endianess,
    #[serde(default="default_scaler")]
    pub scaler: f32,
    #[serde(default="default_precision")]
    pub precision: u32,
    /// Reference to a SunSpec scale factor register name (e.g., "A_SF")
    /// The value from that register will be used as 10^x multiplier
    #[serde(default)]
    pub scale_factor: Option<String>,
    #[serde(default="default_none_str")]
    pub unit_of_measurement: String,
    #[serde(default="default_none_str")]
    pub device_class: String,
    #[serde(default="default_none_str")]
    pub state_class: String,
    #[serde(default="default_platform")]
    pub platform: String,
    #[serde(default)]
    pub mappings: Vec<Mapping>,
    #[serde(default)]
    pub command_template: String,
    #[serde(default)]
    pub value_template: String,
    #[serde(default)]
    pub options: Vec<String>,

    pub min: Option<u32>,
    pub max: Option<u32>,
    pub step: Option<i32>,
}

#[derive(Deserialize, Clone)]
pub struct TemplateRegister {
    pub name: String,
    pub value: String,
    #[serde(default="default_precision")]
    pub precision: u32,
    pub unit_of_measurement: String,
    pub device_class: String,
    pub state_class: String,
    #[serde(default="default_platform")]
    pub platform: String,
    #[serde(default)]
    pub value_template: String,
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
    // Model can include subdirectory path, e.g., "sunspec/sunspec_inverter_3p"
    // Search order:
    // 1. config/modbus/{model}.yaml (user override)
    // 2. defs/modbus/{model}.yaml (built-in)

    let search_paths = [
        format!("config/modbus/{}.yaml", model),
        format!("defs/modbus/{}.yaml", model),
    ];

    let mut file = None;
    let mut used_path = String::new();

    for path in &search_paths {
        if let Ok(f) = File::open(path) {
            file = Some(f);
            used_path = path.clone();
            break;
        }
    }

    match file {
        Some(mut f) => {
            if used_path.starts_with("config/") {
                info!("Using user provided definition of {model} from {used_path}");
            } else {
                info!("Loading definition of {model} from {used_path}");
            }
            parse_registers(&mut f)
        }
        None => {
            error!("Meter definition of {model} not found in any of: {:?}", search_paths);
            (Vec::new(), "".to_string(), "".to_string())
        }
    }
}