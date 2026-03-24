
use serde_json::Value;
use tokio::sync::mpsc::Sender;
use crate::{metering_modbus::registers::{self, Register}, mqtt::{PublishData, SubscribeData, Transmission, home_assistant::{HaComponent2, HaSensor}}};

pub async fn get_cmp_from_reg(reg: Register, discover: &mut HaSensor,
                        sender: &Sender<(String, String)>, hub_sender: &Sender<Transmission>,
                        hub_name: &String, device_name: &String) {

    let (platform, name, device_class,
        unit_of_measurement, state_class) = match reg.clone() {
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

    // Build component using the new HaComponent2 builder
    let mut cmp = HaComponent2::new()
        .name(name.clone())
        .platform(platform.to_string());

    // Only add device_class if it's not NONE
    if !device_class.is_empty() && device_class != "NONE" {
        /* TODO: Add the device class actions like valve_close, we got that for LoRaWAN */
        cmp = cmp.device_class(device_class);
    }

    // Only add unit_of_measurement if it's not NONE
    if !unit_of_measurement.is_empty() && unit_of_measurement != "NONE" {
        cmp = cmp.unit_of_measurement(unit_of_measurement);
    }

    // Only add state_class if it's not NONE
    if !state_class.is_empty() && state_class != "NONE" {
        cmp = cmp.state_class(state_class);
    } else {
        // Remove default state_class if it shouldn't be set
        cmp = cmp.non_numeric();
    }

    /* Some functions are only available to "real" registers */
    if let registers::Register::Modbus(r) = reg {

        /* We only run with Holding and Coil because all others can not be written */
        if r.input_type == registers::ModbusRegisterType::Holding ||
            r.input_type == registers::ModbusRegisterType::Coil {

            let topic= format!("energy2mqtt/cmds/modbus/{}/{}/{}", hub_name, device_name, name);

            match r.device_class.as_str() {
                "number" | "switch" => {
                    cmp = cmp.add_information("command_topic", Value::from(topic.clone()));
                    if !r.command_template.is_empty() {
                        cmp = cmp.add_information("command_template", r.command_template.clone().into());
                    }
                },
                _ => { }
            }

            let _ = hub_sender.send(Transmission::Subscribe(SubscribeData { topic, sender: sender.clone() })).await;

            /* Make sure to register the correct values */
        }


    }

    /* Add our device */
    discover.add_cmp(name.clone(), cmp);
}