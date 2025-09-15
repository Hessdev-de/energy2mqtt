use serde::{Deserialize, Serialize};



#[derive(Serialize)]
pub struct HaDevice {
    ids: String,
    name: String,
    manufacturer: String,
    model: String,
}
#[derive(Serialize)]
pub struct HaOrigin {
    pub name: String,
    pub sw_version: String,
    pub support_url: String,
}

fn is_none_str(value: &String) -> bool {
    if value.is_empty() || value == "NONE" {
        return true;
    }
    return false;
}

#[derive(Serialize, PartialEq, Deserialize, Clone, Default)]
pub enum HAPlatform {
    #[default]
    Sensor,
    BinarySensor,
    Button,
}

impl HAPlatform {
    pub fn to_string(&self) -> String {
        match self {
            HAPlatform::Sensor => "sensor".to_string(),
            HAPlatform::BinarySensor => "binary_sensor".to_string(),
            HAPlatform::Button => "button".to_string(),
        }
    }
}

#[derive(Serialize)]
pub struct HaComponent {
    pub p: String,
    pub name: String,
    #[serde(skip_serializing_if = "is_none_str")]
    pub device_class: String,
    #[serde(skip_serializing_if = "is_none_str")]
    pub unit_of_measurement: String,
    pub value_template: String,
    pub unique_id: String,
    pub object_id: String,
    pub via_device: String,
    #[serde(skip_serializing_if = "is_none_str")]
    pub state_class: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub payload_on: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub payload_off: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub payload_press: Option<String>
}

impl HaComponent {
    pub fn new(platform: HAPlatform, device: String, dclass: String, uof: String, proto: String, name: String, state_class: String) -> Self {

        let safe_name= name.clone().replace(" ", "_");

        let mut p_off = None;
        let mut p_on = None;
        let mut p_press : Option<String> = None;

        if platform == HAPlatform::BinarySensor {
            p_off = Some(false);
            p_on = Some(true);
        }

        if dclass == "restart" {
            p_press = Some("restart".to_string());
        }

        return HaComponent {
            p: platform.to_string(),
            name: name.clone(),
            device_class: dclass,
            unit_of_measurement: uof,
            value_template: format!("{{{{ value_json.{name} }}}}"),
            unique_id: format!("e2m_{proto}_{device}_{safe_name}").to_lowercase(),
            object_id: format!("{device}_{safe_name}").to_lowercase(),
            state_class: state_class,
            payload_on: p_on,
            payload_off: p_off,
            payload_press: p_press,
            via_device: "e2m_management".to_string(),
         }
    }

    pub fn new_energy(device: String, uof: String, proto: String, name: String, json_key: String) -> Self {
        let safe_name= name.clone().replace(" ", "_");
        return HaComponent {
            p: "sensor".to_string(),
            name: name,
            device_class: "energy".to_string(),
            unit_of_measurement: uof,
            value_template: format!("{{{{ value_json.{json_key} }}}}"),
            unique_id: format!("e2m_{proto}_{device}_{safe_name}").to_lowercase(),
            object_id: format!("{device}_{safe_name}").to_lowercase(),
            state_class: "total_increasing".to_string(),
            payload_on: None,
            payload_off: None,
            payload_press: None,
            via_device: "e2m_management".to_string(),
         }
    }

    pub fn new_freq(device: String, proto: String, name: String, json_key: String) -> Self {
        let safe_name= name.clone().replace(" ", "_");
        return HaComponent {
            p: "sensor".to_string(),
            name: name,
            device_class: "frequency".to_string(),
            unit_of_measurement: "Hz".to_string(),
            value_template: format!("{{{{ value_json.{json_key} }}}}"),
            unique_id: format!("e2m_{proto}_{device}_{safe_name}").to_lowercase(),
            object_id: format!("{device}_{safe_name}").to_lowercase(),
            state_class: "measurement".to_string(),
            payload_on: None,
            payload_off: None,
            payload_press: None,
            via_device: "e2m_management".to_string(),
         }
    }

    pub fn new_current(device: String, proto: String, name: String, json_key: String) -> Self {
        let safe_name= name.clone().replace(" ", "_");
        return HaComponent {
            p: "sensor".to_string(),
            name: name,
            device_class: "current".to_string(),
            unit_of_measurement: "A".to_string(),
            value_template: format!("{{{{ value_json.{json_key} }}}}"),
            unique_id: format!("e2m_{proto}_{device}_{safe_name}").to_lowercase(),
            object_id: format!("{device}_{safe_name}").to_lowercase(),
            state_class: "measurement".to_string(),
            payload_on: None,
            payload_off: None,
            payload_press: None,
            via_device: "e2m_management".to_string(),
         }
    }

    pub fn new_power(device: String, proto: String, name: String, json_key: String) -> Self {
        let safe_name= name.clone().replace(" ", "_");
        return HaComponent {
            p: "sensor".to_string(),
            name: name,
            device_class: "power".to_string(),
            unit_of_measurement: "W".to_string(),
            value_template: format!("{{{{ value_json.{json_key} }}}}"),
            unique_id: format!("e2m_{proto}_{device}_{safe_name}").to_lowercase(),
            object_id: format!("{device}_{safe_name}").to_lowercase(),
            state_class: "measurement".to_string(),
            payload_on: None,
            payload_off: None,
            payload_press: None,
            via_device: "e2m_management".to_string(),
         }
    }

    pub fn new_voltage(device: String, proto: String, name: String, json_key: String) -> Self {
        let safe_name= name.clone().replace(" ", "_");
        return HaComponent {
            p: "sensor".to_string(),
            name: name,
            device_class: "voltage".to_string(),
            unit_of_measurement: "V".to_string(),
            value_template: format!("{{{{ value_json.{json_key} }}}}"),
            unique_id: format!("e2m_{proto}_{device}_{safe_name}").to_lowercase(),
            object_id: format!("{device}_{safe_name}",).to_lowercase(),
            state_class: "measurement".to_string(),
            payload_on: None,
            payload_off: None,
            payload_press: None,
            via_device: "e2m_management".to_string(),
         }
    }

    pub fn new_percent(device: String, dclass: String, proto: String, name: String, json_key: String) -> Self {
        let safe_name= name.clone().replace(" ", "_");
        return HaComponent {
            p: "sensor".to_string(),
            name: name,
            device_class: dclass,
            unit_of_measurement: "%".to_string(),
            value_template: format!("{{{{ value_json.{json_key} }}}}"),
            unique_id: format!("e2m_{proto}_{device}_{safe_name}").to_lowercase(),
            object_id: format!("{device}_{safe_name}").to_lowercase(),
            state_class: "measurement".to_string(),
            payload_on: None,
            payload_off: None,
            payload_press: None,
            via_device: "e2m_management".to_string(),
         }
    }

    pub fn set_via(&mut self, via: String) {
        self.via_device = via;
    }

    pub fn new_full_sensor(name: String, device_class: String, unit: String, json_key: String, object_id: String, unique_id: String) -> Self {
        return HaComponent {
            p: "sensor".to_string(),
            name: name,
            device_class: device_class,
            unit_of_measurement: unit,
            value_template: format!("{{{{ value_json.{json_key} }}}}"),
            unique_id: unique_id,
            object_id: object_id,
            state_class: "measurement".to_string(),
            payload_on: None,
            payload_off: None,
            payload_press: None,
            via_device: "e2m_management".to_string(),
         }
    }
}

#[derive(Serialize)]
pub struct HaDiscover {
    pub dev: HaDevice,
    pub o: HaOrigin,
    pub cmps: serde_json::Map<String, serde_json::Value>,
    pub state_topic: String,
    pub qos: u32,
    #[serde(skip_serializing)]
    pub discover_topic: String,
}

impl HaDiscover {
    pub fn new(name: String, manu: String, model: String, proto: String) -> Self {
        return HaDiscover {
            discover_topic: format!("homeassistant/device/e2m_{}-{}/config", proto.clone(), name.clone()),
            dev: HaDevice {
                ids: format!("e2m_{}_{}", proto.clone(), name.clone()),
                name: name.clone(),
                manufacturer: manu,
                model: model,
            }, 
            o: HaOrigin {
                name: "energy2mqtt".to_string(),
                sw_version: "0.1.1".to_string(),
                support_url: "https://energy2mqtt.org".to_string()
            },
            cmps: serde_json::Map::new(),
            state_topic: format!("energy2mqtt/devs/{}/{}", proto, name),
            qos: 2
        }
    }
    pub fn new_with_topic_from_name(name: String, manu: String, model: String, proto: String, topic: String) -> Self {
        return HaDiscover {
            discover_topic: format!("homeassistant/device/e2m_{}-{}/config", proto.clone(), name.clone()),
            dev: HaDevice {
                ids: format!("e2m_{}_{}", proto.clone(), name.clone()),
                name: name,
                manufacturer: manu,
                model: model
            }, 
            o: HaOrigin {
                name: "energy2mqtt".to_string(),
                sw_version: "0.1.1".to_string(),
                support_url: "https://energy2mqtt.org".to_string()
            },
            cmps: serde_json::Map::new(),
            state_topic: format!("energy2mqtt/devs/{}/{}", proto, topic),
            qos: 2
        }
    }

    pub fn get_dev_id(&self) -> String {
        return self.dev.ids.clone();
    }
}
