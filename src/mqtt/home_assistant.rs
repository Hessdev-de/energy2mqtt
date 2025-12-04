use serde::Serialize;
use serde_json::{Map, Value};


pub trait HaToJSON {
    fn to_json(&self) -> Value;
}

pub fn get_state_topic(proto: &String, device: &String) -> String {
    format!("energy2mqtt/devs/{proto}/{device}")
}

pub fn get_command_topic(proto: &String, instance: &String, device: &String) -> String {
    format!("energy2mqtt/cmds/{proto}/{instance}/{device}")
}

pub fn get_dev_cmd_proto_from_topic(topic: &String) -> (String, String, String) {
    let (base, cmd) = topic.rsplit_once('/').unwrap_or_default();
    let (base, device) = base.rsplit_once('/').unwrap_or_default();
    let (_, proto) = base.rsplit_once('/').unwrap_or_default();

    return (device.to_string(), cmd.to_string(), proto.to_string());
}

/* The origin of the device to be modelled, mostly energy2mqtt */
#[derive(Serialize)]
pub struct HaOrigin2  {
    pub name: String,
    pub sw_version: String,
    pub support_url: String,
}

impl HaOrigin2 {
    pub fn new(name: String, sw_version: String, url: String) -> Self {
        HaOrigin2 {
            name,
            sw_version,
            support_url: url,
        }
    }
}

impl HaToJSON for HaOrigin2 {
    fn to_json(&self) -> Value {
        serde_json::to_value(self).unwrap_or(Map::new().into())
    }
}

pub struct HaSensor {
    proto: String,
    device: String,
    /* This information is hidden and can only be used with builder */
    definition: Map<String, Value>,
}

impl HaToJSON for HaSensor {
    fn to_json(&self) -> Value {
        self.definition.clone().into()
    }
}

impl HaSensor {
    pub fn new(proto: String, device: String, manu: Option<String>, model: Option<String>) -> Self {
        let mut definition: Map<String, Value> = Map::new();

        /* We set some defaults here, which may be overriden later on */
        let mut dev = Map::new();
        let proto = proto.replace("/", "_");
        dev.insert("ids".to_string(), format!("e2m_{proto}_{device}").into());
        dev.insert("name".to_string(), device.clone().into());
        dev.insert("via_device".to_string(), "e2m_management".into());


        dev.insert("manufacturer".to_string(), match manu {
            Some(m) => m,
            None => "Unknown".to_string(),
        }.into());

        dev.insert("model".to_string(), match model {
            Some(m) => m,
            None => "Unknown".to_string(),
        }.into());

        definition.insert("dev".to_string(), dev.into());

        definition.insert("o".to_string(), HaOrigin2::new("energy2mqtt".to_string(), 
                                "0.1.1".to_string(),
                                "https://energy2mqtt.org".to_string()).to_json());
        definition.insert("cmps".to_string(), Map::new().into());
        definition.insert("state_topic".to_string(), get_state_topic(&proto, &device).into());
        HaSensor {
            proto,
            device,
            definition
        }
    }

    pub fn add_cmp(&mut self, key: String, cmp: HaComponent2) {
        let mut m: Map<String, Value> = self.definition.clone();
        if let Some(cmps) = m["cmps"].as_object_mut() {
            cmps.insert(key.clone(), 
                        cmp
                            .key(key)
                            .proto(self.proto.clone())
                            .dev(self.device.clone())
                            .to_json());
            self.definition.insert("cmps".to_string(), serde_json::to_value(cmps).unwrap());
        } else {
            panic!("Our component definition is broken!");
        }
    }

    pub fn get_disc_topic(&self) -> String {
        /* Some protocols have sub protocols like zridh/lora or zridh/oms */
        let proto = self.proto.replace("/", "_");
        let device = self.device.clone();
        format!("homeassistant/device/e2m_{proto}_{device}/config")
    }

    /* Set parent device */
    pub fn via(mut self, via: String) -> Self {
        /* Via is set within the dev part of the object, so change it */
        let mut v = self.definition["dev"].clone();
        let dev = v.as_object_mut().unwrap();
        dev.insert("via_device".to_string(), Value::from(via));
        self.definition.insert("dev".to_string(), serde_json::to_value(dev).unwrap());
        self
    }

    /* Set the device friendly name (different from the ID) */
    pub fn device_name(mut self, name: String) -> Self {
        let mut v = self.definition["dev"].clone();
        let dev = v.as_object_mut().unwrap();
        dev.insert("name".to_string(), Value::from(name));
        self.definition.insert("dev".to_string(), serde_json::to_value(dev).unwrap());
        self
    }

    pub fn add_information(mut self, key: String, value: Value) -> Self {
        self.definition.insert(key, value);
        self
    }

}

pub struct HaComponent2 {
    defs: Map<String, Value>
}

impl  HaToJSON for HaComponent2 {
    fn to_json(&self) -> Value {
        /* Build our IDs and value templates */
        let mut m = self.defs.clone();

        let key = m["_key"].as_str().unwrap_or_default().to_string();

        if !m.contains_key("value_template") {
            m.insert("value_template".to_string(), Value::from(format!("{{{{ value_json.{} }}}}", key.clone())));
        } else {
            /* We allow ## as a replacement for the correct key and replace it, if found */
            let mut v: String = m["value_template"].as_str().unwrap().to_string();
            v = v.replace("##",  &format!("value_json.{key}"));
            m.insert("value_template".to_string(), v.into());
        }

        let proto = m["_proto"].as_str().unwrap_or_default().to_string();
        let device = m["_device"].as_str().unwrap_or_default().to_string();
    
        let safe_name= key.clone().replace(" ", "_");

        m.insert("unique_id".to_string(), format!("e2m_{proto}_{device}_{safe_name}").to_lowercase().into());
        let p = m["p"].as_str().unwrap_or_default().to_string();
        m.insert("default_entity_id".to_string(), format!("{p}.e2m_{proto}_{device}_{safe_name}").to_lowercase().into());

        /* Remove the unneeded parts of the map */
        m.remove(&"_key".to_string());
        m.remove(&"_proto".to_string());
        m.remove(&"_device".to_string());

        serde_json::to_value(m).unwrap_or(Map::new().into())
    }
}

impl HaComponent2 {
    pub fn new() -> Self {
        let mut m = Map::new();
        /* Some defaults which may or may not be changed */

        /* Most parts we are using a sensor */
        m.insert("p".to_string(), Value::from("sensor"));

        /* Most time we are using a measument as state_class*/
        m.insert("state_class".to_string(), Value::from("measurement"));

        HaComponent2 { defs: m }
    }

    /* We need to store the name of the component for the value_template later */
    pub fn key(mut self, key: String) -> Self {
        self.defs.insert("_key".to_string(), Value::from(key));
        self
    }

    /* We need to store the device identification of the whole device for later */
    pub fn dev(mut self, device: String) -> Self {
        self.defs.insert("_device".to_string(), Value::from(device));
        self
    }

    /* Proto is part of the whole device and will be set adding the component */
    pub fn proto(mut self, proto: String) -> Self {
        self.defs.insert("_proto".to_string(), Value::from(proto));
        self
    }

    /* Set the friendly name */
    pub fn name(mut self, name: String) -> Self {
        self.defs.insert("name".to_string(), Value::from(name));
        self
    }

    /* Set the platform */
    pub fn platform(mut self, platform: String) -> Self {
        self.defs.insert("p".to_string(), Value::from(platform));
        self
    }

    /* Set the state class */
    pub fn state_class(mut self, state_class: String) -> Self {
        self.defs.insert("state_class".to_string(), Value::from(state_class));
        self
    }

    /* Set the device class */
    pub fn device_class(mut self, device_class: String) -> Self {
        self.defs.insert("device_class".to_string(), Value::from(device_class));
        self
    }

    /* Set the unit of measurement */
    pub fn unit_of_measurement(mut self, uom: String) -> Self {
        self.defs.insert("unit_of_measurement".to_string(), Value::from(uom));
        self
    }

    pub fn entity_category(mut self, cat: String) -> Self {
        self.defs.insert("entity_category".to_string(), Value::from(cat));
        self
    }

    pub fn cat_diagnostic(mut self) -> Self {
        self.defs.insert("entity_category".to_string(), Value::from("diagnostic"));
        self
    }
    pub fn hidden(mut self) -> Self {
        self.defs.insert("enabled_by_default".to_string(), Value::from(false));
        self
    }

    pub fn non_numeric(mut self) -> Self {
        self.defs.remove("state_class");
        self
    }

    pub fn del_information(mut self, key: &str) -> Self {
        self.defs.remove(&key.to_string());
        self
    }

    pub fn ent_platform(mut self, plat: String) -> Self {
        self.defs.insert("p".to_string(), Value::from(plat));
        self
    }

    pub fn add_information(mut self, key: &str, value: Value) -> Self {
        self.defs.insert(key.to_string(), value);
        self
    }

}
