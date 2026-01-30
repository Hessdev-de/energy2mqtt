use serde::Serialize;
use serde_json::{Map, Value};


pub trait HaToJSON {
    fn to_json(&self) -> Value;
}

/// Convert a sensor key to a hierarchical topic path
/// Examples:
/// - "voltage_l1" -> "voltage/l1"
/// - "power_l2" -> "power/l2"
/// - "cache_misses" -> "cache_misses"
/// - "energy_all" -> "energy/all"
fn key_to_topic_path(key: &str) -> String {
    // Split on common suffixes that should become path segments
    // Phase identifiers: l1, l2, l3, all, avg, sum, total
    let suffixes = ["_l1", "_l2", "_l3", "_all", "_avg", "_sum", "_total"];

    let mut result = key.to_string();
    for suffix in suffixes {
        if result.ends_with(suffix) {
            result = result.replacen(suffix, &suffix.replace("_", "/"), 1);
            break;
        }
    }

    result
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
#[derive(Serialize, Clone)]
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

/// Individual entity discovery message
/// Sent to homeassistant/{platform}/{unique_id}/config
#[derive(Clone)]
pub struct HaEntityDiscovery {
    pub topic: String,
    pub payload: Value,
}

/// Device info shared by all entities
#[derive(Clone)]
pub struct HaDeviceInfo {
    pub ids: String,
    pub name: String,
    pub manufacturer: String,
    pub model: String,
    pub via_device: String,
}

impl HaDeviceInfo {
    pub fn to_json(&self) -> Value {
        let mut dev = Map::new();
        dev.insert("ids".to_string(), Value::from(self.ids.clone()));
        dev.insert("name".to_string(), Value::from(self.name.clone()));
        dev.insert("manufacturer".to_string(), Value::from(self.manufacturer.clone()));
        dev.insert("model".to_string(), Value::from(self.model.clone()));
        dev.insert("via_device".to_string(), Value::from(self.via_device.clone()));
        Value::Object(dev)
    }
}

pub struct HaSensor {
    proto: String,
    device: String,
    device_info: HaDeviceInfo,
    origin: HaOrigin2,
    state_topic: String,
    components: Vec<(String, HaComponent2)>,
}

impl HaToJSON for HaSensor {
    fn to_json(&self) -> Value {
        // Legacy: build combined discovery (may be too large for MQTT)
        let mut definition: Map<String, Value> = Map::new();
        definition.insert("dev".to_string(), self.device_info.to_json());
        definition.insert("o".to_string(), self.origin.to_json());
        definition.insert("state_topic".to_string(), Value::from(self.state_topic.clone()));

        let mut cmps = Map::new();
        for (key, cmp) in &self.components {
            cmps.insert(key.clone(),
                cmp.clone()
                    .key(key.clone())
                    .proto(self.proto.clone())
                    .dev(self.device.clone())
                    .to_json());
        }
        definition.insert("cmps".to_string(), Value::Object(cmps));

        Value::Object(definition)
    }
}

impl HaSensor {
    pub fn new(proto: String, device: String, manu: Option<String>, model: Option<String>) -> Self {
        let proto = proto.replace("/", "_");

        let device_info = HaDeviceInfo {
            ids: format!("e2m_{proto}_{device}"),
            name: device.clone(),
            manufacturer: manu.unwrap_or_else(|| "Unknown".to_string()),
            model: model.unwrap_or_else(|| "Unknown".to_string()),
            via_device: "e2m_management".to_string(),
        };

        let origin = HaOrigin2::new(
            "energy2mqtt".to_string(),
            "0.1.1".to_string(),
            "https://energy2mqtt.org".to_string()
        );

        let state_topic = get_state_topic(&proto, &device);

        HaSensor {
            proto,
            device,
            device_info,
            origin,
            state_topic,
            components: Vec::new(),
        }
    }

    pub fn add_cmp(&mut self, key: String, cmp: HaComponent2) {
        self.components.push((key, cmp));
    }

    /// Get legacy combined discovery topic (for backwards compatibility)
    pub fn get_disc_topic(&self) -> String {
        /*let proto = self.proto.replace("/", "_");
        let device = self.device.clone();
        format!("homeassistant/device/e2m_{proto}_{device}/config")
        */
        self.state_topic.clone()
    }

    /// Generate individual discovery messages for each entity
    /// This is the new approach that avoids MQTT message size limits
    pub fn get_entity_discoveries(&self) -> Vec<HaEntityDiscovery> {
        let mut discoveries = Vec::new();

        for (key, cmp) in &self.components {
            let built_cmp = cmp.clone()
                .key(key.clone())
                .proto(self.proto.clone())
                .dev(self.device.clone());

            // Build the individual entity payload
            let mut payload = built_cmp.to_json_map();

            // Add device info to link this entity to the device
            payload.insert("device".to_string(), self.device_info.to_json());

            // Add origin info
            payload.insert("origin".to_string(), self.origin.to_json());

            // Add state topic
            payload.insert("state_topic".to_string(), Value::from(self.state_topic.clone()));

            // Add availability topic for online/offline status
            payload.insert("availability_topic".to_string(), Value::from("energy2mqtt/status"));
            payload.insert("payload_available".to_string(), Value::from("online"));
            payload.insert("payload_not_available".to_string(), Value::from("offline"));

            // Get platform for the topic
            let platform = payload.get("p")
                .and_then(|v| v.as_str())
                .unwrap_or("sensor")
                .to_string();

            // Remove the short "p" field, HA doesn't need it in individual discovery
            payload.remove("p");

            // Build hierarchical topic path from key
            // Convert phase suffixes like _l1, _l2, _l3 to /l1, /l2, /l3
            let key_path = key_to_topic_path(key);

            // Device ID for topic grouping
            let device_id = format!("e2m_{}_{}", self.proto, self.device).to_lowercase();

            // Topic format: homeassistant/{platform}/{device_id}/{key_path}/config
            let topic = format!("homeassistant/{platform}/{device_id}/{key_path}/config");

            discoveries.push(HaEntityDiscovery {
                topic,
                payload: Value::Object(payload),
            });
        }

        discoveries
    }

    /* Set parent device */
    pub fn via(mut self, via: String) -> Self {
        self.device_info.via_device = via;
        self
    }

    /* Set the device friendly name (different from the ID) */
    pub fn device_name(mut self, name: String) -> Self {
        self.device_info.name = name;
        self
    }

    pub fn add_information(self, _key: String, _value: Value) -> Self {
        // Legacy method - no longer used in new approach
        self
    }
}

#[derive(Clone)]
pub struct HaComponent2 {
    defs: Map<String, Value>
}

impl HaToJSON for HaComponent2 {
    fn to_json(&self) -> Value {
        Value::Object(self.to_json_map())
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

    /// Build JSON map with value_template and unique_id populated
    pub fn to_json_map(&self) -> Map<String, Value> {
        let mut m = self.defs.clone();

        let key = m.get("_key").and_then(|v| v.as_str()).unwrap_or_default().to_string();

        if !m.contains_key("value_template") {
            m.insert("value_template".to_string(), Value::from(format!("{{{{ value_json.{} }}}}", key.clone())));
        } else {
            /* We allow ## as a replacement for the correct key and replace it, if found */
            let mut v: String = m.get("value_template").and_then(|v| v.as_str()).unwrap_or_default().to_string();
            v = v.replace("##",  &format!("value_json.{key}"));
            m.insert("value_template".to_string(), v.into());
        }

        let proto = m.get("_proto").and_then(|v| v.as_str()).unwrap_or_default().to_string();
        let device = m.get("_device").and_then(|v| v.as_str()).unwrap_or_default().to_string();

        let safe_name = key.clone().replace(" ", "_");

        m.insert("unique_id".to_string(), format!("e2m_{proto}_{device}_{safe_name}").to_lowercase().into());
        let p = m.get("p").and_then(|v| v.as_str()).unwrap_or_default().to_string();
        m.insert("default_entity_id".to_string(), format!("{p}.e2m_{proto}_{device}_{safe_name}").to_lowercase().into());

        /* Remove the unneeded parts of the map */
        m.remove("_key");
        m.remove("_proto");
        m.remove("_device");

        m
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_key_to_topic_path() {
        // Phase suffixes become path segments
        assert_eq!(key_to_topic_path("voltage_l1"), "voltage/l1");
        assert_eq!(key_to_topic_path("current_l2"), "current/l2");
        assert_eq!(key_to_topic_path("power_l3"), "power/l3");

        // Aggregate suffixes become path segments
        assert_eq!(key_to_topic_path("energy_all"), "energy/all");
        assert_eq!(key_to_topic_path("voltage_avg"), "voltage/avg");
        assert_eq!(key_to_topic_path("power_sum"), "power/sum");
        assert_eq!(key_to_topic_path("current_total"), "current/total");

        // No suffix - stays as is
        assert_eq!(key_to_topic_path("cache_misses"), "cache_misses");
        assert_eq!(key_to_topic_path("temperature"), "temperature");

        // Only last matching suffix is converted
        assert_eq!(key_to_topic_path("total_energy_all"), "total_energy/all");
    }
}
