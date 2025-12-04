use std::{collections::HashMap, sync::{Arc, RwLock}, time::Duration};
use log::{debug, error, info};
use rumqttc::AsyncClient;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::{fs, sync::Mutex, time};
use walkdir::WalkDir;
use lazy_static::lazy_static;

use crate::{MeteringData, StoredData, config::ZennerDatahubConfig, get_id, get_unix_ts, metering_zennerdatahub::{PayLoadMessage, ZennerDatahubData}, models::DeviceProtocol, mqtt::{SubscribeData, Transmission, home_assistant::{HaComponent2, HaSensor, get_command_topic, get_state_topic}}};


#[derive(Deserialize, Clone)]
enum LoRaWanAliases {
    #[allow(non_snake_case)]
    DevEUI(String),
    #[allow(non_snake_case)]
    DataField(String, String),
    #[allow(non_snake_case)]
    DevAndData(String, String, String),
    #[allow(non_snake_case)]
    ProductId(String),
}

#[derive(Deserialize, Clone)]
pub struct LoRaWANDef {
    manufacturer: String,
    model: String,
    support_url: Option<String>,
    aliases: Vec<LoRaWanAliases>,
    data_fields: Vec<LoRaWANDataDef>,
    timings: Option<LoRaWANDefTimings>,
    options: Option<LoRaWANDefOptions>,
    #[serde(skip_deserializing)]
    internal_key_map_table: HashMap<String, String>,
    #[serde(skip_deserializing)]
    exported_keys: Vec<String>,
    #[serde(skip_deserializing)]
    persisted_keys: Vec<String>,
    #[serde(skip_deserializing)]
    pub e2m_command_handler: Option<String>,
}

impl Default for LoRaWANDef {
    fn default() -> Self {
        let unset = "Unset".to_string();
        Self {
            manufacturer: unset.clone(),
            model: unset,
            support_url: None,
            aliases: Vec::new(),
            data_fields: Vec::new(),
            timings: None,
            options: None,
            internal_key_map_table: HashMap::new(),
            persisted_keys: Vec::new(),
            exported_keys: Vec::new(),
            e2m_command_handler: None,   
        }
    }
}

fn def_state_class() -> String { "measurement".to_string() }
fn def_platform() -> String { "sensor".to_string() }
fn def_bool_false() -> bool { false }
fn def_bool_true() -> bool { true }

/* Hint: that may also work for NB-IoT later */
#[derive(Deserialize, Clone)]
struct ComplexPayload {
    data: String,
    port: Option<u64>,
    imme: Option<bool>,
    confirmed: Option<bool>,
}

#[derive(Deserialize, Clone)]
enum PayLoadDef {
    Basic(String),                      /* The basic type  is string */
    Complex(ComplexPayload),            /* a complex but static LoRaWAN downlink can be modelled like this */
}

#[derive(Deserialize, Clone)]
struct LoRaWANDataDefTriggers {
    subtype: String,
    payload: String,
}

#[derive(Deserialize, Clone)]
struct LoRaWANDataDef {
    #[serde(default="def_platform")]
    platform: String,
    name: String,
    friendly_name: String,
    unit_of_measurement: Option<String>,
    device_class: Option<String>,
    #[serde(default="def_state_class")]
    state_class: String,

    /* Generic override for value template */
    value_template: Option<String>,

    /* Stuff for the binary sensor */
    payload_off: Option<String>,
    payload_on: Option<String>,

    /* Stuff for the valve */
    payload_open: Option<PayLoadDef>,
    payload_close: Option<PayLoadDef>,
    payload_stop: Option<PayLoadDef>,
    state_open: Option<String>,
    state_close: Option<String>,

    /* Stuff for the device triggers */
    triggers: Option<Vec<LoRaWANDataDefTriggers>>,

    /* Stuff for fan */
    modes: Option<Vec<String>>, /* also used with climate */
    preset_mode_key: Option<String>,
    payload_oscillation_off: Option<String>,
    payload_oscillation_on: Option<String>,
    oscillation_value_template: Option<String>,

    /* Stuff for climate */
    temperature_state_template: Option<String>,
    temperature_command_topic: Option<String>,
    current_temperature_template: Option<String>,
    current_humidity_template: Option<String>,
    fan_mode_state_template: Option<String>,
    fan_mode_command_topic: Option<String>,
    fan_modes: Option<Vec<String>>,

    mode_state_template: Option<String>,
    mode_command_topic: Option<String>,

    min_temp: Option<u32>,
    max_temp: Option<u32>,
    min_humidity: Option<u32>,
    max_humidity: Option<u32>,
    temp_step: Option<f32>,


    /* Those are not Home Assistant Parts but values we use to store further information we use internaly */
    map_key: Option<String>,
    #[serde(default="def_bool_true")]
    e2m_persist: bool,

    e2m_extra_keys: Option<Vec<String>>
}

fn max_time_10() -> u64 {
    10*60
}

fn max_time_20() -> u64 {
    20*60
}

#[derive(Deserialize, Clone)]
struct LoRaWANDefTimings {
    #[serde(default="max_time_20")]
    max_eventtime: u64,
    #[serde(default="max_time_10")]
    max_devtime: u64
}

fn default_bool_false() -> bool { false }

#[derive(Deserialize, Clone)]
struct LoRaWANDefOptions {
    #[serde(default = "default_bool_false")]
    has_gps: bool,
    #[serde(default = "default_bool_false")]
    ignore_generated_values: bool,
    #[serde(default = "default_bool_false")]
    has_command_topic: bool,
}

/* We need to verify against different data of the parsed output ... */
fn match_field(_data: &HashMap<String, Value>, _field: &String, _content: &String) -> bool {
    return false;
}

lazy_static! {
    static ref KNOWN_DEFS: RwLock<HashMap<String /* dev_eui */, LoRaWANDef /* definition */>> = RwLock::new(HashMap::new());
}

pub async fn find_defintion(dev_eui: &String, known_devices: &mut HashMap<String, LoRaWANDef>, field_map: Option<&HashMap<String, Value>>) -> Option<LoRaWANDef> {

    /* Check if the dev_eui is already known */
    if known_devices.contains_key(dev_eui) {
        return Some(known_devices[dev_eui].clone());
    }

    for entry in WalkDir::new("defs/lorawan/")
                            .follow_links(false)
                            .into_iter()
                            .filter_map(Result::ok)
                            .filter(|e| e.file_type().is_file())
                            .filter(| e| e.file_name().to_string_lossy().ends_with(".yaml")) {
        /* Parse this defition file */
        let file_name = entry.path().to_str().unwrap_or("/nonexisting/dir");
        debug!("Loading {file_name}");
        let fs = fs::read_to_string(&file_name).await.unwrap_or("".to_string());

        let mut mapping: LoRaWANDef = match serde_yml::from_str(&fs) {
            Ok(d) => d,
            Err(e) => {
                error!("File {file_name} could not be loaded: {e:?}");
                continue;
            },
        };

        /* Check if we have a command handler and if so add it */
        let command = file_name.replace(".yaml", ".command");
        if let Ok(b) = fs::try_exists(&command).await {
            if b == true {
                mapping.e2m_command_handler = Some(command);
            }
        }

        for alias in &mapping.aliases {
            match alias {
                LoRaWanAliases::DevEUI(query) => {
                                    let re = regex::Regex::new(query.as_str());
                                    if re.is_err() { error!("{file_name} contains invalid regex: {query}"); continue; }
                                    let re = re.unwrap();
                                    if re.is_match(&dev_eui) {
                        info!("{file_name} contains a mapping based on the sensors DevEUI {dev_eui}");
                        known_devices.insert(dev_eui.clone(), mapping.clone());
                        return Some(mapping);
                    }
                },
                LoRaWanAliases::DataField(field, content) => {
                    if let Some(fields) = field_map {
                        if match_field(&fields, field, content) {
                            known_devices.insert(dev_eui.clone(), mapping.clone());
                            return Some(mapping);
                        }
                    }
                },
                LoRaWanAliases::DevAndData(query, field, content) => {
                    let re = regex::Regex::new(query.as_str());
                    if re.is_err() { error!("{file_name} contains invalid regex: {query}"); continue; }
                    let re = re.unwrap();
                    if re.is_match(&dev_eui) {
                        /* Verify if the data also matches the required part */
                        if let Some(fields) = field_map {
                            if match_field(&fields, field, content) {
                                known_devices.insert(dev_eui.clone(), mapping.clone());
                                return Some(mapping);
                            }
                        }
                    }
                },
                LoRaWanAliases::ProductId(product) => {
                    if let Some(fields) = field_map {
                        if let Some(prod_id) = fields.get("prod_id") {
                            if prod_id == product {
                                info!("{file_name} contains the correct product id {prod_id} for {dev_eui}");
                                known_devices.insert(dev_eui.clone(), mapping.clone());
                                return Some(mapping);
                            }
                        }
                    }
                },
            }
        }
    }

    return None;
}

pub async fn handle_incoming_lora(data: &mut Arc<Mutex<ZennerDatahubData>>, instance: String, payload: String, via: String) {

    /* Define some structs used in this function to parse json comming from ZENNER Datahub */
    #[derive(Deserialize)]
    #[allow(non_snake_case)] // We need to take what the JSON author gave us...
    struct ZriDhData {
        ownernumber: String,
        proposedDevAddr: Option<String>,
        unmapped: HashMap<String, Value>,
        rssi: f32,
        lora: ZriDhDataLoRa,
    }

    #[derive(Deserialize)]
    struct ZriDhDataLoRa {
        lsnr: f32,
    }

    #[derive(Deserialize)]
    struct ZriDhJSON {
        r#type: String,
        data: ZriDhData,
        ts: HashMap<String, Value>,
        geo: Option<ZriDhJsonGeo>,
        #[serde(default)]
        meta: ZriDhJsonMeta,
    }

    #[derive(Deserialize)]
    struct ZriDhJsonGeo {
        r#type: String,
        coordinates: Vec<f32>
    }

    #[derive(Deserialize)]
    struct ZriDhJsonMeta {
        generated: Option<bool>
    }

    impl Default for ZriDhJsonMeta {
        fn default() -> Self {
        Self { generated: None }
    }
    }

    let mut d = data.lock().await;

    d.stats.inc("received_all");

    /* Try to parse the JSON object */
    let json: ZriDhJSON = match serde_json::from_str(&payload.as_str()) {
        Ok(d) => d,
        Err(e) => {
            debug!("Parsing the JSON {payload} failed: {e:?}");
            return; 
        },
    };

    d.stats.inc("received_valid");

    let dh_proto = json.r#type;
    if dh_proto != "lora" {
        error!("Not supported protocol received: {dh_proto}");
        return;
    }

    d.stats.inc("received_lorawan");

    let proto = format!("zridh_{dh_proto}");
    let dev_eui = json.data.ownernumber;

    if !d.known_devices.contains_key(&dev_eui) {
        /*  This device is not known to home assistant, send discovery */
        info!("New device {dev_eui} received from ZENNER Datahub, checking if known defintion is available");

        match find_defintion(&dev_eui, &mut d.defs, Some(&json.data.unmapped)).await {
            Some(mut def) => {
                /* Build Home Assistant discovery message */
                let mut disc = HaSensor::new(proto.clone(), dev_eui.clone(),
                                                        Some(def.manufacturer.clone()),
                                                        Some(def.model.clone()))
                                                        .via(via.clone());

                /* TODO: Check if we want to add a support url .. looks like the wrong place! */
                if let Some(_url) = &def.support_url {
                    //disc = disc.add_information("support_url".to_string(), url.clone().into());
                }

                /* Store our trigger datapoints, we need to run them later on because EACH entry is an own component entry */
                let mut triggers = Vec::new();

                for datapoint in &def.data_fields {
                    /* Make sure we publish that value */
                    def.exported_keys.push(datapoint.name.clone());

                    /* We need to make sure to add all fields we got from the discovery and NOT the ones in the JSON doc (that may be incomplete) */
                    let mut cmp = HaComponent2::new()
                                                .ent_platform(datapoint.platform.clone())
                                                .name(datapoint.friendly_name.clone())
                                                .state_class(datapoint.state_class.clone());

                    if let Some(dev_class) = datapoint.device_class.clone() {
                        cmp = cmp.device_class(dev_class);
                    }

                    if let Some(uom) = datapoint.unit_of_measurement.clone() {
                        cmp = cmp.unit_of_measurement(uom);
                    }
                    
                    /* Some fields should never be persistat, we can remove them here */
                    if datapoint.e2m_persist {
                        def.persisted_keys.push(datapoint.name.clone());
                    }

                    /* Handle the parts that are not a base sensor */
                    match datapoint.platform.as_str() {
                        "valve" => {
                            /* Check if we have payload open or closed set if not the sensor uses the home assistant defaults OPEN and CLOSE */
                            if let Some(open) = &datapoint.state_open {
                                cmp = cmp.add_information("state_open", open.clone().into());
                            }
                            if let Some(close) = &datapoint.state_close {
                                cmp = cmp.add_information("state_close", close.clone().into());
                            }
                            if let Some(open) = &datapoint.payload_open {
                                let val = match open {
                                    PayLoadDef::Basic(v) => PayLoadMessage::new("lora", &dev_eui, &v, None, None),
                                    PayLoadDef::Complex(complex_payload) => {
                                        PayLoadMessage::new("lora", &dev_eui, &complex_payload.data, complex_payload.port, complex_payload.imme)
                                    },
                                    //_ => PayLoadMessage::new("lora", &dev_eui, &"none".to_string(), None, None),
                                }.to_ha_discover_payload();

                                cmp = cmp.add_information("payload_open", Value::from(val));
                            }
                            if let Some(close) = &datapoint.payload_close {
                                let val = match close {
                                    PayLoadDef::Basic(v) => PayLoadMessage::new("lora", &dev_eui, &v, None, None),
                                    PayLoadDef::Complex(complex_payload) => {
                                        PayLoadMessage::new("lora", &dev_eui, &complex_payload.data, complex_payload.port, complex_payload.imme)
                                    },
                                    //_ => PayLoadMessage::new("lora", &dev_eui, &"none".to_string(), None, None),
                                }.to_ha_discover_payload();
                                cmp = cmp.add_information("payload_close", Value::from(val));
                            }
                            if let Some(stop) = &datapoint.payload_stop {
                                let val = match stop {
                                    PayLoadDef::Basic(v) => PayLoadMessage::new("lora", &dev_eui, &v, None, None),
                                    PayLoadDef::Complex(complex_payload) => {
                                        PayLoadMessage::new("lora", &dev_eui, &complex_payload.data, complex_payload.port, complex_payload.imme)
                                    },
                                    //_ => { PayLoadMessage::new("lora", &dev_eui, &"none".to_string(), None, None) },
                                }.to_ha_discover_payload();
                                cmp = cmp.add_information("payload_stop", Value::from(val));
                            }
                        },
                        "binary_sensor" => {
                            /* It's either value_template or payload not both */
                            if let Some(template) = &datapoint.value_template {
                                cmp = cmp.add_information("value_template", template.clone().into());
                            } else {
                                if let Some(on) = &datapoint.payload_on {
                                    cmp = cmp.add_information("payload_on", on.clone().into());
                                }
                                if let Some(off) = &datapoint.payload_off {
                                    cmp = cmp.add_information("payload_off", off.clone().into());
                                }
                            }
                        }

                        "device_automation" => {
                            /* Trigger handling is special, because we want to register an endpoint for each and every posible payload */
                            triggers.push(datapoint);
                            continue;
                        }

                        "fan" => {
                            /* There is no state class */
                            cmp = cmp.del_information("state_class");

                            /* We require a command topic to be set */
                            let topic = get_command_topic(&proto, &dev_eui, &"command".to_string());
                            cmp = cmp.add_information("command_topic", Value::from(topic.clone()));
                            let _ = d.sender.send(Transmission::Subscribe( SubscribeData { 
                                                    topic: topic,
                                                    sender: d.callback_sender.clone()
                                                })).await;

                            /* Let use define on and off if needed */
                            if let Some(template) = &datapoint.value_template {
                                cmp = cmp.add_information("state_value_template", template.clone().into());
                                cmp = cmp.add_information("state_topic", Value::from(get_state_topic(&proto, &dev_eui)));
                            } else {
                                if let Some(on) = &datapoint.payload_on {
                                    cmp = cmp.add_information("payload_on", on.clone().into());
                                    cmp = cmp.add_information("state_topic", Value::from(get_state_topic(&proto, &dev_eui)));
                                }
                                if let Some(off) = &datapoint.payload_off {
                                    cmp = cmp.add_information("payload_off", off.clone().into());
                                    cmp = cmp.add_information("state_topic", Value::from(get_state_topic(&proto, &dev_eui)));
                                }
                            }

                            /* Add oscillation if needed */
                            if let Some(template) = &datapoint.oscillation_value_template {
                                cmp = cmp.add_information("oscillation_value_template", Value::from(template.clone()));
                                cmp = cmp.add_information("oscillation_state_topic", Value::from(get_state_topic(&proto, &dev_eui)));
                            } else {
                                if let Some(on) = &datapoint.payload_oscillation_on {
                                    cmp = cmp.add_information("payload_oscillation_on", on.clone().into());
                                    cmp = cmp.add_information("oscillation_state_topic", Value::from(get_state_topic(&proto, &dev_eui)));
                                }
                                if let Some(off) = &datapoint.payload_oscillation_off {
                                    cmp = cmp.add_information("payload_oscillation_off", off.clone().into());
                                    cmp = cmp.add_information("oscillation_state_topic", Value::from(get_state_topic(&proto, &dev_eui)));
                                }
                            }

                            /* Fan may have presents to be used */
                            if let Some(preset_mode_key) = &datapoint.preset_mode_key {
                                cmp = cmp.add_information("preset_mode_state_topic", Value::from(get_state_topic(&proto, &dev_eui)));
                                cmp = cmp.add_information("preset_mode_value_template", format!("{{{{ value_json.{preset_mode_key} }}}}").into());

                                if !def.exported_keys.contains(&preset_mode_key) {
                                    def.exported_keys.push(preset_mode_key.clone());
                                }

                                if let Some(m) = &datapoint.modes {
                                    cmp = cmp.add_information("preset_modes", Value::Array(m.iter().map(|e| Value::from(e.clone())).collect()));

                                    let topic = get_command_topic(&proto, &dev_eui, &"preset_mode_command".to_string());
                                    cmp = cmp.add_information("preset_mode_command_topic", Value::from(topic.clone()));

                                    let _ = d.sender.send(Transmission::Subscribe( SubscribeData { 
                                                    topic: topic,
                                                    sender: d.callback_sender.clone()
                                            })).await;
                                }
                            }

                            cmp = cmp.add_information("retain", Value::from(false));
                        },

                        "climate" => {
                            cmp = cmp.del_information("state_class");

                            let state_topic = get_state_topic(&proto, &dev_eui);

                            /* The current temperature is stored here */
                            if let Some(template) = &datapoint.current_temperature_template {
                                cmp = cmp.add_information("current_temperature_topic", state_topic.clone().into());
                                cmp = cmp.add_information("current_temperature_template", template.clone().into());
                            }

                            /* The topic to change the target temperature */
                            if let Some(topic) = &datapoint.temperature_command_topic {
                                let topic =  get_command_topic(&proto, &dev_eui,&topic);
                                cmp = cmp.add_information("temperature_command_topic",topic.clone().into());

                                let _ = d.sender.send(Transmission::Subscribe( SubscribeData { 
                                                    topic: topic,
                                                    sender: d.callback_sender.clone()
                                                })).await;
                            }

                            /* The target temperature */
                            if let Some(template) = &datapoint.temperature_state_template {
                                cmp = cmp.add_information("temperature_state_topic", state_topic.clone().into());
                                cmp = cmp.add_information("temperature_state_template", template.clone().into());
                            }

                            /* The current humidity is stored here */
                            if let Some(template) = &datapoint.current_humidity_template {
                                cmp = cmp.add_information("current_humidity_topic", state_topic.clone().into());
                                cmp = cmp.add_information("current_humidity_template", template.clone().into());
                            }

                            /* There may be a fan in the ac system which can be controlled */
                            if let Some(template) = &datapoint.fan_mode_state_template {
                                cmp = cmp.add_information("fan_mode_state_topic", state_topic.clone().into());
                                cmp = cmp.add_information("fan_mode_state_template", template.clone().into());
                                let topic =  get_command_topic(&proto, &dev_eui,
                                                                &datapoint.fan_mode_command_topic.clone()
                                                                        .unwrap_or("fan_mode_command".to_string()));
                                cmp = cmp.add_information("fan_mode_command_topic",topic.clone().into());

                                let _ = d.sender.send(Transmission::Subscribe( SubscribeData { 
                                                    topic: topic,
                                                    sender: d.callback_sender.clone()
                                                })).await;

                                if let Some(m) = &datapoint.fan_modes {
                                    cmp = cmp.add_information("fan_modes", Value::Array(m.iter().map(|e| Value::from(e.clone())).collect()));
                                }
                            }

                            /* Some devices allow to switch between heating, cooling and automatic option */
                            if let Some(template) = &datapoint.mode_state_template {
                                cmp = cmp.add_information("mode_state_topic", state_topic.clone().into());
                                cmp = cmp.add_information("mode_state_template", template.clone().into());
                                let topic =  get_command_topic(&proto, &dev_eui,
                                                                &datapoint.mode_command_topic.clone()
                                                                        .unwrap_or("mode_command".to_string()));
                                cmp = cmp.add_information("mode_command_topic",topic.clone().into());

                                let _ = d.sender.send(Transmission::Subscribe( SubscribeData { 
                                                    topic: topic,
                                                    sender: d.callback_sender.clone()
                                                })).await;

                                if let Some(m) = &datapoint.modes {
                                    cmp = cmp.add_information("modes", Value::Array(m.iter().map(|e| Value::from(e.clone())).collect()));
                                }
                            }

                            /* Add information about our temperatures */
                            if let Some(data) = datapoint.min_temp {
                                cmp = cmp.add_information("min_temp", Value::from(data));
                            }
                            if let Some(data) = datapoint.max_temp {
                                cmp = cmp.add_information("max_temp", Value::from(data));
                            }
                            if let Some(data) = datapoint.min_humidity {
                                cmp = cmp.add_information("min_humidity", Value::from(data));
                            }
                            if let Some(data) = datapoint.max_humidity {
                                cmp = cmp.add_information("max_humidity", Value::from(data));
                            }
                            if let Some(data) = datapoint.temp_step {
                                cmp = cmp.add_information("temp_step", Value::from(data));
                            }

                            cmp = cmp.add_information("retain", Value::from(false));
                        }
                        e => { debug!("No special handling for platform {e} needed"); }
                    }

                    if let Some(extras) = &datapoint.e2m_extra_keys {
                        for extra in extras {
                            if !def.exported_keys.contains(&extra) {
                                def.exported_keys.push(extra.clone());
                            }

                            if !def.persisted_keys.contains(&extra) {
                                def.persisted_keys.push(extra.clone());
                            }
                        }
                    }

                    match &datapoint.map_key {
                        Some(name) => {
                            def.internal_key_map_table.insert(datapoint.name.clone(), name.clone());
                            disc.add_cmp(name.clone(), cmp)
                        },
                        None => disc.add_cmp(datapoint.name.clone(), cmp),
                    }
                }

                /* Special handling for triggers to have it at a designated place in the code and not in the main loop */
                for def in triggers {
                    /* Name is already in the export list and all prechecks are done, we now add all components */
                    if let Some(payloads) = &def.triggers {
                        for trigger in payloads {
                            /* See https://www.home-assistant.io/integrations/device_trigger.mqtt/ for an example */
                            let key = match &def.map_key {        
                                                    Some(n) => n.clone(),
                                                    None => def.name.clone(),
                                                };

                            let subtype = &trigger.subtype;
                            let payload = &trigger.payload;

                            /* INFO: The Topic needs to be there even if the device has a state topic on it's own */
                            let cmp = HaComponent2::new()
                                                .ent_platform("device_automation".into())
                                                .add_information("automation_type", "trigger".into())
                                                .add_information("type", subtype.clone().into())  
                                                .add_information("subtype", key.clone().into())
                                                .add_information("payload", payload.clone().into())
                                                .add_information("topic", Value::from(get_state_topic(&proto, &dev_eui)))
                                                .add_information("value_template", format!("{{{{ value_json.{key} }}}}").into());
                            
                            disc.add_cmp(format!("{key}_{subtype}"), cmp);
                        }
                    }
                    
                }

                /* The GPS handling of datahub and home assistant can not be mapped directly, so we need to move that part out */
                if let Some(options) = &def.options {
                    /* 
                        Datahub allows to enable a Location for every device but that is mostly a static information for 
                        people working in field service. 

                        We only add devices which expose a real GPS value using an option in the configuration field.
                    */
                    if options.has_gps {
                        /* We add the gps part now */
                        let cmp = HaComponent2::new().name("Location".to_string())
                                        .platform("device_tracker".to_string())
                                        .add_information("json_attributes_topic", Value::from(get_state_topic(&proto, &dev_eui)))
                                        .add_information("json_attributes_template", Value::from("{{ value_json.location | tojson }}"));
                        /* Override topics and json_attribute settings */
                        disc.add_cmp("location".to_string(), cmp);
                    }

                    if options.has_command_topic {
                        /* Now check if we need to handle a command topic */
                        disc = disc.add_information("command_topic".to_string(),
                                                    Value::from(crate::mqtt::home_assistant::get_command_topic(&"zridh".to_string(),
                                                                                                                &instance, 
                                                                                                                &"json".to_string())));
                    }
                }
                
                /* RSSI is in data.rssi */
                let cmp = HaComponent2::new().name("RSSI".to_string())
                                        .device_class("signal_strength".to_string())
                                        .unit_of_measurement("dBm".to_string())
                                        .cat_diagnostic();
                disc.add_cmp("rssi".to_string(), cmp);


                /* SNR is in data.lora.lsnr */
                let cmp = HaComponent2::new().name("SNR".to_string())
                                        .device_class("signal_strength".to_string())
                                        .unit_of_measurement("dB".to_string())
                                        .cat_diagnostic();
                disc.add_cmp("snr".to_string(), cmp);


                /* SNR is in data.lora.proposedDevAddr */
                let cmp = HaComponent2::new().name("Device Address".to_string())
                                        .non_numeric().cat_diagnostic().hidden();
                disc.add_cmp("devaddr".to_string(), cmp);


                info!("Sending HA discovery");
                let _ = d.sender.send(Transmission::AutoDiscovery2(disc)).await;
                d.known_devices.insert(dev_eui.clone(), def);

                /* Give home assistant some time to subscribe to the value topic */
                time::sleep(Duration::from_secs(3)).await;
            },
            None => {
                debug!("No defintion for {dev_eui} found, just igonoring that data");
                return;
            },
        }
    }

    let def = &d.known_devices[&dev_eui];

    if let Some(gen) = json.meta.generated {
        if gen == true {
            if let Some(opt) = &def.options {
                if opt.ignore_generated_values {
                    info!("ignoring generated value of {dev_eui}");
                }
            }
        }
    }

    let mut device_time = 0;
    let now = get_unix_ts();
    if json.ts.contains_key("eventtime") {

        /* The default is 20 minutes */
        let mut offset_max = 20*60;
        if let Some(timings) = &def.timings {
            offset_max = timings.max_eventtime;
            debug!("Setting max time for {dev_eui} to {offset_max}");
        }

        let event_time = json.ts.get("eventtime").unwrap_or(&Value::Number(now.into())).as_u64().unwrap_or(now);
        /* Only accept values for eventtime which are too old */
        if event_time > now - offset_max {
            debug!("ts.eventtime used to set the correct timing");
            device_time = event_time;
        } else {
            info!("The eventtime of {dev_eui} is too far away, we ignore the document may be values from last month or start of the year -> {event_time} diff {}", now - event_time);
            d.stats.inc("error_eventtime_old");
            return;
        }
    }

    if json.ts.contains_key("device") {
        /* The default is 10 minutes */
        let mut offset_max = 10*60;
        if let Some(timings) = &def.timings {
            offset_max = timings.max_devtime;
        }
        let event_time = json.ts.get("device").unwrap_or(&Value::Number(now.into())).as_u64().unwrap_or(now);
        /* That device value should not be older than some minutes otherwise there may be stuck data in ZENNER Datahub, we do not want data older than 30 minutes */
        if event_time > now - offset_max {
            debug!("ts.device used to set the correct timing");
            device_time = event_time;
        } else {
            info!("The device time of {dev_eui} is too far away, we ignore it -> {event_time} diff {}", now - event_time);
            d.stats.inc("error_devicetime_old");
        }
    }

    if device_time == 0 {
        error!("Neither ts.eventtime nor ts.device contains correct data, faking date information for {dev_eui}");
        device_time = now;
    }

    /* Now send the metering data  */
    let mut meter_data = MeteringData::new().unwrap();
    meter_data.meter_name = dev_eui.clone();
    meter_data.protocol = DeviceProtocol::ZennerDatahub;
    meter_data.id = get_id(format!("zridh_{dh_proto}"), &dev_eui);
    meter_data.metered_time = device_time;
    meter_data.state_topic_base = proto; // We need to make sure that we handle our sub protocol right at this point

    /* We only use the values which are defined as exported */

    let mut perist = StoredData::load(format!("zridh_{dh_proto}"), &dev_eui).await;

    for k in d.known_devices[&dev_eui].exported_keys.iter() {

        debug!("Verifying if exported key is in unmapped: {k}");
        /* Check for the existance of the key */
        let mut value =  match json.data.unmapped.contains_key(k) {
            true => {
                debug!("Found real value for key {k}");
                json.data.unmapped[k].clone()
            },
            false => { 
                /* 
                    The current json does not include that field name, 
                    Check for old values, will return None if not found which is the same as not having the filed in the transmission
                */
                debug!("No direct data found will use peristance data for {k}");
                perist.get_data(&k)
            },
        };

        /* There may be some arrays which we need to handle */
        if let Value::Array(array) = &value {
            if array.len() > 1 {
                value = array.get(0).unwrap_or(&value).clone();
            }
        }

        /* Some sensors use a value below the name of the key, parse that one */
        if let Value::Object(obj) = &value {
            if obj.contains_key("value") {
                value = obj["value"].clone();
            }
        }

        /* Convert Bool to HA representation */
        if let Value::Bool(b) = value {
            value = match b {
                true => "ON".into(),
                false => "OFF".into(),
            }
        }

        /* Home Assistant wants ON and OFF not on or off */
        if let Value::String(s) = &value {
            if s == "on" {
                value = Value::from("ON");
            } else if s == "off" {
                value = Value::from("OFF");
            }
        }

        debug!("Value after array, bool an case checks {value}");

        /* check if we need to map the name of the key to another key */
        let mut key = k.clone();
        if let Some(v) = d.known_devices[&dev_eui].internal_key_map_table.get(&key) {
            key = v.clone();
        }

        /* Only if the key should be persisted */
        if d.known_devices[&dev_eui].persisted_keys.contains(&k) {
            /* WARNING: persistance works with the REAL key not the mapped one */
            perist.set_data(&k, &value);
        }

        meter_data.metered_values.insert(key.clone(), value);
    }

    /* Store rssi and SNR */
    meter_data.metered_values.insert("rssi".to_string(), json.data.rssi.into());
    meter_data.metered_values.insert("snr".to_string(), json.data.lora.lsnr.into());
    meter_data.metered_values.insert("devaddr".to_string(), json.data.proposedDevAddr.into());

    if let Some(geo) = json.geo {
        if geo.r#type == "Point" {
            if geo.coordinates.len() == 2 {
                /* We now got the needed data */
                let latitude = geo.coordinates[1];
                let longitude = geo.coordinates[0];
                #[derive(Serialize)]
                struct MqttGeo {
                    longitude: f32,
                    latitude: f32,
                    gps_accuracy: u32
                }

                let coord = MqttGeo { longitude, latitude, gps_accuracy: 100 };

                meter_data.metered_values.insert("location".to_string(), serde_json::to_value(coord).unwrap());
            } else {
                error!("[{dev_eui}] GeoJSON needs to be encoded with two elements in coordinates");
            }
        } else {
            error!("[{dev_eui}] GeoJSON needs to be encoded as Point not as {}", geo.r#type);
        }
    }

    info!("Sending data");
    let _ = d.sender.send(Transmission::Metering(meter_data)).await;
    d.stats.inc("data_published");

}

pub async fn handle_downlink_lora_complex(payload: &PayLoadMessage, publish_c: &AsyncClient, config: &ZennerDatahubConfig) {
    /* 
        We received a payload, we need to write that back to the main mqtt server used by ZENNER Datahub
    */
    let base_topic = &config.base_topic;
    let publish_topic = format!("{base_topic}/W/lora/{}", &payload.id);

    #[derive(Serialize)]
    #[allow(non_snake_case)]
    struct ZriDhLoRaDownlink {
        devEUI: String,
        data: String,
        port: u64,
        imme: bool,
        confirmed: bool,
        retriesLeft: u32,
    }

    let downlink = ZriDhLoRaDownlink {
        devEUI: payload.id.clone(),
        data: payload.payload.clone(),
        port: payload.port.unwrap_or(0),
        imme: payload.imme.unwrap_or(false),
        confirmed: true,
        retriesLeft: 3
    };

    if let Ok(content) = serde_json::to_string(&downlink) {
        info!("Publishing {publish_topic} --> {content}");
        let _ = publish_c.publish(publish_topic, rumqttc::QoS::AtLeastOnce, false, content).await;
    } else {
        error!("Can not generate string from LoRaWAN payload");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lorawan_def_default() {
        let def = LoRaWANDef::default();

        assert_eq!(def.manufacturer, "Unset");
        assert_eq!(def.model, "Unset");
        assert!(def.support_url.is_none());
        assert!(def.aliases.is_empty());
        assert!(def.data_fields.is_empty());
        assert!(def.timings.is_none());
        assert!(def.options.is_none());
        assert!(def.internal_key_map_table.is_empty());
        assert!(def.exported_keys.is_empty());
        assert!(def.persisted_keys.is_empty());
        assert!(def.e2m_command_handler.is_none());
    }

    #[test]
    fn test_def_state_class() {
        assert_eq!(def_state_class(), "measurement");
    }

    #[test]
    fn test_def_platform() {
        assert_eq!(def_platform(), "sensor");
    }

    #[test]
    fn test_def_bool_false() {
        assert!(!def_bool_false());
    }

    #[test]
    fn test_def_bool_true() {
        assert!(def_bool_true());
    }

    #[test]
    fn test_default_bool_false() {
        assert!(!default_bool_false());
    }

    #[test]
    fn test_max_time_10() {
        assert_eq!(max_time_10(), 10 * 60);
    }

    #[test]
    fn test_max_time_20() {
        assert_eq!(max_time_20(), 20 * 60);
    }

    #[test]
    fn test_match_field_returns_false() {
        let data: HashMap<String, Value> = HashMap::new();
        let field = "test_field".to_string();
        let content = "test_content".to_string();

        // Currently match_field always returns false
        assert!(!match_field(&data, &field, &content));
    }

    #[test]
    fn test_match_field_with_data_returns_false() {
        let mut data: HashMap<String, Value> = HashMap::new();
        data.insert("test_field".to_string(), Value::String("test_content".to_string()));

        let field = "test_field".to_string();
        let content = "test_content".to_string();

        // Currently match_field always returns false (stub implementation)
        assert!(!match_field(&data, &field, &content));
    }

    #[test]
    fn test_complex_payload_deserialization() {
        let json = r#"{"data": "AABBCC", "port": 10, "imme": true, "confirmed": false}"#;
        let payload: ComplexPayload = serde_json::from_str(json).unwrap();

        assert_eq!(payload.data, "AABBCC");
        assert_eq!(payload.port, Some(10));
        assert_eq!(payload.imme, Some(true));
        assert_eq!(payload.confirmed, Some(false));
    }

    #[test]
    fn test_complex_payload_deserialization_minimal() {
        let json = r#"{"data": "1234"}"#;
        let payload: ComplexPayload = serde_json::from_str(json).unwrap();

        assert_eq!(payload.data, "1234");
        assert!(payload.port.is_none());
        assert!(payload.imme.is_none());
        assert!(payload.confirmed.is_none());
    }

    #[test]
    fn test_payload_def_basic() {
        // PayLoadDef uses externally tagged format, so Basic variant wraps a string
        let json = r#"{"Basic": "AABBCC"}"#;
        let payload: PayLoadDef = serde_json::from_str(json).unwrap();

        match payload {
            PayLoadDef::Basic(s) => assert_eq!(s, "AABBCC"),
            _ => panic!("Expected Basic variant"),
        }
    }

    #[test]
    fn test_payload_def_complex() {
        // PayLoadDef uses externally tagged format
        let json = r#"{"Complex": {"data": "1234", "port": 5}}"#;
        let payload: PayLoadDef = serde_json::from_str(json).unwrap();

        match payload {
            PayLoadDef::Complex(c) => {
                assert_eq!(c.data, "1234");
                assert_eq!(c.port, Some(5));
            },
            _ => panic!("Expected Complex variant"),
        }
    }

    #[test]
    fn test_lorawan_def_timings_defaults() {
        let json = r#"{}"#;
        let timings: LoRaWANDefTimings = serde_json::from_str(json).unwrap();

        assert_eq!(timings.max_eventtime, 20 * 60);
        assert_eq!(timings.max_devtime, 10 * 60);
    }

    #[test]
    fn test_lorawan_def_timings_custom() {
        let json = r#"{"max_eventtime": 300, "max_devtime": 120}"#;
        let timings: LoRaWANDefTimings = serde_json::from_str(json).unwrap();

        assert_eq!(timings.max_eventtime, 300);
        assert_eq!(timings.max_devtime, 120);
    }

    #[test]
    fn test_lorawan_def_options_defaults() {
        let json = r#"{}"#;
        let options: LoRaWANDefOptions = serde_json::from_str(json).unwrap();

        assert!(!options.has_gps);
        assert!(!options.ignore_generated_values);
        assert!(!options.has_command_topic);
    }

    #[test]
    fn test_lorawan_def_options_custom() {
        let json = r#"{"has_gps": true, "ignore_generated_values": true, "has_command_topic": true}"#;
        let options: LoRaWANDefOptions = serde_json::from_str(json).unwrap();

        assert!(options.has_gps);
        assert!(options.ignore_generated_values);
        assert!(options.has_command_topic);
    }

    #[test]
    fn test_lorawan_data_def_defaults() {
        let json = r#"{"name": "temperature", "friendly_name": "Temperature"}"#;
        let data_def: LoRaWANDataDef = serde_json::from_str(json).unwrap();

        assert_eq!(data_def.platform, "sensor");
        assert_eq!(data_def.state_class, "measurement");
        assert_eq!(data_def.name, "temperature");
        assert_eq!(data_def.friendly_name, "Temperature");
        assert!(data_def.e2m_persist);
    }

    #[test]
    fn test_lorawan_data_def_binary_sensor() {
        let json = r#"{
            "platform": "binary_sensor",
            "name": "door_status",
            "friendly_name": "Door Status",
            "payload_on": "OPEN",
            "payload_off": "CLOSED"
        }"#;
        let data_def: LoRaWANDataDef = serde_json::from_str(json).unwrap();

        assert_eq!(data_def.platform, "binary_sensor");
        assert_eq!(data_def.payload_on, Some("OPEN".to_string()));
        assert_eq!(data_def.payload_off, Some("CLOSED".to_string()));
    }

    #[test]
    fn test_lorawan_data_def_valve() {
        let json = r#"{
            "platform": "valve",
            "name": "valve_control",
            "friendly_name": "Valve Control",
            "state_open": "OPEN",
            "state_close": "CLOSED"
        }"#;
        let data_def: LoRaWANDataDef = serde_json::from_str(json).unwrap();

        assert_eq!(data_def.platform, "valve");
        assert_eq!(data_def.state_open, Some("OPEN".to_string()));
        assert_eq!(data_def.state_close, Some("CLOSED".to_string()));
    }

    #[test]
    fn test_lorawan_data_def_climate() {
        let json = r#"{
            "platform": "climate",
            "name": "thermostat",
            "friendly_name": "Thermostat",
            "min_temp": 16,
            "max_temp": 30,
            "temp_step": 0.5,
            "modes": ["heat", "cool", "auto"]
        }"#;
        let data_def: LoRaWANDataDef = serde_json::from_str(json).unwrap();

        assert_eq!(data_def.platform, "climate");
        assert_eq!(data_def.min_temp, Some(16));
        assert_eq!(data_def.max_temp, Some(30));
        assert_eq!(data_def.temp_step, Some(0.5));
        assert_eq!(data_def.modes, Some(vec!["heat".to_string(), "cool".to_string(), "auto".to_string()]));
    }

    #[test]
    fn test_lorawan_data_def_fan() {
        let json = r#"{
            "platform": "fan",
            "name": "fan_control",
            "friendly_name": "Fan",
            "payload_on": "ON",
            "payload_off": "OFF",
            "modes": ["low", "medium", "high"]
        }"#;
        let data_def: LoRaWANDataDef = serde_json::from_str(json).unwrap();

        assert_eq!(data_def.platform, "fan");
        assert_eq!(data_def.payload_on, Some("ON".to_string()));
        assert_eq!(data_def.payload_off, Some("OFF".to_string()));
        assert_eq!(data_def.modes, Some(vec!["low".to_string(), "medium".to_string(), "high".to_string()]));
    }

    #[test]
    fn test_lorawan_aliases_dev_eui() {
        // Uses YAML tag format for enum variants
        let yaml = r#"!DevEUI "^0018.*""#;
        let alias: LoRaWanAliases = serde_yml::from_str(yaml).unwrap();

        match alias {
            LoRaWanAliases::DevEUI(pattern) => assert_eq!(pattern, "^0018.*"),
            _ => panic!("Expected DevEUI variant"),
        }
    }

    #[test]
    fn test_lorawan_aliases_product_id() {
        // Uses YAML tag format for enum variants
        let yaml = r#"!ProductId "SENSOR-001""#;
        let alias: LoRaWanAliases = serde_yml::from_str(yaml).unwrap();

        match alias {
            LoRaWanAliases::ProductId(id) => assert_eq!(id, "SENSOR-001"),
            _ => panic!("Expected ProductId variant"),
        }
    }

    #[test]
    fn test_lorawan_aliases_data_field() {
        // Uses YAML tag format with sequence for tuple variants
        let yaml = r#"!DataField ["manufacturer", "ACME"]"#;
        let alias: LoRaWanAliases = serde_yml::from_str(yaml).unwrap();

        match alias {
            LoRaWanAliases::DataField(field, content) => {
                assert_eq!(field, "manufacturer");
                assert_eq!(content, "ACME");
            },
            _ => panic!("Expected DataField variant"),
        }
    }

    #[test]
    fn test_lorawan_aliases_dev_and_data() {
        // Uses YAML tag format with sequence for tuple variants
        let yaml = r#"!DevAndData ["^0018.*", "type", "sensor"]"#;
        let alias: LoRaWanAliases = serde_yml::from_str(yaml).unwrap();

        match alias {
            LoRaWanAliases::DevAndData(query, field, content) => {
                assert_eq!(query, "^0018.*");
                assert_eq!(field, "type");
                assert_eq!(content, "sensor");
            },
            _ => panic!("Expected DevAndData variant"),
        }
    }

    #[test]
    fn test_lorawan_data_def_triggers() {
        let json = r#"{
            "platform": "device_automation",
            "name": "button",
            "friendly_name": "Button",
            "triggers": [
                {"subtype": "single_press", "payload": "1"},
                {"subtype": "double_press", "payload": "2"}
            ]
        }"#;
        let data_def: LoRaWANDataDef = serde_json::from_str(json).unwrap();

        assert_eq!(data_def.platform, "device_automation");
        let triggers = data_def.triggers.unwrap();
        assert_eq!(triggers.len(), 2);
        assert_eq!(triggers[0].subtype, "single_press");
        assert_eq!(triggers[0].payload, "1");
        assert_eq!(triggers[1].subtype, "double_press");
        assert_eq!(triggers[1].payload, "2");
    }

    #[test]
    fn test_lorawan_def_full_deserialization() {
        // LoRaWanAliases uses YAML tags for enum variants
        let yaml = r#"
manufacturer: TestCorp
model: TestSensor
support_url: https://example.com/docs
aliases:
  - !DevEUI "^TEST.*"
data_fields:
  - name: temperature
    friendly_name: Temperature
    unit_of_measurement: "°C"
    device_class: temperature
timings:
  max_eventtime: 600
  max_devtime: 300
options:
  has_gps: false
  ignore_generated_values: false
  has_command_topic: true
"#;
        let def: LoRaWANDef = serde_yml::from_str(yaml).unwrap();

        assert_eq!(def.manufacturer, "TestCorp");
        assert_eq!(def.model, "TestSensor");
        assert_eq!(def.support_url, Some("https://example.com/docs".to_string()));
        assert_eq!(def.aliases.len(), 1);
        assert_eq!(def.data_fields.len(), 1);
        assert_eq!(def.data_fields[0].name, "temperature");
        assert_eq!(def.data_fields[0].unit_of_measurement, Some("°C".to_string()));
        assert!(def.timings.is_some());
        assert!(def.options.is_some());
        assert!(def.options.as_ref().unwrap().has_command_topic);
    }

    #[test]
    fn test_zridh_downlink_struct_serialization() {
        // Test the ZriDhLoRaDownlink struct serialization used in handle_downlink_lora_complex
        #[derive(serde::Serialize)]
        #[allow(non_snake_case)]
        struct ZriDhLoRaDownlink {
            devEUI: String,
            data: String,
            port: u64,
            imme: bool,
            confirmed: bool,
            retriesLeft: u32,
        }

        let downlink = ZriDhLoRaDownlink {
            devEUI: "0018B20000001234".to_string(),
            data: "AABBCCDD".to_string(),
            port: 10,
            imme: true,
            confirmed: true,
            retriesLeft: 3,
        };

        let json = serde_json::to_string(&downlink).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();

        assert_eq!(parsed["devEUI"], "0018B20000001234");
        assert_eq!(parsed["data"], "AABBCCDD");
        assert_eq!(parsed["port"], 10);
        assert_eq!(parsed["imme"], true);
        assert_eq!(parsed["confirmed"], true);
        assert_eq!(parsed["retriesLeft"], 3);
    }

    #[test]
    fn test_lorawan_data_def_with_map_key() {
        let json = r#"{
            "name": "internal_temp",
            "friendly_name": "Temperature",
            "map_key": "temperature"
        }"#;
        let data_def: LoRaWANDataDef = serde_json::from_str(json).unwrap();

        assert_eq!(data_def.name, "internal_temp");
        assert_eq!(data_def.map_key, Some("temperature".to_string()));
    }

    #[test]
    fn test_lorawan_data_def_with_extra_keys() {
        let json = r#"{
            "name": "sensor_value",
            "friendly_name": "Sensor",
            "e2m_extra_keys": ["raw_value", "calibration_offset"]
        }"#;
        let data_def: LoRaWANDataDef = serde_json::from_str(json).unwrap();

        let extra_keys = data_def.e2m_extra_keys.unwrap();
        assert_eq!(extra_keys.len(), 2);
        assert_eq!(extra_keys[0], "raw_value");
        assert_eq!(extra_keys[1], "calibration_offset");
    }

    #[test]
    fn test_lorawan_data_def_persist_default_true() {
        let json = r#"{"name": "test", "friendly_name": "Test"}"#;
        let data_def: LoRaWANDataDef = serde_json::from_str(json).unwrap();

        assert!(data_def.e2m_persist);
    }

    #[test]
    fn test_lorawan_data_def_persist_false() {
        let json = r#"{"name": "test", "friendly_name": "Test", "e2m_persist": false}"#;
        let data_def: LoRaWANDataDef = serde_json::from_str(json).unwrap();

        assert!(!data_def.e2m_persist);
    }
}
