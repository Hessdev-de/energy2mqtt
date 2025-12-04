use std::{collections::HashMap, sync::Arc, time::Duration};

use base64::Engine;
use log::{debug, error, info};
use rumqttc::{AsyncClient, Event, MqttOptions, Packet};
use serde::{Deserialize, Serialize};
use serde_json::Map;
use tokio::{process::Command, sync::{Mutex, mpsc::Sender}, task::JoinHandle};

use crate::{CONFIG, MeteringData, StoredData, config::{ConfigBases, ConfigChange, ConfigOperation, ZennerDatahubConfig}, get_id, metering_zennerdatahub::handle_lora::{LoRaWANDef, find_defintion}, models::DeviceProtocol, mqtt::{Transmission, home_assistant::{HaComponent2, HaSensor, get_command_topic}, publish_protocol_count}};

mod handle_lora;

#[derive(Serialize, Deserialize)]
pub struct PayLoadMessage {
    proto: String,      /* lora, nbiot, ... */
    id: String,         /* ID of the device  */
    payload: String,    /* The real payload to transmit */
    #[serde(skip_serializing_if = "Option::is_none")]
    port: Option<u64>,  /* Frameport or NBIOT device port  */
    #[serde(skip_serializing_if = "Option::is_none")]
    imme: Option<bool>, /* Some protocols defer sending the data if imme is set we want to push them directly */
}

impl PayLoadMessage {
    pub fn new(proto: &str, id: &String, payload: &String, port: Option<u64>, imme: Option<bool>) -> Self {
        PayLoadMessage {
            proto: proto.to_string(),
            id: id.clone(),
            payload: payload.clone(),
            port,
            imme,
        }
    }

    pub fn to_ha_discover_payload(&self) -> String {
        let ha_str = serde_json::to_string(self).unwrap_or("".to_string());
        let mut buf = String::new();
        base64::engine::general_purpose::STANDARD.encode_string(ha_str, &mut buf);
        buf
    }
}

pub struct ZennerDatahubManager {
    sender: Sender<Transmission>,
    config_change: tokio::sync::broadcast::Receiver<ConfigChange>,
    threads: Vec<JoinHandle<()>>,
    config: Vec<ZennerDatahubConfig>,
}

#[derive(Default, Serialize, Clone)]
struct ZennerDatahubStats {
    received_all: u64,
    received_valid: u64,
    received_lorawan: u64,
    error_eventtime_old: u64,
    error_devicetime_old: u64,
    data_published: u64,
}

impl ZennerDatahubStats {
    pub fn inc(&mut self, field: &str) {
        match field {
            "received_all" => { self.received_all += 1; },
            "received_valid" => { self.received_valid += 1; },
            "received_lorawan" => { self.received_lorawan += 1; },
            "error_eventtime_old" =>  {self.error_eventtime_old += 1; },
            "error_devicetime_old" =>  {self.error_devicetime_old += 1; },
            "data_published" =>  {self.data_published += 1; },
            _ => { debug!("Unkown statistic field: {field}"); }
        }
    }
}

struct ZennerDatahubData  {
    known_devices: HashMap<String, handle_lora::LoRaWANDef>,
    sender: Sender<Transmission>,
    callback_sender: Sender<(String, String)>,
    stats: ZennerDatahubStats,
    defs: HashMap<String /* dev_eui */, LoRaWANDef /* definition */>,
}

impl ZennerDatahubData {
    pub fn new(sender: Sender<Transmission>, callback_sender: Sender<(String, String)>) -> Self {
        return ZennerDatahubData { 
            known_devices: HashMap::new(),
            sender,
            callback_sender,
            stats: ZennerDatahubStats::default(),
            defs: HashMap::new(),
        }
    }
}

impl ZennerDatahubManager {
    pub fn new(sender: Sender<Transmission>) -> Self {
        let config: Vec<ZennerDatahubConfig> = crate::get_config_or_panic!("zridh", ConfigBases::ZRIDH);

        return ZennerDatahubManager {
            sender,
            config_change: CONFIG.read().unwrap().get_change_receiver(),
            threads: Vec::new(),
            config,
        };
    }

    pub async fn start_thread(&mut self) -> ! {
        /* There may be not config to start with, so sleep until there is  */
        if self.config.len() == 0 {
            info!("No ZENNER Datahub config found, waiting for a config change to wake me up");
            loop {
                let change = self.config_change.recv().await.unwrap();
                if change.operation != ConfigOperation::ADD || change.base != "zridh" {
                    continue;
                }
                
                /* we need to read the config now as this change is about our part of the code */
                break;
            }
        }

        info!("Started ZENNER Datahub configuration");
        loop {
            let mut device_count = 0;

            for conf in self.config.iter() {
                 if !conf.enabled {
                    info!("ZENNER Datahub connection to {}:{} is disabled", conf.broker_host, conf.broker_port);
                    continue;
                }

                device_count += 1;

                let (callback_sender, mut callback_receiver) = tokio::sync::mpsc::channel(10);

                info!("Starting MQTT connection to {}:{}", conf.broker_host, conf.broker_port);
                let mut mqttoptions   = MqttOptions::new(
                                                    conf.client_name.clone(),
                                                    conf.broker_host.clone(),
                                                    conf.broker_port);

                mqttoptions.set_keep_alive(Duration::from_secs(5));
                mqttoptions.set_credentials(conf.broker_user.clone(), conf.broker_pass.clone());

                let (client, mut eventloop) = AsyncClient::new(mqttoptions, 10);
                let reconnect_c = client.clone();
                let host = conf.broker_host.clone();
                let port = conf.broker_port;
                let lconfig = conf.clone();
                
                let data = Arc::new(Mutex::new(ZennerDatahubData::new(self.sender.clone(), callback_sender.clone())));
                let data_clone = data.clone();
                let name_clone = conf.name.clone();

                let handle = tokio::spawn( async move {
                    info!("[{name_clone} {host}:{port}] MQTT Eventloop starting ...");
                    let manager_name = format!("e2m_zridh_manager_{name_clone}");
                    let base_topic = lconfig.base_topic;
                    loop {
                        match eventloop.poll().await {
                            Ok(Event::Incoming(Packet::Publish(p))) => {
                                let topic = p.topic;
                                let payload = String::from_utf8(p.payload.to_vec()).unwrap();

                                debug!("[{name_clone} {host}:{port}] Received {topic} -> {payload:?}");

                                /* To save memory zenner datahub is free to invalidate a topic */
                                if payload == "" {
                                    continue;
                                }

                                /* Check for the relevant datatype */
                                if topic.starts_with(&format!("{base_topic}/R/lora/")) {
                                    let pclone = payload.clone();
                                    let viaclone = manager_name.clone();
                                    let mut dataclone = data_clone.clone();
                                    let nclone = name_clone.clone();
                                    tokio::spawn(async move {
                                        handle_lora::handle_incoming_lora(&mut dataclone, nclone, pclone, viaclone).await
                                    });
                                } else {
                                    info!("Unknown datatype received");
                                }

                                /* Update our stats */
                                let mut meter_data = MeteringData::new().unwrap();
                                meter_data.meter_name = name_clone.clone();
                                meter_data.protocol = DeviceProtocol::ZennerDatahub;
                                meter_data.id = get_id(format!("zridh_manager"), &name_clone);
                                meter_data.state_topic_base = "zridh_manager".to_string(); // We need to make sure that we handle our sub protocol right at this point
                                meter_data.metered_values = serde_json::to_value(data_clone.lock().await.stats.clone()).unwrap_or_default().as_object().unwrap_or(&Map::new()).clone();
                                let _ = data_clone.lock().await.sender.send(Transmission::Metering(meter_data)).await;
                            },
                            Ok(Event::Incoming(Packet::ConnAck(_))) => {
                                info!("[{name_clone} {host}:{port}] Connected, resubscribing basetopic read path");
                                let _ = reconnect_c.subscribe(format!("{base_topic}/R/lora/+/value"),rumqttc::QoS::AtLeastOnce).await;
                                let _ = reconnect_c.subscribe(format!("{base_topic}/R/omsraw/+/value"),rumqttc::QoS::AtLeastOnce).await;
                                let _ = reconnect_c.subscribe(format!("{base_topic}/R/nbiot/+/value"),rumqttc::QoS::AtLeastOnce).await;

                                let manager = "__manager__".to_string();
                                /* Check if we need to add our manager instance for linkage */
                                if !data_clone.lock().await.known_devices.contains_key(&manager) {
                                    let mut disc = HaSensor::new("zridh_manager".to_string(), 
                                                                            name_clone.clone(),
                                                                            Some("Energy2Mqtt".to_string()),
                                                                            Some("ZENNER Datahub Bridge".to_string()));

                                    let cmp = HaComponent2::new().name("Packages received".to_string());
                                    disc.add_cmp("received_all".to_string(), cmp);

                                    let cmp = HaComponent2::new().name("Packages valid".to_string());
                                    disc.add_cmp("received_valid".to_string(), cmp);

                                    let cmp = HaComponent2::new().name("Packages LoRaWAN".to_string());
                                    disc.add_cmp("received_lorawan".to_string(), cmp);

                                    let cmp = HaComponent2::new().name("Old eventtime".to_string());
                                    disc.add_cmp("error_eventtime_old".to_string(), cmp);

                                    let cmp = HaComponent2::new().name("Old devicetime".to_string());
                                    disc.add_cmp("error_devicetime_old".to_string(), cmp);

                                    let cmp = HaComponent2::new().name("Data published".to_string());
                                    disc.add_cmp("data_published".to_string(), cmp);

                                    let _ = data_clone.lock().await.sender.send(Transmission::AutoDiscovery2(disc)).await;

                                    data_clone.lock().await.known_devices.insert(manager, LoRaWANDef::default());
                                }
                            },
                            Ok(Event::Incoming(Packet::SubAck(_))) => {
                                debug!("A subscription ack was received");
                            },
                            Ok(_) => {}, 
                            Err(e) => {
                                error!("[{name_clone} {host}:{port}] Error in MQTT {:?}", e);
                            }
                        }
                    }
                });

                self.threads.push(handle);

                /* TODO: thread to handle services and downlinks */

                let name_clone = conf.name.clone();
                let sender_clone = self.sender.clone();
                let publish_c = client.clone();
                let lconfig = conf.clone();
                let data_clone = data.clone();

                let handle = tokio::spawn(async move {
                    let command_topic = get_command_topic(&"zridh".to_string(), &name_clone, &"json".to_string());
                    let register = Transmission::Subscribe(crate::mqtt::SubscribeData {
                        topic: command_topic.clone(),
                        sender: callback_sender.clone()
                    });

                    let _ = sender_clone.send(register).await;


                    info!("Waiting for ZENNER Datahub downlink messages instance {name_clone}");
                    while let Some((topic, message)) = callback_receiver.recv().await {

                        let decoded: Vec<u8>;

                        /* Call for some of the other workers */
                        if topic != command_topic {
                            /* we need to know the device id of that topic */
                            let (dev_eui, command, proto) = crate::mqtt::home_assistant::get_dev_cmd_proto_from_topic(&topic);

                            if let Some(def) = find_defintion(&dev_eui, 
                                                            &mut data_clone.lock().await.defs,
                                                            None).await {
                                if let Some(cmd) = def.e2m_command_handler {
                                    info!("Command handler for {dev_eui} found as {cmd}");

                                    let mut handler = Command::new(cmd.clone());
                                    /* The command of home assistant */
                                    handler.arg(command);
                                    /* device is the device if needed may be also used for logging */
                                    handler.env("DEVICE", &dev_eui);
                                    /* Payload is the Home Assistant command payload may be something like 21.2 as temperature */
                                    handler.env("PAYLOAD", &message);

                                    /* We need to now the states which are known  */
                                    let d = StoredData::load(proto.clone(), &dev_eui).await.get_map();
                                    for (key, value) in d {
                                        let key = key.replace(" ", "_").to_ascii_uppercase();
                                        let mut v = serde_json::to_string(&value).unwrap_or("unknown".to_string());
                                        v = v.trim_matches('"').to_string();

                                        handler.env(format!("DEV_{key}"), v);
                                    }

                                    match handler.output().await {
                                        Ok(o) => {
                                            if o.status.success() {
                                                /* Outut mut contain  */
                                                decoded = o.stdout;
                                                info!("Got Payload: {}", String::from_utf8(decoded.clone()).unwrap_or_default());
                                            } else {
                                                error!("{cmd} returned error: {} -> {}", o.status.code().unwrap_or(0), 
                                                                String::from_utf8(o.stderr.clone()).unwrap_or_default());
                                                continue;
                                            }
                                        },
                                        Err(e) => {
                                            error!("Could not run {cmd}: {e:?}");
                                            continue;
                                        },
                                    }
                                } else {
                                    error!("No command handler for {dev_eui}::{command}");
                                    continue;
                                }
                            } else {
                                error!("Unknown topic received: {topic} -> {message}");
                                continue;
                            }
                        } else {
                            /* We only use JSON encoded downlink payloads, so use them */
                            let d = base64::engine::general_purpose::STANDARD.decode(&message);
                            if d.is_err() {
                                error!("Non base64 decoded value: {message}");
                                continue;
                            }

                            decoded = d.unwrap();
                        }

                        let payload = match serde_json::from_slice::<PayLoadMessage>(decoded.as_slice()) {
                            Ok(d) => d,
                            Err(e) => {
                                error!("Bogus Payload message received from home assistant: {decoded:?} -> {e:?}");
                                continue;
                            },
                        };

                        if payload.proto == "lora" {
                            handle_lora::handle_downlink_lora_complex(&payload, &publish_c, &lconfig).await;
                        }
                    }

                    error!("ZENNER Datahub downlink message instance {name_clone} exited loop");
                });

                self.threads.push(handle);

            }

            publish_protocol_count(&self.sender, "zrihd", device_count).await;

            info!("All ZENNER Datahub {device_count} devices setup, waiting for config changes");

            loop {
                let change = self.config_change.recv().await.unwrap();
                if change.base == "zridh" {
                    break;
                }
            }

            /* We are waken up because some of our config changed so stop the threads and start over */
            info!("ZENNER Datahub is stopping threads");
            for thread in self.threads.iter() {
                thread.abort();
            }

            self.threads.clear();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_payload_message_new() {
        let msg = PayLoadMessage::new("lora", &"dev123".to_string(), &"AABBCC".to_string(), Some(10), Some(true));

        assert_eq!(msg.proto, "lora");
        assert_eq!(msg.id, "dev123");
        assert_eq!(msg.payload, "AABBCC");
        assert_eq!(msg.port, Some(10));
        assert_eq!(msg.imme, Some(true));
    }

    #[test]
    fn test_payload_message_new_without_optionals() {
        let msg = PayLoadMessage::new("nbiot", &"sensor456".to_string(), &"1234".to_string(), None, None);

        assert_eq!(msg.proto, "nbiot");
        assert_eq!(msg.id, "sensor456");
        assert_eq!(msg.payload, "1234");
        assert_eq!(msg.port, None);
        assert_eq!(msg.imme, None);
    }

    #[test]
    fn test_payload_message_to_ha_discover_payload() {
        let msg = PayLoadMessage::new("lora", &"dev123".to_string(), &"AABBCC".to_string(), Some(10), None);
        let encoded = msg.to_ha_discover_payload();

        // Decode and verify
        let decoded = base64::engine::general_purpose::STANDARD.decode(&encoded).unwrap();
        let decoded_str = String::from_utf8(decoded).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&decoded_str).unwrap();

        assert_eq!(parsed["proto"], "lora");
        assert_eq!(parsed["id"], "dev123");
        assert_eq!(parsed["payload"], "AABBCC");
        assert_eq!(parsed["port"], 10);
        // imme should not be present when None due to skip_serializing_if
        assert!(parsed.get("imme").is_none());
    }

    #[test]
    fn test_payload_message_serialization_skips_none() {
        let msg = PayLoadMessage::new("lora", &"test".to_string(), &"data".to_string(), None, None);
        let json = serde_json::to_string(&msg).unwrap();

        // port and imme should not appear in JSON when None
        assert!(!json.contains("port"));
        assert!(!json.contains("imme"));
    }

    #[test]
    fn test_zenner_datahub_stats_default() {
        let stats = ZennerDatahubStats::default();

        assert_eq!(stats.received_all, 0);
        assert_eq!(stats.received_valid, 0);
        assert_eq!(stats.received_lorawan, 0);
        assert_eq!(stats.error_eventtime_old, 0);
        assert_eq!(stats.error_devicetime_old, 0);
        assert_eq!(stats.data_published, 0);
    }

    #[test]
    fn test_zenner_datahub_stats_inc_received_all() {
        let mut stats = ZennerDatahubStats::default();
        stats.inc("received_all");
        assert_eq!(stats.received_all, 1);
        stats.inc("received_all");
        assert_eq!(stats.received_all, 2);
    }

    #[test]
    fn test_zenner_datahub_stats_inc_received_valid() {
        let mut stats = ZennerDatahubStats::default();
        stats.inc("received_valid");
        assert_eq!(stats.received_valid, 1);
    }

    #[test]
    fn test_zenner_datahub_stats_inc_received_lorawan() {
        let mut stats = ZennerDatahubStats::default();
        stats.inc("received_lorawan");
        assert_eq!(stats.received_lorawan, 1);
    }

    #[test]
    fn test_zenner_datahub_stats_inc_error_eventtime_old() {
        let mut stats = ZennerDatahubStats::default();
        stats.inc("error_eventtime_old");
        assert_eq!(stats.error_eventtime_old, 1);
    }

    #[test]
    fn test_zenner_datahub_stats_inc_error_devicetime_old() {
        let mut stats = ZennerDatahubStats::default();
        stats.inc("error_devicetime_old");
        assert_eq!(stats.error_devicetime_old, 1);
    }

    #[test]
    fn test_zenner_datahub_stats_inc_data_published() {
        let mut stats = ZennerDatahubStats::default();
        stats.inc("data_published");
        assert_eq!(stats.data_published, 1);
    }

    #[test]
    fn test_zenner_datahub_stats_inc_unknown_field() {
        let mut stats = ZennerDatahubStats::default();
        // Should not panic, just log debug message
        stats.inc("unknown_field");
        // All fields should remain 0
        assert_eq!(stats.received_all, 0);
        assert_eq!(stats.received_valid, 0);
        assert_eq!(stats.received_lorawan, 0);
        assert_eq!(stats.error_eventtime_old, 0);
        assert_eq!(stats.error_devicetime_old, 0);
        assert_eq!(stats.data_published, 0);
    }

    #[test]
    fn test_zenner_datahub_stats_inc_all_fields() {
        let mut stats = ZennerDatahubStats::default();

        stats.inc("received_all");
        stats.inc("received_valid");
        stats.inc("received_lorawan");
        stats.inc("error_eventtime_old");
        stats.inc("error_devicetime_old");
        stats.inc("data_published");

        assert_eq!(stats.received_all, 1);
        assert_eq!(stats.received_valid, 1);
        assert_eq!(stats.received_lorawan, 1);
        assert_eq!(stats.error_eventtime_old, 1);
        assert_eq!(stats.error_devicetime_old, 1);
        assert_eq!(stats.data_published, 1);
    }

    #[test]
    fn test_zenner_datahub_stats_serialization() {
        let mut stats = ZennerDatahubStats::default();
        stats.inc("received_all");
        stats.inc("received_all");
        stats.inc("data_published");

        let json = serde_json::to_string(&stats).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();

        assert_eq!(parsed["received_all"], 2);
        assert_eq!(parsed["data_published"], 1);
        assert_eq!(parsed["received_valid"], 0);
    }
}
