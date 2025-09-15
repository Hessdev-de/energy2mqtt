use serde_json::Value;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tokio::time::sleep;
use crate::config::{ConfigChange, ConfigOperation, VictronConfig};
use crate::models::DeviceProtocol;
use crate::mqtt::{publish_protocol_count, Transmission};
use crate::config::ConfigBases;
use crate::{get_config_or_panic, get_id, get_unix_ts, MeteringData, CONFIG};
use log::{debug, error, info};
use tokio::sync::mpsc::Sender;
use rumqttc::{AsyncClient, Event, MqttOptions, Packet};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

pub mod utils;
pub mod detect;

pub struct VictronManager {
    sender: Sender<Transmission>,
    config_change: tokio::sync::broadcast::Receiver<ConfigChange>,
    threads: Vec<JoinHandle<()>>,
    config: Vec<VictronConfig>,
}

#[derive(Clone, Debug)]
pub struct Topic {
    pub payload: String,
    pub updated: u64,
    pub json_key: String,
    pub need_read: bool,
}

impl Topic {
    pub fn new(payload: String) -> Self {
        return Topic {
            payload: payload,
            updated: get_unix_ts(),
            json_key: "".to_string(),
            need_read: true,
        }
    }

    pub fn new_with_key(payload: String, key: String) -> Self {
        return Topic {
            payload: payload,
            updated: get_unix_ts(),
            json_key: key,
            need_read: true,
        }
    }

    pub fn create_from(old: &Topic, payload: String) -> Self {
        return Topic {
            payload: payload,
            updated: get_unix_ts(),
            json_key: old.json_key.clone(),
            need_read: true,
        }
    }

    pub fn empty() -> Self {
        return Topic {
            payload: "".to_string(),
            updated: 0,
            json_key: "".to_string(),
            need_read: true,
        }
    }

}

pub struct VictronData {
    pub portal_id: String,
    pub read_topics: Vec<String>,
    pub topic_mapping: HashMap<String, Option<Topic>>,
    pub conf: VictronConfig
}

impl VictronData {
    pub fn new(conf: &VictronConfig) -> Self {
        return VictronData {
            portal_id: "".to_string(),
            read_topics: Vec::new(),
            topic_mapping: HashMap::new(),
            conf: conf.clone()
        };
    }

    pub fn set_portal(&mut self, portal: String) {
        self.portal_id = portal;
    }

    pub fn add_read_topic(&mut self, topic: String) {
        let topic = topic.replacen("N/", "R/",1);
        if self.read_topics.contains(&topic) {
            return;
        }

        self.read_topics.push(topic);
    }
}

impl VictronManager {
    pub fn new(sender: Sender<Transmission>) -> Self {
        let config: Vec<VictronConfig> = get_config_or_panic!("victron", ConfigBases::Victron);

        return VictronManager {
            sender,
            config_change: CONFIG.read().unwrap().get_change_receiver(),
            threads: Vec::new(),
            config,
        };
    }

    pub async fn start_thread(&mut self) -> ! {
        /* There may be not config to start with, so sleep until there is  */
        if self.config.len() == 0 {
            info!("No Victron devices found, waiting for a config change to wake me up");
            loop {
                let change = self.config_change.recv().await.unwrap();
                if change.operation != ConfigOperation::ADD || change.base != "modbus" {
                    continue;
                }
                
                /* we need to read the config now as this change is about our part of the code */
                break;
            }
        }

        info!("Started Victron configuration");
        loop {
            let mut device_count = 0;

            for conf in self.config.iter() {

                if !conf.enabled {
                    info!("Victron connection to {}:{} is disabled", conf.broker_host, conf.broker_port);
                    continue;
                }

                device_count += 1;
                info!("Starting MQTT connection to {}:{}", conf.broker_host, conf.broker_port);

                let mut mqttoptions   = MqttOptions::new(
                                                    conf.client_name.clone(),
                                                    conf.broker_host.clone(),
                                                    conf.broker_port);

                mqttoptions.set_keep_alive(Duration::from_secs(5));

                let (client, mut eventloop) = AsyncClient::new(mqttoptions, 10);
                let reconnect_c = client.clone();
                let host = conf.broker_host.clone();
                let port = conf.broker_port;
                
                /* TODO: find a better way to do this */
                let _ = client.subscribe("N/+/system/0/Serial",rumqttc::QoS::AtLeastOnce).await;

                let data = Arc::new(Mutex::new(VictronData::new(conf)));
                let data_clone = data.clone();

                let mut handle = tokio::spawn( async move {
                    info!("[{host}:{port}] MQTT Eventloop starting ...");

                    let mut last_error = String::new();
                    let mut counter = 0;
                    loop {
                        match eventloop.poll().await {
                            Ok(Event::Incoming(Packet::Publish(p))) => {
                                let topic = p.topic;
                                let payload = String::from_utf8(p.payload.to_vec()).unwrap();

                                /* Victron resets data if not read again, make sure to igonore it! */
                                if payload == "" {
                                    continue;
                                }

                                debug!("[{host}:{port}] Received {topic} -> {payload:?}");

                                if topic.ends_with("/system/0/Serial") {
                                    /* We found our portal id id */
                                    let parts: Vec<&str> = topic.split("/").collect();
                                    if parts.len() > 2 {
                                        debug!("[{host}:{port}] Portal id found: {}", parts[1]);
                                        data_clone.lock().await.set_portal(parts[1].to_string());
                                    }
                                }

                                let mut data = data_clone.lock().await;
                                if data.topic_mapping.contains_key(&topic) {
                                        let tdata = match data.topic_mapping.get(&topic).unwrap() {
                                            Some(old) => { Topic::create_from(old, payload.clone())},
                                            None => { Topic::new(payload.clone()) },
                                        };
                        
                                        data.topic_mapping.insert(
                                                    topic, 
                                                    Some(tdata)
                                                    );
                                } else {
                                    debug!("We are not handling topic {topic} but received data");
                                }
                            },
                            Ok(Event::Incoming(Packet::ConnAck(_))) => {
                                info!("[{host}:{port}] Connected, resubscribing everything");
                                let _ = reconnect_c.subscribe("N/+/system/0/Serial",rumqttc::QoS::AtLeastOnce).await;
                                loop {
                                    match data_clone.try_lock() {
                                        Ok(data) => {                                           
                                            for topic in data.topic_mapping.keys() {
                                                let c = reconnect_c.clone();
                                                let topic = topic.clone();
                                                tokio::spawn(async move {
                                                    let _ = c.subscribe(topic, rumqttc::QoS::AtLeastOnce).await;
                                                });
                                            };
                                            break;
                                        },
                                        Err(_) => {
                                            debug!("Can not get mutex, waiting!");
                                        },
                                    }
                                }
                            },
                            Ok(Event::Incoming(Packet::SubAck(_))) => {
                                debug!("A subscription ack was received");
                            },
                            Ok(_) => {},
                            Err(e) => {
                                if e.to_string() == last_error {
                                    /* Rate limting */
                                    counter += 1;
                                    if counter < 100_000 {
                                        continue;
                                    }
                                }

                                counter = 0;
                                error!("[{host}:{port}] Error in MQTT {:?}", e);
                                last_error = e.to_string();
                            }
                        }
                    }
                });
                
                self.threads.push(handle);


                let host = conf.broker_host.clone();
                let port = conf.broker_port;
                let send_dupe = self.sender.clone();

                sleep(Duration::from_secs(10)).await;
                info!("[{host}:{port}] Eventloop started, now starting data processing");

                /* Our mqtt event loop is up and running, now we need to implement the application itself */
                handle = tokio::spawn(async move { 
                    /* We wait 10 seconds until we get our portal id */
                    let duration = Duration::from_secs(10);
                    sleep(Duration::from_secs(10)).await;

                    loop {
                        let portal_id = utils::get_portal(&data).await;
                        if  portal_id == "" {
                            info!("[{host}:{port}] Portal id not known up until now");
                            sleep(duration).await;
                            continue;
                        }

                        let _ = detect::run_initial_detection(&client, &data, &send_dupe, format!("[{host}:{port}]")).await;
                        
                        loop {
                            /* Trigger reading, but copy the list because we are not allowed to keep the lock */
                            let read_topics = data.lock().await.read_topics.clone();
                            let mut wait_time = 0u64;
                            for topic in read_topics {
                                debug!("Starting to read from {topic}");
                                let _ = client.publish(topic, 
                                            rumqttc::QoS::AtLeastOnce, false, "").await;
                                /* Each topic adds 1 second to the read timeout because we should never flood the GX device */
                                wait_time += 1;
                            }

                            sleep(Duration::from_secs(wait_time)).await;
                            
                            let config = data.lock().await.conf.clone();

                            let mut meter_data = MeteringData::new().unwrap();
                            meter_data.meter_name = config.name.clone();
                            meter_data.protocol = DeviceProtocol::Victron;
                            meter_data.id = get_id("victron".to_string(), &config.name.clone());
                            meter_data.transmission_time = get_unix_ts();
                            meter_data.metered_time = meter_data.transmission_time;
        
                            let topics = data.lock().await.topic_mapping.clone();

                            for entry in topics.iter() {
                                let tname = entry.0.clone();

                                debug!("Building data for {tname}");
                                if entry.1.is_none() {
                                    debug!("[{host}:{port} {tname}]  Topic is none");
                                    continue;
                                }

                                let tdata = entry.1.clone().unwrap();

                                /* skip internal json data */
                                if tdata.json_key.starts_with("_") { continue; }

                                /* We need to parse the json messages, we do it here because we have time */
                                let doc = serde_json::from_str::<Value>(&tdata.payload);
                                match doc {
                                    Err(_) => {
                                        continue;
                                    }
                                    Ok(v) => {
                                        match v.get("value") {
                                            Some(v) => { meter_data.metered_values.insert(tdata.json_key, v.clone()); }
                                            None => { info!("[{host}:{port} {tname}] Malformed JSON found") }
                                        }
                                        
                                    }
                                }
                            }
                            
                            /* send the meter reading to the MQTT thread */
                            let _ = send_dupe.send(Transmission::Metering(meter_data)).await;

                            sleep(duration).await;
                        }
                    }
                });

                self.threads.push(handle);
            }
        
            publish_protocol_count(&self.sender, "victron", device_count).await;

            info!("All Victron {device_count} devices setup, waiting for config changes");

            loop {
                let change = self.config_change.recv().await.unwrap();
                if change.base == "victron" {
                    break;
                }
            }

            /* We are waken up because some of our config changed so stop the threads and start over */
            info!("Victron is stopping threads");
            for thread in self.threads.iter() {
                thread.abort();
            }

            self.threads.clear();
        }
    }
}
