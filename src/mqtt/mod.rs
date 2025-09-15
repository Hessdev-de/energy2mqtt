pub mod internal_commands;
pub mod ha_interface;

use std::collections::HashMap;
use lazy_static::lazy_static;
use tokio::sync::RwLock;
use std::io::Error;
use crate::mqtt::ha_interface::HaDiscover;
use crate::{config::ConfigBases, models::DeviceProtocol};
use crate::{get_config_or_panic, CONFIG};
use log::{debug, error, info};
use tokio::sync::mpsc::{Receiver, Sender};
use serde::{Serialize, Deserialize};
use serde_json;
use rumqttc::{AsyncClient, Event, MqttOptions, Packet, QoS};
use std::time::{Duration, Instant};


#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MqttConnectionStatus {
    Connected,
    Disconnected,
    Reconnecting,
    Error(String),
}

#[derive(Clone)]
pub struct MqttHealthStatus {
    pub status: MqttConnectionStatus,
    pub last_connected: Option<Instant>,
    pub last_message_sent: Option<Instant>,
    pub last_message_received: Option<Instant>,
    pub connection_attempts: u64,
}

#[derive(Clone)]
pub struct AppStatus {
    pub start_time: Instant,
    pub mqtt_health: MqttHealthStatus,
}

impl MqttHealthStatus {
    pub fn new() -> Self {
        Self {
            status: MqttConnectionStatus::Disconnected,
            last_connected: None,
            last_message_sent: None,
            last_message_received: None,
            connection_attempts: 0,
        }
    }
}

impl AppStatus {
    pub fn new() -> Self {
        Self {
            start_time: Instant::now(),
            mqtt_health: MqttHealthStatus::new(),
        }
    }

    pub fn uptime_seconds(&self) -> u64 {
        self.start_time.elapsed().as_secs()
    }
}

#[derive(Serialize, Deserialize)]
pub enum TranmissionValueType{
    Now,
    Daily,
    Hourly,
    Monthly,
    KeyValue
}

#[derive(Serialize, Deserialize)]
pub struct MeteringData {
    pub id: String,
    pub meter_name: String,
    pub tenant: String, 
    pub protocol: DeviceProtocol,
    pub transmission_time: u64,
    pub transmission_type: TranmissionValueType,
    pub metered_time: u64,
    pub metered_values: serde_json::Map<String, serde_json::Value>
}

impl MeteringData {
    pub fn new() -> Result<Self, Error> {
        return Ok(MeteringData {
            id: "".to_string(),
            tenant: "".to_string(),
            meter_name: "".to_string(),
            protocol: DeviceProtocol::Unknown, 
            transmission_time: 0,
            transmission_type: TranmissionValueType::Now,
            metered_time: 0, 
            metered_values: serde_json::Map::new() 
        });
    }
}


pub struct CommandData {
    topic: String,
    value: String,
    retain: bool,
}

pub struct PublishData {
    pub topic: String,
    pub payload: String,
    pub qos: u8,
    pub retain: bool,
}

pub struct SubscribeData {
    pub topic: String,
    pub sender: tokio::sync::mpsc::Sender<String>
}

pub enum Transmission {
    Metering(MeteringData),
    AutoDiscovery(HaDiscover),
    Command(CommandData),
    Subscribe(SubscribeData),
    Publish(PublishData)
}

pub struct MqttManager {
    rx: Receiver<Transmission>,
    exit_thread: bool,
    client: AsyncClient,
}

pub struct Callbacks {
    calls: HashMap<String, Vec<tokio::sync::mpsc::Sender<String>>>,
}

impl Callbacks {
    pub fn new() -> Self {
        return Callbacks { calls: HashMap::new() };
    }

    pub fn insert(&mut self, topic: String, callback: tokio::sync::mpsc::Sender<String>) {
        if !self.calls.contains_key(&topic) {
            debug!("Adding new vector to topic {topic}");
            self.calls.insert(topic, vec![callback]);
        } else {
            debug!("Adding a new element to known vector at topic {topic}");
            let v = self.calls.get_mut(&topic).unwrap();
            v.push(callback);
        }
    }

    pub async fn send(&self, topic: String, payload: String) {
        if !self.calls.contains_key(&topic) {
            debug!("Send for unkonwn topic {topic}");
            return;
        }

        let v = self.calls.get(&topic).unwrap();
        for call in v {
            debug!("Sending to callback: {payload}");
            let _ = call.send(payload.clone()).await.unwrap();
        }
    }

    pub async fn get_topics(&self) -> Vec<String> {
        let mut d: Vec<String>  = Vec::new();
        for key in self.calls.keys() {
            d.push(key.clone());
        }
        return d;
    }

}

lazy_static! {
    pub static ref CALLBACKS: RwLock<Callbacks> = RwLock::new(Callbacks::new());
    pub static ref APP_STATUS: RwLock<AppStatus> = RwLock::new(AppStatus::new());
}

impl MqttManager {
    pub fn new() -> Result<(Self, Sender<Transmission>), Error> {
        let (mtx,mrx) = tokio::sync::mpsc::channel(100);

        info!("MQTT connection starting up");
        let config = get_config_or_panic!("mqtt", ConfigBases::Mqtt);
        let mut mqttoptions   = MqttOptions::new(config.client_name.clone(), config.host.clone(), config.port);
        mqttoptions.set_keep_alive(Duration::from_secs(5));
        mqttoptions.set_credentials(config.user.clone(), config.pass.clone());

        let (client, mut eventloop) = AsyncClient::new(mqttoptions, 10);

        // Spawn a new thread to handle the incomming commands
        let reconnect_c = client.clone();
        tokio::spawn( async move {
            info!("MQTT Eventloop started");
            loop {
                match eventloop.poll().await {
                    Ok(Event::Incoming(Packet::Publish(p))) => {
                        /* TODO: Handle incomming commands! */
                        let topic = p.topic;
                        let payload = String::from_utf8(p.payload.to_vec()).unwrap();
                        debug!("Received MQTT command {payload:?}");

                        let callback = CALLBACKS.write().await;
                        callback.send(topic.clone(), payload.clone()).await;
                    },
                    Ok(Event::Incoming(Packet::ConnAck(_))) => {
                        info!("Connected, resubscribing everything");
                        
                        /* We are connected resubstribe to everything */
                        let callbacks = CALLBACKS.read().await.get_topics().await;
                        for callback in callbacks {
                            /* Move the resubscription to it's own thread */
                            let client_clone = reconnect_c.clone();
                            tokio::spawn(async move {
                                let _ = client_clone.subscribe(callback, QoS::AtLeastOnce).await.unwrap();
                            });
                        }
                    },
                    Ok(_) => {},
                    Err(e) => {
                        error!("Error in MQTT {:?}, reconnecting ", e);
                    }
                }
            }
        });

        return Ok((MqttManager {
            client: client,
            rx: mrx,
            exit_thread: false,
        }, mtx));
    }

    pub async fn start_thread(&mut self, broadcast: tokio::sync::broadcast::Sender<String>) {
       
        // Handle all the incomming metering stuff
        while !self.exit_thread {
            let option = self.rx.recv().await;

            if option.is_none() {
                debug!("Reading returned none, we exit now");
                self.exit_thread = true;
                continue;
            }
            
            match option.unwrap() {
                Transmission::Metering(data) => {
                                info!("Metering data received: {}", data.id);
                                match self.client.publish("energy2mqtt/raw", QoS::AtLeastOnce, false, serde_json::to_string(&data).unwrap()).await {
                                    Err(e) => { error!("Error sending: {}", e); },
                                    Ok(_) => { 
                                        debug!("Send successfully");
                                        // Update health status
                                        tokio::spawn(async {
                                            let mut app_status = APP_STATUS.write().await;
                                            app_status.mqtt_health.last_message_sent = Some(Instant::now());
                                        });
                                    }
                                }
        
                                let _ = broadcast.send(serde_json::to_string_pretty(&data).unwrap());
                                let _ = self.client.publish(format!("energy2mqtt/devs/{:?}/{}", data.protocol, data.meter_name),
                                                            QoS::AtLeastOnce,
                                                            false,
                                                            serde_json::to_string(&data.metered_values.clone()).unwrap()).await;

                            },
                Transmission::Command(command) => {
                                let _ = self.client.publish(command.topic, QoS::AtLeastOnce, command.retain, command.value).await;
                            }
                Transmission::AutoDiscovery(disc) => {
                                let _ = self.client.publish(disc.discover_topic.clone(),QoS::AtLeastOnce, true, serde_json::to_string(&disc).unwrap()).await;
                            }
                Transmission::Subscribe(subscribe_data) =>  {
                                let topic = format!("energy2mqtt/{}", subscribe_data.topic);
                                if self.client.subscribe(topic.clone(), QoS::AtLeastOnce).await.is_ok() {
                                    CALLBACKS.write().await.insert(topic.clone(), subscribe_data.sender);
                                    info!("Registered Callback {topic}");
                                }
                            },
                Transmission::Publish(publish_data) => {
                                match self.client.publish(
                                    publish_data.topic,
                                    match publish_data.qos {
                                        0 => QoS::AtMostOnce,
                                        1 => QoS::AtLeastOnce,
                                        2 => QoS::ExactlyOnce,
                                        _ => QoS::AtMostOnce,
                                    },
                                    publish_data.retain,
                                    publish_data.payload
                                ).await {
                                    Err(e) => { error!("Error publishing: {}", e); },
                                    Ok(_) => { debug!("Published successfully"); }
                                }
                            },
            };
        }

        if self.exit_thread == true {
            info!("Thread exit, waiting");
        } else {
            error!("Exited without need to do so ... spookie");
        }
    }

    pub async fn register_device(&self, proto: String, name: String, disc: HaDiscover) {
        let _ = self.client.publish(format!("homeassistant/device/e2m_{}-{}", proto, name),QoS::AtLeastOnce, true, serde_json::to_string(&disc).unwrap()).await;
    }
}

pub async fn get_mqtt_health_status() -> MqttHealthStatus {
    APP_STATUS.read().await.mqtt_health.clone()
}

pub async fn get_app_status() -> AppStatus {
    APP_STATUS.read().await.clone()
}

#[derive(Serialize)]
pub struct ManagementData {
    pub uptime_seconds: u64,
    pub meter_counts: MeterCounts,
    pub timestamp: u64,
}

#[derive(Serialize, Clone)]
pub struct MeterCounts {
    pub modbus: u32,
    pub knx: u32,
    pub victron: u32,
}

// Each protocol module will publish its own count individually

pub async fn publish_uptime(mqtt_sender: &Sender<Transmission>) {
    let app_status = get_app_status().await;
    
    // Publish uptime only - protocol modules will publish their own counts
    let uptime_publish = PublishData {
        topic: "energy2mqtt/mgt/uptime".to_string(),
        payload: app_status.uptime_seconds().to_string(),
        qos: 1,
        retain: true,
    };
    let _ = mqtt_sender.send(Transmission::Publish(uptime_publish)).await;
}

pub async fn publish_protocol_count(mqtt_sender: &Sender<Transmission>, protocol: &str, count: u32) {
    let count_publish = PublishData {
        topic: format!("energy2mqtt/mgt/{}/count", protocol),
        payload: count.to_string(),
        qos: 1,
        retain: true,
    };
    let _ = mqtt_sender.send(Transmission::Publish(count_publish)).await;
}
