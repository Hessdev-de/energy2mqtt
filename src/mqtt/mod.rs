pub mod internal_commands;
pub mod ha_interface;
pub mod home_assistant;
pub mod migration;

use std::collections::HashMap;
use lazy_static::lazy_static;
use tokio::sync::RwLock;
use std::io::Error;
use crate::mqtt::ha_interface::HaDiscover;
use crate::mqtt::home_assistant::HaSensor;
use crate::mqtt::migration::run_migration_if_needed;
use crate::config::{ConfigBases, MQTT_DISCOVERY_VERSION_CURRENT};
use crate::models::DeviceProtocol;
use crate::{CONFIG, get_config_or_panic, get_unix_ts};
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
    pub metered_values: serde_json::Map<String, serde_json::Value>,
    pub state_topic_base: String,
}

impl MeteringData {
    pub fn new() -> Result<Self, Error> {
        let now = get_unix_ts();
        Ok(MeteringData {
            id: "".to_string(),
            tenant: "".to_string(),
            meter_name: "".to_string(),
            protocol: DeviceProtocol::Unknown, 
            transmission_time: now,
            transmission_type: TranmissionValueType::Now,
            metered_time: now, 
            metered_values: serde_json::Map::new(),
            state_topic_base: "".to_string(),
        })
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
    pub sender: tokio::sync::mpsc::Sender<(String, String)>
}

pub struct TaskCrashData {
    pub manager: String,
    pub task_name: String,
    pub task_type: String,
    pub message: String,
    pub restart_count: u32,
}

pub enum Transmission {
    Metering(MeteringData),
    AutoDiscovery(HaDiscover),
    AutoDiscovery2(HaSensor),
    Command(CommandData),
    Subscribe(SubscribeData),
    Publish(PublishData),
    TaskCrash(TaskCrashData),
}

pub struct MqttManager {
    rx: Receiver<Transmission>,
    exit_thread: bool,
    client: AsyncClient,
}

pub struct Callbacks {
    calls: HashMap<String, tokio::sync::mpsc::Sender<(String, String)>>,
}

impl Callbacks {
    pub fn new() -> Self {
        return Callbacks { calls: HashMap::new() };
    }

    pub fn insert(&mut self, topic: String, callback: tokio::sync::mpsc::Sender<(String, String)>) {
        if !self.calls.contains_key(&topic) {
            debug!("Adding new vector to topic {topic}");
        } else {
            debug!("Replacing known callback at topic {topic}");
        }

        self.calls.insert(topic, callback);
    }

    pub async fn send(&self, topic: String, payload: String) {
        if !self.calls.contains_key(&topic) {
            debug!("Send for unkonwn topic {topic}");
            return;
        }

        let call = self.calls.get(&topic).unwrap();
        debug!("Sending to callback: {payload}");
        let _ = call.send((topic.clone(), payload.clone())).await.unwrap();
    }

    pub async fn get_topics(&self) -> Vec<String> {
        let mut d: Vec<String>  = Vec::new();
        for key in self.calls.keys() {
            d.push(key.clone());
        }
        return d;
    }

}

/// Direction of MQTT message for live view
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum LiveEventDirection {
    Outgoing,
    Incoming,
}

/// Type of MQTT event for live view
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum LiveEventType {
    Metering,
    Command,
    AutoDiscovery,
    Publish,
    Subscribe,
    System,
}

/// Live event structure for real-time MQTT monitoring
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LiveEvent {
    pub timestamp: u64,
    pub direction: LiveEventDirection,
    pub event_type: LiveEventType,
    pub topic: String,
    pub payload: serde_json::Value,
    pub retain: Option<bool>,
    pub qos: Option<u8>,
}

impl LiveEvent {
    pub fn outgoing(event_type: LiveEventType, topic: String, payload: serde_json::Value) -> Self {
        Self {
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
            direction: LiveEventDirection::Outgoing,
            event_type,
            topic,
            payload,
            retain: None,
            qos: None,
        }
    }

    pub fn incoming(topic: String, payload: serde_json::Value) -> Self {
        Self {
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
            direction: LiveEventDirection::Incoming,
            event_type: LiveEventType::Command,
            topic,
            payload,
            retain: None,
            qos: None,
        }
    }

    pub fn with_retain(mut self, retain: bool) -> Self {
        self.retain = Some(retain);
        self
    }

    pub fn with_qos(mut self, qos: u8) -> Self {
        self.qos = Some(qos);
        self
    }
}

lazy_static! {
    pub static ref CALLBACKS: RwLock<Callbacks> = RwLock::new(Callbacks::new());
    pub static ref APP_STATUS: RwLock<AppStatus> = RwLock::new(AppStatus::new());
    pub static ref LIVE_EVENTS: tokio::sync::broadcast::Sender<LiveEvent> = {
        let (tx, _) = tokio::sync::broadcast::channel(100);
        tx
    };
}

impl MqttManager {
    pub fn new() -> Result<(Self, Sender<Transmission>), Error> {
        let (mtx,mrx) = tokio::sync::mpsc::channel(100);

        info!("MQTT connection starting up");
        let config = get_config_or_panic!("mqtt", ConfigBases::Mqtt);

        // Check if migration is needed (will be run async after connection)
        let needs_migration = config.discovery_version < MQTT_DISCOVERY_VERSION_CURRENT;
        if needs_migration {
            info!("MQTT discovery migration pending: version {} -> {}",
                  config.discovery_version, MQTT_DISCOVERY_VERSION_CURRENT);
        }
        let mut mqttoptions   = MqttOptions::new(config.client_name.clone(), config.host.clone(), config.port);
        mqttoptions.set_keep_alive(Duration::from_secs(5));
        mqttoptions.set_credentials(config.user.clone(), config.pass.clone());

        info!("Connection setup to {}@{}:{}", config.user, config.host, config.port);

        // Set last will message for availability - broker publishes "offline" if we disconnect unexpectedly
        let last_will = rumqttc::LastWill::new(
            "energy2mqtt/status",
            "offline".as_bytes().to_vec(),
            QoS::AtLeastOnce,
            true  // retain = true so new subscribers see the status
        );
        mqttoptions.set_last_will(last_will);

        let (client, mut eventloop) = AsyncClient::new(mqttoptions, 10);

        // Spawn a new thread to handle the incomming commands
        let reconnect_c = client.clone();
        let migration_config = config.clone();
        let migration_needed = needs_migration;
        tokio::spawn( async move {
            info!("MQTT Eventloop started");

            // Backoff state for reconnection
            let mut backoff_secs: u64 = 1;
            const MAX_BACKOFF_SECS: u64 = 60;
            const INITIAL_BACKOFF_SECS: u64 = 1;
            let mut last_error_log = Instant::now();
            let mut consecutive_errors: u32 = 0;
            let mut migration_done = !migration_needed;

            loop {
                match eventloop.poll().await {
                    Ok(Event::Incoming(Packet::Publish(p))) => {
                        /* TODO: Handle incomming commands! */
                        let topic = p.topic;
                        let payload = String::from_utf8(p.payload.to_vec()).unwrap();
                        debug!("Received MQTT command {payload:?}");

                        // Broadcast incoming message to live view
                        let payload_json = serde_json::from_str(&payload)
                            .unwrap_or_else(|_| serde_json::Value::String(payload.clone()));
                        let live_event = LiveEvent::incoming(topic.clone(), payload_json);
                        let _ = LIVE_EVENTS.send(live_event);

                        let callback = CALLBACKS.write().await;
                        callback.send(topic.clone(), payload.clone()).await;
                    },
                    Ok(Event::Incoming(Packet::ConnAck(_))) => {
                        info!("MQTT Connected, resubscribing everything");

                        // Reset backoff on successful connection
                        backoff_secs = INITIAL_BACKOFF_SECS;
                        consecutive_errors = 0;

                        // Update health status to connected
                        {
                            let mut app_status = APP_STATUS.write().await;
                            app_status.mqtt_health.status = MqttConnectionStatus::Connected;
                            app_status.mqtt_health.last_connected = Some(Instant::now());
                            app_status.mqtt_health.connection_attempts += 1;
                        }

                        // Run migration if needed (only once on first connect)
                        if !migration_done {
                            info!("Running MQTT discovery migration...");
                            let mig_config = migration_config.clone();
                            match run_migration_if_needed(&mig_config).await {
                                Ok(true) => {
                                    info!("Migration completed, updating config version");
                                    // Update config with new version
                                    if let Ok(mut cfg) = CONFIG.write() {
                                        cfg.config.mqtt.discovery_version = MQTT_DISCOVERY_VERSION_CURRENT;
                                        cfg.dirty = true;
                                        cfg.save();
                                        info!("Config updated to discovery version {}", MQTT_DISCOVERY_VERSION_CURRENT);
                                    }
                                }
                                Ok(false) => {
                                    debug!("No migration was needed");
                                }
                                Err(e) => {
                                    error!("Migration failed: {}", e);
                                }
                            }
                            migration_done = true;
                        }

                        // Publish "online" availability status (retained)
                        let online_client = reconnect_c.clone();
                        tokio::spawn(async move {
                            if let Err(e) = online_client.publish(
                                "energy2mqtt/status",
                                QoS::AtLeastOnce,
                                true,  // retain
                                "online"
                            ).await {
                                error!("Failed to publish online status: {:?}", e);
                            } else {
                                info!("Published online availability status");
                            }
                        });

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
                    Ok(Event::Incoming(Packet::Disconnect)) => {
                        info!("MQTT Disconnected");
                        let mut app_status = APP_STATUS.write().await;
                        app_status.mqtt_health.status = MqttConnectionStatus::Disconnected;
                    },
                    Ok(_) => {},
                    Err(e) => {
                        consecutive_errors += 1;

                        // Only log errors periodically to avoid flooding
                        let now = Instant::now();
                        if consecutive_errors == 1 || now.duration_since(last_error_log).as_secs() >= 30 {
                            error!("MQTT connection error: to {:?} (attempt {}, next retry in {}s)",
                                   e, consecutive_errors, backoff_secs);
                            last_error_log = now;
                        }

                        // Update health status on error
                        {
                            let mut app_status = APP_STATUS.write().await;
                            app_status.mqtt_health.status = MqttConnectionStatus::Reconnecting;
                        }

                        // Apply backoff delay before next reconnection attempt
                        tokio::time::sleep(Duration::from_secs(backoff_secs)).await;

                        // Exponential backoff with max limit
                        backoff_secs = (backoff_secs * 2).min(MAX_BACKOFF_SECS);
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
                    let raw_topic = "energy2mqtt/raw".to_string();
                    let raw_payload = serde_json::to_string(&data).unwrap();

                    // Broadcast to live view
                    let live_event = LiveEvent::outgoing(
                        LiveEventType::Metering,
                        raw_topic.clone(),
                        serde_json::to_value(&data).unwrap_or_default()
                    );
                    let _ = LIVE_EVENTS.send(live_event);

                    match self.client.publish(raw_topic, QoS::AtLeastOnce, false, raw_payload).await {
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
                    let mut proto_path = data.protocol.to_string();
                    if !data.state_topic_base.is_empty() {
                        proto_path = data.state_topic_base.clone();
                    }

                    let dev_topic = format!("energy2mqtt/devs/{}/{}", proto_path, data.meter_name);
                    let dev_payload = serde_json::to_string(&data.metered_values.clone()).unwrap();

                    // Broadcast device topic to live view
                    let live_event = LiveEvent::outgoing(
                        LiveEventType::Metering,
                        dev_topic.clone(),
                        serde_json::Value::Object(data.metered_values.clone())
                    );
                    let _ = LIVE_EVENTS.send(live_event);

                    let _ = self.client.publish(dev_topic, QoS::AtLeastOnce, false, dev_payload).await;

                },
                Transmission::Command(command) => {
                    let payload_json = serde_json::from_str(&command.value)
                        .unwrap_or_else(|_| serde_json::Value::String(command.value.clone()));
                    let live_event = LiveEvent::outgoing(LiveEventType::Command, command.topic.clone(), payload_json)
                        .with_retain(command.retain);
                    let _ = LIVE_EVENTS.send(live_event);

                    let _ = self.client.publish(command.topic, QoS::AtLeastOnce, command.retain, command.value).await;
                },
                Transmission::AutoDiscovery(disc) => {
                    let topic = disc.discover_topic.clone();
                    let payload = serde_json::to_value(&disc).unwrap_or_default();
                    let live_event = LiveEvent::outgoing(LiveEventType::AutoDiscovery, topic.clone(), payload)
                        .with_retain(true);
                    let _ = LIVE_EVENTS.send(live_event);

                    let _ = self.client.publish(topic, QoS::AtLeastOnce, true, serde_json::to_string(&disc).unwrap()).await;
                },
                Transmission::AutoDiscovery2(disc) => {
                    // Send individual discovery messages per entity to avoid MQTT size limits
                    let discoveries = disc.get_entity_discoveries();

                    for entity_disc in discoveries {
                        let live_event = LiveEvent::outgoing(
                            LiveEventType::AutoDiscovery,
                            entity_disc.topic.clone(),
                            entity_disc.payload.clone()
                        ).with_retain(true);
                        let _ = LIVE_EVENTS.send(live_event);

                        let _ = self.client.publish(
                            entity_disc.topic,
                            QoS::AtLeastOnce,
                            true,
                            serde_json::to_string(&entity_disc.payload).unwrap_or_default()
                        ).await;

                        // Small delay between discovery messages to not overwhelm the broker
                        tokio::time::sleep(Duration::from_millis(50)).await;
                    }
                },
                Transmission::Subscribe(subscribe_data) =>  {
                    let mut topic = subscribe_data.topic.clone();
                    if !topic.starts_with("energy2mqtt/") && !topic.starts_with("homeassistant/") {
                        topic = format!("energy2mqtt/{}", subscribe_data.topic);
                    }

                    if self.client.subscribe(topic.clone(), QoS::AtLeastOnce).await.is_ok() {
                        CALLBACKS.write().await.insert(topic.clone(), subscribe_data.sender);
                        info!("Registered Callback {topic}");

                        // Broadcast subscribe to live view
                        let live_event = LiveEvent::outgoing(
                            LiveEventType::Subscribe,
                            topic,
                            serde_json::json!({"action": "subscribe"})
                        );
                        let _ = LIVE_EVENTS.send(live_event);
                    }
                },
                Transmission::Publish(publish_data) => {
                    let payload_json = serde_json::from_str(&publish_data.payload)
                        .unwrap_or_else(|_| serde_json::Value::String(publish_data.payload.clone()));
                    let live_event = LiveEvent::outgoing(LiveEventType::Publish, publish_data.topic.clone(), payload_json)
                        .with_retain(publish_data.retain)
                        .with_qos(publish_data.qos);
                    let _ = LIVE_EVENTS.send(live_event);

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
                Transmission::TaskCrash(data) => {
                    error!("TASK CRASH NOTIFICATION: [{}] {} ({}) - {}", data.manager, data.task_name, data.task_type, data.message);

                    // Publish crash notification to MQTT for external monitoring
                    let crash_payload = serde_json::json!({
                        "manager": data.manager,
                        "task_name": data.task_name,
                        "task_type": data.task_type,
                        "message": data.message,
                        "restart_count": data.restart_count,
                        "timestamp": get_unix_ts(),
                    });

                    let crash_topic = format!("energy2mqtt/mgt/task_crash/{}/{}", data.manager, data.task_name);

                    // Broadcast to live view
                    let live_event = LiveEvent::outgoing(LiveEventType::System, crash_topic.clone(), crash_payload.clone());
                    let _ = LIVE_EVENTS.send(live_event);

                    let _ = self.client.publish(
                        crash_topic,
                        QoS::AtLeastOnce,
                        false,
                        crash_payload.to_string()
                    ).await;

                    // Also publish to a general crash topic for easy monitoring
                    let live_event = LiveEvent::outgoing(
                        LiveEventType::System,
                        "energy2mqtt/mgt/crashes".to_string(),
                        crash_payload.clone()
                    );
                    let _ = LIVE_EVENTS.send(live_event);

                    let _ = self.client.publish(
                        "energy2mqtt/mgt/crashes",
                        QoS::AtLeastOnce,
                        false,
                        crash_payload.to_string()
                    ).await;
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
