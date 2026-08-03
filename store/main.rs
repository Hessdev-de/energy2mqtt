use std::{collections::{HashMap, HashSet}, time::{Duration, SystemTime}};
use log::{debug, error, info};
use rumqttc::{AsyncClient, ConnectionError, Event, MqttOptions, Packet};
use tokio::sync::{RwLock, mpsc::Sender};

use crate::{configuration::{CONFIG, verify_timing}, cronjob::cronjob, retention::retainer, writer::{Writer, WriterData}};

mod configuration;
mod writer;
mod cronjob;
mod retention;

lazy_static::lazy_static! {
    pub static ref DATA: RwLock<HashMap<String, (SystemTime, Vec<u8>)>> = RwLock::new(HashMap::new());
}

#[tokio::main]
async fn main() -> std::io::Result<()> {

    env_logger::init();

    if !CONFIG.read().await.is_valid {
        error!("Reading configuration failed");
        return Ok(());
    }

    std::env::set_var("TZ", CONFIG.read().await.get_timezone());

    // Init the database system before MQTT
    let mut writer = Writer::new();

    // Setup the store 
    let (host, port, user, passwort, client_id) = CONFIG.read().await.get_mqtt_config();
    let mut options = MqttOptions::new(client_id, host, port);

    options.set_clean_session(true);
    options.set_credentials(user, passwort);
    options.set_keep_alive(Duration::from_secs(60));

    let (sender, mut receiver) = tokio::sync::mpsc::channel(1024);

    let (client, mut eloop) = AsyncClient::new(options, 100);

    // Spawn the cronjob thread (needs clone of sender to inform about new data to be published)
    tokio::spawn(cronjob(sender.clone()));

    // Spawn the thread to clean old data
    tokio::spawn(retainer());

    loop {
        tokio::select! {
            Some(data) = receiver.recv() => {
                writer.store(data, &client).await;
            }
            event = eloop.poll() => {
                eventloop(&event, &client, &sender).await;
            }
        }
    }
}


async fn handle_topics_from_wildcard(topic: &String, p: &rumqttc::Publish, sender: &Sender<WriterData>, old: Vec<u8>, new: Vec<u8>) {
    // The topic may or may not be a wildcard so we need to check that
    let topic = topic.replace("#", "");

    let old_value = String::from_utf8(old).unwrap_or_default();
    let new_value = String::from_utf8(new).unwrap_or_default();

    let reason = format!("{{\"type\":\"trigger\", \"topic\": \"{}\", \"values\": [\"{}\", \"{}\"]}}",
                                    p.topic, old_value, new_value);

    // Get a list of all topics where we received data from and verify against the list of all requested topics (including wildcard)
    let d = DATA.read().await.clone();
    let topics: Vec<String> = d.iter().filter(|e| e.0.starts_with(&topic)).map(|e| e.0.clone()).collect();

    for topic in topics {
        debug!("Trigger {} called, will write the real topic {} now", p.topic, topic);

        if let Some((storage_time, data)) = DATA.read().await.get(&topic) {

            if let Some(tc) = CONFIG.read().await.get_topic_config(&topic) {
                if !verify_timing(&tc.max_variant, &storage_time) {
                    error!("Trigger called for too old data");
                    continue;
                }
            }

            let _ = sender.send(WriterData {
                topic: topic.clone(),
                data: data.clone(),
                timeing: SystemTime::UNIX_EPOCH.elapsed().unwrap_or_default().as_secs() as i64,
                reason: reason.clone(),
            }).await;
        }
    }
}
async fn eventloop(event: &Result<Event, ConnectionError>, client: &AsyncClient, sender: &Sender<WriterData>) {
    // Get a list of all topics which should be subscribed during connection
    let subscriptions: HashSet<String> = CONFIG.read().await.get_subscribed_topics();

    match event {
        Ok(Event::Incoming(Packet::ConnAck(_))) => {
            // We are connected
            info!("MQTT connection established, subscribing");
            let subs = subscriptions.clone();
            let c_client = client.clone();

            // Resubscribe to the topics in our configuration
            tokio::spawn(async move {
                for sub in &subs {
                    debug!("Subscribing to '{sub}'");
                    if let Err(e) = c_client.subscribe(sub, rumqttc::QoS::AtLeastOnce).await {
                        error!("Failed to subscribe: {e:?}");
                    }
                }
            });
        },

        Ok(Event::Incoming(Packet::Publish(p))) => {
            debug!("Received {}: {}", p.topic, String::from_utf8(p.payload.to_vec()).unwrap_or("UNPARSABLE".to_string()));
            if CONFIG.read().await.is_instant(&p.topic)  {
                // We need to write this value directly based on the configuration
                let _ = sender.send(WriterData {
                    topic: p.topic.clone(),
                    data: p.payload.to_vec(),
                    timeing: SystemTime::UNIX_EPOCH.elapsed().unwrap_or_default().as_secs() as i64,
                    reason: format!("{{\"type\":\"instant\"}}"),
                }).await;
            } else {
                // Store the new value and let the cronpart of the tool do it's job also store the old value if set
                let old = match DATA.write().await.insert(
                                                    p.topic.clone(),
                                                    (SystemTime::now(), p.payload.to_vec())
                                                ) {
                    Some((_, d)) => d,
                    None => { Vec::new() }
                };
                
                // Check if this topic is a trigger for something else
                let base_topics = CONFIG.read().await.trigger_for_topic(&p.topic);
                // if so we want to handle the topics
                if !base_topics.is_empty() {

                    let new = p.payload.to_vec().clone();

                    // Call the handlers for the wildcards
                    for topic in &base_topics {
                        handle_topics_from_wildcard(topic, &p, sender, old.clone(), new.clone()).await;
                    }
                }

            }
        },
        
        Ok(_) => { /* Non handled data */}
        
        Err(e) => {
            error!("MQTT connection failed: {e:?}");
            /* Wait some seconds to reconnect */
            tokio::time::sleep(Duration::from_secs(10)).await;
        },
    }
}