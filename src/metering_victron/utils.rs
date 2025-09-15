use std::{sync::Arc, time::Duration};
use log::debug;
use rumqttc::AsyncClient;
use serde_json::Value;
use tokio::{sync::Mutex, time::sleep};

use crate::metering_victron::{Topic, VictronData};

pub async fn get_portal(data: &Arc<Mutex<VictronData>>) -> String {
    return data.lock().await.portal_id.clone();
}

pub async fn set_topic(client: &AsyncClient, data: &Arc<Mutex<VictronData>>, key: &String, topic: Option<Topic>) {
    let mut lock = data.lock().await;

    if !lock.topic_mapping.contains_key(key) {
        /* We need to subscribe in order to handle the changes */
        drop(lock);
        let _ = client.subscribe(key, rumqttc::QoS::AtLeastOnce).await;
        lock = data.lock().await;
    }

    lock.topic_mapping.insert(key.clone(), topic);
}

pub async fn get_topic(data: &Arc<Mutex<VictronData>>, key: &String) -> Option<Topic> {
    let lock = data.lock().await;
    let k = lock.topic_mapping.get(key);
    if k.is_none() {
        return None;
    }

    let d = k.unwrap();
    if d.is_none() { return None; }
    let d = d.clone().unwrap();
    return Some(d);
}

pub fn victron_value_to_u64(value: &String, default: u64) -> u64 {
    let doc = serde_json::from_str::<Value>(value);
    match doc {
        Err(_) => { return default; }
        Ok(v) => {
            match v.get("value") {
                Some(v) => { return v.as_u64().unwrap_or(default); }
                None => { return default; }
            }
        }
    }
}

pub fn victron_value_to_string(value: &String, default: &str) -> String {
    let doc = serde_json::from_str::<Value>(value);
    match doc {
        Err(_) => { return default.to_string(); }
        Ok(v) => {
            match v.get("value") {
                Some(v) => { return v.as_str().unwrap_or(default).to_string(); }
                None => { return default.to_string(); }
            }
        }
    }
}

pub fn victron_value_to_value(value: &String, default: Value) -> Value {
    let doc = serde_json::from_str::<Value>(value);
    match doc {
        Err(_) => { return default; }
        Ok(v) => {
            match v.get("value") {
                Some(v) => { return v.clone(); }
                None => { return default; }
            }
        }
    }
}

pub async fn read_topic_u64(client: &AsyncClient, data: &Arc<Mutex<VictronData>>, topic: &String, json_key: String) -> Option<u64> {

    let t = Topic::new_with_key("".to_string(), json_key);
    set_topic(client, data, &topic, Some(t)).await;

    let _ = client.publish(topic.clone().replacen("N", "R", 1), 
                            rumqttc::QoS::AtLeastOnce, false, "").await;

    /* We wait up to five second */
    let mut res: Option<Topic> = None;
    for _ in 0..=500 {
        sleep(Duration::from_millis(10)).await;
        res = get_topic(data, &topic).await;
        if res.is_some() {
            if res.clone().unwrap().payload != "" {
                break;
            }
        }
    }

    if res.is_none() {
        return None;
    }

    let topic_data = res.clone().unwrap();
    if topic_data.payload == "" {
        debug!("{topic} No payload found, so returning default ");
        return None;
    }

    return Some(victron_value_to_u64(&topic_data.payload, 0));
}

pub async fn read_topic_string(client: &AsyncClient, data: &Arc<Mutex<VictronData>>, topic: &String, json_key: String) -> Option<String> {

    let t = Topic::new_with_key("".to_string(), json_key);
    set_topic(client, data, &topic, Some(t)).await;

    let _ = client.publish(topic.clone().replacen("N", "R", 1), 
                            rumqttc::QoS::AtLeastOnce, false, "").await;

    /* We wait up to five second */
    let mut res: Option<Topic> = None;
    for _ in 0..=500 {
        sleep(Duration::from_millis(10)).await;
        res = get_topic(data, &topic).await;
        if res.is_some() {
            if res.clone().unwrap().payload != "" {
                break;
            }
        }
    }

    if res.is_none() {
        return None;
    }

    let topic_data = res.clone().unwrap();
    if topic_data.payload == "" {
        debug!("{topic} No payload found, so returning default ");
        return None;
    }

    return Some(victron_value_to_string(&topic_data.payload, ""));
}

pub async fn read_topic_value(client: &AsyncClient, data: &Arc<Mutex<VictronData>>, topic: &String, json_key: String) -> Option<Value> {

    let t = Topic::new_with_key("".to_string(), json_key);
    set_topic(client, data, &topic, Some(t)).await;

    let _ = client.publish(topic.clone().replacen("N", "R", 1), 
                            rumqttc::QoS::AtLeastOnce, false, "").await;

    /* We wait up to five second */
    let mut res: Option<Topic> = None;
    for _ in 0..=500 {
        sleep(Duration::from_millis(10)).await;
        res = get_topic(data, &topic).await;
        if res.is_some() {
            if res.clone().unwrap().payload != "" {
                break;
            }
        }
    }

    if res.is_none() {
        return None;
    }

    let topic_data = res.clone().unwrap();
    if topic_data.payload == "" {
        debug!("{topic} No payload found, so returning default ");
        return None;
    }

    return Some(victron_value_to_value(&topic_data.payload, Value::Null));
}
