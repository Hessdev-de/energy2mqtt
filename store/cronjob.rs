use std::{collections::HashMap, time::{Duration, SystemTime}};
use chrono::{DateTime, Local, Timelike};
use log::{error, info};
use tokio::{sync::mpsc::Sender, time};
use crate::{DATA, configuration::{CONFIG, ConfigIntervals, verify_timing}, writer::WriterData};


/// Check if the given schedule should be handled now
/// 
/// # Arguments
/// 
/// * `schedule` - The schedule to be checked
/// * `now` - Current time we check that schedule against
/// 
/// # Returns
/// 
/// None if the time is right to handle this schedule. If the schedule should be handled
/// this functions retunrs the timestamp to store and a boolean if the timestamp should be
/// modified to fit the recording intervals.
/// 
fn should_handle(schedule: &ConfigIntervals, now: &DateTime<Local>) -> Option<(i64, bool)> {

    let timestamp = now.timestamp();
    if *schedule == ConfigIntervals::Instant {
        // Handled in main and ignored here
        return None;
    }

    // We match the correct exact timing
    if let ConfigIntervals::Exact(h, m, s) = schedule {
        if now.hour() == *h && now.minute() == *m && now.second() == *s {
            info!("Exact time matched");
            return Some((timestamp, false));
        }
    }

    // We match the interval, but we may need to rework the timestamp
    if timestamp % schedule.to_secs() as i64 == 0 {
        return Some((timestamp, true));
    }
    
    None
}

/// Get the data for a specific topic from the DATA HashMap
/// 
/// The function handles wildcards in a way that we just remove the # and check
/// if the topic in DATA begins with the newly created topic to search.
/// 
/// # Arguments
/// 
/// * `topic` - the topic is the beginning for the data to look up and return
/// 
/// # Returns
/// 
/// A Vector containing the topic matched, the time the data was received and
/// the data itself.
///   
pub async fn get_data(topic: &String) -> Vec<(String, SystemTime, Vec<u8>)> {
    let d = DATA.read().await;

    // Filter all elements if they match (begins with the correct topic)
    let needle = topic.replace("#", "");
    d.iter().filter(|e| e.0.starts_with(&needle))
            .map(|e| (e.0.clone(), e.1.0.clone(), e.1.1.clone()))
            .collect()
}

pub async fn cronjob(sender: Sender<WriterData>) {

    let topics = CONFIG.read().await.topics.clone();

    // TODO: Build that based on the minimal tick needed and do not through the cpu cycles away
    let mut tick = time::interval(Duration::from_millis(900));

    // Store the last written value to not repeat over and over again
    let mut last_send: HashMap<String, i64> = HashMap::new();

    loop {
        let _ = tick.tick().await;

        let now = Local::now();

        // Cycle through the configuration for the topics
        for tc in &topics {

            // Each topic can have may schedules which need to be handled
            for schedule in &tc.store {

                // Check if we should handle this scheudle
                if let Some((mut ts, round_ts)) = should_handle(schedule, &now) {

                    // The clone should not be more time consuming than to get the RW lock twice
                    let storage = get_data(&tc.topic).await;
                    if storage.is_empty() {
                        // That value or wildcard parts have not been seen up until now
                        continue;
                    }

                    for (topic, storage_time, data) in storage {

                        // We may need to make sure we have the right event timestamp in order to map the data correctly
                        if round_ts {
                            ts -= ts % schedule.to_secs() as i64;
                        }

                        // If we have already handled this timestamp for this topic and do no store twice
                        if let Some(timestamp) = last_send.get(&topic) {
                            if *timestamp == now.timestamp() {
                                continue;
                            }
                        }

                        // Check if the write of the data is within the regulated parameters
                        if !verify_timing(&tc.max_variant, &storage_time) {
                            error!("The value of {} is too old", topic);
                            continue;
                        }

                        // We should handle this topic based on that schedule
                        info!("Storing value of {} for timestamp {}", topic, now.timestamp());

                        // Publish the data to the MQTT thread
                        let _ = sender.send(WriterData {
                            topic: topic.clone(),
                            data,
                            timeing: ts,
                            reason: format!("{{\"type\":\"cron\", \"ts\": {} }}", ts)
                        }).await;

                        last_send.insert(topic, ts);

                    }
                }
            }
        }
    }
}