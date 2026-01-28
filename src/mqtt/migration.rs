//! MQTT Discovery Migration
//!
//! Handles cleanup of old discovery topics when the format version changes.
//! This ensures Home Assistant doesn't have duplicate/orphaned entities.

use log::{debug, error, info, warn};
use rumqttc::{AsyncClient, Event, MqttOptions, Packet, QoS};
use std::collections::HashSet;
use std::time::Duration;
use tokio::time::timeout;

use crate::config::{MqttConfig, MQTT_DISCOVERY_VERSION_CURRENT};

/// Check if a topic is in the old flat format that needs migration
///
/// Old format (v1): homeassistant/sensor/e2m_knx_device_sensor/config
/// New format (v2): homeassistant/sensor/e2m_knx_device/sensor/config
fn is_old_format_topic(topic: &str) -> bool {
    // Pattern 1: Old combined device discovery
    // homeassistant/device/e2m_*/config
    if topic.starts_with("homeassistant/device/e2m_") && topic.ends_with("/config") {
        return true;
    }

    // Pattern 2: Old flat entity discovery
    // homeassistant/{platform}/e2m_{something}/config
    // where {something} contains underscores but no path separators
    let parts: Vec<&str> = topic.split('/').collect();

    // Expected old format: ["homeassistant", "{platform}", "e2m_*", "config"]
    // Expected new format: ["homeassistant", "{platform}", "e2m_*", "{key}", "config"] or more segments
    if parts.len() == 4
        && parts[0] == "homeassistant"
        && parts[2].starts_with("e2m_")
        && parts[3] == "config"
    {
        // This is old format - flat structure with device_sensor all in one segment
        return true;
    }

    false
}

/// Run migration if needed - cleans up old discovery topics
pub async fn run_migration_if_needed(config: &MqttConfig) -> Result<bool, String> {
    if config.discovery_version >= MQTT_DISCOVERY_VERSION_CURRENT {
        debug!("MQTT discovery version {} is current ({}), no migration needed",
               config.discovery_version, MQTT_DISCOVERY_VERSION_CURRENT);
        return Ok(false);
    }

    info!("MQTT discovery migration needed: version {} -> {}",
          config.discovery_version, MQTT_DISCOVERY_VERSION_CURRENT);

    run_cleanup(config).await
}

/// Force cleanup of old discovery topics (ignores version check)
pub async fn force_cleanup(config: &MqttConfig) -> Result<usize, String> {
    info!("Force cleanup of old discovery topics requested");
    run_cleanup(config).await?;
    Ok(0) // TODO: return actual count
}

/// Internal cleanup implementation
async fn run_cleanup(config: &MqttConfig) -> Result<bool, String> {

    // Connect to MQTT broker
    let client_name = format!("{}_migration", config.client_name);
    let mut mqttoptions = MqttOptions::new(&client_name, &config.host, config.port);
    mqttoptions.set_keep_alive(Duration::from_secs(30));
    mqttoptions.set_credentials(&config.user, &config.pass);
    // Clean session to ensure we get all retained messages
    mqttoptions.set_clean_session(true);

    let (client, mut eventloop) = AsyncClient::new(mqttoptions, 500);

    // Wait for connection before subscribing
    let connect_timeout = Duration::from_secs(10);
    let connected = timeout(connect_timeout, async {
        loop {
            match eventloop.poll().await {
                Ok(Event::Incoming(Packet::ConnAck(ack))) => {
                    if ack.code == rumqttc::ConnectReturnCode::Success {
                        info!("Migration client connected to MQTT broker");
                        return Ok(());
                    } else {
                        return Err(format!("Connection rejected: {:?}", ack.code));
                    }
                }
                Ok(_) => continue,
                Err(e) => return Err(format!("Connection error: {:?}", e)),
            }
        }
    }).await;

    match connected {
        Ok(Ok(())) => {}
        Ok(Err(e)) => return Err(e),
        Err(_) => return Err("Connection timeout".to_string()),
    }

    // Subscribe to all homeassistant topics to discover existing entries
    client.subscribe("homeassistant/#", QoS::AtLeastOnce).await
        .map_err(|e| format!("Failed to subscribe for migration: {:?}", e))?;

    info!("Subscribed to homeassistant/# - scanning for old discovery topics...");

    // Collect topics that need cleanup
    let mut topics_to_delete: HashSet<String> = HashSet::new();
    let mut all_e2m_topics: HashSet<String> = HashSet::new();
    let mut message_count = 0;
    let scan_timeout = Duration::from_secs(30);
    let idle_timeout = Duration::from_secs(2);  // Increased from 500ms
    let mut last_message = std::time::Instant::now();

    // Scan for retained messages
    info!("Waiting for retained messages (up to {}s)...", scan_timeout.as_secs());

    let scan_result = timeout(scan_timeout, async {
        loop {
            match timeout(idle_timeout, eventloop.poll()).await {
                Ok(Ok(Event::Incoming(Packet::Publish(p)))) => {
                    message_count += 1;
                    last_message = std::time::Instant::now();

                    let topic = p.topic.clone();

                    // Only process retained messages with non-empty payloads that are e2m topics
                    if p.retain && !p.payload.is_empty() && topic.contains("/e2m_") {
                        all_e2m_topics.insert(topic.clone());

                        if is_old_format_topic(&topic) {
                            info!("Found old format topic to delete: {}", topic);
                            topics_to_delete.insert(topic);
                        } else {
                            debug!("Keeping new format topic: {}", topic);
                        }
                    }
                }
                Ok(Ok(Event::Incoming(Packet::SubAck(_)))) => {
                    debug!("Subscription acknowledged");
                    // Reset timer after SubAck to allow time for retained messages
                    last_message = std::time::Instant::now();
                }
                Ok(Ok(_)) => {
                    // Other events, continue
                }
                Ok(Err(e)) => {
                    error!("Migration eventloop error: {:?}", e);
                    return Err(format!("Eventloop error: {:?}", e));
                }
                Err(_) => {
                    // Idle timeout - check if we've been idle long enough
                    if last_message.elapsed() >= idle_timeout {
                        info!("Scan complete: received {} messages, found {} e2m topics, {} to delete",
                              message_count, all_e2m_topics.len(), topics_to_delete.len());
                        break;
                    }
                }
            }
        }
        Ok(())
    }).await;

    match scan_result {
        Ok(Ok(())) => {}
        Ok(Err(e)) => {
            warn!("Migration scan error: {}", e);
        }
        Err(_) => {
            info!("Migration scan timeout after {} messages ({} e2m topics found)",
                  message_count, all_e2m_topics.len());
        }
    }

    if topics_to_delete.is_empty() {
        info!("No old discovery topics found to delete");
        let _ = client.disconnect().await;
        return Ok(true);
    }

    info!("Deleting {} old discovery topics...", topics_to_delete.len());

    // Delete old topics by publishing empty retained messages
    for topic in &topics_to_delete {
        info!("Deleting: {}", topic);
        if let Err(e) = client.publish(topic, QoS::AtLeastOnce, true, Vec::new()).await {
            warn!("Failed to queue deletion for {}: {:?}", topic, e);
        }
    }

    // Pump the eventloop to actually send the deletions
    let flush_timeout = Duration::from_secs(10);
    let flush_result = timeout(flush_timeout, async {
        let mut published = 0;
        let target = topics_to_delete.len();
        loop {
            match timeout(Duration::from_millis(100), eventloop.poll()).await {
                Ok(Ok(Event::Outgoing(rumqttc::Outgoing::Publish(_)))) => {
                    published += 1;
                    if published >= target {
                        debug!("All {} deletions sent", published);
                        break;
                    }
                }
                Ok(Ok(_)) => continue,
                Ok(Err(e)) => {
                    warn!("Error while flushing deletions: {:?}", e);
                    break;
                }
                Err(_) => {
                    // Timeout waiting for outgoing, try a few more times
                    if published > 0 {
                        break;
                    }
                }
            }
        }
    }).await;

    if flush_result.is_err() {
        warn!("Timeout while flushing deletions, some may not have been sent");
    }

    // Give broker time to process
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Disconnect migration client
    let _ = client.disconnect().await;

    info!("Migration complete: deleted {} old discovery topics", topics_to_delete.len());

    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_old_format_topic() {
        // Old device format
        assert!(is_old_format_topic("homeassistant/device/e2m_knx_meter/config"));
        assert!(is_old_format_topic("homeassistant/device/e2m_modbus_device/config"));

        // Old flat sensor format - these should be detected as OLD
        assert!(is_old_format_topic("homeassistant/sensor/e2m_knx_meter_voltage/config"));
        assert!(is_old_format_topic("homeassistant/sensor/e2m_knx_dg1_j_energy/config"));
        assert!(is_old_format_topic("homeassistant/sensor/e2m_modbus_device_power_l1/config"));
        assert!(is_old_format_topic("homeassistant/switch/e2m_knx_meter_switch/config"));

        // New hierarchical format - should NOT be detected as old
        assert!(!is_old_format_topic("homeassistant/sensor/e2m_knx_meter/voltage/config"));
        assert!(!is_old_format_topic("homeassistant/sensor/e2m_knx_dg1_j/energy/config"));
        assert!(!is_old_format_topic("homeassistant/sensor/e2m_modbus_device/power/l1/config"));
        assert!(!is_old_format_topic("homeassistant/switch/e2m_knx_meter/switch/config"));

        // Non-e2m topics should not match
        assert!(!is_old_format_topic("homeassistant/sensor/other_device/config"));
        assert!(!is_old_format_topic("homeassistant/sensor/something_else/config"));
    }
}
