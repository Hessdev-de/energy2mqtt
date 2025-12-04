//! KNX Metering Module
//!
//! This module implements KNX bus communication via KNX/IP UDP tunneling
//! using the knx_rust library.

use crate::config::{ConfigBases, ConfigChange, ConfigOperation, KnxAdapterConfig, KnxDatapointType, KnxMeterConfig};
use crate::models::DeviceProtocol;
use crate::mqtt::home_assistant::{get_state_topic, HaComponent2, HaSensor};
use crate::mqtt::{publish_protocol_count, Transmission};
use crate::task_monitor::TaskMonitor;
use crate::{get_id, get_unix_ts, MeteringData, CONFIG};
use log::{debug, error, info, warn};
use serde::Serialize;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc::Sender;
use tokio::sync::RwLock;

pub mod group_address;
pub mod knxd_client;

use group_address::GroupAddress;
use knxd_client::{KnxClient, KnxError};

/// A cached value from the KNX bus
#[derive(Debug, Clone)]
struct CachedValue {
    data: Vec<u8>,
    #[allow(dead_code)]
    timestamp: Instant,
}

/// Shared cache for group address values
type GroupAddressCache = Arc<RwLock<HashMap<u16, CachedValue>>>;

/// Set of group addresses we're interested in caching
type GroupAddressSet = HashSet<u16>;

/// Statistics for a KNX adapter
#[derive(Default, Serialize, Clone, Debug)]
struct KnxAdapterStats {
    /// Number of read requests sent to the bus
    read_requests_sent: u64,
    /// Number of group value responses received
    responses_received: u64,
    /// Number of values found in cache during publish
    cache_hits: u64,
    /// Number of values not found in cache during publish
    cache_misses: u64,
    /// Number of connection errors
    connection_errors: u64,
    /// Number of reconnections
    reconnections: u64,
    /// Number of poll cycles completed
    poll_cycles_completed: u64,
    /// Number of meter data publishes
    meters_published: u64,
}

impl KnxAdapterStats {
    pub fn inc(&mut self, field: &str) {
        match field {
            "read_requests_sent" => self.read_requests_sent += 1,
            "responses_received" => self.responses_received += 1,
            "cache_hits" => self.cache_hits += 1,
            "cache_misses" => self.cache_misses += 1,
            "connection_errors" => self.connection_errors += 1,
            "reconnections" => self.reconnections += 1,
            "poll_cycles_completed" => self.poll_cycles_completed += 1,
            "meters_published" => self.meters_published += 1,
            _ => debug!("Unknown statistic field: {}", field),
        }
    }

    pub fn add(&mut self, field: &str, count: u64) {
        match field {
            "cache_hits" => self.cache_hits += count,
            "cache_misses" => self.cache_misses += count,
            _ => debug!("Unknown statistic field for add: {}", field),
        }
    }
}

/// Shared stats for a KNX adapter
type SharedStats = Arc<RwLock<KnxAdapterStats>>;

/// KNX Manager - handles all KNX adapters
pub struct KnxManager {
    sender: Sender<Transmission>,
    config_change: tokio::sync::broadcast::Receiver<ConfigChange>,
    task_monitor: TaskMonitor,
}

impl KnxManager {
    pub fn new(sender: Sender<Transmission>) -> Self {
        KnxManager {
            sender: sender.clone(),
            config_change: CONFIG.read().unwrap().get_change_receiver(),
            task_monitor: TaskMonitor::with_mqtt("knx", sender),
        }
    }

    pub async fn start_thread(&mut self) {
        let config: Vec<KnxAdapterConfig> = crate::get_config_or_panic!("knx", ConfigBases::Knx);

        if config.is_empty() {
            info!("No KNX adapters configured, waiting for config change");
            loop {
                let change = self.config_change.recv().await.unwrap();
                if change.operation != ConfigOperation::ADD || change.base != "knx" {
                    continue;
                }
                break;
            }
        }

        info!("Starting KNX manager, waiting for config to stabilize");
        tokio::time::sleep(Duration::from_secs(3)).await;

        loop {
            let config: Vec<KnxAdapterConfig> = crate::get_config_or_panic!("knx", ConfigBases::Knx);
            let mut device_count: u32 = 0;

            for adapter_config in config.iter() {
                if !adapter_config.enabled {
                    info!("KNX adapter '{}' is disabled, skipping", adapter_config.name);
                    continue;
                }

                device_count += adapter_config.meters.len() as u32;
                device_count += adapter_config.switches.len() as u32;

                let adapter_name = adapter_config.name.clone();
                let sender = self.sender.clone();
                let config = adapter_config.clone();

                self.task_monitor
                    .spawn(
                        &format!("knx_adapter_{}", adapter_name),
                        "knx_adapter",
                        async move {
                            run_adapter(config, sender).await;
                        },
                    )
                    .await;
            }

            publish_protocol_count(&self.sender, "knx", device_count).await;

            info!(
                "KNX manager started with {} adapters, {} devices",
                config.iter().filter(|a| a.enabled).count(),
                device_count
            );

            loop {
                tokio::select! {
                    change_result = self.config_change.recv() => {
                        match change_result {
                            Ok(change) if change.base == "knx" => {
                                info!("KNX config changed, restarting adapters");
                                break;
                            }
                            Ok(_) => continue,
                            Err(e) => {
                                error!("Config change receiver error: {:?}", e);
                                continue;
                            }
                        }
                    }
                    _ = tokio::time::sleep(Duration::from_secs(30)) => {
                        let crashed = self.task_monitor.check_all_tasks().await;
                        if !crashed.is_empty() {
                            warn!(
                                "KNX: {} task(s) crashed: {:?}",
                                crashed.len(),
                                crashed.iter().map(|(name, _, _)| name.clone()).collect::<Vec<_>>()
                            );
                        }
                    }
                }
            }

            info!("Stopping all KNX adapter tasks");
            self.task_monitor.clear_all().await;
        }
    }
}

/// Run a single KNX adapter - loops forever, reconnecting on errors
async fn run_adapter(config: KnxAdapterConfig, sender: Sender<Transmission>) {
    let port = if config.port == 6720 { 3671 } else { config.port };
    info!(
        "Starting KNX adapter '{}' connecting to {}:{}",
        config.name, config.host, port
    );

    // Create shared stats
    let stats: SharedStats = Arc::new(RwLock::new(KnxAdapterStats::default()));

    // Send HA discovery for the adapter itself (parent device for meters)
    let adapter_disc = build_adapter_ha_discovery(&config, true);
    let _ = sender.send(Transmission::AutoDiscovery2(adapter_disc)).await;

    let group_addresses = build_group_address_set(&config);
    let poll_addresses = build_poll_addresses(&config);

    info!(
        "KNX adapter '{}': Monitoring {} group addresses",
        config.name,
        group_addresses.len()
    );

    let mut reconnect_delay = Duration::from_secs(5);
    let max_reconnect_delay = Duration::from_secs(60);

    loop {
        match run_adapter_connection(&config, port, &sender, &group_addresses, &poll_addresses, stats.clone()).await {
            Ok(_) => {
                // This shouldn't happen, but if it does, reconnect
                info!("KNX adapter '{}': Connection ended, reconnecting...", config.name);
                reconnect_delay = Duration::from_secs(5);
            }
            Err(e) => {
                error!(
                    "KNX adapter '{}': Error: {:?}, reconnecting in {:?}",
                    config.name, e, reconnect_delay
                );
                stats.write().await.inc("connection_errors");
            }
        }

        stats.write().await.inc("reconnections");
        tokio::time::sleep(reconnect_delay).await;

        // Exponential backoff with max limit
        reconnect_delay = (reconnect_delay * 2).min(max_reconnect_delay);
    }
}

/// Run the adapter connection
async fn run_adapter_connection(
    config: &KnxAdapterConfig,
    port: u16,
    sender: &Sender<Transmission>,
    group_addresses: &GroupAddressSet,
    poll_addresses: &[(GroupAddress, KnxDatapointType)],
    stats: SharedStats,
) -> Result<(), KnxError> {
    let mut client = KnxClient::new(&config.host, port);
    client.set_read_timeout(Duration::from_secs(config.read_timeout));
    client.connect().await?;

    info!("KNX adapter '{}': Connected and monitoring", config.name);

    let cache: GroupAddressCache = Arc::new(RwLock::new(HashMap::new()));

    let poll_interval_secs = config.meters
        .iter()
        .filter(|m| m.enabled)
        .map(|m| m.read_interval)
        .min()
        .unwrap_or(60);
    let response_wait_secs = config.response_wait.unwrap_or(3);

    let client = Arc::new(client);

    info!(
        "KNX adapter '{}': Starting poll cycle (interval={}s, wait={}s)",
        config.name, poll_interval_secs, response_wait_secs
    );

    // Run both tasks concurrently - return when either completes (with error)
    tokio::select! {
        result = bus_reader_task(
            client.clone(),
            cache.clone(),
            &config.name,
            group_addresses,
            stats.clone(),
        ) => {
            warn!("KNX adapter '{}': Bus reader task ended: {:?}", config.name, result);
            result
        }
        result = poll_cycle_task(
            client.clone(),
            cache.clone(),
            config,
            poll_addresses,
            poll_interval_secs,
            response_wait_secs,
            sender,
            stats.clone(),
        ) => {
            warn!("KNX adapter '{}': Poll cycle task ended: {:?}", config.name, result);
            result
        }
    }
}

/// Bus reader task
async fn bus_reader_task(
    client: Arc<KnxClient>,
    cache: GroupAddressCache,
    adapter_name: &str,
    group_addresses: &GroupAddressSet,
    stats: SharedStats,
) -> Result<(), KnxError> {
    info!("KNX adapter '{}': Bus reader started", adapter_name);

    loop {
        match client.recv_group_event().await {
            Ok(event) => {
                if event.is_read() {
                    debug!("KNX {}: Ignoring read request for {}", adapter_name, event.address);
                    continue;
                }

                let ga_key = event.address.to_u16();

                if group_addresses.contains(&ga_key) {
                    debug!(
                        "KNX {}: Caching {} ({:?}) = {:02X?}",
                        adapter_name, event.address, event.event_type, event.data
                    );

                    let mut cache_guard = cache.write().await;
                    cache_guard.insert(
                        ga_key,
                        CachedValue {
                            data: event.data.clone(),
                            timestamp: Instant::now(),
                        },
                    );
                    stats.write().await.inc("responses_received");
                } else {
                    debug!("KNX {}: Ignoring unmonitored group address {}", adapter_name, event.address);
                }
            }
            Err(KnxError::ReadTimeout) => {
                debug!("KNX {}: Read timeout, continuing", adapter_name);
            }
            Err(e) => {
                error!("KNX {}: Bus reader error: {:?}", adapter_name, e);
                return Err(e);
            }
        }
    }
}

/// Poll cycle task
async fn poll_cycle_task(
    client: Arc<KnxClient>,
    cache: GroupAddressCache,
    config: &KnxAdapterConfig,
    poll_addresses: &[(GroupAddress, KnxDatapointType)],
    poll_interval_secs: u64,
    response_wait_secs: u64,
    sender: &Sender<Transmission>,
    stats: SharedStats,
) -> Result<(), KnxError> {
    let mut last_poll = Instant::now() - Duration::from_secs(poll_interval_secs);
    let mut discovered_meters: HashSet<String> = HashSet::new();

    loop {
        let now = Instant::now();
        let time_since_last_poll = now.duration_since(last_poll);

        if time_since_last_poll >= Duration::from_secs(poll_interval_secs) {
            info!(
                "KNX adapter '{}': Starting poll cycle for {} addresses",
                config.name,
                poll_addresses.len()
            );

            let mut consecutive_errors = 0;
            for (ga, _) in poll_addresses {
                match client.send_read_request(*ga).await {
                    Ok(_) => {
                        debug!("KNX adapter '{}': Sent read request for {}", config.name, ga);
                        stats.write().await.inc("read_requests_sent");
                        consecutive_errors = 0;
                    }
                    Err(KnxError::ConnectionClosed) | Err(KnxError::ConnectionFailed(_)) | Err(KnxError::SequenceOverflow) => {
                        error!(
                            "KNX adapter '{}': Connection lost or overflow while sending read request for {}, reconnecting",
                            config.name, ga
                        );
                        return Err(KnxError::ConnectionClosed);
                    }
                    Err(e) => {
                        warn!(
                            "KNX adapter '{}': Failed to send read request for {}: {:?}",
                            config.name, ga, e
                        );
                        consecutive_errors += 1;
                        if consecutive_errors >= 5 {
                            error!(
                                "KNX adapter '{}': Too many consecutive errors, reconnecting",
                                config.name
                            );
                            return Err(e);
                        }
                    }
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }

            info!("KNX adapter '{}': Waiting {}s for responses", config.name, response_wait_secs);
            tokio::time::sleep(Duration::from_secs(response_wait_secs)).await;

            let cache_snapshot = {
                let cache_guard = cache.read().await;
                cache_guard.clone()
            };

            let mut cycle_cache_hits: u64 = 0;
            let mut cycle_cache_misses: u64 = 0;
            let mut cycle_meters_published: u64 = 0;

            for meter in &config.meters {
                if !meter.enabled {
                    continue;
                }

                // Send Home Assistant discovery if not yet done for this meter
                let meter_key = sanitize_id(&format!("{}_{}", config.name, meter.name));
                if !discovered_meters.contains(&meter_key) {
                    info!(
                        "KNX adapter '{}': Sending HA discovery for meter '{}'",
                        config.name, meter.name
                    );
                    let disc = build_ha_discovery(&config.name, meter);
                    let _ = sender.send(Transmission::AutoDiscovery2(disc)).await;
                    discovered_meters.insert(meter_key);

                    // Give Home Assistant time to process discovery
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }

                let (hits, misses, published) = publish_meter_values(&config.name, meter, &cache_snapshot, sender).await;
                cycle_cache_hits += hits;
                cycle_cache_misses += misses;
                if published {
                    cycle_meters_published += 1;
                }
            }

            // Update stats
            {
                let mut stats_guard = stats.write().await;
                stats_guard.add("cache_hits", cycle_cache_hits);
                stats_guard.add("cache_misses", cycle_cache_misses);
                stats_guard.inc("poll_cycles_completed");
                for _ in 0..cycle_meters_published {
                    stats_guard.inc("meters_published");
                }
            }

            // Publish adapter stats
            let stats_snapshot = stats.read().await.clone();
            publish_adapter_stats(&config.name, &stats_snapshot, sender).await;

            info!(
                "KNX adapter '{}': Poll cycle complete (hits={}, misses={}, published={})",
                config.name, cycle_cache_hits, cycle_cache_misses, cycle_meters_published
            );
            last_poll = Instant::now();
        }

        let sleep_duration = Duration::from_secs(poll_interval_secs)
            .saturating_sub(Instant::now().duration_since(last_poll))
            .max(Duration::from_secs(1));

        tokio::time::sleep(sleep_duration).await;
    }
}

/// Publish all values for a meter in a single MQTT message
/// Returns (cache_hits, cache_misses, published) tuple
async fn publish_meter_values(
    adapter_name: &str,
    meter: &KnxMeterConfig,
    cache: &HashMap<u16, CachedValue>,
    sender: &Sender<Transmission>,
) -> (u64, u64, bool) {
    let mut cache_hits: u64 = 0;
    let mut cache_misses: u64 = 0;
    // Use sanitized device_id matching HA discovery (meter name only)
    let device_id = sanitize_id(&meter.name);

    let mut meter_data = MeteringData::new().unwrap();
    meter_data.protocol = DeviceProtocol::KNX;
    meter_data.state_topic_base = "KNX".to_string();
    meter_data.meter_name = device_id.clone();
    meter_data.id = get_id("knx".to_string(), &device_id);
    meter_data.transmission_time = get_unix_ts();
    meter_data.metered_time = meter_data.transmission_time;

    let mut value_count = 0;

    // Check if this is a multi-phase or single-meter config
    let has_phases = !meter.phases.is_empty();

    if has_phases {
        // Multi-phase configuration
        let mut voltage_sum: Option<f64> = None;
        let mut current_sum: Option<f64> = None;
        let mut power_sum: Option<f64> = None;
        let mut energy_sum: Option<f64> = None;

        for phase in &meter.phases {
            let phase_suffix = sanitize_id(&phase.name);

            // Voltage
            if let Some(ga_str) = &phase.voltage_ga {
                if let Ok(ga) = GroupAddress::from_str(ga_str) {
                    if let Some(cached) = cache.get(&ga.to_u16()) {
                        cache_hits += 1;
                        if let Some(value) = parse_dpt_value(&phase.voltage_type, &cached.data) {
                            if let Some(v) = value.as_f64() {
                                voltage_sum = Some(voltage_sum.unwrap_or(0.0) + v);
                            }
                            meter_data.metered_values.insert(format!("voltage_{}", phase_suffix), value);
                            value_count += 1;
                        }
                    } else {
                        cache_misses += 1;
                    }
                }
            }

            // Current
            if let Some(ga_str) = &phase.current_ga {
                if let Ok(ga) = GroupAddress::from_str(ga_str) {
                    if let Some(cached) = cache.get(&ga.to_u16()) {
                        cache_hits += 1;
                        if let Some(value) = parse_dpt_value(&phase.current_type, &cached.data) {
                            if let Some(v) = value.as_f64() {
                                current_sum = Some(current_sum.unwrap_or(0.0) + v);
                            }
                            meter_data.metered_values.insert(format!("current_{}", phase_suffix), value);
                            value_count += 1;
                        }
                    } else {
                        cache_misses += 1;
                    }
                }
            }

            // Power
            if let Some(ga_str) = &phase.power_ga {
                if let Ok(ga) = GroupAddress::from_str(ga_str) {
                    if let Some(cached) = cache.get(&ga.to_u16()) {
                        cache_hits += 1;
                        if let Some(value) = parse_dpt_value(&phase.power_type, &cached.data) {
                            if let Some(v) = value.as_f64() {
                                power_sum = Some(power_sum.unwrap_or(0.0) + v);
                            }
                            meter_data.metered_values.insert(format!("power_{}", phase_suffix), value);
                            value_count += 1;
                        }
                    } else {
                        cache_misses += 1;
                    }
                }
            }

            // Energy
            if let Some(ga_str) = &phase.energy_ga {
                if let Ok(ga) = GroupAddress::from_str(ga_str) {
                    if let Some(cached) = cache.get(&ga.to_u16()) {
                        cache_hits += 1;
                        if let Some(value) = parse_dpt_value(&phase.energy_type, &cached.data) {
                            if let Some(v) = value.as_f64() {
                                energy_sum = Some(energy_sum.unwrap_or(0.0) + v);
                            }
                            meter_data.metered_values.insert(format!("energy_{}", phase_suffix), value);
                            value_count += 1;
                        }
                    } else {
                        cache_misses += 1;
                    }
                }
            }
        }

        // Handle totals - either from bus or calculated
        // Total energy
        if let Some(ga_str) = &meter.total_energy_ga {
            if let Ok(ga) = GroupAddress::from_str(ga_str) {
                if let Some(cached) = cache.get(&ga.to_u16()) {
                    cache_hits += 1;
                    if let Some(value) = parse_dpt_value(&meter.energy_type, &cached.data) {
                        meter_data.metered_values.insert("energy_all".to_string(), value);
                        value_count += 1;
                    }
                } else {
                    cache_misses += 1;
                }
            }
        } else if meter.calculate_totals {
            if let Some(sum) = energy_sum {
                meter_data.metered_values.insert("energy_all".to_string(), serde_json::Value::from(sum));
                value_count += 1;
            }
        }

        // Total power
        if let Some(ga_str) = &meter.total_power_ga {
            if let Ok(ga) = GroupAddress::from_str(ga_str) {
                if let Some(cached) = cache.get(&ga.to_u16()) {
                    cache_hits += 1;
                    if let Some(value) = parse_dpt_value(&meter.power_type, &cached.data) {
                        meter_data.metered_values.insert("power_all".to_string(), value);
                        value_count += 1;
                    }
                } else {
                    cache_misses += 1;
                }
            }
        } else if meter.calculate_totals {
            if let Some(sum) = power_sum {
                meter_data.metered_values.insert("power_all".to_string(), serde_json::Value::from(sum));
                value_count += 1;
            }
        }

        // Total current
        if let Some(ga_str) = &meter.total_current_ga {
            if let Ok(ga) = GroupAddress::from_str(ga_str) {
                if let Some(cached) = cache.get(&ga.to_u16()) {
                    cache_hits += 1;
                    if let Some(value) = parse_dpt_value(&meter.current_type, &cached.data) {
                        meter_data.metered_values.insert("current_all".to_string(), value);
                        value_count += 1;
                    }
                } else {
                    cache_misses += 1;
                }
            }
        } else if meter.calculate_totals {
            if let Some(sum) = current_sum {
                meter_data.metered_values.insert("current_all".to_string(), serde_json::Value::from(sum));
                value_count += 1;
            }
        }

        // Voltage average (not sum) for 3-phase systems
        if meter.calculate_totals {
            if let Some(sum) = voltage_sum {
                let phase_count = meter.phases.iter().filter(|p| p.voltage_ga.is_some()).count();
                if phase_count > 0 {
                    let avg = sum / phase_count as f64;
                    meter_data.metered_values.insert("voltage_avg".to_string(), serde_json::Value::from(avg));
                    value_count += 1;
                }
            }
        }

    } else {
        // Single-meter configuration (no phases)

        // Voltage
        if let Some(ga_str) = &meter.voltage_ga {
            if let Ok(ga) = GroupAddress::from_str(ga_str) {
                if let Some(cached) = cache.get(&ga.to_u16()) {
                    cache_hits += 1;
                    if let Some(value) = parse_dpt_value(&meter.voltage_type, &cached.data) {
                        meter_data.metered_values.insert("voltage".to_string(), value);
                        value_count += 1;
                    }
                } else {
                    cache_misses += 1;
                }
            }
        }

        // Current
        if let Some(ga_str) = &meter.current_ga {
            if let Ok(ga) = GroupAddress::from_str(ga_str) {
                if let Some(cached) = cache.get(&ga.to_u16()) {
                    cache_hits += 1;
                    if let Some(value) = parse_dpt_value(&meter.current_type, &cached.data) {
                        meter_data.metered_values.insert("current".to_string(), value);
                        value_count += 1;
                    }
                } else {
                    cache_misses += 1;
                }
            }
        }

        // Power
        if let Some(ga_str) = &meter.power_ga {
            if let Ok(ga) = GroupAddress::from_str(ga_str) {
                if let Some(cached) = cache.get(&ga.to_u16()) {
                    cache_hits += 1;
                    if let Some(value) = parse_dpt_value(&meter.power_type, &cached.data) {
                        meter_data.metered_values.insert("power".to_string(), value);
                        value_count += 1;
                    }
                } else {
                    cache_misses += 1;
                }
            }
        }

        // Energy
        if let Some(ga_str) = &meter.energy_ga {
            if let Ok(ga) = GroupAddress::from_str(ga_str) {
                if let Some(cached) = cache.get(&ga.to_u16()) {
                    cache_hits += 1;
                    if let Some(value) = parse_dpt_value(&meter.energy_type, &cached.data) {
                        meter_data.metered_values.insert("energy".to_string(), value);
                        value_count += 1;
                    }
                } else {
                    cache_misses += 1;
                }
            }
        }
    }

    // Only publish if we have values
    let published = if value_count > 0 {
        debug!(
            "KNX {}: Publishing meter '{}' with {} values",
            adapter_name, meter.name, value_count
        );
        let _ = sender.send(Transmission::Metering(meter_data)).await;
        true
    } else {
        debug!(
            "KNX {}: No cached values for meter '{}', skipping publish",
            adapter_name, meter.name
        );
        false
    };

    (cache_hits, cache_misses, published)
}

/// Publish adapter statistics to MQTT
async fn publish_adapter_stats(
    adapter_name: &str,
    stats: &KnxAdapterStats,
    sender: &Sender<Transmission>,
) {
    let device_id = sanitize_id(adapter_name);

    let mut stats_data = MeteringData::new().unwrap();
    stats_data.protocol = DeviceProtocol::KNX;
    stats_data.state_topic_base = "KNX".to_string();
    stats_data.meter_name = device_id.clone();
    stats_data.id = get_id("knx-stats".to_string(), &device_id);
    stats_data.transmission_time = get_unix_ts();
    stats_data.metered_time = stats_data.transmission_time;

    stats_data.metered_values.insert("read_requests_sent".to_string(), serde_json::Value::from(stats.read_requests_sent));
    stats_data.metered_values.insert("responses_received".to_string(), serde_json::Value::from(stats.responses_received));
    stats_data.metered_values.insert("cache_hits".to_string(), serde_json::Value::from(stats.cache_hits));
    stats_data.metered_values.insert("cache_misses".to_string(), serde_json::Value::from(stats.cache_misses));
    stats_data.metered_values.insert("connection_errors".to_string(), serde_json::Value::from(stats.connection_errors));
    stats_data.metered_values.insert("reconnections".to_string(), serde_json::Value::from(stats.reconnections));
    stats_data.metered_values.insert("poll_cycles_completed".to_string(), serde_json::Value::from(stats.poll_cycles_completed));
    stats_data.metered_values.insert("meters_published".to_string(), serde_json::Value::from(stats.meters_published));

    debug!("KNX {}: Publishing adapter stats", adapter_name);
    let _ = sender.send(Transmission::Metering(stats_data)).await;
}

/// Build list of all group addresses to poll
fn build_poll_addresses(config: &KnxAdapterConfig) -> Vec<(GroupAddress, KnxDatapointType)> {
    let mut addresses = Vec::new();

    for meter in &config.meters {
        if !meter.enabled {
            continue;
        }

        // Multi-phase addresses
        for phase in &meter.phases {
            if let Some(ga_str) = &phase.voltage_ga {
                if let Ok(ga) = GroupAddress::from_str(ga_str) {
                    addresses.push((ga, phase.voltage_type.clone()));
                }
            }
            if let Some(ga_str) = &phase.current_ga {
                if let Ok(ga) = GroupAddress::from_str(ga_str) {
                    addresses.push((ga, phase.current_type.clone()));
                }
            }
            if let Some(ga_str) = &phase.power_ga {
                if let Ok(ga) = GroupAddress::from_str(ga_str) {
                    addresses.push((ga, phase.power_type.clone()));
                }
            }
            if let Some(ga_str) = &phase.energy_ga {
                if let Ok(ga) = GroupAddress::from_str(ga_str) {
                    addresses.push((ga, phase.energy_type.clone()));
                }
            }
        }

        // Single-meter addresses
        if let Some(ga_str) = &meter.voltage_ga {
            if let Ok(ga) = GroupAddress::from_str(ga_str) {
                addresses.push((ga, meter.voltage_type.clone()));
            }
        }
        if let Some(ga_str) = &meter.current_ga {
            if let Ok(ga) = GroupAddress::from_str(ga_str) {
                addresses.push((ga, meter.current_type.clone()));
            }
        }
        if let Some(ga_str) = &meter.power_ga {
            if let Ok(ga) = GroupAddress::from_str(ga_str) {
                addresses.push((ga, meter.power_type.clone()));
            }
        }
        if let Some(ga_str) = &meter.energy_ga {
            if let Ok(ga) = GroupAddress::from_str(ga_str) {
                addresses.push((ga, meter.energy_type.clone()));
            }
        }

        // Total addresses
        if let Some(ga_str) = &meter.total_energy_ga {
            if let Ok(ga) = GroupAddress::from_str(ga_str) {
                addresses.push((ga, meter.energy_type.clone()));
            }
        }
        if let Some(ga_str) = &meter.total_power_ga {
            if let Ok(ga) = GroupAddress::from_str(ga_str) {
                addresses.push((ga, meter.power_type.clone()));
            }
        }
        if let Some(ga_str) = &meter.total_current_ga {
            if let Ok(ga) = GroupAddress::from_str(ga_str) {
                addresses.push((ga, meter.current_type.clone()));
            }
        }
    }

    addresses
}

/// Build a set of group addresses for caching
fn build_group_address_set(config: &KnxAdapterConfig) -> GroupAddressSet {
    let mut set = HashSet::new();

    for meter in &config.meters {
        if !meter.enabled {
            continue;
        }

        // Multi-phase addresses
        for phase in &meter.phases {
            if let Some(ga_str) = &phase.voltage_ga {
                if let Ok(ga) = GroupAddress::from_str(ga_str) {
                    set.insert(ga.to_u16());
                }
            }
            if let Some(ga_str) = &phase.current_ga {
                if let Ok(ga) = GroupAddress::from_str(ga_str) {
                    set.insert(ga.to_u16());
                }
            }
            if let Some(ga_str) = &phase.power_ga {
                if let Ok(ga) = GroupAddress::from_str(ga_str) {
                    set.insert(ga.to_u16());
                }
            }
            if let Some(ga_str) = &phase.energy_ga {
                if let Ok(ga) = GroupAddress::from_str(ga_str) {
                    set.insert(ga.to_u16());
                }
            }
        }

        // Single-meter addresses
        if let Some(ga_str) = &meter.voltage_ga {
            if let Ok(ga) = GroupAddress::from_str(ga_str) {
                set.insert(ga.to_u16());
            }
        }
        if let Some(ga_str) = &meter.current_ga {
            if let Ok(ga) = GroupAddress::from_str(ga_str) {
                set.insert(ga.to_u16());
            }
        }
        if let Some(ga_str) = &meter.power_ga {
            if let Ok(ga) = GroupAddress::from_str(ga_str) {
                set.insert(ga.to_u16());
            }
        }
        if let Some(ga_str) = &meter.energy_ga {
            if let Ok(ga) = GroupAddress::from_str(ga_str) {
                set.insert(ga.to_u16());
            }
        }

        // Total addresses
        if let Some(ga_str) = &meter.total_energy_ga {
            if let Ok(ga) = GroupAddress::from_str(ga_str) {
                set.insert(ga.to_u16());
            }
        }
        if let Some(ga_str) = &meter.total_power_ga {
            if let Ok(ga) = GroupAddress::from_str(ga_str) {
                set.insert(ga.to_u16());
            }
        }
        if let Some(ga_str) = &meter.total_current_ga {
            if let Ok(ga) = GroupAddress::from_str(ga_str) {
                set.insert(ga.to_u16());
            }
        }
    }

    set
}

/// Parse a DPT value from raw bytes
fn parse_dpt_value(dpt: &KnxDatapointType, data: &[u8]) -> Option<serde_json::Value> {
    match dpt {
        KnxDatapointType::ActiveEnergyWh
        | KnxDatapointType::ActiveEnergyKwh
        | KnxDatapointType::ReactiveEnergyVah
        | KnxDatapointType::ReactiveEnergyKvarh
        | KnxDatapointType::ApparentEnergyKvah => {
            if data.len() >= 4 {
                let raw = i32::from_be_bytes([data[0], data[1], data[2], data[3]]);
                // Convert to kWh/kVArh/kVAh - divide by 1000 for Wh/VAh types
                let value = match dpt {
                    KnxDatapointType::ActiveEnergyWh | KnxDatapointType::ReactiveEnergyVah => {
                        raw as f64 / 1000.0
                    }
                    _ => raw as f64,
                };
                Some(serde_json::Value::from(value))
            } else {
                None
            }
        }

        KnxDatapointType::PowerW => {
            if data.len() >= 4 {
                let raw = u32::from_be_bytes([data[0], data[1], data[2], data[3]]);
                let value = f32::from_bits(raw) as f64;
                Some(serde_json::Value::from(value))
            } else if data.len() >= 2 {
                let value = parse_dpt9(data);
                Some(serde_json::Value::from(value))
            } else {
                None
            }
        }

        KnxDatapointType::VoltageV
        | KnxDatapointType::CurrentA
        | KnxDatapointType::PowerKw
        | KnxDatapointType::PowerDensityWm2 => {
            if data.len() >= 2 {
                let value = parse_dpt9(data);
                Some(serde_json::Value::from(value))
            } else {
                None
            }
        }

        KnxDatapointType::Switch => {
            if !data.is_empty() {
                let value = (data[0] & 0x01) != 0;
                Some(serde_json::Value::from(if value { "ON" } else { "OFF" }))
            } else {
                None
            }
        }
    }
}

/// Sanitize a name for use as identifier (remove spaces and special chars)
fn sanitize_id(name: &str) -> String {
    name.chars()
        .map(|c| if c.is_alphanumeric() || c == '_' { c } else { '_' })
        .collect::<String>()
        .to_lowercase()
}

/// Get the unit of measurement for a power datapoint type
fn get_power_unit(dpt: &KnxDatapointType) -> &'static str {
    match dpt {
        KnxDatapointType::PowerKw => "kW",
        _ => "W",
    }
}

/// Get the unit of measurement for an energy datapoint type
/// All energy values are converted to kWh/kVArh/kVAh in parse_dpt_value
fn get_energy_unit(dpt: &KnxDatapointType) -> &'static str {
    match dpt {
        KnxDatapointType::ActiveEnergyWh | KnxDatapointType::ActiveEnergyKwh => "kWh",
        KnxDatapointType::ReactiveEnergyVah | KnxDatapointType::ReactiveEnergyKvarh => "kVArh",
        KnxDatapointType::ApparentEnergyKvah => "kVAh",
        _ => "kWh",
    }
}

/// Generate consistent HA device ID for KNX devices
fn get_ha_device_id(proto: &str, name: &str) -> String {
    format!("e2m_{}_{}", proto, sanitize_id(name))
}

/// Build Home Assistant discovery for a KNX adapter (parent device for meters)
fn build_adapter_ha_discovery(config: &KnxAdapterConfig, with_stats: bool) -> HaSensor {
    let proto = "KNX".to_string();
    let device_id = sanitize_id(&config.name);

    let mut disc = HaSensor::new(
        proto.clone(),
        device_id.clone(),
        Some("KNX/IP".to_string()),
        Some("Gateway".to_string()),
    )
    .device_name(config.name.clone())
    .via("e2m_management".to_string());

    // Add state topic for stats
    let state_topic = get_state_topic(&proto, &device_id);
    disc = disc.add_information("state_topic".to_string(), serde_json::Value::from(state_topic));

    if with_stats {
        // Read requests sent
        let cmp = HaComponent2::new()
            .name("Read Requests Sent".to_string())
            .non_numeric()
            .state_class("total_increasing".to_string())
            .cat_diagnostic();
        disc.add_cmp("read_requests_sent".to_string(), cmp);

        // Responses received
        let cmp = HaComponent2::new()
            .name("Responses Received".to_string())
            .non_numeric()
            .state_class("total_increasing".to_string())
            .cat_diagnostic();
        disc.add_cmp("responses_received".to_string(), cmp);

        // Cache hits
        let cmp = HaComponent2::new()
            .name("Cache Hits".to_string())
            .non_numeric()
            .state_class("total_increasing".to_string())
            .cat_diagnostic();
        disc.add_cmp("cache_hits".to_string(), cmp);

        // Cache misses
        let cmp = HaComponent2::new()
            .name("Cache Misses".to_string())
            .non_numeric()
            .state_class("total_increasing".to_string())
            .cat_diagnostic();
        disc.add_cmp("cache_misses".to_string(), cmp);

        // Connection errors
        let cmp = HaComponent2::new()
            .name("Connection Errors".to_string())
            .non_numeric()
            .state_class("total_increasing".to_string())
            .cat_diagnostic();
        disc.add_cmp("connection_errors".to_string(), cmp);

        // Reconnections
        let cmp = HaComponent2::new()
            .name("Reconnections".to_string())
            .non_numeric()
            .state_class("total_increasing".to_string())
            .cat_diagnostic();
        disc.add_cmp("reconnections".to_string(), cmp);

        // Poll cycles completed
        let cmp = HaComponent2::new()
            .name("Poll Cycles Completed".to_string())
            .non_numeric()
            .state_class("total_increasing".to_string())
            .cat_diagnostic();
        disc.add_cmp("poll_cycles_completed".to_string(), cmp);

        // Meters published
        let cmp = HaComponent2::new()
            .name("Meters Published".to_string())
            .non_numeric()
            .state_class("total_increasing".to_string())
            .cat_diagnostic();
        disc.add_cmp("meters_published".to_string(), cmp);
    }

    disc
}

/// Build Home Assistant discovery for a KNX meter
fn build_ha_discovery(adapter_name: &str, meter: &KnxMeterConfig) -> HaSensor {
    let proto = "KNX".to_string();
    let device_id = sanitize_id(&meter.name);

    // Use manufacturer/model from config if set, otherwise defaults
    let manufacturer = meter.manufacturer.clone().unwrap_or_else(|| "KNX".to_string());
    let model = meter.model.clone().unwrap_or_else(|| "Energy Meter".to_string());

    let mut disc = HaSensor::new(
        proto.clone(),
        device_id.clone(),
        Some(manufacturer),
        Some(model),
    );

    // Set the friendly name (original name from config, not sanitized)
    disc = disc.device_name(meter.name.clone());

    // Link to adapter as parent device
    let adapter_device_id = get_ha_device_id(&proto, adapter_name);
    disc = disc.via(adapter_device_id);

    // Set the state topic
    let state_topic = get_state_topic(&proto, &device_id);
    disc = disc.add_information("state_topic".to_string(), serde_json::Value::from(state_topic));

    let has_phases = !meter.phases.is_empty();

    if has_phases {
        // Multi-phase configuration
        for phase in &meter.phases {
            let phase_suffix = sanitize_id(&phase.name);

            // Voltage
            if phase.voltage_ga.is_some() {
                let cmp = HaComponent2::new()
                    .name(format!("Voltage {}", phase.name))
                    .device_class("voltage".to_string())
                    .unit_of_measurement("V".to_string())
                    .state_class("measurement".to_string());
                disc.add_cmp(format!("voltage_{}", phase_suffix), cmp);
            }

            // Current
            if phase.current_ga.is_some() {
                let cmp = HaComponent2::new()
                    .name(format!("Current {}", phase.name))
                    .device_class("current".to_string())
                    .unit_of_measurement("A".to_string())
                    .state_class("measurement".to_string());
                disc.add_cmp(format!("current_{}", phase_suffix), cmp);
            }

            // Power - use configured type to determine unit
            if phase.power_ga.is_some() {
                let cmp = HaComponent2::new()
                    .name(format!("Power {}", phase.name))
                    .device_class("power".to_string())
                    .unit_of_measurement(get_power_unit(&phase.power_type).to_string())
                    .state_class("measurement".to_string());
                disc.add_cmp(format!("power_{}", phase_suffix), cmp);
            }

            // Energy - always kWh (converted in parsing)
            if phase.energy_ga.is_some() {
                let cmp = HaComponent2::new()
                    .name(format!("Energy {}", phase.name))
                    .device_class("energy".to_string())
                    .unit_of_measurement(get_energy_unit(&phase.energy_type).to_string())
                    .state_class("total_increasing".to_string());
                disc.add_cmp(format!("energy_{}", phase_suffix), cmp);
            }
        }

        // Check which phase values are configured
        let has_phase_current = meter.phases.iter().any(|p| p.current_ga.is_some());
        let has_phase_power = meter.phases.iter().any(|p| p.power_ga.is_some());
        let has_phase_energy = meter.phases.iter().any(|p| p.energy_ga.is_some());

        // Totals - only if we have total_*_ga or (calculate_totals AND phases have the value)
        if meter.total_current_ga.is_some() || (meter.calculate_totals && has_phase_current) {
            let cmp = HaComponent2::new()
                .name("Current Total".to_string())
                .device_class("current".to_string())
                .unit_of_measurement("A".to_string())
                .state_class("measurement".to_string());
            disc.add_cmp("current_all".to_string(), cmp);
        }

        if meter.total_power_ga.is_some() || (meter.calculate_totals && has_phase_power) {
            let cmp = HaComponent2::new()
                .name("Power Total".to_string())
                .device_class("power".to_string())
                .unit_of_measurement(get_power_unit(&meter.power_type).to_string())
                .state_class("measurement".to_string());
            disc.add_cmp("power_all".to_string(), cmp);
        }

        if meter.total_energy_ga.is_some() || (meter.calculate_totals && has_phase_energy) {
            let cmp = HaComponent2::new()
                .name("Energy Total".to_string())
                .device_class("energy".to_string())
                .unit_of_measurement(get_energy_unit(&meter.energy_type).to_string())
                .state_class("total_increasing".to_string());
            disc.add_cmp("energy_all".to_string(), cmp);
        }

        // Voltage average for 3-phase systems
        if meter.calculate_totals && meter.phases.iter().any(|p| p.voltage_ga.is_some()) {
            let cmp = HaComponent2::new()
                .name("Voltage Average".to_string())
                .device_class("voltage".to_string())
                .unit_of_measurement("V".to_string())
                .state_class("measurement".to_string());
            disc.add_cmp("voltage_avg".to_string(), cmp);
        }
    } else {
        // Single-meter configuration
        if meter.voltage_ga.is_some() {
            let cmp = HaComponent2::new()
                .name("Voltage".to_string())
                .device_class("voltage".to_string())
                .unit_of_measurement("V".to_string())
                .state_class("measurement".to_string());
            disc.add_cmp("voltage".to_string(), cmp);
        }

        if meter.current_ga.is_some() {
            let cmp = HaComponent2::new()
                .name("Current".to_string())
                .device_class("current".to_string())
                .unit_of_measurement("A".to_string())
                .state_class("measurement".to_string());
            disc.add_cmp("current".to_string(), cmp);
        }

        if meter.power_ga.is_some() {
            let cmp = HaComponent2::new()
                .name("Power".to_string())
                .device_class("power".to_string())
                .unit_of_measurement(get_power_unit(&meter.power_type).to_string())
                .state_class("measurement".to_string());
            disc.add_cmp("power".to_string(), cmp);
        }

        if meter.energy_ga.is_some() {
            let cmp = HaComponent2::new()
                .name("Energy".to_string())
                .device_class("energy".to_string())
                .unit_of_measurement(get_energy_unit(&meter.energy_type).to_string())
                .state_class("total_increasing".to_string());
            disc.add_cmp("energy".to_string(), cmp);
        }
    }

    disc
}

/// Parse DPT9 (2-byte KNX float)
fn parse_dpt9(data: &[u8]) -> f64 {
    if data.len() < 2 {
        return 0.0;
    }

    let raw = u16::from_be_bytes([data[0], data[1]]);
    let sign = if (raw & 0x8000) != 0 { -1.0 } else { 1.0 };
    let exp = ((raw >> 11) & 0x0F) as i32;
    let mantissa = (raw & 0x07FF) as i32;

    let mantissa = if sign < 0.0 {
        mantissa - 2048
    } else {
        mantissa
    };

    0.01 * (mantissa as f64) * 2.0_f64.powi(exp)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_dpt9_positive() {
        let data = [0x0C, 0x1A];
        let value = parse_dpt9(&data);
        assert!((value - 21.0).abs() < 0.5);
    }

    #[test]
    fn test_parse_dpt9_zero() {
        let data = [0x00, 0x00];
        let value = parse_dpt9(&data);
        assert_eq!(value, 0.0);
    }

    #[test]
    fn test_parse_dpt_value_energy_wh() {
        let data = [0x00, 0x01, 0xE2, 0x40];
        let value = parse_dpt_value(&KnxDatapointType::ActiveEnergyWh, &data);
        assert!(value.is_some());
        let v = value.unwrap().as_f64().unwrap();
        assert!((v - 123.456).abs() < 0.001);
    }

    #[test]
    fn test_parse_dpt_value_power_float() {
        let data = [0x44, 0x7A, 0x00, 0x00];
        let value = parse_dpt_value(&KnxDatapointType::PowerW, &data);
        assert!(value.is_some());
        let v = value.unwrap().as_f64().unwrap();
        assert!((v - 1000.0).abs() < 0.01);
    }

    #[test]
    fn test_parse_dpt_value_insufficient_data() {
        let data = [0x00, 0x01];
        let value = parse_dpt_value(&KnxDatapointType::ActiveEnergyKwh, &data);
        assert!(value.is_none());
    }
}
