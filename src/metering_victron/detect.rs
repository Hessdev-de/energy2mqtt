/*
    This code builds the detect engine for Victron devices in energy2mqtt

    The support will be extended further as we got new victron installations to test against

    Clusters:
    - Victron Hub (parent device for all Victron devices)
    - Grid Meters (AC meters connected to grid)
    - Batteries (battery storage systems)
    - PV Chargers (solar charge controllers)
    - VEBus (inverter/charger devices)
*/

use std::sync::Arc;
use log::{error, info};
use rumqttc::AsyncClient;
use serde_json::Value;
use tokio::sync::{mpsc::Sender, Mutex};
use crate::{
    metering_victron::{utils::{self, read_topic_u64, set_topic}, Topic},
    mqtt::{Transmission, home_assistant::{HaSensor, HaComponent2}}
};
use super::VictronData;

/// Sanitize a name for use as device ID (lowercase, spaces to underscores)
fn sanitize_id(name: &str) -> String {
    name.to_lowercase().replace(" ", "_").replace("-", "_")
}

/// Build Home Assistant discovery for the Victron Hub (parent device for all Victron devices)
fn build_hub_discovery(devname: &str, portal_id: &str) -> HaSensor {

    HaSensor::new(
        crate::models::DeviceProtocol::Victron.to_string(),
        sanitize_id(devname),
        Some("Victron".to_string()),
        Some("GX Device".to_string()),
    )
    .device_name(devname.to_string())
    .via("e2m_management".to_string())

}

/// Build Home Assistant discovery for a Grid Meter
fn build_grid_meter_discovery(
    devname: &str,
    meter_idx: u64,
    serial: &str,
    productname: &str,
    nr_phases: u64,
) -> HaSensor {
    let proto = crate::models::DeviceProtocol::Victron.to_string();
    let device_id = format!("{}_grid_{}", sanitize_id(devname), sanitize_id(serial));

    let mut disc = HaSensor::new(
        proto.clone(),
        device_id.clone(),
        Some("Victron".to_string()),
        Some(productname.to_string()),
    )
    .device_name(format!("Grid Meter {}", meter_idx))
    .via(format!("e2m_{}_{}", proto, sanitize_id(devname)));

    // Total energy from grid
    let cmp = HaComponent2::new()
        .name("Energy Imported".to_string())
        .device_class("energy".to_string())
        .unit_of_measurement("kWh".to_string())
        .state_class("total_increasing".to_string());
    disc.add_cmp("energy_positive".to_string(), cmp);

    // Total energy to grid
    let cmp = HaComponent2::new()
        .name("Energy Exported".to_string())
        .device_class("energy".to_string())
        .unit_of_measurement("kWh".to_string())
        .state_class("total_increasing".to_string());
    disc.add_cmp("energy_negative".to_string(), cmp);

    // Grid frequency
    let cmp = HaComponent2::new()
        .name("Frequency".to_string())
        .device_class("frequency".to_string())
        .unit_of_measurement("Hz".to_string())
        .state_class("measurement".to_string());
    disc.add_cmp("frequency".to_string(), cmp);

    // Total power
    let cmp = HaComponent2::new()
        .name("Power".to_string())
        .device_class("power".to_string())
        .unit_of_measurement("W".to_string())
        .state_class("measurement".to_string());
    disc.add_cmp("power".to_string(), cmp);

    // Per-phase measurements
    for p in 1..=nr_phases {
        let phase_suffix = format!("l{}", p);

        // Phase energy positive
        let cmp = HaComponent2::new()
            .name(format!("Energy Imported L{}", p))
            .device_class("energy".to_string())
            .unit_of_measurement("kWh".to_string())
            .state_class("total_increasing".to_string());
        disc.add_cmp(format!("energy_positive_{}", phase_suffix), cmp);

        // Phase energy negative
        let cmp = HaComponent2::new()
            .name(format!("Energy Exported L{}", p))
            .device_class("energy".to_string())
            .unit_of_measurement("kWh".to_string())
            .state_class("total_increasing".to_string());
        disc.add_cmp(format!("energy_negative_{}", phase_suffix), cmp);

        // Phase voltage
        let cmp = HaComponent2::new()
            .name(format!("Voltage L{}", p))
            .device_class("voltage".to_string())
            .unit_of_measurement("V".to_string())
            .state_class("measurement".to_string());
        disc.add_cmp(format!("voltage_{}", phase_suffix), cmp);

        // Phase current
        let cmp = HaComponent2::new()
            .name(format!("Current L{}", p))
            .device_class("current".to_string())
            .unit_of_measurement("A".to_string())
            .state_class("measurement".to_string());
        disc.add_cmp(format!("current_{}", phase_suffix), cmp);

        // Phase power
        let cmp = HaComponent2::new()
            .name(format!("Power L{}", p))
            .device_class("power".to_string())
            .unit_of_measurement("W".to_string())
            .state_class("measurement".to_string());
        disc.add_cmp(format!("power_{}", phase_suffix), cmp);
    }

    disc
}

/// Build Home Assistant discovery for a Battery
fn build_battery_discovery(
    devname: &str,
    battery_idx: usize,
    manufacturer: &str,
    productname: &str,
    is_pylontech: bool,
) -> HaSensor {
    let proto = crate::models::DeviceProtocol::Victron.to_string();
    let device_id = format!("{}_battery_{}", sanitize_id(devname), battery_idx);

    let mut disc = HaSensor::new(
        proto.clone(),
        device_id.clone(),
        Some(manufacturer.to_string()),
        Some(productname.to_string()),
    )
    .device_name(format!("Battery {}", battery_idx))
    .via(format!("e2m_{}_{}", proto, sanitize_id(devname)));

    // State of Charge
    let cmp = HaComponent2::new()
        .name("State of Charge".to_string())
        .device_class("battery".to_string())
        .unit_of_measurement("%".to_string())
        .state_class("measurement".to_string());
    disc.add_cmp("soc".to_string(), cmp);

    // State of Health
    let cmp = HaComponent2::new()
        .name("State of Health".to_string())
        .unit_of_measurement("%".to_string())
        .state_class("measurement".to_string());
    disc.add_cmp("soh".to_string(), cmp);

    // Voltage
    let cmp = HaComponent2::new()
        .name("Voltage".to_string())
        .device_class("voltage".to_string())
        .unit_of_measurement("V".to_string())
        .state_class("measurement".to_string());
    disc.add_cmp("voltage".to_string(), cmp);

    // Current
    let cmp = HaComponent2::new()
        .name("Current".to_string())
        .device_class("current".to_string())
        .unit_of_measurement("A".to_string())
        .state_class("measurement".to_string());
    disc.add_cmp("current".to_string(), cmp);

    // Power
    let cmp = HaComponent2::new()
        .name("Power".to_string())
        .device_class("power".to_string())
        .unit_of_measurement("W".to_string())
        .state_class("measurement".to_string());
    disc.add_cmp("power".to_string(), cmp);

    // Temperature
    let cmp = HaComponent2::new()
        .name("Temperature".to_string())
        .device_class("temperature".to_string())
        .unit_of_measurement("°C".to_string())
        .state_class("measurement".to_string());
    disc.add_cmp("temperature".to_string(), cmp);

    // Pylontech-specific cell data
    if is_pylontech {
        // Min cell temperature
        let cmp = HaComponent2::new()
            .name("Min Cell Temperature".to_string())
            .device_class("temperature".to_string())
            .unit_of_measurement("°C".to_string())
            .state_class("measurement".to_string());
        disc.add_cmp("min_cell_temp".to_string(), cmp);

        // Max cell temperature
        let cmp = HaComponent2::new()
            .name("Max Cell Temperature".to_string())
            .device_class("temperature".to_string())
            .unit_of_measurement("°C".to_string())
            .state_class("measurement".to_string());
        disc.add_cmp("max_cell_temp".to_string(), cmp);

        // Min cell voltage
        let cmp = HaComponent2::new()
            .name("Min Cell Voltage".to_string())
            .device_class("voltage".to_string())
            .unit_of_measurement("V".to_string())
            .state_class("measurement".to_string());
        disc.add_cmp("min_cell_voltage".to_string(), cmp);

        // Max cell voltage
        let cmp = HaComponent2::new()
            .name("Max Cell Voltage".to_string())
            .device_class("voltage".to_string())
            .unit_of_measurement("V".to_string())
            .state_class("measurement".to_string());
        disc.add_cmp("max_cell_voltage".to_string(), cmp);
    }

    disc
}

/// Build Home Assistant discovery for a PV Charger (Solar Charge Controller)
fn build_pv_charger_discovery(
    devname: &str,
    charger_idx: u64,
    productname: &str,
    nr_trackers: u64,
) -> HaSensor {
    let proto = crate::models::DeviceProtocol::Victron.to_string();
    let device_id = format!("{}_pv_{}", sanitize_id(devname), charger_idx);

    let mut disc = HaSensor::new(
        proto.clone(),
        device_id.clone(),
        Some("Victron".to_string()),
        Some(productname.to_string()),
    )
    .device_name(format!("PV Charger {}", charger_idx))
    .via(format!("e2m_{}_{}", proto, sanitize_id(devname)));

    // Total PV power
    let cmp = HaComponent2::new()
        .name("PV Power".to_string())
        .device_class("power".to_string())
        .unit_of_measurement("W".to_string())
        .state_class("measurement".to_string());
    disc.add_cmp("pv_power".to_string(), cmp);

    // Total PV voltage
    let cmp = HaComponent2::new()
        .name("PV Voltage".to_string())
        .device_class("voltage".to_string())
        .unit_of_measurement("V".to_string())
        .state_class("measurement".to_string());
    disc.add_cmp("pv_voltage".to_string(), cmp);

    // Total PV current
    let cmp = HaComponent2::new()
        .name("PV Current".to_string())
        .device_class("current".to_string())
        .unit_of_measurement("A".to_string())
        .state_class("measurement".to_string());
    disc.add_cmp("pv_current".to_string(), cmp);

    // Yield today
    let cmp = HaComponent2::new()
        .name("Yield Today".to_string())
        .device_class("energy".to_string())
        .unit_of_measurement("kWh".to_string())
        .state_class("total_increasing".to_string());
    disc.add_cmp("yield_today".to_string(), cmp);

    // Yield total
    let cmp = HaComponent2::new()
        .name("Yield Total".to_string())
        .device_class("energy".to_string())
        .unit_of_measurement("kWh".to_string())
        .state_class("total_increasing".to_string());
    disc.add_cmp("yield_total".to_string(), cmp);

    // Max power today
    let cmp = HaComponent2::new()
        .name("Max Power Today".to_string())
        .device_class("power".to_string())
        .unit_of_measurement("W".to_string())
        .state_class("measurement".to_string());
    disc.add_cmp("max_power_today".to_string(), cmp);

    // Battery voltage (output)
    let cmp = HaComponent2::new()
        .name("Battery Voltage".to_string())
        .device_class("voltage".to_string())
        .unit_of_measurement("V".to_string())
        .state_class("measurement".to_string());
    disc.add_cmp("battery_voltage".to_string(), cmp);

    // Battery current (output)
    let cmp = HaComponent2::new()
        .name("Battery Current".to_string())
        .device_class("current".to_string())
        .unit_of_measurement("A".to_string())
        .state_class("measurement".to_string());
    disc.add_cmp("battery_current".to_string(), cmp);

    // Charger state
    let cmp = HaComponent2::new()
        .name("Charger State".to_string())
        .non_numeric();
    disc.add_cmp("charger_state".to_string(), cmp);

    // Per-tracker measurements for multi-tracker chargers
    for t in 0..nr_trackers {
        let tracker_suffix = format!("tracker_{}", t);

        // Tracker power
        let cmp = HaComponent2::new()
            .name(format!("Tracker {} Power", t))
            .device_class("power".to_string())
            .unit_of_measurement("W".to_string())
            .state_class("measurement".to_string());
        disc.add_cmp(format!("pv_power_{}", tracker_suffix), cmp);

        // Tracker voltage
        let cmp = HaComponent2::new()
            .name(format!("Tracker {} Voltage", t))
            .device_class("voltage".to_string())
            .unit_of_measurement("V".to_string())
            .state_class("measurement".to_string());
        disc.add_cmp(format!("pv_voltage_{}", tracker_suffix), cmp);
    }

    disc
}

/// Build Home Assistant discovery for a VEBus device (Inverter/Charger)
fn build_vebus_discovery(
    devname: &str,
    instance: u64,
    productname: &str,
) -> HaSensor {
    let proto = crate::models::DeviceProtocol::Victron.to_string();
    let device_id = format!("{}_vebus_{}", sanitize_id(devname), instance);

    let mut disc = HaSensor::new(
        proto.clone(),
        device_id.clone(),
        Some("Victron Energy".to_string()),
        Some(productname.to_string()),
    )
    .device_name(format!("Inverter/Charger {}", instance))
    .via(format!("e2m_{}_{}", proto, sanitize_id(devname)));

    // Energy flow measurements
    let energy_flows = [
        ("energy_inv_to_acin1", "Inverter to AC-IN1"),
        ("energy_inv_to_acin2", "Inverter to AC-IN2"),
        ("energy_acout_to_inv", "AC-Out to Inverter"),
        ("energy_inv_to_acout", "Inverter to AC-Out"),
        ("energy_acin1_to_inv", "AC-IN1 to Inverter"),
        ("energy_acin2_to_inv", "AC-IN2 to Inverter"),
        ("energy_acout_to_acin1", "AC-Out to AC-IN1"),
        ("energy_acout_to_acin2", "AC-Out to AC-IN2"),
        ("energy_acin1_to_acout", "AC-IN1 to AC-Out"),
        ("energy_acin2_to_acout", "AC-IN2 to AC-Out"),
    ];

    for (key, name) in energy_flows {
        let cmp = HaComponent2::new()
            .name(name.to_string())
            .device_class("energy".to_string())
            .unit_of_measurement("kWh".to_string())
            .state_class("total_increasing".to_string());
        disc.add_cmp(key.to_string(), cmp);
    }

    // AC Output power measurements (per phase and total)
    let cmp = HaComponent2::new()
        .name("AC Output Power".to_string())
        .device_class("power".to_string())
        .unit_of_measurement("W".to_string())
        .state_class("measurement".to_string());
    disc.add_cmp("ac_out_power".to_string(), cmp);

    for p in 1..=3 {
        let cmp = HaComponent2::new()
            .name(format!("AC Output Power L{}", p))
            .device_class("power".to_string())
            .unit_of_measurement("W".to_string())
            .state_class("measurement".to_string());
        disc.add_cmp(format!("ac_out_power_l{}", p), cmp);
    }

    // AC Input power measurements (per phase and total)
    let cmp = HaComponent2::new()
        .name("AC Input Power".to_string())
        .device_class("power".to_string())
        .unit_of_measurement("W".to_string())
        .state_class("measurement".to_string());
    disc.add_cmp("ac_in_power".to_string(), cmp);

    for p in 1..=3 {
        let cmp = HaComponent2::new()
            .name(format!("AC Input Power L{}", p))
            .device_class("power".to_string())
            .unit_of_measurement("W".to_string())
            .state_class("measurement".to_string());
        disc.add_cmp(format!("ac_in_power_l{}", p), cmp);
    }

    // DC Power
    let cmp = HaComponent2::new()
        .name("DC Power".to_string())
        .device_class("power".to_string())
        .unit_of_measurement("W".to_string())
        .state_class("measurement".to_string());
    disc.add_cmp("dc_power".to_string(), cmp);

    disc
}

/// Register topic for reading and JSON key mapping with device tracking
async fn register_topic(
    client: &AsyncClient,
    data: &Arc<Mutex<VictronData>>,
    topic: &str,
    json_key: String,
    device_id: String,
) {
    set_topic(client, data, &topic.to_string(), Some(Topic::new_with_device("".to_string(), json_key, device_id))).await;
}

pub async fn run_initial_detection(
    client: &AsyncClient,
    data: &Arc<Mutex<VictronData>>,
    sender: &Sender<Transmission>,
    log_prefix: String,
) -> bool {
    let devname = data.lock().await.conf.name.clone();
    let portal_id = utils::get_portal(&data).await;

    info!("{log_prefix} Starting detection for Victron portal {}", portal_id);

    // Register the portal ID topic
    set_topic(client, data,
        &format!("N/{portal_id}/system/0/Serial"),
        Some(Topic::new_with_key(portal_id.clone(), "portal_id".to_string()))).await;

    // Send Hub discovery (parent device)
    let mut hub_disc = build_hub_discovery(&devname, &portal_id);

    // ========== GRID METERS CLUSTER ==========
    let meter_c = read_topic_u64(client, data,
        &format!("N/{portal_id}/system/0/Ac/In/NumberOfAcInputs"),
        "ac_meter_count".to_string()).await;

    if let Some(meter_count) = meter_c {
        info!("{log_prefix} System has {meter_count} AC meters");
        let mut grid_meter_found = false;

        for i in 0..meter_count {
            let base_topic = format!("N/{portal_id}/system/0/Ac/In/{i}");

            let service = utils::read_topic_string(client, data,
                &format!("{base_topic}/ServiceType"),
                format!("meter_{i}_service")).await.unwrap_or_default();

            if service.is_empty() {
                info!("{log_prefix} ServiceType for meter {i} is unknown");
                continue;
            }

            if service == "grid" {
                grid_meter_found = true;
            }

            let connected = read_topic_u64(client, data,
                &format!("{base_topic}/Connected"),
                format!("meter_{i}_connected")).await.unwrap_or(0);

            if connected != 1 {
                info!("{log_prefix} Meter {i} is not connected");
                continue;
            }

            let device_instance = read_topic_u64(client, data,
                &format!("{base_topic}/DeviceInstance"),
                format!("meter_{i}_deviceInstance")).await.unwrap_or(0);

            if device_instance == 0 {
                info!("{log_prefix} Device Instance for meter {i} is invalid");
                continue;
            }

            let meter_base = format!("N/{portal_id}/{service}/{device_instance}");
            data.lock().await.add_read_topic(format!("{meter_base}/"));

            let serial = utils::read_topic_string(client, data,
                &format!("{meter_base}/Serial"),
                format!("meter_{i}_serial")).await
                .unwrap_or(format!("unknown_{i}"));
            let serial = sanitize_id(&serial);

            let productname = utils::read_topic_string(client, data,
                &format!("{meter_base}/ProductName"),
                format!("meter_{serial}_productname")).await
                .unwrap_or("Energy Meter".to_string());

            let _ = utils::read_topic_string(client, data,
                &format!("{meter_base}/HardwareVersion"),
                format!("meter_{serial}_hardware_version")).await;

            let nr_phases = read_topic_u64(client, data,
                &format!("{meter_base}/NrOfPhases"),
                format!("meter_{serial}_nr_phases")).await.unwrap_or(1);

            info!("{log_prefix} Grid meter {serial} has {nr_phases} phases");

            // Build device ID for JSON keys
            let meter_device_id = format!("{}_grid_{}", sanitize_id(&devname), serial);

            // Register topics with proper JSON keys
            register_topic(client, data,
                &format!("{meter_base}/Ac/Energy/Forward"),
                "energy_positive".to_string(),
                meter_device_id.clone()).await;

            register_topic(client, data,
                &format!("{meter_base}/Ac/Energy/Reverse"),
                "energy_negative".to_string(),
                meter_device_id.clone()).await;

            register_topic(client, data,
                &format!("{meter_base}/Ac/Frequency"),
                "frequency".to_string(),
                meter_device_id.clone()).await;

            register_topic(client, data,
                &format!("{meter_base}/Ac/Power"),
                "power".to_string(),
                meter_device_id.clone()).await;

            for p in 1..=nr_phases {
                let phase_suffix = format!("l{}", p);

                register_topic(client, data,
                    &format!("{meter_base}/Ac/L{p}/Energy/Forward"),
                    format!("energy_positive_{phase_suffix}"),
                    meter_device_id.clone()).await;

                register_topic(client, data,
                    &format!("{meter_base}/Ac/L{p}/Energy/Reverse"),
                    format!("energy_negative_{phase_suffix}"),
                    meter_device_id.clone()).await;

                register_topic(client, data,
                    &format!("{meter_base}/Ac/L{p}/Voltage"),
                    format!("voltage_{phase_suffix}"),
                    meter_device_id.clone()).await;

                register_topic(client, data,
                    &format!("{meter_base}/Ac/L{p}/Current"),
                    format!("current_{phase_suffix}"),
                    meter_device_id.clone()).await;

                register_topic(client, data,
                    &format!("{meter_base}/Ac/L{p}/Power"),
                    format!("power_{phase_suffix}"),
                    meter_device_id.clone()).await;
            }

            // Send Grid Meter discovery
            let disc = build_grid_meter_discovery(&devname, i, &serial, &productname, nr_phases);
            disc.get_disc_topic();
            let _ = sender.send(Transmission::AutoDiscovery2(disc)).await;
        }

        if !grid_meter_found {
            error!("{log_prefix} No grid meter found - initialization may have failed");
        }
    } else {
        info!("{log_prefix} No AC meter data found");
    }

    // ========== BATTERY CLUSTER ==========
    let battery_data = utils::read_topic_value(client, data,
        &format!("N/{portal_id}/system/0/Batteries"),
        "_system_batteries".to_string()).await
        .unwrap_or(Value::Null);

    if let Some(battery_array) = battery_data.as_array() {
        info!("{log_prefix} Found {} batteries", battery_array.len());

        for (b, battery) in battery_array.iter().enumerate() {
            let battery_obj = match battery.as_object() {
                Some(obj) => obj,
                None => {
                    info!("{log_prefix} Battery {b} is not an object");
                    continue;
                }
            };

            let instance = battery_obj.get("instance")
                .and_then(|v| v.as_u64())
                .unwrap_or(0);

            let base_topic = format!("N/{portal_id}/battery/{instance}");
            data.lock().await.add_read_topic(format!("{base_topic}/"));

            let productname = utils::read_topic_string(client, data,
                &format!("{base_topic}/ProductName"),
                format!("battery_{b}_productname")).await
                .unwrap_or("Battery".to_string());

            let manufacturer = utils::read_topic_string(client, data,
                &format!("{base_topic}/Manufacturer"),
                format!("battery_{b}_manufacturer")).await
                .unwrap_or("Unknown".to_string());

            let is_pylontech = manufacturer == "PYLON";

            // Build device ID for JSON keys
            let battery_device_id = format!("{}_battery_{}", sanitize_id(&devname), b);

            // Register battery topics
            register_topic(client, data,
                &format!("{base_topic}/Soc"),
                "soc".to_string(),
                battery_device_id.clone()).await;

            register_topic(client, data,
                &format!("{base_topic}/Soh"),
                "soh".to_string(),
                battery_device_id.clone()).await;

            register_topic(client, data,
                &format!("{base_topic}/Dc/0/Voltage"),
                "voltage".to_string(),
                battery_device_id.clone()).await;

            register_topic(client, data,
                &format!("{base_topic}/Dc/0/Current"),
                "current".to_string(),
                battery_device_id.clone()).await;

            register_topic(client, data,
                &format!("{base_topic}/Dc/0/Power"),
                "power".to_string(),
                battery_device_id.clone()).await;

            register_topic(client, data,
                &format!("{base_topic}/Dc/0/Temperature"),
                "temperature".to_string(),
                battery_device_id.clone()).await;

            if is_pylontech {
                register_topic(client, data,
                    &format!("{base_topic}/System/MinCellTemperature"),
                    "min_cell_temp".to_string(),
                    battery_device_id.clone()).await;

                register_topic(client, data,
                    &format!("{base_topic}/System/MaxCellTemperature"),
                    "max_cell_temp".to_string(),
                    battery_device_id.clone()).await;

                register_topic(client, data,
                    &format!("{base_topic}/System/MinCellVoltage"),
                    "min_cell_voltage".to_string(),
                    battery_device_id.clone()).await;

                register_topic(client, data,
                    &format!("{base_topic}/System/MaxCellVoltage"),
                    "max_cell_voltage".to_string(),
                    battery_device_id.clone()).await;
            }

            // Send Battery discovery
            let disc = build_battery_discovery(&devname, b, &manufacturer, &productname, is_pylontech);
            let _ = sender.send(Transmission::AutoDiscovery2(disc)).await;
        }
    }

    // ========== PV CHARGERS CLUSTER ==========
    let pv_charger_count = read_topic_u64(client, data,
        &format!("N/{portal_id}/system/0/Dc/Pv/NumberOfTrackers"),
        "_pv_tracker_count".to_string()).await.unwrap_or(0);

    if pv_charger_count > 0 {
        info!("{log_prefix} System has {pv_charger_count} PV trackers");

        // Get list of solar chargers
        let chargers_data = utils::read_topic_value(client, data,
            &format!("N/{portal_id}/system/0/Dc/Pv/Chargers"),
            "_pv_chargers".to_string()).await
            .unwrap_or(Value::Null);

        if let Some(chargers_array) = chargers_data.as_array() {
            for (c, charger) in chargers_array.iter().enumerate() {
                let instance = charger.as_u64().unwrap_or(c as u64);

                let base_topic = format!("N/{portal_id}/solarcharger/{instance}");
                data.lock().await.add_read_topic(format!("{base_topic}/"));

                let productname = utils::read_topic_string(client, data,
                    &format!("{base_topic}/ProductName"),
                    format!("pv_{c}_productname")).await
                    .unwrap_or("Solar Charger".to_string());

                let nr_trackers = read_topic_u64(client, data,
                    &format!("{base_topic}/NrOfTrackers"),
                    format!("pv_{c}_nr_trackers")).await.unwrap_or(1);

                // Build device ID for JSON keys
                let pv_device_id = format!("{}_pv_{}", sanitize_id(&devname), c);

                // Register PV charger topics
                register_topic(client, data,
                    &format!("{base_topic}/Yield/Power"),
                    "pv_power".to_string(),
                    pv_device_id.clone()).await;

                register_topic(client, data,
                    &format!("{base_topic}/Pv/V"),
                    "pv_voltage".to_string(),
                    pv_device_id.clone()).await;

                register_topic(client, data,
                    &format!("{base_topic}/Pv/I"),
                    "pv_current".to_string(),
                    pv_device_id.clone()).await;

                register_topic(client, data,
                    &format!("{base_topic}/Yield/User"),
                    "yield_total".to_string(),
                    pv_device_id.clone()).await;

                register_topic(client, data,
                    &format!("{base_topic}/History/Daily/0/Yield"),
                    "yield_today".to_string(),
                    pv_device_id.clone()).await;

                register_topic(client, data,
                    &format!("{base_topic}/History/Daily/0/MaxPower"),
                    "max_power_today".to_string(),
                    pv_device_id.clone()).await;

                register_topic(client, data,
                    &format!("{base_topic}/Dc/0/Voltage"),
                    "battery_voltage".to_string(),
                    pv_device_id.clone()).await;

                register_topic(client, data,
                    &format!("{base_topic}/Dc/0/Current"),
                    "battery_current".to_string(),
                    pv_device_id.clone()).await;

                register_topic(client, data,
                    &format!("{base_topic}/State"),
                    "charger_state".to_string(),
                    pv_device_id.clone()).await;

                // Per-tracker measurements
                for t in 0..nr_trackers {
                    register_topic(client, data,
                        &format!("{base_topic}/Pv/{t}/P"),
                        format!("pv_power_tracker_{t}"),
                        pv_device_id.clone()).await;

                    register_topic(client, data,
                        &format!("{base_topic}/Pv/{t}/V"),
                        format!("pv_voltage_tracker_{t}"),
                        pv_device_id.clone()).await;
                }

                // Send PV Charger discovery
                let disc = build_pv_charger_discovery(&devname, c as u64, &productname, nr_trackers);
                let _ = sender.send(Transmission::AutoDiscovery2(disc)).await;
            }
        }
    }

    // Also check for system-level PV power (in case individual chargers aren't enumerated)
    let system_pv_power = utils::read_topic_value(client, data,
        &format!("N/{portal_id}/system/0/Dc/Pv/Power"),
        "system_pv_power".to_string()).await;

    if system_pv_power.is_some() && pv_charger_count == 0 {
        info!("{log_prefix} Found system-level PV data without individual chargers");
        // The system reports aggregate PV data even without individual charger enumeration
        // This will be published as part of the hub device
    }

    // ========== VEBUS CLUSTER ==========
    let vebus_instance = utils::read_topic_u64(client, data,
        &format!("N/{portal_id}/system/0/VebusInstance"),
        "_vebus_instance".to_string()).await.unwrap_or(0);

    if vebus_instance != 0 {
        let base_topic = format!("N/{portal_id}/vebus/{vebus_instance}");
        data.lock().await.add_read_topic(format!("{base_topic}/"));

        let productname = utils::read_topic_string(client, data,
            &format!("{base_topic}/ProductName"),
            format!("vebus_{vebus_instance}_productname")).await
            .unwrap_or("Inverter/Charger".to_string());

        let _ = utils::read_topic_u64(client, data,
            &format!("{base_topic}/Devices/NumberOfMultis"),
            format!("vebus_{vebus_instance}_nr_of_multis")).await;

        // Build device ID for JSON keys
        let vebus_device_id = format!("{}_vebus_{}", sanitize_id(&devname), vebus_instance);

        // Register VEBus energy flow topics
        let energy_mappings = [
            ("InverterToAcIn1", "energy_inv_to_acin1"),
            ("InverterToAcIn2", "energy_inv_to_acin2"),
            ("OutToInverter", "energy_acout_to_inv"),
            ("InverterToAcOut", "energy_inv_to_acout"),
            ("AcIn1ToInverter", "energy_acin1_to_inv"),
            ("AcIn2ToInverter", "energy_acin2_to_inv"),
            ("AcOutToAcIn1", "energy_acout_to_acin1"),
            ("AcOutToAcIn2", "energy_acout_to_acin2"),
            ("AcIn1ToAcOut", "energy_acin1_to_acout"),
            ("AcIn2ToAcOut", "energy_acin2_to_acout"),
        ];

        for (victron_key, json_key) in energy_mappings {
            register_topic(client, data,
                &format!("{base_topic}/Energy/{victron_key}"),
                json_key.to_string(),
                vebus_device_id.clone()).await;
        }

        // Register AC Output power topics (per phase)
        for p in 1..=3 {
            register_topic(client, data,
                &format!("{base_topic}/Ac/Out/L{p}/P"),
                format!("ac_out_power_l{p}"),
                vebus_device_id.clone()).await;
        }

        // Register AC Input power topics (per phase)
        for p in 1..=3 {
            register_topic(client, data,
                &format!("{base_topic}/Ac/ActiveIn/L{p}/P"),
                format!("ac_in_power_l{p}"),
                vebus_device_id.clone()).await;
        }

        // Register total AC Output power
        register_topic(client, data,
            &format!("{base_topic}/Ac/Out/P"),
            "ac_out_power".to_string(),
            vebus_device_id.clone()).await;

        // Register total AC Input power
        register_topic(client, data,
            &format!("{base_topic}/Ac/ActiveIn/P"),
            "ac_in_power".to_string(),
            vebus_device_id.clone()).await;

        // Register DC power
        register_topic(client, data,
            &format!("{base_topic}/Dc/0/Power"),
            "dc_power".to_string(),
            vebus_device_id.clone()).await;

        // Send VEBus discovery
        let disc = build_vebus_discovery(&devname, vebus_instance, &productname);
        let _ = sender.send(Transmission::AutoDiscovery2(disc)).await;
    }



    /* A parent device needs to have at least one information otherwise Home Assistant will add it as "unnamed device" */
    let cmp = HaComponent2::new()
        .name("Portal ID".to_string())
        .del_information("state_class");
        
    hub_disc.add_cmp("portal_id".to_string(), cmp);

    let _ = sender.send(Transmission::AutoDiscovery2(hub_disc)).await;

    info!("{log_prefix} Detection completed for Victron portal {}", portal_id);
    true
}
