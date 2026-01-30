
use actix_files;
use actix_web::{web, App, HttpRequest, HttpResponse, HttpServer, Responder};
use log::{error, info};
use utoipa::OpenApi;
use utoipa_swagger_ui::SwaggerUi;
use serde::{Serialize, Deserialize};
use std::time::{SystemTime, UNIX_EPOCH, Duration};
use utoipa::ToSchema;

use crate::{config::{ConfigBases, ModbusHubConfig, KnxAdapterConfig, KnxMeterConfig, KnxSwitchConfig, MqttConfig, ConfigHolder, ConfigStatus, ZennerDatahubConfig, OmsConfig, VictronConfig}, get_config_or_panic, CONFIG};
use crate::mqtt::{get_app_status, MqttConnectionStatus, LIVE_EVENTS};
use crate::mqtt::migration::force_cleanup;
use crate::discovered_devices::{DiscoveredDevice, DiscoveredDeviceUpdate, get_discovered_devices};
use rumqttc::{MqttOptions, Client};


pub struct ApiManager;

#[derive(Serialize, ToSchema)]
pub struct HealthResponse {
    pub status: String,
    pub mqtt: MqttHealthInfo,
    pub uptime_seconds: u64,
    pub timestamp: u64,
}

#[derive(Serialize, ToSchema)]
pub struct MqttHealthInfo {
    pub status: String,
    pub last_connected_ago_seconds: Option<u64>,
    pub last_message_sent_ago_seconds: Option<u64>,
    pub last_message_received_ago_seconds: Option<u64>,
    pub connection_attempts: u64,
}

// GET handlers to retrieve the current configuration

#[utoipa::path(get,
    path = "/health",
    summary = "Health check endpoint for container monitoring",
    responses(
        (status = 200, description = "Service is healthy", body = HealthResponse),
        (status = 503, description = "Service is unhealthy")
    ),
)]
pub async fn health_check() -> impl Responder {
    let app_status = get_app_status().await;
    let mqtt_health = &app_status.mqtt_health;
    let now = std::time::Instant::now();
    let system_time = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    // Calculate time differences
    let last_connected_ago = mqtt_health.last_connected
        .map(|t| now.duration_since(t).as_secs());
    let last_message_sent_ago = mqtt_health.last_message_sent
        .map(|t| now.duration_since(t).as_secs());
    let last_message_received_ago = mqtt_health.last_message_received
        .map(|t| now.duration_since(t).as_secs());

    let mqtt_status = match &mqtt_health.status {
        MqttConnectionStatus::Connected => "connected",
        MqttConnectionStatus::Disconnected => "disconnected",
        MqttConnectionStatus::Reconnecting => "reconnecting",
        MqttConnectionStatus::Error(_) => "error",
    };

    // Consider healthy if MQTT is connected
    // The message timing check is too strict for systems without constant traffic
    let overall_healthy = matches!(mqtt_health.status, MqttConnectionStatus::Connected);

    let response = HealthResponse {
        status: if overall_healthy { "healthy".to_string() } else { "unhealthy".to_string() },
        mqtt: MqttHealthInfo {
            status: mqtt_status.to_string(),
            last_connected_ago_seconds: last_connected_ago,
            last_message_sent_ago_seconds: last_message_sent_ago,
            last_message_received_ago_seconds: last_message_received_ago,
            connection_attempts: mqtt_health.connection_attempts,
        },
        uptime_seconds: app_status.uptime_seconds(),
        timestamp: system_time,
    };

    // Always return 200 OK with the health status in the JSON body
    // This allows clients to parse the response and handle status appropriately
    HttpResponse::Ok().json(response)
}

// ==================== SETUP WIZARD ENDPOINTS ====================

#[derive(Serialize, ToSchema)]
pub struct SetupStatusResponse {
    pub needs_setup: bool,
    pub config_status: String,
    pub config_error: Option<String>,
    pub mqtt_configured: bool,
}

#[utoipa::path(get,
    path = "/api/v1/setup/status",
    summary = "Check if initial setup is required",
    responses(
        (status = 200, description = "Setup status", body = SetupStatusResponse)
    ),
)]
pub async fn get_setup_status() -> impl Responder {
    let status = ConfigHolder::get_status();
    let config = CONFIG.read().unwrap();
    let mqtt_configured = config.is_configured();

    let (status_str, error) = match &status {
        ConfigStatus::Valid => ("valid".to_string(), None),
        ConfigStatus::Missing => ("missing".to_string(), None),
        ConfigStatus::Invalid(e) => ("invalid".to_string(), Some(e.clone())),
    };

    let needs_setup = !mqtt_configured || matches!(status, ConfigStatus::Missing | ConfigStatus::Invalid(_));

    HttpResponse::Ok().json(SetupStatusResponse {
        needs_setup,
        config_status: status_str,
        config_error: error,
        mqtt_configured,
    })
}

#[derive(Deserialize, ToSchema)]
pub struct MqttSetupRequest {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub pass: String,
    pub ha_enabled: bool,
    pub client_name: Option<String>,
}

#[derive(Serialize, ToSchema)]
pub struct MqttTestResponse {
    pub success: bool,
    pub message: String,
}

#[utoipa::path(post,
    path = "/api/v1/setup/mqtt/test",
    summary = "Test MQTT connection without saving",
    request_body(content = MqttSetupRequest, description = "MQTT settings to test"),
    responses(
        (status = 200, description = "Test result", body = MqttTestResponse)
    ),
)]
pub async fn test_mqtt_connection(req: web::Json<MqttSetupRequest>) -> impl Responder {
    let client_name = req.client_name.clone().unwrap_or_else(|| "e2m_test".to_string());
    let host = req.host.clone();
    let port = req.port;
    let user = req.user.clone();
    let pass = req.pass.clone();

    // Spawn a blocking task to test the connection
    let result = actix_web::rt::task::spawn_blocking(move || {
        // Create MQTT options
        let mut mqttoptions = MqttOptions::new(&client_name, &host, port);
        mqttoptions.set_keep_alive(Duration::from_secs(5));
        mqttoptions.set_credentials(&user, &pass);

        // Try to connect
        let (client, mut connection) = Client::new(mqttoptions, 10);

        // Try to poll for a connection acknowledgment
        let start = std::time::Instant::now();
        let timeout = Duration::from_secs(10);

        while start.elapsed() < timeout {
            match connection.iter().next() {
                Some(Ok(rumqttc::Event::Incoming(rumqttc::Packet::ConnAck(ack)))) => {
                    // Check if connection was successful
                    if ack.code == rumqttc::ConnectReturnCode::Success {
                        // Disconnect gracefully
                        let _ = client.disconnect();
                        return Ok("Connection successful".to_string());
                    } else {
                        return Err(format!("Connection rejected: {:?}", ack.code));
                    }
                },
                Some(Ok(_)) => continue,
                Some(Err(e)) => {
                    return Err(format!("Connection error: {}", e));
                },
                None => break,
            }
        }
        Err("Connection timeout".to_string())
    }).await;

    match result {
        Ok(Ok(msg)) => HttpResponse::Ok().json(MqttTestResponse {
            success: true,
            message: msg,
        }),
        Ok(Err(msg)) => HttpResponse::Ok().json(MqttTestResponse {
            success: false,
            message: msg,
        }),
        Err(e) => HttpResponse::Ok().json(MqttTestResponse {
            success: false,
            message: format!("Connection test failed: {}", e),
        }),
    }
}

#[utoipa::path(post,
    path = "/api/v1/setup/mqtt/save",
    summary = "Save MQTT configuration and create initial config file",
    request_body(content = MqttSetupRequest, description = "MQTT settings to save"),
    responses(
        (status = 200, description = "Configuration saved"),
        (status = 500, description = "Failed to save configuration")
    ),
)]
pub async fn save_mqtt_setup(req: web::Json<MqttSetupRequest>) -> impl Responder {
    let mqtt_config = MqttConfig {
        host: req.host.clone(),
        port: req.port,
        user: req.user.clone(),
        pass: req.pass.clone(),
        ha_enabled: req.ha_enabled,
        client_name: req.client_name.clone().unwrap_or_else(|| "energy2mqtt".to_string()),
        discovery_version: crate::config::MQTT_DISCOVERY_VERSION_CURRENT,
    };

    // Try to create the config file
    let base_path = {
        let config = CONFIG.read().unwrap();
        config.base_path.clone()
    };

    match ConfigHolder::create_initial_config(mqtt_config.clone(), &base_path) {
        Ok(_) => {
            info!("Initial configuration saved successfully. Scheduling exit for restart...");

            // Schedule graceful exit after a short delay to allow response to be sent
            tokio::spawn(async {
                tokio::time::sleep(Duration::from_secs(2)).await;
                info!("Exiting for restart after initial configuration...");
                std::process::exit(0);
            });

            HttpResponse::Ok().json(serde_json::json!({
                "status": "success",
                "message": "Configuration saved. Service will restart automatically.",
                "restarting": true
            }))
        },
        Err(e) => {
            error!("Failed to save initial config: {}", e);
            HttpResponse::InternalServerError().json(serde_json::json!({
                "status": "error",
                "message": e
            }))
        }
    }
}

#[utoipa::path(get,
    path = "/api/v1/config",
    summary = "Get the whole configuration as stored in the memory of the application",
    responses(
        (status = 200, description = "Get current running config")
    ),
)]
pub async fn get_config() -> impl Responder {
    let config = CONFIG.read().unwrap().get_complete_config();
    HttpResponse::Ok().content_type("application/json").json(config)
}

#[utoipa::path(get,
    path = "/api/v1/config/status",
    summary = "Get configuration status (dirty flag)",
    responses(
        (status = 200, description = "Configuration status")
    ),
)]
pub async fn get_config_status() -> impl Responder {
    let is_dirty = CONFIG.read().unwrap().is_dirty();
    HttpResponse::Ok().content_type("application/json").json(serde_json::json!({
        "dirty": is_dirty
    }))
}

#[utoipa::path(post,
    path = "/api/v1/ha/restart",
    summary = "Restart the service (for Home Assistant integration)",
    responses(
        (status = 200, description = "Restart command received"),
        (status = 500, description = "Failed to restart")
    ),
)]
pub async fn ha_restart_service() -> impl Responder {
    info!("Home Assistant requested service restart");
    // In a real implementation, this would trigger a graceful restart
    // For now, we'll just return a message
    HttpResponse::Ok().json(serde_json::json!({
        "status": "restart_requested",
        "message": "Service restart has been requested. Implementation depends on container orchestration."
    }))
}

#[utoipa::path(post,
    path = "/api/v1/ha/config/save",
    summary = "Force save current configuration (for Home Assistant integration)",
    responses(
        (status = 200, description = "Configuration saved"),
        (status = 500, description = "Failed to save configuration")
    ),
)]
pub async fn ha_save_config() -> impl Responder {
    info!("Home Assistant requested config save");
    // Force the config to be dirty so it will be saved
    {
        let mut config = CONFIG.write().unwrap();
        config.dirty = true;
        config.save();
    }
    HttpResponse::Ok().json(serde_json::json!({
        "status": "success",
        "message": "Configuration saved successfully"
    }))
}

#[utoipa::path(post,
    path = "/api/v1/ha/config/reload",
    summary = "Reload configuration from disk (for Home Assistant integration)",
    responses(
        (status = 200, description = "Configuration reloaded"),
        (status = 500, description = "Failed to reload configuration")
    ),
)]
pub async fn ha_reload_config() -> impl Responder {
    info!("Home Assistant requested config reload");
    // This would need to be implemented in the CONFIG structure
    HttpResponse::Ok().json(serde_json::json!({
        "status": "success",
        "message": "Configuration reload requested. Implementation depends on config management system."
    }))
}

//////////////////// MQTT MIGRATION //////////////////////////////////////////////////////////////////////////////////////

#[utoipa::path(post,
    path = "/api/v1/mqtt/cleanup",
    summary = "Force cleanup of old MQTT discovery topics",
    description = "Scans homeassistant/# for old format e2m discovery topics and deletes them. Use this if you have duplicate entities after upgrading.",
    responses(
        (status = 200, description = "Cleanup completed"),
        (status = 500, description = "Cleanup failed")
    ),
)]
pub async fn force_mqtt_cleanup() -> impl Responder {
    info!("Force MQTT cleanup requested via API");

    let config = get_config_or_panic!("mqtt", ConfigBases::Mqtt);

    match force_cleanup(&config).await {
        Ok(_) => {
            HttpResponse::Ok().json(serde_json::json!({
                "status": "success",
                "message": "Old discovery topics have been cleaned up. Restart energy2mqtt to send new discovery messages."
            }))
        }
        Err(e) => {
            error!("Force cleanup failed: {}", e);
            HttpResponse::InternalServerError().json(serde_json::json!({
                "status": "error",
                "message": format!("Cleanup failed: {}", e)
            }))
        }
    }
}

//////////////////// MODBUS //////////////////////////////////////////////////////////////////////////////////////////////

/* Modbus configuration */
#[utoipa::path(get,
    path = "/api/v1/modbus",
    summary = "Get all modbus hubs and devices",
    responses(
        (status = 200, description = "Get current running modbus config")
    ),
)]
pub async fn get_modbus_config() -> impl Responder {
    let config = get_config_or_panic!("modbus", ConfigBases::Modbus);
    HttpResponse::Ok().content_type("application/json").json(config)
}

//////////////////// MODBUS HUBS ////////////////////////////////////////////////////////////////////////////////////////
// Modbus HUB settings
// Add a new hub
#[utoipa::path(post,
    path = "/api/v1/modbus",
    summary = "Add a new modbus hub, may include devices",
    request_body (content = ModbusHubConfig, description = "Hub definiton to be added to the configuration", content_type = "application/json"),
    responses (
        (status = 200, description = "The hub was added"),
        (status = 400, description = "The name of the hub is already taken")
    ),
)]
pub async fn add_modbus_hub(
    hub_req: web::Json<ModbusHubConfig>,
) -> impl Responder {

    info!("Adding new Modbus Hub {}", hub_req.name);

    let mut config = get_config_or_panic!("modbus", ConfigBases::Modbus);

    // Check if a hub with this name already exists
    if config.hubs.iter().any(|h| h.name == hub_req.name) {
        return HttpResponse::BadRequest().body("Hub with this name already exists");
    }
    
    config.hubs.push(hub_req.into_inner());
    
    let mut writer = CONFIG.write().unwrap();
    writer.update_config(crate::config::ConfigOperation::ADD, ConfigBases::Modbus(config));

    HttpResponse::Created().body("Created")
}


#[utoipa::path(delete,
    path = "/api/v1/modbus/{name}",
    summary = "Delete a modbus hub and all devices which belong to that hub",
    params(
        ("name", description = "Name of the hub to delete")
    ),
    responses(
        (status = 200, description = "The hub was deleted"),
        (status = 404, description = "The hub was not found in the configuration")
    ),
)]
pub async fn delete_modbus_hub(
    path: web::Path<String>,
) -> impl Responder {
    let hub_name = path.into_inner();
    let mut config = get_config_or_panic!("modbus", ConfigBases::Modbus);
    info!("Called to delete \"{hub_name}\"");

    let initial_len = config.hubs.len();
    config.hubs.retain(|h| h.name != hub_name);

    if config.hubs.len() < initial_len {
        // Notify about the config change
        CONFIG.write().unwrap().update_config(crate::config::ConfigOperation::DELETE, ConfigBases::Modbus(config));
        HttpResponse::Ok().body(format!("Hub '{}' deleted", hub_name))
    } else {
        HttpResponse::NotFound().content_type("text/plain").body(format!("Hub '{}' not found", hub_name))
    }
}

#[utoipa::path(put,
    path = "/api/v1/modbus/{name}",
    summary = "Update a modbus hub configuration",
    params(
        ("name", description = "Name of the hub to update")
    ),
    request_body(content = ModbusHubConfig, description = "Updated hub configuration", content_type = "application/json"),
    responses(
        (status = 200, description = "The hub was updated"),
        (status = 404, description = "The hub was not found in the configuration")
    ),
)]
pub async fn update_modbus_hub(
    path: web::Path<String>,
    hub_req: web::Json<ModbusHubConfig>,
) -> impl Responder {
    let hub_name = path.into_inner();
    let mut config = get_config_or_panic!("modbus", ConfigBases::Modbus);
    info!("Updating Modbus Hub \"{}\"", hub_name);

    if let Some(hub) = config.hubs.iter_mut().find(|h| h.name == hub_name) {
        *hub = hub_req.into_inner();
        CONFIG.write().unwrap().update_config(crate::config::ConfigOperation::CHANGE, ConfigBases::Modbus(config));
        HttpResponse::Ok().body(format!("Hub '{}' updated", hub_name))
    } else {
        HttpResponse::NotFound().content_type("text/plain").body(format!("Hub '{}' not found", hub_name))
    }
}

//////////////////// KNX //////////////////////////////////////////////////////////////////////////////////////////////

#[utoipa::path(get,
    path = "/api/v1/knx",
    summary = "Get all KNX adapters configuration",
    responses(
        (status = 200, description = "Get current running KNX config")
    ),
)]
pub async fn get_knx_config() -> impl Responder {
    let config = get_config_or_panic!("knx", ConfigBases::Knx);
    HttpResponse::Ok().content_type("application/json").json(config)
}

#[utoipa::path(post,
    path = "/api/v1/knx",
    summary = "Add a new KNX adapter",
    request_body(content = KnxAdapterConfig, description = "KNX adapter definition to be added", content_type = "application/json"),
    responses(
        (status = 201, description = "The adapter was added"),
        (status = 400, description = "The name of the adapter is already taken")
    ),
)]
pub async fn add_knx_adapter(
    adapter_req: web::Json<KnxAdapterConfig>,
) -> impl Responder {
    info!("Adding new KNX Adapter {}", adapter_req.name);

    let mut config = get_config_or_panic!("knx", ConfigBases::Knx);

    // Check if an adapter with this name already exists
    if config.iter().any(|a| a.name == adapter_req.name) {
        return HttpResponse::BadRequest().body("Adapter with this name already exists");
    }

    config.push(adapter_req.into_inner());

    CONFIG.write().unwrap().update_config(crate::config::ConfigOperation::ADD, ConfigBases::Knx(config));

    HttpResponse::Created().body("Created")
}

#[utoipa::path(put,
    path = "/api/v1/knx/{name}",
    summary = "Update a KNX adapter configuration",
    params(
        ("name", description = "Name of the adapter to update")
    ),
    request_body(content = KnxAdapterConfig, description = "Updated adapter configuration", content_type = "application/json"),
    responses(
        (status = 200, description = "The adapter was updated"),
        (status = 404, description = "The adapter was not found in the configuration")
    ),
)]
pub async fn update_knx_adapter(
    path: web::Path<String>,
    adapter_req: web::Json<KnxAdapterConfig>,
) -> impl Responder {
    let adapter_name = path.into_inner();
    let mut config = get_config_or_panic!("knx", ConfigBases::Knx);
    info!("Updating KNX Adapter \"{}\"", adapter_name);

    if let Some(adapter) = config.iter_mut().find(|a| a.name == adapter_name) {
        *adapter = adapter_req.into_inner();
        CONFIG.write().unwrap().update_config(crate::config::ConfigOperation::CHANGE, ConfigBases::Knx(config));
        HttpResponse::Ok().body(format!("Adapter '{}' updated", adapter_name))
    } else {
        HttpResponse::NotFound().content_type("text/plain").body(format!("Adapter '{}' not found", adapter_name))
    }
}

#[utoipa::path(delete,
    path = "/api/v1/knx/{name}",
    summary = "Delete a KNX adapter and all its meters and switches",
    params(
        ("name", description = "Name of the adapter to delete")
    ),
    responses(
        (status = 200, description = "The adapter was deleted"),
        (status = 404, description = "The adapter was not found in the configuration")
    ),
)]
pub async fn delete_knx_adapter(
    path: web::Path<String>,
) -> impl Responder {
    let adapter_name = path.into_inner();
    let mut config = get_config_or_panic!("knx", ConfigBases::Knx);
    info!("Called to delete KNX adapter \"{adapter_name}\"");

    let initial_len = config.len();
    config.retain(|a| a.name != adapter_name);

    if config.len() < initial_len {
        CONFIG.write().unwrap().update_config(crate::config::ConfigOperation::DELETE, ConfigBases::Knx(config));
        HttpResponse::Ok().body(format!("Adapter '{}' deleted", adapter_name))
    } else {
        HttpResponse::NotFound().content_type("text/plain").body(format!("Adapter '{}' not found", adapter_name))
    }
}

#[utoipa::path(post,
    path = "/api/v1/knx/{adapter_name}/meters",
    summary = "Add a meter to a KNX adapter",
    params(
        ("adapter_name", description = "Name of the adapter to add the meter to")
    ),
    request_body(content = KnxMeterConfig, description = "Meter configuration", content_type = "application/json"),
    responses(
        (status = 201, description = "The meter was added"),
        (status = 404, description = "The adapter was not found")
    ),
)]
pub async fn add_knx_meter(
    path: web::Path<String>,
    meter_req: web::Json<KnxMeterConfig>,
) -> impl Responder {
    let adapter_name = path.into_inner();
    let mut config = get_config_or_panic!("knx", ConfigBases::Knx);

    if let Some(adapter) = config.iter_mut().find(|a| a.name == adapter_name) {
        adapter.meters.push(meter_req.into_inner());
        CONFIG.write().unwrap().update_config(crate::config::ConfigOperation::CHANGE, ConfigBases::Knx(config));
        HttpResponse::Created().body("Meter added")
    } else {
        HttpResponse::NotFound().body(format!("Adapter '{}' not found", adapter_name))
    }
}

#[utoipa::path(delete,
    path = "/api/v1/knx/{adapter_name}/meters/{meter_name}",
    summary = "Delete a meter from a KNX adapter",
    params(
        ("adapter_name", description = "Name of the adapter"),
        ("meter_name", description = "Name of the meter to delete")
    ),
    responses(
        (status = 200, description = "The meter was deleted"),
        (status = 404, description = "The adapter or meter was not found")
    ),
)]
pub async fn delete_knx_meter(
    path: web::Path<(String, String)>,
) -> impl Responder {
    let (adapter_name, meter_name) = path.into_inner();
    let mut config = get_config_or_panic!("knx", ConfigBases::Knx);

    if let Some(adapter) = config.iter_mut().find(|a| a.name == adapter_name) {
        let initial_len = adapter.meters.len();
        adapter.meters.retain(|m| m.name != meter_name);

        if adapter.meters.len() < initial_len {
            CONFIG.write().unwrap().update_config(crate::config::ConfigOperation::CHANGE, ConfigBases::Knx(config));
            HttpResponse::Ok().body(format!("Meter '{}' deleted", meter_name))
        } else {
            HttpResponse::NotFound().body(format!("Meter '{}' not found", meter_name))
        }
    } else {
        HttpResponse::NotFound().body(format!("Adapter '{}' not found", adapter_name))
    }
}

#[utoipa::path(post,
    path = "/api/v1/knx/{adapter_name}/switches",
    summary = "Add a switch to a KNX adapter",
    params(
        ("adapter_name", description = "Name of the adapter to add the switch to")
    ),
    request_body(content = KnxSwitchConfig, description = "Switch configuration", content_type = "application/json"),
    responses(
        (status = 201, description = "The switch was added"),
        (status = 404, description = "The adapter was not found")
    ),
)]
pub async fn add_knx_switch(
    path: web::Path<String>,
    switch_req: web::Json<KnxSwitchConfig>,
) -> impl Responder {
    let adapter_name = path.into_inner();
    let mut config = get_config_or_panic!("knx", ConfigBases::Knx);

    if let Some(adapter) = config.iter_mut().find(|a| a.name == adapter_name) {
        adapter.switches.push(switch_req.into_inner());
        CONFIG.write().unwrap().update_config(crate::config::ConfigOperation::CHANGE, ConfigBases::Knx(config));
        HttpResponse::Created().body("Switch added")
    } else {
        HttpResponse::NotFound().body(format!("Adapter '{}' not found", adapter_name))
    }
}

#[utoipa::path(delete,
    path = "/api/v1/knx/{adapter_name}/switches/{switch_name}",
    summary = "Delete a switch from a KNX adapter",
    params(
        ("adapter_name", description = "Name of the adapter"),
        ("switch_name", description = "Name of the switch to delete")
    ),
    responses(
        (status = 200, description = "The switch was deleted"),
        (status = 404, description = "The adapter or switch was not found")
    ),
)]
pub async fn delete_knx_switch(
    path: web::Path<(String, String)>,
) -> impl Responder {
    let (adapter_name, switch_name) = path.into_inner();
    let mut config = get_config_or_panic!("knx", ConfigBases::Knx);

    if let Some(adapter) = config.iter_mut().find(|a| a.name == adapter_name) {
        let initial_len = adapter.switches.len();
        adapter.switches.retain(|s| s.name != switch_name);

        if adapter.switches.len() < initial_len {
            CONFIG.write().unwrap().update_config(crate::config::ConfigOperation::CHANGE, ConfigBases::Knx(config));
            HttpResponse::Ok().body(format!("Switch '{}' deleted", switch_name))
        } else {
            HttpResponse::NotFound().body(format!("Switch '{}' not found", switch_name))
        }
    } else {
        HttpResponse::NotFound().body(format!("Adapter '{}' not found", adapter_name))
    }
}

//////////////////// ZENNER DATAHUB //////////////////////////////////////////////////////////////////////////////////////

#[utoipa::path(get,
    path = "/api/v1/zenner",
    summary = "Get all Zenner Datahub instances configuration",
    responses(
        (status = 200, description = "Get current Zenner Datahub config")
    ),
)]
pub async fn get_zenner_config() -> impl Responder {
    let config = get_config_or_panic!("zridh", ConfigBases::ZRIDH);
    HttpResponse::Ok().content_type("application/json").json(config)
}

#[utoipa::path(post,
    path = "/api/v1/zenner",
    summary = "Add a new Zenner Datahub instance",
    request_body(content = ZennerDatahubConfig, description = "Zenner Datahub instance definition", content_type = "application/json"),
    responses(
        (status = 201, description = "The instance was added"),
        (status = 400, description = "The name is already taken")
    ),
)]
pub async fn add_zenner_instance(
    instance_req: web::Json<ZennerDatahubConfig>,
) -> impl Responder {
    info!("Adding new Zenner Datahub instance {}", instance_req.name);

    let mut config = get_config_or_panic!("zridh", ConfigBases::ZRIDH);

    // Check if an instance with this name already exists
    if config.iter().any(|i| i.name == instance_req.name) {
        return HttpResponse::BadRequest().body("Instance with this name already exists");
    }

    config.push(instance_req.into_inner());

    CONFIG.write().unwrap().update_config(crate::config::ConfigOperation::ADD, ConfigBases::ZRIDH(config));

    HttpResponse::Created().body("Created")
}

#[utoipa::path(put,
    path = "/api/v1/zenner/{name}",
    summary = "Update a Zenner Datahub instance configuration",
    params(
        ("name", description = "Name of the instance to update")
    ),
    request_body(content = ZennerDatahubConfig, description = "Updated instance configuration", content_type = "application/json"),
    responses(
        (status = 200, description = "The instance was updated"),
        (status = 404, description = "The instance was not found")
    ),
)]
pub async fn update_zenner_instance(
    path: web::Path<String>,
    instance_req: web::Json<ZennerDatahubConfig>,
) -> impl Responder {
    let instance_name = path.into_inner();
    let mut config = get_config_or_panic!("zridh", ConfigBases::ZRIDH);
    info!("Updating Zenner Datahub instance \"{}\"", instance_name);

    if let Some(instance) = config.iter_mut().find(|i| i.name == instance_name) {
        *instance = instance_req.into_inner();
        CONFIG.write().unwrap().update_config(crate::config::ConfigOperation::CHANGE, ConfigBases::ZRIDH(config));
        HttpResponse::Ok().body(format!("Instance '{}' updated", instance_name))
    } else {
        HttpResponse::NotFound().content_type("text/plain").body(format!("Instance '{}' not found", instance_name))
    }
}

#[utoipa::path(delete,
    path = "/api/v1/zenner/{name}",
    summary = "Delete a Zenner Datahub instance",
    params(
        ("name", description = "Name of the instance to delete")
    ),
    responses(
        (status = 200, description = "The instance was deleted"),
        (status = 404, description = "The instance was not found")
    ),
)]
pub async fn delete_zenner_instance(
    path: web::Path<String>,
) -> impl Responder {
    let instance_name = path.into_inner();
    let mut config = get_config_or_panic!("zridh", ConfigBases::ZRIDH);
    info!("Called to delete Zenner Datahub instance \"{instance_name}\"");

    let initial_len = config.len();
    config.retain(|i| i.name != instance_name);

    if config.len() < initial_len {
        CONFIG.write().unwrap().update_config(crate::config::ConfigOperation::DELETE, ConfigBases::ZRIDH(config));
        HttpResponse::Ok().body(format!("Instance '{}' deleted", instance_name))
    } else {
        HttpResponse::NotFound().content_type("text/plain").body(format!("Instance '{}' not found", instance_name))
    }
}

//////////////////// OMS METERS ////////////////////////////////////////////////////////////////////////////////////////

#[utoipa::path(get,
    path = "/api/v1/oms",
    summary = "Get all OMS meters configuration",
    responses(
        (status = 200, description = "Get current OMS meters config")
    ),
)]
pub async fn get_oms_config() -> impl Responder {
    let config = get_config_or_panic!("oms", ConfigBases::Oms);
    HttpResponse::Ok().content_type("application/json").json(config)
}

#[utoipa::path(post,
    path = "/api/v1/oms",
    summary = "Add a new OMS meter",
    request_body(content = OmsConfig, description = "OMS meter definition", content_type = "application/json"),
    responses(
        (status = 201, description = "The meter was added"),
        (status = 400, description = "The name or ID is already taken, or invalid key format")
    ),
)]
pub async fn add_oms_meter(
    meter_req: web::Json<OmsConfig>,
) -> impl Responder {
    info!("Adding new OMS meter {}", meter_req.name);

    // Validate key format (must be 32 hex characters for AES-128)
    if meter_req.key.len() != 32 || !meter_req.key.chars().all(|c| c.is_ascii_hexdigit()) {
        return HttpResponse::BadRequest().body("Key must be exactly 32 hexadecimal characters (16 bytes for AES-128)");
    }

    let mut config = get_config_or_panic!("oms", ConfigBases::Oms);

    // Check if a meter with this name already exists
    if config.iter().any(|m| m.name == meter_req.name) {
        return HttpResponse::BadRequest().body("Meter with this name already exists");
    }

    // Check if a meter with this ID already exists
    if config.iter().any(|m| m.id == meter_req.id) {
        return HttpResponse::BadRequest().body("Meter with this ID already exists");
    }

    config.push(meter_req.into_inner());

    CONFIG.write().unwrap().update_config(crate::config::ConfigOperation::ADD, ConfigBases::Oms(config));

    HttpResponse::Created().body("Created")
}

#[utoipa::path(put,
    path = "/api/v1/oms/{name}",
    summary = "Update an OMS meter configuration",
    params(
        ("name", description = "Name of the meter to update")
    ),
    request_body(content = OmsConfig, description = "Updated meter configuration", content_type = "application/json"),
    responses(
        (status = 200, description = "The meter was updated"),
        (status = 400, description = "Invalid key format"),
        (status = 404, description = "The meter was not found")
    ),
)]
pub async fn update_oms_meter(
    path: web::Path<String>,
    meter_req: web::Json<OmsConfig>,
) -> impl Responder {
    let meter_name = path.into_inner();

    // Validate key format
    if meter_req.key.len() != 32 || !meter_req.key.chars().all(|c| c.is_ascii_hexdigit()) {
        return HttpResponse::BadRequest().body("Key must be exactly 32 hexadecimal characters (16 bytes for AES-128)");
    }

    let mut config = get_config_or_panic!("oms", ConfigBases::Oms);
    info!("Updating OMS meter \"{}\"", meter_name);

    if let Some(meter) = config.iter_mut().find(|m| m.name == meter_name) {
        *meter = meter_req.into_inner();
        CONFIG.write().unwrap().update_config(crate::config::ConfigOperation::CHANGE, ConfigBases::Oms(config));
        HttpResponse::Ok().body(format!("Meter '{}' updated", meter_name))
    } else {
        HttpResponse::NotFound().content_type("text/plain").body(format!("Meter '{}' not found", meter_name))
    }
}

#[utoipa::path(delete,
    path = "/api/v1/oms/{name}",
    summary = "Delete an OMS meter",
    params(
        ("name", description = "Name of the meter to delete")
    ),
    responses(
        (status = 200, description = "The meter was deleted"),
        (status = 404, description = "The meter was not found")
    ),
)]
pub async fn delete_oms_meter(
    path: web::Path<String>,
) -> impl Responder {
    let meter_name = path.into_inner();
    let mut config = get_config_or_panic!("oms", ConfigBases::Oms);
    info!("Called to delete OMS meter \"{meter_name}\"");

    let initial_len = config.len();
    config.retain(|m| m.name != meter_name);

    if config.len() < initial_len {
        CONFIG.write().unwrap().update_config(crate::config::ConfigOperation::DELETE, ConfigBases::Oms(config));
        HttpResponse::Ok().body(format!("Meter '{}' deleted", meter_name))
    } else {
        HttpResponse::NotFound().content_type("text/plain").body(format!("Meter '{}' not found", meter_name))
    }
}

//////////////////// VICTRON ////////////////////////////////////////////////////////////////////////////////////////

#[utoipa::path(get,
    path = "/api/v1/victron",
    summary = "Get all Victron GX device configuration",
    responses(
        (status = 200, description = "Get current Victron config")
    ),
)]
pub async fn get_victron_config() -> impl Responder {
    let config = get_config_or_panic!("victron", ConfigBases::Victron);
    HttpResponse::Ok().content_type("application/json").json(config)
}

#[utoipa::path(post,
    path = "/api/v1/victron",
    summary = "Add a new Victron GX device",
    request_body(content = VictronConfig, description = "Victron GX device definition", content_type = "application/json"),
    responses(
        (status = 201, description = "The device was added"),
        (status = 400, description = "The name is already taken")
    ),
)]
pub async fn add_victron_instance(
    instance_req: web::Json<VictronConfig>,
) -> impl Responder {
    info!("Adding new Victron GX device {}", instance_req.name);

    let mut config = get_config_or_panic!("victron", ConfigBases::Victron);

    // Check if a device with this name already exists
    if config.iter().any(|i| i.name == instance_req.name) {
        return HttpResponse::BadRequest().body("Device with this name already exists");
    }

    config.push(instance_req.into_inner());

    CONFIG.write().unwrap().update_config(crate::config::ConfigOperation::ADD, ConfigBases::Victron(config));

    HttpResponse::Created().body("Created")
}

#[utoipa::path(put,
    path = "/api/v1/victron/{name}",
    summary = "Update a Victron GX device configuration",
    params(
        ("name", description = "Name of the device to update")
    ),
    request_body(content = VictronConfig, description = "Updated device configuration", content_type = "application/json"),
    responses(
        (status = 200, description = "The device was updated"),
        (status = 404, description = "The device was not found")
    ),
)]
pub async fn update_victron_instance(
    path: web::Path<String>,
    instance_req: web::Json<VictronConfig>,
) -> impl Responder {
    let instance_name = path.into_inner();
    let mut config = get_config_or_panic!("victron", ConfigBases::Victron);
    info!("Updating Victron GX device \"{}\"", instance_name);

    if let Some(instance) = config.iter_mut().find(|i| i.name == instance_name) {
        *instance = instance_req.into_inner();
        CONFIG.write().unwrap().update_config(crate::config::ConfigOperation::CHANGE, ConfigBases::Victron(config));
        HttpResponse::Ok().body(format!("Device '{}' updated", instance_name))
    } else {
        HttpResponse::NotFound().content_type("text/plain").body(format!("Device '{}' not found", instance_name))
    }
}

#[utoipa::path(delete,
    path = "/api/v1/victron/{name}",
    summary = "Delete a Victron GX device",
    params(
        ("name", description = "Name of the device to delete")
    ),
    responses(
        (status = 200, description = "The device was deleted"),
        (status = 404, description = "The device was not found")
    ),
)]
pub async fn delete_victron_instance(
    path: web::Path<String>,
) -> impl Responder {
    let instance_name = path.into_inner();
    let mut config = get_config_or_panic!("victron", ConfigBases::Victron);
    info!("Called to delete Victron GX device \"{instance_name}\"");

    let initial_len = config.len();
    config.retain(|i| i.name != instance_name);

    if config.len() < initial_len {
        CONFIG.write().unwrap().update_config(crate::config::ConfigOperation::DELETE, ConfigBases::Victron(config));
        HttpResponse::Ok().body(format!("Device '{}' deleted", instance_name))
    } else {
        HttpResponse::NotFound().content_type("text/plain").body(format!("Device '{}' not found", instance_name))
    }
}

// Websocket to push config changes to the client log file
#[utoipa::path(get,
    path = "/api/v1/ws/configChanges",
    summary = "Websocket to get live config changes",
    responses(
        (status = 101, description = "The websocket is active and can be used to get configuration updates"),
    ),
)]
pub async fn ws_config_changes(req: HttpRequest, body: web::Payload) -> actix_web::Result<impl Responder> {
    let (response, mut session, mut _msg_stream) = actix_ws::handle(&req, body)?;

    let mut thread_receiver = CONFIG.read().unwrap().get_change_receiver();
    actix_web::rt::spawn(async move {

        while let Ok(msg) = thread_receiver.recv().await {
            let _ = session.text(serde_json::to_string(&msg).unwrap()).await;
        }

        let _ = session.close(None).await;
    });

    Ok(response)
}

// WebSocket for live MQTT traffic monitoring
#[utoipa::path(get,
    path = "/api/v1/ws/live",
    summary = "WebSocket to get live MQTT traffic",
    responses(
        (status = 101, description = "The websocket is active and streams all MQTT traffic"),
    ),
)]
pub async fn ws_live_events(req: HttpRequest, body: web::Payload) -> actix_web::Result<impl Responder> {
    let (response, mut session, mut _msg_stream) = actix_ws::handle(&req, body)?;

    let mut live_receiver = LIVE_EVENTS.subscribe();
    actix_web::rt::spawn(async move {
        while let Ok(event) = live_receiver.recv().await {
            if let Ok(json) = serde_json::to_string(&event) {
                if session.text(json).await.is_err() {
                    break;
                }
            }
        }

        let _ = session.close(None).await;
    });

    Ok(response)
}

#[utoipa::path(get,
    path = "/prometheus/metrics",
    summary = "Get all information in prometheus format",
    responses(
        (status = 200, description = "Retuns the current application status as prometheus")
    ),
)]
pub async fn e2m_prometheus_generic() -> impl Responder {
    HttpResponse::Ok().content_type("text/plain").body("")
}

#[utoipa::path(get,
    path = "/prometheus/metering",
    summary = "Get all metering data in prometheus format",
    responses(
        (status = 200, description = "Retuns the current meter data as prometheus")
    ),
)]
pub async fn e2m_prometheus_metering() -> impl Responder {
    HttpResponse::Ok().content_type("text/plain").body("")
}

// ==================== DISCOVERED DEVICES ENDPOINTS ====================

/// Response containing discovered devices summary
#[derive(Serialize, ToSchema)]
pub struct DiscoveredDevicesSummary {
    pub total_devices: usize,
    pub protocols: std::collections::HashMap<String, usize>,
}

/// Response containing devices for a specific protocol
#[derive(Serialize, ToSchema)]
pub struct ProtocolDevicesResponse {
    pub protocol: String,
    pub total_devices: usize,
    pub instances: std::collections::HashMap<String, std::collections::HashMap<String, DiscoveredDevice>>,
}

/// Response containing devices for a specific instance
#[derive(Serialize, ToSchema)]
pub struct InstanceDevicesResponse {
    pub protocol: String,
    pub instance: String,
    pub devices: std::collections::HashMap<String, DiscoveredDevice>,
}

#[utoipa::path(get,
    path = "/api/v1/discovered",
    summary = "Get summary of all discovered devices",
    responses(
        (status = 200, description = "Discovered devices summary", body = DiscoveredDevicesSummary),
        (status = 500, description = "Store not initialized")
    ),
)]
pub async fn get_discovered_summary() -> impl Responder {
    let store = match get_discovered_devices() {
        Some(s) => s,
        None => return HttpResponse::InternalServerError().json(serde_json::json!({
            "error": "Discovered devices store not initialized"
        })),
    };

    let summary = store.get_summary();
    let total: usize = summary.values().sum();

    HttpResponse::Ok().json(DiscoveredDevicesSummary {
        total_devices: total,
        protocols: summary,
    })
}

#[utoipa::path(get,
    path = "/api/v1/discovered/{protocol}",
    summary = "Get all discovered devices for a protocol",
    params(
        ("protocol" = String, Path, description = "Protocol name (e.g., zenner_datahub)")
    ),
    responses(
        (status = 200, description = "Protocol devices", body = ProtocolDevicesResponse),
        (status = 500, description = "Store not initialized")
    ),
)]
pub async fn get_discovered_protocol(path: web::Path<String>) -> impl Responder {
    let protocol = path.into_inner();

    let store = match get_discovered_devices() {
        Some(s) => s,
        None => return HttpResponse::InternalServerError().json(serde_json::json!({
            "error": "Discovered devices store not initialized"
        })),
    };

    let instances = store.get_all_devices(&protocol);
    let total: usize = instances.values().map(|d| d.len()).sum();

    HttpResponse::Ok().json(ProtocolDevicesResponse {
        protocol,
        total_devices: total,
        instances,
    })
}

#[utoipa::path(get,
    path = "/api/v1/discovered/{protocol}/{instance}",
    summary = "Get discovered devices for a specific instance",
    params(
        ("protocol" = String, Path, description = "Protocol name"),
        ("instance" = String, Path, description = "Instance name")
    ),
    responses(
        (status = 200, description = "Instance devices", body = InstanceDevicesResponse),
        (status = 500, description = "Store not initialized")
    ),
)]
pub async fn get_discovered_instance(path: web::Path<(String, String)>) -> impl Responder {
    let (protocol, instance) = path.into_inner();

    let store = match get_discovered_devices() {
        Some(s) => s,
        None => return HttpResponse::InternalServerError().json(serde_json::json!({
            "error": "Discovered devices store not initialized"
        })),
    };

    let devices = store.get_instance_devices(&protocol, &instance);

    HttpResponse::Ok().json(InstanceDevicesResponse {
        protocol,
        instance,
        devices,
    })
}

#[utoipa::path(get,
    path = "/api/v1/discovered/{protocol}/{instance}/{device_id}",
    summary = "Get a specific discovered device",
    params(
        ("protocol" = String, Path, description = "Protocol name"),
        ("instance" = String, Path, description = "Instance name"),
        ("device_id" = String, Path, description = "Device ID")
    ),
    responses(
        (status = 200, description = "Device found", body = DiscoveredDevice),
        (status = 404, description = "Device not found"),
        (status = 500, description = "Store not initialized")
    ),
)]
pub async fn get_discovered_device(path: web::Path<(String, String, String)>) -> impl Responder {
    let (protocol, instance, device_id) = path.into_inner();

    let store = match get_discovered_devices() {
        Some(s) => s,
        None => return HttpResponse::InternalServerError().json(serde_json::json!({
            "error": "Discovered devices store not initialized"
        })),
    };

    match store.get_device(&protocol, &instance, &device_id) {
        Some(device) => HttpResponse::Ok().json(device),
        None => HttpResponse::NotFound().json(serde_json::json!({
            "error": "Device not found"
        })),
    }
}

#[utoipa::path(patch,
    path = "/api/v1/discovered/{protocol}/{instance}/{device_id}",
    summary = "Update a discovered device",
    params(
        ("protocol" = String, Path, description = "Protocol name"),
        ("instance" = String, Path, description = "Instance name"),
        ("device_id" = String, Path, description = "Device ID")
    ),
    request_body = DiscoveredDeviceUpdate,
    responses(
        (status = 200, description = "Device updated", body = DiscoveredDevice),
        (status = 404, description = "Device not found"),
        (status = 500, description = "Update failed")
    ),
)]
pub async fn update_discovered_device(
    path: web::Path<(String, String, String)>,
    body: web::Json<DiscoveredDeviceUpdate>,
) -> impl Responder {
    let (protocol, instance, device_id) = path.into_inner();

    let store = match get_discovered_devices() {
        Some(s) => s,
        None => return HttpResponse::InternalServerError().json(serde_json::json!({
            "error": "Discovered devices store not initialized"
        })),
    };

    match store.update_device(&protocol, &instance, &device_id, body.into_inner()) {
        Ok(device) => {
            // Save changes
            if let Err(e) = store.save() {
                log::error!("Failed to save discovered devices: {}", e);
            }
            HttpResponse::Ok().json(device)
        }
        Err(e) => {
            if e.contains("not found") {
                HttpResponse::NotFound().json(serde_json::json!({
                    "error": e
                }))
            } else {
                HttpResponse::InternalServerError().json(serde_json::json!({
                    "error": e
                }))
            }
        }
    }
}

#[utoipa::path(delete,
    path = "/api/v1/discovered/{protocol}/{instance}/{device_id}",
    summary = "Delete/forget a discovered device",
    params(
        ("protocol" = String, Path, description = "Protocol name"),
        ("instance" = String, Path, description = "Instance name"),
        ("device_id" = String, Path, description = "Device ID")
    ),
    responses(
        (status = 200, description = "Device deleted"),
        (status = 500, description = "Delete failed")
    ),
)]
pub async fn delete_discovered_device(path: web::Path<(String, String, String)>) -> impl Responder {
    let (protocol, instance, device_id) = path.into_inner();

    let store = match get_discovered_devices() {
        Some(s) => s,
        None => return HttpResponse::InternalServerError().json(serde_json::json!({
            "error": "Discovered devices store not initialized"
        })),
    };

    match store.delete_device(&protocol, &instance, &device_id) {
        Ok(()) => {
            // Save changes
            if let Err(e) = store.save() {
                log::error!("Failed to save discovered devices: {}", e);
            }
            HttpResponse::Ok().json(serde_json::json!({
                "status": "deleted"
            }))
        }
        Err(e) => HttpResponse::InternalServerError().json(serde_json::json!({
            "error": e
        })),
    }
}

impl ApiManager {
    pub fn new() -> Self {
        return ApiManager;
    }

    pub async fn start_thread(&self) {

        let config = get_config_or_panic!("httpd", ConfigBases::Httpd);

        if !config.enabled {
            error!("Sorry webserver can not be disabled, please fix config");
        }
        
        #[derive(OpenApi)]
        #[openapi(
            info(description = "energy2MQTT API description"),
            paths(
                    health_check,
                    get_setup_status,
                    test_mqtt_connection,
                    save_mqtt_setup,
                    get_config,
                    get_config_status,
                    ws_config_changes,
                    ws_live_events,
                    get_modbus_config,
                    add_modbus_hub,
                    update_modbus_hub,
                    delete_modbus_hub,
                    get_knx_config,
                    add_knx_adapter,
                    update_knx_adapter,
                    delete_knx_adapter,
                    add_knx_meter,
                    delete_knx_meter,
                    add_knx_switch,
                    delete_knx_switch,
                    get_zenner_config,
                    add_zenner_instance,
                    update_zenner_instance,
                    delete_zenner_instance,
                    get_oms_config,
                    add_oms_meter,
                    update_oms_meter,
                    delete_oms_meter,
                    ha_restart_service,
                    ha_save_config,
                    ha_reload_config,
                    get_discovered_summary,
                    get_discovered_protocol,
                    get_discovered_instance,
                    get_discovered_device,
                    update_discovered_device,
                    delete_discovered_device,
            )
        )]
        struct ApiDoc;

        let _ = HttpServer::new(move || {
            App::new()
                // Register routes
                .route("/health", web::get().to(health_check))
                // Setup wizard routes
                .route("/api/v1/setup/status", web::get().to(get_setup_status))
                .route("/api/v1/setup/mqtt/test", web::post().to(test_mqtt_connection))
                .route("/api/v1/setup/mqtt/save", web::post().to(save_mqtt_setup))
                .route("/api/v1/config", web::get().to(get_config))
                .route("/api/v1/config/status", web::get().to(get_config_status))
                // Modbus routes
                .route("/api/v1/modbus", web::get().to(get_modbus_config))
                .route("/api/v1/modbus", web::post().to(add_modbus_hub))
                .route("/api/v1/modbus/{name}", web::put().to(update_modbus_hub))
                .route("/api/v1/modbus/{name}", web::delete().to(delete_modbus_hub))
                // KNX routes
                .route("/api/v1/knx", web::get().to(get_knx_config))
                .route("/api/v1/knx", web::post().to(add_knx_adapter))
                .route("/api/v1/knx/{name}", web::put().to(update_knx_adapter))
                .route("/api/v1/knx/{name}", web::delete().to(delete_knx_adapter))
                .route("/api/v1/knx/{adapter_name}/meters", web::post().to(add_knx_meter))
                .route("/api/v1/knx/{adapter_name}/meters/{meter_name}", web::delete().to(delete_knx_meter))
                .route("/api/v1/knx/{adapter_name}/switches", web::post().to(add_knx_switch))
                .route("/api/v1/knx/{adapter_name}/switches/{switch_name}", web::delete().to(delete_knx_switch))
                // Zenner Datahub routes
                .route("/api/v1/zenner", web::get().to(get_zenner_config))
                .route("/api/v1/zenner", web::post().to(add_zenner_instance))
                .route("/api/v1/zenner/{name}", web::put().to(update_zenner_instance))
                .route("/api/v1/zenner/{name}", web::delete().to(delete_zenner_instance))
                // OMS routes
                .route("/api/v1/oms", web::get().to(get_oms_config))
                .route("/api/v1/oms", web::post().to(add_oms_meter))
                .route("/api/v1/oms/{name}", web::put().to(update_oms_meter))
                .route("/api/v1/oms/{name}", web::delete().to(delete_oms_meter))
                // Victron routes
                .route("/api/v1/victron", web::get().to(get_victron_config))
                .route("/api/v1/victron", web::post().to(add_victron_instance))
                .route("/api/v1/victron/{name}", web::put().to(update_victron_instance))
                .route("/api/v1/victron/{name}", web::delete().to(delete_victron_instance))
                // WebSocket and HA integration
                .route("/api/v1/ws/configChanges", web::get().to(ws_config_changes))
                .route("/api/v1/ws/live", web::get().to(ws_live_events))
                .route("/api/v1/ha/restart", web::post().to(ha_restart_service))
                .route("/api/v1/ha/config/save", web::post().to(ha_save_config))
                .route("/api/v1/ha/config/reload", web::post().to(ha_reload_config))
                // MQTT migration
                .route("/api/v1/mqtt/cleanup", web::post().to(force_mqtt_cleanup))
                // Discovered devices
                .route("/api/v1/discovered", web::get().to(get_discovered_summary))
                .route("/api/v1/discovered/{protocol}", web::get().to(get_discovered_protocol))
                .route("/api/v1/discovered/{protocol}/{instance}", web::get().to(get_discovered_instance))
                .route("/api/v1/discovered/{protocol}/{instance}/{device_id}", web::get().to(get_discovered_device))
                .route("/api/v1/discovered/{protocol}/{instance}/{device_id}", web::patch().to(update_discovered_device))
                .route("/api/v1/discovered/{protocol}/{instance}/{device_id}", web::delete().to(delete_discovered_device))
                // Prometheus
                .route("/prometheus/metrics", web::get().to(e2m_prometheus_generic))
                .route("/prometheus/metering", web::get().to(e2m_prometheus_metering))
                // UI - serve at root and /ui
                .service(actix_files::Files::new("/ui", "ui").show_files_listing().index_file("index.html").use_last_modified(true))
                .service(actix_files::Files::new("/", "ui").index_file("index.html").use_last_modified(true))
                .service(
                    SwaggerUi::new("/swagger-ui/{_:.*}")
                        .url("/api/v1/openapi.json", ApiDoc::openapi()),
                )
        })
        .bind(format!("0.0.0.0:{}", config.port)).unwrap()
        .run()
        .await;

    }
}