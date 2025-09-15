
use actix_files;
use actix_web::{web, App, HttpRequest, HttpResponse, HttpServer, Responder};
use log::{error, info};
use utoipa::OpenApi;
use utoipa_swagger_ui::SwaggerUi;
use serde::Serialize;
use std::time::{SystemTime, UNIX_EPOCH};
use utoipa::ToSchema;

use crate::{config::{ConfigBases, ModbusHubConfig}, get_config_or_panic, CONFIG};
use crate::mqtt::{get_app_status, MqttConnectionStatus};


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
async fn health_check() -> impl Responder {
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
    
    let overall_healthy = matches!(mqtt_health.status, MqttConnectionStatus::Connected) &&
        last_message_sent_ago.unwrap_or(3600) < 300; // Consider healthy if last message sent within 5 minutes
    
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
    
    if overall_healthy {
        HttpResponse::Ok().json(response)
    } else {
        HttpResponse::ServiceUnavailable().json(response)
    }
}

#[utoipa::path(get,
    path = "/api/v1/config",
    summary = "Get the whole configuration as stored in the memory of the application",
    responses(
        (status = 200, description = "Get current running config")
    ),
)]
async fn get_config() -> impl Responder {
    let config = CONFIG.read().unwrap().get_complete_config();
    HttpResponse::Ok().content_type("application/json").json(config)
}

#[utoipa::path(post,
    path = "/api/v1/ha/restart",
    summary = "Restart the service (for Home Assistant integration)",
    responses(
        (status = 200, description = "Restart command received"),
        (status = 500, description = "Failed to restart")
    ),
)]
async fn ha_restart_service() -> impl Responder {
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
async fn ha_save_config() -> impl Responder {
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
async fn ha_reload_config() -> impl Responder {
    info!("Home Assistant requested config reload");
    // This would need to be implemented in the CONFIG structure
    HttpResponse::Ok().json(serde_json::json!({
        "status": "success",
        "message": "Configuration reload requested. Implementation depends on config management system."
    }))
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
async fn get_modbus_config() -> impl Responder {
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
async fn add_modbus_hub(
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
async fn delete_modbus_hub(
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


// Websocket to push config changes to the client log file
#[utoipa::path(get,
    path = "/api/v1/ws/configChanges",
    summary = "Websocket to get live config changes",
    responses(
        (status = 101, description = "The websocket is active and can be used to get configuration updates"),
    ),
)]
async fn ws_config_changes(req: HttpRequest, body: web::Payload) -> actix_web::Result<impl Responder> {
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

#[utoipa::path(get,
    path = "/prometheus/metrics",
    summary = "Get all information in prometheus format",
    responses(
        (status = 200, description = "Retuns the current application status as prometheus")
    ),
)]
async fn e2m_prometheus_generic() -> impl Responder {
    HttpResponse::Ok().content_type("text/plain").body("")
}

#[utoipa::path(get,
    path = "/prometheus/metering",
    summary = "Get all metering data in prometheus format",
    responses(
        (status = 200, description = "Retuns the current meter data as prometheus")
    ),
)]
async fn e2m_prometheus_metering() -> impl Responder {
    HttpResponse::Ok().content_type("text/plain").body("")
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
                    get_config,
                    ws_config_changes,
                    get_modbus_config,
                    add_modbus_hub,
                    delete_modbus_hub,
                    ha_restart_service,
                    ha_save_config,
                    ha_reload_config,
                    
            )
        )]
        struct ApiDoc;

        let _ = HttpServer::new(move || {
            App::new()
                //.app_data(web::Data::new(app_state.clone()))
                // Register routes
                .route("/health", web::get().to(health_check))
                .route("/api/v1/config", web::get().to(get_config))
                .route("/api/v1/modbus", web::get().to(get_modbus_config))
                .route("/api/v1/modbus", web::post().to(add_modbus_hub))
                .route("/api/v1/modbus/{name}", web::delete().to(delete_modbus_hub))
                .route("/api/v1/ws/configChanges", web::get().to(ws_config_changes))
                .route("/api/v1/ha/restart", web::post().to(ha_restart_service))
                .route("/api/v1/ha/config/save", web::post().to(ha_save_config))
                .route("/api/v1/ha/config/reload", web::post().to(ha_reload_config))
                .route("/prometheus/metrics", web::post().to(e2m_prometheus_generic))
                .route("/prometheus/metering", web::post().to(e2m_prometheus_metering))
                //.route("/config/modbus/hubs/{name}", web::put().to(update_modbus_hub))
                //.route("/config/modbus/devices", web::post().to(add_modbus_device))
                //.route("/config/modbus/hubs/{hub_name}/devices/{device_name}", web::delete().to(delete_modbus_device))
                //.route("/config/modbus/hubs/{hub_name}/devices/{device_name}", web::put().to(update_modbus_device))
                //.route("/config/tibber", web::get().to(get_tibber_configs))
                //.route("/config/tibber", web::post().to(add_tibber_config))
                //.route("/config/tibber/{name}", web::delete().to(delete_tibber_config))
                //.route("/config/tibber/{name}", web::put().to(update_tibber_config))
                .service(actix_files::Files::new("/ui","ui").show_files_listing().index_file("index.html").use_last_modified(true))
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