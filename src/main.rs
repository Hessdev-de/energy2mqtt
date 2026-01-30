use energy2mqtt::{ApiManager, CONFIG, DeviceManager, Iec62056Manager, KnxManager, ModbusManger, OmsManager, SmlManager, VictronManager, ZennerDatahubManager, init_discovered_devices, get_discovered_devices, mqtt::{MqttManager, internal_commands::CommandHandler, publish_uptime}};
use tokio::task::JoinHandle;
use std::{env, path::PathBuf, time::Duration};
use log::info;


#[actix_web::main]
async fn main() -> std::io::Result<()> {
    // Initialize logging
    let default_filter =  std::env::var("E2M_LOG_LEVEL").unwrap_or("info".to_string());
    env_logger::init_from_env(env_logger::Env::new().default_filter_or(default_filter));

    env::set_var("RUST_BACKTRACE", "1");

    // Initialize discovered devices store
    let discovered_devices_path = {
        let config = CONFIG.read().unwrap();
        let base_path = &config.base_path;
        PathBuf::from(base_path).join(&config.config.storage.discovered_devices_path)
    };
    init_discovered_devices(discovered_devices_path);
    info!("Discovered devices store initialized");

    // we need a channel for the subparts to send metering data to the handler
    let (mut mqtt, tx) = MqttManager::new().unwrap();
    
    // Initialize device manager
    let device_manager = DeviceManager::new(tx);
    
    let mut threads: Vec<JoinHandle<()>> = Vec::new();

    let bsender = device_manager.get_broadcast_sender();
    threads.push(tokio::spawn(async move {
        mqtt.start_thread(bsender).await;
    }));

    // Start Modbus if needed
    let mr_sender = device_manager.get_sender_instance();
    let mut modbus = ModbusManger::new(mr_sender);
    threads.push(tokio::spawn(async move {
            modbus.start_thread().await;
    }));

    // Start OMS manager
    let mr_sender = device_manager.get_sender_instance();
    let mut oms = OmsManager::new(mr_sender);
    threads.push(tokio::spawn(async move {
        oms.start_thread().await;
    }));

    // Start IEC 62056-21 manager
    let mr_sender = device_manager.get_sender_instance();
    let mut iec62056 = Iec62056Manager::new(mr_sender);
    threads.push(tokio::spawn(async move {
        iec62056.start_thread().await;
    }));

    // Start SML manager
    let mr_sender = device_manager.get_sender_instance();
    let mut sml = SmlManager::new(mr_sender);
    threads.push(tokio::spawn(async move {
        sml.start_thread().await;
    }));

    // Start Victron managers for each configured instance
    let victron_configs = {
        let config = CONFIG.read().unwrap();
        config.config.victron.clone()
    };
    
    for victron_config in victron_configs {
        if victron_config.enabled {
            let mr_sender = device_manager.get_sender_instance();
            let mut victron = VictronManager::new(mr_sender);
            threads.push(tokio::spawn(async move {
                victron.start_thread().await;
            }));
        }
    }

    // Start manager for ZENNER datahub
    let mr_sender = device_manager.get_sender_instance();
    let mut zridh = ZennerDatahubManager::new(mr_sender);
    threads.push(tokio::spawn(async move {
        zridh.start_thread().await;
    }));

    // Start KNX manager
    let mr_sender = device_manager.get_sender_instance();
    let mut knx = KnxManager::new(mr_sender);
    threads.push(tokio::spawn(async move {
        knx.start_thread().await;
    }));

    /* Run our api gateway now */
    let api = ApiManager::new();
    threads.push(tokio::spawn(async move {
        let _ = api.start_thread().await;
    }));

    /* Make sure to handle the dirty flag of the configuration and discovered devices */
    threads.push(tokio::spawn(async move {
        loop {
            let _ = tokio::time::sleep(Duration::from_secs(60)).await;

            // Save config if dirty
            let mut c = CONFIG.write().unwrap();
            let dirty = c.is_dirty();
            if dirty {
                c.save();
            }
            drop(c);

            // Save discovered devices if dirty
            if let Some(store) = get_discovered_devices() {
                if let Err(e) = store.save_if_dirty() {
                    log::error!("Failed to save discovered devices: {}", e);
                }
            }
        }
    }));

    /* Periodic uptime publishing */
    let uptime_sender = device_manager.get_sender_instance();
    threads.push(tokio::spawn(async move {
        // Publish immediately on startup
        publish_uptime(&uptime_sender).await;
        
        // Then publish every minute
        let mut interval = tokio::time::interval(Duration::from_secs(10));
        interval.tick().await; // Skip first immediate tick since we already published
        
        loop {
            interval.tick().await;
            publish_uptime(&uptime_sender).await;
        }
    }));

    /* Last but not least start our command handling */
    let mr_sender = device_manager.get_sender_instance();
    let command = CommandHandler::new(mr_sender);
    threads.push(tokio::spawn(async move {
        command.start_thread().await;
    }));


    info!("All modules started, now waiting for a signal to exit");
    loop {
        tokio::time::sleep(Duration::from_secs(10)).await;
        let mut kill_all_tasks = false;
        for task in threads.iter() {
            if task.is_finished() {
                kill_all_tasks = true;
            }
        }

        if kill_all_tasks == true {
            for task in threads.iter_mut() {
                task.abort();
            }
            break;
        }
    }
    Ok(())
}
