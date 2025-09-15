use energy2mqtt::{mqtt::{internal_commands::CommandHandler, publish_uptime, MqttManager}, ApiManager, DeviceManager, Iec62056Manager, ModbusManger, OmsManager, SmlManager, VictronManager, CONFIG};
use tokio::task::JoinHandle;
use std::{env, time::Duration};
use log::info;


#[actix_web::main]
async fn main() -> std::io::Result<()> {
    // Initialize logging
    let default_filter =  std::env::var("E2M_LOG_LEVEL").unwrap_or("info".to_string());
    env_logger::init_from_env(env_logger::Env::new().default_filter_or(default_filter));
    
    env::set_var("RUST_BACKTRACE", "1");

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

    /* Run our api gateway now */
    let api = ApiManager::new();
    threads.push(tokio::spawn(async move {
        let _ = api.start_thread().await;
    }));

    /* Make sure to handle the dirty flag of the configuration */
    threads.push(tokio::spawn(async move {
        loop {
            let _ = tokio::time::sleep(Duration::from_secs(60)).await;
            let mut c = CONFIG.write().unwrap();
            let dirty = c.is_dirty();
            if dirty {
                c.save();
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
