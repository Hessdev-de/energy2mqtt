/*
    This code builds the detect engine for Victron devices in energy2mqtt

    The support will be extended further as we got new victron installations to test against    
*/

use std::sync::Arc;
use log::{error, info};
use rumqttc::AsyncClient;
use serde_json::Value;
use tokio::sync::{mpsc::Sender, Mutex};
use crate::{metering_victron::{utils::{self, read_topic_u64, set_topic}, Topic}, mqtt::{Transmission, ha_interface::{HaComponent, HaDiscover}}};
use super::VictronData;

pub async fn run_initial_detection(client: &AsyncClient, data: &Arc<Mutex<VictronData>>, sender: &Sender<Transmission>, log_prefix: String) -> bool {

    let devname = data.lock().await.conf.name.clone();
    let portal_id = utils::get_portal(&data).await;

    info!("Starting detection for victon id {}", portal_id);

    let _vdisc = HaDiscover::new_with_topic_from_name(format!("{devname}_mqtt_{portal_id}"),
                                                "Victron".to_string(),
                                                "MQTT Bridge".to_string(),
                                                "Victron".to_string(),
                                                devname.clone());
    set_topic(client, data,
            &format!("N/{portal_id}/system/0/Serial"), 
            Some(Topic::new_with_key(portal_id.clone(), "portal_id".to_string()))).await;

    /* Detect the meters and export them to the json */
    let meter_c= read_topic_u64(client, data, 
                        &format!("N/{portal_id}/system/0/Ac/In/NumberOfAcInputs"), 
                        "ac_meter_count".to_string()).await;

    if meter_c.is_some() {
        let meter_count = meter_c.unwrap();
        info!("{log_prefix} System has {meter_count} AC meters");

        let mut grid_meter_found = false;

        for i in 0..meter_count {
            let base_topic = format!("N/{portal_id}/system/0/Ac/In/{i}");

            let service = utils::read_topic_string(client, data, 
                        &format!("{base_topic}/ServiceType"), 
                        format!("meter_{i}_service")).await.unwrap_or("".to_string());
            
            if service == "" {
                info!("{log_prefix} ServiceType is unknown");
                continue;
            }

            if service == "grid" {
                grid_meter_found = true;
            }

            let connected= read_topic_u64(client, data, 
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
                info!("{log_prefix} Device Instance for meter is invalid");
                continue;
            }

            // We need to read everything below the device_instnce regulary, so add that to the readout topics
            let meter_base = format!("N/{portal_id}/{service}/{device_instance}");
            data.lock().await.add_read_topic(format!("{meter_base}/"));

            // Read product serial
            let mut serial =  utils::read_topic_string(client, data, 
                        &format!("{meter_base}/Serial"), 
                        format!("meter_{i}_serial")).await.unwrap_or(format!("Unknown Serial {i}"));
            
            serial = serial.to_lowercase().replace(" ", "_");
            
            // Read product name
            let productname =  utils::read_topic_string(client, data, 
                        &format!("{meter_base}/ProductName"), 
                        format!("meter_{serial}_productname")).await.unwrap_or("unknown".to_string());
            // Hardware revision
            let _ =  utils::read_topic_string(client, data, 
                        &format!("{meter_base}/HardwareVersion"), 
                        format!("meter_{serial}_hardware_version")).await;

            let nr_phases = read_topic_u64(client, data, 
                        &format!("{meter_base}/NrOfPhases"), 
                        format!("meter_{serial}_nr_phases")).await.unwrap_or(0);

            let mut disc = HaDiscover::new_with_topic_from_name(format!("{devname}_meter{i}"),
                                                "Victron".to_string(),
                                                productname,
                                                "Victron".to_string(),
                                                devname.clone());
            
            info!("{log_prefix} Meter {serial} ({i}) has {nr_phases} phases");

            /* Energy from grid */
            let mut device_name = "energy_positive".to_string();
            let mut json_key = format!("meter_{serial}_energy_positive");

            let _ = read_topic_u64(client, data, 
                        &format!("{meter_base}/Ac/Energy/Forward"), 
                        json_key.clone()).await.unwrap_or(0);

            let c = HaComponent::new_full_sensor("Total Energy positive".to_string(), 
                                                                "energy".to_string(),
                                                                "kWh".to_string(),
                                                                json_key.clone(), 
                                                                device_name.clone(),
                                                                format!("e2m_victron_{devname}_{json_key}"));

            disc.cmps.insert(device_name, serde_json::to_value(c).unwrap());

            /* Energy to grid */
            device_name = "energy_negative".to_string();
            json_key = format!("meter_{serial}_energy_negative");

            let _ = read_topic_u64(client, data, 
                        &format!("{meter_base}/Ac/Energy/Reverse"), 
                        json_key.clone()).await.unwrap_or(0);

            let c = HaComponent::new_full_sensor("Total Energy negative".to_string(), 
                                                                "energy".to_string(),
                                                                "kWh".to_string(),
                                                                json_key.clone(),
                                                                device_name.clone(),
                                                                format!("e2m_victron_{devname}_meter{json_key}"));

            disc.cmps.insert(device_name, serde_json::to_value(c).unwrap());

            /* Frequency of the network */
            device_name = "frequency".to_string();
            json_key = format!("meter_{serial}_frequency");
            let _ = read_topic_u64(client, data, 
                        &format!("{meter_base}/Ac/Frequency"), 
                        json_key.clone()).await.unwrap_or(0);
            
            let c = HaComponent::new_full_sensor("Grid Frequency".to_string(), 
                                                                "frequency".to_string(),
                                                                "Hz".to_string(),
                                                                json_key.clone(), 
                                                                device_name.clone(),
                                                                format!("e2m_victron_{devname}_{json_key}"));

            disc.cmps.insert(device_name, serde_json::to_value(c).unwrap());

            /* Global power over all phases */
            device_name = "power".to_string();
            json_key = format!("meter_{serial}_power");
            let _ = read_topic_u64(client, data, 
                        &format!("{meter_base}/Ac/Power"), 
                        json_key.clone()).await.unwrap_or(0);
            
            let c = HaComponent::new_full_sensor("Grid Power".to_string(), 
                                                                "power".to_string(),
                                                                "W".to_string(),
                                                                json_key.clone(), 
                                                                device_name.clone(),
                                                                format!("e2m_victron_{devname}_{json_key}"));

            disc.cmps.insert(device_name, serde_json::to_value(c).unwrap());

            for p in 1..=nr_phases {
                /* Get the data of each phase */

                /* Energy from grid to home */
                json_key = format!("meter_{serial}_p{p}_energy_positive");
                device_name = format!("energy_positive_p{p}");
                let _ = read_topic_u64(client, data, 
                            &format!("{meter_base}/Ac/L{p}/Energy/Forward"),
                            json_key.clone()).await.unwrap_or(0);

                let c = HaComponent::new_full_sensor(format!("P{p} Energy positive"), 
                                                                "energy".to_string(),
                                                                "kWh".to_string(),
                                                                json_key.clone(),
                                                                device_name.clone(),
                                                                format!("e2m_victron_{devname}_{json_key}")); 

                disc.cmps.insert(device_name, serde_json::to_value(c).unwrap());
            
                /* Energy from home to grid */
                json_key = format!("meter_{serial}_p{p}_energy_negative");
                device_name = format!("energy_negative_p{p}");

                let _ = read_topic_u64(client, data, 
                            &format!("{meter_base}/Ac/L{p}/Energy/Reverse"), 
                            json_key.clone()).await.unwrap_or(0);
                
                let c = HaComponent::new_full_sensor(format!("P{p} Energy negative"), 
                                                                "energy".to_string(),
                                                                "kWh".to_string(),
                                                                json_key.clone(), 
                                                                device_name.clone(),
                                                                format!("e2m_victron_{devname}_{json_key}")); 
 
                disc.cmps.insert(device_name, serde_json::to_value(c).unwrap());

                /* Voltage of the phase  */
                json_key = format!("meter_{serial}_p{p}_voltage");
                device_name = format!("voltage_p{p}");
                let _ = read_topic_u64(client, data, 
                            &format!("{meter_base}/Ac/L{p}/Voltage"), 
                            json_key.clone()).await.unwrap_or(0);
                
                let c = HaComponent::new_full_sensor(format!("P{p} Voltage"), 
                                                                "voltage".to_string(),
                                                                "V".to_string(),
                                                                json_key.clone(), 
                                                                device_name.clone(),
                                                                format!("e2m_victron_{devname}_{json_key}")); 
                disc.cmps.insert(device_name, serde_json::to_value(c).unwrap());

                /* Current of the phase */
                json_key = format!("meter_{serial}_p{p}_current");
                device_name = format!("current_p{p}");
                let _ = read_topic_u64(client, data, 
                            &format!("{meter_base}/Ac/L{p}/Current"), 
                            json_key.clone()).await.unwrap_or(0);

                let c = HaComponent::new_full_sensor(format!("P{p} Current"), 
                                                                "current".to_string(),
                                                                "A".to_string(),
                                                                json_key.clone(), 
                                                                device_name.clone(),
                                                                format!("e2m_victron_{devname}_{json_key}"));
                disc.cmps.insert(device_name, serde_json::to_value(c).unwrap());

                /* Power of the phase */
                json_key = format!("meter_{serial}_p{p}_power");
                device_name = format!("power_p{p}");
                let _ = read_topic_u64(client, data, 
                            &format!("{meter_base}/Ac/L{p}/Power"), 
                            json_key.clone()).await.unwrap_or(0);

                let c = HaComponent::new_full_sensor(format!("P{p} Power"), 
                                                                "power".to_string(),
                                                                "W".to_string(),
                                                                json_key.clone(), 
                                                                device_name.clone(),
                                                                format!("e2m_victron_{devname}_{json_key}"));
                disc.cmps.insert(device_name, serde_json::to_value(c).unwrap());
            }

            let _ = sender.send(Transmission::AutoDiscovery(disc)).await;
        }
        
        if grid_meter_found == false {
            error!("We have not found a grid meter, looks like the init phase failed");
        }
    } else {
        info!("No Data found for our request after one second, meter count is empty");
    }

    /* Scan for Batteries */
    let d = utils::read_topic_value(client, data, 
                                &format!("N/{portal_id}/system/0/Batteries"), 
                                "_system_batteries".to_string())
                                .await.unwrap_or(Value::Null);

    let batterie_array = d.as_array().unwrap();

    for b in 0..batterie_array.len() {
        let battery = batterie_array.get(b).unwrap();
        if !battery.is_object() {
            info!("Battery id {b} is not an object");
            continue;
        }

        let map = serde_json::Map::new();
        let battery_obj = battery.as_object().unwrap_or(&map);

        let instance: u64 = match battery_obj.get("instance") {
            Some(d) => d.as_u64().unwrap_or(0),
            None => 0u64,
        };

        /* Getting battery information */
        let base_topic = format!("N/{portal_id}/battery/{instance}");

        /* Make sure to reread the battery stuff */
        data.lock().await.add_read_topic(format!("{base_topic}/"));

        let productname =  utils::read_topic_string(client, data, 
                        &format!("{base_topic}/ProductName"),
                        format!("battery_{b}_productname")).await.unwrap_or("unknown".to_string());

        let manufacturer =  utils::read_topic_string(client, data, 
                        &format!("{base_topic}/Manufacturer"),
                        format!("battery_{b}_manufacturer")).await.unwrap_or("unknown".to_string());

        let mut disc = HaDiscover::new_with_topic_from_name(format!("{devname}_battery{b}"),
                                            manufacturer.clone(),
                                            productname,
                                            "Victron".to_string(),
                                            devname.clone());

        /* State of charge */
        let _ = read_topic_u64(client, data,
                        &format!("{base_topic}/Soc"), 
                        format!("battery_{b}_soc")).await.unwrap_or(0);

        let c = HaComponent::new_percent("soc".to_string(),
                                                        "battery".to_string(), 
                                                        "victron".to_string(),
                                                        "State of Charge".to_string(),
                                                        format!("battery_{b}_soc"));
        disc.cmps.insert("soc".to_string(), serde_json::to_value(c).unwrap());

        /* The state of health */
        let _ = read_topic_u64(client, data,
                        &format!("{base_topic}/Soh"), 
                        format!("battery_{b}_soh")).await.unwrap_or(0);

        let c = HaComponent::new_percent("soh".to_string(),
                                                        "battery".to_string(), 
                                                        "victron".to_string(),
                                                        "State of Health".to_string(),
                                                        format!("battery_{b}_soh"));
        disc.cmps.insert("soh".to_string(), serde_json::to_value(c).unwrap());

        /* Voltage of the DC system */
        let mut json_key = format!("battery_{b}_voltage");
        let mut device_name = format!("voltage_battery_{b}");

        let _ = read_topic_u64(client, data, 
                        &format!("{base_topic}/Dc/0/Voltage"),
                        json_key.clone()).await.unwrap_or(0);

        let c = HaComponent::new_full_sensor(format!("Voltage"), 
                                                            "voltage".to_string(),
                                                            "V".to_string(),
                                                            json_key.clone(), 
                                                            device_name.clone(),
                                                            format!("e2m_victron_{devname}_{json_key}"));
                
        disc.cmps.insert(device_name, serde_json::to_value(c).unwrap());

        /* Power of the DC system */
        json_key = format!("battery_{b}_power");
        device_name = format!("power_battery_{b}");

        let _ = read_topic_u64(client, data, 
                        &format!("{base_topic}/Dc/0/Power"),
                        json_key.clone()).await.unwrap_or(0);

        let c = HaComponent::new_full_sensor(format!("Power"), 
                                                            "power".to_string(),
                                                            "W".to_string(),
                                                            json_key.clone(), 
                                                            device_name.clone(),
                                                            format!("e2m_victron_{devname}_{json_key}"));
                
        disc.cmps.insert(device_name, serde_json::to_value(c).unwrap()); 


        /* Power of the DC system */
        json_key = format!("battery_{b}_current");
        device_name = format!("current_battery_{b}");

        let _ = read_topic_u64(client, data, 
                        &format!("{base_topic}/Dc/0/Current"),
                        json_key.clone()).await.unwrap_or(0);

        let c = HaComponent::new_full_sensor(format!("Current"), 
                                                            "current".to_string(),
                                                            "A".to_string(),
                                                            json_key.clone(), 
                                                            device_name.clone(),
                                                            format!("e2m_victron_{devname}_{json_key}"));
        
        disc.cmps.insert(device_name, serde_json::to_value(c).unwrap()); 


        /* Temperature of the DC system */
        json_key = format!("battery_{b}_temp");
        device_name = format!("temperature_battery_{b}");

        let _ = read_topic_u64(client, data, 
                        &format!("{base_topic}/Dc/0/Temperature"),
                        json_key.clone()).await.unwrap_or(0);

        let c = HaComponent::new_full_sensor(format!("Temperature"), 
                                                            "temperature".to_string(),
                                                            "°C".to_string(),
                                                            json_key.clone(), 
                                                            device_name.clone(),
                                                            format!("e2m_victron_{devname}_{json_key}"));
        
        disc.cmps.insert(device_name, serde_json::to_value(c).unwrap());

        
        if manufacturer == "PYLON" {
            /* Pylontech batteries include some nice information we want to have */
            /* lowest temperature of the batteries */
            json_key = format!("battery_{b}_min_temp_cell");
            device_name = format!("temperature_battery_{b}_cell_min");

            let _ = read_topic_u64(client, data, 
                            &format!("{base_topic}/System/MinCellTemperature"),
                            json_key.clone()).await.unwrap_or(0);

            let c = HaComponent::new_full_sensor(format!("Minimal Cell Temperature"), 
                                                                "temperature".to_string(),
                                                                "°C".to_string(),
                                                                json_key.clone(), 
                                                                device_name.clone(),
                                                                format!("e2m_victron_{devname}_{json_key}"));
            
            disc.cmps.insert(device_name, serde_json::to_value(c).unwrap());


            /* highest temperature of batteries */
            json_key = format!("battery_{b}_max_temp_cell");
            device_name = format!("temperature_battery_{b}_cell_max");

            let _ = read_topic_u64(client, data, 
                            &format!("{base_topic}/System/MaxCellTemperature"),
                            json_key.clone()).await.unwrap_or(0);

            let c = HaComponent::new_full_sensor(format!("Maximal Cell Temperature"), 
                                                                "temperature".to_string(),
                                                                "°C".to_string(),
                                                                json_key.clone(), 
                                                                device_name.clone(),
                                                                format!("e2m_victron_{devname}_{json_key}"));
            
            disc.cmps.insert(device_name, serde_json::to_value(c).unwrap());


            /* The voltage is also interessting */

            /* lowest voltage of cells in the batteries */
            json_key = format!("battery_{b}_min_voltage_cell");
            device_name = format!("voltage_battery_{b}_cell_min");

            let _ = read_topic_u64(client, data, 
                            &format!("{base_topic}/System/MinCellVoltage"),
                            json_key.clone()).await.unwrap_or(0);

            let c = HaComponent::new_full_sensor(format!("Minimal Cell Voltage"), 
                                                                "voltage".to_string(),
                                                                "V".to_string(),
                                                                json_key.clone(), 
                                                                device_name.clone(),
                                                                format!("e2m_victron_{devname}_{json_key}"));
            
            disc.cmps.insert(device_name, serde_json::to_value(c).unwrap());


            /* highest voltage of cells in the batteries */
            json_key = format!("battery_{b}_max_voltage_cell");
            device_name = format!("voltage_battery_{b}_cell_max");

            let _ = read_topic_u64(client, data, 
                            &format!("{base_topic}/System/MaxCellVoltage"),
                            json_key.clone()).await.unwrap_or(0);

            let c = HaComponent::new_full_sensor(format!("Maximal Cell Cell Voltage"), 
                                                                "voltage".to_string(),
                                                                "V".to_string(),
                                                                json_key.clone(), 
                                                                device_name.clone(),
                                                                format!("e2m_victron_{devname}_{json_key}"));
            
            disc.cmps.insert(device_name, serde_json::to_value(c).unwrap());

        }

        let _ = sender.send(Transmission::AutoDiscovery(disc)).await;
    }

    /* We now have the basis data, now get the kWh out of the system itself */
    let topic = format!("N/{portal_id}/system/0/VebusInstance");
    let device_instance  =utils::read_topic_u64(client, data, 
                                &topic,
                                "_vebus_instance".to_string())
                                .await.unwrap_or(0);
    if d != 0 {
        /* VEBus is our main entry now  */
        let service = "vebus";
        let base_topic = format!("N/{portal_id}/{service}/{device_instance}");

        data.lock().await.add_read_topic(format!("{base_topic}/"));

        let productname =  utils::read_topic_string(client, data, 
                        &format!("{base_topic}/ProductName"), 
                        format!("vebus_{device_instance}_productname"))
                        .await.unwrap_or("unknown".to_string());

        let mut disc = HaDiscover::new_with_topic_from_name(format!("{devname}_vebus_{device_instance}"),
                                            "Victron Energy".to_string(),
                                            productname,
                                            "Victron".to_string(),
                                            devname.clone());
        
        let number_of_multis = utils::read_topic_u64(client, data, 
                                &format!("{base_topic}/Devices/NumberOfMultis"), 
                                format!("vebus_{device_instance}_nr_of_multis"))
                                .await.unwrap_or(0);

        /*let c = HaComponent::new_full_sensor(format!("Number of MultiPlus Devices"), 
                                                                "energy".to_string(),
                                                                "kWh".to_string(),
                                                                json_key.clone(),
                                                                device_name.clone(),
                                                                format!("e2m_victron_{devname}_{json_key}")); 

        disc.cmps.insert(device_name, serde_json::to_value(c).unwrap());*/
        
        for _i in 0..=number_of_multis {
            /* TODO: Get the phase information from the multis  */
        }


        /* Energy generated by inverter and pushed to AC-IN 1 */
        let mut json_key = format!("vebus{device_instance}_energy_inv_acin1");
        let mut device_name = format!("energy_vebus{device_instance}_inv_acin1");

        let _ = utils::read_topic_u64(client, data, 
                                &format!("{base_topic}/Energy/InverterToAcIn1"), 
                                json_key.clone())
                                .await.unwrap_or(0);
        
        let c = HaComponent::new_full_sensor(format!("Inverter to AC-IN 1"), 
                                                                "energy".to_string(),
                                                                "kWh".to_string(),
                                                                json_key.clone(),
                                                                device_name.clone(),
                                                                format!("e2m_victron_{devname}_{json_key}")); 
        disc.cmps.insert(device_name, serde_json::to_value(c).unwrap());

        /* Energy generated by inverter and pushed to AC-IN 2 */
        json_key = format!("vebus{device_instance}_energy_inv_acin2");
        device_name = format!("energy_vebus{device_instance}_inv_acin2");

        let _ = utils::read_topic_u64(client, data, 
                                &format!("{base_topic}/Energy/InverterToAcIn2"), 
                                json_key.clone())
                                .await.unwrap_or(0);
        
        let c = HaComponent::new_full_sensor(format!("Inverter to AC-IN 2"), 
                                                                "energy".to_string(),
                                                                "kWh".to_string(),
                                                                json_key.clone(),
                                                                device_name.clone(),
                                                                format!("e2m_victron_{devname}_{json_key}")); 
        disc.cmps.insert(device_name, serde_json::to_value(c).unwrap());

        /* Energy flowing from AC Out into the inverter */
        json_key = format!("vebus{device_instance}_energy_out_inv");
        device_name = format!("energy_vebus{device_instance}_out_inv");

        let _ = utils::read_topic_u64(client, data, 
                                &format!("{base_topic}/Energy/OutToInverter"), 
                                json_key.clone())
                                .await.unwrap_or(0);
        
        let c = HaComponent::new_full_sensor(format!("AC-Out to Inverter"), 
                                                                "energy".to_string(),
                                                                "kWh".to_string(),
                                                                json_key.clone(),
                                                                device_name.clone(),
                                                                format!("e2m_victron_{devname}_{json_key}")); 
        disc.cmps.insert(device_name, serde_json::to_value(c).unwrap());

        /* Energy flowing from AC Out into the inverter */
        json_key = format!("vebus{device_instance}_energy_inv_out");
        device_name = format!("energy_vebus{device_instance}_inv_out");

        let _ = utils::read_topic_u64(client, data, 
                                &format!("{base_topic}/Energy/InverterToAcOut"), 
                                json_key.clone())
                                .await.unwrap_or(0);
        
        let c = HaComponent::new_full_sensor(format!("Inverter to AC-Out"), 
                                                                "energy".to_string(),
                                                                "kWh".to_string(),
                                                                json_key.clone(),
                                                                device_name.clone(),
                                                                format!("e2m_victron_{devname}_{json_key}")); 
        disc.cmps.insert(device_name, serde_json::to_value(c).unwrap());


        /* Energy flowing from AC In 1 into the inverter */
        json_key = format!("vebus{device_instance}_energy_acin1_inv");
        device_name = format!("energy_vebus{device_instance}_acin1_inv");

        let _ = utils::read_topic_u64(client, data, 
                                &format!("{base_topic}/Energy/AcIn1ToInverter"), 
                                json_key.clone())
                                .await.unwrap_or(0);
        
        let c = HaComponent::new_full_sensor(format!("AC-IN1 to Inverter"), 
                                                                "energy".to_string(),
                                                                "kWh".to_string(),
                                                                json_key.clone(),
                                                                device_name.clone(),
                                                                format!("e2m_victron_{devname}_{json_key}")); 
        disc.cmps.insert(device_name, serde_json::to_value(c).unwrap());

        /* Energy flowing from AC In 2 into the inverter */
        json_key = format!("vebus{device_instance}_energy_acin2_inv");
        device_name = format!("energy_vebus{device_instance}_acin2_inv");

        let _ = utils::read_topic_u64(client, data, 
                                &format!("{base_topic}/Energy/AcIn2ToInverter"), 
                                json_key.clone())
                                .await.unwrap_or(0);
        
        let c = HaComponent::new_full_sensor(format!("AC-IN2 to Inverter"), 
                                                                "energy".to_string(),
                                                                "kWh".to_string(),
                                                                json_key.clone(),
                                                                device_name.clone(),
                                                                format!("e2m_victron_{devname}_{json_key}")); 
        disc.cmps.insert(device_name, serde_json::to_value(c).unwrap());

        /* Energy flowing from AC Out into to AC-In1 */
        json_key = format!("vebus{device_instance}_energy_acout_acin1");
        device_name = format!("energy_vebus{device_instance}_acout_acin1");

        let _ = utils::read_topic_u64(client, data, 
                                &format!("{base_topic}/Energy/AcOutToAcIn1"), 
                                json_key.clone())
                                .await.unwrap_or(0);
        
        let c = HaComponent::new_full_sensor(format!("AC-Out to AC-IN1"), 
                                                                "energy".to_string(),
                                                                "kWh".to_string(),
                                                                json_key.clone(),
                                                                device_name.clone(),
                                                                format!("e2m_victron_{devname}_{json_key}")); 
        disc.cmps.insert(device_name, serde_json::to_value(c).unwrap());

        /* Energy flowing from AC Out into to AC-In2 */
        json_key = format!("vebus{device_instance}_energy_acout_acin2");
        device_name = format!("energy_vebus{device_instance}_acout_acin2");

        let _ = utils::read_topic_u64(client, data, 
                                &format!("{base_topic}/Energy/AcOutToAcIn2"), 
                                json_key.clone())
                                .await.unwrap_or(0);
        
        let c = HaComponent::new_full_sensor(format!("AC-Out to AC-IN2"), 
                                                                "energy".to_string(),
                                                                "kWh".to_string(),
                                                                json_key.clone(),
                                                                device_name.clone(),
                                                                format!("e2m_victron_{devname}_{json_key}")); 
        disc.cmps.insert(device_name, serde_json::to_value(c).unwrap());

        /* Energy flowing from AC-In1 into to AC-Out */
        json_key = format!("vebus{device_instance}_energy_acin1_acout");
        device_name = format!("energy_vebus{device_instance}_acin1_acout");

        let _ = utils::read_topic_u64(client, data, 
                                &format!("{base_topic}/Energy/AcIn1ToAcOut"), 
                                json_key.clone())
                                .await.unwrap_or(0);
        
        let c = HaComponent::new_full_sensor(format!("AC-IN1 to AC-Out"), 
                                                                "energy".to_string(),
                                                                "kWh".to_string(),
                                                                json_key.clone(),
                                                                device_name.clone(),
                                                                format!("e2m_victron_{devname}_{json_key}")); 
        disc.cmps.insert(device_name, serde_json::to_value(c).unwrap());

        /* Energy flowing from AC-In2 into to AC-Out */
        json_key = format!("vebus{device_instance}_energy_acin2_acout");
        device_name = format!("energy_vebus{device_instance}_acin2_acout");

        let _ = utils::read_topic_u64(client, data, 
                                &format!("{base_topic}/Energy/AcIn2ToAcOut"), 
                                json_key.clone())
                                .await.unwrap_or(0);
        
        let c = HaComponent::new_full_sensor(format!("AC-IN2 to AC-Out"), 
                                                                "energy".to_string(),
                                                                "kWh".to_string(),
                                                                json_key.clone(),
                                                                device_name.clone(),
                                                                format!("e2m_victron_{devname}_{json_key}")); 
        disc.cmps.insert(device_name, serde_json::to_value(c).unwrap());


        /* Those should be in the battery:
            N/c0619ab38650/vebus/276/BatteryOperationalLimits/MaxDischargeCurrent
            N/c0619ab38650/vebus/276/BatteryOperationalLimits/MaxChargeVoltage
            N/c0619ab38650/vebus/276/BatteryOperationalLimits/BatteryLowVoltage
            N/c0619ab38650/vebus/276/BatteryOperationalLimits/MaxChargeCurrent
        */
        /* Extras:
            N/c0619ab38650/vebus/276/Mode

        */

        let _ = sender.send(Transmission::AutoDiscovery(disc)).await;
    }

            /* VERSATILES  */
    return true;
}
