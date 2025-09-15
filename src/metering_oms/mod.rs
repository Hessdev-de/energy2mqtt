use lazy_static::lazy_static;
use std::sync::Mutex;
use crate::{models::DeviceProtocol, mqtt::{SubscribeData, Transmission}, MeteringData};
use log::{debug, error, info};
use tokio::sync::mpsc::Sender;
use hex;
use thiserror::Error;

pub mod utils;
pub mod structs;
pub mod div_vif_parser;

pub struct OmsManager {
    sender: Sender<Transmission>,
}

lazy_static! {
    static ref waitFor: Mutex<i32> = Mutex::new(0);
}


impl OmsManager {
    pub fn new(sender: Sender<Transmission>) -> Self {
        return OmsManager { 
            sender: sender,
         }
    }

    pub async fn start_thread(&mut self) {
        info!("Starting OMS thread");
        /* We need to subscribe to an MQTT topic and wait for data to fill our buffers */
        let (sender, mut receiver) = tokio::sync::mpsc::channel(10);

        let register = Transmission::Subscribe(SubscribeData{
            topic: "oms_input".to_string(),
            sender
        });

        let _ = self.sender.send(register).await;

        info!("Starting OMS waiting for messages");
        while let Some(c) = receiver.recv().await {
            let dec =  hex::decode(c);
            if dec.is_err() {
                error!("Non hex string received");
                continue;
            }

            let dec = dec.unwrap();
            let dec = parse_oms_telegram(&dec, true);
            match dec {
                Ok(doc) => { let _ = self.sender.send(Transmission::Metering(doc)).await; },
                Err(e) => { error!("OMS telegram can not be parsed: {e:?}"); },
            }
        }
    }
}


/// Custom error types for OMS Parsing
#[derive(Error, Debug)]
pub enum OmsParseError {
    #[error("Telegram too short")]
    TelegramTooShort,
    #[error("Telegram too long")]
    TelegramTooLong,

    #[error("Unsupported OMS message type")]
    UnsupportedTelegramType,
    #[error("CRC mismatch")]
    CRCMissMatch,
    #[error("Wired protocol not suppored")]
    WiredProtocolNotSupported,

    #[error("security mode not suppored")]
    SecurityModeNotSupported,
    #[error("Decryption failed")]
    DecryptionFailed,
    #[error("CI Field not supported")]
    SecurityCiTypeNotSupported,
    #[error("Sensor not configured")]
    SensorNotConfigured,
}

/*macro_rules! bit_set {
    ($source: expr, $bit: expr) => {
        (($source & (1<< $bit)) > 0)
    };
}*/

fn parse_oms_telegram(telegram: &Vec<u8>, with_crc: bool) -> Result<MeteringData, OmsParseError> {
    
    /* Some definitions direction slave to master only */
    let tpl_no_header_ids : Vec<u8> = vec![0x66, 0x70, 0x71];
    /* Annex D D.2 */
    let tpl_short_header_ids : Vec<u8> = vec![0x67, 0x6E, 0x74, 0x7A, 0x7D, 0x7F, 0x88, 0x9E, 0xC1, 0xC4];
    /* Annex D D.2 */
    let tpl_long_header_ids: Vec<u8> = vec![0x68, 0x6F, 0x72, 0x75, 0x7C, 0x7E, 0x9F, 0xC2, 0xC5];

    /* 
        Some OMS hardware removes the CRC block during reception other transport it to the backend.
    */
    let telegram = match with_crc {
        true => utils::verifiy_crc(telegram)?,
        false => telegram.clone(),
    };

    let telegram_len = telegram.len();
    if telegram_len < 10 {
        return Err(OmsParseError::TelegramTooShort);
    }

    if telegram_len > 255 {
        return Err(OmsParseError::TelegramTooLong);
    }

    let len = telegram[0];

    /* Verify the whole telegram as received */
    if len as usize > telegram_len {
        return Err(OmsParseError::TelegramTooShort);
    }

    let mut mr = MeteringData::new().unwrap();
    mr.protocol = DeviceProtocol::OMS;

    let mut protocol_map = serde_json::Map::new();
    protocol_map.insert("type".to_string(), "oms".into());
    protocol_map.insert("crc_verified".to_string(), serde_json::Value::from(with_crc));

    /* C Field */
    if telegram[1] != 0x44 {
        return Err(OmsParseError::UnsupportedTelegramType);
    } else {
        debug!("SND_NR message found");
        protocol_map.insert("c_field".to_string(), serde_json::Value::from("SND_NR"));
    }
    
    let manfucturer = utils::get_manufacturer(&telegram);
    protocol_map.insert("manufacturer".to_string(), manfucturer.clone().into());
    let ident_no = utils::get_ident_no(&telegram);
    protocol_map.insert("device_number".to_string(), ident_no.clone().into());
    let version = format!("{:02x}",telegram[8]);
    protocol_map.insert("version_number".to_string(), version.clone().into());
    let device_type = format!("{:x}",telegram[9]);
    protocol_map.insert("device_medium".to_string(), utils::get_device_medium(&device_type).into());

    /* We follow the naming based on DIN 43863-5:2012 for the meter data */
    let din_addr = format!("{device_type}{manfucturer}{version}{ident_no}");

    /*
        A long header can change the identification of the meter but not the sender,
        we need to store both to handle it correctly
    */
    protocol_map.insert("din_addr_sender".to_string(), din_addr.clone().into());
    protocol_map.insert("din_addr_meter".to_string(), din_addr.clone().into());
    
    let config = match utils::get_meter_config(&din_addr) {
        Some(c) => c,
        None => { return Err(OmsParseError::SensorNotConfigured); },
    };

    debug!("DLL (DataLink layer) correct, trying TPL (TransPort Layer)");

    let ci = telegram[10];

    let access_no: u8;
    let status: u32;
    /* Format based Issue 5.0.1 / 2023-12 (RELEASE) 7.2.4.1 General */
    let config_field : u16;

    if tpl_short_header_ids.contains(&ci) {
        protocol_map.insert("ci_field".to_string(), serde_json::Value::from("short"));
        access_no = telegram[11] as u8;
        status = telegram[12] as u32;
        config_field = (telegram[14] as u16) << 8 | telegram[13] as u16;
        /* Get  */
    } else if tpl_long_header_ids.contains(&ci) {
        protocol_map.insert("ci_field".to_string(), serde_json::Value::from("long"));
        todo!("Support long header");
    } else if tpl_no_header_ids.contains(&ci) {
        info!("Message ignored, M-Bus will be implemented in later versions");
        return Err(OmsParseError::WiredProtocolNotSupported);
    } else {
        /* OMS LPWAN may be one of those not supported currently */
        return Err(OmsParseError::SecurityCiTypeNotSupported);
    }

    /* Check status for errors */
    match status & 0x03 {
        0 => protocol_map.insert("status".to_string(), serde_json::Value::from("ok")),
        1 => protocol_map.insert("status".to_string(), serde_json::Value::from("application busy")),
        2 => protocol_map.insert("status".to_string(), serde_json::Value::from("application error")),
        3 => protocol_map.insert("status".to_string(), serde_json::Value::from("alarm")),
        _ => panic!("ored value has more than 4 values")
    };

    protocol_map.insert("transmission_counter".to_string(), serde_json::Value::from(access_no));

    /* Get the security mode, Issue 5.0.1 / 2023-12 (RELEASE)  Table 18 */
    let security_mode = (config_field >> 8) & 0x1F;
    
    /* The encrypted payload will be stored here */
    let mut dec_data: Vec<u8> = Vec::new();

    match security_mode {
        5 => {
                protocol_map.insert("security_mode".to_string(), serde_json::Value::from(security_mode));
                let key = hex::decode(config.key).unwrap_or_default();
        
                dec_data = utils::decrypt_mode5(&telegram, access_no, 15, &key);

                /* Verify that the data is valid */
                if dec_data.len() < 2 || (dec_data[0] != 0x2f || dec_data[1] != 0x2F) {
                        return Err(OmsParseError::DecryptionFailed);
                }
                
                dec_data = utils::remove_oms_filler(&dec_data);

                mr.meter_name = config.name;
            },
        7 => {

        },
        _ => { return Err(OmsParseError::SecurityModeNotSupported); }
    }

    /* Add the decrypted payload to the document */
    mr.metered_values.insert("payload".to_string(), (dec_data.iter().map(|byte| format!("{:02X}", byte)).collect::<String>()).into());

    let mut parsed_data = div_vif_parser::parse_payload(&dec_data);
    mr.metered_values.append(&mut parsed_data);

    mr.metered_values.insert("proto".to_string(), protocol_map.into());
    return Ok(mr);
}



#[cfg(test)]
mod oms_parse_tests {
    use super::*;

    #[test]
    fn get_mr() {

        /* Example from 
            Open Metering System Specification Vol. 2 – Annex N
            RELEASE E (2023-12)
            N.2.1. wM-Bus Meter with Security profile A
        */

        
        /* To use with MQTT: 2E44931578563412330333637A2A0020255923C95AAA26D1B2E7493BC2AD013EC4A6F6D3529B520EDFF0EA6DEFC955B29D6D69EBF3EC8A */
        // Witout CRC: 2E4493157856341233037A2A0020255923C95AAA26D1B2E7493B013EC4A6F6D3529B520EDFF0EA6DEFC99D6D69EBF3
        let data: Vec<u8> = vec![
            0x2E, /* L Field        0   */
            0x44, /* C Field        1   */
            0x93, /* M Field        2   */
            0x15, /* M Field        3   */
            0x78, /* A Field        4   */
            0x56, /* A Field        5   */
            0x34, /* A Field        6   */
            0x12, /* A Field        7   */
            0x33, /* A Field        8   */
            0x03, /* A Field        9   */
            0x33, /* CRC */
            0x63, /* CRC */

            0x7A, /* CI Field       10 */
            0x2A, /* Access Number  11  */
            0x00, /* Status         12  */
            0x20, /* Config Field   13  */
            0x25, /* Config Field   14  */
            0x59, /* AES-Verify 0x2F */
            0x23, /* AES-Verify 0x2F */
            0xC9, /* DIF */
            0x5A, /* VIF */
            0xAA, /* Value LSB */
            0x26, /* Value */
            0xD1, /* Value */
            0xB2, /* Value MSB*/
            0xE7, /* DIF */
            0x49, /* VIF */
            0x3B, /* Value LSB */
            0xC2, /* CRC */
            0xAD, /* CRC */

            0x01, /* Value */
            0x3E, /* VALUE */
            0xC4, /* Value MSB */
            0xA6, /* DIF */
            0xF6, /* VIF */
            0xD3, /* VIFE */
            0x52, /* Value LSB */
            0x9B, /* Value MSB */
            0x52, /* Fill Byte due to AES */
            0x0E, /* Fill Byte due to AES */
            0xDF, /* Fill Byte due to AES */
            0xF0, /* Fill Byte due to AES */
            0xEA, /* Fill Byte due to AES */
            0x6D, /* Fill Byte due to AES */
            0xEF, /* Fill Byte due to AES */
            0xC9, /* Fill Byte due to AES */
            0x55, /* CRC */
            0xB2, /* CRC */

            0x9D, /* Fill Byte due to AES */
            0x6D, /* Fill Byte due to AES */
            0x69, /* Fill Byte due to AES */
            0xEB, /* Fill Byte due to AES */
            0xF3, /* Fill Byte due to AES */
            0xEC, /* CRC */
            0x8A, /* CRC */
            ];


        let test = utils::verifiy_crc(&data);
        assert_eq!(test.is_err(), false);
        let new_data = test.unwrap();
        assert_eq!(data.len() - 8, new_data.len());

        assert_eq!(utils::get_manufacturer(&data), "ELS");
        assert_eq!(utils::get_ident_no(&data), "12345678");


        let _key: Vec<u8> = vec![
            0x01,
            0x02,
            0x03,
            0x04,
            0x05,
            0x06,
            0x07,
            0x08,
            0x09,
            0x0A,
            0x0B,
            0x0C,
            0x0D,
            0x0E,
            0x0F,
            0x11
        ];

        let result = parse_oms_telegram(&data, true);
        assert_eq!(result.is_err(), false);
        let result = result.unwrap();
        assert_eq!(result.meter_name, "3ELS3312345678");

    }
}