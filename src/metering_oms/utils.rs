use crc16::{State, EN_13757};
use aes::cipher::{block_padding::NoPadding, generic_array::GenericArray, BlockDecryptMut, KeyIvInit};
use crate::{config::{ConfigBases, OmsConfig}, get_config_or_panic, CONFIG};

use super::OmsParseError;
type Aes128CbcDec = cbc::Decryptor<aes::Aes128>;


/* This functions returns a new vector with all data if the crc matches */
pub fn verifiy_crc(telegram: &Vec<u8>) -> Result<Vec<u8>, OmsParseError> {
    let mut result: Vec<u8> = Vec::new();
    /* 
        The first block has 10 bytes,
        each following block has either 16 bytes or 
        is -2 bytes if length not worked
        */
    let mut start = 0;
    let mut first_block = true;
    loop {
        let mut len = match first_block {
            true => { first_block = false; 10}
            false => 16
        };

        /* We got a short package */
        if telegram.len() < start + 17 {
            len = telegram.len() - start - 2;
        }

        /* at the end of the data there are two bytes of CRC */
        let end_of_data_to_crc = start + len;

        let mut state = State::<EN_13757>::new();
        for i in start..end_of_data_to_crc {
            let byte = &telegram[i].to_le_bytes();
            state.update(byte);
            result.push(telegram[i]);
        }

        /* check the CRC matches */
        let s = state.get().to_be_bytes().to_vec();
        if s[0] != telegram[end_of_data_to_crc] || s[1] != telegram[end_of_data_to_crc + 1] {
            return Err(OmsParseError::CRCMissMatch);
        }

        /* Add the two bytes of CRC to it */
        start = end_of_data_to_crc + 2;
        if telegram.len() == start {
            break;
        }
    }

    return Ok(result);
}

/* Taken from: https://www.m-bus.de/man.html */
pub fn get_manufacturer(telegram: &Vec<u8>) -> String {
    let mut m : u16 = ((telegram[3] as u16) << 8) as u16;
    m += telegram[2] as u16;

    return format!("{}{}{}",
                    String::from_utf8(vec![(((m >> 10) & 0x1F) + 64) as u8]).unwrap(),
                    String::from_utf8(vec![(((m >> 5) & 0x1F) + 64) as u8]).unwrap(),
                    String::from_utf8(vec![((m & 0x1F) + 64) as u8]).unwrap());
}

pub fn get_ident_no(telegram: &Vec<u8>) -> String {
  return format!("{:02x}{:02x}{:02x}{:02x}",telegram[7], telegram[6], telegram[5], telegram[4]);
}

pub fn get_meter_config(din_addr: &String) -> Option<OmsConfig> {
    let conf = get_config_or_panic!("oms", ConfigBases::Oms);
        
    for sensor in conf {
        if sensor.id == *din_addr {
            return Some(sensor.clone());
        }
    }

    return None;
}

pub fn remove_oms_filler(original: &Vec<u8>) -> Vec<u8> {
    /* remove element 0 and 1 */
    let ret = original[2..].to_vec();
    /* Now find how many times a 0x2F is at the end to pad the AES encryption and remove those, too */
    let aes_filler: u8 = 0x2F;
    let element_to_remove = ret.iter().rev().take_while(|&&x| x == aes_filler).count() + 2;
    let ret = ret[..(original.len()-element_to_remove)].to_vec();
    return ret;
}

pub fn get_device_medium(device_type: &String) -> String {
    return match device_type as &str {
        "2" => "Electricity",
        "3" => "Gas",
        "4" => "Heat",
        "6" => "Water (hot)",
        "7" => "Water (cold)",
        "8" => "Heat Cost Allocator",
        "A" => "Cooling",
        "B" => "Cooling",
        "C" => "Heat",
        "D" => "Heat / Cooling Combined",
        /* Warning those will break the DIN ID creation */
        "15" => "Water (hot)",
        "16" => "Water (cold)",
        "20" => "Breaker / Valve",
        "21" => "Breaker / Valve",
        _ => { "unknown" },
    }.to_string();
}
pub fn decrypt_mode5(telegram: &Vec<u8>, access_no: u8, start_encryption: usize, key: &Vec<u8>) -> Vec<u8> {
    let iv : Vec<u8> = vec![
        telegram[2],    /* M-Field */
        telegram[3],    /* M-Field */
        telegram[4],    /* A-Field 1 */
        telegram[5],    /* A-Field 2 */
        telegram[6],    /* A-Field 3 */
        telegram[7],    /* A-Field 4 */
        telegram[8],    /* A-Field 5 */
        telegram[9],    /* A-Field 6 */
        access_no,      /* Access number repeated 8 times */
        access_no,
        access_no,
        access_no,
        access_no,
        access_no,
        access_no,
        access_no
    ];

    let intermed = telegram[start_encryption..].to_vec();
    let ciphertext: &[u8] = &intermed.as_slice();
    let k = GenericArray::clone_from_slice(&key);
    let i = GenericArray::clone_from_slice(&iv);
    let decryption = Aes128CbcDec::new(&k.into(), &i.into()).decrypt_padded_vec_mut::<NoPadding>(ciphertext);
    return decryption.unwrap_or_default();
}