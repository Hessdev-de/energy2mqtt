use log::debug;
use serde_json::Value;

type DifHandler = fn(start: &Vec<u8>, cur_pos: usize) -> (usize /* bytes to skip */, Value /* Data read */);

fn dif_no_data(_start: &Vec<u8>, _cur_pos: usize) -> (usize, Value) {
    return (0, serde_json::Value::from(""));
}

fn dif_read_8bit_int(start: &Vec<u8>, cur_pos: usize) -> (usize, Value) {
    let value : u8 = start[cur_pos];
    debug!("Reading 8bit int as {value}");
    return (1, Value::from(value));
}

fn dif_read_16bit_int(start: &Vec<u8>, cur_pos: usize) -> (usize, Value) {
    let value : u16 = (start[cur_pos + 1] as u16) << 8 | start[cur_pos] as u16;
    debug!("Reading 16bit int as {value}");
    return (2, Value::from(value));
}

fn dif_read_24bit_int(start: &Vec<u8>, cur_pos: usize) -> (usize, Value) {
    let value : u32 = (start[cur_pos + 2] as u32) << 16 | (start[cur_pos + 1] as u32) << 8 | start[cur_pos] as u32;
    debug!("Reading 24bit int as {value}");
    return (3, Value::from(value));
}

fn dif_read_32bit_int(start: &Vec<u8>, cur_pos: usize) -> (usize, Value) {
    let value : u32 = (start[cur_pos + 3] as u32) << 24 | (start[cur_pos + 2] as u32) << 16 | (start[cur_pos + 1] as u32) << 8 | start[cur_pos] as u32;
    debug!("Reading 32bit int as {value}");
    return (4,Value::from(value as i64));
}

fn dif_read_48bit_int(start: &Vec<u8>, cur_pos: usize) -> (usize, Value) {
    let value : u64 = (start[cur_pos + 5] as u64) << 40 | (start[cur_pos + 4] as u64) << 32 | (start[cur_pos + 3] as u64) << 24 | (start[cur_pos + 2] as u64) << 16 | (start[cur_pos + 1] as u64) << 8 | start[cur_pos] as u64;
    debug!("Reading 48bit int as {value}");
    return (6, Value::from(value));
    
}

fn dif_read_64bit_int(start: &Vec<u8>, cur_pos: usize) -> (usize, Value) {
    let value : u64 = (start[cur_pos + 7] as u64) << 56 | (start[cur_pos + 6] as u64) << 48 |(start[cur_pos + 5] as u64) << 40 | (start[cur_pos + 4] as u64) << 32 | (start[cur_pos + 3] as u64) << 24 | (start[cur_pos + 2] as u64) << 16 | (start[cur_pos + 1] as u64) << 8 | start[cur_pos] as u64;
    debug!("Reading 64bit int as {value}");
    return (8, Value::from(value));
}

fn dif_read_8digest_bcd(_start: &Vec<u8>, _cur_pos: usize) -> (usize, Value) {
    let value = "1111 1111";
    debug!("Reading 8 digest BCD as {value}");
    return (4, Value::from(value));
}

fn bcd_to_integer_sized(start: &Vec<u8>, cur_pos: usize, len: usize) -> u64 {
    let mut result: u64 = 0;
    /* build our range to read */
    let pos = cur_pos..cur_pos+len;
    for i in pos.rev() {
        let byte = start[i];
        let high = (byte >> 4) & 0x0F;
        let low = byte & 0x0F;
        result = result * 100 + (high * 10 + low) as u64;
    }
    result
}

fn dif_read_12digest_bcd(start: &Vec<u8>, cur_pos: usize) -> (usize, Value) {
    let value = bcd_to_integer_sized(start, cur_pos, 4);
    debug!("Reading 12 digest BCD as {value}");
    return (4, Value::from(value));
}

fn dif_read_32bit_real(_start: &Vec<u8>, _cur_pos: usize) -> (usize, Value) {
    return (4, Value::from(0.0));
}

fn get_dif_function(start: &Vec<u8>, cur_pos: usize) -> (usize, DifHandler, bool) {
    let dif = start[cur_pos];
    debug!("dif: {dif:02x}");
    match dif {
        /* No data, just skip that read */
        0x00 => { return (1, dif_no_data, false); },
        /* 8 bit integer */
        0x01 => { return (1, dif_read_8bit_int, true) },
        /* 16 bit integer */
        0x02 => { return (1, dif_read_16bit_int, true) },
        /* 24 bit integer */
        0x03 => { return (1, dif_read_24bit_int, true) },
        /* 32 bit integer */
        0x04 => { return (1, dif_read_32bit_int, true) },
        /* 32 bit real */
        0x05 => { return (1,dif_read_32bit_real, true )}
        /* 48 bit integer */
        0x06 => { return (1, dif_read_48bit_int, true) },
        /* 64 bit integer */
        0x07 => { return (1, dif_read_64bit_int, true) },
        /* Selection for Readout */
        0x08 => { return (1, dif_no_data, false); }
        

        /* 12 digest BCD */
        0x0C => { return (1, dif_read_12digest_bcd, true) }
        /* 8 digest BCD */
        0xF0 => { return (1, dif_read_8digest_bcd, true) },
        /* Idlefiller */
        0x2F => { return (1, dif_no_data, false); }
        _ => { debug!("Unkonwn dif"); return (1, dif_no_data, true); }
    }
}

type VifHandler = fn(vif: u32, data: Value) -> Value /* Data to store*/;

struct VifData {
    vif: u32,
    fildname: String,
    scaler: f64,
    unit: String,
    vif_function: Option<VifHandler>,
}

fn parse_time_point(vif: u32, data: Value) -> Value {
    /* make sure we got an int */
    if !data.is_number() {
        return Value::from("unparseable not a number");
    }
    let v = data.as_number().unwrap();
    println!("{v:?}");
    let time = v.as_i64();
    println!("{time:?}");
    if time.is_none() {
        return Value::from("unparseable not i64");
    }
    let time = time.unwrap() as u32;

    let is_type_f = vif & 0x1;

    if is_type_f == 1 {
        /* type f time & date */

        /* Based on the format G and the examples the higher byte is in format of G */
        /* 
            15 14 13 12 11 10 09 08 07 06 05 04 03 02 01 00
            y6 y5 y4 y3 m3 m2 m1 m0 y2 y1 y0 d4 d3 d3 d1 d0
            y = year
            d = day
            m = month
            taken from https://icplan.de/wp-content/uploads/2021/03/mbus_doc_01.pdf
            also https://github.com/rscada/libmbus/blob/master/mbus/mbus-protocol.c
        */

        let min = time & 0x3F;
        let hour = (time >> 8) & 0x1F;
        let day = (time >> 16) & 0x1F;
        let month = (time >> 24) & 0x0F;
        let mut year = ((time >> 16 & 0xE0) >> 5) | ((time >> 24 & 0xF0) >> 1);
        let mut hundred_year = (time & 0x60) >> 5;
        if hundred_year == 0 && year <= 80  //  compatibility with old meters with a circular two digit date
        {
            hundred_year = 1;
        }
        year  = 1900 + 100 * hundred_year + year;
        return Value::from(format!("{day:02}.{month:02}.{year:04} {hour:02}:{min:02}"));
    } else {
        /* type G date */
        /*
            yyyy mmmm yyyd dddd
            0001 0101 0001 1111
        */
        let day = time & 0x1F;
        let month = time >> 8 & 0x0F;
        let mut year = (time & 0xE0 >> 5) | ((time >> 16 & 0xF0) >> 1);
        let mut hundred_year = time & 0x60 >> 5;
        if hundred_year == 0 && year <= 80  //  compatibility with old meters with a circular two digit date
        {
            hundred_year = 1;
        }
        year  = 1900 + 100 * hundred_year + year;
        return Value::from(format!("{day:02}.{month:02}.{year:04}"));
    }
}

fn parse_on_time(vif: u32, data: Value) -> Value {
    /* make sure we got an int */
    if !data.is_number() {
        return Value::from("unparseable");
    }
    let v = data.as_number().unwrap();
    let time = v.as_i64();
    if time.is_none() {
        return Value::from("unparseable");
    }
    let time = time.unwrap();

    return Value::from(match vif & 0x3 {
        0b00 => time,
        0b01 => time * 60, /* Minutes to seconds */
        0b10 => time * 60 * 60, /* Hours to seconds */
        0b11 => time * 24 * 60 * 60, /* Days to seconds */
        _ => time
    });
}

fn vif_handle_binary(_vif: u32, data: Value) -> Value {
    if !data.is_number() {
        return Value::from("unparseable");
    }

    let v = data.as_number().unwrap();
    let d = v.as_i64();
    if d.is_none() {
        return Value::from("unparseable");
    }
    let d = d.unwrap() as u64;
    
    return Value::from(format!("{d:X}"));
}

fn get_vif_extension_fb(start: &Vec<u8>, cur_pos: usize) -> (usize /* bytes to skip */, VifData) {
    let vif: u32 = start[cur_pos + 1] as u32;
    let base: f64 = 10.0;
    return match vif & 0x7F {
        /* Previous mappings could be included here */
        
        /*    E000000n	Energy	10(n-1) MWh	0.1MWh to 1MWh */
        0b00000000..=0b00000001 => (2, VifData{ fildname: "energy".to_string(), scaler: base.powi((vif as i32 & 0x1) - 1) as f64, vif_function: None, unit: "MWh".to_string(), vif: vif }),
        /*    E000100n	Energy	10(n-1) GJ	0.1GJ to 1GJ */
        0b00001000..=0b00001001 => (2, VifData{ fildname: "energy".to_string(), scaler: base.powi((vif as i32 & 0x1) - 1) as f64, vif_function: None, unit: "GJ".to_string(), vif: vif }),
        /*    E001000n	Volume	10(n+2) m3	100m3 to 1000m3 */
        0b00010000..=0b00010001 => (2, VifData{ fildname: "volume".to_string(), scaler: base.powi((vif as i32 & 0x1) + 2) as f64, vif_function: None, unit: "m³".to_string(), vif: vif }),
        /*    E001100n	Mass	10(n+2) t	100t to 1000t */
        0b00011000..=0b00011001 => (2, VifData{ fildname: "mass".to_string(), scaler: base.powi((vif as i32 & 0x1) + 2) as f64, vif_function: None, unit: "t".to_string(), vif: vif }),
        /*    E0100001	Volume §	0,1 feet^3 */
        0b00100001 => (2, VifData{ fildname: "volume".to_string(), scaler: 0.1, vif_function: None, unit: "feet³".to_string(), vif: vif }),
        /*    E0100010	Volume §	0,1 american gallon */
        0b00100010 => (2, VifData{ fildname: "volume".to_string(), scaler: 0.1, vif_function: None, unit: "american_gallon".to_string(), vif: vif }),
        /*    E0100011	Volume	1 american gallon */
        0b00100011 => (2, VifData{ fildname: "volume".to_string(), scaler: 1.0, vif_function: None, unit: "american_gallon".to_string(), vif: vif }),
        /*    E0100100	Volume flow §	0,001 american gallon/min */
        0b00100100 => (2, VifData{ fildname: "volume_flow".to_string(), scaler: 0.001, vif_function: None, unit: "american_gallon/min".to_string(), vif: vif }),
        /*    E0100101	Volume flow	1 american gallon/min */
        0b00100101 => (2, VifData{ fildname: "volume_flow".to_string(), scaler: 1.0, vif_function: None, unit: "american_gallon/min".to_string(), vif: vif }),
        /*    E0100110	Volume flow	1 american gallon/h */
        0b00100110 => (2, VifData{ fildname: "volume_flow".to_string(), scaler: 1.0, vif_function: None, unit: "american_gallon/h".to_string(), vif: vif }),
        /*    E010100n	Power	10(n-1) MW	0.1MW to 1MW */
        0b00101000..=0b00101001 => (2, VifData{ fildname: "power".to_string(), scaler: base.powi((vif as i32 & 0x1) - 1) as f64, vif_function: None, unit: "MW".to_string(), vif: vif }),
        /*    E011000n	Power	10(n-1) GJ/h	0.1GJ/h to 1GJ/h */
        0b00110000..=0b00110001 => (2, VifData{ fildname: "power".to_string(), scaler: base.powi((vif as i32 & 0x1) - 1) as f64, vif_function: None, unit: "GJ/h".to_string(), vif: vif }),
        /*    E10110nn	Flow Temperature	10(nn-3) °F	0.001°F to 1°F */
        0b01011000..=0b01011011 => (2, VifData{ fildname: "flow_temperature".to_string(), scaler: base.powi((vif as i32 & 0x3) - 3) as f64, vif_function: None, unit: "°F".to_string(), vif: vif }),
        /*    E10111nn	Return Temperature	10(nn-3) °F	0.001°F to 1°F */
        0b01011100..=0b01011111 => (2, VifData{ fildname: "return_temperature".to_string(), scaler: base.powi((vif as i32 & 0x3) - 3) as f64, vif_function: None, unit: "°F".to_string(), vif: vif }),
        /*    E11000nn	Temperature Difference	10(nn-3) °F	0.001°F to 1°F */
        0b01100000..=0b01100011 => (2, VifData{ fildname: "temperature_difference".to_string(), scaler: base.powi((vif as i32 & 0x3) - 3) as f64, vif_function: None, unit: "°F".to_string(), vif: vif }),
        /*    E11001nn	External Temperature	10(nn-3) °F	0.001°F to 1°F */
        0b01100100..=0b01100111 => (2, VifData{ fildname: "external_temperature".to_string(), scaler: base.powi((vif as i32 & 0x3) - 3) as f64, vif_function: None, unit: "°F".to_string(), vif: vif }),
        /*    E11100nn	Cold / Warm Temperature Limit	10(nn-3) °F	0.001°F to 1°F */
        0b01110000..=0b01110011 => (2, VifData{ fildname: "cold_warm_temperature_limit".to_string(), scaler: base.powi((vif as i32 & 0x3) - 3) as f64, vif_function: None, unit: "°F".to_string(), vif: vif }),
        /*    E11101nn	Cold / Warm Temperature Limit	10(nn-3) °C	0.001°C to 1°C */
        0b01110100..=0b01110111 => (2, VifData{ fildname: "cold_warm_temperature_limit".to_string(), scaler: base.powi((vif as i32 & 0x3) - 3) as f64, vif_function: None, unit: "°C".to_string(), vif: vif }),
        /*    E1111nnn	cumul. count max power §	10(nnn-3) W	0.001W to 10000W */
        0b01111000..=0b01111111 => (2, VifData{ fildname: "cumul_count_max_power".to_string(), scaler: base.powi((vif as i32 & 0x7) - 3) as f64, vif_function: None, unit: "W".to_string(), vif: vif }),
        
        /* TODO: The § symbol appears in some entries but its meaning is not specified */
        
        _ => (1, VifData{ fildname: format!("unknown_at_{cur_pos}"), scaler: 1.0, vif_function: None, unit: "".to_string(), vif: vif })
    };
}

fn get_vif_extension_fd(start: &Vec<u8>, cur_pos: usize) -> (usize /* bytes to skip */, VifData) {
    let vif: u32 = start[cur_pos + 1] as u32;
    let base: f64 = 10.0;
    return match vif & 0x7F {
        /* Previous mappings could be included here */
        /*    E00000nn  Credit of 10nn-3 of the nominal local legal currency units */
        0b00000000..=0b00000011 => (2, VifData{ fildname: "credit".to_string(), scaler: base.powi((vif as i32 & 0x3) - 3) as f64, vif_function: None, unit: "currency_units".to_string(), vif: vif }),
        /*    E00001nn  Debit of 10nn-3 of the nominal local legal currency units */
        0b00000100..=0b00000111 => (2, VifData{ fildname: "debit".to_string(), scaler: base.powi((vif as i32 & 0x3) - 3) as f64, vif_function: None, unit: "currency_units".to_string(), vif: vif }),
        /*    E0001000  Access Number (transmission count) */
        0b00001000 => (2, VifData{ fildname: "access_number".to_string(), scaler: 1.0, vif_function: None, unit: "count".to_string(), vif: vif }),
        /*    E0001001  Medium (as in fixed header) */
        0b00001001 => (2, VifData{ fildname: "medium".to_string(), scaler: 1.0, vif_function: None, unit: "".to_string(), vif: vif }),
        /*    E0001010  Manufacturer (as in fixed header) */
        0b00001010 => (2, VifData{ fildname: "manufacturer".to_string(), scaler: 1.0, vif_function: None, unit: "".to_string(), vif: vif }),
        /*    E0001011  Parameter set identification Enhanced Identification */
        0b00001011 => (2, VifData{ fildname: "parameter_set_identification".to_string(), scaler: 1.0, vif_function: None, unit: "".to_string(), vif: vif }),
        /*    E0001100  Model / Version */
        0b00001100 => (2, VifData{ fildname: "model_version".to_string(), scaler: 1.0, vif_function: None, unit: "".to_string(), vif: vif }),
        /*    E0001101  Hardware version # */
        0b00001101 => (2, VifData{ fildname: "hardware_version".to_string(), scaler: 1.0, vif_function: None, unit: "".to_string(), vif: vif }),
        /*    E0001110  Firmware version # */
        0b00001110 => (2, VifData{ fildname: "firmware_version".to_string(), scaler: 1.0, vif_function: None, unit: "".to_string(), vif: vif }),
        /*    E0001111  Software version # */
        0b00001111 => (2, VifData{ fildname: "software_version".to_string(), scaler: 1.0, vif_function: None, unit: "".to_string(), vif: vif }),
        /*    E0010000  Customer location */
        0b00010000 => (2, VifData{ fildname: "customer_location".to_string(), scaler: 1.0, vif_function: None, unit: "".to_string(), vif: vif }),
        /*    E0010001  Customer */
        0b00010001 => (2, VifData{ fildname: "customer".to_string(), scaler: 1.0, vif_function: None, unit: "".to_string(), vif: vif }),
        /*    E0010010  Access Code User */
        0b00010010 => (2, VifData{ fildname: "access_code_user".to_string(), scaler: 1.0, vif_function: None, unit: "".to_string(), vif: vif }),
        /*    E0010011  Access Code Operator */
        0b00010011 => (2, VifData{ fildname: "access_code_operator".to_string(), scaler: 1.0, vif_function: None, unit: "".to_string(), vif: vif }),
        /*    E0010100  Access Code System Operator */
        0b00010100 => (2, VifData{ fildname: "access_code_system_operator".to_string(), scaler: 1.0, vif_function: None, unit: "".to_string(), vif: vif }),
        /*    E0010101  Access Code Developer */
        0b00010101 => (2, VifData{ fildname: "access_code_developer".to_string(), scaler: 1.0, vif_function: None, unit: "".to_string(), vif: vif }),
        /*    E0010110  Password */
        0b00010110 => (2, VifData{ fildname: "password".to_string(), scaler: 1.0, vif_function: None, unit: "".to_string(), vif: vif }),
        /*    E0010111  Error flags (binary) */
        0b00010111 => (2, VifData{ fildname: "error_flags".to_string(), scaler: 1.0, vif_function: Some(vif_handle_binary), unit: "".to_string(), vif: vif }),
        /*    E0011000  Error mask */
        0b00011000 => (2, VifData{ fildname: "error_mask".to_string(), scaler: 1.0, vif_function: None, unit: "".to_string(), vif: vif }),
        /*    E0011001  Reserved */
        0b00011001 => (2, VifData{ fildname: "reserved_0x19".to_string(), scaler: 1.0, vif_function: None, unit: "".to_string(), vif: vif }),
        /*    E0011010  Digital Output (binary) */
        0b00011010 => (2, VifData{ fildname: "digital_output".to_string(), scaler: 1.0, vif_function: Some(vif_handle_binary), unit: "".to_string(), vif: vif }),
        /*    E0011011  Digital Input (binary) */
        0b00011011 => (2, VifData{ fildname: "digital_input".to_string(), scaler: 1.0, vif_function: Some(vif_handle_binary), unit: "".to_string(), vif: vif }),
        /*    E0011100  Baudrate [Baud] */
        0b00011100 => (2, VifData{ fildname: "baudrate".to_string(), scaler: 1.0, vif_function: None, unit: "Baud".to_string(), vif: vif }),
        /*    E0011101  response delay time [bittimes] */
        0b00011101 => (2, VifData{ fildname: "response_delay_time".to_string(), scaler: 1.0, vif_function: None, unit: "bittimes".to_string(), vif: vif }),
        /*    E0011110  Retry */
        0b00011110 => (2, VifData{ fildname: "retry".to_string(), scaler: 1.0, vif_function: None, unit: "".to_string(), vif: vif }),
        /*    E0011111  Reserved */
        0b00011111 => (2, VifData{ fildname: "reserved_0x1f".to_string(), scaler: 1.0, vif_function: None, unit: "".to_string(), vif: vif }),
        /*    E0100000  First storage # for cyclic storage */
        0b00100000 => (2, VifData{ fildname: "first_storage_for_cyclic_storage".to_string(), scaler: 1.0, vif_function: None, unit: "".to_string(), vif: vif }),
        /*    E0100001  Last storage # for cyclic storage */
        0b00100001 => (2, VifData{ fildname: "last_storage_for_cyclic_storage".to_string(), scaler: 1.0, vif_function: None, unit: "".to_string(), vif: vif }),
        /*    E0100010  Size of storage block */
        0b00100010 => (2, VifData{ fildname: "size_of_storage_block".to_string(), scaler: 1.0, vif_function: None, unit: "".to_string(), vif: vif }),
        /*    E0100011  Reserved */
        0b00100011 => (2, VifData{ fildname: "reserved_0x23".to_string(), scaler: 1.0, vif_function: None, unit: "".to_string(), vif: vif }),
        /*    E01001nn  Storage interval [sec(s)..day(s)] */
        0b00100100..=0b00100111 => (2, VifData{ fildname: "storage_interval".to_string(), scaler: 1.0, vif_function: None, unit: "time".to_string(), vif: vif }),
        /*    E0101000  Storage interval month(s) */
        0b00101000 => (2, VifData{ fildname: "storage_interval_months".to_string(), scaler: 1.0, vif_function: None, unit: "months".to_string(), vif: vif }),
        /*    E0101001  Storage interval year(s) */
        0b00101001 => (2, VifData{ fildname: "storage_interval_years".to_string(), scaler: 1.0, vif_function: None, unit: "years".to_string(), vif: vif }),
        /*    E0101010  Reserved */
        0b00101010 => (2, VifData{ fildname: "reserved_0x2a".to_string(), scaler: 1.0, vif_function: None, unit: "".to_string(), vif: vif }),
        /*    E0101011  Reserved */
        0b00101011 => (2, VifData{ fildname: "reserved_0x2b".to_string(), scaler: 1.0, vif_function: None, unit: "".to_string(), vif: vif }),
        /*    E01011nn  Duration since last readout [sec(s)..day(s)] */
        0b00101100..=0b00101111 => (2, VifData{ fildname: "duration_since_last_readout".to_string(), scaler: 1.0, vif_function: None, unit: "time".to_string(), vif: vif }),
        /*    E0110000  Start (date/time) of tariff */
        0b00110000 => (2, VifData{ fildname: "start_of_tariff".to_string(), scaler: 1.0, vif_function: None, unit: "datetime".to_string(), vif: vif }),
        /*    E01100nn  Duration of tariff (nn=01 ..11: min to days) */
        0b00110001..=0b00110011 => (2, VifData{ fildname: "duration_of_tariff".to_string(), scaler: 1.0, vif_function: None, unit: "time".to_string(), vif: vif }),
        /*    E01101nn  Period of tariff [sec(s) to day(s)] */
        0b00110100..=0b00110111 => (2, VifData{ fildname: "period_of_tariff".to_string(), scaler: 1.0, vif_function: None, unit: "time".to_string(), vif: vif }),
        /*    E0111000  Period of tariff months(s) */
        0b00111000 => (2, VifData{ fildname: "period_of_tariff_months".to_string(), scaler: 1.0, vif_function: None, unit: "months".to_string(), vif: vif }),
        /*    E0111001  Period of tariff year(s) */
        0b00111001 => (2, VifData{ fildname: "period_of_tariff_years".to_string(), scaler: 1.0, vif_function: None, unit: "years".to_string(), vif: vif }),
        /*    E0111010  dimensionless / no VIF */
        0b00111010 => (2, VifData{ fildname: "dimensionless".to_string(), scaler: 1.0, vif_function: None, unit: "".to_string(), vif: vif }),
        /*    E0111011  Reserved */
        0b00111011 => (2, VifData{ fildname: "reserved_0x3b".to_string(), scaler: 1.0, vif_function: None, unit: "".to_string(), vif: vif }),
        /*    E01111xx  Reserved */
        0b00111100..=0b00111111 => (2, VifData{ fildname: "reserved_0x3c_0x3f".to_string(), scaler: 1.0, vif_function: None, unit: "".to_string(), vif: vif }),
        /*    E100nnnn  10nnnn-9 Volt */
        0b01000000..=0b01001111 => (2, VifData{ fildname: "voltage".to_string(), scaler: base.powi((vif as i32 & 0xF) - 9) as f64, vif_function: None, unit: "V".to_string(), vif: vif }),
        /*    E101nnnn  10nnnn-12 A */
        0b01010000..=0b01011111 => (2, VifData{ fildname: "current".to_string(), scaler: base.powi((vif as i32 & 0xF) - 12) as f64, vif_function: None, unit: "A".to_string(), vif: vif }),
        
        _ => (2, VifData{ fildname: format!("unknown_at_{cur_pos}"), scaler: 1.0, vif_function: None, unit: "".to_string(), vif: vif })
    };
}

fn get_vif_function(start: &Vec<u8>, cur_pos: usize) -> (usize /* bytes to skip */, VifData) {
    let vif: u32 = start[cur_pos] as u32;

    if vif == 0xFB {
        return get_vif_extension_fb(start, cur_pos);
    } else if vif == 0xFD {
        return get_vif_extension_fd(start, cur_pos);
    }

    let base: f64 = 10.0;
    /* Comments from https://m-bus.com/documentation-wired/08-appendix */
    let x= match vif & 0x7F {
        /*    E0000nnn	Energy	10(nnn-3) Wh	0.001Wh to 10000Wh */
        0b00000000..=0b00000111 => (1, VifData{ fildname: "energy".to_string(), scaler: base.powi((vif as i32 & 0x7) - 3) as f64 as f64 , vif_function: None, unit: "Wh".to_string(), vif: vif }),
        /*    E0001nnn	Energy	10(nnn) J	0.001kJ to 10000kJ  */
        0b00001000..=0b00001111 => (1, VifData{ fildname: "energy".to_string(), scaler: base.powi((vif as i32 & 0x7) - 3) as f64 as f64 , vif_function: None, unit: "J".to_string(), vif: vif }),
        /*    E0010nnn	Volume	10(nnn-6) m3	0.001l to 10000l */
        0b00010000..=0b00010111 => (1, VifData{ fildname: "volume".to_string(), scaler: base.powi((vif as i32 & 0x7) - 6) as f64 as f64 , vif_function: None, unit: "m³".to_string(), vif: vif }),
        /*    E0011nnn	Mass	10(nnn-3) kg	0.001kg to 10000kg */
        0b00011000..=0b00011111 => (1, VifData{ fildname: "mass".to_string(), scaler: base.powi((vif as i32 & 0x7) - 3) as f64, vif_function: None, unit: "kg".to_string(), vif: vif }),
        /*  E010 00nn	On Time nn = 00 seconds nn = 01 minutes nn = 10 hours nn = 11 days */
        0b00100000..=0b00100000 => (1, VifData{ fildname: "on_time".to_string(), scaler: 0.0, vif_function: Some(parse_on_time), unit: "s".to_string(), vif: vif }),
        /* E010 01nn	Operating Time */
        0b00100100..=0b00100111 => (1, VifData{ fildname: "operation_time".to_string(), scaler: 0.0, vif_function: Some(parse_on_time), unit: "s".to_string(), vif: vif }),
        /*    E0101nnn	Power	10(nnn-3) W	0.001W to 10000W */
        0b00101000..=0b00101111 => (1, VifData{ fildname: "power".to_string(), scaler: base.powi((vif as i32 & 0x7) - 3) as f64, vif_function: None, unit: "W".to_string(), vif: vif }),
        /*    E0110nnn	Power	10(nnn) J/h	0.001kJ/h to 10000kJ/h */
        0b00110000..=0b00110111 => (1, VifData{ fildname: "power".to_string(), scaler: base.powi(vif as i32 & 0x7) as f64, vif_function: None, unit: "J/h".to_string(), vif: vif }),
        /*    E0111nnn	Volume Flow	10(nnn-6) m3/h	0.001l/h to 10000l/h */
        0b00111000..=0b00111111 => (1, VifData{ fildname: "volume_flow".to_string(), scaler: base.powi((vif as i32 & 0x7) - 6) as f64, vif_function: None, unit: "m³/h".to_string(), vif: vif }),
        /*    E1000nnn	Volume Flow ext.	10(nnn-7) m3/min	0.0001l/min to 1000l/min */
        0b01000000..=0b01000111 => (1, VifData{ fildname: "volume_flow_ext".to_string(), scaler: base.powi((vif as i32 & 0x7) - 7) as f64, vif_function: None, unit: "m³/min".to_string(), vif: vif }),
        /*    E1001nnn	Volume Flow ext.	10(nnn-9) m3/s	0.001ml/s to 10000ml/s */
        0b01001000..=0b01001111 => (1, VifData{ fildname: "volume_flow_ext".to_string(), scaler: base.powi((vif as i32 & 0x7) - 9) as f64, vif_function: None, unit: "m³/s".to_string(), vif: vif }),
        /*    E1010nnn	Mass flow	10(nnn-3) kg/h	0.001kg/h to 10000kg/h */
        0b01010000..=0b01010111 => (1, VifData{ fildname: "mass_flow".to_string(), scaler: base.powi((vif as i32 & 0x7) - 3) as f64, vif_function: None, unit: "kg/h".to_string(), vif: vif }),
        /*    E10110nn	Flow Temperature	10(nn-3) °C	0.001°C to 1°C */
        0b01011000..=0b01011011 => (1, VifData{ fildname: "flow_temperature".to_string(), scaler: base.powi((vif as i32 & 0x3) - 3) as f64, vif_function: None, unit: "°C".to_string(), vif: vif }),
        /*    E10111nn	Return Temperature	10(nn-3) °C	0.001°C to 1°C */
        0b01011100..=0b01011111 => (1, VifData{ fildname: "return_temperature".to_string(), scaler: base.powi((vif as i32 & 0x3) - 3) as f64, vif_function: None, unit: "°C".to_string(), vif: vif }),
        /*    E11000nn	Temperature Difference	10(nn-3) K	1mK to 1000mK */
        0b01100000..=0b01100011 => (1, VifData{ fildname: "temperature_difference".to_string(), scaler: base.powi((vif as i32 & 0x3) - 3) as f64, vif_function: None, unit: "K".to_string(), vif: vif }),
        /*    E11001nn	External Temperature	10(nn-3) °C	0.001°C to 1°C */
        0b01100100..=0b01100111 => (1, VifData{ fildname: "external_temperature".to_string(), scaler: base.powi((vif as i32 & 0x3) - 3) as f64, vif_function: None, unit: "°C".to_string(), vif: vif }),
        /*    E11010nn	Pressure	10(nn-3) bar	1mbar to 1000mbar */
        0b01101000..=0b01101011 => (1, VifData{ fildname: "pressure".to_string(), scaler: base.powi((vif as i32 & 0x3) - 3) as f64, vif_function: None, unit: "bar".to_string(), vif: vif }),
        /*    E110110n	Time Point	n = 0 date (datatype G) n = 1 time & date (datatype F) */
        0b01101100..=0b01101101 => (1, VifData{ fildname: "time_of_readout".to_string(), scaler: 0.0, vif_function: Some(parse_time_point), unit: "".to_string(), vif: vif }),
        /*    E1101110	Units for H.C.A.	dimensionless */
        0b01101110 => (1, VifData{ fildname: "hca_units".to_string(), scaler: 1.0, vif_function: None, unit: "".to_string(), vif: vif }),
        /* E111 00nn	Averaging Duration	coded like OnTime	  */
        0b01110000..=01110011 => (1, VifData{ fildname: "averaging_duration".to_string(), scaler: 0.0, vif_function: Some(parse_on_time), unit: "s".to_string(), vif: vif }),
        /* E111 01nn	Actuality Duration	coded like OnTime	  */
        0b01110100..=01110111 => (1, VifData{ fildname: "actuality_duration".to_string(), scaler: 0.0, vif_function: Some(parse_on_time), unit: "s".to_string(), vif: vif }),
        _ => (1, VifData{ fildname: format!("unknown_at_{cur_pos}_{vif:x}"), scaler: 1.0, vif_function: None, unit: "unknown".to_string(), vif: vif })
    };

    return x;
}

pub fn parse_payload(payload: &Vec<u8>) -> serde_json::Map<String, serde_json::Value> {
    let mut ret = serde_json::Map::new();

    let mut cur_pos: usize = 0;
    while cur_pos < payload.len() {
        /* Each package cotains a DIF or DIFE, a DIF is one Byte DIFE can exceed that, therefor the offset */
        let (offset, handler, check_further) = get_dif_function(payload, cur_pos);
        cur_pos += offset;

        /* Skip the rest if the DIF is a noop */
        if check_further {
            let (offset, vif_data) = get_vif_function(payload, cur_pos);
            cur_pos += offset;

            /* we get a handler which allows us to do fancy stuff like reading int or bcd */
            let (offset, mut value) = handler(payload, cur_pos);
            cur_pos += offset;

            /* Most data is just reworked with a scaler but some requires a special parsing like times and stuff */
            if vif_data.vif_function.is_some() {
                let converter = vif_data.vif_function.unwrap();
                value = converter(vif_data.vif, value);
            } else if value.is_number() && vif_data.scaler != 1.0 {
                let v: f64 = value.as_number().unwrap().as_f64().unwrap();

                value = Value::from(v * vif_data.scaler);
            }

            ret.insert(vif_data.fildname.clone(), value);
            ret.insert(vif_data.fildname + "_unit", vif_data.unit.into());
        }
    }

    return ret;
}