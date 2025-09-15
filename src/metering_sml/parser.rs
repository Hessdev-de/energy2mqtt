use super::structs::*;
use super::SmlError;
use log::{debug, warn};

// SML Constants
const SML_ESCAPE: u8 = 0x1B;
const SML_VERSION: u8 = 0x01;
const SML_START_SEQUENCE: [u8; 4] = [0x1B, 0x1B, 0x1B, 0x1B];
const SML_END_SEQUENCE: [u8; 4] = [0x1B, 0x1B, 0x1B, 0x1A];

// SML Message Type constants
const SML_GET_LIST_RESPONSE: u16 = 0x701;
const SML_GET_PROC_PARAMETER_RESPONSE: u16 = 0x601;
const SML_ATTENTION: u16 = 0x901;

pub fn parse_sml_message(data: &[u8]) -> Result<SmlFile, SmlError> {
    debug!("Parsing SML message of {} bytes", data.len());
    
    // Find SML file boundaries
    let start_pos = find_sml_start(data)?;
    let end_pos = find_sml_end(data, start_pos)?;
    
    // Extract SML file content (between start and end sequences)
    let sml_content = &data[start_pos + 8..end_pos]; // +8 to skip start sequence and padding
    
    // Parse SML file structure
    let mut parser = SmlParser::new(sml_content);
    parser.parse_sml_file()
}

fn find_sml_start(data: &[u8]) -> Result<usize, SmlError> {
    for i in 0..data.len().saturating_sub(4) {
        if &data[i..i + 4] == &SML_START_SEQUENCE {
            return Ok(i);
        }
    }
    Err(SmlError::InvalidMessage)
}

fn find_sml_end(data: &[u8], start_pos: usize) -> Result<usize, SmlError> {
    for i in start_pos + 4..data.len().saturating_sub(3) {
        if i + 4 <= data.len() && &data[i..i + 4] == &SML_END_SEQUENCE {
            return Ok(i);
        }
    }
    Err(SmlError::InvalidMessage)
}

struct SmlParser<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> SmlParser<'a> {
    fn new(data: &'a [u8]) -> Self {
        Self { data, pos: 0 }
    }
    
    fn parse_sml_file(&mut self) -> Result<SmlFile, SmlError> {
        let mut messages = Vec::new();
        
        // Parse messages until end of data
        while self.pos < self.data.len() && self.data[self.pos] != 0x00 {
            match self.parse_sml_message() {
                Ok(message) => messages.push(message),
                Err(e) => {
                    warn!("Failed to parse SML message at position {}: {:?}", self.pos, e);
                    break;
                }
            }
        }
        
        if messages.is_empty() {
            return Err(SmlError::ParseError("No valid SML messages found".to_string()));
        }
        
        Ok(SmlFile { messages })
    }
    
    fn parse_sml_message(&mut self) -> Result<SmlMessage, SmlError> {
        // Parse message structure
        let transaction_id = self.parse_octet_string()?.unwrap_or_default();
        let group_no = self.parse_unsigned8()?;
        let abort_on_error = self.parse_unsigned8()?;
        let message_body = self.parse_message_body()?;
        let crc = self.parse_optional_unsigned16()?;
        let end_of_message = self.parse_unsigned8()?;
        
        Ok(SmlMessage {
            transaction_id,
            group_no,
            abort_on_error,
            message_body,
            crc,
            end_of_message,
            client_id: None, // Will be populated from message body if available
        })
    }
    
    fn parse_message_body(&mut self) -> Result<SmlMessageBody, SmlError> {
        // Parse message type
        let msg_type = self.parse_unsigned16()?;
        
        let mut body = SmlMessageBody {
            msg_type,
            get_list_response: None,
            get_proc_parameter_response: None,
            attention_response: None,
        };
        
        match msg_type {
            SML_GET_LIST_RESPONSE => {
                body.get_list_response = Some(self.parse_get_list_response()?);
            }
            SML_GET_PROC_PARAMETER_RESPONSE => {
                body.get_proc_parameter_response = Some(self.parse_get_proc_parameter_response()?);
            }
            SML_ATTENTION => {
                body.attention_response = Some(self.parse_attention_message()?);
            }
            _ => {
                warn!("Unknown SML message type: 0x{:04x}", msg_type);
                // Skip unknown message types
                self.skip_list()?;
            }
        }
        
        Ok(body)
    }
    
    fn parse_get_list_response(&mut self) -> Result<SmlGetListResponse, SmlError> {
        let client_id = self.parse_optional_octet_string()?;
        let server_id = self.parse_optional_octet_string()?;
        let list_name = self.parse_optional_octet_string()?;
        let act_sensor_time = self.parse_optional_unsigned32()?;
        let val_list = self.parse_val_list()?;
        let list_signature = self.parse_optional_octet_string()?;
        let act_gateway_time = self.parse_optional_unsigned32()?;
        
        Ok(SmlGetListResponse {
            client_id,
            server_id,
            list_name,
            act_sensor_time,
            val_list,
            list_signature,
            act_gateway_time,
        })
    }
    
    fn parse_val_list(&mut self) -> Result<Vec<SmlListEntry>, SmlError> {
        let list_length = self.parse_list_length()?;
        let mut entries = Vec::with_capacity(list_length);
        
        for _ in 0..list_length {
            entries.push(self.parse_list_entry()?);
        }
        
        Ok(entries)
    }
    
    fn parse_list_entry(&mut self) -> Result<SmlListEntry, SmlError> {
        let obis_code = self.parse_optional_octet_string()?;
        let status = self.parse_optional_unsigned64()?;
        let val_time = self.parse_optional_unsigned32()?;
        let unit = self.parse_optional_unsigned8()?;
        let scaler = self.parse_optional_signed8()?;
        let value = self.parse_optional_value()?;
        let value_signature = self.parse_optional_octet_string()?;
        
        Ok(SmlListEntry {
            obis_code,
            status,
            val_time,
            unit,
            scaler,
            value,
            value_signature,
        })
    }
    
    fn parse_get_proc_parameter_response(&mut self) -> Result<SmlGetProcParameterResponse, SmlError> {
        let server_id = self.parse_optional_octet_string()?;
        let parameter_tree_path = self.parse_octet_string()?.unwrap_or_default();
        let parameter_tree = self.parse_optional_tree()?;
        
        Ok(SmlGetProcParameterResponse {
            server_id,
            parameter_tree_path,
            parameter_tree,
        })
    }
    
    fn parse_attention_message(&mut self) -> Result<SmlAttentionMessage, SmlError> {
        let server_id = self.parse_optional_octet_string()?;
        let attention_no = self.parse_octet_string()?.unwrap_or_default();
        let attention_msg = self.parse_optional_octet_string()?;
        let attention_details = self.parse_optional_tree()?;
        
        Ok(SmlAttentionMessage {
            server_id,
            attention_no,
            attention_msg,
            attention_details,
        })
    }
    
    // Basic type parsers
    fn parse_type_length(&mut self) -> Result<(u8, usize), SmlError> {
        if self.pos >= self.data.len() {
            return Err(SmlError::ParseError("Unexpected end of data".to_string()));
        }
        
        let first_byte = self.data[self.pos];
        self.pos += 1;
        
        // Type and length encoding
        let type_field = (first_byte >> 4) & 0x07;
        let length_field = first_byte & 0x0F;
        
        let length = if length_field == 0x0F {
            // Extended length
            if self.pos >= self.data.len() {
                return Err(SmlError::ParseError("Unexpected end in extended length".to_string()));
            }
            let extended = self.data[self.pos];
            self.pos += 1;
            extended as usize
        } else {
            length_field as usize
        };
        
        Ok((type_field, length))
    }
    
    fn parse_list_length(&mut self) -> Result<usize, SmlError> {
        let (type_field, length) = self.parse_type_length()?;
        if type_field != 7 { // List type
            return Err(SmlError::ParseError("Expected list type".to_string()));
        }
        Ok(length)
    }
    
    fn parse_octet_string(&mut self) -> Result<Option<Vec<u8>>, SmlError> {
        let (type_field, length) = self.parse_type_length()?;
        
        if type_field == 0 {
            // Null/Optional not present
            return Ok(None);
        }
        
        if type_field != 0 && length > 0 {
            if self.pos + length > self.data.len() {
                return Err(SmlError::ParseError("Octet string extends beyond data".to_string()));
            }
            
            let value = self.data[self.pos..self.pos + length].to_vec();
            self.pos += length;
            return Ok(Some(value));
        }
        
        Ok(None)
    }
    
    fn parse_optional_octet_string(&mut self) -> Result<Option<Vec<u8>>, SmlError> {
        self.parse_octet_string()
    }
    
    fn parse_unsigned8(&mut self) -> Result<u8, SmlError> {
        let (type_field, length) = self.parse_type_length()?;
        if length != 1 {
            return Err(SmlError::ParseError("Invalid unsigned8 length".to_string()));
        }
        
        if self.pos >= self.data.len() {
            return Err(SmlError::ParseError("Unexpected end of data".to_string()));
        }
        
        let value = self.data[self.pos];
        self.pos += 1;
        Ok(value)
    }
    
    fn parse_optional_unsigned8(&mut self) -> Result<Option<u8>, SmlError> {
        let (type_field, length) = self.parse_type_length()?;
        
        if type_field == 0 {
            return Ok(None);
        }
        
        if length != 1 {
            return Err(SmlError::ParseError("Invalid optional unsigned8 length".to_string()));
        }
        
        if self.pos >= self.data.len() {
            return Err(SmlError::ParseError("Unexpected end of data".to_string()));
        }
        
        let value = self.data[self.pos];
        self.pos += 1;
        Ok(Some(value))
    }
    
    fn parse_signed8(&mut self) -> Result<i8, SmlError> {
        let (type_field, length) = self.parse_type_length()?;
        if length != 1 {
            return Err(SmlError::ParseError("Invalid signed8 length".to_string()));
        }
        
        if self.pos >= self.data.len() {
            return Err(SmlError::ParseError("Unexpected end of data".to_string()));
        }
        
        let value = self.data[self.pos] as i8;
        self.pos += 1;
        Ok(value)
    }
    
    fn parse_optional_signed8(&mut self) -> Result<Option<i8>, SmlError> {
        let (type_field, length) = self.parse_type_length()?;
        
        if type_field == 0 {
            return Ok(None);
        }
        
        if length != 1 {
            return Err(SmlError::ParseError("Invalid optional signed8 length".to_string()));
        }
        
        if self.pos >= self.data.len() {
            return Err(SmlError::ParseError("Unexpected end of data".to_string()));
        }
        
        let value = self.data[self.pos] as i8;
        self.pos += 1;
        Ok(Some(value))
    }
    
    fn parse_unsigned16(&mut self) -> Result<u16, SmlError> {
        let (type_field, length) = self.parse_type_length()?;
        if length != 2 {
            return Err(SmlError::ParseError("Invalid unsigned16 length".to_string()));
        }
        
        if self.pos + 2 > self.data.len() {
            return Err(SmlError::ParseError("Unexpected end of data".to_string()));
        }
        
        let value = u16::from_be_bytes([self.data[self.pos], self.data[self.pos + 1]]);
        self.pos += 2;
        Ok(value)
    }
    
    fn parse_optional_unsigned16(&mut self) -> Result<Option<u16>, SmlError> {
        let (type_field, length) = self.parse_type_length()?;
        
        if type_field == 0 {
            return Ok(None);
        }
        
        if length != 2 {
            return Err(SmlError::ParseError("Invalid optional unsigned16 length".to_string()));
        }
        
        if self.pos + 2 > self.data.len() {
            return Err(SmlError::ParseError("Unexpected end of data".to_string()));
        }
        
        let value = u16::from_be_bytes([self.data[self.pos], self.data[self.pos + 1]]);
        self.pos += 2;
        Ok(Some(value))
    }
    
    fn parse_unsigned32(&mut self) -> Result<u32, SmlError> {
        let (type_field, length) = self.parse_type_length()?;
        if length != 4 {
            return Err(SmlError::ParseError("Invalid unsigned32 length".to_string()));
        }
        
        if self.pos + 4 > self.data.len() {
            return Err(SmlError::ParseError("Unexpected end of data".to_string()));
        }
        
        let value = u32::from_be_bytes([
            self.data[self.pos],
            self.data[self.pos + 1],
            self.data[self.pos + 2],
            self.data[self.pos + 3],
        ]);
        self.pos += 4;
        Ok(value)
    }
    
    fn parse_optional_unsigned32(&mut self) -> Result<Option<u32>, SmlError> {
        let (type_field, length) = self.parse_type_length()?;
        
        if type_field == 0 {
            return Ok(None);
        }
        
        if length != 4 {
            return Err(SmlError::ParseError("Invalid optional unsigned32 length".to_string()));
        }
        
        if self.pos + 4 > self.data.len() {
            return Err(SmlError::ParseError("Unexpected end of data".to_string()));
        }
        
        let value = u32::from_be_bytes([
            self.data[self.pos],
            self.data[self.pos + 1],
            self.data[self.pos + 2],
            self.data[self.pos + 3],
        ]);
        self.pos += 4;
        Ok(Some(value))
    }
    
    fn parse_unsigned64(&mut self) -> Result<u64, SmlError> {
        let (type_field, length) = self.parse_type_length()?;
        if length != 8 {
            return Err(SmlError::ParseError("Invalid unsigned64 length".to_string()));
        }
        
        if self.pos + 8 > self.data.len() {
            return Err(SmlError::ParseError("Unexpected end of data".to_string()));
        }
        
        let value = u64::from_be_bytes([
            self.data[self.pos],
            self.data[self.pos + 1],
            self.data[self.pos + 2],
            self.data[self.pos + 3],
            self.data[self.pos + 4],
            self.data[self.pos + 5],
            self.data[self.pos + 6],
            self.data[self.pos + 7],
        ]);
        self.pos += 8;
        Ok(value)
    }
    
    fn parse_optional_unsigned64(&mut self) -> Result<Option<u64>, SmlError> {
        let (type_field, length) = self.parse_type_length()?;
        
        if type_field == 0 {
            return Ok(None);
        }
        
        if length != 8 {
            return Err(SmlError::ParseError("Invalid optional unsigned64 length".to_string()));
        }
        
        if self.pos + 8 > self.data.len() {
            return Err(SmlError::ParseError("Unexpected end of data".to_string()));
        }
        
        let value = u64::from_be_bytes([
            self.data[self.pos],
            self.data[self.pos + 1],
            self.data[self.pos + 2],
            self.data[self.pos + 3],
            self.data[self.pos + 4],
            self.data[self.pos + 5],
            self.data[self.pos + 6],
            self.data[self.pos + 7],
        ]);
        self.pos += 8;
        Ok(Some(value))
    }
    
    fn parse_optional_value(&mut self) -> Result<Option<SmlValue>, SmlError> {
        let (type_field, length) = self.parse_type_length()?;
        
        if type_field == 0 {
            return Ok(None);
        }
        
        // Rewind position to re-parse with correct type
        self.pos -= 1;
        
        match type_field {
            5 => Ok(Some(SmlValue::Bool(self.parse_bool()?))),
            6 => match length {
                1 => Ok(Some(SmlValue::Int8(self.parse_signed8()?))),
                2 => Ok(Some(SmlValue::Int16(self.parse_signed16()?))),
                4 => Ok(Some(SmlValue::Int32(self.parse_signed32()?))),
                8 => Ok(Some(SmlValue::Int64(self.parse_signed64()?))),
                _ => Err(SmlError::ParseError("Invalid signed integer length".to_string())),
            },
            0 => match length {
                1 => Ok(Some(SmlValue::UInt8(self.parse_unsigned8()?))),
                2 => Ok(Some(SmlValue::UInt16(self.parse_unsigned16()?))),
                4 => Ok(Some(SmlValue::UInt32(self.parse_unsigned32()?))),
                8 => Ok(Some(SmlValue::UInt64(self.parse_unsigned64()?))),
                _ => Err(SmlError::ParseError("Invalid unsigned integer length".to_string())),
            },
            _ => {
                // Default to octet string for unknown types
                self.pos += 1; // Skip type byte we rewound
                if self.pos + length > self.data.len() {
                    return Err(SmlError::ParseError("Value extends beyond data".to_string()));
                }
                let value = self.data[self.pos..self.pos + length].to_vec();
                self.pos += length;
                Ok(Some(SmlValue::OctetString(value)))
            }
        }
    }
    
    fn parse_bool(&mut self) -> Result<bool, SmlError> {
        let (type_field, length) = self.parse_type_length()?;
        if length != 1 {
            return Err(SmlError::ParseError("Invalid bool length".to_string()));
        }
        
        if self.pos >= self.data.len() {
            return Err(SmlError::ParseError("Unexpected end of data".to_string()));
        }
        
        let value = self.data[self.pos] != 0;
        self.pos += 1;
        Ok(value)
    }
    
    fn parse_signed16(&mut self) -> Result<i16, SmlError> {
        let (type_field, length) = self.parse_type_length()?;
        if length != 2 {
            return Err(SmlError::ParseError("Invalid signed16 length".to_string()));
        }
        
        if self.pos + 2 > self.data.len() {
            return Err(SmlError::ParseError("Unexpected end of data".to_string()));
        }
        
        let value = i16::from_be_bytes([self.data[self.pos], self.data[self.pos + 1]]);
        self.pos += 2;
        Ok(value)
    }
    
    fn parse_signed32(&mut self) -> Result<i32, SmlError> {
        let (type_field, length) = self.parse_type_length()?;
        if length != 4 {
            return Err(SmlError::ParseError("Invalid signed32 length".to_string()));
        }
        
        if self.pos + 4 > self.data.len() {
            return Err(SmlError::ParseError("Unexpected end of data".to_string()));
        }
        
        let value = i32::from_be_bytes([
            self.data[self.pos],
            self.data[self.pos + 1],
            self.data[self.pos + 2],
            self.data[self.pos + 3],
        ]);
        self.pos += 4;
        Ok(value)
    }
    
    fn parse_signed64(&mut self) -> Result<i64, SmlError> {
        let (type_field, length) = self.parse_type_length()?;
        if length != 8 {
            return Err(SmlError::ParseError("Invalid signed64 length".to_string()));
        }
        
        if self.pos + 8 > self.data.len() {
            return Err(SmlError::ParseError("Unexpected end of data".to_string()));
        }
        
        let value = i64::from_be_bytes([
            self.data[self.pos],
            self.data[self.pos + 1],
            self.data[self.pos + 2],
            self.data[self.pos + 3],
            self.data[self.pos + 4],
            self.data[self.pos + 5],
            self.data[self.pos + 6],
            self.data[self.pos + 7],
        ]);
        self.pos += 8;
        Ok(value)
    }
    
    fn parse_optional_tree(&mut self) -> Result<Option<SmlTree>, SmlError> {
        let (type_field, _length) = self.parse_type_length()?;
        
        if type_field == 0 {
            return Ok(None);
        }
        
        // Simplified tree parsing - in real implementation would be recursive
        let parameter_name = self.parse_optional_octet_string()?;
        let parameter_value = self.parse_optional_value()?;
        let child_list = None; // Simplified - would parse child list in full implementation
        
        Ok(Some(SmlTree {
            parameter_name,
            parameter_value,
            child_list,
        }))
    }
    
    fn skip_list(&mut self) -> Result<(), SmlError> {
        let (_type_field, length) = self.parse_type_length()?;
        // Skip the entire list structure
        if self.pos + length > self.data.len() {
            return Err(SmlError::ParseError("Skip extends beyond data".to_string()));
        }
        self.pos += length;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_find_sml_boundaries() {
        let data = [
            0x1B, 0x1B, 0x1B, 0x1B, // Start sequence (positions 0-3)
            0x01, 0x01, 0x01, 0x01, // Version + padding (positions 4-7)
            0x76, 0x05, 0x04, 0x03, // Sample SML data (positions 8-11)
            0x1B, 0x1B, 0x1B, 0x1A, // End sequence (positions 12-15)
        ];
        
        assert_eq!(find_sml_start(&data).unwrap(), 0);
        assert_eq!(find_sml_end(&data, 0).unwrap(), 12); // End sequence starts at position 12
    }

    #[test]
    fn test_sml_obis_code() {
        let bytes = [0x01, 0x00, 0x01, 0x08, 0x00, 0xFF];
        let obis = SmlObisCode::from_bytes(&bytes).unwrap();
        assert_eq!(obis.to_string(), "1-0:1.8.0.255");
    }

    #[test]
    fn test_parse_type_length() {
        let data = [0x72, 0x05]; // Type 7 (list), length 2
        let mut parser = SmlParser::new(&data);
        let (type_field, length) = parser.parse_type_length().unwrap();
        assert_eq!(type_field, 7);
        assert_eq!(length, 2);
    }
}