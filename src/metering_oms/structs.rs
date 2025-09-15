struct OmsSecurityMode0 { mode: u8 }

struct OmsSecurityMode5 {
    bidirectional_communication: bool,
    accessibility: bool,
    synchronous: bool,
    mode: u16,
    number_of_enc_blocks: u16,
    content_of_message: u16,
    repeated_access: bool,
    hop_counter: u16,
}

struct OmsSecurityMode7 {
    content_of_message: u16,
    mode: u16,
    number_of_enc_blocks: u16,
    padding: bool,
    content_index: u16,
    kdf_selection: u8,
    key_id: u8
}

enum OmsModeData {
    Mode0(OmsSecurityMode0),
    Mode5(OmsSecurityMode5),
    Mode7(OmsSecurityMode7)
}
