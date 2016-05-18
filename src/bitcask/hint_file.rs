use std::fs::File;


pub struct HintFile {
    file: File,
    file_id: u32,
    write_offset: Option<u32>,
}


pub struct HintEntry {
    file_id: u32,
    value_size: u32,
    value_pos: u32,
    timestamp: u32,
}


impl HintFile {}
