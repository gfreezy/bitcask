use bitcask::hint_file::HintEntry;
use bitcask::data_file::DataFile;
use bitcask::hint_file::HintFile;
use std::collections::HashMap;


pub struct Bitcask {
    hint_entries: HashMap<String, HintEntry>,
    data_files: HashMap<u32, DataFile>,
    write_data: DataFile,
    write_hint: HintFile,
}

pub struct BitcaskOptions {

}


impl Bitcask {
    pub fn new(path: String, option: BitcaskOptions) -> Bitcask {
        Bitcask {
            hint_entries: HashMap::new(),
            data_files: HashMap::new(),
            write_data: DataFile::new(path, 1, None),
            write_hint: HintFile::new(),
        }
    }

    pub fn get(&self, key: String) -> Option<String> {
        None
    }

    pub fn put(&mut self, key: String, value: String) -> Result<bool, String> {
        Ok(true)
    }

    pub fn delete(&mut self, key: String) -> Result<bool, String> {
        Ok(true)
    }
}
