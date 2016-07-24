use std::collections::HashMap;

use time;
use std::path::Path;

use bitcask::hint_file::HintEntry;
use bitcask::data_file::DataFile;
use bitcask::data_file::DataEntry;
use bitcask::hint_file::HintFile;


const TOMBSTONE: [u8;4] = [0, 0, 0, 0];
const FILE_SIZE: u64 = 1024 * 1024 * 100;

struct Entry {
    timestamp: u32,
    value_size: u32,
    value_pos: u64,
    file_id: u32,
}


pub struct Bitcask {
    entries: HashMap<String, Entry>,
    data_files: HashMap<u32, DataFile>,
    write_data: DataFile,
    write_hint: HintFile,
    write_id: u32,
    option: BitcaskOptions,
    path: String,
}

pub struct BitcaskOptions {
    file_size_limit: u64
}


impl Bitcask {
    pub fn new(path: String, option: BitcaskOptions) -> Bitcask {
        let mut latest_file_id: u32 = 0;
        let mut data_files = HashMap::new();
        let mut entries = HashMap::new();

        {
            let p = Path::new(&path);
            let files = p.read_dir().unwrap();
            for file in files {
                let file_path = match file {
                    Ok(f) => f.path(),
                    Err(_) => continue
                };
                let filename = match file_path.file_stem() {
                    Some(f) => f.to_string_lossy(),
                    None => continue
                };
                let file_id = match filename.parse::<u32>() {
                    Ok(i) => i,
                    Err(_) => continue
                };
                latest_file_id = file_id;
                let ext = match file_path.extension() {
                    Some(e) => e.to_string_lossy(),
                    None => continue
                };
                if ext == "data" {
                    let data_file = DataFile::new(&path, file_id, None);
                    data_files.insert(file_id, data_file);
                } else if ext == "hint" {
                    let hint_file = HintFile::new(&path, file_id, None);
                    for hint_entry in hint_file {
                        let entry = Entry {
                            timestamp: hint_entry.timestamp,
                            value_size: hint_entry.value_size,
                            value_pos: hint_entry.value_pos,
                            file_id: file_id,
                        };
                        let key = String::from_utf8(hint_entry.key);
                        entries.insert(key.unwrap(), entry);
                    }
                }
            }
        }

        let write_data = DataFile::new(&path, latest_file_id, Some(0));
        let write_hint = HintFile::new(&path, latest_file_id, Some(0));

        Bitcask {
            entries: entries,
            data_files: data_files,
            write_data: write_data,
            write_hint: write_hint,
            write_id: latest_file_id,
            option: option,
            path: path,
        }
    }

    pub fn get(&mut self, key: String) -> Option<Vec<u8>> {
        let entry = match self.entries.get(&key) {
            None => return None,
            Some(e) => e
        };
        let file_id = entry.file_id;
        let mut data_file = match self.data_files.get_mut(&file_id) {
            None => return None,
            Some(data_file) => data_file
        };
        let mut value = vec![0; entry.value_size as usize];
        data_file.read_exact(entry.value_pos, value.as_mut_slice()).expect("read exact");
        Some(value)
    }

    fn _put_file(&mut self, key: &[u8], value: Vec<u8>) -> Result<u64, String> {
        let key_bytes = key.to_vec();
        let ts = time::get_time().sec as u32;
        let value_size = value.len() as u32;
        let key_size = key_bytes.len() as u8;
        let data_entry = DataEntry{
            crc: 0,
            timestamp: ts,
            key_size: key_size,
            value_size: value_size,
            key: key_bytes.clone(),
            value: value
        };
        let value_pos = try!(self.write_data.write(&data_entry));

        let hint_entry = HintEntry{
            timestamp: ts,
            key_size: key_size,
            value_size: value_size,
            value_pos: value_pos,
            key: key_bytes,
        };
        self.write_hint.write(&hint_entry).expect("write entry hint");

        if value_pos >= self.option.file_size_limit {
            self._new_write_file();
        }

        Ok(value_pos)
    }

    fn _new_write_file(&mut self) {
        self.write_id += 1;
        self.write_hint = HintFile::new(&self.path, self.write_id, Some(0));
        self.write_data = DataFile::new(&self.path, self.write_id, Some(0));
    }

    pub fn delete(&mut self, key: String) -> Result<(), String> {
        self.entries.remove(&key);
        try!(self._put_file(key.as_bytes(), TOMBSTONE.to_vec()));
        Ok(())
    }

    pub fn put(&mut self, key: String, value: Vec<u8>) -> Result<(), String> {
        let ts = time::get_time().sec as u32;
        let value_size = value.len() as u32;
        let value_pos = try!(self._put_file(key.as_bytes(), value));

        let entry = Entry{
            timestamp: ts,
            value_size: value_size,
            value_pos: value_pos,
            file_id: self.write_data.file_id
        };
        self.entries.insert(key.to_owned(), entry);

        Ok(())
    }

    #[allow(dead_code)]
    pub fn merge(&mut self) {

    }
}


impl Default for BitcaskOptions {
    fn default() -> BitcaskOptions {
        BitcaskOptions {
            file_size_limit: FILE_SIZE
        }
    }
}


#[test]
fn test_new() {
    let option = BitcaskOptions::default();
    let bitcask = Bitcask::new("data".to_owned(), option);
}

#[test]
fn test_put() {
    let option = BitcaskOptions::default();
    let mut bitcask = Bitcask::new("data".to_owned(), option);
    let key = "key".to_owned();
    let val = "山东发生地方".to_owned().into_bytes();
    bitcask.put(key.clone(), val.clone());

    assert_eq!(val, bitcask.get(key.clone()).unwrap());
}

#[test]
fn test_delete() {
    let option = BitcaskOptions::default();
    let mut bitcask = Bitcask::new("data".to_owned(), option);
    let key = "key".to_owned();
    let val = "山东发生地方".to_owned().into_bytes();
    bitcask.put(key.clone(), val.clone());
    bitcask.delete(key.clone());
    assert_eq!(None, bitcask.get(key.clone()));
}

