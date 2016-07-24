use std;
use std::fs::File;
use std::path::Path;
use std::fs::OpenOptions;
use std::io::Seek;
use std::io::{Read, Write};
use byteorder::ReadBytesExt;
use byteorder::WriteBytesExt;
use byteorder::LittleEndian;


#[derive(Debug)]
pub struct HintFile {
    file: File,
    pub file_id: u32,
    write_offset: Option<u64>,
}


#[derive(Debug)]
pub struct HintEntry {
    pub timestamp: u32,
    pub key_size: u8,
    pub value_size: u32,
    pub value_pos: u64,
    pub key: Vec<u8>
}


impl HintFile {
    pub fn new<P: AsRef<Path>>(path: P, file_id: u32, write_offset: Option<u64>) -> HintFile {
        let file_path = path.as_ref().join(format!("{}.hint", file_id));
        let mut open_options = OpenOptions::new();
        open_options.read(true);
        let mut file = match write_offset {
            None => open_options.open(&file_path),
            Some(_) => {
                open_options.create(true).append(true).open(&file_path)
            }
        }.expect(&format!("open data file {}/{}", path.as_ref().to_string_lossy(), file_id));

        let new_offset = if write_offset.is_some() {
            Some(file.seek(std::io::SeekFrom::Current(0)).expect("seek file end"))
        } else {
            write_offset
        };

        HintFile {
            file: file,
            file_id: file_id,
            write_offset: new_offset,
        }
    }

    fn is_readonly(&self) -> bool {
        return self.write_offset.is_none()
    }

    pub fn write(&mut self, hint_entry: &HintEntry) -> Result<(), String> {
        if self.is_readonly() {
            unimplemented!()
        }

        if let Err(_) = self.file.seek(std::io::SeekFrom::End(0)) {
            panic!("seek end")
        }
        self.file.write_u32::<LittleEndian>(hint_entry.timestamp).expect("write timestamp");
        self.file.write(&[hint_entry.key_size]).expect("write key size");
        self.file.write_u32::<LittleEndian>(hint_entry.value_size).expect("write value size");
        self.file.write_u64::<LittleEndian>(hint_entry.value_pos).expect("write value pos");
        self.file.write_all(&hint_entry.key).expect("write key");
        self.file.flush().expect("flush");

        Ok(())
    }
}


impl Iterator for HintFile {
    type Item = HintEntry;
    fn next(&mut self) -> Option<HintEntry> {
        let timestamp = match self.file.read_u32::<LittleEndian>() {
            Ok(t) => t,
            Err(_) => return None
        };
        let mut key_buf: [u8; 1] = [0];
        let key_size = match self.file.read(&mut key_buf) {
            Ok(_) => key_buf[0],
            Err(_) => return None
        };
        let value_size = match self.file.read_u32::<LittleEndian>() {
            Ok(v) => v,
            Err(_) => return None
        };
        let value_pos = match self.file.read_u64::<LittleEndian>() {
            Ok(p) => p,
            Err(_) => return None
        };
        let mut key = vec![0; key_size as usize];
        if let Err(_) = self.file.read_exact(&mut key) {
            return None;
        }

        Some(HintEntry {
            timestamp: timestamp,
            key_size: key_size,
            value_size: value_size,
            value_pos: value_pos,
            key: key
        })
    }
}


#[test]
fn test_read_write() {
    {
        let mut db = HintFile::new(".".to_owned(), 0, Some(0));
        let value = "你好".as_bytes().to_vec();
        let key = "哈哈".as_bytes().to_vec();
        let entry = HintEntry {
            timestamp: 1,
            key_size: key.len() as u8,
            value_size: value.len() as u32,
            value_pos: db.write_offset.unwrap(),
            key: key
        };

        println!("write: {:?}", entry);
        assert!(db.write(&entry).is_ok());
    }
    {
        let db = HintFile::new(".".to_owned(), 0, None);
        for entry in db {
            println!("read {:?}", entry);
        }
    }
}
