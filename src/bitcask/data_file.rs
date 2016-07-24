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
pub struct DataFile {
    file: File,
    pub file_id: u32,
    write_offset: Option<u64>,
}


#[derive(Debug)]
pub struct DataEntry {
    pub crc: u16,
    pub timestamp: u32,
    pub key_size: u8,
    pub value_size: u32,
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}


impl DataFile {
    pub fn new<P: AsRef<Path>>(path: P, file_id: u32, write_offset: Option<u64>) -> DataFile {
        let file_path = path.as_ref().join(format!("{}.data", file_id));
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

        DataFile {
            file: file,
            file_id: file_id,
            write_offset: new_offset,
        }
    }

    fn is_readonly(&self) -> bool {
        return self.write_offset.is_none()
    }

    pub fn read_exact(&mut self, value_offse: u64, value: &mut [u8]) -> Result<(), String> {
        if let Err(_) = self.file.seek(std::io::SeekFrom::Start(value_offse)) {
            panic!("seek")
        }

        self.file.read_exact(value).expect("read value");

        Ok(())
    }

    pub fn write(&mut self, data_entry: &DataEntry) -> Result<u64, String> {
        if self.is_readonly() {
            unimplemented!()
        }

        if let Err(_) = self.file.seek(std::io::SeekFrom::End(0)) {
            panic!("seek end")
        }
        self.file.write_u16::<LittleEndian>(data_entry.crc).expect("write crc");
        self.file.write_u32::<LittleEndian>(data_entry.timestamp).expect("write timestamp");
        self.file.write(&[data_entry.key_size]).expect("write key size");
        self.file.write_u32::<LittleEndian>(data_entry.value_size).expect("write value size");
        self.file.write_all(&data_entry.key).expect("write key");
        let value_pos = self.file.seek(std::io::SeekFrom::Current(0)).expect("seek current position");
        self.file.write_all(data_entry.value.as_slice()).expect("write value");
        self.file.flush().expect("flush");

        Ok(value_pos)
    }
}


impl Iterator for DataFile {
    type Item = DataEntry;
    fn next(&mut self) -> Option<DataEntry> {
        let crc = self.file.read_u16::<LittleEndian>().expect("read crc");
        let timestamp = self.file.read_u32::<LittleEndian>().expect("read timestamp");
        let mut buf = [0; 1];
        self.file.read(&mut buf).expect("read key size");
        let key_size = buf[0];
        let value_size = self.file.read_u32::<LittleEndian>().expect("read value size");

        let mut key = vec![0; key_size as usize];
        self.file.read_exact(&mut key).expect("read key");
        let mut value = vec![0; value_size as usize];
        self.file.read_exact(&mut value).expect("read value");

        Some(DataEntry {
            crc: crc,
            timestamp: timestamp,
            key_size: key_size,
            value_size: value_size,
            key: key,
            value: value,
        })
    }
}


#[test]
fn test_write() {
    {
        let mut db = DataFile::new(".".to_owned(), 10, Some(0));
        let value = "你好".as_bytes().to_vec();
        let key = "哈哈".as_bytes().to_vec();
        let entry = DataEntry {
            crc: 1,
            timestamp: 1,
            key_size: key.len() as u8,
            value_size: value.len() as u32,
            key: key,
            value: value,
        };

        println!("write: {:?}", entry);
        assert!(db.write(&entry).is_ok());
    }
    {
        let mut db = DataFile::new(".".to_owned(), 10, None);
        let mut entry = DataEntry {
            crc: 1,
            timestamp: 1,
            key_size: 2,
            value_size: 2,
            key: Vec::new(),
            value: Vec::new(),
        };
        let ret = db.next();
        assert!(ret.is_some());
        println!("read: {:?}", entry);
    }
}
