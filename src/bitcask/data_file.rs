use std;
use std::fs::File;
use std::fs::OpenOptions;
use std::io::Seek;
use std::io::{Read, Write};
use std::error::Error;
use byteorder::ReadBytesExt;
use byteorder::WriteBytesExt;
use byteorder::ByteOrder;
use byteorder::LittleEndian;
use std::str;


pub struct DataFile {
    pub path: String,
    pub file: File,
    pub file_id: u32,
    pub write_offset: Option<u32>,
}


#[derive(Debug)]
pub struct DataEntry {
    pub crc: u16,
    pub timestamp: u32,
    pub key_size: u8,
    pub value_size: u32,
    pub key: String,
    pub value: Vec<u8>,
}


impl DataFile {
    pub fn new(path: String, file_id: u32, write_offset: Option<u32>) -> DataFile {
        let mut open_options = OpenOptions::new();
        open_options.read(true);
        let file = match write_offset {
            None => open_options.open(&path),
            Some(_) => open_options.write(true).create(true).truncate(true).open(&path),
        };

        DataFile {
            path: path,
            file: file.unwrap(),
            file_id: file_id,
            write_offset: write_offset,
        }
    }

    pub fn is_readonly(&self) -> bool {
        return self.write_offset.is_none()
    }

    pub fn read(&mut self, offset: u32, data_entry: &mut DataEntry) -> Result<(), String> {
        if let Err(e) = self.file.seek(std::io::SeekFrom::Start(offset as u64)) {
            panic!("seek")
        }
        let mut buf: [u8; 1024] = [0; 1024];
        let size = match self.file.read(&mut buf) {
            Ok(size) => {
                if size < 11 {
                    return Err("read error".to_owned());
                }
                size
            },
            Err(err) => return Err(err.description().to_owned()),
        };
        data_entry.crc = LittleEndian::read_u16(&buf);
        data_entry.timestamp = LittleEndian::read_u32(&buf[2..]);
        data_entry.key_size = buf[6];
        data_entry.value_size = LittleEndian::read_u32(&buf[7..]);

        {
            let s = match str::from_utf8(&buf[11..11+data_entry.key_size as usize]) {
                Err(err) => return Err("read error".to_owned()),
                Ok(s) => s,
            };
            data_entry.key.push_str(s);
        }
        let mut key_size = data_entry.key_size as isize - data_entry.key.len() as isize;
        while key_size > 0 {
            match self.file.read(&mut buf) {
                Err(err) => return Err(err.description().to_owned()),
                Ok(s) => s,
            };
            data_entry.key.push_str(match str::from_utf8(&buf[0..key_size as usize]) {
                Err(err) => return Err("read error".to_owned()),
                Ok(s) => s,
            });

            key_size -= buf.len() as isize;
        }

        {
            data_entry.value.extend_from_slice(&buf[]);
        }
        let mut value_size = data_entry.value_size as isize;
        while value_size > 0 {
            match self.file.read(&mut buf) {
                Err(err) => return Err(err.description().to_owned()),
                Ok(s) => s,
            };
            data_entry.value.extend_from_slice(&buf[..value_size as usize]);
            value_size -= buf.len() as isize;
        }

        Ok(())
    }

    pub fn write(&mut self, data_entry: &DataEntry) -> Result<(), String> {
        if self.is_readonly() {
            unimplemented!()
        }

        if let Err(e) = self.file.seek(std::io::SeekFrom::End(0)) {
            panic!("seek end")
        }
        let mut buf: [u8; 1024] = [0; 1024];
        LittleEndian::write_u16(&mut buf, data_entry.crc);
        LittleEndian::write_u32(&mut buf[2..], data_entry.timestamp);
        buf[6] = data_entry.key_size;
        LittleEndian::write_u32(&mut buf[7..], data_entry.value_size);

        match self.file.write(&buf[..11]) {
            Err(err) => return Err("error write1".to_owned()),
            Ok(size) => {
                if size != 11 {
                    return Err("error write2".to_owned());
                }
            }
        };
        self.file.write_all(data_entry.key.as_bytes());
        self.file.write_all(data_entry.value.as_slice());

        Ok(())
    }
}


#[test]
fn test_write() {
    {
        let mut db = DataFile::new("test.db".to_owned(), 10, Some(0));
        let value = "你好".as_bytes().to_vec();
        let key = "哈哈".to_owned();
        let entry = DataEntry{
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
        let mut db = DataFile::new("test.db".to_owned(), 10, None);
        let mut entry = DataEntry{
            crc: 1,
            timestamp: 1,
            key_size: 2,
            value_size: 2,
            key: String::new(),
            value: Vec::new(),
        };
        let ret = db.read(0, &mut entry);
        assert!(ret.is_ok());
        println!("read: {:?}", entry);
    }
}
