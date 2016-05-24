use std;
use std::fs::File;
use std::path::Path;
use std::fs::OpenOptions;
use std::io::Seek;
use std::io::{Read, Write};
use byteorder::ReadBytesExt;
use byteorder::WriteBytesExt;
use byteorder::ByteOrder;
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
        let mut open_options = OpenOptions::new();
        open_options.read(true);
        let file = match write_offset {
            None => open_options.open(&path),
            Some(_) => open_options.write(true).create(true).truncate(true).open(&path),
        };

        DataFile {
            file: file.unwrap(),
            file_id: file_id,
            write_offset: write_offset,
        }
    }

    pub fn is_readonly(&self) -> bool {
        return self.write_offset.is_none()
    }

    pub fn read(&mut self, offset: u32, data_entry: &mut DataEntry) -> Result<(), String> {
        if let Err(_) = self.file.seek(std::io::SeekFrom::Start(offset as u64)) {
            panic!("seek")
        }

        data_entry.crc = self.file.read_u16::<LittleEndian>().expect("read crc");
        data_entry.timestamp = self.file.read_u32::<LittleEndian>().expect("read timestamp");
        let mut buf = [0;1];
        self.file.read(&mut buf).expect("read key size");
        data_entry.key_size = buf[0];
        data_entry.value_size = self.file.read_u32::<LittleEndian>().expect("read value size");

        let mut key_buf = vec![0; data_entry.key_size as usize];
        self.file.read_exact(&mut key_buf).expect("read key");
        data_entry.key = key_buf;
        let mut value_buf = vec![0; data_entry.value_size as usize];
        self.file.read_exact(&mut value_buf).expect("read value");
        data_entry.value = value_buf;

        Ok(())
    }

    pub fn read_exact(&mut self, value_offse: u32, value: &mut [u8]) -> Result<(), String> {
        if let Err(_) = self.file.seek(std::io::SeekFrom::Start(value_offse as u64)) {
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
        let mut buf = [0; 11];
        LittleEndian::write_u16(&mut buf, data_entry.crc);
        LittleEndian::write_u32(&mut buf[2..], data_entry.timestamp);
        buf[6] = data_entry.key_size;
        LittleEndian::write_u32(&mut buf[7..], data_entry.value_size);

        match self.file.write(&buf[..11]) {
            Err(_) => return Err("error write1".to_owned()),
            Ok(size) => {
                if size != 11 {
                    return Err("error write2".to_owned());
                }
            }
        };
        self.file.write_all(&data_entry.key).expect("write key");
        let value_pos = self.file.seek(std::io::SeekFrom::Current(0)).expect("seek current position");
        self.file.write_all(data_entry.value.as_slice()).expect("write value");

        Ok(value_pos)
    }
}


#[test]
fn test_write() {
    {
        let mut db = DataFile::new("test.db".to_owned(), 10, Some(0));
        let value = "你好".as_bytes().to_vec();
        let key = "哈哈".as_bytes().to_vec();
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
            key: Vec::new(),
            value: Vec::new(),
        };
        let ret = db.read(0, &mut entry);
        assert!(ret.is_ok());
        println!("read: {:?}", entry);
    }
}
