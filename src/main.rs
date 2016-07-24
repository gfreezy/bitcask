extern crate byteorder;
extern crate core;
extern crate time;
extern crate memcached_protocal;
#[macro_use]
extern crate error_chain;

mod bitcask;
mod protocal;
mod error;

use std::net::TcpListener;
use std::net::TcpStream;
use std::thread;
use std::sync::Arc;
use std::sync::RwLock;

use memcached_protocal::Delete;
use memcached_protocal::Store;
use memcached_protocal::Retrieval;
use memcached_protocal::RetrievalResponse;
use memcached_protocal::RetrievalResponseItem;
use memcached_protocal::DeleteResponse;
use memcached_protocal::StoreResponse;

use ::protocal::memcached::MemcachedClient;
use ::error::ErrorKind;


fn handle_client(stream: TcpStream, db: Arc<RwLock<bitcask::Bitcask>>) {
    let mut client = MemcachedClient::new(&stream);
    loop {
        let cmd = match client.read() {
            Ok(cmd) => cmd,
            Err(e) => {
                println!("{:?}", e);

                match *e.kind() {
                    ErrorKind::Protocal(memcached_protocal::ErrorKind::StdIO) => {
                        return;
                    },
                    _ => {
                        continue;
                    }
                }
            }
        };

        match cmd {
            Retrieval(ref cmd) => {
                let mut locked_db = db.write().unwrap();
                let resp = RetrievalResponse(cmd.keys.iter()
                    .filter_map(|key| {
                        locked_db.get(key.clone())
                            .map(|value| {
                                RetrievalResponseItem{
                                    key: key.clone(),
                                    flags: 0,
                                    bytes: value.len() as u32,
                                    cas_unique: None,
                                    data_block: value.clone(),
                                }
                            })
                    })
                    .collect::<Vec<RetrievalResponseItem>>()
                );
                let _ = client.write(resp);
            },
            Delete(ref cmd) => {
                let mut locked_db = db.write().unwrap();
                locked_db.delete(cmd.key.clone());
                let _ = client.write(DeleteResponse::Deleted);
            },
            Store(ref cmd) => {
                if cmd.command_name.as_bytes() == b"set" {
                    let mut locked_db = db.write().unwrap();
                    locked_db.put(cmd.key.clone(), cmd.data_block.clone());
                    let _ = client.write(StoreResponse::Stored);
                }
            }
        }
    }
}


fn main() {
    let mut db = Arc::new(RwLock::new(bitcask::Bitcask::new("data".to_owned(), bitcask::BitcaskOptions::default())));

    let listener = TcpListener::bind("0.0.0.0:12340").expect("bind error");
    println!("bind");

    for stream in listener.incoming() {
        println!("new connection");
        match stream {
            Ok(stream) => {
                let db_clone = db.clone();
                thread::spawn(move || {
                    handle_client(stream, db_clone);
                });
            }
            Err(e) => {
                println!("{:?}", e);
            }
        }
    }
    //    for i in 1..100 {
    //        let _ = db.put("hello".to_owned(), vec![i]);
    //    }
    //    let _ = db.delete("a".to_owned());
    //    println!("{:?}", db.get("hello".to_owned()));
}
