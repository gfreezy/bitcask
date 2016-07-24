use std::net::TcpStream;
use std::io::Write;
use std::io::Read;

#[test]
fn test_get() {
    let mut client = TcpStream::connect("127.0.0.1:12340").unwrap();
    client.write_all(b"set a 0 0 1\r\nk\r\n");
    client.write_all(b"set a 0 0 1\r\nk\r\n");
    client.write_all(b"set a 0 0 1\r\nk\r\n");
    client.write_all(b"delete b\r\n");
    client.write_all(b"delete kkk\r\n");
    client.write_all(b"gets b\r\n");
    client.write_all(b"set a 0 0 1\r\nk\r\n");

    client.write_all(b"gets b\r\n");
    let mut s = String::new();
    client.read_to_string(&mut s);
    println!("{:?}", s);
}