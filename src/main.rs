
use std::io;

use tokio::prelude::*;
use tokio::net::*;
use tokio::codec::{Decoder, BytesCodec, FramedRead, FramedWrite};
use bytes::BytesMut;

struct ChunkDecoder {
    size: usize,
}

const PACKET_SIZE: usize = 1316;

impl ChunkDecoder {
    pub fn new(size: usize) -> Self {
        ChunkDecoder { size }
    }
}

impl Decoder for ChunkDecoder {
    type Item = BytesMut;
    type Error = std::io::Error;

    fn decode(&mut self, buf: &mut BytesMut) -> Result<Option<BytesMut>, io::Error> {
        // println!("Decoding {}", buf.len());
        if buf.len() >= self.size {
            let out = buf.split_to(self.size);
            buf.reserve(self.size);
            Ok(Some(out))
        } else {
            Ok(None)
        }
    }
}

fn udp_to_tcp(udp_addr: &SocketAddr, tcp_addr: &SocketAddr) {
    let tcp = TcpStream::connect(tcp_addr);
    let udp = UdpSocket::bind(udp_addr).unwrap();
    // println!("local udp {:?}", udp);

    let srv = tcp.map(|w| {
        let tcp_sink = FramedWrite::new(w, BytesCodec::new());
        let (_udp_sink, udp_stream) = UdpFramed::new(udp, BytesCodec::new()).split();
        let read = udp_stream.map(|(msg, _addr)| {
            // println!("recv: {} from {:?}", String::from_utf8_lossy(&msg), addr);
            msg.freeze()
        });

        tokio::spawn(tcp_sink.send_all(read).map(|_| ()).map_err(|e| println!("{:?}", e)));
    });

    tokio::run(srv
            .map_err(|e| println!("error {:?}", e)));
}

fn tcp_to_udp(udp_addr: &SocketAddr, tcp_addr: &SocketAddr) {
    let tcp = TcpStream::connect(tcp_addr);
    let udp = UdpSocket::bind(&"127.0.0.1:0".parse().unwrap()).unwrap();
    let udp_addr = udp_addr.clone();

    let srv = tcp.map(move |w| {
        let tcp_stream = FramedRead::new(w, ChunkDecoder::new(PACKET_SIZE));
        let (udp_sink, _udp_stream) = UdpFramed::new(udp, BytesCodec::new()).split();

        let read = tcp_stream.map(move |buf| {
            // println!("sending: {}", String::from_utf8_lossy(&buf));
            (buf.freeze(), udp_addr)
        });

        tokio::spawn(udp_sink.send_all(read).map(|_| ()).map_err(|e| println!("{:?}", e)));
    });

    tokio::run(srv.map_err(|e| println!("error {:?}", e)));
}

/*
fn tcp_to_udp_listen(udp_addr: &SocketAddr, tcp_addr: &SocketAddr) {
    let tcp = TcpListener::bind(tcp_addr).unwrap();
    let udp_addr = udp_addr.clone();

    let srv = tcp.incoming()
        .sleep_on_error(std::time::Duration::from_millis(100))
        .map(move |socket| {
            eprintln!("Incoming connection on {:?}", socket);
            let udp = UdpSocket::bind(&"127.0.0.1:0".parse().unwrap()).unwrap();
            // println!("local udp {:?}", udp);

            let tcp_stream = FramedRead::new(socket, ChunkDecoder::new(PACKET_SIZE));
            let (udp_sink, _udp_stream) = UdpFramed::new(udp, BytesCodec::new()).split();

            let read = tcp_stream.map(move |buf| {
                // println!("sending: {}", String::from_utf8_lossy(&buf));
                (buf.freeze(), udp_addr)
            });

            tokio::spawn(udp_sink.send_all(read).map(|_| ()).map_err(|e| println!("{:?}", e)));

            Ok(())
        }).listen(1);

    tokio::run(srv);
}
*/

use structopt::StructOpt;

use std::net::SocketAddr;

#[derive(Debug, StructOpt)]
struct Opt {
    /// UDP address in `ip:port` format
    #[structopt(short, long, name="UDP_ADDR")]
    udp_addr: SocketAddr,
    /// TCP address in `ip:port` format
    #[structopt(short, long, name="TCP_ADDR")]
    tcp_addr: SocketAddr,
    // Set to listen on the tcp port
    // #[structopt(short)]
    // listen_tcp: bool,
    /// Send UDP data over the tcp connection
    #[structopt(short)]
    send_tcp: bool,
}

fn main() {
    let opt = Opt::from_args();

    if !opt.send_tcp {
        eprintln!("Sending TCP data to UDP {:?}", opt.tcp_addr);
        tcp_to_udp(&opt.udp_addr, &opt.tcp_addr);
    } else {
        eprintln!("Sending UDP data to TCP {:?}", opt.tcp_addr);
        udp_to_tcp(&opt.udp_addr, &opt.tcp_addr);
    }
}
