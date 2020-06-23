use std::io;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use bytes::BytesMut;
use tokio::codec::{BytesCodec, Decoder, FramedRead, FramedWrite};
use tokio::net::*;
use tokio::prelude::*;

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

fn udp_to_tcp(
    udp_addr: &SocketAddr,
    tcp_addr: &SocketAddr,
    udp_mcast_interface: &Option<Ipv4Addr>,
) -> Result<()> {
    let tcp = TcpStream::connect(tcp_addr);
    let udp = UdpSocket::bind(udp_addr)?;
    // println!("local udp {:?}", udp);

    if udp_addr.ip().is_multicast() {
        if let IpAddr::V4(addr) = udp_addr.ip() {
            udp.join_multicast_v4(
                &addr,
                udp_mcast_interface
                    .as_ref()
                    .unwrap_or(&Ipv4Addr::UNSPECIFIED),
            )
            .context("cannot join group")?;
        }
    }

    let srv = tcp.map(|w| {
        let tcp_sink = FramedWrite::new(w, BytesCodec::new());
        let (_udp_sink, udp_stream) = UdpFramed::new(udp, BytesCodec::new()).split();
        let mut now = Instant::now();
        let mut size = 0;
        let read = udp_stream.map(move |(msg, _addr)| {
            let elapsed = now.elapsed();
            if elapsed > Duration::from_secs(1) {
                eprint!(
                    "bps {:}\r",
                    (size as f32 / elapsed.as_millis() as f32) * 8000f32
                );
                now = Instant::now();
                size = 0;
            } else {
                size += msg.len();
            }
            // println!("recv: {} from {:?}", String::from_utf8_lossy(&msg), addr);
            msg.freeze()
        });

        tokio::spawn(
            tcp_sink
                .send_all(read)
                .map(|_| ())
                .map_err(|e| println!("{:?}", e)),
        );
    });

    tokio::run(srv.map_err(|e| println!("error {:?}", e)));

    Ok(())
}

fn tcp_to_udp(
    udp_addr: &SocketAddr,
    tcp_addr: &SocketAddr,
    udp_mcast_interface: &Option<Ipv4Addr>,
) -> Result<()> {
    let tcp = TcpStream::connect(tcp_addr);
    let udp_addr = udp_addr.clone();

    let udp = UdpSocket::bind(&"127.0.0.1:0".parse().unwrap())?;
    if udp_addr.ip().is_multicast() {
        if let IpAddr::V4(addr) = udp_addr.ip() {
            udp.join_multicast_v4(
                &addr,
                udp_mcast_interface
                    .as_ref()
                    .unwrap_or(&Ipv4Addr::UNSPECIFIED),
            )
            .context("cannot join group")?;
        }
    } else {
        udp.connect(&udp_addr).context("Cannot connect")?;
    }

    let srv = tcp.map(move |w| {
        let tcp_stream = FramedRead::new(w, ChunkDecoder::new(PACKET_SIZE));
        let (udp_sink, _udp_stream) = UdpFramed::new(udp, BytesCodec::new()).split();
        let mut now = Instant::now();
        let mut size = 0;
        let read = tcp_stream.map(move |buf| {
            let elapsed = now.elapsed();
            if elapsed > Duration::from_secs(1) {
                eprint!(
                    "bps {:}\r",
                    size as f32 / (elapsed.as_millis() * 1000) as f32
                );
                now = Instant::now();
            } else {
                size += buf.len();
            } // println!("sending: {}", String::from_utf8_lossy(&buf));
            (buf.freeze(), udp_addr)
        });

        tokio::spawn(
            udp_sink
                .send_all(read)
                .map(|_| ())
                .map_err(|e| println!("{:?}", e)),
        );
    });

    tokio::run(srv.map_err(|e| println!("error {:?}", e)));

    Ok(())
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

use std::net::{IpAddr, Ipv4Addr, SocketAddr};

#[derive(Debug, StructOpt)]
struct Opt {
    /// UDP multicast interface address in `ipv4` format
    #[structopt(short = "i", long, name = "MCAST_INTERFACE_ADDR")]
    udp_mcast_interface: Option<Ipv4Addr>,
    /// UDP address in `ip:port` format
    #[structopt(short, long, name = "UDP_ADDR")]
    udp_addr: SocketAddr,
    /// TCP address in `ip:port` format
    #[structopt(short, long, name = "TCP_ADDR")]
    tcp_addr: SocketAddr,
    // Set to listen on the tcp port
    // #[structopt(short)]
    // listen_tcp: bool,
    /// Send UDP data over the tcp connection
    #[structopt(short)]
    send_tcp: bool,
}

fn main() -> Result<()> {
    let opt = Opt::from_args();

    if !opt.send_tcp {
        eprintln!(
            "Sending TCP data to UDP {:?} -> {:?}",
            opt.tcp_addr, opt.udp_addr
        );
        tcp_to_udp(&opt.udp_addr, &opt.tcp_addr, &opt.udp_mcast_interface)?;
    } else {
        eprintln!(
            "Sending UDP data to TCP {:?} -> {:?}",
            opt.udp_addr, opt.tcp_addr
        );
        udp_to_tcp(&opt.udp_addr, &opt.tcp_addr, &opt.udp_mcast_interface)?;
    }

    Ok(())
}
