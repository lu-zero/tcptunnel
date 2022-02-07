use std::io;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use bytes::BytesMut;
use futures::stream::{StreamExt, TryStreamExt};
use tokio::net::*;
use tokio_util::codec::{BytesCodec, Decoder, FramedRead, FramedWrite};
use tokio_util::udp::UdpFramed;

struct ChunkDecoder {
    size: usize,
}

// const PACKET_SIZE: usize = 1316;

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

impl Opt {
    fn setup_udp(&self, addr: SocketAddr) -> Result<UdpSocket> {
        use socket2::*;

        let udp_ip = self.udp_addr.ip();
        let is_multicast = udp_ip.is_multicast();
        let sockaddr = SockAddr::from(addr);

        let udp = match udp_ip {
            IpAddr::V4(ref addr) => {
                let udp = Socket::new(Domain::ipv4(), Type::dgram(), Some(Protocol::udp()))?;
                udp.set_reuse_address(true)?;
                udp.set_reuse_port(true)?;
                if let Some(udp_buffer) = self.udp_buffer {
                    udp.set_send_buffer_size(udp_buffer)?;
                    udp.set_recv_buffer_size(udp_buffer)?;
                }
                udp.bind(&sockaddr)?;
                if is_multicast {
                    let mcast_if = match self.udp_mcast_interface_address {
                        Some(ref mcast_if) => mcast_if,
                        None => &Ipv4Addr::UNSPECIFIED,
                    };
                    udp.join_multicast_v4(addr, mcast_if)
                        .context("cannot join group")?;

                    if let Some(ttl) = self.multicast_ttl {
                        udp.set_multicast_ttl_v4(ttl)?;
                    }
                }
                udp
            }
            IpAddr::V6(ref addr) => {
                let udp = Socket::new(Domain::ipv6(), Type::dgram(), Some(Protocol::udp()))?;
                udp.set_reuse_address(true)?;
                udp.set_reuse_port(true)?;
                if let Some(udp_buffer) = self.udp_buffer {
                    udp.set_send_buffer_size(udp_buffer)?;
                    udp.set_recv_buffer_size(udp_buffer)?;
                }
                udp.bind(&sockaddr)?;
                if is_multicast {
                    let mcast_idx = match self.udp_mcast_interface_index {
                        Some(mcast_idx) => mcast_idx,
                        None => 0,
                    };

                    udp.join_multicast_v6(addr, mcast_idx)?;

                    if let Some(hops) = self.multicast_hops {
                        udp.set_multicast_hops_v6(hops)?;
                    }
                }
                udp
            }
        };

        let udp = UdpSocket::from_std(udp.into_udp_socket())?;

        Ok(udp)
    }

    async fn udp_to_tcp(&self) -> Result<()> {
        let tcp = TcpStream::connect(&self.tcp_addr).await?;
        let udp = self.setup_udp(self.udp_addr)?;

        let tcp_sink = FramedWrite::new(tcp, BytesCodec::new());
        let (_udp_sink, udp_stream) = UdpFramed::new(udp, BytesCodec::new()).split();
        let mut now = Instant::now();
        let mut size: usize = 0;

        let read = udp_stream.map_ok(move |(msg, _addr)| {
            let elapsed = now.elapsed();
            if elapsed > Duration::from_secs(1) {
                eprint!(
                    "bps {:} last packet size {}\r",
                    (size as f32 / elapsed.as_millis() as f32) * 8000f32,
                    msg.len()
                );
                now = Instant::now();
                size = 0;
            } else {
                size += msg.len();
            }
            // println!("recv: {} from {:?}", String::from_utf8_lossy(&msg), addr);
            msg.freeze()
        });

        read.forward(tcp_sink).await?;

        Ok(())
    }

    async fn tcp_to_udp(&self) -> Result<()> {
        let tcp = TcpStream::connect(&self.tcp_addr)
            .await
            .context("Connecting to TCP")?;
        let udp_addr = self.udp_addr.clone();

        let localaddr = SocketAddr::new(
            if let Some(addr) = self.udp_mcast_interface_address {
                addr.into()
            } else {
                if udp_addr.is_ipv4() {
                    Ipv4Addr::UNSPECIFIED.into()
                } else {
                    Ipv6Addr::UNSPECIFIED.into()
                }
            },
            0,
        );
        let udp = self.setup_udp(localaddr)?;

        let tcp_stream = FramedRead::new(tcp, ChunkDecoder::new(self.packet_size));
        let (udp_sink, _udp_stream) = UdpFramed::new(udp, BytesCodec::new()).split();
        let mut now = Instant::now();
        let mut size = 0;
        let read = tcp_stream.map_ok(move |buf| {
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

        read.forward(udp_sink).await?;

        // udp_sink.send_all(read).await?;

        Ok(())
    }
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

use clap::Parser;

use std::net::ToSocketAddrs;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

fn to_socket_addr(s: &str) -> Result<SocketAddr> {
    let mut socks = s.to_socket_addrs()?;

    socks
        .next()
        .ok_or(anyhow::anyhow!("No address for name {}", s))
}

#[derive(Debug, Parser)]
#[clap(name = "tcptunnel")]
struct Opt {
    /// UDP multicast interface address (IPv4 only)
    #[clap(short = 'a', long, name = "MCAST_INTERFACE_ADDR")]
    udp_mcast_interface_address: Option<Ipv4Addr>,
    /// UDP multicast interface index (IPv6 only)
    #[clap(short = 'i', long, name = "MCAST_INTERFACE_INDEX")]
    udp_mcast_interface_index: Option<u32>,
    /// UDP address in `ip:port` format
    #[clap(short, long, name = "UDP_ADDR", parse(try_from_str = to_socket_addr))]
    udp_addr: SocketAddr,
    /// TCP address in `ip:port` format
    #[clap(short, long, name = "TCP_ADDR", parse(try_from_str = to_socket_addr))]
    tcp_addr: SocketAddr,
    // Set to listen on the tcp port
    // #[clap(short)]
    // listen_tcp: bool,
    /// Send UDP data over the tcp connection
    #[clap(short)]
    send_tcp: bool,
    /// IPv4 Multicast TTL
    #[clap(short = 'm', long = "ttl")]
    multicast_ttl: Option<u32>,
    /// IPv6 Multicast Hops
    #[clap(long = "hops")]
    multicast_hops: Option<u32>,
    /// UDP OS send/receive buffer in bytes
    #[clap(long = "udp_buffer")]
    udp_buffer: Option<usize>,
    /// UDP packet size
    #[clap(short, default_value = "1316")]
    packet_size: usize,
}

#[tokio::main]
async fn main() -> Result<()> {
    let opt = Opt::parse();

    if !opt.send_tcp {
        eprintln!(
            "Sending TCP data to UDP {:?} -> {:?}",
            opt.tcp_addr, opt.udp_addr
        );
        opt.tcp_to_udp().await?;
    } else {
        eprintln!(
            "Sending UDP data to TCP {:?} -> {:?}",
            opt.udp_addr, opt.tcp_addr
        );
        opt.udp_to_tcp().await?;
    }

    Ok(())
}
