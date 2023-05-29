use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::time::Duration;

use anyhow::{bail, Context, Result};
use bytes::Bytes;
use futures::prelude::{Sink, Stream};
use futures::stream::TryStreamExt;
use pin_project::pin_project;
use tokio::io::{stdin, stdout};
use tokio::net::*;
use tokio::time::Instant;
use tokio_util::codec::{BytesCodec, FramedRead, FramedWrite};
use tokio_util::udp::UdpFramed;

use url::Url;

mod chunk;

pub use chunk::*;

pub fn to_endpoint(s: &str) -> Result<EndPoint> {
    let u = Url::parse(s)?;
    let query = u.query_pairs().collect::<HashMap<_, _>>();

    let packet_size = query
        .get("packet_size")
        .map(|m| m.parse())
        .transpose()?
        .unwrap_or(1316);

    let end = match u.scheme() {
        "udp" => {
            let socks = u.socket_addrs(|| Some(5555))?;
            let addr = *socks
                .first()
                .ok_or(anyhow::anyhow!("No address for name {}", s))?;

            let query = u.query_pairs().collect::<HashMap<_, _>>();

            let mut multicast_interface_address = None;
            let mut multicast_interface_index = None;

            if let Some(s) = query.get("multicast") {
                if addr.is_ipv4() {
                    multicast_interface_address = Some(s.parse()?);
                } else {
                    multicast_interface_index = Some(s.parse()?);
                }
            }

            let multicast_ttl = query.get("multicast_ttl").map(|m| m.parse()).transpose()?;
            let multicast_hops = query.get("multicast_hops").map(|m| m.parse()).transpose()?;
            let buffer = query.get("buffer").map(|m| m.parse()).transpose()?;
            let multicast_loop = query
                .get("multicast_loop")
                .map(|m| m.parse())
                .transpose()?
                .unwrap_or(false);

            EndPoint::Udp(UdpEndPoint {
                multicast_interface_address,
                multicast_interface_index,
                multicast_ttl,
                multicast_hops,
                multicast_loop,
                buffer,
                addr,
            })
        }
        "stdin" => EndPoint::Stdin(StdioEndPoint { packet_size }),
        "stdout" => EndPoint::Stdout(StdioEndPoint { packet_size }),
        _ => {
            bail!("Only udp is supported {:?}", u)
        }
    };

    Ok(end)
}

#[derive(Debug, Clone)]
pub struct UdpEndPoint {
    /// UDP multicast interface address (IPv4 only)
    pub multicast_interface_address: Option<Ipv4Addr>,
    /// UDP multicast interface index (IPv6 only)
    pub multicast_interface_index: Option<u32>,
    /// UDP address in `ip:port` format
    pub addr: SocketAddr,
    /// IPv4 Multicast TTL
    pub multicast_ttl: Option<u32>,
    /// IPv6 Multicast Hops
    pub multicast_hops: Option<u32>,
    /// local delivery of packets
    pub multicast_loop: bool,
    /// UDP OS buffer size
    pub buffer: Option<usize>,
}

pub trait EndPointStream {
    fn make_stream(&self) -> anyhow::Result<Box<dyn Stream<Item = Result<Bytes, std::io::Error>>>>;
}
pub trait EndPointSink {
    fn make_sink(&self) -> anyhow::Result<Box<dyn Sink<Bytes, Error = std::io::Error>>>;
}

impl UdpEndPoint {
    fn setup_udp(&self, localaddr: SocketAddr, join_multicast: bool) -> Result<UdpSocket> {
        use socket2::*;

        let udp_ip = self.addr.ip();
        let is_multicast = udp_ip.is_multicast();
        let sockaddr = SockAddr::from(localaddr);

        let udp = match udp_ip {
            IpAddr::V4(ref addr) => {
                let udp = Socket::new(Domain::IPV4, Type::DGRAM, Some(Protocol::UDP))?;
                udp.set_nonblocking(true)?;
                udp.set_reuse_address(true)?;
                udp.set_reuse_port(true)?;
                udp.set_multicast_loop_v4(self.multicast_loop)?;
                if let Some(udp_buffer) = self.buffer {
                    udp.set_send_buffer_size(udp_buffer)?;
                    udp.set_recv_buffer_size(udp_buffer)?;
                }
                udp.bind(&sockaddr)?;
                if is_multicast {
                    if join_multicast {
                        let mcast_if = match self.multicast_interface_address {
                            Some(ref mcast_if) => mcast_if,
                            None => &Ipv4Addr::UNSPECIFIED,
                        };
                        udp.join_multicast_v4(addr, mcast_if)
                            .context("cannot join group")?;
                    }
                    if let Some(ttl) = self.multicast_ttl {
                        udp.set_multicast_ttl_v4(ttl)?;
                    }
                }
                udp
            }
            IpAddr::V6(ref addr) => {
                let udp = Socket::new(Domain::IPV6, Type::DGRAM, Some(Protocol::UDP))?;
                udp.set_nonblocking(true)?;
                udp.set_reuse_address(true)?;
                udp.set_reuse_port(true)?;
                udp.set_multicast_loop_v6(self.multicast_loop)?;
                if let Some(udp_buffer) = self.buffer {
                    udp.set_send_buffer_size(udp_buffer)?;
                    udp.set_recv_buffer_size(udp_buffer)?;
                }
                udp.bind(&sockaddr)?;
                if is_multicast {
                    if join_multicast {
                        let mcast_idx = match self.multicast_interface_index {
                            Some(mcast_idx) => mcast_idx,
                            None => 0,
                        };

                        udp.join_multicast_v6(addr, mcast_idx)?;
                    }
                    if let Some(hops) = self.multicast_hops {
                        udp.set_multicast_hops_v6(hops)?;
                    }
                }
                udp
            }
        };

        let udp = UdpSocket::from_std(udp.into())?;

        Ok(udp)
    }

    pub fn make_input(&self) -> anyhow::Result<UdpFramed<BytesCodec>> {
        let udp = self.setup_udp(self.addr, true)?;

        eprintln!("Input {:#?}", self);

        Ok(UdpFramed::new(udp, BytesCodec::new()))
    }

    pub fn make_output(&self) -> anyhow::Result<UdpFramed<BytesCodec>> {
        let localaddr = SocketAddr::new(
            if let Some(addr) = self.multicast_interface_address {
                addr.into()
            } else {
                if self.addr.is_ipv4() {
                    Ipv4Addr::UNSPECIFIED.into()
                } else {
                    Ipv6Addr::UNSPECIFIED.into()
                }
            },
            0,
        );
        let udp = self.setup_udp(localaddr, false)?;

        eprintln!("Output {:#?}", self);

        Ok(UdpFramed::new(udp, BytesCodec::new()))
    }
}

#[pin_project]
struct UdpSink {
    #[pin]
    inner: UdpFramed<BytesCodec>,
    addr: SocketAddr,
}

impl Sink<Bytes> for UdpSink {
    type Error = <UdpFramed<BytesCodec> as Sink<(Bytes, SocketAddr)>>::Error;

    fn poll_ready(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::result::Result<(), Self::Error>> {
        <UdpFramed<BytesCodec> as futures::Sink<(Bytes, std::net::SocketAddr)>>::poll_ready(
            self.project().inner,
            cx,
        )
    }

    fn start_send(
        self: std::pin::Pin<&mut Self>,
        item: Bytes,
    ) -> std::result::Result<(), Self::Error> {
        let addr = self.addr;
        self.project().inner.start_send((item, addr))
    }

    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::result::Result<(), Self::Error>> {
        <UdpFramed<BytesCodec> as futures::Sink<(Bytes, std::net::SocketAddr)>>::poll_flush(
            self.project().inner,
            cx,
        )
    }

    fn poll_close(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::result::Result<(), Self::Error>> {
        <UdpFramed<BytesCodec> as futures::Sink<(Bytes, std::net::SocketAddr)>>::poll_close(
            self.project().inner,
            cx,
        )
    }
}

impl EndPointStream for UdpEndPoint {
    fn make_stream(&self) -> anyhow::Result<Box<dyn Stream<Item = Result<Bytes, std::io::Error>>>> {
        let input = self.make_input()?;
        let mut now = Instant::now();
        let mut size: usize = 0;

        let read = input.map_ok(move |(msg, _addr)| {
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
            msg.freeze()
        });

        Ok(Box::new(read))
    }
}
impl EndPointSink for UdpEndPoint {
    fn make_sink(&self) -> anyhow::Result<Box<dyn Sink<Bytes, Error = std::io::Error>>> {
        let inner = self.make_output()?;

        Ok(Box::new(UdpSink {
            inner,
            addr: self.addr,
        }))
    }
}

#[derive(Debug, Clone)]
pub struct StdioEndPoint {
    packet_size: usize,
}

impl EndPointStream for StdioEndPoint {
    fn make_stream(&self) -> anyhow::Result<Box<dyn Stream<Item = Result<Bytes, std::io::Error>>>> {
        let stdin = stdin();
        let input = FramedRead::new(stdin, ChunkDecoder::new(self.packet_size));
        let mut now = Instant::now();
        let mut size: usize = 0;

        let read = input.map_ok(move |msg| {
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
            msg.freeze()
        });

        Ok(Box::new(read))
    }
}

impl EndPointSink for StdioEndPoint {
    fn make_sink(&self) -> anyhow::Result<Box<dyn Sink<Bytes, Error = std::io::Error>>> {
        let stdout = stdout();
        let output = FramedWrite::new(stdout, BytesCodec::new());

        Ok(Box::new(output))
    }
}

#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum EndPoint {
    Udp(UdpEndPoint),
    Stdin(StdioEndPoint),
    Stdout(StdioEndPoint),
}

impl EndPointStream for EndPoint {
    fn make_stream(&self) -> anyhow::Result<Box<dyn Stream<Item = Result<Bytes, std::io::Error>>>> {
        match self {
            Udp(udp) => udp.make_stream(),
            Stdin(stdin) => stdin.make_stream(),
            _ => bail!("Unsupported"),
        }
    }
}
impl EndPointSink for EndPoint {
    fn make_sink(&self) -> anyhow::Result<Box<dyn Sink<Bytes, Error = std::io::Error>>> {
        match self {
            Udp(udp) => udp.make_sink(),
            Stdout(stdout) => stdout.make_sink(),
            _ => bail!("Unsupported"),
        }
    }
}

use EndPoint::*;
#[allow(unreachable_patterns)]
impl EndPoint {
    pub fn get_addr(&self) -> anyhow::Result<SocketAddr> {
        match self {
            Udp(udp) => Ok(udp.addr),
            _ => bail!("Not a network endpoint"),
        }
    }

    pub fn make_udp_input(&self) -> anyhow::Result<UdpFramed<BytesCodec>> {
        match self {
            Udp(udp) => udp.make_input(),
            _ => bail!("Not an udp endpoint"),
        }
    }
    pub fn make_udp_output(&self) -> anyhow::Result<UdpFramed<BytesCodec>> {
        match self {
            Udp(udp) => udp.make_output(),
            _ => bail!("Not an udp endpoint"),
        }
    }
}
