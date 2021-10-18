use std::collections::HashMap;
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use futures::stream::{StreamExt, TryStreamExt};
use tokio::net::*;
use tokio_util::codec::BytesCodec;
use tokio_util::udp::UdpFramed;

impl EndPoint {
    fn setup_udp(&self, localaddr: SocketAddr) -> Result<UdpSocket> {
        use socket2::*;

        let udp_ip = self.addr.ip();
        let is_multicast = udp_ip.is_multicast();
        let sockaddr = SockAddr::from(localaddr);

        let udp = match udp_ip {
            IpAddr::V4(ref addr) => {
                let udp = Socket::new(Domain::ipv4(), Type::dgram(), Some(Protocol::udp()))?;
                udp.set_reuse_address(true)?;
                udp.set_reuse_port(true)?;
                if let Some(udp_buffer) = self.buffer {
                    udp.set_send_buffer_size(udp_buffer)?;
                    udp.set_recv_buffer_size(udp_buffer)?;
                }
                udp.bind(&sockaddr)?;
                if is_multicast {
                    let mcast_if = match self.multicast_interface_address {
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
                if let Some(udp_buffer) = self.buffer {
                    udp.set_send_buffer_size(udp_buffer)?;
                    udp.set_recv_buffer_size(udp_buffer)?;
                }
                udp.bind(&sockaddr)?;
                if is_multicast {
                    let mcast_idx = match self.multicast_interface_index {
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
}

use structopt::StructOpt;

use url::Url;

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

fn to_endpoint(s: &str) -> Result<EndPoint> {
    let u = Url::parse(s)?;

    if u.scheme() != "udp" {
        bail!("Only udp is supported {:?}", u);
    }

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

    Ok(EndPoint {
        multicast_interface_address,
        multicast_interface_index,
        multicast_ttl,
        multicast_hops,
        buffer,
        addr,
    })
}

#[derive(Debug)]
struct EndPoint {
    /// UDP multicast interface address (IPv4 only)
    multicast_interface_address: Option<Ipv4Addr>,
    /// UDP multicast interface index (IPv6 only)
    multicast_interface_index: Option<u32>,
    /// UDP address in `ip:port` format
    addr: SocketAddr,
    /// IPv4 Multicast TTL
    multicast_ttl: Option<u32>,
    /// IPv6 Multicast Hops
    multicast_hops: Option<u32>,
    /// UDP OS buffer size
    buffer: Option<usize>,
}

#[derive(Debug, StructOpt)]
struct Opt {
    /// Input source url
    /// It supports the following query parameters
    /// multicast=<ipv4_interface or ipv6_index>
    /// multicast_ttl=<u32> (IPv4-only)
    /// multicast_hops=<u32> (IPv6-only)
    /// buffer=<usize>
    #[structopt(long, short,  parse(try_from_str = to_endpoint))]
    input: EndPoint,
    /// Output source
    /// It supports the following query parameters
    /// multicast=<ipv4_interface or ipv6_index>
    /// multicast_ttl=<u32> (IPv4-only)
    /// multicast_hops=<u32> (IPv6-only)
    /// buffer=<usize>
    #[structopt(long, short, parse(try_from_str = to_endpoint))]
    output: EndPoint,
    /// Verbose logging
    #[structopt(long, short)]
    verbose: bool,
}

impl Opt {
    fn input_endpoint(&self) -> anyhow::Result<UdpFramed<BytesCodec>> {
        let e = &self.input;
        let udp = e.setup_udp(e.addr)?;

        eprintln!("Input {:#?}", e);

        Ok(UdpFramed::new(udp, BytesCodec::new()))
    }

    fn output_endpoint(&self) -> anyhow::Result<UdpFramed<BytesCodec>> {
        let e = &self.output;
        let localaddr = SocketAddr::new(
            if let Some(addr) = e.multicast_interface_address {
                addr.into()
            } else {
                if e.addr.is_ipv4() {
                    Ipv4Addr::UNSPECIFIED.into()
                } else {
                    Ipv6Addr::UNSPECIFIED.into()
                }
            },
            0,
        );
        let udp = e.setup_udp(localaddr)?;

        eprintln!("Output {:#?}", e);

        Ok(UdpFramed::new(udp, BytesCodec::new()))
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let opt = Opt::from_args();

    let (_sink, udp_stream) = opt.input_endpoint()?.split();
    let (udp_sink, _stream) = opt.output_endpoint()?.split();
    let udp_addr = opt.output.addr;

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
        (msg.freeze(), udp_addr)
    });

    read.forward(udp_sink).await?;

    Ok(())
}
