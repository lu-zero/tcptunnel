use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

use anyhow::{bail, Context, Result};
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
                let udp = Socket::new(Domain::IPV4, Type::DGRAM, Some(Protocol::UDP))?;
                udp.set_nonblocking(true)?;
                udp.set_reuse_address(true)?;
                udp.set_reuse_port(true)?;
                udp.set_multicast_loop_v4(false)?;
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
                let udp = Socket::new(Domain::IPV6, Type::DGRAM, Some(Protocol::UDP))?;
                udp.set_nonblocking(true)?;
                udp.set_reuse_address(true)?;
                udp.set_reuse_port(true)?;
                udp.set_multicast_loop_v6(false)?;
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

        let udp = UdpSocket::from_std(udp.into())?;

        Ok(udp)
    }

    pub fn make_input(&self) -> anyhow::Result<UdpFramed<BytesCodec>> {
        let udp = self.setup_udp(self.addr)?;

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
        let udp = self.setup_udp(localaddr)?;

        eprintln!("Output {:#?}", self);

        Ok(UdpFramed::new(udp, BytesCodec::new()))
    }
}

use url::Url;

pub fn to_endpoint(s: &str) -> Result<EndPoint> {
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

#[derive(Debug, Clone)]
pub struct EndPoint {
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
    /// UDP OS buffer size
    pub buffer: Option<usize>,
}
