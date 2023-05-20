use std::time::{Duration, Instant};

use anyhow::Result;
use futures::stream::{StreamExt, TryStreamExt};

use tcptunnel::{to_endpoint, EndPoint};

use clap::Parser;

#[derive(Debug, Parser)]
#[clap(name = "udpcopy")]
struct Opt {
    /// Input source url
    /// It supports the following query parameters
    /// multicast=<ipv4_interface or ipv6_index>
    /// multicast_ttl=<u32> (IPv4-only)
    /// multicast_hops=<u32> (IPv6-only)
    /// buffer=<usize>
    #[clap(long, short, value_parser = to_endpoint)]
    input: EndPoint,
    /// Output source
    /// It supports the following query parameters
    /// multicast=<ipv4_interface or ipv6_index>
    /// multicast_ttl=<u32> (IPv4-only)
    /// multicast_hops=<u32> (IPv6-only)
    /// buffer=<usize>
    #[clap(long, short, value_parser = to_endpoint)]
    output: EndPoint,
    /// Verbose logging
    #[clap(long, short)]
    verbose: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    let opt = Opt::parse();

    match (opt.input, opt.output) {
        (EndPoint::Udp(input), EndPoint::Udp(output)) => {
            let udp_stream = input.make_input()?;
            let udp_sink = output.make_output()?;
            let udp_addr = output.addr;

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
        }
        _ => anyhow::bail!("Unsupported schemes"),
    }

    Ok(())
}
