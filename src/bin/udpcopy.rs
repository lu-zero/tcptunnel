use anyhow::Result;
use futures::stream::StreamExt;

use tcptunnel::{to_endpoint, EndPoint, EndPointSink, EndPointStream};

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

    let input = opt.input.make_stream()?;
    let output = opt.output.make_sink()?;

    Box::into_pin(input).forward(Box::into_pin(output)).await?;
    Ok(())
}
