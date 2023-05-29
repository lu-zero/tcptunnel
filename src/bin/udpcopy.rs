use anyhow::Result;
use futures::stream::StreamExt;

use tcptunnel::{to_endpoint, EndPoint, EndPointSink, EndPointStream};

use clap::Parser;

static AFTER_HELP: &str = r#"
The following uri syntaxes are supported:
- udp://<ip>:<port>?<query>
  With the following query arguments:
  - multicast=[ipv4_interface or ipv6_index]
  - multicast_ttl=<u32> (IPv4-only)
  - multicast_hops=<u32> (IPv6-only)
  - multicast_loop=<bool>
  - buffer=<usize>
- stdin:// (Only as Input)
  With the following query arguments:
  - packet_size
- stdout:// (Only as Output)
  With the following query arguments:
  - packet_size
"#;

#[derive(Debug, Parser)]
#[command(name = "udpcopy", after_help(AFTER_HELP))]
struct Opt {
    /// Input source uri
    #[clap(long, short, value_parser = to_endpoint)]
    input: EndPoint,
    /// Output sink uri
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
