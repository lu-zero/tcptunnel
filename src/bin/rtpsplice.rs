use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;

use bytes::Bytes;
use futures::{SinkExt, StreamExt, TryFutureExt, TryStreamExt};
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use rtp::header::Header;
// use rtp::header::Header;
use tokio::sync::broadcast;
use tokio::sync::mpsc;
use tokio_stream::wrappers::BroadcastStream;
use tokio_util::codec::BytesCodec;
use tokio_util::udp::UdpFramed;

use tcptunnel::{to_endpoint, EndPoint};

use structopt::StructOpt;
use webrtc_util::Unmarshal;

/// Splice a udp/rtp input to a multiple outputs
#[derive(Debug, StructOpt)]
struct Opt {
    /// Input source url
    /// It supports the following query parameters
    /// multicast=<ipv4_interface or ipv6_index>
    /// multicast_ttl=<u32> (IPv4-only)
    /// multicast_hops=<u32> (IPv6-only)
    /// buffer=<usize>
    #[structopt(long, short,  parse(try_from_str = to_endpoint))]
    input: Vec<EndPoint>,
    /// Output source
    /// It supports the following query parameters
    /// multicast=<ipv4_interface or ipv6_index>
    /// multicast_ttl=<u32> (IPv4-only)
    /// multicast_hops=<u32> (IPv6-only)
    /// buffer=<usize>
    #[structopt(long, short, parse(try_from_str = to_endpoint))]
    output: Vec<EndPoint>,
    //    /// Verbose logging
    //    #[structopt(long, short)]
    //    verbose: bool,
}

impl Opt {
    fn input_endpoints(&self, m: &MultiProgress) -> anyhow::Result<Vec<mpsc::Receiver<Bytes>>> {
        let mut inputs = Vec::with_capacity(self.input.len());

        for e in &self.input {
            let addr = e.addr;
            let udp = e.setup_udp(e.addr)?;

            let (_sink, udp_stream) = UdpFramed::new(udp, BytesCodec::new()).split();
            let (send, recv) = mpsc::channel(1);

            let mut now = Instant::now();
            let mut size: usize = 0;

            let pb = m.add(ProgressBar::new(!0).with_style(ProgressStyle::default_spinner()));
            pb.println(format!("Input {:?}", e));

            tokio::spawn(async move {
                let read = udp_stream
                    .map_err(anyhow::Error::new)
                    .map_ok(move |(msg, _addr)| {
                        pb.inc(1);
                        pb.set_message(format!(
                            "Input {:?} packet {:?}",
                            addr,
                            Header::unmarshal(&mut msg.clone()).unwrap(),
                        ));
                        let elapsed = now.elapsed();
                        if elapsed > Duration::from_secs(1) {
                            pb.set_message(format!(
                                "Input {:?} bps {:} last packet size {}\r",
                                addr,
                                (size as f32 / elapsed.as_millis() as f32) * 8000f32,
                                msg.len()
                            ));
                            now = Instant::now();
                            size = 0;
                        } else {
                            size += msg.len();
                        }
                        msg.freeze()
                    })
                    .try_for_each(|msg| send.send(msg).map_err(anyhow::Error::new));

                read.await
            });

            inputs.push(recv);
        }

        Ok(inputs)
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let opt = Opt::from_args();

    if opt.input.len() != 1 {
        anyhow::bail!("Only 1 input supported for now");
    }

    let m = Arc::new(MultiProgress::new());

    let mut inputs = opt.input_endpoints(&m)?;

    let (tx, _) = broadcast::channel(1000);

    for e in opt.output {
        let mut now = Instant::now();
        let mut size: usize = 0;
        let udp_addr = e.addr;
        let pb = m.add(ProgressBar::new(!0).with_style(ProgressStyle::default_spinner()));
        pb.println(format!("Output {:?}", e));

        let rx = tx.subscribe();
        let stream = BroadcastStream::new(rx);
        let (sink, _stream) = e.make_output()?.split();
        let sink = sink.sink_map_err(anyhow::Error::new);
        let read = stream
            .map_err(anyhow::Error::new)
            .map_ok(move |msg: Bytes| {
                pb.inc(1);
                pb.set_message(format!(
                    "Output {:?} packet {:?}",
                    udp_addr,
                    Header::unmarshal(&mut msg.clone()).unwrap(),
                ));
                let elapsed = now.elapsed();
                if elapsed > Duration::from_secs(1) {
                    pb.set_message(format!(
                        "Output {:?} bps {:} last packet size {}\r",
                        udp_addr,
                        (size as f32 / elapsed.as_millis() as f32) * 8000f32,
                        msg.len()
                    ));
                    now = Instant::now();
                    size = 0;
                } else {
                    size += msg.len();
                }
                (msg, udp_addr)
            });

        tokio::spawn(async move { read.forward(sink).await });
    }

    eprintln!("COUNT {}", tx.receiver_count());

    tokio::spawn(async move {
        while let Some(msg) = inputs[0].recv().await {
            tx.send(msg).unwrap();
        }
    });

    tokio::spawn(async move {
        m.join_and_clear().unwrap();
    })
    .await?;

    //    tokio::signal::ctrl_c().await?;

    Ok(())
}
