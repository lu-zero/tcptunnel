use std::collections::VecDeque;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;

use bytes::Bytes;
use futures::{SinkExt, StreamExt, TryFutureExt, TryStreamExt};
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use rtp::header::Header;
use tokio::runtime::Runtime;
use tokio::sync::broadcast;
use tokio::sync::mpsc;
use tokio::time::timeout;
use tokio_stream::wrappers::BroadcastStream;
use tokio_util::codec::BytesCodec;
use tokio_util::udp::UdpFramed;
use tracing::Instrument;
use webrtc_util::Unmarshal;

use tcptunnel::{to_endpoint, EndPoint};

use structopt::StructOpt;

/// Bound multiple udp/rtp input to a single output
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
    fn input_endpoints(
        &self,
        m: &MultiProgress,
        rt: &Runtime,
    ) -> anyhow::Result<Vec<mpsc::Receiver<Bytes>>> {
        let _guard = rt.enter();
        let (senders, receivers): (Vec<mpsc::Sender<Bytes>>, Vec<mpsc::Receiver<Bytes>>) =
            self.input.iter().map(|_| mpsc::channel(1)).unzip();

        for (e, send) in self.input.iter().zip(senders) {
            let addr = e.addr;
            let udp = e.setup_udp(e.addr)?;

            let (_sink, udp_stream) = UdpFramed::new(udp, BytesCodec::new()).split();

            let mut now = Instant::now();
            let mut size: usize = 0;

            let pb = m.add(ProgressBar::new(!0).with_style(ProgressStyle::default_spinner()));
            pb.println(format!("Input {:?}", e));

            tokio::spawn(async move {
                let read = udp_stream
                    .map_err(|e| {
                        tracing::error!("Error {}", e);
                        anyhow::Error::new(e)
                    })
                    .map_ok(move |(msg, _addr)| {
                        tracing::info!("got from {:?}", addr);
                        let elapsed = now.elapsed();
                        if elapsed > Duration::from_secs(1) {
                            pb.inc(size as u64);
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

                read.instrument(tracing::info_span!("UDP Reader")).await
            });
        }

        Ok(receivers)
    }
}

#[derive(PartialEq, Eq)]
struct HeaderAndData(Header, Bytes);

use std::hash::{Hash, Hasher};

impl Hash for HeaderAndData {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.1.hash(state);
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let opt = Opt::from_args();

    if opt.output.len() != 1 {
        anyhow::bail!("Only 1 output supported for now");
    }

    let m = Arc::new(MultiProgress::new());

    let in_rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(4)
        .build()
        .unwrap();

    let mut inputs = opt.input_endpoints(&m, &in_rt)?;

    let (tx, _rx) = broadcast::channel(1000);

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
            .map_err(|e| {
                tracing::error!("Error {}", e);
                anyhow::Error::new(e)
            })
            .map_ok(move |msg: Bytes| {
                pb.inc(1);
                let mut pkt = msg.clone();
                if let Ok(header) = Header::unmarshal(&mut pkt) {
                    pb.set_message(format!(
                        "Out seq {} ts {}",
                        header.sequence_number, header.timestamp
                    ));
                    // eprintln!("OUT seq {} ts {}", header.sequence_number, header.timestamp);
                }
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

        tokio::spawn(async move {
            read.forward(sink)
                .instrument(tracing::info_span!("UDP Writer"))
                .await
        });
    }

    // wait until
    tokio::time::sleep(Duration::from_millis(200)).await;

    tokio::spawn(async move {
        let limit = 100;
        let mut seen = VecDeque::with_capacity(limit);
        'out: loop {
            for input in inputs.iter_mut() {
                if let Ok(out) = timeout(Duration::from_millis(1), input.recv()).await {
                    if let Some(msg) = out {
                        let mut pkt = msg.clone();
                        if let Ok(header) = Header::unmarshal(&mut pkt) {
                            let key = (header.sequence_number, header.timestamp);
                            if !seen.contains(&key) {
                                tx.send(msg).unwrap();
                                if seen.len() == limit {
                                    let _ = seen.pop_front();
                                }
                                seen.push_back(key);
                            }
                        } else {
                            // todo fail clearly
                            break 'out;
                        }
                    } else {
                        break 'out;
                    }
                }
            }
        }
    });

    m.join_and_clear().unwrap();

    tokio::signal::ctrl_c().await?;

    Ok(())
}
