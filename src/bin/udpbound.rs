use std::collections::VecDeque;
// use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use futures::{stream, SinkExt, StreamExt, TryFutureExt, TryStreamExt};
// use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use clap::Parser;
use priority_queue::DoublePriorityQueue;
use tokio::sync::broadcast;
use tokio::sync::mpsc;
use tokio_stream::wrappers::{BroadcastStream, ReceiverStream};
use tokio_util::codec::BytesCodec;
use tokio_util::udp::UdpFramed;
use tracing::Instrument;

use tcptunnel::{to_endpoint, EndPoint};

/// Bound multiple udp/rtp input to a single output
#[derive(Debug, Parser)]
#[clap(name = "udpbound")]
struct Opt {
    /// Input source url
    /// It supports the following query parameters
    /// multicast=<ipv4_interface or ipv6_index>
    /// multicast_ttl=<u32> (IPv4-only)
    /// multicast_hops=<u32> (IPv6-only)
    /// buffer=<usize>
    #[clap(long, short, value_parser = to_endpoint)]
    input: Vec<EndPoint>,
    /// Output source
    /// It supports the following query parameters
    /// multicast=<ipv4_interface or ipv6_index>
    /// multicast_ttl=<u32> (IPv4-only)
    /// multicast_hops=<u32> (IPv6-only)
    /// buffer=<usize>
    #[clap(long, short, value_parser = to_endpoint)]
    output: Vec<EndPoint>,
    //    /// Verbose logging
    //    #[clap(long, short)]
    //    verbose: bool,
    #[clap(long, short, default_value = "100")]
    lookbehind: usize,
}

impl Opt {
    fn input_endpoints(
        &self,
        // m: &MultiProgress,
    ) -> anyhow::Result<Vec<ReceiverStream<Bytes>>> {
        let (senders, receivers): (Vec<mpsc::Sender<Bytes>>, Vec<ReceiverStream<Bytes>>) = self
            .input
            .iter()
            .map(|_| {
                let (sender, receiver) = mpsc::channel(1);
                (sender, ReceiverStream::new(receiver))
            })
            .unzip();

        for (e, send) in self.input.iter().zip(senders) {
            let addr = e.addr;
            let udp = e.setup_udp(e.addr)?;

            let udp_stream = UdpFramed::new(udp, BytesCodec::new());

            let mut now = Instant::now();
            let mut size: usize = 0;

            // let pb = m.add(ProgressBar::new(!0).with_style(ProgressStyle::default_spinner()));
            // pb.println(format!("Input {:?}", e));

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
                            // pb.inc(size as u64);
                            // pb.set_message(format!(
                            eprintln!(
                                "Input {:?} bps {:} last packet size {}",
                                addr,
                                (size as f32 / elapsed.as_millis() as f32) * 8000f32,
                                msg.len()
                            );
                            //));
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

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .init();

    let opt = Opt::parse();

    //    let m = Arc::new(MultiProgress::new());

    let mut inputs = opt.input_endpoints(/* &m */)?;

    let (tx, _rx) = broadcast::channel(1000);

    let strip_input_header = opt.input.len() > 1;
    let add_output_header = opt.output.len() > 1;

    for e in opt.output {
        let mut now = Instant::now();
        let mut size: usize = 0;
        let udp_addr = e.addr;

        let rx = tx.subscribe();
        let stream = BroadcastStream::new(rx);
        let (sink, _stream) = e.make_output()?.split();
        let sink = sink.sink_map_err(anyhow::Error::new);
        let mut counter = 0;
        let read = stream
            .map_err(|e| {
                tracing::error!("Error {}", e);
                anyhow::Error::new(e)
            })
            .map_ok(move |msg: Bytes| {
                let elapsed = now.elapsed();
                if elapsed > Duration::from_secs(1) {
                    //pb.set_message(format!(
                    eprintln!(
                        "Output {:?} bps {:} last packet size {}",
                        udp_addr,
                        (size as f32 / elapsed.as_millis() as f32) * 8000f32,
                        msg.len()
                    );
                    //));
                    now = Instant::now();
                    size = 0;
                } else {
                    size += msg.len();
                }

                let msg = if add_output_header {
                    let mut buf = BytesMut::new();
                    buf.put_u64(counter);
                    buf.put(msg);
                    counter = counter.wrapping_add(1);
                    buf.freeze()
                } else {
                    msg
                };

                (msg, udp_addr)
            });

        tokio::spawn(async move {
            read.forward(sink)
                .instrument(tracing::info_span!("UDP Writer"))
                .await
        });
    }

    let lookbehind = opt.lookbehind;

    tokio::spawn(async move {
        let mut seen = VecDeque::with_capacity(lookbehind);
        let mut buf = DoublePriorityQueue::with_capacity(lookbehind);
        let mut index = 0;
        stream::select_all(inputs.iter_mut())
            .for_each(move |mut msg| {
                if strip_input_header {
                    let key = msg.get_u64();
                    if !seen.contains(&key) {
                        if index < key {
                            let _ = buf.push(msg, key);
                            if buf.len() == lookbehind {
                                // Set the index to the last packet in the queue
                                index = *buf.peek_max().unwrap().1 + 1;
                                while let Some((prev, _)) = buf.pop_min() {
                                    tx.send(prev).unwrap();
                                }
                            }
                        } else {
                            // Packet loss or in-order
                            while let Some((prev, _)) = buf.pop_min() {
                                tx.send(prev).unwrap();
                            }
                            index = key + 1;
                            tx.send(msg).unwrap();
                        }
                        if seen.len() == lookbehind {
                            let _ = seen.pop_front();
                        }
                        seen.push_back(key);
                    }
                } else {
                    tx.send(msg).unwrap();
                }

                futures::future::ready(())
            })
            .await;
    });

    // m.join_and_clear().unwrap();

    tokio::signal::ctrl_c().await?;

    Ok(())
}
