use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;

use bytes::Bytes;
use futures::{SinkExt, StreamExt, TryFutureExt, TryStreamExt};
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use priority_queue::DoublePriorityQueue;
use rtp::header::Header;
use tokio::sync::broadcast;
use tokio::sync::mpsc;
use tokio::time::timeout;
use tokio_stream::wrappers::BroadcastStream;
use tokio_util::codec::BytesCodec;
use tokio_util::udp::UdpFramed;
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

                read.await
            });

            inputs.push(recv);
        }

        Ok(inputs)
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
    let opt = Opt::from_args();

    if opt.output.len() != 1 {
        anyhow::bail!("Only 1 output supported for now");
    }

    let m = Arc::new(MultiProgress::new());

    let mut inputs = opt.input_endpoints(&m)?;

    let (tx, _) = broadcast::channel(10);

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

        tokio::spawn(async move { read.forward(sink).await });
    }

    // wait until
    tokio::time::sleep(Duration::from_millis(200)).await;

    tokio::spawn(async move {
        let limit = 100;
        let mut queue = DoublePriorityQueue::with_capacity(limit);
        'out: loop {
            for input in inputs.iter_mut() {
                if let Ok(out) = timeout(Duration::from_millis(10), input.recv()).await {
                    if let Some(msg) = out {
                        let mut pkt = msg.clone();
                        if let Ok(header) = Header::unmarshal(&mut pkt) {
                            /*  eprintln!(
                                "{:p}: Inserting seq {} ts {} in queue len {}",
                                input,
                                header.sequence_number,
                                header.timestamp,
                                queue.len()
                            ); */
                            if let Some((prev_header_and_data, _timestamp)) = queue.peek_min() {
                                let HeaderAndData(prev_header, _) = prev_header_and_data;
                                // wrap around in the timestamps, drain the queue before inserting
                                if header.sequence_number < prev_header.sequence_number
                                    && header.timestamp >= prev_header.timestamp
                                {
                                    eprintln!(
                                        "Wrap around! {} {} {}",
                                        header.timestamp,
                                        prev_header.timestamp,
                                        queue.len()
                                    );

                                    for (HeaderAndData(header, msg), _ts) in
                                        queue.into_sorted_iter()
                                    {
                                        eprintln!(
                                            " Pushing from wrap around out seq {} ts {}",
                                            header.sequence_number, header.timestamp,
                                        );
                                        tx.send(msg).unwrap();
                                    }

                                    queue = DoublePriorityQueue::with_capacity(limit);
                                }
                            }
                            let seq = header.sequence_number;
                            let _ = queue.push(HeaderAndData(header, msg), seq);
                        } else {
                            // todo fail clearly
                            break 'out;
                        }
                    } else {
                        break 'out;
                    }
                } else {
                    // eprintln!("{:p} timed out", input);
                }
            }

            if queue.len() > limit / 2 {
                if let Some((HeaderAndData(_header, msg), _ts)) = queue.pop_max() {
                    tx.send(msg).unwrap();
                }
            }
        }

        for (HeaderAndData(_header, msg), _ts) in queue.into_sorted_iter().rev() {
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
