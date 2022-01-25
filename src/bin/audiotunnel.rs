use std::convert::TryInto;
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr};

use anyhow::{anyhow, bail, Result};
use audiopus::{Channels, SampleRate, TryFrom};
use bytes::Bytes;
use clap::{ArgEnum, Parser};
use cpal::traits::{DeviceTrait, HostTrait, StreamTrait};
use cpal::Device;
use flume::{Receiver, Sender};
use futures::stream::{StreamExt, TryStreamExt};
use futures::TryFutureExt;
use tokio::runtime::Runtime;
use tokio_util::codec::BytesCodec;
use tokio_util::udp::UdpFramed;

use tcptunnel::{to_endpoint, EndPoint};
use tracing::{debug, info, warn};
use tracing_subscriber::EnvFilter;

#[derive(Debug, Clone, ArgEnum)]
enum Codec {
    /// Opus
    Opus,
    /// Linear PCM
    Pcm,
}

/// Capture from an audio device and stream to udp or
/// listen to udp and output to an audio device
#[derive(Debug, Parser)]
struct Opt {
    /// Input source url
    /// It supports the following query parameters
    /// multicast=<ipv4_interface or ipv6_index>
    /// multicast_ttl=<u32> (IPv4-only)
    /// multicast_hops=<u32> (IPv6-only)
    /// buffer=<usize>
    #[clap(long, short,  parse(try_from_str = to_endpoint))]
    input: Option<EndPoint>,
    /// Output sink url
    /// It supports the following query parameters
    /// multicast=<ipv4_interface or ipv6_index>
    /// multicast_ttl=<u32> (IPv4-only)
    /// multicast_hops=<u32> (IPv6-only)
    /// buffer=<usize>
    #[clap(long, short, parse(try_from_str = to_endpoint))]
    output: Option<EndPoint>,

    /// The input or output audio device to use
    #[clap(long, short, default_value = "default")]
    audio_device: String,

    /// The audio sample rate
    #[clap(long, short, default_value = "48000")]
    sample_rate: u32,

    /// The number of audio channels
    #[clap(long, short, default_value = "1")]
    channels: u16,

    /// The size of the audio buffer in use in samples
    #[clap(long, short, default_value = "500")]
    buffer: u32,

    /// Verbose logging
    #[clap(long, short)]
    verbose: bool,

    /// Select a codec
    #[clap(long, default_value = "opus", arg_enum)]
    codec: Codec,
}

fn input_endpoint(e: &EndPoint) -> anyhow::Result<UdpFramed<BytesCodec>> {
    let udp = e.setup_udp(e.addr)?;

    eprintln!("Input {:#?}", e);

    Ok(UdpFramed::new(udp, BytesCodec::new()))
}

fn output_endpoint(e: &EndPoint) -> anyhow::Result<UdpFramed<BytesCodec>> {
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

fn input_device(dev: &str) -> Result<Device> {
    let host = cpal::default_host();
    let dev = if dev == "default" {
        host.default_input_device()
    } else {
        host.input_devices()?
            .find(|x| x.name().map(|y| y.starts_with(dev)).unwrap_or(false))
    }
    .ok_or_else(|| anyhow!("Cannot find the specified input device"))?;
    info!("Input device {}", dev.name()?);

    Ok(dev)
}

fn output_device(dev: &str) -> Result<Device> {
    let host = cpal::default_host();
    let dev = if dev == "default" {
        host.default_output_device()
    } else {
        host.output_devices()?
            .find(|x| x.name().map(|y| y.starts_with(dev)).unwrap_or(false))
    }
    .ok_or_else(|| anyhow!("Cannot find the specified input device"))?;
    info!("Output device {}", dev.name()?);

    Ok(dev)
}

const MAX_PACKET: usize = 1400;

trait Decoder: Send {
    fn decode(&mut self, packet: &[u8], buf: &mut [f32]) -> anyhow::Result<usize>;
}

trait Encoder: Send {
    fn encode(&mut self, buf: &[f32], packet: &mut [u8]) -> anyhow::Result<usize>;
}

impl Decoder for audiopus::coder::Decoder {
    fn decode(&mut self, packet: &[u8], buf: &mut [f32]) -> anyhow::Result<usize> {
        let size = self.decode_float(Some(packet), buf, false)?;

        Ok(size)
    }
}

impl Encoder for audiopus::coder::Encoder {
    fn encode(&mut self, buf: &[f32], packet: &mut [u8]) -> anyhow::Result<usize> {
        let size = self.encode_float(buf, packet)?;

        Ok(size)
    }
}

// TODO: validate input
struct Pcm();

impl Decoder for Pcm {
    fn decode(&mut self, packet: &[u8], buf: &mut [f32]) -> anyhow::Result<usize> {
        // TODO use array_chunks
        for (b, p) in buf.iter_mut().zip(packet.chunks(4)) {
            *b = f32::from_be_bytes(p.try_into()?);
        }

        Ok(buf.len().min(packet.len() / 4))
    }
}

impl Encoder for Pcm {
    fn encode(&mut self, buf: &[f32], packet: &mut [u8]) -> anyhow::Result<usize> {
        for (p, b) in packet.chunks_mut(4).zip(buf.iter()) {
            p.copy_from_slice(&b.to_be_bytes());
        }
        Ok(buf.len().min(packet.len() / 4) * 4)
    }
}

fn err_cb(err: cpal::StreamError) {
    warn!("Audio error {}", err);
}

fn main() -> Result<()> {
    let opt = Opt::parse();

    let filter_layer = EnvFilter::try_from_default_env()
        .or_else(|_| {
            if opt.verbose {
                EnvFilter::try_new("debug")
            } else {
                EnvFilter::try_new("warn")
            }
        })
        .unwrap();

    tracing_subscriber::fmt()
        .with_env_filter(filter_layer)
        .init();

    if (opt.input.is_some() && opt.output.is_some())
        || (opt.input.is_none() && opt.output.is_none())
    {
        bail!("Either set an input or an output");
    }

    let rt = Runtime::new().unwrap();

    // 20ms frames
    let samples = match opt.codec {
        Codec::Opus => opt.sample_rate as usize * 20 * opt.channels as usize / 1000,
        Codec::Pcm => MAX_PACKET / 4,
    };

    // The channel to share samples between the codec and the audio device
    let (audio_send, audio_recv) = flume::bounded(samples * 20);
    // The channel to share packets between the codec and the network
    let (net_send, net_recv) = flume::bounded::<Bytes>(4);

    if let Some(input) = opt.input {
        let output_cb = move |data: &mut [f32], _: &cpal::OutputCallbackInfo| {
            let mut input_fell_behind = false;
            debug!("Writing audio data in a {} buffer", data.len());
            for sample in data {
                *sample = match audio_recv.try_recv().ok() {
                    Some(s) => s,
                    None => {
                        input_fell_behind = true;
                        0.0
                    }
                };
            }
            if input_fell_behind {
                warn!("decoding fell behind!!");
            }
        };

        let device = output_device(&opt.audio_device)?;
        let mut config: cpal::StreamConfig = device.default_output_config()?.into();

        config.sample_rate = cpal::SampleRate(opt.sample_rate);
        config.channels = opt.channels;
        config.buffer_size = cpal::BufferSize::Fixed(opt.buffer);

        info!("Audio configuration {:?}", config);

        let mut dec = match opt.codec {
            Codec::Opus => {
                let dec = audiopus::coder::Decoder::new(
                    SampleRate::try_from(config.sample_rate.0.try_into()?)?,
                    Channels::try_from(config.channels.try_into()?)?,
                )?;

                Box::new(dec) as Box<dyn Decoder>
            }
            Codec::Pcm => Box::new(Pcm()) as Box<dyn Decoder>,
        };

        let audio_stream = device.build_output_stream(&config, output_cb, err_cb)?;

        std::thread::spawn(move || {
            let mut buf = vec![0f32; samples];
            let mut fell_behind = false;
            for packet in net_recv.iter() {
                let packet = packet.as_ref();
                debug!("Received packet of {}", packet.len());
                match dec.decode(packet, &mut buf) {
                    Ok(size) => {
                        info!(
                            "Decoded {}/{} from {} capacity {}",
                            buf.len(),
                            size,
                            packet.len(),
                            audio_send.len()
                        );
                        for &sample in buf.iter() {
                            if audio_send.try_send(sample).is_err() {
                                fell_behind = true;
                            }
                        }

                        if fell_behind {
                            warn!("Input stream fell behind!!");
                            fell_behind = false;
                        }
                    }
                    Err(err) => warn!("Error decoding {}", err),
                }
            }
        });

        async fn udp_input(e: &EndPoint, send: &Sender<Bytes>) -> anyhow::Result<()> {
            let (_sink, stream) = input_endpoint(&e)?.split();

            let map = stream
                .map_err(|e| {
                    tracing::error!("Error {}", e);
                    anyhow::Error::new(e)
                })
                .try_for_each(move |(msg, _addr)| {
                    debug!("Received from network {} data", msg.len());
                    send.send_async(msg.freeze())
                        .map_err(|e| anyhow::Error::new(e))
                });

            map.await?;

            Ok(())
        }

        audio_stream.play()?;

        rt.block_on(async move { udp_input(&input, &net_send).await })?;
    } else if let Some(output) = opt.output {
        let input_cb = move |data: &[f32], _: &cpal::InputCallbackInfo| {
            let mut fell_behind = false;
            debug!("Sending audio buffer of {}", data.len());
            for &sample in data {
                if audio_send.try_send(sample).is_err() {
                    fell_behind = true;
                }
            }
            if fell_behind {
                warn!("encoding fell behind!!");
            }
        };

        let device = input_device(&opt.audio_device)?;
        let mut config: cpal::StreamConfig = device.default_input_config()?.into();

        config.sample_rate = cpal::SampleRate(opt.sample_rate);
        config.channels = opt.channels;
        config.buffer_size = cpal::BufferSize::Fixed(opt.buffer);

        info!("Audio configuration {:?}", config);

        let mut enc = match opt.codec {
            Codec::Opus => {
                let enc = audiopus::coder::Encoder::new(
                    SampleRate::try_from(config.sample_rate.0.try_into()?)?,
                    Channels::try_from(config.channels.try_into()?)?,
                    audiopus::Application::Audio,
                )?;
                Box::new(enc) as Box<dyn Encoder>
            }
            Codec::Pcm => Box::new(Pcm()) as Box<dyn Encoder>,
        };

        let audio_stream = device.build_input_stream(&config, input_cb, err_cb)?;

        audio_stream.play()?;

        std::thread::spawn(move || {
            let mut buf = vec![0f32; samples];
            let mut out = [0u8; MAX_PACKET];
            let mut fell_behind = false;
            loop {
                for sample in buf.iter_mut() {
                    *sample = match audio_recv.recv().ok() {
                        Some(s) => s,
                        None => {
                            fell_behind = true;
                            0.0
                        }
                    }
                }

                if fell_behind {
                    warn!("Input stream fell behind!!");
                    fell_behind = false;
                }
                debug!("Copied samples {} left in the queue", audio_recv.len());
                match enc.encode(&buf, &mut out) {
                    Ok(size) => {
                        info!("Encoded {} to {}", buf.len(), size);
                        let bytes = Bytes::copy_from_slice(&out[..size]);
                        if net_send.send(bytes).is_err() {
                            warn!("Cannot send to the channel");
                        }
                    }
                    Err(err) => warn!("Error encoding {}", err),
                }
            }
        });

        async fn udp_output(e: &EndPoint, recv: &Receiver<Bytes>) -> anyhow::Result<()> {
            let addr = e.addr;
            let (sink, _stream) = output_endpoint(&e)?.split();

            let read = recv.stream().map(move |msg| Ok((msg, addr)));

            read.forward(sink).await?;

            Ok(())
        }

        rt.block_on(async move { udp_output(&output, &net_recv).await })?;
    }

    Ok(())
}
