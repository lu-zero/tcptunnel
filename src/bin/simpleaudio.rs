use std::convert::TryInto;
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr};
use std::time::Duration;

use anyhow::{anyhow, Result};
use audiopus::{Channels, SampleRate, TryFrom};
use bytes::Bytes;
use clap::{ArgEnum, Args, Parser, Subcommand};
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

impl Codec {
    fn samples(&self, audio: &AudioOpt) -> usize {
        match self {
            Codec::Opus => audio.sample_rate as usize * 20 * audio.channels as usize / 1000,
            Codec::Pcm => MAX_PACKET / 2,
        }
    }
}

#[derive(Debug, Args)]
struct AudioOpt {
    /// The input or output audio device to use
    #[clap(long, short, default_value = "default", global = true)]
    audio_device: String,

    /// The audio sample rate
    #[clap(long, short, default_value = "48000", global = true)]
    sample_rate: u32,

    /// The number of audio channels
    #[clap(long, short, default_value = "1", global = true)]
    channels: u16,

    /// The size of the audio buffer in use in samples
    #[clap(long, short, default_value = "500", global = true)]
    buffer: u32,
}

impl AudioOpt {
    fn input<F>(&self, cb: F) -> Result<cpal::Stream>
    where
        F: FnMut(&[i16], &cpal::InputCallbackInfo) + Send + 'static,
    {
        let device = input_device(&self.audio_device)?;
        let config = device.default_input_config()?;

        info!("Buffer size {:?}", config.buffer_size());

        let mut config: cpal::StreamConfig = config.into();

        config.sample_rate = cpal::SampleRate(self.sample_rate);
        config.channels = self.channels;
        config.buffer_size = cpal::BufferSize::Fixed(self.buffer);

        info!("Audio configuration {:?}", config);

        let stream = device.build_input_stream(&config, cb, err_cb)?;

        Ok(stream)
    }

    fn output<F>(&self, cb: F) -> Result<cpal::Stream>
    where
        F: FnMut(&mut [i16], &cpal::OutputCallbackInfo) + Send + 'static,
    {
        let device = output_device(&self.audio_device)?;
        let config = device.default_output_config()?;

        info!("Buffer size {:?}", config.buffer_size());

        let mut config: cpal::StreamConfig = config.into();

        config.sample_rate = cpal::SampleRate(self.sample_rate);
        config.channels = self.channels;
        config.buffer_size = cpal::BufferSize::Fixed(self.buffer);

        info!("Audio configuration {:?}", config);

        let stream = device.build_output_stream(&config, cb, err_cb)?;

        Ok(stream)
    }

    /// Amount of samples from ms
    fn prebuffering(&self, ms_prebuffering: usize) -> usize {
        self.sample_rate as usize * ms_prebuffering * self.channels as usize / 1000
    }
}

#[derive(Debug, Args)]
struct EncoderOpt {
    /// Select a codec
    #[clap(long, default_value = "opus", arg_enum)]
    codec: Codec,

    /// Use cbr (by default vbr is used when available)
    #[clap(long)]
    cbr: bool,

    /// Set the target bitrate
    #[clap(long)]
    bitrate: Option<i32>,
}

impl EncoderOpt {
    fn encoder(&self, audio: &AudioOpt) -> Result<Box<dyn Encoder>> {
        let enc = match self.codec {
            Codec::Opus => {
                let mut enc = audiopus::coder::Encoder::new(
                    SampleRate::try_from(audio.sample_rate.try_into()?)?,
                    Channels::try_from(audio.channels.try_into()?)?,
                    audiopus::Application::Audio,
                )?;
                enc.set_vbr(!self.cbr)?;
                if let Some(bitrate) = self.bitrate {
                    enc.set_bitrate(audiopus::Bitrate::BitsPerSecond(bitrate))?;
                }
                Box::new(enc) as Box<dyn Encoder>
            }
            Codec::Pcm => Box::new(Pcm()) as Box<dyn Encoder>,
        };
        Ok(enc)
    }
}

#[derive(Debug, Args)]
struct DecoderOpt {
    /// Select a codec
    #[clap(long, default_value = "opus", arg_enum)]
    codec: Codec,
}

impl DecoderOpt {
    fn decoder(&self, audio: &AudioOpt) -> Result<Box<dyn Decoder>> {
        let dec = match self.codec {
            Codec::Opus => {
                let dec = audiopus::coder::Decoder::new(
                    SampleRate::try_from(audio.sample_rate.try_into()?)?,
                    Channels::try_from(audio.channels.try_into()?)?,
                )?;

                Box::new(dec) as Box<dyn Decoder>
            }
            Codec::Pcm => Box::new(Pcm()) as Box<dyn Decoder>,
        };

        Ok(dec)
    }
}

#[derive(Debug, Args)]
struct Playback {
    /// Input source url
    /// It supports the following query parameters
    /// multicast=<ipv4_interface or ipv6_index>
    /// multicast_ttl=<u32> (IPv4-only)
    /// multicast_hops=<u32> (IPv6-only)
    /// buffer=<usize>
    #[clap(long, short,  parse(try_from_str = to_endpoint))]
    input: EndPoint,

    #[clap(flatten)]
    codec: DecoderOpt,

    /// Prebuffering
    #[clap(long, default_value = "0")]
    prebuffering: usize,
}

impl Playback {
    fn run(self, audio: &AudioOpt) -> Result<()> {
        let rt = Runtime::new().unwrap();

        // 20ms frames
        let samples = self.codec.codec.samples(audio);
        let prebuffering = audio.prebuffering(self.prebuffering);

        // The channel to share samples between the codec and the audio device
        let (audio_send, audio_recv) = flume::bounded(samples * 20 + prebuffering);
        // The channel to share packets between the codec and the network
        let (net_send, net_recv) = flume::bounded::<Bytes>(4);

        for _ in 0..prebuffering {
            let _ = audio_send.send(0);
        }

        let output_cb = move |data: &mut [i16], _: &cpal::OutputCallbackInfo| {
            let mut input_fell_behind = false;
            debug!("Writing audio data in a {} buffer", data.len());
            for sample in data {
                *sample = match audio_recv.try_recv().ok() {
                    Some(s) => s,
                    None => {
                        input_fell_behind = true;
                        0
                    }
                };
            }
            if input_fell_behind {
                warn!("decoding fell behind!!");
            }
        };

        let audio_stream = audio.output(output_cb)?;

        let mut dec = self.codec.decoder(&audio)?;

        std::thread::spawn(move || {
            let mut buf = vec![0i16; samples];
            let mut fell_behind = false;
            let mut print = 0;
            for packet in net_recv.iter() {
                let packet = packet.as_ref();
                debug!("Received packet of {}", packet.len());
                match dec.decode(packet, &mut buf) {
                    Ok(size) => {
                        if print % 25u32 == 0 {
                            info!(
                                "Decoded {}/{} from {} capacity {}",
                                buf.len(),
                                size,
                                packet.len(),
                                audio_send.len()
                            );
                        }
                        print = print.wrapping_add(1);
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

        rt.block_on(async move { udp_input(&self.input, &net_send).await })?;

        Ok(())
    }
}

#[derive(Debug, Args)]
struct Record {
    /// Output sink url
    /// It supports the following query parameters
    /// multicast=<ipv4_interface or ipv6_index>
    /// multicast_ttl=<u32> (IPv4-only)
    /// multicast_hops=<u32> (IPv6-only)
    /// buffer=<usize>
    #[clap(long, short, parse(try_from_str = to_endpoint))]
    output: EndPoint,

    #[clap(flatten)]
    codec: EncoderOpt,
}

impl Record {
    fn run(&self, audio: &AudioOpt) -> Result<()> {
        let rt = Runtime::new().unwrap();

        // 20ms frames
        let samples = self.codec.codec.samples(audio);

        // The channel to share samples between the codec and the audio device
        let (mut audio_send, mut audio_recv) = ringbuf::RingBuffer::new(samples * 20).split();
        // The channel to share packets between the codec and the network
        let (net_send, net_recv) = flume::bounded::<Bytes>(4);

        let input_cb = move |data: &[i16], _: &cpal::InputCallbackInfo| {
            let mut fell_behind = false;
            debug!("Sending audio buffer of {}", data.len());
            for &sample in data {
                if audio_send.push(sample).is_err() {
                    fell_behind = true;
                }
            }
            if fell_behind {
                warn!("encoding fell behind!!");
            }
        };

        let audio_stream = audio.input(input_cb)?;

        let mut enc = self.codec.encoder(&audio)?;

        audio_stream.play()?;

        std::thread::spawn(move || {
            let mut buf = vec![0i16; samples];
            let mut out = [0u8; MAX_PACKET];
            let mut print = 0u32;
            loop {
                for sample in buf.iter_mut() {
                    loop {
                        match audio_recv.pop() {
                            Some(s) => {
                                *sample = s;
                                break;
                            }
                            None => {
                                std::thread::sleep(Duration::from_millis(1));
                            }
                        }
                    }
                }

                debug!("Copied samples {} left in the queue", audio_recv.len());
                match enc.encode(&buf, &mut out) {
                    Ok(size) => {
                        if print % 25 == 0 {
                            info!("Encoded {} to {}", buf.len(), size);
                        }
                        print = print.wrapping_add(1);
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

        rt.block_on(async move { udp_output(&self.output, &net_recv).await })?;

        Ok(())
    }
}

#[derive(Debug, Subcommand)]
enum Cmd {
    /// Receive from UDP, decode and playback through the audio device
    Playback(Playback),
    /// Record from the audio device, encode and send over UDP
    Record(Record),
}

/// Capture from an audio device and stream to udp or
/// listen to udp and output to an audio device
#[derive(Debug, Parser)]
struct Opt {
    #[clap(flatten)]
    audio: AudioOpt,

    /// Verbose logging
    #[clap(long, short, global = true)]
    verbose: bool,

    #[clap(subcommand)]
    command: Cmd,
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
    fn decode(&mut self, packet: &[u8], buf: &mut [i16]) -> anyhow::Result<usize>;
}

trait Encoder: Send {
    fn encode(&mut self, buf: &[i16], packet: &mut [u8]) -> anyhow::Result<usize>;
}

impl Decoder for audiopus::coder::Decoder {
    fn decode(&mut self, packet: &[u8], buf: &mut [i16]) -> anyhow::Result<usize> {
        let size = self.decode(Some(packet), buf, false)?;

        Ok(size)
    }
}

impl Encoder for audiopus::coder::Encoder {
    fn encode(&mut self, buf: &[i16], packet: &mut [u8]) -> anyhow::Result<usize> {
        let size = audiopus::coder::Encoder::encode(self, buf, packet)?;

        Ok(size)
    }
}

// TODO: validate input
struct Pcm();

impl Decoder for Pcm {
    fn decode(&mut self, packet: &[u8], buf: &mut [i16]) -> anyhow::Result<usize> {
        // TODO use array_chunks
        for (b, p) in buf.iter_mut().zip(packet.chunks(2)) {
            *b = i16::from_be_bytes(p.try_into()?);
        }

        Ok(buf.len().min(packet.len() / 4))
    }
}

impl Encoder for Pcm {
    fn encode(&mut self, buf: &[i16], packet: &mut [u8]) -> anyhow::Result<usize> {
        for (p, b) in packet.chunks_mut(2).zip(buf.iter()) {
            p.copy_from_slice(&b.to_be_bytes());
        }
        Ok(buf.len().min(packet.len() / 2) * 2)
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
                EnvFilter::try_new("info")
            }
        })
        .unwrap();

    tracing_subscriber::fmt()
        .with_env_filter(filter_layer)
        .init();

    match opt.command {
        Cmd::Playback(input) => input.run(&opt.audio),
        Cmd::Record(output) => output.run(&opt.audio),
    }
}
