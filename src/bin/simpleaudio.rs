use std::convert::TryInto;
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr};
use std::time::Duration;

use anyhow::{anyhow, Result};
use audiopus::{Channels, SampleRate, TryFrom};
use bytes::Bytes;
use clap::{ArgEnum, Args, Parser, Subcommand};
use cpal::traits::{DeviceTrait, HostTrait, StreamTrait};
use cpal::Device;
use flume::Receiver;
use futures::future::ready;
use futures::stream::{StreamExt, TryStreamExt};
use speexdsp::resampler::*;
use tokio::runtime::Builder;
use tokio_util::codec::BytesCodec;
use tokio_util::udp::UdpFramed;

use tcptunnel::{loudnorm, to_endpoint, EndPoint};
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

fn dbfs(v: i16) -> f32 {
    let v = if v == 0 {
        0.0
    } else {
        20.0 * (v.abs() as f32).log10()
    };

    v - 90.31
}

#[derive(Debug, Args)]
struct AudioOpt {
    /// The input or output audio device to use
    #[clap(long, short, global = true)]
    audio_device: Option<String>,

    /// The audio sample rate
    #[clap(long, short, default_value = "48000", global = true)]
    sample_rate: u32,

    /// The number of audio channels
    #[clap(long, short, default_value = "1", global = true)]
    channels: u16,

    /// The size of the audio buffer in use in samples
    #[clap(long, short, default_value = "500", global = true)]
    buffer: u32,

    /// Bind the audio thread to a specific core
    #[clap(long, short = 'p', global = true)]
    cpu: Option<usize>,

    /// size of the feeding buffer in ms
    #[clap(long, default_value = "80", global = true)]
    feeding_buffer: usize,

    /// Insert a loudness normalization filter (Increases the latency by 300ms)
    #[clap(long, global = true)]
    normalize: bool,
}

impl AudioOpt {
    fn input<F>(&self, cb: F) -> Result<cpal::Stream>
    where
        F: FnMut(&[i16], &cpal::InputCallbackInfo) + Send + 'static,
    {
        let device = input_device(
            self.audio_device
                .as_ref()
                .expect("Capturing requires a device"),
        )?;
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
        if let Some(ref audio_device) = self.audio_device {
            let device = output_device(audio_device)?;
            let config = device.default_output_config()?;

            info!("Buffer size {:?}", config.buffer_size());

            let mut config: cpal::StreamConfig = config.into();

            config.sample_rate = cpal::SampleRate(self.sample_rate);
            config.channels = self.channels;
            config.buffer_size = cpal::BufferSize::Fixed(self.buffer);

            info!("Audio configuration {:?}", config);

            let stream = device.build_output_stream(&config, cb, err_cb)?;

            Ok(stream)
        } else {
            unreachable!()
        }
    }

    /// Amount of samples from ms
    fn samples_from_ms(&self, ms_prebuffering: usize) -> usize {
        self.sample_rate as usize * ms_prebuffering * self.channels as usize / 1000
    }

    /// Create an idempotent resampler
    fn resampler(&self) -> Result<State> {
        State::new(
            self.channels as usize,
            self.sample_rate as usize,
            self.sample_rate as usize,
            4,
        )
        .map_err(|_| anyhow!("Cannot setup the resampler"))
    }

    /// Create a loudness normalization filter
    fn norm(&self) -> loudnorm::State {
        let settings = loudnorm::Settings::default();

        loudnorm::State::new(&settings, self.channels as usize, self.sample_rate as usize)
    }
}

#[cfg(target_os = "linux")]
mod affinity {
    use nix::sched::{sched_getaffinity, sched_setaffinity, CpuSet};
    use nix::unistd::Pid;

    pub struct Affinity {
        normal: CpuSet,
        audio: CpuSet,
    }

    impl Affinity {
        pub fn new(core: Option<usize>) -> anyhow::Result<Affinity> {
            let mut normal = sched_getaffinity(Pid::from_raw(0))?;
            let mut audio = CpuSet::new();
            if let Some(core) = core {
                normal.unset(core)?;
                audio.set(core)?;
            }

            Ok(Affinity { normal, audio })
        }
        /// Set the core affinity for the audio threads
        pub fn audio_affinity(&self) {
            let _ = sched_setaffinity(Pid::from_raw(0), &self.audio);
        }

        /// Set the core affinity for the network and encoder threads
        pub fn normal_affinity(&self) {
            let _ = sched_setaffinity(Pid::from_raw(0), &self.normal);
        }
    }
}

#[cfg(not(target_os = "linux"))]
mod affinity {
    pub struct Affinity {}
    impl Affinity {
        pub fn new(_: Option<usize>) -> anyhow::Result<Affinity> {
            Ok(Affinity {})
        }
        /// Set the core affinity for the audio threads
        pub fn audio_affinity(&self) {}

        /// Set the core affinity for the network and encoder threads
        pub fn normal_affinity(&self) {}
    }
}

use affinity::Affinity;
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
}

trait AudioOut {
    fn playback(&mut self, buf: &[i16]);
}

struct NoOp {
    channels: usize,
    print: u32,
}

impl NoOp {
    fn new(channels: usize) -> Self {
        NoOp { channels, print: 0 }
    }

    fn print_stats(&self, buf: &[i16]) {
        info!(
            "Decoded {} {}",
            buf.len(),
            if self.channels == 1 {
                format!("dbfs {:.3}", dbfs(buf[0]))
            } else {
                let r = buf[0];
                let l = buf[1];
                format!("dbfs {:.3} {:.3}", dbfs(r), dbfs(l))
            },
        );
    }
}

impl AudioOut for NoOp {
    fn playback(&mut self, buf: &[i16]) {
        if self.print % 25 == 0 {
            self.print_stats(buf)
        }

        self.print = self.print.wrapping_add(1);
    }
}

struct Direct {
    st: State,
    audio_send: ringbuf::Producer<i16>,
    _audio_stream: cpal::Stream,
    out_buf: Vec<i16>,
    channels: usize,
    print: u32,
}

impl Direct {
    fn new(
        audio: &AudioOpt,
        audio_stream: cpal::Stream,
        audio_send: ringbuf::Producer<i16>,
        samples: usize,
    ) -> Result<Self> {
        let out_buf = vec![0i16; samples * 11 / 10]; // 10% more samples out at most
        let st = audio.resampler()?;
        let channels = audio.channels as usize;
        let input_latency = st.get_input_latency();
        let output_latency = st.get_output_latency();

        info!(
            "Resampler input latency {} output_latency {}",
            input_latency, output_latency
        );

        Ok(Self {
            st,
            audio_send,
            _audio_stream: audio_stream,
            out_buf,
            channels,
            print: 0,
        })
    }

    fn print_stats(&self, buf: &[i16]) {
        let water_level = self.audio_send.len();
        let (in_rate, out_rate) = self.st.get_rate();
        let channels = self.channels;

        info!(
            "Decoded {} pending {} {} resampling {}/{}",
            buf.len(),
            water_level,
            if channels == 1 {
                format!("dbfs {:.3}", dbfs(buf[0]))
            } else {
                let r = buf[0];
                let l = buf[1];
                format!("dbfs {:.3} {:.3}", dbfs(r), dbfs(l))
            },
            in_rate,
            out_rate
        );
    }
}

impl AudioOut for Direct {
    fn playback(&mut self, buf: &[i16]) {
        let buf_len = buf.len();
        let mut fell_behind = false;
        let (consumed, written) = self
            .st
            .process_interleaved_int(buf, &mut self.out_buf)
            .expect("Resampling failed");
        assert_eq!(consumed, buf_len); // It should be always true

        let capacity = self.audio_send.capacity() as isize;

        for &sample in &self.out_buf[..written] {
            if self.audio_send.push(sample).is_err() {
                fell_behind = true;
            }
        }

        let (in_rate, _) = self.st.get_rate();
        let water_level = self.audio_send.capacity();

        let new_rate = (in_rate as isize
            - (in_rate as isize * (water_level as isize * 2 - capacity) / capacity / 100))
            as usize;

        self.st
            .set_rate(in_rate, new_rate)
            .expect("Resampler error");

        if fell_behind {
            warn!("Input stream fell behind!!");
        }

        if self.print % 25 == 0 {
            self.print_stats(buf);
        }

        self.print = self.print.wrapping_add(1);
    }
}

struct LoudNormOut {
    out: Direct,
    loudnorm: loudnorm::State,
    buf: Vec<f64>,
}

impl LoudNormOut {
    fn new(
        audio: &AudioOpt,
        audio_stream: cpal::Stream,
        audio_send: ringbuf::Producer<i16>,
        samples: usize,
    ) -> Result<Self> {
        let out = Direct::new(audio, audio_stream, audio_send, samples)?;
        let loudnorm = audio.norm();
        let buf_len = loudnorm.current_samples_per_frame * out.channels;

        Ok(Self {
            out,
            loudnorm,
            buf: vec![0.0; buf_len],
        })
    }
}

impl AudioOut for LoudNormOut {
    fn playback(&mut self, buf: &[i16]) {
        let max = std::i16::MAX as f64;
        let channels = self.out.channels;
        self.buf.extend(buf.iter().map(|&s| s as f64 / max));

        if self.buf.len() / channels >= self.loudnorm.current_samples_per_frame {
            let rem = self.buf.split_off(self.loudnorm.current_samples_per_frame);
            let out = self.loudnorm.process(&self.buf).expect("loudnorm failed");
            let size = self.out.out_buf.len();
            for chunk in out.chunks(size) {
                let buf = chunk.iter().map(|f| (f * max) as i16).collect::<Vec<_>>();
                self.out.playback(&buf);
            }
            self.buf = rem;
        }
    }
}

impl Playback {
    fn run(self, audio: &AudioOpt) -> Result<()> {
        let af = Affinity::new(audio.cpu)?;

        // 20ms frames for opus, 7ms for PCM
        let samples = self.codec.codec.samples(audio);
        let feeding_ms = if audio.normalize {
            audio.feeding_buffer + 300
        } else {
            audio.feeding_buffer
        };
        let feeding_buffer = audio.samples_from_ms(feeding_ms);
        let prebuffering = feeding_buffer / 2;

        // The channel to share samples between the codec and the audio device
        let (mut audio_send, mut audio_recv) = ringbuf::RingBuffer::new(feeding_buffer).split();

        for _ in 0..prebuffering {
            let _ = audio_send.push(0);
        }

        let output_cb = move |data: &mut [i16], _: &cpal::OutputCallbackInfo| {
            let mut input_fell_behind = false;
            debug!("Writing audio data in a {} buffer", data.len());
            for sample in data {
                *sample = match audio_recv.pop() {
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

        let mut dec = self.codec.decoder(&audio)?;

        async fn udp_input(
            e: &EndPoint,
            out: &mut dyn AudioOut,
            dec: &mut dyn Decoder,
            samples: usize,
        ) -> anyhow::Result<()> {
            let mut buf = vec![0i16; samples];

            let (_sink, stream) = input_endpoint(&e)?.split();

            let map = stream
                .map_err(|e| {
                    tracing::error!("Error {}", e);
                    anyhow::Error::new(e)
                })
                .try_for_each(move |(msg, _addr)| {
                    debug!("Received from network {} data", msg.len());

                    let packet = msg.as_ref();
                    match dec.decode(packet, &mut buf) {
                        Ok(size) => out.playback(&buf[..size]),
                        Err(err) => warn!("Error decoding {}", err),
                    }

                    ready(Ok(()))
                });

            map.await?;

            Ok(())
        }

        af.audio_affinity();
        let mut out = if audio.audio_device.is_some() {
            let audio_stream = audio.output(output_cb)?;
            audio_stream.play()?;

            if audio.normalize {
                let out = LoudNormOut::new(audio, audio_stream, audio_send, samples)?;
                Box::new(out) as Box<dyn AudioOut>
            } else {
                let out = Direct::new(audio, audio_stream, audio_send, samples)?;
                Box::new(out) as Box<dyn AudioOut>
            }
        } else {
            let channels = audio.channels as usize;
            Box::new(NoOp::new(channels)) as Box<dyn AudioOut>
        };

        af.normal_affinity();
        let rt = Builder::new_current_thread().enable_io().build()?;

        rt.block_on(
            async move { udp_input(&self.input, out.as_mut(), dec.as_mut(), samples).await },
        )?;

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
        let af = Affinity::new(audio.cpu)?;
        af.normal_affinity();

        // 20ms frames for opus, 7ms for PCM
        let samples = self.codec.codec.samples(audio);
        let feeding_buffer = audio.samples_from_ms(audio.feeding_buffer);

        // The channel to share samples between the codec and the audio device
        let (mut audio_send, mut audio_recv) = ringbuf::RingBuffer::new(feeding_buffer).split();
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

        let mut enc = self.codec.encoder(&audio)?;

        let channels = audio.channels;
        let join_handle = std::thread::Builder::new()
            .name("simple-audio-encoder".to_owned())
            .spawn(move || {
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
                                info!(
                                    "Encoded {} to {} {}",
                                    buf.len(),
                                    size,
                                    if channels == 1 {
                                        format!("dbfs {:.3}", dbfs(buf[0]))
                                    } else {
                                        let r = buf[0];
                                        let l = buf[1];
                                        format!("dbfs {:.3} {:.3}", dbfs(r), dbfs(l))
                                    }
                                );
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
            })?;

        async fn udp_output(e: &EndPoint, recv: &Receiver<Bytes>) -> anyhow::Result<()> {
            let addr = e.addr;
            let (sink, _stream) = output_endpoint(&e)?.split();

            let read = recv.stream().map(move |msg| Ok((msg, addr)));

            read.forward(sink).await?;

            Ok(())
        }

        af.audio_affinity();
        let audio_stream = audio.input(input_cb)?;
        audio_stream.play()?;

        af.normal_affinity();
        let rt = Builder::new_current_thread().enable_io().build()?;

        rt.block_on(async move { udp_output(&self.output, &net_recv).await })?;

        join_handle.join().unwrap();

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
#[clap(name = "simpleaudio")]
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
        .with_writer(std::io::stderr)
        .init();

    match opt.command {
        Cmd::Playback(input) => input.run(&opt.audio),
        Cmd::Record(output) => output.run(&opt.audio),
    }
}
