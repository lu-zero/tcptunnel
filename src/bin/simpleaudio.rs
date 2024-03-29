use std::convert::TryInto;

use std::time::Duration;

use anyhow::{anyhow, Result};
use audiopus::{Channels, SampleRate, TryFrom};
use bytes::Bytes;
use clap::{Args, Parser, Subcommand, ValueEnum};
use cpal::traits::{DeviceTrait, HostTrait, StreamTrait};
use cpal::Device;
use flume::Receiver;
use futures::future::ready;
use futures::stream::{StreamExt, TryStreamExt};
use ringbuf::HeapProducer as Producer;
use ringbuf::HeapRb as RingBuffer;
use speexdsp::resampler::*;
use tokio::runtime::Builder;

use tcptunnel::{to_endpoint, EndPoint};
use tracing::{debug, info, warn};
use tracing_subscriber::EnvFilter;

#[derive(Debug, Clone, ValueEnum)]
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
        20.0 * (v as f32).abs().log10()
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

        let stream = device.build_input_stream(&config, cb, err_cb, None)?;

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

            let stream = device.build_output_stream(&config, cb, err_cb, None)?;

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
    #[clap(long, default_value = "opus")]
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
    #[clap(long, default_value = "opus")]
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
    #[clap(long, short,  value_parser  = to_endpoint)]
    input: EndPoint,

    #[clap(flatten)]
    codec: DecoderOpt,
}

impl Playback {
    fn run(self, audio: &AudioOpt) -> Result<()> {
        let af = Affinity::new(audio.cpu)?;

        let st = audio.resampler()?;
        let input_latency = st.get_input_latency();
        let output_latency = st.get_output_latency();

        info!(
            "Resampler input latency {} output_latency {}",
            input_latency, output_latency
        );

        // 20ms frames for opus, 7ms for PCM
        let samples = self.codec.codec.samples(audio);
        let feeding_buffer = audio.samples_from_ms(audio.feeding_buffer);
        let prebuffering = feeding_buffer / 2;

        // The channel to share samples between the codec and the audio device
        let (mut audio_send, mut audio_recv) = RingBuffer::new(feeding_buffer).split();

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

        let channels = audio.channels;

        async fn udp_input(
            e: &EndPoint,
            mut audio_send: Option<&mut Producer<i16>>,
            dec: &mut dyn Decoder,
            mut st: State,
            channels: u16,
            samples: usize,
        ) -> anyhow::Result<()> {
            let mut buf = vec![0i16; samples];
            let mut out_buf = vec![0i16; samples * 11 / 10]; // 10% more samples out at most
            let mut fell_behind = false;
            let mut print = 0;

            let stream = e.make_udp_input()?;

            let map = stream
                .map_err(|e| {
                    tracing::error!("Error {}", e);
                    anyhow::Error::new(e)
                })
                .try_for_each(move |(msg, _addr)| {
                    debug!("Received from network {} data", msg.len());

                    let packet = msg.as_ref();
                    match dec.decode(packet, &mut buf) {
                        Ok(size) => {
                            let buf_len = buf.len();
                            let (water_level, in_rate, new_rate) =
                                if let Some(ref mut audio_send) = audio_send {
                                    let (consumed, written) = st
                                        .process_interleaved_int(&buf, &mut out_buf)
                                        .expect("Resampling failed");
                                    assert_eq!(consumed, buf_len); // It should be always true

                                    let capacity = audio_send.capacity() as isize;

                                    for &sample in &out_buf[..written] {
                                        if audio_send.push(sample).is_err() {
                                            fell_behind = true;
                                        }
                                    }

                                    let water_level = audio_send.len() as isize;

                                    let (in_rate, _) = st.get_rate();

                                    let new_rate = in_rate as isize
                                        - (in_rate as isize * (water_level * 2 - capacity)
                                            / capacity
                                            / 100);

                                    st.set_rate(in_rate, new_rate as usize)
                                        .expect("Resampler error");

                                    if fell_behind {
                                        warn!("Input stream fell behind!!");
                                        fell_behind = false;
                                    }
                                    (water_level, in_rate, new_rate)
                                } else {
                                    (0, 0, 0)
                                };

                            if print % 25u32 == 0 {
                                info!(
                                    "Decoded {}/{} from {} decoded pending {} {} resampling {}/{}",
                                    buf.len(),
                                    size,
                                    packet.len(),
                                    water_level,
                                    if channels == 1 {
                                        format!("dbfs {:.3}", dbfs(buf[0]))
                                    } else {
                                        let r = buf[0];
                                        let l = buf[1];
                                        format!("dbfs {:.3} {:.3}", dbfs(r), dbfs(l))
                                    },
                                    in_rate,
                                    new_rate
                                );
                            }
                            print = print.wrapping_add(1);
                        }
                        Err(err) => warn!("Error decoding {}", err),
                    }

                    ready(Ok(()))
                });

            map.await?;

            Ok(())
        }

        af.audio_affinity();
        let _audio_stream = if audio.audio_device.is_some() {
            let audio_stream = audio.output(output_cb)?;
            audio_stream.play()?;
            Some(audio_stream)
        } else {
            None
        };

        af.normal_affinity();
        let rt = Builder::new_current_thread().enable_io().build()?;

        rt.block_on(async move {
            let audio_send = audio.audio_device.as_ref().map(|_| &mut audio_send);
            udp_input(&self.input, audio_send, dec.as_mut(), st, channels, samples).await
        })?;

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
    #[clap(long, short, value_parser = to_endpoint)]
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
        let (mut audio_send, mut audio_recv) = RingBuffer::new(feeding_buffer).split();
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
            let addr = e.get_addr()?;
            let (sink, _stream) = e.make_udp_output()?.split();

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
