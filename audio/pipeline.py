import asyncio
import subprocess
import threading
import queue
import logging

import av

logger = logging.getLogger(__name__)

SAMPLE_RATE   = 48000
CHANNELS      = 2
FRAME_SAMPLES = 960
BITRATE       = 128_000


class AudioPipeline:

    def __init__(self, source: str, ffmpeg_path: str = "ffmpeg"):
        self.source      = source
        self.ffmpeg_path = ffmpeg_path
        self._proc       = None
        self._thread     = None
        self._queue      = queue.Queue(maxsize=50)
        self._stop_event = threading.Event()
        self._codec_ctx  = None

    def start(self):
        self._setup_encoder()
        self._proc = self._start_ffmpeg()
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._read_loop, daemon=True)
        self._thread.start()
        logger.info(f"Audio pipeline started: {self.source[:60]}...")

    def stop(self):
        self._stop_event.set()
        if self._proc:
            self._proc.kill()
            self._proc = None
        if self._thread:
            self._thread.join(timeout=2)
        while not self._queue.empty():
            try:
                self._queue.get_nowait()
            except queue.Empty:
                break
        logger.info("Audio pipeline stopped.")

    def get_frame(self, timeout: float = 0.1) -> bytes | None:
        try:
            return self._queue.get(timeout=timeout)
        except queue.Empty:
            return None

    @property
    def is_alive(self) -> bool:
        return (
            not self._stop_event.is_set()
            and self._thread is not None
            and self._thread.is_alive()
        )

    def _setup_encoder(self):
        codec             = av.CodecContext.create("libopus", "w")
        codec.sample_rate = SAMPLE_RATE
        codec.layout      = "stereo"        # ✅ FIX: .channels nahi, .layout use karo
        codec.format      = av.AudioFormat("s16")
        codec.bit_rate    = BITRATE
        codec.open()
        self._codec_ctx = codec

    def _start_ffmpeg(self) -> subprocess.Popen:
        cmd = [
            self.ffmpeg_path,
            "-reconnect",           "1",
            "-reconnect_streamed",  "1",
            "-reconnect_delay_max", "5",
            "-i",                   self.source,
            "-vn",
            "-acodec",              "pcm_s16le",
            "-ar",                  str(SAMPLE_RATE),
            "-ac",                  str(CHANNELS),
            "-f",                   "s16le",
            "pipe:1",
            "-loglevel",            "error",
        ]
        return subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    def _read_loop(self):
        bytes_per_frame = FRAME_SAMPLES * CHANNELS * 2

        while not self._stop_event.is_set():
            raw = self._proc.stdout.read(bytes_per_frame)
            if not raw:
                logger.info("FFmpeg stream ended.")
                break
            if len(raw) < bytes_per_frame:
                raw += b"\x00" * (bytes_per_frame - len(raw))
            try:
                opus_bytes = self._encode_frame(raw)
                if opus_bytes:
                    self._queue.put(opus_bytes, timeout=1)
            except Exception as e:
                logger.warning(f"Encode error: {e}")

        self._stop_event.set()

    def _encode_frame(self, pcm_bytes: bytes) -> bytes | None:
        frame             = av.AudioFrame(format="s16", layout="stereo", samples=FRAME_SAMPLES)
        frame.sample_rate = SAMPLE_RATE
        frame.planes[0].update(pcm_bytes)
        packets = self._codec_ctx.encode(frame)
        if packets:
            return bytes(packets[0])
        return None


class QueueManager:

    def __init__(self):
        self._songs:    list[dict]           = []
        self._current:  dict | None          = None
        self._pipeline: AudioPipeline | None = None

    def add_song(self, song: dict):
        self._songs.append(song)

    def set_current(self, song: dict):
        self._current = song

    def skip(self) -> dict | None:
        if self._pipeline:
            self._pipeline.stop()
            self._pipeline = None
        if self._songs:
            self._current = self._songs.pop(0)
            return self._current
        self._current = None
        return None

    def stop(self):
        if self._pipeline:
            self._pipeline.stop()
            self._pipeline = None
        self._songs.clear()
        self._current = None

    def start_pipeline(self, stream_url: str) -> AudioPipeline:
        if self._pipeline:
            self._pipeline.stop()
        pipeline = AudioPipeline(stream_url)
        pipeline.start()
        self._pipeline = pipeline
        return pipeline

    @property
    def current(self) -> dict | None:
        return self._current

    @property
    def queue(self) -> list[dict]:
        return self._songs.copy()

    @property
    def pipeline(self) -> AudioPipeline | None:
        return self._pipeline
