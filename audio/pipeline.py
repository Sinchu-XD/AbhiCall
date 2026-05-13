"""
audio/pipeline.py
-----------------
FFmpeg se audio read karke Opus frames produce karta hai.
WebRTC ko raw Opus frames chahiye SRTP mein send karne ke liye.
"""

import asyncio
import subprocess
import threading
import queue
import logging

import av

logger = logging.getLogger(__name__)

SAMPLE_RATE   = 48000
CHANNELS      = 2
FRAME_SAMPLES = 960     # 20ms @ 48kHz
BITRATE       = 128_000 # 128 kbps


class AudioPipeline:
    """
<<<<<<< HEAD
    Audio source (direct CDN stream URL) ko Opus frames mein convert karta hai.

    Flow:
        stream URL (from resolver)
=======
    Audio source ko Opus frames mein convert karta hai.

    Flow:
        URL / File
>>>>>>> 8a23c75f2cef4ab6f1dc4ced82d2529668f0e1a5
           ↓  FFmpeg (subprocess)
        PCM s16le 48kHz stereo
           ↓  PyAV Opus encoder
        Opus frames (20ms each)
           ↓
        WebRTCEngine → Telegram Group Call
    """

    def __init__(self, source: str, ffmpeg_path: str = "ffmpeg"):
        self.source      = source
        self.ffmpeg_path = ffmpeg_path
        self._proc       = None
        self._thread     = None
        self._queue      = queue.Queue(maxsize=50)
        self._stop_event = threading.Event()
        self._codec_ctx  = None

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def start(self):
        """FFmpeg subprocess start karo aur background thread mein read karo."""
        self._setup_encoder()
        self._proc = self._start_ffmpeg()
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._read_loop, daemon=True)
        self._thread.start()
<<<<<<< HEAD
        logger.info(f"Audio pipeline started: {self.source[:60]}...")
=======
        logger.info(f"Audio pipeline started: {self.source}")
>>>>>>> 8a23c75f2cef4ab6f1dc4ced82d2529668f0e1a5

    def stop(self):
        """Pipeline band karo."""
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
<<<<<<< HEAD
        """Ek Opus frame lo (blocking with timeout)."""
=======
        """
        Ek Opus frame lo (blocking with timeout).
        Returns: bytes | None
        """
>>>>>>> 8a23c75f2cef4ab6f1dc4ced82d2529668f0e1a5
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

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _setup_encoder(self):
<<<<<<< HEAD
=======
        """PyAV Opus encoder initialize karo."""
>>>>>>> 8a23c75f2cef4ab6f1dc4ced82d2529668f0e1a5
        codec             = av.CodecContext.create("libopus", "w")
        codec.sample_rate = SAMPLE_RATE
        codec.channels    = CHANNELS
        codec.format      = av.AudioFormat("s16")
        codec.bit_rate    = BITRATE
        codec.open()
        self._codec_ctx = codec

    def _start_ffmpeg(self) -> subprocess.Popen:
<<<<<<< HEAD
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
=======
        """
        FFmpeg process start karo:
          - kisi bhi source (URL, file) ko accept kare
          - PCM s16le 48kHz stereo mein convert kare
          - stdout mein stream kare
        """
        cmd = [
            self.ffmpeg_path,
            "-reconnect",            "1",
            "-reconnect_streamed",   "1",
            "-reconnect_delay_max",  "5",
            "-i",                    self.source,
            "-vn",
            "-acodec",               "pcm_s16le",
            "-ar",                   str(SAMPLE_RATE),
            "-ac",                   str(CHANNELS),
            "-f",                    "s16le",
            "pipe:1",
            "-loglevel",             "error",
        ]
        return subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

    def _read_loop(self):
        """Background thread: PCM read karo → Opus encode karo → queue mein daalo."""
        bytes_per_frame = FRAME_SAMPLES * CHANNELS * 2  # s16le = 2 bytes/sample
>>>>>>> 8a23c75f2cef4ab6f1dc4ced82d2529668f0e1a5

        while not self._stop_event.is_set():
            raw = self._proc.stdout.read(bytes_per_frame)
            if not raw:
                logger.info("FFmpeg stream ended.")
                break
            if len(raw) < bytes_per_frame:
                raw += b"\x00" * (bytes_per_frame - len(raw))
<<<<<<< HEAD
=======

>>>>>>> 8a23c75f2cef4ab6f1dc4ced82d2529668f0e1a5
            try:
                opus_bytes = self._encode_frame(raw)
                if opus_bytes:
                    self._queue.put(opus_bytes, timeout=1)
            except Exception as e:
                logger.warning(f"Encode error: {e}")

        self._stop_event.set()

    def _encode_frame(self, pcm_bytes: bytes) -> bytes | None:
<<<<<<< HEAD
        frame             = av.AudioFrame(format="s16", layout="stereo", samples=FRAME_SAMPLES)
        frame.sample_rate = SAMPLE_RATE
        frame.planes[0].update(pcm_bytes)
=======
        """PCM bytes → Opus frame."""
        frame             = av.AudioFrame(format="s16", layout="stereo", samples=FRAME_SAMPLES)
        frame.sample_rate = SAMPLE_RATE
        frame.planes[0].update(pcm_bytes)

>>>>>>> 8a23c75f2cef4ab6f1dc4ced82d2529668f0e1a5
        packets = self._codec_ctx.encode(frame)
        if packets:
            return bytes(packets[0])
        return None


# -----------------------------------------------------------------------
<<<<<<< HEAD
# Queue Manager — song dicts (resolver format) store karta hai
# -----------------------------------------------------------------------

class QueueManager:
    """
    Song queue manage karta hai.
    Songs resolver ke format mein hote hain:
    {title, url, stream, duration_text, channel, thumb, is_video, requested_by, ...}
    """

    def __init__(self):
        self._songs:    list[dict]           = []
        self._current:  dict | None          = None
        self._pipeline: AudioPipeline | None = None

    # ------------------------------------------------------------------
    # Song management
    # ------------------------------------------------------------------

    def add_song(self, song: dict):
        """Resolver se aaya hua song dict queue mein daalo."""
        self._songs.append(song)

    def set_current(self, song: dict):
        """Current playing song set karo."""
        self._current = song

    def skip(self) -> dict | None:
        """Current song skip karo, queue se agla lo."""
=======
# Queue Manager
# -----------------------------------------------------------------------

class QueueManager:
    """Song queue manage karta hai — playlist, skip, pause, resume."""

    def __init__(self):
        self._songs:    list[dict]            = []
        self._current:  dict | None           = None
        self._pipeline: AudioPipeline | None  = None

    def add(self, title: str, url: str, requested_by: int):
        self._songs.append({"title": title, "url": url, "requested_by": requested_by})

    def next(self) -> dict | None:
>>>>>>> 8a23c75f2cef4ab6f1dc4ced82d2529668f0e1a5
        if self._pipeline:
            self._pipeline.stop()
            self._pipeline = None
        if self._songs:
            self._current = self._songs.pop(0)
            return self._current
        self._current = None
        return None

<<<<<<< HEAD
    def stop(self):
        """Sab band karo, queue saaf karo."""
=======
    def skip(self) -> dict | None:
        return self.next()

    def stop(self):
>>>>>>> 8a23c75f2cef4ab6f1dc4ced82d2529668f0e1a5
        if self._pipeline:
            self._pipeline.stop()
            self._pipeline = None
        self._songs.clear()
        self._current = None

<<<<<<< HEAD
    # ------------------------------------------------------------------
    # Pipeline
    # ------------------------------------------------------------------

    def start_pipeline(self, stream_url: str) -> AudioPipeline:
        """Pehle wala band karo, naya pipeline start karo."""
        if self._pipeline:
            self._pipeline.stop()
        pipeline = AudioPipeline(stream_url)
=======
    def start_pipeline(self, url: str) -> AudioPipeline:
        pipeline = AudioPipeline(url)
>>>>>>> 8a23c75f2cef4ab6f1dc4ced82d2529668f0e1a5
        pipeline.start()
        self._pipeline = pipeline
        return pipeline

<<<<<<< HEAD
    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

=======
>>>>>>> 8a23c75f2cef4ab6f1dc4ced82d2529668f0e1a5
    @property
    def current(self) -> dict | None:
        return self._current

    @property
    def queue(self) -> list[dict]:
        return self._songs.copy()

    @property
    def pipeline(self) -> AudioPipeline | None:
        return self._pipeline
