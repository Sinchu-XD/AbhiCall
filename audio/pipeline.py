import subprocess
import threading
import queue
import logging

logger = logging.getLogger(__name__)

SAMPLE_RATE   = 48000
CHANNELS      = 2
FRAME_SAMPLES = 960
BYTES_PER_FRAME = FRAME_SAMPLES * CHANNELS * 2  # s16le = 2 bytes/sample


class AudioPipeline:

    def __init__(self, source: str, ffmpeg_path: str = "ffmpeg"):
        self.source      = source
        self.ffmpeg_path = ffmpeg_path
        self._proc       = None
        self._thread     = None
        self._queue      = queue.Queue(maxsize=50)
        self._stop_event = threading.Event()

    def start(self):
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

    def _start_ffmpeg(self) -> subprocess.Popen:
        cmd = [
            self.ffmpeg_path,
            "-reconnect",           "1",
            "-reconnect_streamed",  "1",
            "-reconnect_delay_max", "5",
            "-i",                   self.source,
            "-vn",
            "-acodec",              "pcm_s16le",   # raw PCM — aiortc khud encode karega
            "-ar",                  str(SAMPLE_RATE),
            "-ac",                  str(CHANNELS),
            "-f",                   "s16le",
            "pipe:1",
            "-loglevel",            "error",
        ]
        return subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    def _read_loop(self):
        while not self._stop_event.is_set():
            raw = self._proc.stdout.read(BYTES_PER_FRAME)
            if not raw:
                logger.info("FFmpeg stream ended.")
                break
            if len(raw) < BYTES_PER_FRAME:
                raw += b"\x00" * (BYTES_PER_FRAME - len(raw))
            try:
                self._queue.put(raw, timeout=1)   # raw PCM bytes, no encoding
            except queue.Full:
                pass

        self._stop_event.set()


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
