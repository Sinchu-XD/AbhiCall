"""
webrtc/engine.py — Fixed v2

FIXES:
  1. prepare() creates PC + offer BEFORE JoinGroupCall — real DTLS fingerprint
     extracted here and sent to Telegram (was always empty before).
  2. complete_connect() guards against empty remote fingerprint — if Telegram
     sends no fingerprint (bad parse), raises a clear error instead of letting
     aiortc crash with a bare AssertionError deep in its internals.
  3. Callbacks capture `pc` as closure variable — no more NullPointerError
     when self._pc is set to None during cleanup.
"""

import asyncio
import logging
from fractions import Fraction
from typing import Callable, Optional

from aiortc import (
    RTCPeerConnection,
    RTCSessionDescription,
    RTCConfiguration,
    RTCIceServer,
    MediaStreamTrack,
)
from av import AudioFrame

logger = logging.getLogger(__name__)

SAMPLE_RATE     = 48000
FRAME_SAMPLES   = 960
BYTES_PER_FRAME = FRAME_SAMPLES * 2 * 2   # s16le stereo


class SwitchableAudioTrack(MediaStreamTrack):
    kind = "audio"

    def __init__(self):
        super().__init__()
        self._pipeline  = None
        self._timestamp = 0

    def set_pipeline(self, pipeline):
        self._pipeline = pipeline

    def switch_pipeline(self, new_pipeline):
        self._pipeline = new_pipeline
        logger.info("Audio track pipeline switched.")

    async def recv(self) -> AudioFrame:
        loop = asyncio.get_running_loop()

        if self._pipeline and self._pipeline.is_alive:
            pcm_bytes = await loop.run_in_executor(
                None, lambda: self._pipeline.get_frame(timeout=0.05)
            )
        else:
            pcm_bytes = None

        if pcm_bytes is None:
            await asyncio.sleep(0.02)
            pcm_bytes = b"\x00" * BYTES_PER_FRAME

        frame             = AudioFrame(format="s16", layout="stereo", samples=FRAME_SAMPLES)
        frame.sample_rate = SAMPLE_RATE
        frame.pts         = self._timestamp
        frame.time_base   = Fraction(1, SAMPLE_RATE)
        frame.planes[0].update(pcm_bytes)
        self._timestamp  += FRAME_SAMPLES
        return frame


class WebRTCEngine:

    def __init__(self, stun_url: str = "stun:stun.l.google.com:19302"):
        self.stun_url     = stun_url
        self._pc          = None
        self._track: Optional[SwitchableAudioTrack] = None
        self._connected   = False
        self._on_failed: Optional[Callable] = None

        self._prepared_offer_sdp: Optional[str] = None
        self._prepared_fp:        Optional[str] = None

    def set_reconnect_callback(self, callback: Callable):
        self._on_failed = callback

    async def prepare(self, pipeline) -> tuple[str, str, str]:
        """
        Phase 1 — call BEFORE JoinGroupCall.
        Returns (ufrag, pwd, fingerprint) to send to Telegram.
        """
        if self._pc:
            await self._cleanup_pc()

        config   = RTCConfiguration(iceServers=[RTCIceServer(urls=[self.stun_url])])
        self._pc = RTCPeerConnection(configuration=config)

        self._track = SwitchableAudioTrack()
        self._track.set_pipeline(pipeline)
        self._pc.addTrack(self._track)

        offer = await self._pc.createOffer()
        await self._pc.setLocalDescription(
            RTCSessionDescription(sdp=offer.sdp, type="offer")
        )

        while self._pc.iceGatheringState != "complete":
            await asyncio.sleep(0.1)

        local_sdp   = self._pc.localDescription.sdp
        ufrag, pwd  = self._extract_ice_credentials(local_sdp)
        fingerprint = self._extract_fingerprint(local_sdp)

        self._prepared_offer_sdp = local_sdp
        self._prepared_fp        = fingerprint

        logger.info(f"WebRTC prepared — ufrag: {ufrag}  fingerprint: {fingerprint[:30]}...")
        return ufrag, pwd, fingerprint

    async def complete_connect(self, group_call_params: dict) -> bool:
        """
        Phase 2 — call AFTER JoinGroupCall succeeds.
        Sets remote description to finish the WebRTC handshake.
        """
        if not self._pc or not self._prepared_offer_sdp:
            logger.error("complete_connect() called before prepare()!")
            return False

        transport  = group_call_params.get("transport", {})
        candidates = transport.get("candidates", [])
        fp_list    = transport.get("fingerprints", [])

        # FIX: Catch the case where parsing failed and fingerprints are missing.
        # aiortc crashes with a bare AssertionError without this guard.
        if not fp_list:
            logger.error(
                "Telegram returned no fingerprints in transport params — "
                "JoinGroupCall response was likely not parsed correctly. "
                "Check _parse_join_response logs above."
            )
            return False

        if not candidates:
            logger.warning(
                "Telegram returned 0 ICE candidates — "
                "ICE will likely fail. Check that the VC is active and reachable."
            )

        self._setup_callbacks(self._pc)

        remote_sdp = self._build_remote_sdp(self._prepared_offer_sdp, group_call_params)

        await self._pc.setRemoteDescription(
            RTCSessionDescription(sdp=remote_sdp, type="answer")
        )

        self._connected = True
        logger.info("✅ WebRTC connected to Telegram Group Call!")
        return True

    async def disconnect(self):
        self._connected          = False
        self._prepared_offer_sdp = None
        self._prepared_fp        = None
        await self._cleanup_pc()
        logger.info("WebRTC disconnected.")

    def switch_track(self, new_pipeline):
        if self._track:
            self._track.switch_pipeline(new_pipeline)

    @property
    def is_connected(self) -> bool:
        return self._connected

    async def _cleanup_pc(self):
        if self._track:
            self._track.stop()
            self._track = None
        if self._pc:
            await self._pc.close()
            self._pc = None

    def _extract_ice_credentials(self, sdp: str) -> tuple[str, str]:
        ufrag = ""
        pwd   = ""
        for line in sdp.split("\r\n"):
            if line.startswith("a=ice-ufrag:"):
                ufrag = line[len("a=ice-ufrag:"):]
            elif line.startswith("a=ice-pwd:"):
                pwd   = line[len("a=ice-pwd:"):]
        return ufrag, pwd

    def _extract_fingerprint(self, sdp: str) -> str:
        for line in sdp.split("\r\n"):
            if line.startswith("a=fingerprint:"):
                return line[len("a=fingerprint:"):]
        return ""

    def _setup_callbacks(self, pc: RTCPeerConnection):
        """
        FIX: `pc` is a closure variable, not self._pc.
        Safe even after self._pc is set to None during cleanup.
        """
        @pc.on("connectionstatechange")
        async def on_state():
            try:
                state = pc.connectionState
            except Exception:
                return
            logger.info(f"WebRTC state: {state}")
            if state in ("failed", "closed"):
                self._connected = False
                if self._on_failed:
                    logger.warning("WebRTC failed — triggering reconnect...")
                    asyncio.create_task(self._on_failed())

        @pc.on("iceconnectionstatechange")
        async def on_ice():
            try:
                logger.info(f"ICE state: {pc.iceConnectionState}")
            except Exception:
                pass

    def _build_remote_sdp(self, offer_sdp: str, params: dict) -> str:
        transport    = params.get("transport", {})
        fingerprints = transport.get("fingerprints", [])

        if fingerprints:
            fp_hash  = fingerprints[0].get("hash", "sha-256")
            fp_value = fingerprints[0].get("fingerprint", "")
        else:
            fp_hash  = "sha-256"
            fp_value = ""

        ufrag      = transport.get("ufrag", "telegram")
        pwd        = transport.get("pwd",   "telegram")
        candidates = transport.get("candidates", [])

        sections     = []
        current      = []
        session_done = False

        for line in offer_sdp.split("\r\n"):
            if not line:
                continue
            if line.startswith("m="):
                if session_done:
                    sections.append(current)
                else:
                    session_done = True
                current = [line]
            elif session_done:
                current.append(line)
        if current:
            sections.append(current)

        answer = [
            "v=0",
            "o=- 0 0 IN IP4 127.0.0.1",
            "s=-",
            "t=0 0",
            "a=ice-lite",
            "a=group:BUNDLE 0",
            "a=msid-semantic:WMS *",
        ]

        for section in sections:
            m_line = section[0]

            if "audio" in m_line:
                answer.append(m_line)
                answer.append("c=IN IP4 0.0.0.0")
                answer.append(f"a=ice-ufrag:{ufrag}")
                answer.append(f"a=ice-pwd:{pwd}")
                if fp_value:
                    answer.append(f"a=fingerprint:{fp_hash} {fp_value}")
                answer.append("a=setup:passive")

                for line in section[1:]:
                    if any(line.startswith(p) for p in (
                        "a=rtpmap", "a=fmtp", "a=rtcp-fb",
                        "a=mid", "a=extmap", "a=ice-options",
                    )):
                        answer.append(line)

                answer.append("a=rtcp:9 IN IP4 0.0.0.0")
                answer.append("a=rtcp-mux")
                answer.append("a=rtcp-rsize")
                answer.append("a=sendrecv")

                for c in candidates:
                    answer.append(
                        f"a=candidate:{c.get('foundation', '1')} 1 "
                        f"{c.get('protocol', 'udp')} "
                        f"{c.get('priority', 2130706431)} "
                        f"{c.get('ip', '0.0.0.0')} "
                        f"{c.get('port', 0)} "
                        f"typ {c.get('type', 'host')}"
                    )
                answer.append("a=end-of-candidates")

            else:
                parts    = m_line.split()
                parts[1] = "0"
                answer.append(" ".join(parts))
                answer.append("c=IN IP4 0.0.0.0")
                for line in section[1:]:
                    if line.startswith("a=mid"):
                        answer.append(line)

        return "\r\n".join(answer) + "\r\n"
