"""
webrtc/engine.py — Custom WebRTC engine (aiortc, no PyTgCalls)
"""

import asyncio
import logging
from fractions import Fraction

from aiortc import (
    RTCPeerConnection,
    RTCSessionDescription,
    RTCConfiguration,
    RTCIceServer,
    MediaStreamTrack,
)
from av import AudioFrame

logger = logging.getLogger(__name__)

SAMPLE_RATE   = 48000
FRAME_SAMPLES = 960



class OpusStreamTrack(MediaStreamTrack):
    kind = "audio"

    def __init__(self, pipeline):
        super().__init__()
        self._pipeline  = pipeline
        self._timestamp = 0

    async def recv(self) -> AudioFrame:
        loop = asyncio.get_event_loop()
        pcm_bytes = await loop.run_in_executor(       # ✅ naam bhi badla
            None, lambda: self._pipeline.get_frame(timeout=0.05)
        )
        if pcm_bytes is None:
            pcm_bytes = b"\x00" * (FRAME_SAMPLES * 2 * 2)  # ✅ silence PCM, Opus nahi

        frame             = AudioFrame(format="s16", layout="stereo", samples=FRAME_SAMPLES)
        frame.sample_rate = SAMPLE_RATE
        frame.pts         = self._timestamp
        frame.time_base   = Fraction(1, SAMPLE_RATE)
        frame.planes[0].update(pcm_bytes)             # ✅ YEH LINE MISSING THI — data daalo
        self._timestamp  += FRAME_SAMPLES
        return frame

    def switch_pipeline(self, new_pipeline):
        self._pipeline = new_pipeline
        logger.info("Audio track pipeline switched.")


class WebRTCEngine:

    def __init__(self, stun_url: str = "stun:stun.l.google.com:19302"):
        self.stun_url   = stun_url
        self._pc        = None
        self._track     = None
        self._connected = False

    async def connect(self, group_call_params: dict, pipeline) -> dict:
        config   = RTCConfiguration(
            iceServers=[RTCIceServer(urls=[self.stun_url])]
        )
        self._pc = RTCPeerConnection(configuration=config)
        self._setup_callbacks()

        self._track = OpusStreamTrack(pipeline)
        self._pc.addTrack(self._track)

        offer = await self._pc.createOffer()
        await self._pc.setLocalDescription(offer)

        await self._wait_for_ice()

        offer_sdp  = self._pc.localDescription.sdp
        remote_sdp = self._build_remote_sdp(offer_sdp, group_call_params)

        await self._pc.setRemoteDescription(
            RTCSessionDescription(sdp=remote_sdp, type="answer")
        )

        self._connected = True
        logger.info("✅ WebRTC connected to Telegram Group Call!")

        return {
            "sdp":  self._pc.localDescription.sdp,
            "type": self._pc.localDescription.type,
        }

    async def disconnect(self):
        if self._track:
            self._track.stop()
        if self._pc:
            await self._pc.close()
        self._connected = False
        logger.info("WebRTC disconnected.")

    def switch_track(self, new_pipeline):
        if self._track:
            self._track.switch_pipeline(new_pipeline)

    @property
    def is_connected(self) -> bool:
        return self._connected

    def _setup_callbacks(self):
        @self._pc.on("connectionstatechange")
        async def on_state():
            state = self._pc.connectionState
            logger.info(f"WebRTC state: {state}")
            if state in ("failed", "closed"):
                self._connected = False

        @self._pc.on("iceconnectionstatechange")
        async def on_ice():
            logger.info(f"ICE state: {self._pc.iceConnectionState}")

    async def _wait_for_ice(self, timeout: float = 10.0):
        loop     = asyncio.get_event_loop()
        deadline = loop.time() + timeout
        while self._pc.iceGatheringState != "complete":
            if loop.time() > deadline:
                logger.warning("ICE gathering timeout — proceeding anyway")
                break
            await asyncio.sleep(0.1)

    def _build_remote_sdp(self, offer_sdp: str, params: dict) -> str:
        transport   = params.get("transport", {})
        fingerprint = transport.get("fingerprint", {})
        fp_hash     = fingerprint.get("hash", "sha-256")
        fp_value    = fingerprint.get("value", "")
        ufrag       = transport.get("ufrag", "telegram")
        pwd         = transport.get("pwd", "telegram")
        ssrc        = params.get("ssrc", 0)
        candidates  = transport.get("candidates", [])

        # Offer se m= sections parse karo
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
                        "a=rtpmap", "a=fmtp", "a=rtcp-fb", "a=mid"
                    )):
                        answer.append(line)

                answer.append("a=rtcp-mux")   # ✅ FIX 3: RTCP mux
                answer.append("a=sendonly")

                if ssrc:
                    answer.append(f"a=ssrc:{ssrc} cname:telegram")

                for c in candidates:
                    answer.append(
                        f"a=candidate:{c.get('foundation', '1')} 1 "
                        f"{c.get('protocol', 'udp')} "
                        f"{c.get('priority', 2130706431)} "
                        f"{c.get('ip', '0.0.0.0')} "
                        f"{c.get('port', 0)} "
                        f"typ {c.get('type', 'host')}"
                    )

            else:
                # Data channel / video — reject
                parts    = m_line.split()
                parts[1] = "0"
                answer.append(" ".join(parts))
                answer.append("c=IN IP4 0.0.0.0")
                for line in section[1:]:
                    if line.startswith("a=mid"):
                        answer.append(line)

        return "\r\n".join(answer) + "\r\n"
