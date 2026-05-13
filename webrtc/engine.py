"""
webrtc/engine.py
----------------
Custom WebRTC engine — aiortc use karta hai, PyTgCalls nahi.

Kya karta hai:
  1. RTCPeerConnection banata hai (aiortc)
  2. OpusStreamTrack se audio pipeline frames inject karta hai
  3. ICE/STUN se Telegram media server se connect karta hai
  4. DTLS handshake complete karta hai
  5. SRTP stream Telegram ko bhejta hai
"""

import asyncio
import logging

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
FRAME_SAMPLES = 960   # 20ms @ 48kHz


# -----------------------------------------------------------------------
# Custom Audio Track — Pipeline se Opus frames inject karta hai
# -----------------------------------------------------------------------

class OpusStreamTrack(MediaStreamTrack):
    """
    aiortc AudioStreamTrack.
    AudioPipeline se frames uthata hai aur WebRTC ko deliver karta hai.
    """

    kind = "audio"

    def __init__(self, pipeline):
        super().__init__()
        self._pipeline  = pipeline
        self._timestamp = 0

    async def recv(self) -> AudioFrame:
        loop = asyncio.get_event_loop()

        # Pipeline se Opus frame lo (non-blocking with timeout)
        opus_bytes = await loop.run_in_executor(
            None, lambda: self._pipeline.get_frame(timeout=0.05)
        )

        if opus_bytes is None:
            # Silence / comfort noise frame
            opus_bytes = b"\xf8\xff\xfe"

        # AudioFrame wrap karo
        frame              = AudioFrame(format="s16", layout="stereo", samples=FRAME_SAMPLES)
        frame.sample_rate  = SAMPLE_RATE
        frame.pts          = self._timestamp
        frame.time_base    = f"1/{SAMPLE_RATE}"
        self._timestamp   += FRAME_SAMPLES
        return frame

    def switch_pipeline(self, new_pipeline):
        """Chal rahe connection mein audio source badlo (skip ke liye)."""
        self._pipeline = new_pipeline
        logger.info("Audio track pipeline switched.")


# -----------------------------------------------------------------------
# WebRTC Engine
# -----------------------------------------------------------------------

class WebRTCEngine:
    """
    Telegram Group Call ke saath WebRTC connection manage karta hai.

    Usage:
        engine = WebRTCEngine(stun_url="stun:stun.l.google.com:19302")
        await engine.connect(group_call_params, pipeline)
        # ... music plays ...
        await engine.disconnect()
    """

    def __init__(self, stun_url: str = "stun:stun.l.google.com:19302"):
        self.stun_url   = stun_url
        self._pc        = None   # RTCPeerConnection
        self._track     = None   # OpusStreamTrack
        self._connected = False

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def connect(self, group_call_params: dict, pipeline) -> dict:
        """
        Telegram Group Call se WebRTC connect karo.

        group_call_params = {
            "transport": { candidates, fingerprint, ufrag, pwd },
            "ssrc": int,
        }

        Returns: local SDP dict (Telegram ko wapas bhejna hai)
        """
        # 1. RTCPeerConnection with STUN
        config   = RTCConfiguration(iceServers=[RTCIceServer(urls=self.stun_url)])
        self._pc = RTCPeerConnection(configuration=config)
        self._setup_callbacks()

        # 2. Audio track add karo
        self._track = OpusStreamTrack(pipeline)
        self._pc.addTrack(self._track)

        # 3. SDP Offer create karo
        offer = await self._pc.createOffer()
        await self._pc.setLocalDescription(offer)

        # 4. ICE gathering ka wait karo
        await self._wait_for_ice()

        # 5. Telegram ke params se Remote SDP set karo
        remote_sdp = self._build_remote_sdp(group_call_params)
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
        """Connection band karo."""
        if self._track:
            self._track.stop()
        if self._pc:
            await self._pc.close()
        self._connected = False
        logger.info("WebRTC disconnected.")

    def switch_track(self, new_pipeline):
        """Chal rahe connection mein audio source switch karo (skip)."""
        if self._track:
            self._track.switch_pipeline(new_pipeline)

    @property
    def is_connected(self) -> bool:
        return self._connected

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

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
        """ICE gathering complete hone tak wait karo."""
        deadline = asyncio.get_event_loop().time() + timeout
        while self._pc.iceGatheringState != "complete":
            if asyncio.get_event_loop().time() > deadline:
                logger.warning("ICE gathering timeout — proceeding anyway")
                break
            await asyncio.sleep(0.1)

    def _build_remote_sdp(self, params: dict) -> str:
        """
        Telegram ke transport params se SDP answer banao.
        Production mein yeh phone.joinGroupCall response ka actual data hoga.
        """
        transport   = params.get("transport", {})
        fingerprint = transport.get("fingerprint", {})
        ufrag       = transport.get("ufrag", "telegram")
        pwd         = transport.get("pwd", "telegram")
        ssrc        = params.get("ssrc", 0)

        sdp = (
            "v=0\r\n"
            "o=- 0 0 IN IP4 0.0.0.0\r\n"
            "s=-\r\n"
            "t=0 0\r\n"
            "m=audio 1 RTP/SAVPF 111\r\n"
            "c=IN IP4 0.0.0.0\r\n"
            f"a=ice-ufrag:{ufrag}\r\n"
            f"a=ice-pwd:{pwd}\r\n"
            f"a=fingerprint:{fingerprint.get('hash', 'sha-256')} "
            f"{fingerprint.get('value', '')}\r\n"
            "a=setup:passive\r\n"
            "a=rtpmap:111 opus/48000/2\r\n"
            "a=fmtp:111 minptime=10;useinbandfec=1\r\n"
            f"a=ssrc:{ssrc} cname:telegram\r\n"
            "a=recvonly\r\n"
        )

        for candidate in transport.get("candidates", []):
            sdp += (
                f"a=candidate:{candidate.get('foundation', '1')} 1 "
                f"{candidate.get('protocol', 'udp')} "
                f"{candidate.get('priority', 2130706431)} "
                f"{candidate.get('ip', '0.0.0.0')} "
                f"{candidate.get('port', 0)} "
                f"typ {candidate.get('type', 'host')}\r\n"
            )

        return sdp
