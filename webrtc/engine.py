"""
webrtc/engine.py — Custom WebRTC engine (aiortc, no PyTgCalls)

ROOT FIX for silence + 20-35s disconnect:
  Telegram's media server is ICE-lite — it responds to STUN checks but never
  sends USE-CANDIDATE to nominate a pair.  Without nomination the ICE connection
  never reaches "connected", DTLS never starts, and after ~30s aiortc gives up.

  Adding `a=ice-lite` to the remote answer SDP tells aiortc:
    "The remote is ICE-lite; YOU are controlling — nominate the pair yourself."
  aiortc then sends USE-CANDIDATE, ICE completes, DTLS runs, audio flows.

  ICE credential patch in connect() ensures Telegram's consent-refresh STUNs
  are accepted (fixes the original 40-second disconnect).
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
    """
    Audio track whose pipeline can be swapped at runtime without replaceTrack().
    get_frame() is a blocking threading.Queue call — MUST run in an executor.
    """
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
            # run_in_executor is mandatory — threading.Queue.get() is blocking
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
        self.stun_url    = stun_url
        self._pc         = None
        self._track: Optional[SwitchableAudioTrack] = None
        self._connected  = False
        self._on_failed: Optional[Callable] = None

    def set_reconnect_callback(self, callback: Callable):
        self._on_failed = callback

    async def connect(
        self,
        group_call_params: dict,
        pipeline,
        pre_ufrag: Optional[str] = None,
        pre_pwd:   Optional[str] = None,
    ) -> bool:
        """
        Single-phase connect. Patches ICE credentials into the offer SDP so
        aiortc uses the same ufrag/pwd that was already registered with Telegram.
        """
        if self._pc:
            await self._cleanup_pc()

        config   = RTCConfiguration(iceServers=[RTCIceServer(urls=[self.stun_url])])
        self._pc = RTCPeerConnection(configuration=config)
        self._setup_callbacks()

        # Wire up real pipeline immediately
        self._track = SwitchableAudioTrack()
        self._track.set_pipeline(pipeline)
        self._pc.addTrack(self._track)

        # Build offer
        offer     = await self._pc.createOffer()
        offer_sdp = offer.sdp

        # Patch ICE credentials to match what we already sent Telegram
        if pre_ufrag and pre_pwd:
            orig_ufrag, orig_pwd = self._extract_ice_credentials(offer_sdp)
            if orig_ufrag:
                offer_sdp = offer_sdp.replace(
                    f"a=ice-ufrag:{orig_ufrag}", f"a=ice-ufrag:{pre_ufrag}"
                )
            if orig_pwd:
                offer_sdp = offer_sdp.replace(
                    f"a=ice-pwd:{orig_pwd}", f"a=ice-pwd:{pre_pwd}"
                )
            logger.info(f"ICE creds patched — ufrag: {pre_ufrag}  pwd: {pre_pwd[:8]}...")

        await self._pc.setLocalDescription(
            RTCSessionDescription(sdp=offer_sdp, type="offer")
        )

        # Wait for ICE candidate gathering
        while self._pc.iceGatheringState != "complete":
            await asyncio.sleep(0.1)

        local_sdp  = self._pc.localDescription.sdp
        remote_sdp = self._build_remote_sdp(local_sdp, group_call_params)

        await self._pc.setRemoteDescription(
            RTCSessionDescription(sdp=remote_sdp, type="answer")
        )

        self._connected = True
        logger.info("✅ WebRTC connected to Telegram Group Call!")
        return True

    async def disconnect(self):
        self._connected = False
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

    def _extract_ice_credentials(self, sdp: str) -> tuple:
        ufrag = ""
        pwd   = ""
        for line in sdp.split("\r\n"):
            if line.startswith("a=ice-ufrag:"):
                ufrag = line[len("a=ice-ufrag:"):]
            elif line.startswith("a=ice-pwd:"):
                pwd   = line[len("a=ice-pwd:"):]
        return ufrag, pwd

    def _setup_callbacks(self):
        @self._pc.on("connectionstatechange")
        async def on_state():
            state = self._pc.connectionState
            logger.info(f"WebRTC state: {state}")
            if state in ("failed", "closed"):
                self._connected = False
                if self._on_failed:
                    logger.warning("WebRTC failed — triggering reconnect...")
                    asyncio.create_task(self._on_failed())

        @self._pc.on("iceconnectionstatechange")
        async def on_ice():
            logger.info(f"ICE state: {self._pc.iceConnectionState}")

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

        # Parse m= sections from the offer
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
            # KEY FIX: Telegram's server is ICE-lite — it responds to STUN checks
            # but never sends USE-CANDIDATE to nominate a pair.  Without this flag
            # aiortc switches to "controlled" and waits forever for Telegram to
            # nominate → ICE never completes → DTLS never starts → silence + dropout.
            # With a=ice-lite aiortc stays "controlling" and nominates itself.
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
                answer.append("a=setup:passive")   # Telegram = DTLS server; aiortc initiates

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

                # NO a=ssrc — adding our SSRC to the remote answer tells aiortc
                # the remote is sending with that SSRC → SRTP flips to receive-mode
                # → all outgoing audio is silenced.

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
                # Disable non-audio m= sections
                parts    = m_line.split()
                parts[1] = "0"
                answer.append(" ".join(parts))
                answer.append("c=IN IP4 0.0.0.0")
                for line in section[1:]:
                    if line.startswith("a=mid"):
                        answer.append(line)

        return "\r\n".join(answer) + "\r\n"
