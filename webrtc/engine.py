"""
webrtc/engine.py — Custom WebRTC engine (aiortc, no PyTgCalls)

FIXES APPLIED:
  1. Two-phase connect: prepare_offer() extracts real ICE credentials (ufrag/pwd)
     so GroupCallManager can send them to Telegram — fixes "Consent to send expired".
  2. _build_remote_sdp now uses self._local_ssrc instead of params.get("ssrc", 0)
     which was always 0 — fixes the a=ssrc:0 bug.
  3. a=setup:active instead of a=setup:passive — bot is DTLS client, Telegram server.
  4. SwitchableAudioTrack: single track object that can be silent or stream real audio,
     avoiding the need for replaceTrack() between phases.
  5. Reconnect callback: when connectionState fails, GroupCallManager is notified.
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

SAMPLE_RATE   = 48000
FRAME_SAMPLES = 960


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
        loop = asyncio.get_event_loop()

        if self._pipeline and self._pipeline.is_alive:
            pcm_bytes = await loop.run_in_executor(
                None, lambda: self._pipeline.get_frame(timeout=0.05)
            )
        else:
            pcm_bytes = None

        if pcm_bytes is None:
            await asyncio.sleep(0.02)
            pcm_bytes = b"\x00" * (FRAME_SAMPLES * 2 * 2)

        frame             = AudioFrame(format="s16", layout="stereo", samples=FRAME_SAMPLES)
        frame.sample_rate = SAMPLE_RATE
        frame.pts         = self._timestamp
        frame.time_base   = Fraction(1, SAMPLE_RATE)
        frame.planes[0].update(pcm_bytes)
        self._timestamp  += FRAME_SAMPLES
        return frame


class WebRTCEngine:

    def __init__(self, stun_url: str = "stun:stun.l.google.com:19302"):
        self.stun_url        = stun_url
        self._pc             = None
        self._track: Optional[SwitchableAudioTrack] = None
        self._connected      = False
        self._local_ssrc     = 0
        self._on_failed: Optional[Callable] = None

    def set_reconnect_callback(self, callback: Callable):
        self._on_failed = callback

    async def prepare_offer(self, ssrc: int) -> tuple:
        """
        Phase 1 — call BEFORE joining Telegram.
        Creates PC + silent track, generates offer, waits for ICE,
        returns (offer_sdp, ufrag, pwd) — the real ICE credentials to
        send to Telegram in JoinGroupCall.
        """
        if self._pc:
            await self._cleanup_pc()

        config = RTCConfiguration(
            iceServers=[RTCIceServer(urls=[self.stun_url])]
        )
        self._pc         = RTCPeerConnection(configuration=config)
        self._local_ssrc = ssrc
        self._setup_callbacks()

        self._track = SwitchableAudioTrack()
        self._pc.addTrack(self._track)

        offer = await self._pc.createOffer()
        await self._pc.setLocalDescription(offer)
        await self._wait_for_ice()

        offer_sdp  = self._pc.localDescription.sdp
        ufrag, pwd = self._extract_ice_credentials(offer_sdp)

        logger.info(f"Offer ICE creds — ufrag: {ufrag}  pwd: {pwd[:8]}...")
        return offer_sdp, ufrag, pwd

    async def finalize_connection(self, transport_params: dict, pipeline) -> bool:
        """
        Phase 2 — call AFTER Telegram returns transport params.
        Switches track to real audio pipeline, sets remote description.
        """
        if not self._pc or not self._track:
            logger.error("prepare_offer() must be called before finalize_connection()")
            return False

        self._track.set_pipeline(pipeline)

        offer_sdp  = self._pc.localDescription.sdp
        remote_sdp = self._build_remote_sdp(offer_sdp, transport_params)

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
        ufrag = "telegram"
        pwd   = "telegram"
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
                    logger.warning("WebRTC failed — triggering reconnect callback...")
                    asyncio.create_task(self._on_failed())

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
        ssrc       = self._local_ssrc   # FIX: was params.get("ssrc", 0) — always 0
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
                answer.append("a=setup:active")   # FIX: was "passive"

                for line in section[1:]:
                    if any(line.startswith(p) for p in (
                        "a=rtpmap", "a=fmtp", "a=rtcp-fb", "a=mid"
                    )):
                        answer.append(line)

                answer.append("a=rtcp-mux")
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
                parts    = m_line.split()
                parts[1] = "0"
                answer.append(" ".join(parts))
                answer.append("c=IN IP4 0.0.0.0")
                for line in section[1:]:
                    if line.startswith("a=mid"):
                        answer.append(line)

        return "\r\n".join(answer) + "\r\n"
