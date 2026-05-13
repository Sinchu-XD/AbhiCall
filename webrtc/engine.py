"""
webrtc/engine.py — Telegram VC Fully Fixed Version

FINAL FIXES:
  ✅ DTLS role fixed for Telegram VC
  ✅ ICE-lite enabled
  ✅ recvonly SDP mode
  ✅ Fingerprint uppercase
  ✅ Poll fallback
  ✅ Proper SSRC extraction
  ✅ Proper aiortc offer handling
  ✅ DTLS timeout debugging
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

SAMPLE_RATE = 48000
FRAME_SAMPLES = 960
BYTES_PER_FRAME = FRAME_SAMPLES * 2 * 2


class SwitchableAudioTrack(MediaStreamTrack):
    kind = "audio"

    def __init__(self):
        super().__init__()
        self._pipeline = None
        self._timestamp = 0

    def set_pipeline(self, pipeline):
        self._pipeline = pipeline

    def switch_pipeline(self, new_pipeline):
        self._pipeline = new_pipeline
        logger.info("Audio pipeline switched.")

    async def recv(self) -> AudioFrame:
        loop = asyncio.get_running_loop()

        if self._pipeline and self._pipeline.is_alive:
            pcm_bytes = await loop.run_in_executor(
                None,
                lambda: self._pipeline.get_frame(timeout=0.05)
            )
        else:
            pcm_bytes = None

        if pcm_bytes is None:
            await asyncio.sleep(0.02)
            pcm_bytes = b"\x00" * BYTES_PER_FRAME

        frame = AudioFrame(
            format="s16",
            layout="stereo",
            samples=FRAME_SAMPLES
        )

        frame.sample_rate = SAMPLE_RATE
        frame.pts = self._timestamp
        frame.time_base = Fraction(1, SAMPLE_RATE)

        frame.planes[0].update(pcm_bytes)

        self._timestamp += FRAME_SAMPLES
        return frame


class WebRTCEngine:

    def __init__(self, stun_url="stun:stun.l.google.com:19302"):
        self.stun_url = stun_url

        self._pc: Optional[RTCPeerConnection] = None
        self._track: Optional[SwitchableAudioTrack] = None

        self._connected = False
        self._dtls_fired = False

        self._on_failed = None
        self._on_connected = None

        self._prepared_offer_sdp = None
        self._prepared_ssrc = 0
        self._prepared_fp = None

    # =========================================================
    # CALLBACKS
    # =========================================================

    def set_reconnect_callback(self, callback: Callable):
        self._on_failed = callback

    def set_connected_callback(self, callback: Callable):
        self._on_connected = callback

    async def _fire_connected(self):
        if self._dtls_fired:
            return

        self._dtls_fired = True

        logger.info("✅ DTLS connected — SRTP is flowing!")

        if self._on_connected:
            asyncio.create_task(self._on_connected())

    # =========================================================
    # PREPARE
    # =========================================================

    async def prepare(self, pipeline):

        if self._pc:
            await self._cleanup_pc()

        self._dtls_fired = False

        config = RTCConfiguration(
            iceServers=[
                RTCIceServer(urls=[self.stun_url])
            ]
        )

        self._pc = RTCPeerConnection(configuration=config)

        self._track = SwitchableAudioTrack()
        self._track.set_pipeline(pipeline)

        self._pc.addTrack(self._track)

        # IMPORTANT
        for transceiver in self._pc.getTransceivers():
            if transceiver.kind == "audio":
                transceiver.direction = "sendonly"

        self._setup_callbacks(self._pc)

        offer = await self._pc.createOffer()

        await self._pc.setLocalDescription(
            RTCSessionDescription(
                sdp=offer.sdp,
                type="offer"
            )
        )

        while self._pc.iceGatheringState != "complete":
            await asyncio.sleep(0.1)

        local_sdp = self._pc.localDescription.sdp

        ufrag, pwd = self._extract_ice_credentials(local_sdp)
        fingerprint = self._extract_fingerprint(local_sdp)
        ssrc = self._extract_ssrc(local_sdp)

        self._prepared_offer_sdp = local_sdp
        self._prepared_ssrc = ssrc
        self._prepared_fp = fingerprint

        logger.info(
            f"WebRTC prepared — "
            f"ufrag: {ufrag} "
            f"ssrc: {ssrc} "
            f"fingerprint: {fingerprint[:40]}..."
        )

        return ufrag, pwd, fingerprint, ssrc

    # =========================================================
    # COMPLETE CONNECT
    # =========================================================

    async def complete_connect(self, group_call_params: dict):

        if not self._pc:
            logger.error("prepare() not called")
            return False

        remote_sdp = self._build_remote_sdp(
            self._prepared_offer_sdp,
            group_call_params
        )

        logger.info("REMOTE SDP START")
        logger.info(remote_sdp)
        logger.info("REMOTE SDP END")

        await self._pc.setRemoteDescription(
            RTCSessionDescription(
                sdp=remote_sdp,
                type="answer"
            )
        )

        self._connected = True

        logger.info(
            "✅ WebRTC handshake started — "
            "ICE/DTLS running in background..."
        )

        asyncio.create_task(
            self._poll_connection_state(self._pc)
        )

        return True

    # =========================================================
    # POLL FALLBACK
    # =========================================================

    async def _poll_connection_state(self, pc):

        last_state = None

        for i in range(60):

            await asyncio.sleep(1)

            try:
                state = pc.connectionState
            except Exception:
                return

            if state != last_state:
                logger.info(
                    f"[poll {i+1}s] connectionState: {state}"
                )
                last_state = state

            if state == "connected":
                await self._fire_connected()
                return

            if state in ("failed", "closed"):

                logger.warning(
                    f"WebRTC {state} — reconnecting..."
                )

                self._connected = False

                if self._on_failed:
                    asyncio.create_task(self._on_failed())

                return

        logger.error(
            f"DTLS timed out after 60s. "
            f"Last connectionState: {last_state}"
        )

    # =========================================================
    # CALLBACKS
    # =========================================================

    def _setup_callbacks(self, pc):

        @pc.on("connectionstatechange")
        async def on_connection():

            try:
                state = pc.connectionState
            except Exception:
                return

            logger.info(
                f"WebRTC state (event): {state}"
            )

            if state == "connected":
                await self._fire_connected()

            if state in ("failed", "closed"):

                self._connected = False

                if self._on_failed:
                    asyncio.create_task(
                        self._on_failed()
                    )

        @pc.on("iceconnectionstatechange")
        async def on_ice():

            try:
                logger.info(
                    f"ICE state: {pc.iceConnectionState}"
                )
            except Exception:
                pass

    # =========================================================
    # REMOTE SDP
    # =========================================================

    def _build_remote_sdp(self, offer_sdp, params):

        transport = params.get("transport", {})

        fp_list = transport.get("fingerprints", [])

        if fp_list:
            fp_hash = fp_list[0].get("hash", "sha-256")
            fp_value = fp_list[0].get(
                "fingerprint",
                ""
            ).upper()
        else:
            fp_hash = "sha-256"
            fp_value = ""

        ufrag = transport.get("ufrag", "telegram")
        pwd = transport.get("pwd", "telegram")

        candidates = transport.get("candidates", [])

        answer = [
            "v=0",
            "o=- 0 0 IN IP4 127.0.0.1",
            "s=-",
            "t=0 0",
            "a=group:BUNDLE 0",
            "a=msid-semantic:WMS *",
        ]

        sections = []
        current = []
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

        for section in sections:

            m_line = section[0]

            if "audio" in m_line:

                answer.append(m_line)

                answer.append("c=IN IP4 0.0.0.0")

                answer.append(f"a=ice-ufrag:{ufrag}")
                answer.append(f"a=ice-pwd:{pwd}")

                if fp_value:
                    answer.append(
                        f"a=fingerprint:{fp_hash} {fp_value}"
                    )

                # CRITICAL FIX
                answer.append("a=setup:passive")

                for line in section[1:]:

                    if any(
                        line.startswith(prefix)
                        for prefix in (
                            "a=rtpmap",
                            "a=fmtp",
                            "a=rtcp-fb",
                            "a=mid",
                            "a=extmap",
                            "a=ice-options",
                        )
                    ):
                        answer.append(line)

                answer.append("a=rtcp:9 IN IP4 0.0.0.0")
                answer.append("a=rtcp-mux")
                answer.append("a=rtcp-rsize")

                # IMPORTANT FIX
                answer.append("a=ice-lite")

                # IMPORTANT FIX
                answer.append("a=recvonly")

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

                parts = m_line.split()

                if len(parts) >= 2:
                    parts[1] = "0"

                answer.append(" ".join(parts))

                answer.append("c=IN IP4 0.0.0.0")

                for line in section[1:]:
                    if line.startswith("a=mid"):
                        answer.append(line)

        return "\r\n".join(answer) + "\r\n"

    # =========================================================
    # HELPERS
    # =========================================================

    def _extract_ice_credentials(self, sdp):

        ufrag = ""
        pwd = ""

        for line in sdp.split("\r\n"):

            if line.startswith("a=ice-ufrag:"):
                ufrag = line.split(":", 1)[1]

            elif line.startswith("a=ice-pwd:"):
                pwd = line.split(":", 1)[1]

        return ufrag, pwd

    def _extract_fingerprint(self, sdp):

        for line in sdp.split("\r\n"):

            if line.startswith("a=fingerprint:"):
                return line.split(":", 1)[1]

        return ""

    def _extract_ssrc(self, sdp):

        for line in sdp.split("\r\n"):

            if line.startswith("a=ssrc:"):

                try:
                    return int(
                        line.split(":")[1].split()[0]
                    )
                except:
                    pass

        return 0

    # =========================================================
    # CLEANUP
    # =========================================================

    async def _cleanup_pc(self):

        if self._track:
            self._track.stop()
            self._track = None

        if self._pc:
            await self._pc.close()
            self._pc = None

    async def disconnect(self):

        self._connected = False
        self._dtls_fired = False

        self._prepared_offer_sdp = None
        self._prepared_ssrc = 0
        self._prepared_fp = None

        await self._cleanup_pc()

        logger.info("WebRTC disconnected.")

    # =========================================================
    # PROPERTIES
    # =========================================================

    @property
    def is_connected(self):
        return self._connected

    @property
    def prepared_ssrc(self):
        return self._prepared_ssrc
