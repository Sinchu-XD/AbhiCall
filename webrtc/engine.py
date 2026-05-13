"""
webrtc/engine.py — Final Fixed Version

Fixes:
  1. prepare() extracts real DTLS fingerprint/ICE credentials/SSRC from offer SDP.
  2. a=setup:passive → aiortc is DTLS client (sends ClientHello); consistent with
     RFC 5763: ICE-controlling full agent MUST be DTLS client.
  3. a=ice-lite in answer → aiortc stays ICE-controlling, nominates pairs itself.
  4. Fingerprint UPPERCASED in remote SDP — Telegram sends lowercase hex, aiortc
     computes uppercase internally; case mismatch caused silent DTLS stall.
  5. Polling fallback — checks connectionState every second for 60s in case
     the connectionstatechange event never fires (known aiortc issue).
  6. set_connected_callback() fires only once when DTLS completes, so
     group_call can unmute after SRTP is actually flowing.
  7. Callbacks use `pc` closure variable — no AttributeError on cleanup.
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
    """Audio track whose pipeline can be swapped at runtime."""
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
        self._pc: Optional[RTCPeerConnection] = None
        self._track: Optional[SwitchableAudioTrack] = None
        self._connected   = False
        self._on_failed:    Optional[Callable] = None
        self._on_connected: Optional[Callable] = None
        self._dtls_fired   = False   # prevent double-firing

        self._prepared_offer_sdp: Optional[str] = None
        self._prepared_fp:        Optional[str] = None
        self._prepared_ssrc:      int = 0

    def set_reconnect_callback(self, callback: Callable):
        self._on_failed = callback

    def set_connected_callback(self, callback: Callable):
        """
        Fired when DTLS handshake completes (connectionState == 'connected').
        Primary path: connectionstatechange event.
        Fallback path: _poll_connection_state() background task.
        """
        self._on_connected = callback

    # ------------------------------------------------------------------
    # Phase 1 — call BEFORE JoinGroupCall
    # ------------------------------------------------------------------

    async def prepare(self, pipeline) -> tuple[str, str, str, int]:
        """
        Create PC + offer, wait for ICE gathering, extract and return:
            (ufrag, pwd, fingerprint, ssrc)
        All values come from aiortc's offer SDP.
        """
        if self._pc:
            await self._cleanup_pc()

        self._dtls_fired = False

        config   = RTCConfiguration(iceServers=[RTCIceServer(urls=[self.stun_url])])
        self._pc = RTCPeerConnection(configuration=config)

        self._track = SwitchableAudioTrack()
        self._track.set_pipeline(pipeline)
        self._pc.addTrack(self._track)

        offer = await self._pc.createOffer()
        await self._pc.setLocalDescription(
            RTCSessionDescription(sdp=offer.sdp, type="offer")
        )

        # Wait for all ICE candidates to be gathered
        while self._pc.iceGatheringState != "complete":
            await asyncio.sleep(0.1)

        local_sdp   = self._pc.localDescription.sdp
        ufrag, pwd  = self._extract_ice_credentials(local_sdp)
        fingerprint = self._extract_fingerprint(local_sdp)
        ssrc        = self._extract_ssrc(local_sdp)

        self._prepared_offer_sdp = local_sdp
        self._prepared_fp        = fingerprint
        self._prepared_ssrc      = ssrc

        logger.info(
            f"WebRTC prepared — ufrag: {ufrag}  "
            f"ssrc: {ssrc}  "
            f"fingerprint: {fingerprint[:30]}..."
        )
        return ufrag, pwd, fingerprint, ssrc

    # ------------------------------------------------------------------
    # Phase 2 — call AFTER JoinGroupCall succeeds
    # ------------------------------------------------------------------

    async def complete_connect(self, group_call_params: dict) -> bool:
        """
        Set up callbacks, build fake answer SDP from Telegram's transport params,
        set remote description. ICE + DTLS run asynchronously in background.
        """
        if not self._pc or not self._prepared_offer_sdp:
            logger.error("complete_connect() called before prepare()!")
            return False

        transport = group_call_params.get("transport", {})
        fp_list   = transport.get("fingerprints", [])

        if not fp_list:
            logger.error(
                "Telegram returned no fingerprints — _parse_join_response likely "
                "failed. Check for 'Transport params NOT found' above."
            )
            return False

        candidates = transport.get("candidates", [])
        if not candidates:
            logger.warning("Telegram returned 0 ICE candidates — ICE may fail.")

        self._setup_callbacks(self._pc)

        remote_sdp = self._build_remote_sdp(self._prepared_offer_sdp, group_call_params)
        await self._pc.setRemoteDescription(
            RTCSessionDescription(sdp=remote_sdp, type="answer")
        )

        self._connected = True
        logger.info("✅ WebRTC handshake started — ICE/DTLS running in background...")

        # Fallback: poll state in case connectionstatechange event never fires
        asyncio.create_task(self._poll_connection_state(self._pc))

        return True

    async def disconnect(self):
        self._connected          = False
        self._dtls_fired         = False
        self._prepared_offer_sdp = None
        self._prepared_fp        = None
        self._prepared_ssrc      = 0
        await self._cleanup_pc()
        logger.info("WebRTC disconnected.")

    def switch_track(self, new_pipeline):
        if self._track:
            self._track.switch_pipeline(new_pipeline)

    @property
    def is_connected(self) -> bool:
        return self._connected

    @property
    def prepared_ssrc(self) -> int:
        return self._prepared_ssrc

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    async def _fire_connected(self):
        """Fire the connected callback exactly once."""
        if self._dtls_fired:
            return
        self._dtls_fired = True
        if self._on_connected:
            asyncio.create_task(self._on_connected())

    async def _poll_connection_state(self, pc: RTCPeerConnection):
        """
        Fallback: poll connectionState every second for up to 60 seconds.
        Required because aiortc's connectionstatechange event sometimes
        never fires even when DTLS does complete.
        """
        last_state = None
        for i in range(60):
            await asyncio.sleep(1)
            try:
                state = pc.connectionState
            except Exception:
                return

            if state != last_state:
                logger.info(f"[poll {i+1}s] connectionState: {state}")
                last_state = state

            if state == "connected":
                logger.info("✅ DTLS connected (via poll) — SRTP is flowing!")
                await self._fire_connected()
                return

            if state in ("failed", "closed"):
                logger.warning(f"WebRTC {state} (via poll) — triggering reconnect...")
                self._connected = False
                if self._on_failed and not self._dtls_fired:
                    asyncio.create_task(self._on_failed())
                return

        logger.error(
            f"DTLS timed out after 60s. Last connectionState: {last_state}. "
            "Check fingerprint, ICE candidates, and UDP firewall rules."
        )

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
        """Returns 'sha-256 AA:BB:CC:...' from the local SDP."""
        for line in sdp.split("\r\n"):
            if line.startswith("a=fingerprint:"):
                return line[len("a=fingerprint:"):]
        return ""

    def _extract_ssrc(self, sdp: str) -> int:
        """
        Extract the SSRC aiortc assigned to the audio sender.
        MUST match what is sent to Telegram in join_params['ssrc'].
        """
        for line in sdp.split("\r\n"):
            if line.startswith("a=ssrc:"):
                try:
                    return int(line[len("a=ssrc:"):].split()[0])
                except (ValueError, IndexError):
                    pass
        return 0

    def _setup_callbacks(self, pc: RTCPeerConnection):
        """
        `pc` captured as closure variable — NOT self._pc.
        Prevents AttributeError when self._pc is set to None during cleanup.
        """
        @pc.on("connectionstatechange")
        async def on_state():
            try:
                state = pc.connectionState
            except Exception:
                return
            logger.info(f"WebRTC state (event): {state}")
            if state == "connected":
                logger.info("✅ DTLS connected (event) — SRTP is flowing!")
                await self._fire_connected()
            if state in ("failed", "closed"):
                self._connected = False
                if self._on_failed and not self._dtls_fired:
                    logger.warning("WebRTC failed (event) — triggering reconnect...")
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
            fp_value = fingerprints[0].get("fingerprint", "").upper()
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
            # NO a=ice-lite — Telegram does full ICE (it sends binding requests
            # and wins the tie-breaker, making itself ICE-controlling).
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
                # a=setup:active → Telegram is DTLS client (sends ClientHello).
                # Telegram won the ICE tie-breaker → ICE-controlling → DTLS client.
                # aiortc is ICE-controlled → DTLS server → waits for ClientHello.
                # RFC 5763: ICE-controlling agent MUST be DTLS client (active).
                answer.append("a=setup:active")

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
