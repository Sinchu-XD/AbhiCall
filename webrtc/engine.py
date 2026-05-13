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
        logger.info("Audio track pipeline switched.")

    async def recv(self) -> AudioFrame:

        logger.warning("recv() called")

        if self._pipeline and self._pipeline.is_alive:
            pcm_bytes = self._pipeline.get_frame(timeout=0.02)
        else:
            pcm_bytes = None

        if pcm_bytes is None:
            pcm_bytes = b"\x00" * BYTES_PER_FRAME

        frame = AudioFrame(
            format="s16",
            layout="stereo",
            samples=FRAME_SAMPLES
        )

        frame.planes[0].update(pcm_bytes)

        frame.sample_rate = SAMPLE_RATE
        frame.time_base = Fraction(1, SAMPLE_RATE)
        frame.pts = self._timestamp

        self._timestamp += FRAME_SAMPLES

        logger.info(
            f"Sending audio frame: {len(pcm_bytes)}"
        )

        return frame


class WebRTCEngine:

    def __init__(
        self,
        stun_url: str = "stun:stun.l.google.com:19302"
    ):

        self.stun_url = stun_url

        self._pc = None

        self._track: Optional[
            SwitchableAudioTrack
        ] = None

        self._connected = False
        self._local_ssrc = 0

        self._on_failed: Optional[
            Callable
        ] = None

    def set_reconnect_callback(
        self,
        callback: Callable
    ):
        self._on_failed = callback

    async def prepare_offer(
        self,
        ssrc: int
    ) -> tuple:

        if self._pc:
            await self._cleanup_pc()

        config = RTCConfiguration(
            iceServers=[
                RTCIceServer(
                    urls=[self.stun_url]
                )
            ]
        )

        self._pc = RTCPeerConnection(
            configuration=config
        )

        self._local_ssrc = ssrc

        self._setup_callbacks()

        self._track = SwitchableAudioTrack()

        sender = self._pc.addTrack(
            self._track
        )

        transceiver = next(
            t for t in self._pc.getTransceivers()
            if t.sender == sender
        )

        transceiver.direction = "sendonly"

        logger.warning(
            f"Transceivers: {self._pc.getTransceivers()}"
        )

        for t in self._pc.getTransceivers():

            logger.warning(
                f"Transceiver currentDirection="
                f"{t.currentDirection} "
                f"direction={t.direction}"
            )

        offer = await self._pc.createOffer()

        logger.warning(
            f"LOCAL SDP:\n{offer.sdp}"
        )

        await self._pc.setLocalDescription(
            offer
        )

        while (
            self._pc.iceGatheringState
            != "complete"
        ):
            await asyncio.sleep(0.1)

        offer_sdp = (
            self._pc.localDescription.sdp
        )

        ufrag, pwd = self._extract_ice_credentials(
            offer_sdp
        )

        logger.info(
            f"Offer ICE creds — "
            f"ufrag: {ufrag} "
            f"pwd: {pwd[:8]}..."
        )

        return offer_sdp, ufrag, pwd

    async def finalize_connection(
        self,
        transport_params: dict,
        pipeline
    ) -> bool:

        if not self._pc or not self._track:

            logger.error(
                "prepare_offer() must be called first"
            )

            return False

        self._track.set_pipeline(
            pipeline
        )

        offer_sdp = (
            self._pc.localDescription.sdp
        )

        remote_sdp = self._build_remote_sdp(
            offer_sdp,
            transport_params
        )

        logger.warning(
            f"REMOTE SDP:\n{remote_sdp}"
        )

        await self._pc.setRemoteDescription(
            RTCSessionDescription(
                sdp=remote_sdp,
                type="answer"
            )
        )

        await asyncio.sleep(5)

        logger.warning(
            f"Connection state after SDP: "
            f"{self._pc.connectionState}"
        )

        self._connected = True

        logger.info(
            "✅ WebRTC connected to Telegram Group Call!"
        )

        return True

    async def disconnect(self):

        self._connected = False

        await self._cleanup_pc()

        logger.info(
            "WebRTC disconnected."
        )

    def switch_track(
        self,
        new_pipeline
    ):

        if self._track:
            self._track.switch_pipeline(
                new_pipeline
            )

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

    def _extract_ice_credentials(
        self,
        sdp: str
    ) -> tuple:

        ufrag = "telegram"
        pwd = "telegram"

        for line in sdp.split("\r\n"):

            if line.startswith(
                "a=ice-ufrag:"
            ):
                ufrag = line.replace(
                    "a=ice-ufrag:",
                    ""
                )

            elif line.startswith(
                "a=ice-pwd:"
            ):
                pwd = line.replace(
                    "a=ice-pwd:",
                    ""
                )

        return ufrag, pwd

    def _setup_callbacks(self):

        @self._pc.on(
            "connectionstatechange"
        )
        async def on_state():

            state = (
                self._pc.connectionState
            )

            logger.info(
                f"WebRTC state: {state}"
            )

            if state in (
                "failed",
                "closed"
            ):

                self._connected = False

                if self._on_failed:

                    logger.warning(
                        "WebRTC failed — "
                        "reconnect callback..."
                    )

                    asyncio.create_task(
                        self._on_failed()
                    )

        @self._pc.on(
            "iceconnectionstatechange"
        )
        async def on_ice():

            logger.info(
                f"ICE state: "
                f"{self._pc.iceConnectionState}"
            )

    def _build_remote_sdp(
        self,
        offer_sdp: str,
        params: dict
    ) -> str:

        transport = params.get(
            "transport",
            {}
        )

        fingerprints = transport.get(
            "fingerprints",
            []
        )

        if fingerprints:

            fp_hash = fingerprints[0].get(
                "hash",
                "sha-256"
            )

            fp_value = fingerprints[0].get(
                "fingerprint",
                ""
            )

        else:

            fp_hash = "sha-256"
            fp_value = ""

        ufrag = transport.get(
            "ufrag",
            "telegram"
        )

        pwd = transport.get(
            "pwd",
            "telegram"
        )

        ssrc = self._local_ssrc

        candidates = transport.get(
            "candidates",
            []
        )

        sections = []
        current = []

        session_done = False

        for line in offer_sdp.split("\r\n"):

            if not line:
                continue

            if line.startswith("m="):

                if session_done:
                    sections.append(
                        current
                    )

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
            "a=group:BUNDLE 0",
            "a=msid-semantic:WMS *"
            
        ]

        for section in sections:

            m_line = section[0]

            if "audio" in m_line:

                answer.append(m_line)

                answer.append(
                    "c=IN IP4 0.0.0.0"
                )

                answer.append(
                    f"a=ice-ufrag:{ufrag}"
                )

                answer.append(
                    f"a=ice-pwd:{pwd}"
                )

                if fp_value:

                    answer.append(
                        f"a=fingerprint:"
                        f"{fp_hash} "
                        f"{fp_value}"
                    )

                answer.append(
                    "a=setup:passive"
                )

                for line in section[1:]:

                    if any(
                        line.startswith(p)
                        for p in (
                            "a=rtpmap",
                            "a=fmtp",
                            "a=rtcp-fb",
                            "a=mid",
                            "a=extmap",
                            "a=msid",
                            "a=ice-options",
                            "a=ssrc-group",
                        )
                    ):
                        answer.append(line)

                answer.append(
                    "a=rtcp:9 IN IP4 0.0.0.0"
                )

                answer.append(
                    "a=rtcp-mux"
                )

                answer.append(
                    "a=rtcp-rsize"
                )

                answer.append(
                    "a=sendrecv"
                )

                if ssrc:

                    answer.append(
                        f"a=ssrc:{ssrc} "
                        f"cname:telegram"
                    )

                for c in candidates:

                    answer.append(
                        f"a=candidate:"
                        f"{c.get('foundation', '1')} "
                        f"1 "
                        f"{c.get('protocol', 'udp')} "
                        f"{c.get('priority', 2130706431)} "
                        f"{c.get('ip', '0.0.0.0')} "
                        f"{c.get('port', 0)} "
                        f"typ "
                        f"{c.get('type', 'host')}"
                    )
                answer.append("a=end-of-candidates")

            else:

                parts = m_line.split()

                parts[1] = "0"

                answer.append(
                    " ".join(parts)
                )

                answer.append(
                    "c=IN IP4 0.0.0.0"
                )

                for line in section[1:]:

                    if line.startswith(
                        "a=mid"
                    ):
                        answer.append(line)

        return (
            "\r\n".join(answer)
            + "\r\n"
        )
