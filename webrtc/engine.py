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

    async def recv(self):

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
        frame.time_base = Fraction(1, SAMPLE_RATE)
        frame.pts = self._timestamp

        frame.planes[0].update(pcm_bytes)

        self._timestamp += FRAME_SAMPLES

        return frame


class WebRTCEngine:

    def __init__(
        self,
        stun_url="stun:stun.l.google.com:19302"
    ):

        self.stun_url = stun_url

        self._pc = None
        self._track = None

        self._connected = False

        self._on_failed = None
        self._on_connected = None

        self._prepared_offer_sdp = None
        self._prepared_ssrc = 0

    def set_reconnect_callback(self, callback: Callable):
        self._on_failed = callback

    def set_connected_callback(self, callback: Callable):
        self._on_connected = callback

    async def prepare(self, pipeline):

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

        self._track = SwitchableAudioTrack()
        self._track.set_pipeline(pipeline)

        sender = self._pc.addTrack(self._track)

        transceiver = next(
            t for t in self._pc.getTransceivers()
            if t.sender == sender
        )

        transceiver.direction = "sendonly"

        self._setup_callbacks(self._pc)

        offer = await self._pc.createOffer()

        await self._pc.setLocalDescription(
            offer
        )

        while (
            self._pc.iceGatheringState
            != "complete"
        ):
            await asyncio.sleep(0.1)

        local_sdp = self._pc.localDescription.sdp

        self._prepared_offer_sdp = local_sdp

        ufrag, pwd = self._extract_ice_credentials(
            local_sdp
        )

        fingerprint = self._extract_fingerprint(
            local_sdp
        )

        ssrc = self._extract_ssrc(
            local_sdp
        )

        self._prepared_ssrc = ssrc

        logger.info(
            f"WebRTC prepared — "
            f"ufrag: {ufrag} "
            f"ssrc: {ssrc} "
            f"fingerprint: {fingerprint[:40]}..."
        )

        return (
            ufrag,
            pwd,
            fingerprint,
            ssrc
        )

    async def complete_connect(
        self,
        group_call_params: dict
    ):

        if not self._pc:
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

        asyncio.create_task(
            self._poll_connection()
        )

        logger.info(
            "✅ WebRTC handshake started"
        )

        return True

    async def _poll_connection(self):

        last = None

        for i in range(60):

            await asyncio.sleep(1)

            if not self._pc:
                return

            state = self._pc.connectionState

            if state != last:

                logger.info(
                    f"[poll {i+1}s] "
                    f"connectionState: {state}"
                )

                last = state

            if state == "connected":

                logger.info(
                    "✅ DTLS CONNECTED"
                )

                if self._on_connected:
                    asyncio.create_task(
                        self._on_connected()
                    )

                return

            if state in (
                "failed",
                "closed"
            ):

                self._connected = False

                if self._on_failed:
                    asyncio.create_task(
                        self._on_failed()
                    )

                return

        logger.error(
            "DTLS timeout"
        )

    def _setup_callbacks(self, pc):

        @pc.on("connectionstatechange")
        async def on_connection():

            try:
                state = pc.connectionState
            except:
                return

            logger.info(
                f"WebRTC state: {state}"
            )

            if state == "connected":

                if self._on_connected:
                    asyncio.create_task(
                        self._on_connected()
                    )

            if state in (
                "failed",
                "closed"
            ):

                self._connected = False

                if self._on_failed:
                    asyncio.create_task(
                        self._on_failed()
                    )

        @pc.on("iceconnectionstatechange")
        async def on_ice():

            try:
                logger.info(
                    f"ICE state: "
                    f"{pc.iceConnectionState}"
                )
            except:
                pass

    def _build_remote_sdp(
        self,
        offer_sdp,
        params
    ):

        transport = params.get(
            "transport",
            {}
        )

        fingerprints = transport.get(
            "fingerprints",
            []
        )

        candidates = transport.get(
            "candidates",
            []
        )

        ufrag = transport.get(
            "ufrag",
            "telegram"
        )

        pwd = transport.get(
            "pwd",
            "telegram"
        )

        fp_hash = "sha-256"
        fp_value = ""

        if fingerprints:

            fp_hash = fingerprints[0].get(
                "hash",
                "sha-256"
            )

            fp_value = fingerprints[0].get(
                "fingerprint",
                ""
            ).upper()

        answer = [
            "v=0",
            "o=- 0 0 IN IP4 127.0.0.1",
            "s=-",
            "t=0 0",
            "a=group:BUNDLE 0",
            "a=msid-semantic:WMS *"
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

                parts = m_line.split()

                parts[1] = "9"

                answer.append(
                    " ".join(parts)
                )

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
                        f"a=fingerprint:{fp_hash} {fp_value}"
                    )

                answer.append(
                    "a=setup:active"
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
                            "a=ice-options",
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
                    "a=recvonly"
                )

                for c in candidates:

                    answer.append(
                        f"a=candidate:{c.get('foundation', '1')} 1 "
                        f"{c.get('protocol', 'udp')} "
                        f"{c.get('priority', 2130706431)} "
                        f"{c.get('ip', '0.0.0.0')} "
                        f"{c.get('port', 0)} "
                        f"typ {c.get('type', 'host')}"
                    )

                answer.append(
                    "a=end-of-candidates"
                )

            else:

                parts = m_line.split()

                if len(parts) >= 2:
                    parts[1] = "0"

                answer.append(
                    " ".join(parts)
                )

                answer.append(
                    "c=IN IP4 0.0.0.0"
                )

                for line in section[1:]:

                    if line.startswith("a=mid"):
                        answer.append(line)

        return (
            "\r\n".join(answer)
            + "\r\n"
        )

    def _extract_ice_credentials(
        self,
        sdp
    ):

        ufrag = ""
        pwd = ""

        for line in sdp.split("\r\n"):

            if line.startswith(
                "a=ice-ufrag:"
            ):
                ufrag = line.split(
                    ":",
                    1
                )[1]

            elif line.startswith(
                "a=ice-pwd:"
            ):
                pwd = line.split(
                    ":",
                    1
                )[1]

        return ufrag, pwd

    def _extract_fingerprint(
        self,
        sdp
    ):

        for line in sdp.split("\r\n"):

            if line.startswith(
                "a=fingerprint:"
            ):
                return line.split(
                    ":",
                    1
                )[1]

        return ""

    def _extract_ssrc(
        self,
        sdp
    ):

        for line in sdp.split("\r\n"):

            if line.startswith(
                "a=ssrc:"
            ):

                try:

                    return int(
                        line.split(":")[1]
                        .split()[0]
                    )

                except:
                    pass

        return 0

    async def _cleanup_pc(self):

        if self._track:

            self._track.stop()

            self._track = None

        if self._pc:

            await self._pc.close()

            self._pc = None

    async def disconnect(self):

        self._connected = False

        self._prepared_offer_sdp = None
        self._prepared_ssrc = 0

        await self._cleanup_pc()

    @property
    def is_connected(self):
        return self._connected

    @property
    def prepared_ssrc(self):
        return self._prepared_ssrc
