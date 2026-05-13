"""
webrtc/engine.py
----------------
Custom WebRTC engine — aiortc use karta hai.
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

SAMPLE_RATE = 48000
FRAME_SAMPLES = 960


# -------------------------------------------------------------------
# Audio Track
# -------------------------------------------------------------------

class OpusStreamTrack(MediaStreamTrack):

    kind = "audio"

    def __init__(self, pipeline):

        super().__init__()

        self._pipeline = pipeline
        self._timestamp = 0

    async def recv(self) -> AudioFrame:

        loop = asyncio.get_event_loop()

        pcm = await loop.run_in_executor(
            None,
            lambda: self._pipeline.get_frame(
                timeout=0.05
            )
        )

        # Silence fallback
        if pcm is None:

            pcm = (
                b"\x00"
                * FRAME_SAMPLES
                * 2
                * 2
            )

        frame = AudioFrame(
            format="s16",
            layout="stereo",
            samples=FRAME_SAMPLES
        )

        frame.sample_rate = SAMPLE_RATE

        frame.planes[0].update(pcm)

        frame.pts = self._timestamp

        frame.time_base = Fraction(
            1,
            SAMPLE_RATE
        )

        self._timestamp += FRAME_SAMPLES

        return frame

    def switch_pipeline(
        self,
        new_pipeline
    ):

        self._pipeline = new_pipeline

        logger.info(
            "Audio track switched."
        )


# -------------------------------------------------------------------
# WebRTC Engine
# -------------------------------------------------------------------

class WebRTCEngine:

    def __init__(
        self,
        stun_url: str = (
            "stun:stun.l.google.com:19302"
        )
    ):

        self.stun_url = stun_url

        self._pc = None
        self._track = None
        self._connected = False

    # ----------------------------------------------------------------
    # Connect
    # ----------------------------------------------------------------

    async def connect(
        self,
        group_call_params: dict,
        pipeline
    ) -> dict:

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

        self._setup_callbacks()

        # ------------------------------------------------------------
        # Audio Track
        # ------------------------------------------------------------

        self._track = OpusStreamTrack(
            pipeline
        )

        self._pc.addTrack(
            self._track
        )

        # ------------------------------------------------------------
        # Offer
        # ------------------------------------------------------------

        offer = await self._pc.createOffer()

        await self._pc.setLocalDescription(
            offer
        )

        # ------------------------------------------------------------
        # ICE wait
        # ------------------------------------------------------------

        await self._wait_for_ice()

        # ------------------------------------------------------------
        # Remote SDP
        # ------------------------------------------------------------

        remote_sdp = self._build_remote_sdp(
            group_call_params
        )

        await self._pc.setRemoteDescription(

            RTCSessionDescription(
                sdp=remote_sdp,
                type="answer"
            )
        )

        self._connected = True

        logger.info(
            "✅ WebRTC connected!"
        )

        return {

            "sdp":
                self._pc.localDescription.sdp,

            "type":
                self._pc.localDescription.type,
        }

    # ----------------------------------------------------------------
    # Disconnect
    # ----------------------------------------------------------------

    async def disconnect(self):

        try:

            if self._track:
                self._track.stop()

            if self._pc:
                await self._pc.close()

        except Exception as e:

            logger.warning(
                f"Disconnect error: {e}"
            )

        self._connected = False

        logger.info(
            "WebRTC disconnected."
        )

    # ----------------------------------------------------------------
    # Switch Track
    # ----------------------------------------------------------------

    def switch_track(
        self,
        new_pipeline
    ):

        if self._track:

            self._track.switch_pipeline(
                new_pipeline
            )

    # ----------------------------------------------------------------
    # State
    # ----------------------------------------------------------------

    @property
    def is_connected(self) -> bool:
        return self._connected

    # ----------------------------------------------------------------
    # Callbacks
    # ----------------------------------------------------------------

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

        @self._pc.on(
            "iceconnectionstatechange"
        )
        async def on_ice():

            logger.info(
                f"ICE state: "
                f"{self._pc.iceConnectionState}"
            )

    # ----------------------------------------------------------------
    # ICE Wait
    # ----------------------------------------------------------------

    async def _wait_for_ice(
        self,
        timeout: float = 10.0
    ):

        loop = asyncio.get_event_loop()

        deadline = loop.time() + timeout

        while (
            self._pc.iceGatheringState
            != "complete"
        ):

            if loop.time() > deadline:

                logger.warning(
                    "ICE timeout."
                )

                break

            await asyncio.sleep(0.1)

    # ----------------------------------------------------------------
    # Build SDP
    # ----------------------------------------------------------------

    def _build_remote_sdp(
        self,
        params: dict
    ) -> str:

        transport = (
            params.get("transport")
            or {}
        )

        fingerprint = (
            transport.get("fingerprint")
            or {}
        )

        ufrag = transport.get(
            "ufrag",
            "telegram"
        )

        pwd = transport.get(
            "pwd",
            "telegram"
        )

        ssrc = params.get(
            "ssrc",
            0
        )

        # ------------------------------------------------------------
        # Fingerprint Fix
        # ------------------------------------------------------------

        fp_hash = fingerprint.get(
            "hash",
            "sha-256"
        )

        fp_value = fingerprint.get(
            "value",
            "00:00:00:00"
        )

        if " " in fp_value:

            parts = fp_value.split(
                " ",
                1
            )

            if len(parts) == 2:

                fp_hash = parts[0]
                fp_value = parts[1]

        # ------------------------------------------------------------
        # SDP
        # ------------------------------------------------------------

        sdp = (
            "v=0\r\n"
            "o=- 0 0 IN IP4 0.0.0.0\r\n"
            "s=-\r\n"
            "t=0 0\r\n"

            "m=audio 9 RTP/SAVPF 111\r\n"

            "c=IN IP4 0.0.0.0\r\n"

            f"a=ice-ufrag:{ufrag}\r\n"
            f"a=ice-pwd:{pwd}\r\n"

            f"a=fingerprint:{fp_hash} "
            f"{fp_value}\r\n"

            "a=setup:passive\r\n"

            "a=rtpmap:111 opus/48000/2\r\n"

            "a=rtcp-mux\r\n"

            "a=fmtp:111 "
            "minptime=10;"
            "useinbandfec=1\r\n"

            f"a=ssrc:{ssrc} "
            "cname:telegram\r\n"

            "a=sendrecv\r\n"
        )

        # ------------------------------------------------------------
        # ICE candidates
        # ------------------------------------------------------------

        for candidate in transport.get(
            "candidates",
            []
        ):

            sdp += (

                f"a=candidate:"
                f"{candidate.get('foundation', '1')} "
                f"1 "

                f"{candidate.get('protocol', 'udp')} "

                f"{candidate.get('priority', 2130706431)} "

                f"{candidate.get('ip', '0.0.0.0')} "

                f"{candidate.get('port', 0)} "

                f"typ "
                f"{candidate.get('type', 'host')}\r\n"
            )

        return sdp
