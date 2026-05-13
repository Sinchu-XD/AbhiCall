import asyncio
import json
import logging
from typing import Optional

from pyrogram import Client
from pyrogram import raw

from webrtc.engine import WebRTCEngine

logger = logging.getLogger(__name__)

MAX_RECONNECT_ATTEMPTS = 5


class GroupCallManager:

    def __init__(
        self,
        client: Client,
        webrtc_engine: WebRTCEngine
    ):

        self.client = client
        self.webrtc = webrtc_engine

        self._call_ref = None
        self._chat_id: Optional[int] = None

        self._joined = False

        self._pipeline = None

        self._transport_params = {}

        self._reconnecting = False
        self._reconnect_count = 0

        self.webrtc.set_reconnect_callback(
            self._on_webrtc_failed
        )

        self.webrtc.set_connected_callback(
            self._on_dtls_connected
        )

    async def join(
        self,
        chat_id: int
    ) -> bool:

        if self._joined:
            await self.leave()

        self._chat_id = chat_id

        call = await self._get_active_call(
            chat_id
        )

        if not call:

            logger.error(
                "Is chat mein active VC nahi hai!"
            )

            return False

        self._call_ref = raw.types.InputGroupCall(
            id=call.id,
            access_hash=call.access_hash,
        )

        return True

    async def connect_audio(
        self,
        pipeline
    ) -> bool:

        if not self._call_ref:

            logger.error(
                "Pehle join() karo!"
            )

            return False

        try:

            self._pipeline = pipeline

            (
                ufrag,
                pwd,
                fingerprint,
                ssrc
            ) = await self.webrtc.prepare(
                pipeline
            )

            fp_hash = "sha-256"
            fp_value = ""

            if " " in fingerprint:

                fp_hash, fp_value = (
                    fingerprint.split(
                        " ",
                        1
                    )
                )

            join_params = {

                "ufrag": ufrag,

                "pwd": pwd,

                "fingerprints": [
                    {
                        "hash": fp_hash,
                        "fingerprint": fp_value,
                    }
                ],

                "ssrc": ssrc,

                "ssrc-groups": [],
            }

            logger.info(
                f"SSRC (real): {ssrc}"
            )

            logger.info(
                f"ICE ufrag: {ufrag}"
            )

            logger.info(
                f"DTLS fingerprint: "
                f"{fp_hash} "
                f"{fp_value[:20]}..."
            )

            result = await self.client.invoke(

                raw.functions.phone.JoinGroupCall(

                    call=self._call_ref,

                    params=raw.types.DataJSON(
                        data=json.dumps(
                            join_params
                        )
                    ),

                    muted=False,

                    video_stopped=True,

                    join_as=raw.types.InputPeerSelf(),
                )
            )

            self._transport_params = (
                self._parse_join_response(
                    result
                )
            )

            transport = (
                self._transport_params
                .get("transport", {})
            )

            fingerprints = transport.get(
                "fingerprints",
                []
            )

            candidates = transport.get(
                "candidates",
                []
            )

            if not fingerprints:

                logger.error(
                    "Telegram returned NO fingerprints"
                )

                return False

            if not candidates:

                logger.error(
                    "Telegram returned NO ICE candidates"
                )

                return False

            logger.info(
                f"ICE candidates from Telegram: "
                f"{len(candidates)}"
            )

            logger.info(
                f"✅ Joined VC in chat "
                f"{self._chat_id}"
            )

            self._joined = True

            ok = await self.webrtc.complete_connect(
                group_call_params=
                self._transport_params
            )

            if ok:

                logger.info(
                    "✅ WebRTC handshake started"
                )

            return ok

        except Exception as e:

            logger.exception(
                f"connect_audio error: {e}"
            )

            return False

    async def leave(self):

        self._pipeline = None
        self._joined = False

        if self._call_ref:

            try:

                await self.client.invoke(

                    raw.functions.phone.LeaveGroupCall(
                        call=self._call_ref,
                        source=0
                    )
                )

            except Exception as e:

                logger.warning(
                    f"Leave error: {e}"
                )

        await self.webrtc.disconnect()

        self._call_ref = None
        self._transport_params = {}

        logger.info(
            "Left group call."
        )

    async def mute(
        self,
        muted: bool
    ):

        if not self._call_ref:
            return

        try:

            me = await self.client.get_me()

            peer = await self.client.resolve_peer(
                me.id
            )

            await self.client.invoke(

                raw.functions.phone.EditGroupCallParticipant(

                    call=self._call_ref,

                    participant=peer,

                    muted=muted,
                )
            )

        except Exception as e:

            logger.warning(
                f"Mute error: {e}"
            )

    async def _on_dtls_connected(self):

        if not self._call_ref:
            return

        try:

            me = await self.client.get_me()

            peer = await self.client.resolve_peer(
                me.id
            )

            await asyncio.sleep(0.5)

            await self.client.invoke(

                raw.functions.phone.EditGroupCallParticipant(

                    call=self._call_ref,

                    participant=peer,

                    muted=False,
                )
            )

            logger.info(
                "✅ Participant unmuted"
            )

        except Exception as e:

            logger.warning(
                f"Unmute failed: {e}"
            )

    @property
    def is_joined(self) -> bool:
        return self._joined

    async def _on_webrtc_failed(self):

        if self._reconnecting:
            return

        if not self._chat_id:
            return

        pipeline = self._pipeline

        if not pipeline:
            return

        self._reconnecting = True

        self._joined = False

        self._reconnect_count += 1

        if (
            self._reconnect_count
            > MAX_RECONNECT_ATTEMPTS
        ):

            logger.error(
                "Max reconnect attempts reached"
            )

            self._reconnecting = False

            return

        delay = min(
            3 * self._reconnect_count,
            15
        )

        logger.warning(
            f"WebRTC reconnect in {delay}s "
            f"(attempt "
            f"{self._reconnect_count}/"
            f"{MAX_RECONNECT_ATTEMPTS})"
        )

        try:

            await asyncio.sleep(delay)

            ok = await self.join(
                self._chat_id
            )

            if not ok:

                logger.error(
                    "Reconnect join failed"
                )

                return

            ok = await self.connect_audio(
                pipeline
            )

            if ok:

                logger.info(
                    "✅ Auto reconnect success"
                )

                self._reconnect_count = 0

            else:

                logger.error(
                    "Reconnect connect_audio failed"
                )

        finally:

            self._reconnecting = False

    async def _get_active_call(
        self,
        chat_id: int
    ):

        try:

            full = await self.client.invoke(

                raw.functions.channels.GetFullChannel(

                    channel=await self.client.resolve_peer(
                        chat_id
                    )
                )
            )

            return getattr(
                full.full_chat,
                "call",
                None
            )

        except Exception as e:

            logger.error(
                f"GetFullChannel failed: {e}"
            )

            return None

    def _parse_join_response(
        self,
        result
    ) -> dict:

        try:

            for update in result.updates:

                if hasattr(update, "params"):

                    if hasattr(
                        update.params,
                        "data"
                    ):

                        return json.loads(
                            update.params.data
                        )

                if hasattr(update, "call"):

                    if hasattr(
                        update.call,
                        "params"
                    ):

                        return json.loads(
                            update.call.params.data
                        )

        except Exception as e:

            logger.exception(
                f"Join response parse failed: {e}"
            )

        return {}
