"""
core/group_call.py
------------------
Pyrogram raw API se Telegram Group Call join/leave karta hai.
WebRTC engine se audio stream connect karta hai.
"""

import json
import random
import logging

from pyrogram import Client
from pyrogram import raw

logger = logging.getLogger(__name__)


class GroupCallManager:

    def __init__(
        self,
        client: Client,
        webrtc_engine
    ):

        self.client = client
        self.webrtc = webrtc_engine

        self._call_ref = None
        self._chat_id = None
        self._joined = False
        self._ssrc = None

    # ------------------------------------------------------------------
    # Join
    # ------------------------------------------------------------------

    async def join(self, chat_id: int) -> bool:
        """
        Group Call join karo.
        """

        try:

            self._chat_id = chat_id

            # ----------------------------------------------------------
            # Active VC lo
            # ----------------------------------------------------------

            call = await self._get_active_call(
                chat_id
            )

            if not call:

                logger.error(
                    "Active Voice Chat nahi mila."
                )

                return False

            self._call_ref = raw.types.InputGroupCall(
                id=call.id,
                access_hash=call.access_hash,
            )

            # ----------------------------------------------------------
            # Unique SSRC
            # ----------------------------------------------------------

            self._ssrc = random.randint(
                100000,
                2_147_483_647
            )

            # ----------------------------------------------------------
            # Join params
            # ----------------------------------------------------------

            join_params = {
                "ufrag": "telegram",
                "pwd": "telegram",
                "fingerprints": [],
                "ssrc": self._ssrc,
            }

            # ----------------------------------------------------------
            # Join VC
            # ----------------------------------------------------------

            result = await self.client.invoke(

                raw.functions.phone.JoinGroupCall(

                    call=self._call_ref,

                    params=raw.types.DataJSON(
                        data=json.dumps(join_params)
                    ),

                    muted=False,

                    video_stopped=True,

                    join_as=raw.types.InputPeerSelf(),
                )
            )

            transport_params = (
                self._parse_join_response(
                    result
                )
            )

            logger.info(
                "Transport params received."
            )

            logger.info(
                f"SSRC: {self._ssrc}"
            )

            self._joined = True

            logger.info(
                f"✅ Joined VC in chat {chat_id}"
            )

            return True

        except Exception as e:

            logger.error(
                f"Join error: {e}"
            )

            return False

    # ------------------------------------------------------------------
    # Connect Audio
    # ------------------------------------------------------------------

    async def connect_audio(
        self,
        pipeline
    ) -> bool:

        if not self._joined:

            logger.error(
                "Pehle join() karo!"
            )

            return False

        try:

            await self.webrtc.connect(

                group_call_params={
                    "transport": {},
                    "ssrc": self._ssrc,
                },

                pipeline=pipeline,
            )

            logger.info(
                "✅ Audio connected!"
            )

            return True

        except Exception as e:

            logger.error(
                f"WebRTC connect error: {e}"
            )

            return False

    # ------------------------------------------------------------------
    # Leave
    # ------------------------------------------------------------------

    async def leave(self):

        try:

            if self._call_ref and self._joined:

                await self.client.invoke(

                    raw.functions.phone.LeaveGroupCall(

                        call=self._call_ref,

                        source=self._ssrc
                        or 0,
                    )
                )

        except Exception as e:

            logger.warning(
                f"Leave error: {e}"
            )

        try:

            await self.webrtc.disconnect()

        except Exception as e:

            logger.warning(
                f"Disconnect error: {e}"
            )

        self._joined = False
        self._call_ref = None
        self._chat_id = None
        self._ssrc = None

        logger.info(
            "Left group call."
        )

    # ------------------------------------------------------------------
    # Mute
    # ------------------------------------------------------------------

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

    # ------------------------------------------------------------------
    # Get Active VC
    # ------------------------------------------------------------------

    async def _get_active_call(
        self,
        chat_id: int
    ):

        try:

            peer = await self.client.resolve_peer(
                chat_id
            )

            # ----------------------------------------------------------
            # Supergroup
            # ----------------------------------------------------------

            if isinstance(
                peer,
                raw.types.InputPeerChannel
            ):

                full = await self.client.invoke(

                    raw.functions.channels.GetFullChannel(
                        channel=peer
                    )
                )

                call_ref = full.full_chat.call

            # ----------------------------------------------------------
            # Normal Group
            # ----------------------------------------------------------

            elif isinstance(
                peer,
                raw.types.InputPeerChat
            ):

                full = await self.client.invoke(

                    raw.functions.messages.GetFullChat(
                        chat_id=peer.chat_id
                    )
                )

                call_ref = full.full_chat.call

            else:

                logger.error(
                    "Sirf groups/supergroups supported."
                )

                return None

            if not call_ref:

                logger.error(
                    "Active VC nahi mila."
                )

                return None

            # ----------------------------------------------------------
            # Full Call Object
            # ----------------------------------------------------------

            result = await self.client.invoke(

                raw.functions.phone.GetGroupCall(

                    call=call_ref,

                    limit=1,
                )
            )

            return result.call

        except Exception as e:

            logger.error(
                f"Get active call error: {e}"
            )

            return None

    # ------------------------------------------------------------------
    # Parse Join Response
    # ------------------------------------------------------------------

    def _parse_join_response(
        self,
        result
    ) -> dict:

        params = {

            "transport": {

                "candidates": [],

                "fingerprint": {
                    "hash": "sha-256",
                    "value": "",
                },

                "ufrag": "",
                "pwd": "",
            },

            "ssrc": self._ssrc,
            "ssrc_group": [],
        }

        try:

            for update in result.updates:

                if hasattr(update, "params"):

                    data = json.loads(
                        update.params.data
                    )

                    params["transport"] = data.get(
                        "transport",
                        params["transport"]
                    )

                    params["ssrc"] = data.get(
                        "ssrc",
                        self._ssrc
                    )

        except Exception as e:

            logger.warning(
                f"Parse response error: {e}"
            )

        return params

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def is_joined(self) -> bool:
        return self._joined
