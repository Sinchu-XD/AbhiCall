"""
core/group_call.py
"""

import asyncio
import json
import logging
from typing import Optional

from pyrogram import Client
from pyrogram import raw

logger = logging.getLogger(__name__)


class GroupCallManager:
    def __init__(self, client: Client, webrtc_engine):
        self.client             = client
        self.webrtc             = webrtc_engine
        self._call_ref          = None
        self._chat_id           = None
        self._joined            = False
        self._transport_params  = {}   # ✅ Real params store karo

    async def join(self, chat_id: int) -> bool:
        self._chat_id = chat_id

        call = await self._get_active_call(chat_id)
        if not call:
            logger.error("Is chat mein koi active Voice Chat nahi hai!")
            return False

        self._call_ref = raw.types.InputGroupCall(
            id=call.id,
            access_hash=call.access_hash,
        )

        join_params = {
            "ufrag": "telegram", "pwd": "telegram",
            "fingerprints": [], "ssrc": 0,
        }

        result = await self.client.invoke(
            raw.functions.phone.JoinGroupCall(
                call=self._call_ref,
                params=raw.types.DataJSON(data=json.dumps(join_params)),
                muted=False,
                video_stopped=True,
                join_as=raw.types.InputPeerSelf(),
            )
        )

        # ✅ FIX: Parse karo AUR store karo
        transport_params       = self._parse_join_response(result)
        self._transport_params = transport_params

        logger.info("Transport params received.")
        logger.info(f"SSRC: {transport_params.get('ssrc', 0)}")
        logger.info(f"ICE candidates: {len(transport_params.get('transport', {}).get('candidates', []))}")

        self._joined = True
        logger.info(f"✅ Joined VC in chat {chat_id}")
        return True

    async def connect_audio(self, pipeline) -> bool:
        if not self._joined:
            logger.error("Pehle join() karo!")
            return False
        try:
            await self.webrtc.connect(
                group_call_params=self._transport_params,  # ✅ REAL PARAMS, empty nahi
                pipeline=pipeline,
            )
            logger.info("✅ Audio connected!")
            return True
        except Exception as e:
            logger.error(f"WebRTC connect error: {e}")
            return False

    async def leave(self):
        if self._call_ref and self._joined:
            try:
                await self.client.invoke(
                    raw.functions.phone.LeaveGroupCall(call=self._call_ref, source=0)
                )
            except Exception as e:
                logger.warning(f"Leave error: {e}")
        await self.webrtc.disconnect()
        self._joined           = False
        self._call_ref         = None
        self._transport_params = {}
        logger.info("Left group call.")

    async def mute(self, muted: bool):
        if not self._call_ref:
            return
        try:
            me   = await self.client.get_me()
            peer = await self.client.resolve_peer(me.id)
            await self.client.invoke(
                raw.functions.phone.EditGroupCallParticipant(
                    call=self._call_ref, participant=peer, muted=muted,
                )
            )
        except Exception as e:
            logger.warning(f"Mute error: {e}")

    async def _get_active_call(self, chat_id: int):
        try:
            peer = await self.client.resolve_peer(chat_id)
            if isinstance(peer, raw.types.InputPeerChannel):
                full     = await self.client.invoke(raw.functions.channels.GetFullChannel(channel=peer))
                call_ref = full.full_chat.call
            elif isinstance(peer, raw.types.InputPeerChat):
                full     = await self.client.invoke(raw.functions.messages.GetFullChat(chat_id=peer.chat_id))
                call_ref = full.full_chat.call
            else:
                logger.error("Sirf groups aur supergroups support hain.")
                return None

            if not call_ref:
                logger.error("Koi active Voice Chat nahi mila.")
                return None

            result = await self.client.invoke(
                raw.functions.phone.GetGroupCall(call=call_ref, limit=1)
            )
            return result.call
        except Exception as e:
            logger.error(f"Active call dhundhne mein error: {e}")
            return None

    def _parse_join_response(self, result) -> dict:
        params = {
            "transport": {
                "candidates": [], "fingerprint": {"hash": "sha-256", "value": ""},
                "ufrag": "", "pwd": "",
            },
            "ssrc": 0, "ssrc_group": [],
        }
        try:
            for update in result.updates:
                if hasattr(update, "params") and update.params:
                    data = json.loads(update.params.data)
                    if "transport" in data:
                        params["transport"] = data["transport"]
                    if "ssrc" in data:
                        params["ssrc"] = data["ssrc"]
                    if "ssrc-groups" in data:
                        params["ssrc_group"] = data["ssrc-groups"]
                    break
        except Exception as e:
            logger.warning(f"Response parse error: {e}")
        return params

    @property
    def is_joined(self) -> bool:
        return self._joined
