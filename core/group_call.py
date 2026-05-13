"""
core/group_call.py
------------------
Pyrogram raw API se Telegram Group Call join/leave karta hai.
WebRTC engine se audio stream connect karta hai.

Telethon ki jagah Pyrogram use kar raha hai:
  client(Request())  →  await client.invoke(raw.functions....)
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
        self.client    = client
        self.webrtc    = webrtc_engine
        self._call_ref = None   # raw.types.InputGroupCall
        self._chat_id  = None
        self._joined   = False

    # ------------------------------------------------------------------
    # Join / Leave
    # ------------------------------------------------------------------

    async def join(self, chat_id: int) -> bool:
        """
        Group Call mein join karo.

        Steps:
          1. Chat ka active group call dhundo
          2. phone.joinGroupCall (Pyrogram raw) call karo
          3. Telegram se transport params lo
          4. WebRTC connect karo
        """
        self._chat_id = chat_id

        # Step 1: Active call dhundo
        call = await self._get_active_call(chat_id)
        if not call:
            logger.error("Is chat mein koi active Voice Chat nahi hai!")
            return False

        self._call_ref = raw.types.InputGroupCall(
            id=call.id,
            access_hash=call.access_hash,
        )

        # Step 2: Join request (dummy params pehle — SDP exchange baad mein)
        join_params = {
            "ufrag":        "telegram",
            "pwd":          "telegram",
            "fingerprints": [],
            "ssrc":         0,
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

        # Step 3: Transport params parse karo
        transport_params = self._parse_join_response(result)
        logger.info(f"Transport params received: {list(transport_params.keys())}")

        self._joined = True
        logger.info(f"✅ Joined group call in chat {chat_id}")
        return True

    async def connect_audio(self, pipeline) -> bool:
        """
        Audio pipeline ko WebRTC se connect karo.
        /play command ke baad yeh call hota hai.
        """
        if not self._joined:
            logger.error("Pehle join() karo!")
            return False

        try:
            await self.webrtc.connect(
                group_call_params={"transport": {}, "ssrc": 0},
                pipeline=pipeline,
            )
            logger.info("✅ Audio connected via WebRTC!")
            return True
        except Exception as e:
            logger.error(f"WebRTC connect error: {e}")
            return False

    async def leave(self):
        """Group Call chhodo."""
        if self._call_ref and self._joined:
            try:
                await self.client.invoke(
                    raw.functions.phone.LeaveGroupCall(
                        call=self._call_ref,
                        source=0,
                    )
                )
            except Exception as e:
                logger.warning(f"Leave error: {e}")
        await self.webrtc.disconnect()
        self._joined   = False
        self._call_ref = None
        logger.info("Left group call.")

    async def mute(self, muted: bool):
        """Apne aap ko mute/unmute karo."""
        if not self._call_ref:
            return
        try:
            me = await self.client.get_me()
            peer = await self.client.resolve_peer(me.id)
            await self.client.invoke(
                raw.functions.phone.EditGroupCallParticipant(
                    call=self._call_ref,
                    participant=peer,
                    muted=muted,
                )
            )
        except Exception as e:
            logger.warning(f"Mute error: {e}")

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    async def _get_active_call(self, chat_id: int):
        """Chat mein active Voice Chat ka InputGroupCall dhundo."""
        try:
            peer = await self.client.resolve_peer(chat_id)

            # Channel / supergroup
            if isinstance(peer, raw.types.InputPeerChannel):
                full = await self.client.invoke(
                    raw.functions.channels.GetFullChannel(channel=peer)
                )
                call_ref = full.full_chat.call

            # Regular group
            elif isinstance(peer, raw.types.InputPeerChat):
                full = await self.client.invoke(
                    raw.functions.messages.GetFullChat(chat_id=peer.chat_id)
                )
                call_ref = full.full_chat.call

            else:
                logger.error("Supported nahi: sirf groups aur supergroups mein kaam karta hai.")
                return None

            if not call_ref:
                logger.error("Koi active Voice Chat nahi mila.")
                return None

            # Full GroupCall object lo
            result = await self.client.invoke(
                raw.functions.phone.GetGroupCall(
                    call=call_ref,
                    limit=1,
                )
            )
            return result.call

        except Exception as e:
            logger.error(f"Active call dhundhne mein error: {e}")
            return None

    def _parse_join_response(self, result) -> dict:
        """
        joinGroupCall response se transport params nikalte hain.
        Updates mein UpdateGroupCallParticipants hota hai.
        """
        params = {
            "transport": {
                "candidates":  [],
                "fingerprint": {"hash": "sha-256", "value": ""},
                "ufrag":       "",
                "pwd":         "",
            },
            "ssrc":       0,
            "ssrc_group": [],
        }
        try:
            for update in result.updates:
                if hasattr(update, "params"):
                    data = json.loads(update.params.data)
                    params["transport"] = data.get("transport", params["transport"])
                    params["ssrc"]      = data.get("ssrc", 0)
        except Exception as e:
            logger.warning(f"Response parse error: {e}")
        return params

    @property
    def is_joined(self) -> bool:
        return self._joined
