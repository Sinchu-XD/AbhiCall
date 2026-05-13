"""
core/group_call.py

FIXES APPLIED:
  1. join() calls webrtc.prepare_offer() FIRST to get real ICE ufrag/pwd,
     then sends those to Telegram — fixes "Consent to send expired".
  2. connect_audio() calls webrtc.finalize_connection() with Telegram's params.
  3. my_ssrc stored and passed through for correct SDP generation.
  4. Auto-reconnect on WebRTC failure.
"""

import asyncio
import json
import logging
import random
from typing import Optional

from pyrogram import Client
from pyrogram import raw

from webrtc.engine import WebRTCEngine

logger = logging.getLogger(__name__)


class GroupCallManager:

    def __init__(self, client: Client, webrtc_engine: WebRTCEngine):
        self.client            = client
        self.webrtc            = webrtc_engine
        self._call_ref         = None
        self._chat_id: Optional[int] = None
        self._joined           = False
        self._transport_params = {}
        self._pipeline         = None
        self._reconnecting     = False

        self.webrtc.set_reconnect_callback(self._on_webrtc_failed)

    async def join(self, chat_id: int) -> bool:
        if self._joined:
            await self.leave()

        self._chat_id = chat_id

        call = await self._get_active_call(chat_id)
        if not call:
            logger.error("Is chat mein koi active Voice Chat nahi hai!")
            return False

        self._call_ref = raw.types.InputGroupCall(
            id=call.id,
            access_hash=call.access_hash,
        )

        my_ssrc = random.randint(1_000_000, 0x7FFFFFFF)

        # FIX: get real ICE credentials from aiortc BEFORE joining Telegram
        try:
            _offer_sdp, my_ufrag, my_pwd = await self.webrtc.prepare_offer(my_ssrc)
        except Exception as e:
            logger.error(f"WebRTC offer preparation failed: {e}")
            return False

        # FIX: send real ufrag/pwd so Telegram validates our STUN requests correctly
        join_params = {
            "ufrag":        my_ufrag,
            "pwd":          my_pwd,
            "fingerprints": [],
            "ssrc":         my_ssrc,
        }

        try:
            result = await self.client.invoke(
                raw.functions.phone.JoinGroupCall(
                    call=self._call_ref,
                    params=raw.types.DataJSON(data=json.dumps(join_params)),
                    muted=False,
                    video_stopped=True,
                    join_as=raw.types.InputPeerSelf(),
                )
            )
        except Exception as e:
            logger.error(f"JoinGroupCall failed: {e}")
            return False

        transport_params = self._parse_join_response(result)
        transport_params["local_ssrc"] = my_ssrc
        self._transport_params = transport_params

        logger.info("Transport params received.")
        logger.info(f"SSRC (local): {my_ssrc}")
        logger.info(f"ICE candidates: {len(transport_params.get('transport', {}).get('candidates', []))}")

        self._joined = True
        logger.info(f"✅ Joined VC in chat {chat_id}")
        return True

    async def connect_audio(self, pipeline) -> bool:
        if not self._joined:
            logger.error("Pehle join() karo!")
            return False
        try:
            success = await self.webrtc.finalize_connection(
                transport_params=self._transport_params,
                pipeline=pipeline,
            )
            if success:
                self._pipeline = pipeline
                logger.info("✅ Audio connected!")
            return success
        except Exception as e:
            logger.error(f"WebRTC connect error: {e}")
            return False

    async def leave(self):
        self._pipeline = None
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

    @property
    def is_joined(self) -> bool:
        return self._joined

    async def _on_webrtc_failed(self):
        if self._reconnecting or not self._joined:
            return

        pipeline = self._pipeline
        chat_id  = self._chat_id
        if not pipeline or not chat_id:
            return

        self._reconnecting = True
        logger.warning("WebRTC dropped — auto-reconnecting in 3s...")
        await asyncio.sleep(3)

        try:
            ok = await self.join(chat_id)
            if not ok:
                logger.error("Auto-reconnect: join() failed")
                return

            ok = await self.connect_audio(pipeline)
            if ok:
                logger.info("✅ Auto-reconnect successful — audio resumed!")
            else:
                logger.error("Auto-reconnect: connect_audio() failed")
        except Exception as e:
            logger.error(f"Auto-reconnect exception: {e}")
        finally:
            self._reconnecting = False

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
