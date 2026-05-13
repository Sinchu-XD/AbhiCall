"""
core/group_call.py

FIX: Reverted to single-phase WebRTC connect (original flow, proven to work).
     join() now generates real random ICE credentials itself and sends them to
     Telegram, then passes them to webrtc.connect() where they are patched into
     the offer SDP — fixing "Consent to send expired" without breaking DTLS.
"""

import asyncio
import json
import logging
import random
import string
from typing import Optional

from pyrogram import Client
from pyrogram import raw

from webrtc.engine import WebRTCEngine

logger = logging.getLogger(__name__)


def _random_ufrag(length: int = 4) -> str:
    return "".join(random.choices(string.ascii_letters + string.digits, k=length))


def _random_pwd(length: int = 22) -> str:
    return "".join(random.choices(string.ascii_letters + string.digits, k=length))


class GroupCallManager:

    def __init__(self, client: Client, webrtc_engine: WebRTCEngine):
        self.client            = client
        self.webrtc            = webrtc_engine
        self._call_ref         = None
        self._chat_id: Optional[int] = None
        self._joined           = False
        self._transport_params = {}
        self._my_ssrc          = 0
        self._my_ufrag         = ""
        self._my_pwd           = ""
        self._pipeline         = None
        self._reconnecting     = False

        self.webrtc.set_reconnect_callback(self._on_webrtc_failed)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

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

        # Generate credentials here — no WebRTC PC needed yet
        self._my_ssrc  = random.randint(1_000_000, 0x7FFFFFFF)
        self._my_ufrag = _random_ufrag()
        self._my_pwd   = _random_pwd()

        join_params = {
            "ufrag":        self._my_ufrag,
            "pwd":          self._my_pwd,
            "fingerprints": [],
            "ssrc":         self._my_ssrc,
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

        self._transport_params = self._parse_join_response(result)

        logger.info("Transport params received.")
        logger.info(f"SSRC (local): {self._my_ssrc}")
        logger.info(f"ICE ufrag sent to Telegram: {self._my_ufrag}")
        logger.info(f"ICE candidates: {len(self._transport_params.get('transport', {}).get('candidates', []))}")

        self._joined = True
        logger.info(f"✅ Joined VC in chat {chat_id}")
        return True

    async def connect_audio(self, pipeline) -> bool:
        if not self._joined:
            logger.error("Pehle join() karo!")
            return False
        try:
            self._pipeline = pipeline
            ok = await self.webrtc.connect(
                group_call_params=self._transport_params,
                pipeline=pipeline,
                pre_ufrag=self._my_ufrag,
                pre_pwd=self._my_pwd,
            )
            if ok:
                logger.info("✅ Audio connected!")
            return ok
        except Exception as e:
            logger.error(f"WebRTC connect error: {e}")
            return False

    async def leave(self):
        self._pipeline = None
        if self._call_ref and self._joined:
            try:
                await self.client.invoke(
                    raw.functions.phone.LeaveGroupCall(
                        call=self._call_ref, source=0
                    )
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

    # ------------------------------------------------------------------
    # Auto-reconnect
    # ------------------------------------------------------------------

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

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    async def _get_active_call(self, chat_id: int):
        try:
            peer = await self.client.resolve_peer(chat_id)
            if isinstance(peer, raw.types.InputPeerChannel):
                full     = await self.client.invoke(
                    raw.functions.channels.GetFullChannel(channel=peer)
                )
                call_ref = full.full_chat.call
            elif isinstance(peer, raw.types.InputPeerChat):
                full     = await self.client.invoke(
                    raw.functions.messages.GetFullChat(chat_id=peer.chat_id)
                )
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
                "candidates":   [],
                "fingerprints": [],
                "ufrag":        "",
                "pwd":          "",
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
