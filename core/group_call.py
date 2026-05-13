"""
core/group_call.py — Fixed v2

FIXES:
  1. _parse_join_response now checks UpdateGroupCallConnection.params directly
     (not update.call.params) — this is why ICE candidates were always 0.
  2. connect_audio() calls webrtc.prepare() for real DTLS fingerprint.
  3. _reconnecting flag always resets in finally block.
  4. Max 5 reconnect attempts with backoff.
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

MAX_RECONNECT_ATTEMPTS = 5


class GroupCallManager:

    def __init__(self, client: Client, webrtc_engine: WebRTCEngine):
        self.client            = client
        self.webrtc            = webrtc_engine
        self._call_ref         = None
        self._chat_id: Optional[int] = None
        self._joined           = False
        self._transport_params = {}
        self._my_ssrc          = 0
        self._pipeline         = None
        self._reconnecting     = False
        self._reconnect_count  = 0

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
        return True

    async def connect_audio(self, pipeline) -> bool:
        if not self._call_ref:
            logger.error("Pehle join() karo!")
            return False

        try:
            self._pipeline = pipeline

            # PHASE 1: Create WebRTC PC, gather ICE, extract real fingerprint
            ufrag, pwd, fingerprint = await self.webrtc.prepare(pipeline)

            fp_parts = fingerprint.split(" ", 1)
            fp_hash  = fp_parts[0] if len(fp_parts) == 2 else "sha-256"
            fp_value = fp_parts[1] if len(fp_parts) == 2 else ""

            self._my_ssrc = random.randint(1_000_000, 0x7FFFFFFF)

            join_params = {
                "ufrag":        ufrag,
                "pwd":          pwd,
                "fingerprints": [{"hash": fp_hash, "fingerprint": fp_value}],
                "ssrc":         self._my_ssrc,
            }

            logger.info(f"SSRC (local): {self._my_ssrc}")
            logger.info(f"ICE ufrag sent to Telegram: {ufrag}")
            logger.info(f"DTLS fingerprint sent to Telegram: {fp_hash} {fp_value[:20]}...")

            # PHASE 2: JoinGroupCall with real fingerprint
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

            candidates_count = len(
                self._transport_params.get("transport", {}).get("candidates", [])
            )
            logger.info(f"ICE candidates from Telegram: {candidates_count}")
            logger.info(f"✅ Joined VC in chat {self._chat_id}")
            self._joined = True

            # PHASE 3: Complete WebRTC handshake
            ok = await self.webrtc.complete_connect(
                group_call_params=self._transport_params,
            )
            if ok:
                logger.info("✅ Audio connected!")
            return ok

        except Exception as e:
            logger.error(f"connect_audio error: {e}")
            return False

    async def leave(self):
        self._pipeline = None
        self._joined   = False
        if self._call_ref:
            try:
                await self.client.invoke(
                    raw.functions.phone.LeaveGroupCall(
                        call=self._call_ref, source=0
                    )
                )
            except Exception as e:
                logger.warning(f"Leave error: {e}")
        await self.webrtc.disconnect()
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
        if self._reconnecting or not self._chat_id:
            return

        pipeline = self._pipeline
        chat_id  = self._chat_id
        if not pipeline:
            return

        self._reconnecting    = True
        self._joined          = False
        self._reconnect_count += 1

        if self._reconnect_count > MAX_RECONNECT_ATTEMPTS:
            logger.error(f"Max reconnect attempts ({MAX_RECONNECT_ATTEMPTS}) reached. Giving up.")
            self._reconnecting = False
            return

        delay = min(3 * self._reconnect_count, 15)
        logger.warning(
            f"WebRTC dropped — reconnecting in {delay}s "
            f"(attempt {self._reconnect_count}/{MAX_RECONNECT_ATTEMPTS})..."
        )

        try:
            await asyncio.sleep(delay)

            ok = await self.join(chat_id)
            if not ok:
                logger.error("Auto-reconnect: join() failed")
                return

            ok = await self.connect_audio(pipeline)
            if ok:
                logger.info("✅ Auto-reconnect successful — audio resumed!")
                self._reconnect_count = 0
            else:
                logger.error("Auto-reconnect: connect_audio() failed")

        finally:
            self._reconnecting = False

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    async def _get_active_call(self, chat_id: int):
        try:
            full = await self.client.invoke(
                raw.functions.channels.GetFullChannel(
                    channel=await self.client.resolve_peer(chat_id)
                )
            )
            return getattr(full.full_chat, "call", None)
        except Exception as e:
            logger.error(f"GetFullChannel failed: {e}")
            return None

    def _parse_join_response(self, result) -> dict:
        """
        FIX: Telegram returns UpdateGroupCallConnection which has `params`
        directly on the update object — NOT nested under `update.call.params`.
        Old code only checked `.call.params` and always got nothing → 0 candidates.
        """
        try:
            for update in result.updates:
                update_type = type(update).__name__

                # Primary: UpdateGroupCallConnection — params is a direct field
                if hasattr(update, "params") and hasattr(update.params, "data"):
                    logger.info(f"Transport params found in {update_type}.params")
                    return json.loads(update.params.data)

                # Fallback: older API shape where it lived under .call.params
                if hasattr(update, "call") and hasattr(update.call, "params"):
                    logger.info(f"Transport params found in {update_type}.call.params")
                    return json.loads(update.call.params.data)

            # Nothing found — log all update types so we can debug further
            types = [type(u).__name__ for u in result.updates]
            logger.warning(f"Transport params NOT found. Update types received: {types}")

        except Exception as e:
            logger.warning(f"_parse_join_response error: {e}")

        return {}
