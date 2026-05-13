"""
core/group_call.py — Final Fixed Version

All fixes:
  1. webrtc.prepare() called BEFORE JoinGroupCall → real DTLS fingerprint sent
  2. Real SSRC from aiortc offer SDP used (not random) → Telegram matches RTP packets
  3. ssrc-groups field added to join_params → required by some Telegram servers
  4. Explicit unmute after connecting → Telegram auto-mutes new participants
  5. _parse_join_response checks UpdateGroupCallConnection.params directly
  6. _reconnecting flag always resets in finally block
  7. Max 5 reconnect attempts with exponential backoff
"""

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

    def __init__(self, client: Client, webrtc_engine: WebRTCEngine):
        self.client            = client
        self.webrtc            = webrtc_engine
        self._call_ref         = None
        self._chat_id: Optional[int] = None
        self._joined           = False
        self._transport_params = {}
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

            # PHASE 1: Create WebRTC PC, get real credentials from aiortc
            # Returns (ufrag, pwd, fingerprint, ssrc) — all extracted from offer SDP
            ufrag, pwd, fingerprint, ssrc = await self.webrtc.prepare(pipeline)

            fp_parts = fingerprint.split(" ", 1)
            fp_hash  = fp_parts[0] if len(fp_parts) == 2 else "sha-256"
            fp_value = fp_parts[1] if len(fp_parts) == 2 else ""

            join_params = {
                "ufrag":        ufrag,
                "pwd":          pwd,
                "fingerprints": [{"hash": fp_hash, "fingerprint": fp_value}],
                "ssrc":         ssrc,        # real SSRC — Telegram matches RTP by this
                "ssrc-groups":  [],          # required by some Telegram server versions
            }

            logger.info(f"SSRC (real, from aiortc): {ssrc}")
            logger.info(f"ICE ufrag sent to Telegram: {ufrag}")
            logger.info(f"DTLS fingerprint sent to Telegram: {fp_hash} {fp_value[:20]}...")

            # PHASE 2: JoinGroupCall with real credentials
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

            # PHASE 3: Complete WebRTC handshake (ICE + DTLS run async)
            ok = await self.webrtc.complete_connect(
                group_call_params=self._transport_params,
            )

            if ok:
                logger.info("✅ Audio connected — waiting for DTLS to complete...")
                # Explicitly unmute — Telegram auto-mutes new participants on some groups
                await asyncio.sleep(1)
                await self._unmute_self()

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

    async def _unmute_self(self):
        """Explicitly unmute — Telegram sometimes auto-mutes participants on join."""
        if not self._call_ref:
            return
        try:
            me   = await self.client.get_me()
            peer = await self.client.resolve_peer(me.id)
            await self.client.invoke(
                raw.functions.phone.EditGroupCallParticipant(
                    call=self._call_ref,
                    participant=peer,
                    muted=False,
                )
            )
            logger.info("✅ Participant unmuted — audio should now relay to members.")
        except Exception as e:
            logger.warning(f"Unmute failed: {e}")

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
        Telegram returns UpdateGroupCallConnection which has `params` as a
        direct field — not nested under `update.call.params`.
        Logs all update types if parsing fails so we can debug further.
        """
        try:
            for update in result.updates:
                update_type = type(update).__name__

                # Primary: UpdateGroupCallConnection — params is a direct field
                if hasattr(update, "params") and hasattr(update.params, "data"):
                    logger.info(f"Transport params found in {update_type}.params")
                    return json.loads(update.params.data)

                # Fallback: older API shape
                if hasattr(update, "call") and hasattr(update.call, "params"):
                    logger.info(f"Transport params found in {update_type}.call.params")
                    return json.loads(update.call.params.data)

            types = [type(u).__name__ for u in result.updates]
            logger.warning(f"Transport params NOT found. Update types: {types}")

        except Exception as e:
            logger.warning(f"_parse_join_response error: {e}")

        return {}
