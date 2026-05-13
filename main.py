"""
main.py — Telegram VC Music Bot (Pyrogram + Custom WebRTC)
Bot       : Commands handle karta hai (bot token se)
Assistant : Real account se VC join + stream karta hai (MTProto + aiortc)

Run: python main.py
"""

import asyncio
import logging
import os
from dotenv import load_dotenv

from pyrogram import Client, idle

from audio.pipeline import QueueManager
from webrtc.engine import WebRTCEngine
from core.group_call import GroupCallManager
from bot.handlers import register_handlers

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%H:%M:%S",
)

logger = logging.getLogger("main")

load_dotenv()

API_ID            = int(os.getenv("API_ID", "0"))
API_HASH          = os.getenv("API_HASH", "")
BOT_TOKEN         = os.getenv("BOT_TOKEN", "")
ASSISTANT_SESSION = os.getenv("ASSISTANT_SESSION", "")
STUN_URL          = os.getenv("STUN_SERVER",   "stun:stun.l.google.com:19302")
TURN_URL          = os.getenv("TURN_SERVER",   "")
TURN_USERNAME     = os.getenv("TURN_USERNAME", "")
TURN_PASSWORD     = os.getenv("TURN_PASSWORD", "")


async def main():
    if not all([API_ID, API_HASH, BOT_TOKEN]):
        logger.error("❌ .env mein API_ID, API_HASH, BOT_TOKEN set karo!")
        logger.error("   cp .env.example .env  → phir values bharo")
        return

    # --- Bot client ---
    bot = Client(
        "bot",
        api_id=API_ID,
        api_hash=API_HASH,
        bot_token=BOT_TOKEN,
    )

    # --- Assistant client ---
    assistant = Client(
        "assistant",
        api_id=API_ID,
        api_hash=API_HASH,
        session_string=ASSISTANT_SESSION,
    )

    # --- Components ---
    queue_manager = QueueManager()

    webrtc_engine = WebRTCEngine(
        stun_url=STUN_URL,
        turn_url=TURN_URL,
        turn_username=TURN_USERNAME,
        turn_password=TURN_PASSWORD,
    )

    group_call_manager = GroupCallManager(
        client=assistant,
        webrtc_engine=webrtc_engine,
    )

    register_handlers(
        bot,
        group_call_manager,
        queue_manager
    )

    logger.info("🚀 Starting bot and assistant...")

    await assistant.start()
    await bot.start()

    me_bot = await bot.get_me()
    me_asst = await assistant.get_me()

    logger.info(f"✅ Bot ready       : @{me_bot.username}")
    logger.info(
        f"✅ Assistant ready : {me_asst.first_name}"
    )

    logger.info("🎵 /play <song name>")

    await idle()

    await bot.stop()
    await assistant.stop()


if __name__ == "__main__":
    asyncio.run(main())
    
