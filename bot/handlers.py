"""
bot/handlers.py — Saare bot commands yahan hain
/join, /play, /skip, /stop, /pause, /resume, /queue
"""

import logging
from pyrogram import Client, filters
from pyrogram.types import Message

from core.resolver import resolve, get_valid_stream
from core.resolver import resolve_url, format_duration
from audio.pipeline import QueueManager
from core.group_call import GroupCallManager

logger = logging.getLogger(__name__)


def register_handlers(bot: Client, group_call_manager: GroupCallManager, queue_manager: QueueManager):

    # ----------------------------------------------------------------
    # /start / /help
    # ----------------------------------------------------------------
    @bot.on_message(filters.command(["start", "help"]))
    async def cmd_help(_, msg: Message):
        await msg.reply_text(
            "🎵 **VC Music Bot** (Custom WebRTC)\n\n"
            "**Commands:**\n"
            "╔ `/join` — Voice Chat mein aao\n"
            "╠ `/play <url ya song name>` — music bajao\n"
            "╠ `/skip` — agla song\n"
            "╠ `/stop` — band karo aur VC chhodo\n"
            "╠ `/pause` — ruko\n"
            "╠ `/resume` — dobara chalao\n"
            "╚ `/queue` — queue dekho\n\n"
            "**Note:** Bot aur Assistant dono group mein hone chahiye.\n"
            "Pehle group mein Voice Chat shuru karo, phir `/join` ya `/play` karo."
            "Pehle group mein Voice Chat shuru karo, phir `/join` karo."
        )

    # ----------------------------------------------------------------
    # /join — VC mein aao
    # ----------------------------------------------------------------
    @bot.on_message(filters.command("join") & filters.group)
    async def cmd_join(_, msg: Message):
        status  = await msg.reply_text("🔄 Voice Chat join kar raha hoon...")
        status = await msg.reply_text("🔄 Voice Chat join kar raha hoon...")
        success = await group_call_manager.join(msg.chat.id)

        if success:
            await status.edit_text(
                "✅ Voice Chat join kar liya!\n\n"
                "`/play <url ya song name>` se music bajao."
            )
        else:
            await status.edit_text(
                "❌ Join nahi ho saka.\n\n"
                "**Check karo:**\n"
                "• Group mein Voice Chat shuru hai?\n"
                "• Assistant account group mein hai?\n"
                "• Assistant ko admin banaya hai?"
            )

    # ----------------------------------------------------------------
    # /play — Music bajao
    # ----------------------------------------------------------------
    @bot.on_message(filters.command("play") & filters.group)
    async def cmd_play(_, msg: Message):
        if len(msg.command) < 2:
            await msg.reply_text("❌ Usage: `/play <YouTube URL ya song name>`")
            return

        query  = " ".join(msg.command[1:])
        status = await msg.reply_text(f"🔍 Dhundh raha hoon: `{query}`...")

        # resolve() returns list of song dicts
        results = await resolve(query, video=False, user_id=msg.from_user.id)
        if not results:
            await status.edit_text("❌ Song nahi mila! URL ya naam check karo.")
            return

        song = results[0]
        if not song or not song.get("stream"):
            await status.edit_text("❌ Stream URL nahi mili. Dobara try karo.")
            return

        title    = song.get("title", "Unknown")
        duration = song.get("duration_text", "N/A")
        channel  = song.get("channel", "")
        thumb    = song.get("thumb")

        # Agar already kuch chal raha hai → queue mein daalo
        if queue_manager.current:
            queue_manager.add_song(song)
            pos = len(queue_manager.queue)
            text = (
                f"📋 **Queue mein add hua:**\n"
                f"🎵 {title}\n"
                f"⏱ `{duration}`"
                + (f"\n👤 {channel}" if channel else "")
                + f"\n📍 Position: #{pos}"
            )
            if thumb:
                await status.delete()
                await msg.reply_photo(thumb, caption=text)
            else:
                await status.edit_text(text)
        info = await resolve_url(query)
        if not info:
            await status.edit_text("❌ Song nahi mila! URL ya naam check karo.")
            return

        title       = info["title"]
        stream_url  = info["url"]
        duration    = format_duration(info["duration"])

        # Agar already kuch chal raha hai toh queue mein daalo
        if queue_manager.current:
            queue_manager.add(
                title=title,
                url=stream_url,
                requested_by=msg.from_user.id,
            )
            await status.edit_text(
                f"📋 **Queue mein add hua:**\n"
                f"🎵 {title} (`{duration}`)\n"
                f"📍 Position: #{len(queue_manager.queue)}"
            )
            return

        await status.edit_text(f"⏳ Load ho raha hai: **{title}**...")

        # VC join karo agar nahi hua
        # Agar VC join nahi hua toh pehle join karo
        if not group_call_manager.is_joined:
            joined = await group_call_manager.join(msg.chat.id)
            if not joined:
                await status.edit_text(
                    "❌ Voice Chat join nahi ho saka!\n\n"
                    "Pehle group mein VC shuru karo."
                )
                return

        # stream URL se pipeline start karo (direct CDN URL)
        stream_url = song["stream"]
        pipeline   = queue_manager.start_pipeline(stream_url)

        # WebRTC se audio connect karo
                    "Pehle group mein VC shuru karo aur `/join` karo."
                )
                return

        # Audio pipeline start karo
        pipeline = queue_manager.start_pipeline(stream_url)

        # WebRTC se connect karo
        connected = await group_call_manager.connect_audio(pipeline)
        if not connected:
            pipeline.stop()
            await status.edit_text("❌ WebRTC connect nahi ho saka!")
            return

        queue_manager.set_current(song)

        text = (
            f"▶️ **Ab chal raha hai:**\n"
            f"🎵 {title}\n"
            f"⏱ `{duration}`"
            + (f"\n👤 {channel}" if channel else "")
        )
        if thumb:
            await status.delete()
            await msg.reply_photo(thumb, caption=text)
        else:
            await status.edit_text(text)
        # Queue mein track karo
        queue_manager.add(
            title=title,
            url=stream_url,
            requested_by=msg.from_user.id,
        )
        queue_manager.next()  # current mein set karo

        await status.edit_text(
            f"▶️ **Ab chal raha hai:**\n"
            f"🎵 {title}\n"
            f"⏱ Duration: `{duration}`"
        )

    # ----------------------------------------------------------------
    # /skip
    # ----------------------------------------------------------------
    @bot.on_message(filters.command("skip") & filters.group)
    async def cmd_skip(_, msg: Message):
        if not group_call_manager.is_joined:
            await msg.reply_text("❌ Abhi kuch chal nahi raha!")
            return

        next_song = queue_manager.skip()
        if next_song:
            # Fresh stream URL lo (cache expire ho sakti hai)
            stream_url = await get_valid_stream(next_song)
            if not stream_url:
                await msg.reply_text("❌ Agle song ki stream nahi mili.")
                return

            pipeline = queue_manager.start_pipeline(stream_url)
            group_call_manager.webrtc.switch_track(pipeline)
            queue_manager.set_current(next_song)

            title = next_song.get("title", "Unknown")
            await msg.reply_text(
                f"⏭ Skipped!\n\n"
                f"▶️ **Ab chal raha hai:**\n"
                f"🎵 {title}"
            pipeline = queue_manager.start_pipeline(next_song["url"])
            group_call_manager.webrtc.switch_track(pipeline)
            queue_manager.next()
            await msg.reply_text(
                f"⏭ Skipped!\n\n"
                f"▶️ **Ab chal raha hai:**\n"
                f"🎵 {next_song['title']}"
            )
        else:
            await group_call_manager.leave()
            await msg.reply_text("⏭ Skipped! Queue khaali hai, VC band kar diya.")

    # ----------------------------------------------------------------
    # /stop
    # ----------------------------------------------------------------
    @bot.on_message(filters.command("stop") & filters.group)
    async def cmd_stop(_, msg: Message):
        if not group_call_manager.is_joined:
            await msg.reply_text("❌ Abhi kuch chal nahi raha!")
            return

        queue_manager.stop()
        await group_call_manager.leave()
        await msg.reply_text("⏹ Band kar diya aur VC chhod diya!")

    # ----------------------------------------------------------------
    # /pause
    # /pause — Pipeline buffer rok deta hai
    # ----------------------------------------------------------------
    @bot.on_message(filters.command("pause") & filters.group)
    async def cmd_pause(_, msg: Message):
        pipeline = queue_manager.pipeline
        if not pipeline or not pipeline.is_alive:
            await msg.reply_text("❌ Kuch chal nahi raha!")
            return
        pipeline.stop()
        await msg.reply_text("⏸ Paused! `/resume` se dobara chalao.")

    # ----------------------------------------------------------------
    # /resume
    # /resume — Nayi pipeline se resume karo
    # ----------------------------------------------------------------
    @bot.on_message(filters.command("resume") & filters.group)
    async def cmd_resume(_, msg: Message):
        current = queue_manager.current
        if not current:
            await msg.reply_text("❌ Koi song nahi hai resume karne ke liye.")
            return

        # Fresh stream URL lo
        stream_url = await get_valid_stream(current)
        if not stream_url:
            await msg.reply_text("❌ Stream nahi mili. `/play` se dobara try karo.")
            return

        pipeline = queue_manager.start_pipeline(stream_url)
        group_call_manager.webrtc.switch_track(pipeline)
        await msg.reply_text(f"▶️ Resume! 🎵 {current.get('title', 'Unknown')}")
        pipeline = queue_manager.start_pipeline(current["url"])
        group_call_manager.webrtc.switch_track(pipeline)
        await msg.reply_text(f"▶️ Resume! 🎵 {current['title']}")

    # ----------------------------------------------------------------
    # /queue
    # ----------------------------------------------------------------
    @bot.on_message(filters.command("queue") & filters.group)
    async def cmd_queue(_, msg: Message):
        current = queue_manager.current
        q       = queue_manager.queue

        if not current and not q:
            await msg.reply_text("📋 Queue khaali hai. `/play` se shuru karo!")
            return

        lines = ["📋 **Music Queue**\n"]

        if current:
            dur = current.get("duration_text", "N/A")
            lines.append(f"▶️ **Ab chal raha hai:**\n🎵 {current.get('title', 'Unknown')} `[{dur}]`")

        if q:
            lines.append("\n**Aage ki line:**")
            for i, song in enumerate(q, 1):
                dur = song.get("duration_text", "N/A")
                lines.append(f"{i}. {song.get('title', 'Unknown')} `[{dur}]`")
        if current:
            lines.append(f"▶️ **Ab chal raha hai:**\n🎵 {current['title']}")
        if q:
            lines.append("\n**Aage ki line:**")
            for i, song in enumerate(q, 1):
                lines.append(f"{i}. {song['title']}")

        await msg.reply_text("\n".join(lines))
