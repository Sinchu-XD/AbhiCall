"""
bot/handlers.py — Saare bot commands yahan hain
/join, /play, /skip, /stop, /pause, /resume, /queue
"""

import logging

from pyrogram import Client, filters
from pyrogram.types import Message

from core.resolver import resolve, get_valid_stream
from audio.pipeline import QueueManager
from core.group_call import GroupCallManager

logger = logging.getLogger(__name__)


def register_handlers(
    bot: Client,
    group_call_manager: GroupCallManager,
    queue_manager: QueueManager
):

    # ----------------------------------------------------------------
    # /start /help
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

            "**Note:**\n"
            "• Bot aur Assistant dono group mein hone chahiye.\n"
            "• Pehle Voice Chat start karo.\n"
            "• Phir `/join` ya `/play` use karo."
        )

    # ----------------------------------------------------------------
    # /join
    # ----------------------------------------------------------------
    @bot.on_message(filters.command("join") & filters.group)
    async def cmd_join(_, msg: Message):

        status = await msg.reply_text(
            "🔄 Voice Chat join kar raha hoon..."
        )

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
                "• Assistant admin hai?"
            )

    # ----------------------------------------------------------------
    # /play
    # ----------------------------------------------------------------
    @bot.on_message(filters.command("play") & filters.group)
    async def cmd_play(_, msg: Message):

        if len(msg.command) < 2:
            await msg.reply_text(
                "❌ Usage:\n`/play <YouTube URL ya song name>`"
            )
            return

        query = " ".join(msg.command[1:])

        status = await msg.reply_text(
            f"🔍 Dhundh raha hoon:\n`{query}`"
        )

        # Resolver
        results = await resolve(
            query,
            video=False,
            user_id=msg.from_user.id
        )

        if not results:
            await status.edit_text(
                "❌ Song nahi mila!"
            )
            return

        song = results[0]

        if not song.get("stream"):
            await status.edit_text(
                "❌ Stream URL nahi mili."
            )
            return

        title = song.get("title", "Unknown")
        duration = song.get("duration_text", "N/A")
        channel = song.get("channel", "")
        thumb = song.get("thumb")

        # Already playing -> queue
        if queue_manager.current:

            queue_manager.add_song(song)

            text = (
                f"📋 **Queue mein add hua:**\n\n"
                f"🎵 {title}\n"
                f"⏱ `{duration}`"
            )

            if channel:
                text += f"\n👤 {channel}"

            text += f"\n📍 Position: #{len(queue_manager.queue)}"

            if thumb:
                await status.delete()

                await msg.reply_photo(
                    photo=thumb,
                    caption=text
                )
            else:
                await status.edit_text(text)

            return

        # VC join
        if not group_call_manager.is_joined:

            joined = await group_call_manager.join(
                msg.chat.id
            )

            if not joined:
                await status.edit_text(
                    "❌ Voice Chat join nahi ho saka!\n\n"
                    "Pehle VC start karo."
                )
                return

        await status.edit_text(
            f"⏳ Load ho raha hai:\n**{title}**"
        )

        # Pipeline
        stream_url = song["stream"]

        pipeline = queue_manager.start_pipeline(
            stream_url
        )

        # Connect audio
        connected = await group_call_manager.connect_audio(
            pipeline
        )

        if not connected:

            pipeline.stop()

            await status.edit_text(
                "❌ WebRTC connect nahi ho saka!"
            )

            return

        queue_manager.set_current(song)

        text = (
            f"▶️ **Ab chal raha hai:**\n\n"
            f"🎵 {title}\n"
            f"⏱ `{duration}`"
        )

        if channel:
            text += f"\n👤 {channel}"

        if thumb:

            await status.delete()

            await msg.reply_photo(
                photo=thumb,
                caption=text
            )

        else:
            await status.edit_text(text)

    # ----------------------------------------------------------------
    # /skip
    # ----------------------------------------------------------------
    @bot.on_message(filters.command("skip") & filters.group)
    async def cmd_skip(_, msg: Message):

        if not group_call_manager.is_joined:
            await msg.reply_text(
                "❌ Abhi kuch chal nahi raha!"
            )
            return

        next_song = queue_manager.skip()

        if next_song:

            stream_url = await get_valid_stream(
                next_song
            )

            if not stream_url:
                await msg.reply_text(
                    "❌ Agle song ki stream nahi mili."
                )
                return

            pipeline = queue_manager.start_pipeline(
                stream_url
            )

            group_call_manager.webrtc.switch_track(
                pipeline
            )

            queue_manager.set_current(next_song)

            title = next_song.get(
                "title",
                "Unknown"
            )

            await msg.reply_text(
                f"⏭ Skipped!\n\n"
                f"▶️ **Ab chal raha hai:**\n"
                f"🎵 {title}"
            )

        else:

            await group_call_manager.leave()

            await msg.reply_text(
                "⏭ Queue khaali hai.\n"
                "VC band kar diya."
            )

    # ----------------------------------------------------------------
    # /stop
    # ----------------------------------------------------------------
    @bot.on_message(filters.command("stop") & filters.group)
    async def cmd_stop(_, msg: Message):

        if not group_call_manager.is_joined:
            await msg.reply_text(
                "❌ Abhi kuch chal nahi raha!"
            )
            return

        queue_manager.stop()

        await group_call_manager.leave()

        await msg.reply_text(
            "⏹ Band kar diya aur VC chhod diya!"
        )

    # ----------------------------------------------------------------
    # /pause
    # ----------------------------------------------------------------
    @bot.on_message(filters.command("pause") & filters.group)
    async def cmd_pause(_, msg: Message):

        pipeline = queue_manager.pipeline

        if not pipeline or not pipeline.is_alive:
            await msg.reply_text(
                "❌ Kuch chal nahi raha!"
            )
            return

        pipeline.stop()

        await msg.reply_text(
            "⏸ Paused!\n"
            "`/resume` se dobara chalao."
        )

    # ----------------------------------------------------------------
    # /resume
    # ----------------------------------------------------------------
    @bot.on_message(filters.command("resume") & filters.group)
    async def cmd_resume(_, msg: Message):

        current = queue_manager.current

        if not current:
            await msg.reply_text(
                "❌ Resume karne ke liye koi song nahi."
            )
            return

        stream_url = await get_valid_stream(current)

        if not stream_url:
            await msg.reply_text(
                "❌ Stream URL nahi mili."
            )
            return

        pipeline = queue_manager.start_pipeline(
            stream_url
        )

        group_call_manager.webrtc.switch_track(
            pipeline
        )

        await msg.reply_text(
            f"▶️ Resume!\n🎵 {current.get('title', 'Unknown')}"
        )

    # ----------------------------------------------------------------
    # /queue
    # ----------------------------------------------------------------
    @bot.on_message(filters.command("queue") & filters.group)
    async def cmd_queue(_, msg: Message):

        current = queue_manager.current
        q = queue_manager.queue

        if not current and not q:
            await msg.reply_text(
                "📋 Queue khaali hai.\n"
                "`/play` se shuru karo!"
            )
            return

        lines = [
            "📋 **Music Queue**\n"
        ]

        if current:

            dur = current.get(
                "duration_text",
                "N/A"
            )

            lines.append(
                "▶️ **Ab chal raha hai:**\n"
                f"🎵 {current.get('title', 'Unknown')} "
                f"`[{dur}]`"
            )

        if q:

            lines.append(
                "\n**Aage ki line:**"
            )

            for i, song in enumerate(q, 1):

                dur = song.get(
                    "duration_text",
                    "N/A"
                )

                lines.append(
                    f"{i}. "
                    f"{song.get('title', 'Unknown')} "
                    f"`[{dur}]`"
                )

        await msg.reply_text(
            "\n".join(lines)
        )
