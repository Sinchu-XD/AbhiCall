"""
core/resolver.py — yt-dlp se audio URL aur info nikalta hai
"""

import asyncio
import logging
from yt_dlp import YoutubeDL

logger = logging.getLogger(__name__)

YDL_OPTS = {
    "format":         "bestaudio/best",
    "quiet":          True,
    "no_warnings":    True,
    "extract_flat":   False,
    "default_search": "ytsearch",
    "noplaylist":     True,
}


async def resolve_url(query: str) -> dict | None:
    """
    Query (YouTube URL ya search term) se audio info nikalo.
    Returns: {"title": str, "url": str, "duration": int} ya None
    """
    loop = asyncio.get_event_loop()
    try:
        return await loop.run_in_executor(None, _extract, query)
    except Exception as e:
        logger.error(f"Resolve error: {e}")
        return None


def _extract(query: str) -> dict | None:
    with YoutubeDL(YDL_OPTS) as ydl:
        if not query.startswith("http"):
            query = f"ytsearch:{query}"

        data = ydl.extract_info(query, download=False)

        if "entries" in data:
            data = data["entries"][0]

        if not data:
            return None

        return {
            "title":    data.get("title", "Unknown"),
            "url":      data.get("url") or data.get("webpage_url"),
            "duration": data.get("duration", 0),
        }


def format_duration(seconds: int) -> str:
    if not seconds:
        return "N/A"
    m, s = divmod(int(seconds), 60)
    h, m = divmod(m, 60)
    return f"{h}:{m:02d}:{s:02d}" if h else f"{m}:{s:02d}"
