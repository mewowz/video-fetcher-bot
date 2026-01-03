from __future__ import annotations

import time
import json
import re
from dataclasses import dataclass
from typing import Optional
from urllib.parse import urlparse

import discord
from discord.ext import commands

# Move to ULID later
import uuid


URL_RE = re.compile(r"https?://\S+")

URL_VALID_HOST_RE = re.compile(
        r"^(?:"
        r"(?:www\.|m\.)?youtube\.com|"
        r"youtu\.be|"
        r"(?:www\.|m\.)?instagram\.com|"
        r"(?:www\.|m\.)?tiktok\.com|"
        r"vm\.tiktok\.com"
        r")$",
        re.IGNORECASE,
)

@dataclass
class DownloadRequest:
    url: str
    source: str = "auto"        # "yt" / "ig" / "tt"/ "auto"
    format: str = "mp4"         # "mp4" default
    audio_only: bool = False

@dataclass
class JobPolicy:
    max_size_bytes: int = 10 * 1024 * 1024  # 10MB discord default
    prefer_discord_upload: bool = True

class DownloadVideoCog(commands.Cog):
    """
    Enqueues download jobs into Redis; does NOT download or upload here.
    """

    def __init__(self, bot: commands.Bot, redis):
        self.bot = bot
        self.redis = redis  # expects an asyncio redis client (redis.asyncio)

    def _valid_url(self, url: str):
        url = url.strip().strip("<>")
        if not URL_RE.match(url):
            return False
        parsed = urlparse(url)
        host = (parsed.hostname or "").lower()

        return bool(URL_VALID_HOST_RE.fullmatch(host))


    @commands.command(name="dl")
    @commands.cooldown(rate=2, per=10, type=commands.BucketType.user)  # basic safety
    async def dl(self, ctx: commands.Context, *, url: str):
        """
        Usage: &dl <url>
        """
        vu = self._valid_url(url)
        if vu is False:
            # needs logging
            return await ctx.reply("That URL is invalid.")


        # In DMs, ctx.guild is None
        guild_id = ctx.guild.id if ctx.guild else None

        # Immediately post a status message so we have a message_id to update later.
        status_msg = await ctx.reply("Queued (starting soon...)")

        job_id = uuid.uuid4().hex  # replace w/ ULID later

        req = DownloadRequest(url=url)
        policy = JobPolicy()

        payload = {
            "v": 1,
            "job_id": job_id,
            "created_at": int(time.time()),
            "request": {
                "url": req.url,
                "source": req.source,
                "format": req.format,
                "audio_only": req.audio_only,
            },
            "reply": {
                "channel_id": ctx.channel.id,
                "guild_id": guild_id,
                "requester_id": ctx.author.id,
                "request_message_id": ctx.message.id,
                "status_message_id": status_msg.id,
            },
            "policy": {
                "max_size_bytes": policy.max_size_bytes,
                "prefer_discord_upload": policy.prefer_discord_upload,
            },
            # Worker fills these in:
            "status": "queued",
            "error": None,
            "result": None,
        }

        # Store job record + enqueue job id

        # Store the payload
        job_key = f"dl:job:{job_id}"
        await self.redis.set(job_key, json.dumps(payload), ex=60 * 60)  # 1h TTL for now

        # Store the UUID for the job
        await self.redis.rpush("dl:in_queue", job_id)

        # Later, if I care to make this more robust, move this from rpush to
        # use streams + consumer groups (xadd/xgroup/...)

        await status_msg.edit(content=f"Queued Job `{job_id}`\nYour download will be posted shortly.")

    @dl.error
    async def dl_error(self, ctx: commands.Context, error: Exception):
        # Minimal friendly errors; add logging later.
        if isinstance(error, commands.CommandOnCooldown):
            await ctx.reply(f"Slow down a bit — try again in {error.retry_after:.1f}s.")
        else:
            await ctx.reply("Something went wrong enqueuing that request.")
            raise error  


async def setup(bot: commands.Bot):
    # You must pass a redis client into the cog.
    # I'd recommend setting bot.redis = redis_client in bot.py
    cog = DownloadVideoCog(bot, redis=getattr(bot, "redis", None))
    if cog.redis is None:
        raise RuntimeError("Redis client not found on bot (set bot.redis first).")
    await bot.add_cog(cog)

