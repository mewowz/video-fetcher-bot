from __future__ import annotations

import time
import json
import re
from dataclasses import dataclass
from typing import Optional
from urllib.parse import urlparse

import discord
from discord.ext import commands
from discord import app_commands

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
    max_size_bytes: int = 10 * 1000 * 1000  # 10MB discord default
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

    @staticmethod
    def _is_in_dms():
        def predicate(self, interaction: discord.Interaction) -> bool:
            return isinstance(interaction.channel, discord.DMChannel)
        return app_commands.check(predicate)

    @app_commands.command(name="dl", description="Download a video from various streaming sites!")
    @app_commands.checks.cooldown(rate=2, per=10, key=lambda i: (i.user.id))
    @_is_in_dms()
    @app_commands.describe(
        url="The url to the video you wish to download",
        list_formats="Returns the list of available formats to download",
        desired_filetype="The desired final video format (typically mp4 or webm)",
        audio_format_id="The number denoting which audio format you wish to download",
        video_format_id="The number denoting which video format you wish to download",
        merge_formats="Whether or not you wish to receive one merged video or two separate files (audio & video)",
        upload_to_discord="Whether or not to upload directly to discord **NOTE**: files WILL be 10MB or less; quality may suffer"
    )
    async def dl(self, 
                 interaction: discord.Interaction, 
                 url: str, 
                 list_formats: bool = False,
                 desired_filetype: str = None,
                 audio_format_id: int = None,
                 video_format_id: int = None,
                 merge_formats: bool = True,
                 upload_to_discord: bool = None,
    ):
        vu = self._valid_url(url)
        if vu is False:
            # needs logging
            return await interaction.response.send_message("That URL is invalid.")

        await interaction.response.defer(thinking=True)

        job_id = uuid.uuid4().hex  # replace w/ ULID later

        req = DownloadRequest(url, "auto", desired_filetype, False)
        policy = JobPolicy()

        payload = { # This thing is a mess but I'm gonna roll with it until I feel like refactoring the whole thing
            "v": 1.01,
            "job_id": job_id,
            "created_at": int(time.time()),
            "request": {
                "url": req.url,
                "source": req.source,
                "output_filetype": req.format,
                "audio_only": req.audio_only,
                "audio_fmt_id": audio_format_id,
                "video_fmt_id": video_format_id,
                "merge_formats": merge_formats,
                "upload_to_discord": upload_to_discord
            },
            "reply": {
                "channel_id": interaction.channel.id,
                "guild_id": interaction.guild_id,
                "requester_id": interaction.user.id,
                "request_message_id": interaction.message.id,
                "webook_url": interaction.followup.webook.url
            },
            "policy": {
                "discord_max_size_bytes": policy.max_size_bytes,
                "prefer_discord_upload": policy.prefer_discord_upload,
            },
            # Worker fills these in:
            "status": "queued",
            "error": None,
            "result": None,
        }

        # Store job for workers to handle
        await self.redis.lpush("dlqueue", json.dumps(payload))


    @dl.error
    async def dl_error(self, interaction: discord.Interaction, error: app_commands.AppCommandError):
        # Minimal friendly errors; add logging later.
        if isinstance(error, app_commands.CommandOnCooldown):
            await interaction.response.send_message(f"Slow down a bit — try again in {error.retry_after:.1f} seconds.")
        else:
            await interaction.response.send_message("Something went wrong enqueuing that request.")
            raise error  


async def setup(bot: commands.Bot):
    # You must pass a redis client into the cog.
    # I'd recommend setting bot.redis = redis_client in bot.py
    cog = DownloadVideoCog(bot, redis=getattr(bot, "redis", None))
    if cog.redis is None:
        raise RuntimeError("Redis client not found on bot (set bot.redis first).")
    await bot.add_cog(cog)

