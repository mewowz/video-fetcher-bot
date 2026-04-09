from __future__ import annotations

import logging
logger = logging.getLogger(__name__)

import time
import json
import re
from dataclasses import dataclass
from typing import Optional
from urllib.parse import urlparse

import discord
from discord.ext import commands
from discord import app_commands

#from utils.logging_utils import get_stdout_logger, get_cog_logger

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

    def __init__(self, bot: commands.Bot, redis, custom_logger = None):
        self.bot = bot
        self.redis = redis  # expects an asyncio redis client (redis.asyncio)
        self.logger = custom_logger or logger # Here in case we expand this to a LoggerAdapter class
        self.logger.debug(f"{self.__class__.__name__} Init done")

    def _valid_url(self, url: str):
        url = url.strip().strip("<>")
        if not URL_RE.match(url):
            return False
        parsed = urlparse(url)
        host = (parsed.hostname or "").lower()

        return bool(URL_VALID_HOST_RE.fullmatch(host))

    @staticmethod
    def _create_job_dict(
        self,
        interaction: discord.Interaction,
        job_id: int,
        dl_request: DownloadRequest,
        policy: JobPolicy,
        list_formats: bool,
        audio_format_id: int,
        video_format_id: int,
        merge_formats: bool,
        upload_to_discord: bool,
    ):
        payload = { 
            "v": 1.01,
            "job_id": job_id,
            "created_at": int(time.time()),
            "request": {
                "url": dl_request.url,
                "source": dl_request.source,
                "output_filetype": dl_request.format,
                "list_formats": list_formats,
                "audio_only": dl_request.audio_only,
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

        return payload

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
    async def dl(
        self, 
        interaction: discord.Interaction, 
        url: str, 
        list_formats: bool = False,
        desired_filetype: str = None,
        audio_format_id: int = None,
        video_format_id: int = None,
        merge_formats: bool = True,
        upload_to_discord: bool = None,
    ):
        self.logger.info(
                f"Receieved request from {interaction.user.id} "
                f"to download video {url}"
        )
        self.logger.debug(
                f"[dl] receieved args {json.dumps(locals())}"
        )

        vu = self._valid_url(url)
        self.logger.debug(f"[dl] self._valid_url(url = {url}) = {vu}")
        if vu is False:
            self.logger.debug(f"[dl] Reporting URL validity and returning")
            return await interaction.response.send_message("That URL is invalid.")

        self.logger.debug(f"[dl] Deferring with thinking=True")
        await interaction.response.defer(thinking=True)
        self.logger.debug(f"[dl] Defer successful")

        job_id = uuid.uuid4().hex  # replace w/ ULID later

        req = DownloadRequest(url, "auto", desired_filetype, False)
        policy = JobPolicy()

        payload = self._create_job_dict(
            interaction, job_id, req, policy, list_formats, 
            audio_format_id, video_format_id, merge_formats,
            upload_to_discord
        )
        
        self.logger.debug(
                f"[dl] Pushing job to 'dlqueue' on valkey server "
                f"with payload {json.dumps(payload)}"
        )

        # Store job for workers to handle
        await self.redis.lpush("dlqueue", json.dumps(payload))

        self.logger.info("[dl] Successfully pushed job to queue. Returning")


    @dl.error
    async def dl_error(self, interaction: discord.Interaction, error: app_commands.AppCommandError):
        self.logger.error(f"[dl] Receieved error for interaction {interaction.id}: {error}")
        if isinstance(error, app_commands.CommandOnCooldown):
            await interaction.response.send_message(f"Slow down a bit — try again in {error.retry_after:.1f} seconds.")
        elif isinstance(error, discord.HTTPException):
            self._handle_http_err(error)
        elif isinstance(error, discord.InteractionResponded):
            self.logger.error(
                "[dl] Ignoring already responded interaction with "
                f"interaction ID {interaction.id}"
            )
            return
        else:
            await interaction.response.send_message("Something went wrong enqueuing that request.")
            self.logger.error(f"[dl] Unhandled error for interaction {interaction.id}")
            raise error  


async def setup(bot: commands.Bot, custom_logger: logging.Logger = None):
    # You must pass a redis client into the cog.
    # I'd recommend setting bot.redis = redis_client in bot.py
    if not isinstance(custom_logger, logging.Logger):
        #logger = get_cog_logger(DownloadVideoCog.__name__, level=logging.DEBUG)
        custom_logger = logger

    cog = DownloadVideoCog(bot, redis=getattr(bot, "redis", None), custom_logger=custom_logger)
    if cog.redis is None:
        raise RuntimeError("Redis client not found on bot (set bot.redis first).")
    await bot.add_cog(cog)

