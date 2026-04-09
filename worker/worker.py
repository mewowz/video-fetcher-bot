import asyncio
import redis

import logging
logger = logging.getLogger(__name__)

from yt_dlp import YoutubeDL

YTDL_OPTS_BASE = {
        "quiet": True,
        "no_warnings": True,
        "format": "mp4", # Keep this as the default
        "check_formats": True, # Check all the formats to see if any of them are downloadable, at least
        "format": "bv*+ba",
        "restrictfilenames": True,
        "noplaylist": True,
        "final_ext": "mp4",
        "fixup": "warn",
        "noprogress": True,
        "consoletitle": False,



}


