import asyncio
import redis
import json
import logging
import sys

from urllib.parse import urlparse
from uuid import uuid4
from pathlib import Path
from yt_dlp import YoutubeDL

YTDL_OPTS_BASE = {
        "quiet": True,
        "no_warnings": True,
        "format": "mp4", # Keep this as the default
        "check_formats": True, # Check all the formats to see if any of them are downloadable, at least
        #"format": "bv*+ba",
        "restrictfilenames": True,
        "noplaylist": True,
        "final_ext": "mp4",
        "fixup": "warn",
        "noprogress": True,
        "consoletitle": False,
}
 
class Downloader:
    DEFAULT_DOWNLOADER_OPTS = {
            "cold_dl_path": Path.cwd() / Path("data") / Path("videos"),
    }

    def __init__(self, name: str, ytdlp_opts: dict, logger: logging.Logger = None, downloader_opts: dict = dict()):
        self.name = name
        self.downloader_opts = (Downloader.DEFAULT_DOWNLOADER_OPTS | downloader_opts)

        if not isinstance(logger, logging.Logger):
            # Make an stdout logger so it's a lot easier to read while doing initial testing
            self.logger = logging.getLogger(name)
            self.logger.setLevel(logging.DEBUG)
            handler = logging.StreamHandler(sys.stdout)
            handler.setFormatter(
                    logging.Formatter(
                        "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
                        )
                    )
            self.logger.addHandler(handler)
            self.logger.propagate = False
        else:
            self.logger = logger

        self.ytdlp_opts = (YTDL_OPTS_BASE | ytdlp_opts)
        self.ytdlp_opts["logger"] = self.logger
        self.logger.debug(f"Created worker '{self.name}'")

        try:
            self.downloader_opts["cold_dl_path"].mkdir(parents=True, exist_ok=True)
        except FileExistsError as e:
            self.logger.error(f"Unable to create dir for saving videos @ '{str(self.downloader_opts['cold_dl_path'])}'")
            raise # for now, just exit so I don't have to worry about this right now

    def _extract_info(self, link: str, extra_opts: dict = {}):
        with YoutubeDL(self.ytdlp_opts | extra_opts) as ydl:
            info = ydl.extract_info(link, download=False)
        return info

    def _download_video(self, link: str, extra_opts: dict = {}):
        with YoutubeDL(self.ytdlp_opts | extra_opts) as ydl:
            e = ydl.download([link])
        return e

    def _get_unq_dl_path(self, video_id: str, dl_type: str) -> Path:
        if dl_type == "tmp":
            pass
        elif dl_type == "cold":
            try:
                p = (
                        self.downloader_opts.get("cold_dl_path")
                        / Path(str(uuid4()))
                        / Path(video_id)
                    )
                p.mkdir(parents=True, exist_ok=False)
                return p
            except FileExistsError as e:
                self.logger.error(f"Unable to make unique download dir @ '{str(p)}'")
                raise

    async def download(self, link: str, extra_opts: dict = {}):
        if not isinstance(extra_opts, dict):
            self.logger.error(f"Argument 'extra_opts' is not of instance 'dict'")
            raise ValueError(f"Argument 'extra_opts' is not of instance 'dict'")

        self.logger.debug(f"Fetching video info for {link}.")
        video_info = await asyncio.to_thread(self._extract_info, link, extra_opts) # for later

        dl_path = self._get_unq_dl_path(video_info.get("id"), dl_type="cold")
        
        self.logger.debug(f"Downloading video '{link}' to {str(dl_path)}")
        rc = await asyncio.to_thread(self._download_video, link, ({"home": dl_path} | extra_opts))
        



