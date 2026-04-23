import logging
logger = logging.getLogger(__name__)

from urllib.parse import urlparse
from uuid import uuid4
from pathlib import Path
from yt_dlp import YoutubeDL
from yt_dlp.utils import (
    DownloadError, 
    UnavailableVideoError,
    UnsupportedError,
    GeoRestrictedError,
)

from utils.config import YTDL_OUTPUT_DIR

LOCAL_DL_PATH = Path(YTDL_OUTPUT_DIR)

YTDL_OPTS_BASE = {
        "quiet": True,
        "no_warnings": True,
        "format": "best[ext=mp4]", # Keep this as the default
        # Commenting out check_formats for now as this adds a lot of extra time to each download
        #"check_formats": True, # Check all the formats to see if any of them are downloadable, at least
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
            "local_dl_path": LOCAL_DL_PATH,
    }

    VALID_DOWNLOAD_LOCATION_TYPES = {
        "local",
        "tmp",
        "remote",
    }

    def __init__(
        self, 
        name: str, 
        *,
        dl_type: str = "local",
        ytdlp_opts: dict = dict(), 
        custom_logger: logging.Logger = None, 
        downloader_opts: dict = dict(),
    ):
        self.name = name
        self.downloader_opts = (Downloader.DEFAULT_DOWNLOADER_OPTS | downloader_opts)

        if not isinstance(custom_logger, logging.Logger):
            self.logger = logger
            self.logger.warning(
                "custom_logger was not passed an instance of logging.Logger. "
                "Using default logger"
            )
        else:
            self.logger = custom_logger

        if not isinstance(ytdlp_opts, dict):
            raise ValueError("ytdlp_opts must be of instance dict")
        else:
            self.ytdlp_opts = (YTDL_OPTS_BASE | ytdlp_opts)
        self.ytdlp_opts["logger"] = self.logger

        if dl_type in self.VALID_DOWNLOAD_LOCATION_TYPES:
            self.dl_type = dl_type
        else:
            raise ValueError(
                f"dl_type = '{dl_type}' not a valid type"
                "in VALID_DOWNLOAD_LOCATION_TYPES"
            )


        try:
            self.downloader_opts["local_dl_path"].mkdir(parents=True, exist_ok=True)
        except FileExistsError as e:
            self.logger.error(f"Unable to create dir for saving videos @ '{str(self.downloader_opts['local_dl_path'])}'")
            raise # for now, just exit so I don't have to worry about this right now


        self.logger.debug(f"Created Downloader '{self.name}'")

    def download(self, link: str, extra_opts: dict = {}) -> tuple[int, str]:
        if not isinstance(extra_opts, dict):
            self.logger.error(f"Argument 'extra_opts' is not of instance 'dict'")
            raise ValueError(f"Argument 'extra_opts' is not of instance 'dict'")


        try:
            self.logger.debug(f"Fetching video info for {link}.")
            video_info = self._extract_info(link, extra_opts) # for later
            self.logger.debug(f"Successfuly obtained video info for {link}")
        except UnsupportedError as e:
            self.logger.error(f"The provided url, {link}, is unsupported by any yt-dlp extractor")
            self.logger.debug(f"Error: {e}")
            return (-1, "")
        except GeoRestrictedError as e:
            self.logger.error(f"Unable to download {link} due to geo-restrictions")
            self.logger.debug(f"Error: {e}")
            return (-1, "")
        except Exception as e:
            self.logger.error(f"Got unknown error: {e}.\nRe-raising exception")
            raise


        dl_path = self._get_unique_dl_path(video_info.get("id"), dl_type=self.dl_type)
        try:
            self._make_dl_path(dl_path)
            self.logger.debug(f"Successfully obtained unique download path for video {link} "
                              f"@ {dl_path}")
        except FileExistsError as e:
            self.logger.debug(f"Could not obtain unique download path for video {link} with dl_type={self.dl_type}")
            raise
        except NotImplementedError as e:
            self.logger.debug(f"dl_type={self.dl_type} is not implemented for Downloader._get_unique_dl_path()")
            raise

        
        try:
            self.logger.debug(f"Downloading video @ {link} to {str(dl_path)}")
            outtmpl = {"outtmpl": str( dl_path / "%(id)s.%(ext)s") }
            rc = self._download_video(link, ({"home": dl_path} | outtmpl | extra_opts) )
            self.logger.debug(f"Successfully downloaded video {link}")
        except DownloadError as e:
            self.logger.error(f"Failed to download {link}: Caught DownloadError.")
            self.logger.debug(f"Error: {e}")
            return (-1, "")
        except UnavailableVideoError as e:
            self.logger.error(f"Failed to download {link}: Video unavailable")
            self.logger.debug(f"Error: {e}")
            return (-1, "")
        except Exception as e:
            self.logger.debug(f"Got unknown error: {e}.\nRe-raising exception")
            raise

        # TODO: Look into source for ytdlp's error codes. 
        # ytdlp has YoutubeDL.download() return an integer error code, but
        # I'm unsure exactly what those codes are nor what they mean
        return (rc, str(dl_path))

    def _extract_info(self, link: str, extra_opts: dict = {}) -> dict:
        with YoutubeDL(self.ytdlp_opts | extra_opts) as ydl:
            video_info_dict = ydl.extract_info(link, download=False)
        if video_info_dict == None:
            raise Exception("Could not fetch video info")
        return video_info_dict

    def _download_video(self, link: str, extra_opts: dict = {}):
        with YoutubeDL(self.ytdlp_opts | extra_opts) as ydl:
            err_code = ydl.download([link])
        return err_code

    def _get_unique_dl_path(self, video_id: str, dl_type: str = "local") -> Path:
        if dl_type == "tmp":
            raise NotImplementedError("'tmp' storage type is not yet implemented")
        elif dl_type == "remote":
            raise NotImplementedError("'remote' storage type is not yet implemented")
        elif dl_type == "local":
            p = (
                    self.downloader_opts.get("local_dl_path")
                    / Path(str(uuid4()))
                    / Path(video_id)
                )
            return p

    def _make_dl_path(self, dl_path: Path):
        try:
            dl_path.mkdir(parents=True, exist_ok=False)
        except FileExistsError as e:
            raise

    


