import asyncio
import json
from yt_dlp import YoutubeDL
from pathlib import Path
from dataclasses import dataclass, asdict
import logging

from .config import DOWNLOADS_DIR, TMP_DOWNLOADS_DIR, MAX_DOWNLOAD_SIZE_BYTES, init_dirs
from .dl_logger import get_stdout_logger, WorkerLogger


@dataclass
class DownloadResponse:
    success: bool
    info_dict: dict
    paths: dict[str, tuple[Path, str]] # {"format_id": [ Path("/path/to/file.ext"), "ao" or "vo"] }
    error_msg: str = None

    def __str__(self):
        return json.dumps(asdict(self))


class YTDownloader:
    def __init__(self, name: str, redis_conn, params: dict = {}, logger: logging.Logger = None):
        default_params = {
                    "postprocessors": [], # necessary so that yt-dlp doesn't call ffmpeg on its own
                    "paths": { "home": str(DOWNLOADS_DIR), "temp": "tmp" },
                    "outtmpl": { "default": "%(id)s/%(format_id)s.%(ext)s" }
        }
        self.worker_name = name
        self.redis_conn = redis_conn
        self.params = default_params | params

        init_dirs()
        
        base_logger = logger = logger or get_stdout_logger()
        self.logger = WorkerLogger(base_logger)
        self.params["logger"] = self.logger


    async def run(self):
        while True:
            try:
                _, job = await self.redis_conn.brpop("dlqueue") # Just wait forever until a job shows up
                job = json.loads(job)
                self.logger.info(f"Got job ({job['job_id'][:5]}...): {job}")
                rc = await self._handle_job(job)
                self.logger.debug(f"YoutubeDL._handle_job(job) returned {str(rc)}")
                    f"YoutubeDL._handle_job(job) returned {str(rc)}"
                    self.logger.info(f"Job {job['job'][:5]}... returned successfully")
                if not rc.success:
                    self.logger.info(
                                f"Job {job['job'][:5]}... returned with error: "
                                f"{rc.error_msg}"
                    )

            except Exception as e:
                self.logger.error("Error in _handle_job", exc_info=True)
                raise

    def _get_viable_formats(self, info_dict: dict, max_size_bytes: int,
                                  *, max_audio_br_kbps:int=120, min_vbr_kbps:int=200):
        fmts = info_dict["formats"]
        ao = [fmt for fmt in fmts if fmt["vcodec"] == "none" and fmt["acodec"] != "none"]
        vo = [fmt for fmt in fmts if fmt["vcodec"] != "none" and fmt["acodec"] == "none"]

        # yt-dlp seems to report only an integer, which drops the fractional value,
        # so add 1 just to account for extra overhead it might not be reporting
        video_duration = info_dict["duration"] + 1 
        max_size_mbytes = (max_size_bytes / 1000) / 1000

        overhead_fac = 0.97 # Account for some overhead from the container. This is quite a significant overhead factor
        usable_br_kbps = (max_size_mbytes * 8000 / video_duration) * overhead_fac - max_audio_br_kbps

        print(f"usable_br_kbps: {usable_br_kbps}")
        if usable_br_kbps < min_vbr_kbps:
            return (None, None) # Nothing will be suitable for this.

        output_sz_mbytes = ((usable_br_kbps + max_audio_br_kbps) * video_duration) / 8000
        print(f"output_sz_mbytes: {output_sz_mbytes}")
        if output_sz_mbytes > max_size_mbytes:
            return (None, None) # Even a low bitrate won't shrink this file enough
    
        # Find the highest bitrate that's below usable_br_kbps
        highest_vbr = vo[0]["vbr"]
        vtarget_format_id = None
        for fmt in vo:
            if fmt["vbr"] >= highest_vbr and fmt["vbr"] <= usable_br_kbps:
                highest_vbr = fmt["vbr"]
                vtarget_format_id = fmt["format_id"]
        
        # Now just sift through all the audio tracks that are closest to the max_audio_br
        highest_abr = ao[0]["abr"]
        atarget_format_id = None
        for fmt in ao:
            if fmt["abr"] >= highest_abr and fmt["abr"] <= max_audio_br_kbps:
                higest_abr = fmt["abr"]
                atarget_format_id = fmt["format_id"]

        return (vtarget_format_id, atarget_format_id)

    def _get_formats_below_size(self, info_dict: dict, max_size_bytes: int):
        fmts = info_dict["formats"]
        ao = [fmt for fmt in fmts if fmt["vcodec"] == "none" and fmt["acodec"] != "none"]
        vo = [fmt for fmt in fmts if fmt["vcodec"] != "none" and fmt["acodec"] == "none"]

        valid_fmts = dict()

        for vfmt in vo:
            vsize = vfmt.get("filesize")
            if vsize is None:
                continue
            for afmt in ao:
                asize = afmt.get("filesize")
                if asize is None:
                    continue
                size = vsize + asize
                if size < max_size_bytes:
                    valid_fmts[(vfmt["format_id"], afmt["format_id"])] = size

        # Sort by largest download (usually better quality)
        if len(valid_fmts) == 0:
            return (None, None)

        valid_fmts = sorted(valid_fmts.items(), key=lambda itm: itm[1])
        return valid_fmts[-1][0]

    def _get_file_paths(self, info_dict: dict, format_ids: list):
        fmts = [
            fmt for fmt in info_dict["formats"]
                if fmt["format_id"] in format_ids
        ]

        paths = {}
        for fmt in fmts:
            paths[fmt["format_id"]] = [
                    Path(DOWNLOADS_DIR) / Path(info_dict["id"]) / Path(fmt["format_id"] + fmt["ext"]),
                    "ao" if fmt["abr"] > 0.0 else "vo"
            ]
        return paths

    def _write_json_file(self, info_dict: dict, formats: list):
        json_outfile = DOWNLOADS_DIR / Path(info_dict["id"]) / Path("info.json")
        formats = [f for f in formats if f is not None]
        info_dict |= {"formats": formats}
        with open(json_outfile, "w") as jf:
            json.dump(info_dict, jf)


    async def _handle_job(self, job: dict):
        self.logger.debug(f"Calling YoutubeDL.extract_info for URL: '{job['request']['url']}'")
        try:
            
            with YoutubeDL() as ytdl:
                info_dict = await asyncio.to_thread(ytdl.extract_info, job["request"]["url"], download=False)
        except Exception as e:
            self.logger.error(f"Failed to extract_info for url {job['request']['url']}", exc_info=True)
            raise

        self.logger.debug(
            f"YoutubeDL.extract_video successful for URL: "
            f"'{job['request']['url']}'"
        )

        # Determine the job type and defer to the according class method
        requested_format_ids = [
            job["request"]["audio_fmt_id"],
            job["request"]["video_fmt_id"]
        ]


        if any(fmt != None for fmt in requested_format_ids): 
            # User specified custom formats to download
            r = await self._job_custom_formats(info_dict, job)
        elif job["request"]["upload_to_discord"] == True:
            # User requires we upload directly to discord
            r = await self._job_direct_discord_upload(info_dict, job)
        elif job["policy"]["prefer_discord_upload"]:
            # The producer specified we should prioritize uploads directly to discord
            # This can fail if there are downloads too big to download + merge and upload to discord
            # "Prefer" is just a "try it first, still serve if we can't get small files"
            r = await self._job_direct_upload_discord(info_dict, job)
            # Handle the case where this fails
            if not r.success:
                r = await self._job_serve_on_server(info_dict, job)
        else:
            # Just try and download a video below the max download size and serve it to the user from our servers
            r = await self._job_serve_on_server(info_dict, job)



        return r

    async def _job_direct_discord_upload(self, info_dict: dict, job: dict):
        self.logger.debug("Processing job in YTDownloader._job_direct_discord_upload")
        max_size_bytes = job["policy"]["discord_max_size_bytes"]
        formats = self._get_viable_formats(info_dict, max_size_bytes)
        self.logger.debug(
            f"self._get_viable_formats(info_dict, max_size_bytes = {max_size_bytes}) returned "
            f"{formats}"
        )
        if None in formats:
            return DownloadResponse(False, None, None, 
                                    "No valid audio+video formats cumulatively "
                                    f"below size {(max_size_bytes / 1000 / 1000):.0f}MB")
        formats = [str(f) for f in formats]
        format_ids = ",".join(f for f in formats)
        params = self.params | {"format": format_ids}
        self.logger.debug(    
            "Attempting to download video with params = "
            f"{params}"
        )
        try:                  
            with YoutubeDL(params) as ytdl:
                rc = await asyncio.to_thread(ytdl.download, job["request"]["url"])
        except Exception as e:
            self.logger.error("Error downloading video", exc_info=True)
            raise

        self._write_json_file(info_dict, formats)
        fpaths = self._get_file_paths(info_dict, formats)

        self.logger.debug("Download successful!")

        return DownloadResponse(True, info_dict, fpaths, "")


    async def _job_serve_from_server(self, info_dict: dict, job: dict):
        self.logger.debug("Processing job in YTDownloader._job_serve_from_server")
        formats = self._get_formats_below_size(info_dict, MAX_DOWNLOAD_SIZE_BYTES)
        self.logger.debug(
            "YTDownloader._get_formats_below_size(info_dict, "
            f"MAX_DOWNLOAD_SIZE_BYTES = {MAX_DOWNLOAD_SIZE_BYTES}) = "
            f"{formats}"
        )

        if None in formats:
            return DownloadResponse(
                False, None, None, 
                "Cannot download video: "
                f"no viable formats under size MAX_DOWNLOAD_SIZE_BYTES = {MAX_DOWNLOAD_SIZE_BYTES}B"
            )

        formats = [str(f) for f in formats]
        format_ids = ",".join(f for f in formats)
        params = self.params | {"format": format_ids}
        self.logger.debug(
            "Attempting to download video with params = "
            f"{params}"
        ) 
        try:
            with YoutubeDL(params) as ytdl:
                rc = await asyncio.to_thread(ytdl.download, job["request"]["url"])
        except Exception as e:
            self.logger.error("Error downloading video", exc_info=True)
            raise

        self._write_json_file(info_dict, formats)
        fpaths = self._get_file_paths(info_dict, formats)

        self.logger.debug("Download successful!")

        return DownloadResponse(True, info_dict, fpaths, "")


    async def _job_custom_formats(self, info_dict: dict, job: dict):
        self.logger.debug("Processing job in YTDownloader._job_custom_formats")

        fmts = info_dict["formats"]

        formats_ao = [fmt for fmt in fmts if fmt["vcodec"] == "none" and fmt["acodec"] != "none"]
        formats_vo = [fmt for fmt in fmts if fmt["vcodec"] != "none" and fmt["acodec"] == "none"]

        req_afmt_id = job["request"]["audio_fmt_id"]
        req_vfmt_id = job["request"]["video_fmt_id"]
        req_afmt = req_vfmt = None
        format_ids = ""

        if req_afmt_id is not None and not any(f["format_id"] == req_afmt_id for f in formats_ao):
            return DownloadResponse(False, None, None, f"Requested audio format {req_afmt_id} is not a valid format id for this video")
        else:
            req_afmt = next((fmt for fmt in formats_ao if fmt["format_id"] == req_afmt_id), None)
            self.logger.debug(f"Got req_afmt = {req_afmt}")
            if req_afmt is None:
                raise ValueError(f"Error finding format {req_afmt_id}")
            format_ids += req_afmt_id + ","
        if req_vfmt_id is not None and not any(f["format_id"] == req_vfmt_id for f in formats_vo):
            return DownloadResponse(False, None, None, f"Requested video format {req_vfmt_id} is not a valid format id for this video")
        else:
            req_vfmt = next((fmt for fmt in formats_vo if fmt["format_id"] == req_vfmt_id), None)
            self.logger.debug(f"Got req_vfmt = {req_vfmt}")
            if req_vfmt is None:
                raise ValueError(f"Error finding format {req_vfmt_id}")
            format_ids += req_vfmt_id + ","

        format_ids = format_ids.rstrip(",")
        self.logger.debug(f"Got format_ids = {format_ids}")
        # Make sure both downloads are below MAX_DOWNLOAD_SIZE_BYTES
        #
        # Policy should be updated to allow/disallow EACH file being less than MAX_DOWNLOAD_SIZE_BYTES
        # or cumulatively be less than MAX_DOWNLOAD_SIZE_BYTES
        self.logger.debug(
            f"req_afmt['filesize'] = {req_afmt['filesize']}, "
            f"req_vfmt['filesize'] = {req_vfmt['filesize']}"
        )
        if req_afmt is not None and req_afmt["filesize"] < MAX_DOWNLOAD_SIZE_BYTES:
            return DownloadResponse(False, None, None, f"Requested audio format {req_afmt_id} is too large ({req_afmt["filesize"]}B)")
        if req_vfmt is not None and req_vfmt["filesize"] < MAX_DOWNLOAD_SIZE_BYTES:
            return DownloadResponse(False, None, None, f"Requested video format {req_vfmt_id} is too large ({req_vfmt["filesize"]}B)")

        params = self.params | {"format": format_ids}
        self.logger.debug(
            "Attempting to download video with params = "
            f"{params}"
        )
        try:
            with YoutubeDL(params) as ytdl:
                rc = await asyncio.to_thread(ytdl.download, job["request"]["url"])
        except Exception as e:
            self.logger.error("Error downloading video", exc_info=True)
            raise


        formats = [req_afmt, req_vfmt]

        self._write_json_file(info_dict, formats)
        fpaths = self._get_file_paths(info_dict, formats)

        self.logger.debug("Download successful!")

        return DownloadResponse(True, info_dict, fpaths, "")

