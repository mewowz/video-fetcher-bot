import asyncio
import json
from yt_dlp import YoutubeDL
from pathlib import Path
from dataclasses import dataclass

from .config import DOWNLOADS_DIR, TMP_DOWNLOADS_DIR, MAX_DOWNLOAD_SIZE_BYTES, init_dirs


@dataclass
class DownloadResponse:
    success: bool
    info_dict: dict
    paths: dict[str, tuple[Path, str]] # {"format_id": [ Path("/path/to/file.ext"), "ao" or "vo"] }
    error_msg: str = None


class YTDownloader:

    def __init__(self, name: str, redis_conn, params: dict = {}):
        default_params = {
                    "postprocessors": [], # necessary so that yt-dlp doesn't call ffmpeg on its own
                    "paths": { "home": str(DOWNLOADS_DIR), "temp": "tmp" },
                    "outtmpl": { "default": "%(id)s/%(format_id)s.%(ext)s" }
        }
        self.worker_name = name
        self.redis_conn = redis_conn
        self.params = default_params | params

        init_dirs()


    async def run(self):
        while True:
            try:
                _, job = await self.redis_conn.brpop("dlqueue") # Just wait forever until a job shows up
                job = json.loads(job)
                rc = await self._handle_job(job)
            except:
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


    async def _handle_job(self, job: dict):
        can_download = False
        try:
            with YoutubeDL() as ytdl:
                info_dict = await asyncio.to_thread(ytdl.extract_info, job["request"]["url"], download=False)

            max_size = job["policy"]["max_size_bytes"]

            formats = self._get_viable_formats(info_dict, max_size)
            print(f"Got {formats} for video {info_dict['webpage_url']}")

            if None in formats:
                if job["request"]["upload_to_discord"] == True:
                    return DownloadResponse(False, None, None, "Cannot directly upload to discord: no viable formats under 10MB")

                formats = self._get_formats_below_size(info_dict, MAX_DOWNLOAD_SIZE_BYTES)
                if None in formats:
                    print(f"No viable download available. Exiting")
                    can_download = False
                    return DownloadResponse(False, None, None, 
                                            f"Cannot download video: no viable formats under policy max_size_bytes = {max_size}B"
                            )

                formats = [str(f) for f in formats]
                format_ids = ",".join(f for f in formats)
                params = self.params | {"format": format_ids}
                with YoutubeDL(params) as ytdl:
                    rc = await asyncio.to_thread(ytdl.download, job["request"]["url"])


            else:
                # Cast the formats to strings so its easier to handle later
                formats = [str(f) for f in formats]
                format_ids = ",".join(f for f in formats)
                params = self.params | {"format": format_ids}
                with YoutubeDL(params) as ytdl:
                    rc = await asyncio.to_thread(ytdl.download, job["request"]["url"])

            
            json_outfile = DOWNLOADS_DIR / Path(info_dict["id"]) / Path("info.json")
            info_dict.update({"formats": [f for f in info_dict["formats"] if f["format_id"] in formats]})
            with open(json_outfile, "w") as jf:
                json.dump(info_dict, jf)

            fpaths = self._get_file_paths(info_dict, formats)

            return DownloadResponse(True, info_dict, fpaths, "")

        except:
            raise

