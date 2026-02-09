import asyncio
import json
from yt_dlp import YoutubeDL
from pathlib import Path

from .config import DOWNLOADS_DIR, TMP_DOWNLOADS_DIR, init_dirs


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


    async def _handle_job(self, job: dict):
        try:
            with YoutubeDL() as ytdl:
                info_dict = await asyncio.to_thread(ytdl.extract_info, job["request"]["url"], download=False)
            max_size = job["policy"]["max_size_bytes"]

            formats = self._get_viable_formats(info_dict, max_size)
            print(f"Got {formats} for video {info_dict['webpage_url']}")
            if None in formats:
                return # TODO: handle this later
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





        except:
            raise








    async def stop(self):
        self.ytdl.close()
