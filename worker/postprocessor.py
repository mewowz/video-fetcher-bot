import logging
logger = logging.getLogger(__name__)
import shutil
import asyncio
import orjson as json

from pathlib import Path
from dataclasses import dataclass

from utils.config import (
    FFPROBE_PATH,
    FFMPEG_PATH,
    CONVERT_MPEGTS_TO_MP4,
    CONVERT_WEBM_TO_MP4,
    THREADS_PER_POSTPROCESSOR_WORKER,
    DELETE_PREPROCESSED_VIDEO
)

if FFMPEG_PATH == "":
    ffmpeg_path = shutil.which("ffmpeg")
    if ffmpeg_path is None:
        raise RuntimeError("FFMPEG_PATH was not set in utils/config.py nor found in the system PATH")
    else:
        FFMPEG_PATH = ffmpeg_path
if FFPROBE_PATH == "":
    ffprobe_path = shutil.which("ffprobe")
    if ffprobe_path is None:
        raise RuntimeError("FFMPROBE_PATH was not set in utils/config.py nor found in the system PATH")
    else:
        FFPROBE_PATH = ffprobe_path

@dataclass
class PostProcessResult:
    output_name:    Path = None
    exit_code:      int  = None

class PostProcessor:
    def __init__(
        self,
        name: str,
        custom_logger: logging.Logger = None,
    ):
        self.name = name
        self.thread_count = THREADS_PER_POSTPROCESSOR_WORKER

        if custom_logger != None and not isinstance(custom_logger, logging.Logger):
            raise ValueError(f"custom_logger must be of instance logging.Logger")
        else:
            self.logger = custom_logger if isinstance(custom_logger, logging.Logger) else logger

        self.logger.debug(f"Initialized PostProcessor worker: {self.name}")
    
    async def _get_ffprobe_json(self, video_path: Path) -> dict:
        ffprobe_args = [
            "-v", "quiet",
            "-print_format", "json", 
            "-show_streams",
            "-show_format",
            str(video_path),
        ]

        ec, stdout, stderr = await self._run_ffprobe(ffprobe_args)

        if ec != 0:
            self.logger.error(f"ffprobe failed with stderr: {stderr.decode()}")
            raise RuntimeError(f"ffprobe failed to execute for file {video_path}")

        return json.loads(stdout)

    async def _run_ffprobe(self, args: list) -> tuple[int, bytes, bytes]:
        proc = await asyncio.create_subprocess_exec(
            FFPROBE_PATH,
            *args,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await proc.communicate()
        return (proc.returncode, stdout, stderr)

    def _is_mp4(self, ffprobe_json: dict) -> bool:
        mp4_format_strings = ["mp4", "m4a", "mov"]
        is_mp4 = any(fmt in ffprobe_json["format"]["format_name"] for fmt in mp4_format_strings)
        return is_mp4

    def _is_mpegts(self, ffprobe_json: dict) -> bool:
        mpegts_format_string = "mpegts"
        is_mpegts = mpegts_format_string in ffprobe_json["format"]["format_name"]
        return is_mpegts

    def _is_webm(self, ffprobe_json: dict) -> bool:
        # TODO: implement this
        # Stub for now so process_video_file works without raising anything
        return False

    async def process_video_file(self, video_path: Path) -> PostProcessResult:
        self.logger.debug(f"Processing video @ {video_path}")
        ffprobe_json = await self._get_ffprobe_json(video_path)
        
        converted_path = None
        if self._is_mp4(ffprobe_json):
            return PostProcessResult(video_path, 0)
        elif self._is_mpegts(ffprobe_json):
            if CONVERT_MPEGTS_TO_MP4 is True:
                converted_path, ec = await self._convert_mpegts_to_mp4(video_path)
            else:
                raise RuntimeError(f"CONVERT_MPEG_TO_MP4 is not enabled")
        elif self._is_webm(ffprobe_json):
            if CONVERT_WEBM_TO_MP4 is True:
                converted_path, ec = await self._convert_webm_to_mp4(video_path)
            else:
                raise RuntimeError(f"CONVERT_WEBM_TO_MP4 is not enabled")
        else:
            raise RuntimeError(f"Unrecognized format for {video_path}")

        if converted_path is not None and ec == 0:
            if DELETE_PREPROCESSED_VIDEO is True:
                try:
                    self._delete_video(video_path)
                except Exception as e:
                    self.logger.error(
                        f"Could not delete preprocessed video @ {video_path} "
                        f"with error: {e}"
                    )

        result = PostProcessResult(converted_path, ec)
        
        self.logger.debug(f"Done processing video @ {video_path}")
        return result

    async def _convert_mpegts_to_mp4(self, video_path: Path) -> tuple[Path, int]:
        # Rename the finalized file and mark that it was converted by suffixing
        # a "_c" to the end of the filename before the .mp4 extension
        video_output_path = Path(str(video_path.with_suffix('')) + "_c" + ".mp4")

        success, ec = await self._mpegtsmp4_try_copy(video_path, video_output_path)
        if success is True:
            return (video_output_path, ec)

        success, ec = await self._mpegtsmp4_try_reencode_h264(video_path, video_output_path)
        if success is True:
            return (video_output_path, ec)

        return (None, ec)


    async def _mpegtsmp4_try_copy(self, video_path, output_path) -> tuple[bool, int]:
        ffmpeg_args = [
            "-y", "-i", str(video_path),
            "-c", "copy", 
            str(output_path)
        ]
        ec, stdout, stderr = await self._call_ffmpeg(ffmpeg_args)

        success = True
        if ec != 0:
            success = False
        return (success, ec)

    async def _mpegtsmp4_try_reencode_h264(self, video_path, output_path) -> tuple[bool, int]:
        ffmpeg_args = [
            "-y", "-i", str(video_path), 
            "-threads", str(self.thread_count), 
            "-c:v", "libx264", "-crf", "20", "-preset", "medium",
            "-c:a", "aac",
            "-b:a", "128k",
            str(output_path)
        ]
        ec, stdout, stderr = await self._call_ffmpeg(ffmpeg_args)
        
        success = True
        if ec != 0:
            success = False
        return (success, ec)

    async def _convert_webm_to_mp4(self, video_path: Path) -> tuple[Path, int]:
        raise NotImplementedError()


    async def _call_ffmpeg(self, args: list) -> tuple[int, bytes, bytes]:
        proc = await asyncio.create_subprocess_exec(
            FFMPEG_PATH,
            *args,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await proc.communicate()
        return (proc.returncode, stdout, stderr)

    def _delete_video(self, video_path: Path):
        video_path.unlink(missing_ok=True)
