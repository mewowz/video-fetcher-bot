LOG_DIR = "./logs"
LOGFILE_NAME = "bot.log"

NEW_JOBS_QUEUE = "dlqueue"
DOWNLOADED_JOBS_QUEUE = "finishedqueue"

YTDL_OUTPUT_DIR = "./data/videos"

import os
CPU_COUNT = os.cpu_count()

REDIS_CONN_ARGS = {
    "host": "localhost",
    "port": 6379,
}

MAX_UPLOAD_WORKERS = CPU_COUNT * 4
MAX_UPLOAD_JOBS = 100
MAX_UPLOAD_RETRIES = 3
CONTENT_SERVER_BASE_URL = "http://localhost"
CONTENT_SERVER_PORT = "4444"
CONTENT_SERVER_BASE_PATH = "videos"

# 100MB in binary format - adjust accordingly
MAX_DOWNLOAD_FILESIZE_BYTES = 100 * (1024**2) 

FFMPEG_PATH = ""
FFPROBE_PATH = ""
CONVERT_MPEGTS_TO_MP4 = True
CONVERT_WEBM_TO_MP4 = False 
