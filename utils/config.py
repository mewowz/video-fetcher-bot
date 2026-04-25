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

# 100MB in binary format - adjust accordingly
MAX_DOWNLOAD_FILESIZE_BYTES = 100 * (1024**2) 
