NEW_JOBS_QUEUE = "dlqueue"
DOWNLOADED_JOBS_QUEUE = "finishedqueue"

YTDL_OUTPUT_DIR = "./data/videos"

import os
CPU_COUNT = os.cpu_count()

REDIS_CONN_ARGS = {
    "host": "localhost",
    "port": 6379,
}
