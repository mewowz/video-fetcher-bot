import pytest

import asyncio
import redis
import threading
import orjson as json
import subprocess
from pathlib import Path
from hashlib import sha256

import worker.downloader
from worker.worker import Worker
from utils.config import YTDL_OUTPUT_DIR, NEW_JOBS_QUEUE, DOWNLOADED_JOBS_QUEUE

@pytest.mark.integration
def test_worker_pipeline(monkeypatch):
    redis_conn = redis.Redis()
    stop_event = threading.Event()
    worker = Worker("test_worker", redis.Redis(), stop_event=stop_event)

    test_youtube_link = "https://youtu.be/umQL37AC_YM" # "I've never installed GNU/Linux"
    fake_uuid_path = "123ABC"
    expected_path = Path(YTDL_OUTPUT_DIR) / fake_uuid_path / "umQL37AC_YM"

    monkeypatch.setattr("worker.downloader.uuid4", lambda: fake_uuid_path)

    example_job = { "job_id": "1234", "request": { "url": test_youtube_link } }

    redis_conn.lpush(NEW_JOBS_QUEUE, json.dumps(example_job))
    worker._handle_job()

    result = redis_conn.blpop(DOWNLOADED_JOBS_QUEUE)
    _, job = result
    job = json.loads(job)

    assert Path(job["download_path"]) == expected_path
    assert expected_path.exists() == True
    assert (expected_path / "umQL37AC_YM.mp4").stat().st_size > 4096 # Arbitrarily checking its larger than 4KB at the minimum

    res = subprocess.run(
            ["ffprobe",
            "-v", "error", str(expected_path / "umQL37AC_YM.mp4")],
            capture_output=True
    )
    assert res.returncode == 0
    

    




