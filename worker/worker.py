import logging
logger = logging.getLogger(__name__)

import redis
import orjson as json
import threading

from pathlib import Path

from worker.downloader import Downloader
from utils.config import NEW_JOBS_QUEUE, DOWNLOADED_JOBS_QUEUE

class Worker:
    def __init__(
        self,
        name: str,
        redis_conn: redis.Redis,
        *,
        stop_event: threading.Event = None,
        redis_timeout: int = 2,
        dl_type: str = "local",
        custom_logger: logging.Logger = None,
        downloader_logger: logging.Logger = None,
        downloader_opts: dict = dict(),
        ytdlp_opts: dict = dict(),
    ):
        if not isinstance(name, str):
            raise ValueError("name must be of instance str")
        else:
            self.name = name

        if not isinstance(redis_conn, redis.Redis):
            raise ValueError("redis_conn must be of instance redis.Redis")
        else:
            self.redis = redis_conn

        if not isinstance(stop_event, threading.Event):
            raise ValueError("stop_event must be of instance threading.Event")
        else:
            self._stop_event = stop_event

        if isinstance(redis_timeout, int) and redis_timeout >= 0:
            self.redis_timeout = redis_timeout
        else:
            raise ValueError(f"redis_timeout must be of int and >= 0")

        if custom_logger != None and not isinstance(custom_logger, logging.Logger):
            raise ValueError(f"custom_logger must be of instance logging.Logger")
        else:
            self.logger = custom_logger if isinstance(custom_logger, logging.Logger) else logger

        self.dl_type = dl_type
        self.downloader_opts = downloader_opts
        self.ytdlp_opts = ytdlp_opts
        self.downloader = Downloader(
            name=self.name + "_downloader",
            dl_type=self.dl_type,
            ytdlp_opts=self.ytdlp_opts,
            custom_logger=self.logger,
            downloader_opts=self.downloader_opts,
        )

        self._startloop = threading.Event()

        self.logger.debug(f"Initialized Worker name: {self.name}")

    def run(self):
        self.logger.info(f"Ready to run worker loop for {self.name}")
        self._wait_startloop()
        self.logger.info(f"Running worker loop for {self.name}")
        while not self._stop_event.is_set():
            try:
                self._handle_job()
            except Exception as e:
                self.logger.error(
                    f"Worker {self.name} received unhandled "
                    f"exception of type {type(e)}. Continuing..."
                )
                self.logger.debug(f"{self.name} Error: {e}")
                break

        self.logger.info(f"Exiting worker loop for {self.name}")

    def stop(self):
        self._startloop.clear()
        self._stop_event.set()

    def start(self):
        self._startloop.set()

    def _wait_startloop(self):
        return self._startloop.wait()

    def _handle_job(self):
        job = self._get_job_from_queue()
        if job == None:
            self.logger.debug(f"{self.name} got timeout. Restarting loop")
            return 
        self.logger.info(f"Receieved job ID: {job['job_id']}")

        try:
            self.logger.info(f"Downloading video {job['request']['url']} for job ID: {job['job_id']}")
            ec, dl_path = self.downloader.download(job['request']['url'])
            self.logger.info(
                f"Successfully downloaded video {job['request']['url']} to "
                f"{dl_path} for job ID: {job['job_id']}"
            )
        except Exception as e:
            self.logger.error(f"Got error for job ID: {job['job_id']} on download.")
            self.logger.debug(
                "Error details:\n"
                f"Exception: {e}.\n"
                f"Job: {job}"
            )
            raise

        try:
            self.logger.info(
                f"Submitting finished download for video {job['request']['url']} "
                f"job ID: {job['job_id']}"
            )
            self._submit_finished_job(job, dl_path)
            self.logger.info(f"Successfully submitted job ID: {job['job_id']}")
        except Exception as e:
            self.logger.error(f"Got error for job ID: {job['job_id']} on submit.")
            self.logger.debug(
                "Error details:\n"
                f"Exception: {e}.\n"
                f"Job: {job}"
            )
            raise

        self.logger.info(f"Successfully downloaded video {job['request']['url']} (Job ID: {job['job_id']})")


    def _get_job_from_queue(self) -> dict:
        result = self.redis.brpop(NEW_JOBS_QUEUE, self.redis_timeout)
        if result is None:
            return None 
        _, job = result
        job = json.loads(job)
        return job
    
    def _submit_finished_job(self, job: dict, dl_path: str):
        if not isinstance(dl_path, str):
            raise TypeError(f"dl_path must be of type 'str'. Got type '{type(dl_path)}'")
        job["status"] = "downloaded"
        job["download_path"] = dl_path
        self.redis.lpush(DOWNLOADED_JOBS_QUEUE, json.dumps(job))



