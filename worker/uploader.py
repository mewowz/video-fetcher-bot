import logging
logger = logging.getLogger(__name__)

import httpx
import asyncio
import os
import redis.asyncio as redis
import orjson as json
import yarl
from urllib.parse import urljoin

from utils.config import (
    MAX_UPLOAD_WORKERS,
    MAX_UPLOAD_JOBS,
    MAX_UPLOAD_RETRIES,
    CONTENT_SERVER_BASE_URL,
    CONTENT_SERVER_PORT,
    CONTENT_SERVER_BASE_PATH,
    DOWNLOADED_JOBS_QUEUE,
    REDIS_CONN_ARGS,
)
    
SENTINEL_OBJ = None

class UploaderPool:
    def __init__(
        self,
        max_upload_workers: int = MAX_UPLOAD_WORKERS,
        max_upload_jobs: int = MAX_UPLOAD_JOBS,
        redis_conn_args: dict = REDIS_CONN_ARGS,
        redis_timeout: float = 2.0,
        custom_logger: logging.Logger = None
    ):
        self._max_upload_workers = max_upload_workers
        self._job_queue = asyncio.Queue(maxsize=max_upload_jobs)
        self._http_aclient = httpx.AsyncClient()
        self._redis_aclient = redis.Redis(**redis_conn_args)
        self._redis_timeout = redis_timeout

        if custom_logger != None and not isinstance(custom_logger, logging.Logger):
            raise ValueError(f"custom_logger must be of instance logging.Logger")
        else:
            self.logger = custom_logger if isinstance(custom_logger, logging.Logger) else logger

        self._job_getter = UploadJobGetter(
            self._job_queue,
            self._redis_aclient,
            self._redis_timeout,
            self.logger,
        )

        self._upload_workers = [
            Uploader(
                f"uploader_{i}",
                self._job_queue,
                self._http_aclient,
                self.logger,
            ) for i in range(self._max_upload_workers)
        ]

        self._workers = []
        self._init_workers()

        self.logger.info("Initialized UploaderPool")

    def _init_workers(self):
        self._workers = [self._job_getter] + self._upload_workers

    async def start_workers(self):
        self.logger.debug("Starting all workers...")
        await self._start_workers()
        self.logger.debug("Successfully started all workers")

    async def _start_workers(self):
        for worker in self._workers:
            worker_task = asyncio.create_task(worker.run())
            worker.set_task(worker_task)
            worker.start()

    async def stop_workers(self):
        self.logger.debug("Stopping all workers...")
        await self._stop_workers()
        self.logger.debug("Successfully stopped all workers")

    async def _stop_workers(self):
        await self._stop_all()

    async def _stop_all(self):
        self._stop_jobgetter()
        for _ in range(self._max_upload_workers):
            try:
                self._stop_uploader()
            except Exception as e:
                raise
        tasks = [w.get_task() for w in self._workers]
        self.logger.debug("Waiting for {len(tasks)} to finish...")
        await asyncio.gather(*tasks, return_exceptions=True)

    def _stop_jobgetter(self):
        self._job_getter.stop()

    def _stop_uploader(self):
        try:
            self._job_queue.put_nowait(SENTINEL_OBJ)
        except asyncio.QueueFull:
            # TODO: handle the case when the queue is full but we need to destroy a worker
            raise
            

class UploadJobGetter:
    def __init__(
        self,
        job_queue: asyncio.Queue,
        redis_aclient: redis.Redis,
        redis_timeout: float = 2.0,
        custom_logger: logging.Logger = None
    ):
        self.job_queue = job_queue
        self.redis_aclient = redis_aclient
        self.redis_timeout = redis_timeout

        if custom_logger != None and not isinstance(custom_logger, logging.Logger):
            raise ValueError(f"custom_logger must be of instance logging.Logger")
        else:
            self.logger = custom_logger if isinstance(custom_logger, logging.Logger) else logger

        self.start_event = asyncio.Event()
        self.stop_event = asyncio.Event()

        self._task = None

        self.logger.debug("Created 1 UploadJobGetter")

    async def run(self):
        self.logger.debug(f"JobGetter waiting to start")
        await self.start_event.wait()
        self.start_event.clear()

        while not self.stop_event.is_set():
            result = await self.redis_aclient.brpop(DOWNLOADED_JOBS_QUEUE, timeout=self.redis_timeout)
            if result is None:
                self.logger.debug(f"Got no job after {self.redis_timeout} seconds")
                continue

            _, job = result
            job = json.loads(job)
            await self.job_queue.put(job)
            self.logger.debug("Pushed 1 job to the queue")
        
        self.stop_event.clear()
        self.logger.debug("JobGetter exiting...")

    def start(self):
        self._start()
    
    def _start(self):
        self.start_event.set()

    def stop(self):
        self._stop()

    def _stop(self):
        self.stop_event.set()

    def set_task(self, task: asyncio.Task):
        self._task = task

    def get_task(self) -> asyncio.Task:
        return self._task

    
class Uploader:
    def __init__(
        self,
        name: str,
        job_queue: asyncio.Queue,
        http_aclient: httpx.AsyncClient,
        custom_logger: logging.Logger = None
    ):
        self.name = name
        self.job_queue = job_queue
        self.http_aclient = http_aclient
        
        if custom_logger != None and not isinstance(custom_logger, logging.Logger):
            raise ValueError(f"custom_logger must be of instance logging.Logger")
        else:
            self.logger = custom_logger if isinstance(custom_logger, logging.Logger) else logger

        self.start_event = asyncio.Event()

        self._task = None

        self.logger.debug(f"Created uploader '{name}'")


    async def run(self):
        self.logger.debug(f"Uploader '{self.name}' waiting to start")
        await self.start_event.wait()
        self.start_event.clear()

        while True:
            job = await self.job_queue.get()
            if job == SENTINEL_OBJ:
                self.logger.info(f"Worker {self.name} exiting")
                break
            self.logger.debug(f"Worker {self.name} got job ID {job['job_id']}")

            try:
                status_code = await self._handle_job(job)
            except Exception as e:
                self.logger.debug(f"Got unhandled exception: {e}")
                continue

            self.logger.debug(
                f"Status: {status_code}. "
                f"Successfully submitted job {job['job_id']}"
            )

    async def _handle_job(self, job) -> int:
        payload = self.get_payload(job)

        try:
            status_code = await self._send_followup(
                job['reply']['webhook_url'],
                payload
            )
            return status_code
        except Exception as e:
            raise
     

    def get_payload(self, job: dict) -> dict:
        payload = {
            "content": str(
                yarl.URL(CONTENT_SERVER_BASE_URL + ":" + CONTENT_SERVER_PORT)
                / CONTENT_SERVER_BASE_PATH
                / job["unique_path_uuid"]
                / (job["filename"])
            ),
        }
        return payload

    async def _send_followup(
        self,
        url: str, 
        payload: dict,
        retries: int = MAX_UPLOAD_RETRIES
    ) -> int:
        if not isinstance(payload, dict):
            raise ValueError(
                "Argument 'payload' is not of type 'dict': "
                f"Got type '{type(payload)}'"
            )

        tries = 0
        while tries != retries:
            tries += 1
            try:
                resp = await self.http_aclient.post(
                    url,
                    json=payload,
                    timeout=10.0
                )
            except httpx.HTTPError as e:
                raise
        
            logger.debug(f"Got response code: {resp.status_code}")
            if resp.status_code != 200:
                logger.debug(f"Error while sending payload to {url}. ")
                if resp.status_code == 429:
                    # Discord actually gives a retry_after field in the JSON response
                    # if you're ever 429'd
                    wait_time = resp.json()["retry_after"]
                    logger.debug(f"Retrying in {wait_time} seconds")
                    # In a later implementation, we should probably just handle this by 
                    # defering them and re-adding them to the queue unless the queue 
                    # is empty or adding a field in the job schema to signal if its worth 
                    # adding to the queue at all.
                    await asyncio.sleep(wait_time)
                    continue
            else:
                logger.debug("Followup successful")
                return resp.status_code
    
        logger.error("Followup unsuccessful")
        return resp.status_code

    def start(self):
        self._start()

    def _start(self):
        self.start_event.set()
       
    def set_task(self, task: asyncio.Task):
        self._task = task

    def get_task(self) -> asyncio.Task:
        return self._task




