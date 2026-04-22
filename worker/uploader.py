import logging
logger = logging.getLogger(__name__)

import httpx
import asyncio
import os
import redis.asyncio as redis
from urllib.parse import urljoin

from utils.config import (
    MAX_UPLOAD_WORKERS,
    MAX_UPLOAD_JOBS,
    MAX_UPLOAD_RETRIES,
    CONTENT_SERVER_BASE_URL,
    DOWNLOADED_JOBS_QUEUE,
    REDIS_CONN_ARGS,
);
    
SENTINEL_OBJ = None

class UploaderPool:

    async def run(self):
        async with asyncio.TaskGroup() as tg:
            async with redis.Redis(**REDIS_CONN_ARGS) as redis_aclient:
                job_getter = tg.create_task(
                    get_jobs(
                        job_queue,
                        redis_aclient,
                        stop_event
                    )
                )
                upload_workers = [
                    tg.create_task(
                        upload_worker(
                            f"upload_worker_{i}",
                            http_client,
                            job_queue,
                        )
                    ) for i in range(MAX_UPLOAD_WORKERS)
                ]
                logger.info(f"Created {MAX_UPLOAD_WORKERS} workers")

        logger.info("TaskGroup has exited. Quitting...")



    def __init__(
        self,
        max_upload_workers: int = MAX_UPLOAD_WORKERS,
        max_upload_jobs: int = MAX_UPLOAD_JOBS,
        redis_conn_args: dict = REDIS_CONN_ARGS,
        redis_timeout: float = 2.0
    ):
        self.job_queue = asyncio.Queue(maxsize=max_upload_jobs)
        self.stop_event = asyncio.Event()
        self.http_aclient = httpx.AsyncClient()
        self.redis_aclient = redis.Redis(**redis_conn_args)
        self.redis_timeout = redis_timeout

        self.job_getter = 
    

    def _destroy_all(self):
        self.destroy_jobgetter()
        for _ in range(max_upload_workers):
            try:
                self.destroy_uploader()
            except Exception as e:
                raise


    def destroy_jobgetter(self):
        self.job_getter.stop()

    def destroy_uploader(self):
        try:
            self.job_queue.put_nowait(SENTINEL_OBJ)
        except QueueFull:
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
            await self.job_queue.put(job)
            self.logger.debug("Pushed 1 job to the queue")
        
        self.stop_event.clear()
        self.logger.debug("JobGetter exiting...")

    def start(self):
        self.start_event.set()

    def stop(self):
        self.stop_event.set()

    
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

        self.logger.debug(f"Created uploader '{name}'")


    async def run(self, name: str):
        self.logger.debug(f"Uploader '{self.name}' waiting to start")
        await self.start_event.wait()
        self.start_event.clear()

        while True:
            job = await self.job_queue.get()
            if job == SENTINEL_OBJ:
                self.logger.info(f"Worker {name} exiting")
                break
            self.logger.debug(f"Worker {name} got job ID {job['job_id']}")

            try:
                await self._handle_job(job)
            except Exception as e:
                self.logger.debug(f"Got unhandled exception: {e}")
                continue

            self.logger.debug(
                f"Status: {status}. "
                f"Successfully submitted job {job['job_id']}"
            )

    async def _handle_job(self, job):
        payload = self.get_payload(job)

        try:
            status = await self._send_followup(
                job['reply']['webhook_url'],
                payload
            )
        except Exception as e:
            raise
     

    def get_payload(self, job: dict) -> dict:
        payload = {
            "content": urljoin(
                CONTENT_SERVER_BASE_URL,
                job['download_path']
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
                    wait_time = 2**tries
                    logger.debug(f"Retrying in {wait_time} seconds")
                    await asyncio.sleep(wait_time)
                    continue
            else:
                logger.debug("Followup successful")
                return resp.status_code
    
        logger.error("Followup unsuccessful")
        return resp.status_code

    def start(self):
        self.start_event.set()
       
        




