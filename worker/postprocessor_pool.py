import logging
logger = logging.getLogger(__name__)

import asyncio
import redis.asyncio as redis
import orjson as json
from pathlib import Path

from worker.postprocessor import PostProcessor, PostProcessResult
from utils.config import (
        MAX_POSTPROCESSOR_WORKERS, 
        REDIS_CONN_ARGS, 
        DOWNLOADED_JOBS_QUEUE, 
        POSTPROCESS_JOBS_QUEUE,
        MAX_POSTPROCESSOR_JOBS
)

SENTINEL_OBJ = None

class PostProcessorPool:
    DEFAULT_MAX_POSTPROCESSOR_WORKERS = 1
    def __init__(
        self,
        max_postprocessor_workers: int = MAX_POSTPROCESSOR_WORKERS,
        max_postprocessor_jobs: int = MAX_POSTPROCESSOR_JOBS,
        redis_conn_args: dict = REDIS_CONN_ARGS,
        redis_timeout: float = 2.0,
        custom_logger: logging.Logger = None
    ):
        self._job_queue = asyncio.Queue(maxsize=max_postprocessor_jobs)
        self._redis_pool = redis.ConnectionPool(**redis_conn_args)
        self._redis_timeout = redis_timeout

        if custom_logger != None and not isinstance(custom_logger, logging.Logger):
            raise ValueError(f"custom_logger must be of instance logging.Logger")
        else:
            self.logger = custom_logger if isinstance(custom_logger, logging.Logger) else logger

        self._max_workers = max_postprocessor_workers
        if self._max_workers < 0:
            self.logger.warning(
                "MAX_POSTPROCESSOR_WORKERS is less than 0: "
                f"Got {max_postprocessor_workers}. "
                f"Defaulting to {self.DEFAULT_MAX_POSTPROCESSOR_WORKERS} workers"
            )
            self._max_workers = self.DEFAULT_MAX_POSTPROCESSOR_WORKERS

        self._job_getter = PostProcessorJobGetter(
            self._job_queue,
            redis.Redis(connection_pool=self._redis_pool),
            self._redis_timeout,
            self.logger,
        )

        self._postprocessor_workers = [
            PostProcessorWorker(
                f"postprocessor_worker_{i}",
                self._job_queue,
                redis.Redis(connection_pool=self._redis_pool),
                self.logger,
            ) for i in range(self._max_workers)
        ]

        self._workers = []
        self._init_workers()

        self.logger.info("Initialized PostProcessorPool")

    def _init_workers(self):
        self._workers = [self._job_getter] + self._postprocessor_workers

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
        for _ in range(self._max_workers):
            try:
                self._stop_postprocessor()
            except Exception as e:
                raise
        tasks = [w.get_task() for w in self._workers]
        self.logger.debug("Waiting for {len(tasks)} to finish...")
        await asyncio.gather(*tasks, return_exceptions=True)

    def _stop_jobgetter(self):
        self._job_getter.stop()

    def _stop_postprocessor(self):
        try:
            self._job_queue.put_nowait(SENTINEL_OBJ)
        except asyncio.QueueFull:
            # TODO: handle the case when the queue is full but we need to destroy a worker
            raise
     


class PostProcessorJobGetter:
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

        self._start_event = asyncio.Event()
        self._stop_event = asyncio.Event()

        self._task = None

        self.logger.debug("Created 1 PostProcessorJobHandler")

    
    async def run(self):
        self.logger.debug(f"PostProcessJobGetter waiting to start")
        await self._start_event.wait()
        self._start_event.clear()

        while not self._stop_event.is_set():
            result = await self.redis_aclient.brpop(DOWNLOADED_JOBS_QUEUE, timeout=self.redis_timeout)
            if result is None:
                self.logger.debug(f"Got no job after {self.redis_timeout} seconds")
                continue

            _, job = result
            job = json.loads(job)
            await self.job_queue.put(job)
            self.logger.debug("Pushed 1 job to the queue")
        
        self._stop_event.clear()
        self.logger.debug("PostProcessJobGetter exiting...")

    def start(self):
        self._start()
    
    def _start(self):
        self._start_event.set()

    def stop(self):
        self._stop()

    def _stop(self):
        self._stop_event.set()

    def set_task(self, task: asyncio.Task):
        self._task = task

    def get_task(self) -> asyncio.Task:
        return self._task


class PostProcessorWorker:
    def __init__(
        self,
        name: str,
        job_queue: asyncio.Queue,
        redis_aclient: redis.Redis,
        custom_logger: logging.Logger = None
    ):
        self.name = name
        self.job_queue = job_queue
        self.redis_aclient = redis_aclient

        if custom_logger != None and not isinstance(custom_logger, logging.Logger):
            raise ValueError(f"custom_logger must be of instance logging.Logger")
        else:
            self.logger = custom_logger if isinstance(custom_logger, logging.Logger) else logger


        self._start_event = asyncio.Event()
        self._stop_event = asyncio.Event()

        self._task = None

        self._pprocessor = PostProcessor(self.name + "_pp", self.logger)


    async def run(self):
        self.logger.debug(f"PostProcessorWorker {self.name} waiting to start")
        await self._start_event.wait()
        self._start_event.clear()
        while not self._stop_event.is_set():
            job = await self.job_queue.get()
            if job == SENTINEL_OBJ:
                self.logger.debug(f"PostProcessorWorker {self.name} exiting...")
                break

            self.logger.debug(f"PostProcessorWorker {self.name} got job ID {job['job_id']}")
            try:
                video_path = self._get_video_path(job)
                result = await self._pprocessor.process_video_file(video_path)
                await self._submit_job(job, result)
            except Exception as e:
                self.logger.error(f"Got unhandled exception: {e}")
                continue

    async def _submit_job(self, job: dict, result: dict):
        if result.exit_code != 0:
            self.logger.debug(f"Job {job['job_id']} failed. Continuing...")
            return
        
        job["filename"] = str(result.output_name.name)
        await self.redis_aclient.lpush(POSTPROCESS_JOBS_QUEUE, json.dumps(job))
        

    def _get_video_path(self, job: dict) -> Path:
        dir = job["download_path"]
        filename = job["filename"]
        return Path(dir) / Path(filename)

    def start(self):
        self._start()
    
    def _start(self):
        self._start_event.set()

    def stop(self):
        self._stop()

    def _stop(self):
        self._stop_event.set()

    def set_task(self, task: asyncio.Task):
        self._task = task

    def get_task(self) -> asyncio.Task:
        return self._task

