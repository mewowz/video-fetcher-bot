import logging
logger = logging.getLogger(__name__)
import redis

import threading
from concurrent.futures import ThreadPoolExecutor, Future

from worker.worker import Worker
from utils.config import CPU_COUNT, REDIS_CONN_ARGS

class WorkerPool:
    def __init__(
        self,
        initial_workers_ct: int = CPU_COUNT,
        pool_min_size: int = 1,
        pool_max_size: int = 4,
        *,
        custom_logger: logging.Logger = None,
        redis_args: dict = None,
    ):
        self._min_workers = pool_min_size
        self._max_workers = pool_max_size
        self._tpe = ThreadPoolExecutor(max_workers=self._max_workers)

        if custom_logger != None and not isinstance(custom_logger, logging.Logger):
            raise ValueError(f"custom_logger must be of instance logging.Logger")
        else:
            self.logger = custom_logger if isinstance(custom_logger, logging.Logger) else logger

        if isinstance(redis_args, dict):
            rd_args = REDIS_CONN_ARGS | redis_args
        else:
            rd_args = REDIS_CONN_ARGS
        self._redis_pool = redis.ConnectionPool(**rd_args)

        worker_name_base = "worker"
        self._dl_workers = self._init_workers(initial_workers_ct, worker_name_base)

        self.logger.info(f"Initialized WorkerPool")

    def _init_workers(self, n_workers: int, worker_name_base: str) -> list[tuple[Worker, Future]]:
        workers = []
        for worker_i in range(n_workers):
            new_worker = Worker(
                f"{worker_name_base}_{worker_i}",
                redis.Redis(connection_pool=self._redis_pool),
                stop_event=threading.Event(),
            )
            fut = self._tpe.submit(new_worker.run)
            workers.append(
                (new_worker, fut,)
            )
        self.debug(f"Initialized {n_workers} for {self.__class__.__name__}")

        return workers

    def start_workers(self):
        for w, fut in self._dl_workers:
            w.start()

    def stop_workers(self):
        for w, fut in self._dl_workers:
            w.stop()

