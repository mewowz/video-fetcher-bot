import sys
import logging
import os

class WorkerLogger(logging.LoggerAdapter):
    def __init__(self, base_logger: logging.Logger, extra={}):
        super().__init__(base_logger, extra)
        self.job_id = None

    def set_job_id(self, job_id):
        self.job_id = job_id

    def clear_job_id(self):
        self.job_id = None

    def process(self, msg, kwargs):
        if self.job_id is not None:
            level = kwargs.get("level", logging.INFO) 
            if level == logging.DEBUG or level == logging.ERROR:
                msg = f"[Job {self.job_id}] {msg}"
        return msg, kwargs

def get_stdout_logger(name: str, level=logging.INFO):
    logger = logging.getLogger(name)

    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(logging.Formatter("%(message)s"))
        logger.addHandler(handler)
        logger.setLevel(level)

    return logger

def get_worker_logger(
    worker_name: str,
    level=logging.INFO,
    *,
    logfile: os.PathLike = None,
    console: bool = True
):
    logger = logging.getLogger(f"worker.{worker_name}")

    if not logger.handlers:
        formatter = logging.Formatter(
        "[%(created).2f] [%(name)s] %(levelname)s: %(message)s"
        )
        if logfile:
            file_handler = logging.FileHandler(logfile, mode='a')
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)
        if console:
            console_handler = logging.StreamHandler(sys.stdout)
            console_handler.setFormatter(formatter)
            logger.addHandler(console_handler)

        logger.setLevel(level)
        logger.propagate = False

    return logger

