import logging
import os
import asyncio
import sys
from pathlib import Path

from bot.bot import bot, init_bot
from utils.config import LOG_DIR, LOGFILE_NAME
from worker.worker_pool import WorkerPool
from worker.uploader import UploaderPool

def basic_stdout_logger():
    logging.basicConfig(
            level=logging.DEBUG,
            format="[%(asctime)s] [%(name)s.%(funcName)s] %(levelname)s: %(message)s",
            stream=sys.stdout
    )
    logging.getLogger("discord").setLevel(logging.WARNING)

def setup_logger():
    Path(LOG_DIR).mkdir(exist_ok=True)

    fmt = logging.Formatter("[%(asctime)s] [%(name)s.%(funcName)s] %(levelname)s: %(message)s",)

    file_handler = logging.FileHandler(LOG_DIR / LOGFILE_NAME)
    file_handler.setFormatter(fmt)
    file_handler.setLevel(logging.DEBUG)

    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(fmt)
    stream_handler.setLevel(logging.DEBUG)

    root = logging.getLogger()
    root.setLevel(logging.DEBUG)
    root.addHandler(file_handler)
    root.addHandler(stream_handler)

    logging.getLogger("discord").setLevel(logging.WARNING)
    logging.getLogger("discord.http").setLevel(logging.WARNING)
    logging.getLogger("discord.gateway").setLevel(logging.WARNING)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)


def main():
    token = os.environ.get("DISCORD_TOKEN", "")
    if not token:
        raise EnvironmentError("No environment variable set for DISCORD_TOKEN")

    downloaders = WorkerPool()
    downloaders.start_workers()

    asyncio.run(async_jobs(token))

async def async_jobs(token):
    uploaders = UploaderPool()
    await uploaders.start_workers()

    await run_bot(token)

async def run_bot(token):
    await init_bot()
    async with bot:
        await bot.start(token)

if __name__ == "__main__":
    # This is just used for testing right now
    #basic_stdout_logger()
    setup_logger()
    main()
