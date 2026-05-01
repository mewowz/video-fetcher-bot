import argparse
import logging
import os
import asyncio
import sys
from pathlib import Path

from bot.bot import bot, init_bot
from utils.config import LOG_DIR, LOGFILE_NAME
from worker.worker_pool import WorkerPool
from worker.uploader import UploaderPool
from worker.postprocessor_pool import PostProcessorPool

AVAILABLE_MODULES = ["bot", "downloader", "postprocessor", "uploader"]
modules_to_run = []

def setup_logger(log_level: int, yes_log_file: bool, log_stdout: bool):
    root = logging.getLogger()
    root.setLevel(log_level)
    fmt = logging.Formatter("[%(asctime)s] [%(name)s.%(funcName)s] %(levelname)s: %(message)s",)

    if yes_log_file:
        Path(LOG_DIR).mkdir(exist_ok=True)
        file_handler = logging.FileHandler(Path(LOG_DIR) / Path(LOGFILE_NAME))
        file_handler.setFormatter(fmt)
        file_handler.setLevel(log_level)
        root.addHandler(file_handler)

    if log_stdout:
        stream_handler = logging.StreamHandler(sys.stdout)
        stream_handler.setFormatter(fmt)
        stream_handler.setLevel(log_level)
        root.addHandler(stream_handler)

    logging.getLogger("discord").setLevel(logging.WARNING)
    logging.getLogger("discord.http").setLevel(logging.WARNING)
    logging.getLogger("discord.gateway").setLevel(logging.WARNING)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="video-fetcher-bot",
        description="video-fetcher: a program for users to download "
                    "their favorite videos and have them available " 
                    "download on discord"
    )
    
    parser.add_argument(
        "--modules",
        nargs="+",
        choices=AVAILABLE_MODULES + ["all"],
        default=["all"],
        metavar="MODULE",
        help=f"Modules to run. Choices: {', '.join(AVAILABLE_MODULES + ['all'])}. Default: all"
    )

    parser.add_argument(
        "-v", "--verbose",
        action="count",
        default=0,
        help="Verbosity level. -v for INFO, -vv for DEBUG. Default: WARNING"
    )

    parser.add_argument(
        "--log-stdout",
        action="store_true",
        default=False,
        help="Enable logging to stdout. Default: disabled"
    )

    parser.add_argument(
        "--no-log-file",
        action="store_true",
        default=False,
        help="Disable logging to file. Default: enabled"
    )

    parser.add_argument(
        "--daemon",
        action="store_true",
        default=False,
        help="Run as a daemon process"
    )

    parser.add_argument(
        "--config",
        type=str,
        default=None,
        metavar="PATH",
        help="Path to an alternate config file"
    )

    return parser.parse_args()

def handle_log_args(args: argparse.Namespace):
    LOG_LEVELS = [logging.WARNING, logging.INFO, logging.DEBUG]
    verbosity_level = LOG_LEVELS[min([args.verbose, 2])]

    yes_log_file = not args.no_log_file
    log_stdout = args.log_stdout

    setup_logger(verbosity_level, yes_log_file, log_stdout)

def handle_module_args(args: argparse.Namespace):
    global modules_to_run
    modules = args.modules
    if "all" in modules:
        modules_to_run = AVAILABLE_MODULES
        return
    else:
        modules_to_run = list(modules)
    
def handle_daemon_arg(args: argparse.Namespace):
    ...

def handle_config_arg(args: argparse.Namespace):
    ...

def handle_args(args: argparse.Namespace):
    handle_log_args(args)
    handle_module_args(args)
    handle_daemon_arg(args)
    handle_config_arg(args)

def main():
    if "downloader" in modules_to_run:
        downloader_pool = WorkerPool()
        downloader_pool.start_workers()

    asyncio.run(async_jobs())

async def async_jobs():
    if "uploader" in modules_to_run:
        uploaders = UploaderPool()
        await uploaders.start_workers()

    if "postprocessor" in modules_to_run:
        postprocessors = PostProcessorPool()
        await postprocessors.start_workers()

    if "bot" in modules_to_run:
        await run_bot()
    else:
        await asyncio.Event().wait() # keep the loop alive incase we're not running the bot


async def run_bot():
    token = os.environ.get("DISCORD_TOKEN", "")
    if not token:
        raise EnvironmentError("No environment variable set for DISCORD_TOKEN")
    await init_bot()
    async with bot:
        await bot.start(token)

if __name__ == "__main__":
    args = parse_args()
    handle_args(args)
    main()
