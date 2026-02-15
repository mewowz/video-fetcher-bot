import logging

def get_stdout_logger(name: str, level=logging.INFO):
    logger = logging.getLogger(name)

    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(logging.Formatter("%(message)s"))
        logger.addHandler(handler)
        logger.setLevel(level)

    return logger

def get_cog_logger(
    cog_name: str,
    level=logging.INFO,
    *,
    logfile: os.PathLike = None,
    console: bool = True
):
    logger = logging.getLogger(f"cog.{cog}")

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

