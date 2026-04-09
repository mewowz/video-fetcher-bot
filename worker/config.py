from pathlib import Path

STAGING_DIR = Path("./staging")
DOWNLOADS_DIR = STAGING_DIR / Path("downloads")
TMP_DOWNLOADS_DIR = DOWNLOADS_DIR / Path("tmp")

MAX_DOWNLOAD_SIZE_BYTES = 500 * 1000 * 1000 # 500MB

_dirs = [STAGING_DIR, DOWNLOADS_DIR, TMP_DOWNLOADS_DIR]

def init_dirs():
    for d in _dirs:
        try:
            d.mkdir(parents=True, exist_ok=True)
        except FileExistsError:
            raise
        except PermissionError:
            raise
        except IOError:
            raise # handle all these later



