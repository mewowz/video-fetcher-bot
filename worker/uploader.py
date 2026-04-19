import logging
logger = logging.getLogger(__name__)

import httpx
import asyncio
import os
from urllib.parse import urljoin

from utils.config import MAX_UPLOAD_WORKERS, CONTENT_SERVER_BASE_URL

SENTINEL_OBJ = None

async def _send_followup(
    client: httpx.AsyncClient, 
    url: str, 
    payload: dict,
    retries: int = 3
) -> int:
    tries = 0
    while tries != retries:
        tries += 1
        if not isinstance(payload, dict):
            raise ValueError(
                "Argument 'payload' is not of type 'dict': "
                f"Got type '{type(payload}'"
            )
        try:
            resp = client.post(
                url,
                json=payload,
                timeout=10.0
            )
            logger.debug(f"Got response code: {resp.status_code}")
            if resp.status_code != 200:
                logger.debug(f"Error while sending payload to {url}. ")
                if resp.status_code == 429:
                    wait_time = 2**tries
                    logger.debug(f"Retrying in {wait_time} seconds")
                    asyncio.sleep(wait_time)
                    continue
            else:
                logger.debug("Followup successful")
                return resp.status_code
        except httpx.HTTPError as e:
            raise


async def upload_worker(
    name: str,
    http_client: httpx.AsyncClient,
    job_queue: asyncio.Queue,
):
    while True:
        job = await job_queue.get()
        if job == SENTINEL_OBJ:
            logger.info(f"Worker {name} exiting")
            break
        logger.debug("Worker {name} got job ID {job['job_id']}")
        try:
            payload = {
                "content": urllib.join(
                    CONTENT_SERVER_BASE_URL,
                    job['download_path']
                ),
            }
            status = _send_followup(
                http_client,
                job['reply']['webhook_url'],
                payload
            )
            logger.debug(f"Status: {status}")
            logger.debug(f"Successfully submitted job {job['job_id']}")
        except Exception as e:
            logger.debug(f"Got unhandled exception: {e}")
            continue

        



