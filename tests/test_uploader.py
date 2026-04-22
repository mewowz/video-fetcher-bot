import pytest
import httpx
import asyncio
from unittest.mock import AsyncMock

from worker.uploader import UploaderPool, UploadJobGetter, Uploader
from utils.config import CONTENT_SERVER_BASE_URL

def test_uploader_get_payload():
    example_path = "./some/relative/path.mp4"
    expected = {
        "content": CONTENT_SERVER_BASE_URL + "/some/relative/path.mp4"
    }
    uploader = Uploader(None, None, None)
    got = uploader.get_payload({"download_path":example_path})
    assert got == expected

def test_uploader_send_followup(monkeypatch):
    uploader = Uploader(None, None, None)
    fake_url = "https://localhost.com"
    transport_200 = httpx.MockTransport(
        lambda r: httpx.Response(200)
    )

    expected_ret = 200
    uploader.http_aclient = httpx.AsyncClient(transport=transport_200)

    got = asyncio.run(uploader._send_followup(fake_url, {}))

    assert got == expected_ret

    monkeypatch.setattr(asyncio, "sleep", AsyncMock())
    transport_429 = httpx.MockTransport(
        lambda r: httpx.Response(429, json={"retry_after": 5})
    )
    expected_ret = 429
    uploader.http_aclient = httpx.AsyncClient(transport=transport_429)
    got = asyncio.run(uploader._send_followup(fake_url, {}))
    
    assert got == expected_ret

    def raise_transport(r):
        raise httpx.HTTPError("blah")
    exception_transport = httpx.MockTransport(raise_transport)
    uploader.http_aclient = httpx.AsyncClient(transport=exception_transport)
    with pytest.raises(httpx.HTTPError) as got_exception:
        got = asyncio.run(uploader._send_followup(fake_url, {}))


