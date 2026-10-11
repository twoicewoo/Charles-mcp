import json

import httpx
import pytest

from charles_mcp.client import CharlesClient, CharlesSessionTooLargeError
from charles_mcp.config import Config


@pytest.mark.asyncio
async def test_export_session_json_stops_at_configured_byte_limit() -> None:
    payload = json.dumps([{"host": "api.example.com", "body": "x" * 512}]).encode()

    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=payload)

    client = CharlesClient(Config(session_export_max_bytes=128))
    client._client = httpx.AsyncClient(
        base_url="http://control.charles",
        transport=httpx.MockTransport(handler),
    )
    try:
        with pytest.raises(CharlesSessionTooLargeError, match="128 byte limit"):
            await client.export_session_json()
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_export_session_json_returns_small_session() -> None:
    payload = [{"host": "api.example.com", "path": "/ready"}]

    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=payload)

    client = CharlesClient(Config(session_export_max_bytes=1024))
    client._client = httpx.AsyncClient(
        base_url="http://control.charles",
        transport=httpx.MockTransport(handler),
    )
    try:
        assert await client.export_session_json() == payload
    finally:
        await client.close()
