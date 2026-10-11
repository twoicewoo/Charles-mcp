import httpx
import pytest

from charles_mcp.client import CharlesClient, CharlesClientError
from charles_mcp.config import Config


class _RecordingClient(CharlesClient):
    def __init__(
        self,
        *,
        clear_result: bool = True,
        start_result: bool = True,
        stop_result: bool = True,
        baseline_result: list[dict] | None = None,
        export_result: list[dict] | None = None,
        recording: bool = False,
    ) -> None:
        super().__init__(Config())
        self.clear_result = clear_result
        self.start_result = start_result
        self.stop_result = stop_result
        self.baseline_result = baseline_result or []
        self.export_result = export_result or []
        self.export_calls = 0
        self.clear_calls = 0
        self.stop_calls = 0
        self.recording = recording

    async def clear_session(self) -> bool:
        self.clear_calls += 1
        return self.clear_result

    async def is_recording(self) -> bool:
        return self.recording

    async def start_recording(self) -> bool:
        return self.start_result

    async def stop_recording(self) -> bool:
        self.stop_calls += 1
        return self.stop_result

    async def export_session_json(self) -> list[dict]:
        self.export_calls += 1
        if self.export_calls == 1:
            return list(self.baseline_result)
        return list(self.export_result)


@pytest.mark.asyncio
async def test_record_session_raises_when_clear_fails() -> None:
    client = _RecordingClient(clear_result=False)

    with pytest.raises(CharlesClientError, match="clear"):
        await client.record_session(1, reset_session=True)


@pytest.mark.asyncio
async def test_record_session_raises_when_start_fails() -> None:
    client = _RecordingClient(start_result=False)

    with pytest.raises(CharlesClientError, match="start"):
        await client.record_session(1)


@pytest.mark.asyncio
async def test_record_session_raises_when_stop_fails() -> None:
    client = _RecordingClient(stop_result=False)

    with pytest.raises(CharlesClientError, match="stop"):
        await client.record_session(1)


@pytest.mark.asyncio
async def test_record_session_raises_when_export_is_empty() -> None:
    client = _RecordingClient(export_result=[])

    with pytest.raises(CharlesClientError, match="empty"):
        await client.record_session(1)


@pytest.mark.asyncio
async def test_record_session_raises_when_export_is_unchanged_from_baseline() -> None:
    stale_snapshot = [{"host": "api.example.com", "path": "/stale"}]
    client = _RecordingClient(
        baseline_result=stale_snapshot,
        export_result=stale_snapshot,
    )

    with pytest.raises(CharlesClientError, match="unchanged"):
        await client.record_session(1)


@pytest.mark.asyncio
async def test_record_session_preserves_baseline_and_does_not_clear_by_default(monkeypatch) -> None:
    async def no_wait(seconds):
        return None

    monkeypatch.setattr("charles_mcp.client.asyncio.sleep", no_wait)
    old = {"host": "old.example"}
    new = {"host": "new.example"}
    client = _RecordingClient(baseline_result=[old], export_result=[old, new])
    result = await client.record_session(1)
    assert result == [new]
    assert client.clear_calls == 0
    assert client.stop_calls == 1


@pytest.mark.asyncio
async def test_record_session_leaves_existing_recording_running(monkeypatch) -> None:
    async def no_wait(seconds):
        return None

    monkeypatch.setattr("charles_mcp.client.asyncio.sleep", no_wait)
    client = _RecordingClient(export_result=[{"host": "new.example"}], recording=True)
    await client.record_session(1)
    assert client.stop_calls == 0
    assert client.clear_calls == 0


@pytest.mark.asyncio
async def test_quit_charles_does_not_claim_success_on_connection_failure() -> None:
    class DisconnectedClient:
        async def get(self, path, timeout):
            raise httpx.ConnectError(
                "connection refused", request=httpx.Request("GET", "http://127.0.0.1/quit")
            )

    client = CharlesClient(Config())
    client._client = DisconnectedClient()  # type: ignore[assignment]
    assert await client.quit_charles() is False
