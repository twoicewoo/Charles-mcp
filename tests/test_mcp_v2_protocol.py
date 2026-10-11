import pytest
from mcp import Client

from charles_mcp.config import Config
from charles_mcp.server import create_server


@pytest.mark.asyncio
async def test_mcp_client_initializes_and_lists_tools(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("CHARLES_VNEXT_STATE_DIR", str(tmp_path / "reverse"))
    config = Config(base_dir=str(tmp_path), state_dir=str(tmp_path))

    async with Client(create_server(config)) as client:
        result = await client.list_tools()
        captures = await client.call_tool("reverse_list_captures", {})

    names = {tool.name for tool in result.tools}
    assert "start_live_capture" in names
    assert "charles_status" in names
    assert "reverse_replay_entry" in names
    assert captures.is_error is False
