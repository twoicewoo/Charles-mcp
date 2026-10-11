import os
import stat

import pytest

from charles_mcp.reverse.config import VNextConfig
from charles_mcp.reverse.storage import SQLiteStore


@pytest.mark.skipif(os.name == "nt", reason="POSIX file mode test")
def test_reverse_state_and_database_are_private(tmp_path) -> None:
    config = VNextConfig(state_root=tmp_path / "reverse")
    store = SQLiteStore(config.database_path)
    try:
        for path in (config.state_root, config.artifacts_dir, config.temp_dir):
            assert stat.S_IMODE(path.stat().st_mode) == 0o700
        assert stat.S_IMODE(config.database_path.stat().st_mode) == 0o600
    finally:
        store.close()


@pytest.mark.skipif(os.name == "nt", reason="POSIX file mode test")
def test_existing_reverse_state_permissions_are_hardened(tmp_path) -> None:
    state_root = tmp_path / "reverse"
    config = VNextConfig(state_root=state_root)
    store = SQLiteStore(config.database_path)
    store.close()
    for path in (state_root, config.artifacts_dir, config.temp_dir):
        path.chmod(0o755)
    config.database_path.chmod(0o644)

    hardened = VNextConfig(state_root=state_root)
    reopened = SQLiteStore(hardened.database_path)
    try:
        for path in (state_root, hardened.artifacts_dir, hardened.temp_dir):
            assert stat.S_IMODE(path.stat().st_mode) == 0o700
        assert stat.S_IMODE(hardened.database_path.stat().st_mode) == 0o600
    finally:
        reopened.close()
