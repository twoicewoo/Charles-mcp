"""Near-real-time snapshot analysis built on official Charles exports."""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from uuid import uuid4

from defusedxml import ElementTree as ET  # noqa: N817 — stdlib idiom

from charles_mcp.reverse.models import CaptureSourceFormat, CaptureSourceKind
from charles_mcp.reverse.services.charles_control_service import CharlesControlService
from charles_mcp.reverse.services.ingest_service import IngestService
from charles_mcp.reverse.services.query_service import QueryService

logger = logging.getLogger(__name__)


@dataclass
class LiveSessionState:
    live_session_id: str
    created_at: str
    last_access_monotonic: float
    initial_recording: bool
    managed_recording: bool
    baseline_transaction_count: int
    cursor_transaction_count: int
    latest_capture_id: str | None = None
    snapshot_capture_ids: list[str] = field(default_factory=list)
    is_reading: bool = False


class LiveAnalysisService:
    """Manage live snapshot sessions without relying on undocumented JSON export."""

    def __init__(
        self,
        *,
        control_service: CharlesControlService,
        ingest_service: IngestService,
        query_service: QueryService,
        temp_dir: Path,
        session_ttl_seconds: int = 900,
        snapshot_history_limit: int = 20,
        snapshot_max_bytes: int = 64 * 1024 * 1024,
        storage_max_bytes: int = 256 * 1024 * 1024,
        storage_max_age_seconds: int = 86400,
    ) -> None:
        self.control_service = control_service
        self.ingest_service = ingest_service
        self.query_service = query_service
        self.temp_dir = temp_dir
        self.temp_dir.mkdir(parents=True, exist_ok=True)
        self.session_ttl_seconds = max(session_ttl_seconds, 1)
        self.snapshot_history_limit = max(snapshot_history_limit, 1)
        self.snapshot_max_bytes = max(snapshot_max_bytes, 1)
        self.storage_max_bytes = max(storage_max_bytes, 1)
        self.storage_max_age_seconds = max(storage_max_age_seconds, 1)
        self._sessions: dict[str, LiveSessionState] = {}
        self._expiry_handles: dict[str, asyncio.TimerHandle] = {}
        self._release_lock = asyncio.Lock()
        self._enforce_snapshot_retention()

    async def start(
        self,
        *,
        reset_session: bool = False,
        start_recording_if_stopped: bool = True,
        snapshot_format: CaptureSourceFormat = CaptureSourceFormat.XML,
    ) -> dict:
        await self._prune_expired_sessions()
        if reset_session and self._sessions:
            raise ValueError("cannot reset Charles while another live analysis session is active")
        status = await self.control_service.get_recording_status()
        initial_recording = bool(status["is_recording"])
        managed_recording = False

        if reset_session:
            await self.control_service.clear_session()

        if start_recording_if_stopped and not initial_recording:
            await self.control_service.start_recording()
            managed_recording = True

        try:
            baseline_count = await self._current_transaction_count()
        except BaseException:
            if managed_recording:
                await self.control_service.stop_recording()
            raise
        live_session_id = f"live-{uuid4().hex}"
        self._sessions[live_session_id] = LiveSessionState(
            live_session_id=live_session_id,
            created_at=datetime.now().isoformat(),
            last_access_monotonic=time.monotonic(),
            initial_recording=initial_recording,
            managed_recording=managed_recording,
            baseline_transaction_count=baseline_count,
            cursor_transaction_count=baseline_count,
        )
        self._schedule_expiry(live_session_id)
        return {
            "live_session_id": live_session_id,
            "initial_recording": initial_recording,
            "managed_recording": managed_recording,
            "baseline_transaction_count": baseline_count,
            "reset_session": reset_session,
            "snapshot_format": snapshot_format.value,
        }

    async def read(
        self,
        *,
        live_session_id: str,
        snapshot_format: CaptureSourceFormat = CaptureSourceFormat.XML,
        host_contains: str | None = None,
        path_contains: str | None = None,
        method_in: list[str] | None = None,
        status_in: list[int] | None = None,
        limit: int = 20,
        advance: bool = True,
    ) -> dict:
        await self._prune_expired_sessions()
        if limit <= 0:
            raise ValueError("limit must be greater than 0")
        state = self._require(live_session_id)
        state.last_access_monotonic = time.monotonic()
        self._schedule_expiry(live_session_id)
        state.is_reading = True
        try:
            imported = await self._snapshot_current_session(snapshot_format=snapshot_format)
        finally:
            state.is_reading = False
            state.last_access_monotonic = time.monotonic()
            if live_session_id in self._sessions:
                self._schedule_expiry(live_session_id)
        previous_capture_id = state.latest_capture_id
        state.latest_capture_id = imported["capture_id"]
        state.snapshot_capture_ids.append(imported["capture_id"])
        if len(state.snapshot_capture_ids) > self.snapshot_history_limit:
            state.snapshot_capture_ids = state.snapshot_capture_ids[-self.snapshot_history_limit :]
        try:
            self._enforce_snapshot_retention()
        except ValueError:
            self.query_service.store.delete_live_capture(imported["capture_id"])
            state.latest_capture_id = previous_capture_id
            state.snapshot_capture_ids.remove(imported["capture_id"])
            raise

        current_transaction_count = self.query_service.store.count_entries(
            capture_id=imported["capture_id"],
            exclude_host="control.charles",
        )
        all_entries = self.query_service.store.list_entries(
            capture_id=imported["capture_id"],
            limit=max(imported["entry_count"], current_transaction_count, 1),
            offset=0,
        )
        non_control_entries = [entry for entry in all_entries if entry.host != "control.charles"]
        start_cursor = (
            0
            if current_transaction_count < state.cursor_transaction_count
            else state.cursor_transaction_count
        )
        unseen_entries = non_control_entries[start_cursor:]
        selected_entries = []
        consumed_count = 0
        for entry in unseen_entries:
            consumed_count += 1
            if _entry_matches(
                entry,
                host_contains=host_contains,
                path_contains=path_contains,
                method_in=method_in,
                status_in=status_in,
            ):
                selected_entries.append(entry)
                if len(selected_entries) == limit:
                    break
        new_transaction_count = len(unseen_entries)
        if advance:
            state.cursor_transaction_count = start_cursor + consumed_count

        return {
            "live_session_id": live_session_id,
            "capture_id": imported["capture_id"],
            "snapshot_format": snapshot_format.value,
            "current_transaction_count": current_transaction_count,
            "new_transaction_count": new_transaction_count,
            "cursor_transaction_count": state.cursor_transaction_count,
            "items": [
                self.query_service.entry_summary(entry)
                for entry in selected_entries
            ],
            "returned": len(selected_entries),
        }

    async def stop(self, *, live_session_id: str, restore_recording: bool = True) -> dict:
        await self._prune_expired_sessions()
        state = self._require(live_session_id)
        restored = await self._release_session(state, restore_recording=restore_recording)
        return {
            "live_session_id": live_session_id,
            "restored_recording": restored,
            "initial_recording": state.initial_recording,
            "managed_recording": state.managed_recording,
            "latest_capture_id": state.latest_capture_id,
            "snapshot_capture_ids": state.snapshot_capture_ids,
        }

    async def status(self, live_session_id: str | None = None) -> dict:
        await self._prune_expired_sessions()
        charles_status = await self.control_service.get_recording_status()
        if live_session_id is None:
            return {
                "charles_recording": charles_status,
                "active_live_sessions": list(self._sessions.keys()),
            }
        state = self._require(live_session_id)
        return {
            "charles_recording": charles_status,
            "live_session": {
                "live_session_id": state.live_session_id,
                "created_at": state.created_at,
                "initial_recording": state.initial_recording,
                "managed_recording": state.managed_recording,
                "baseline_transaction_count": state.baseline_transaction_count,
                "cursor_transaction_count": state.cursor_transaction_count,
                "latest_capture_id": state.latest_capture_id,
                "snapshot_capture_ids": state.snapshot_capture_ids,
            },
        }

    def _require(self, live_session_id: str) -> LiveSessionState:
        if live_session_id not in self._sessions:
            raise ValueError(f"live_session_id `{live_session_id}` was not found")
        return self._sessions[live_session_id]

    async def _prune_expired_sessions(self) -> None:
        now = time.monotonic()
        expired_ids = [
            live_session_id
            for live_session_id, state in self._sessions.items()
            if not state.is_reading
            and now - state.last_access_monotonic > self.session_ttl_seconds
        ]
        for live_session_id in expired_ids:
            state = self._sessions.get(live_session_id)
            if state is not None:
                await self._release_session(state, restore_recording=True)

    async def _release_session(self, state: LiveSessionState, *, restore_recording: bool) -> bool:
        async with self._release_lock:
            if state.live_session_id not in self._sessions:
                return False
            others = [
                item for item in self._sessions.values()
                if item.live_session_id != state.live_session_id
            ]
            restored = False
            if restore_recording and state.managed_recording:
                if others:
                    others[0].managed_recording = True
                    others[0].initial_recording = False
                else:
                    await self.control_service.stop_recording()
                    restored = True
            del self._sessions[state.live_session_id]
            handle = self._expiry_handles.pop(state.live_session_id, None)
            if handle is not None:
                handle.cancel()
            return restored

    def _schedule_expiry(self, live_session_id: str, delay: float | None = None) -> None:
        old = self._expiry_handles.pop(live_session_id, None)
        if old is not None:
            old.cancel()
        loop = asyncio.get_running_loop()
        self._expiry_handles[live_session_id] = loop.call_later(
            self.session_ttl_seconds if delay is None else delay,
            lambda: loop.create_task(self._expire_if_idle(live_session_id)),
        )

    async def _expire_if_idle(self, live_session_id: str) -> None:
        state = self._sessions.get(live_session_id)
        if state is None:
            return
        if state.is_reading:
            self._schedule_expiry(live_session_id)
            return
        remaining = self.session_ttl_seconds - (time.monotonic() - state.last_access_monotonic)
        if remaining > 0:
            self._schedule_expiry(live_session_id, remaining)
            return
        try:
            await self._release_session(state, restore_recording=True)
        except Exception:
            logger.exception("Failed to restore managed Charles recording for expired session")
            if live_session_id in self._sessions:
                self._schedule_expiry(live_session_id)

    async def close_all(self) -> None:
        """Restore managed recording on server shutdown, even without an explicit stop call."""
        for state in list(self._sessions.values()):
            await self._release_session(state, restore_recording=True)

    def _enforce_snapshot_retention(self) -> None:
        protected = {state.latest_capture_id for state in self._sessions.values()}
        for capture_id in self.query_service.store.expired_live_capture_ids(
            max_age_seconds=self.storage_max_age_seconds
        ):
            if capture_id not in protected:
                self.query_service.store.delete_live_capture(capture_id)
        capture_ids, used_bytes = self.query_service.store.live_capture_usage()
        while capture_ids and (
            len(capture_ids) > self.snapshot_history_limit or used_bytes > self.storage_max_bytes
        ):
            expired_id = next((item for item in capture_ids if item not in protected), None)
            if expired_id is None:
                raise ValueError("active live snapshots exceed CHARLES_REVERSE_LIVE_STORAGE_MAX_BYTES")
            self.query_service.store.delete_live_capture(expired_id)
            for state in self._sessions.values():
                state.snapshot_capture_ids = [
                    item for item in state.snapshot_capture_ids if item != expired_id
                ]
            capture_ids, used_bytes = self.query_service.store.live_capture_usage()

    async def _current_transaction_count(self) -> int:
        xml = await self.control_service.export_session_xml()
        return _count_non_control_transactions(xml)

    async def _snapshot_current_session(self, *, snapshot_format: CaptureSourceFormat) -> dict:
        if snapshot_format == CaptureSourceFormat.XML:
            path = self.temp_dir / f"{uuid4().hex}.xml"
            payload = (await self.control_service.export_session_xml()).encode("utf-8")
        elif snapshot_format == CaptureSourceFormat.NATIVE:
            path = self.temp_dir / f"{uuid4().hex}.chls"
            payload = await self.control_service.download_session_native()
        else:
            raise ValueError("live snapshot supports only xml or native")
        if len(payload) > self.snapshot_max_bytes:
            raise ValueError("live snapshot exceeds CHARLES_REVERSE_LIVE_SNAPSHOT_MAX_BYTES")
        try:
            path.write_bytes(payload)
            return self.ingest_service.import_session(
                path=str(path),
                source_format=snapshot_format,
                source_kind=CaptureSourceKind.LIVE_SNAPSHOT,
            )
        finally:
            path.unlink(missing_ok=True)


def _count_non_control_transactions(xml: str) -> int:
    root = ET.fromstring(xml)
    count = 0
    for txn in root.findall("transaction"):
        if (txn.attrib.get("host") or "") == "control.charles":
            continue
        count += 1
    return count


def _entry_matches(
    entry,
    *,
    host_contains: str | None,
    path_contains: str | None,
    method_in: list[str] | None,
    status_in: list[int] | None,
) -> bool:
    if host_contains and host_contains.lower() not in (entry.host or "").lower():
        return False
    if path_contains and path_contains.lower() not in (entry.path or "").lower():
        return False
    if method_in and (entry.method or "").upper() not in {item.upper() for item in method_in}:
        return False
    if status_in and entry.status_code not in set(status_in):
        return False
    return True

