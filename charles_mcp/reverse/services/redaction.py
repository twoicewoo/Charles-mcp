"""Output-only masking for captured HTTP traffic."""

from __future__ import annotations

from typing import Any
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

REDACTED = "[REDACTED]"


def redact_query(query: str | None) -> str | None:
    if not query:
        return query
    return urlencode([(key, REDACTED) for key, _ in parse_qsl(query, keep_blank_values=True)])


def redact_url(url: str) -> str:
    parts = urlsplit(url)
    hostname = parts.hostname or ""
    if ":" in hostname:
        hostname = f"[{hostname}]"
    netloc = hostname if parts.port is None else f"{hostname}:{parts.port}"
    return urlunsplit((parts.scheme, netloc, parts.path, redact_query(parts.query) or "", ""))


def redact_headers(headers: dict[str, Any]) -> dict[str, Any]:
    return {name: REDACTED for name in headers}


def redact_entry_detail(detail: dict[str, Any]) -> dict[str, Any]:
    """Keep routing/shape while hiding all captured values and body content."""
    result = dict(detail)
    entry = result.get("entry")
    if entry:
        result["entry"] = {**entry, "query": redact_query(entry.get("query")), "metadata": {}}
    for side in ("request", "response"):
        message = result.get(side)
        if message:
            result[side] = {
                **message,
                "headers": redact_headers(message.get("headers", {})),
                "cookies": REDACTED,
                "set_cookies": REDACTED,
                "metadata": {},
                "first_line": REDACTED if message.get("first_line") else None,
                "redirect_location": redact_url(message["redirect_location"])
                if message.get("redirect_location") else None,
            }
        blob_key = f"{side}_body_blob"
        blob = result.get(blob_key)
        if blob:
            result[blob_key] = {
                **blob,
                "raw_bytes": REDACTED if blob.get("raw_bytes") is not None else None,
                "raw_text": REDACTED if blob.get("raw_text") is not None else None,
                "external_ref": None,
                "metadata": {},
            }
    result["decoded_artifacts"] = [
        {**artifact, "preview_text": REDACTED, "structured_json": None, "metadata": {}}
        for artifact in result.get("decoded_artifacts", [])
    ]
    return result
