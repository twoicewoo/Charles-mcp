"""Default redacted views for normalized Charles traffic."""

from __future__ import annotations

from charles_mcp.schemas.traffic import TrafficEntry, TrafficSummary

MASK = "[REDACTED]"


def redact_entry(entry: TrafficEntry) -> TrafficEntry:
    def message_view(message):
        body = message.body.model_copy(
            update={
                "preview_text": MASK if message.body.preview_text is not None else None,
                "full_text": MASK if message.body.full_text is not None else None,
                "parsed_json": None,
                "parsed_form": None,
                "multipart_summary": [],
            }
        )
        return message.model_copy(
            update={
                "first_line": MASK if message.first_line else None,
                "headers": [header.model_copy(update={"value": MASK}) for header in message.headers],
                "header_map": {},
                "body": body,
            }
        )

    return entry.model_copy(
        update={
            "query": MASK if entry.query else None,
            "error_message": None,
            "notes": None,
            "request": message_view(entry.request),
            "response": message_view(entry.response),
        }
    )


def redact_summary(summary: TrafficSummary) -> TrafficSummary:
    return summary.model_copy(
        update={
            "request_header_highlights": {
                name: MASK for name in summary.request_header_highlights
            },
            "response_header_highlights": {
                name: MASK for name in summary.response_header_highlights
            },
            "request_body_preview": MASK if summary.request_body_preview is not None else None,
            "response_body_preview": MASK if summary.response_body_preview is not None else None,
            "error_message": None,
        }
    )
