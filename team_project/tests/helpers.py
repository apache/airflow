"""Test helpers for dag_triage."""

from __future__ import annotations

import json
from collections.abc import Iterator


def ndjson_lines(events: list[str]) -> Iterator[str]:
    for event in events:
        yield json.dumps({"timestamp": None, "event": event}) + "\n"


class MockTaskLogReader:
    """Minimal stand-in for Airflow's TaskLogReader."""

    supports_read = True

    def __init__(
        self,
        events: list[str] | None = None,
        *,
        supports_read: bool = True,
        read_error: Exception | None = None,
    ) -> None:
        self.events = events or []
        self.supports_read = supports_read
        self.read_error = read_error
        self.calls: list[tuple[object, int, dict]] = []

    def read_log_stream(self, task_instance: object, try_number: int, metadata: dict) -> Iterator[str]:
        self.calls.append((task_instance, try_number, metadata))
        if self.read_error is not None:
            raise self.read_error
        return ndjson_lines(self.events)
