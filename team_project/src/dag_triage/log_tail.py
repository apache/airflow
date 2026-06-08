"""Fetch the last N lines from an Airflow task-instance log."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING

from dag_triage.config import DEFAULT_LOG_TAIL_LINES, get_log_tail_lines

if TYPE_CHECKING:
    from airflow.models.taskinstance import TaskInstance
    from airflow.models.taskinstancehistory import TaskInstanceHistory
    from airflow.utils.log.log_reader import TaskLogReader

log = logging.getLogger(__name__)

_GROUP_PREFIX = "::"


@dataclass(frozen=True)
class LogTailResult:
    """Outcome of a log-tail fetch."""

    text: str
    line_count: int
    requested_lines: int
    dag_id: str
    task_id: str
    run_id: str
    map_index: int
    try_number: int
    state: str | None = None
    error: str | None = None

    @property
    def ok(self) -> bool:
        return self.error is None


def _is_group_marker(event: str) -> bool:
    return event.startswith(_GROUP_PREFIX)


def _extract_event_text(ndjson_line: str) -> str | None:
    line = ndjson_line.strip()
    if not line:
        return None
    try:
        payload = json.loads(line)
    except json.JSONDecodeError:
        return line
    event = payload.get("event")
    if not isinstance(event, str):
        return None
    if _is_group_marker(event):
        return None
    return event


def _collect_log_lines(
    log_reader: TaskLogReader,
    task_instance: TaskInstance | TaskInstanceHistory,
    try_number: int,
) -> tuple[list[str], str | None]:
    if not log_reader.supports_read:
        return [], "Task log handler does not support read operations."

    lines: list[str] = []
    try:
        for ndjson_line in log_reader.read_log_stream(task_instance, try_number, {}):
            event_text = _extract_event_text(ndjson_line)
            if event_text is not None:
                lines.append(event_text)
    except Exception as exc:
        log.exception(
            "Failed while reading logs for %s.%s run=%s try=%s",
            task_instance.dag_id,
            task_instance.task_id,
            task_instance.run_id,
            try_number,
        )
        message = f"Failed to read task logs: {exc}"
        if lines:
            return lines, message
        return [], message

    return lines, None


def _resolve_try_number(task_instance: TaskInstance | TaskInstanceHistory) -> int:
    return task_instance.try_number


def _tail_lines(lines: list[str], tail_lines: int) -> list[str]:
    if tail_lines <= 0:
        return []
    if len(lines) <= tail_lines:
        return lines
    return lines[-tail_lines:]


def _build_result(
    task_instance: TaskInstance | TaskInstanceHistory,
    tailed: list[str],
    requested_lines: int,
    error: str | None,
) -> LogTailResult:
    state = getattr(task_instance, "state", None)
    state_value = state.value if state is not None and hasattr(state, "value") else state
    if isinstance(state_value, str):
        state_str: str | None = state_value
    elif state_value is None:
        state_str = None
    else:
        state_str = str(state_value)

    return LogTailResult(
        text="\n".join(tailed),
        line_count=len(tailed),
        requested_lines=requested_lines,
        dag_id=task_instance.dag_id,
        task_id=task_instance.task_id,
        run_id=task_instance.run_id,
        map_index=task_instance.map_index,
        try_number=_resolve_try_number(task_instance),
        state=state_str,
        error=error,
    )


def fetch_log_tail(
    task_instance: TaskInstance | TaskInstanceHistory,
    *,
    tail_lines: int | None = None,
    try_number: int | None = None,
    log_reader: TaskLogReader | None = None,
) -> LogTailResult:
    """Read the last *tail_lines* log lines for a task instance.

    Uses Airflow's ``TaskLogReader``, so this works with any configured task log
    handler (local files, S3, GCS, Elasticsearch, etc.) regardless of executor.

    Failures degrade gracefully: returns a ``LogTailResult`` with ``error`` set
    instead of raising.
    """
    requested = tail_lines if tail_lines is not None else get_log_tail_lines()
    resolved_try = try_number if try_number is not None else _resolve_try_number(task_instance)

    if log_reader is None:
        from airflow.utils.log.log_reader import TaskLogReader

        log_reader = TaskLogReader()

    all_lines, read_error = _collect_log_lines(log_reader, task_instance, resolved_try)
    tailed = _tail_lines(all_lines, requested)
    return _build_result(task_instance, tailed, requested, read_error)


def empty_log_tail_result(
    task_instance: TaskInstance | TaskInstanceHistory,
    *,
    error: str,
    requested_lines: int | None = None,
) -> LogTailResult:
    """Build an empty result when log capture fails before a read attempt."""
    requested = requested_lines if requested_lines is not None else DEFAULT_LOG_TAIL_LINES
    return _build_result(task_instance, [], requested, error)
