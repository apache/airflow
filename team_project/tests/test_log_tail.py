"""Tests for task log tail extraction."""

from __future__ import annotations

from unittest import mock

import pytest

from dag_triage.log_tail import (
    LogTailResult,
    _extract_event_text,
    _tail_lines,
    empty_log_tail_result,
    fetch_log_tail,
)
from helpers import MockTaskLogReader


@pytest.mark.parametrize(
    ("ndjson_line", "expected"),
    [
        ('{"timestamp": null, "event": "hello world"}\n', "hello world"),
        ('{"timestamp": null, "event": "::group::details"}\n', None),
        ("plain fallback line\n", "plain fallback line"),
        ("", None),
        ('{"timestamp": null, "event": 42}\n', None),
    ],
)
def test_extract_event_text(ndjson_line: str, expected: str | None) -> None:
    assert _extract_event_text(ndjson_line) == expected


@pytest.mark.parametrize(
    ("lines", "n", "expected"),
    [
        (["a", "b", "c", "d", "e"], 3, ["c", "d", "e"]),
        (["only"], 200, ["only"]),
        ([], 10, []),
        (["a", "b"], 0, []),
    ],
)
def test_tail_lines(lines: list[str], n: int, expected: list[str]) -> None:
    assert _tail_lines(lines, n) == expected


def test_fetch_log_tail_returns_last_n_lines(mock_ti: mock.MagicMock) -> None:
    events = [f"line-{i}" for i in range(1, 11)]
    reader = MockTaskLogReader(events)

    result = fetch_log_tail(mock_ti, tail_lines=3, log_reader=reader)

    assert result.ok
    assert result.text == "line-8\nline-9\nline-10"
    assert result.line_count == 3
    assert result.requested_lines == 3
    assert result.dag_id == "sample_dag"
    assert result.task_id == "fail_task"
    assert result.try_number == 1
    assert result.state == "failed"
    assert reader.calls == [(mock_ti, 1, {})]


def test_fetch_log_tail_skips_group_markers(mock_ti: mock.MagicMock) -> None:
    reader = MockTaskLogReader(
        [
            "::group::Log message source details",
            "/tmp/dag/task/1.log",
            "::endgroup::",
            "actual log line",
        ]
    )

    result = fetch_log_tail(mock_ti, tail_lines=10, log_reader=reader)

    assert result.ok
    assert result.text == "/tmp/dag/task/1.log\nactual log line"
    assert result.line_count == 2


def test_fetch_log_tail_unsupported_handler_returns_error(mock_ti: mock.MagicMock) -> None:
    reader = MockTaskLogReader(supports_read=False)

    result = fetch_log_tail(mock_ti, log_reader=reader)

    assert not result.ok
    assert result.text == ""
    assert result.line_count == 0
    assert "does not support read" in result.error


def test_fetch_log_tail_read_exception_returns_error(mock_ti: mock.MagicMock) -> None:
    reader = MockTaskLogReader(read_error=RuntimeError("connection refused"))

    result = fetch_log_tail(mock_ti, log_reader=reader)

    assert not result.ok
    assert result.text == ""
    assert "connection refused" in result.error


def test_fetch_log_tail_partial_read_on_exception(mock_ti: mock.MagicMock) -> None:
    class FailingReader(MockTaskLogReader):
        def read_log_stream(self, task_instance: object, try_number: int, metadata: dict):
            self.calls.append((task_instance, try_number, metadata))
            yield '{"timestamp": null, "event": "line-1"}\n'
            yield '{"timestamp": null, "event": "line-2"}\n'
            raise RuntimeError("stream interrupted")

    reader = FailingReader()

    result = fetch_log_tail(mock_ti, tail_lines=10, log_reader=reader)

    assert not result.ok
    assert result.text == "line-1\nline-2"
    assert result.line_count == 2
    assert "stream interrupted" in result.error


def test_fetch_log_tail_respects_custom_try_number(mock_ti: mock.MagicMock) -> None:
    reader = MockTaskLogReader(["try-specific line"])

    fetch_log_tail(mock_ti, try_number=2, log_reader=reader)

    assert reader.calls[0][1] == 2


def test_empty_log_tail_result(mock_ti: mock.MagicMock) -> None:
    result = empty_log_tail_result(mock_ti, error="boom", requested_lines=50)

    assert isinstance(result, LogTailResult)
    assert not result.ok
    assert result.text == ""
    assert result.requested_lines == 50
    assert result.error == "boom"


def test_integration_failed_task_log_tail(mock_ti: mock.MagicMock) -> None:
    """End-to-end tail read against a realistic failed-task log stream."""
    events = [
        "::group::Log message source details",
        "/var/log/airflow/sample_dag/fail_task/2024-01-15T00.00.00+00.00/1.log",
        "::endgroup::",
        "[2024-01-15T10:30:45.000+0000] {task_runner.py:1} INFO - Starting task",
        "[2024-01-15T10:30:46.000+0000] {task_runner.py:88} ERROR - Task execution failed",
        "Traceback (most recent call last):",
        '  File "task_runner.py", line 88, in execute',
        "    result = self.hook.run()",
        "AttributeError: 'NoneType' object has no attribute 'run'",
    ]
    reader = MockTaskLogReader(events)

    result = fetch_log_tail(mock_ti, tail_lines=4, log_reader=reader)

    assert result.ok
    assert result.line_count == 4
    assert "Traceback (most recent call last):" in result.text
    assert "AttributeError" in result.text
    assert "Starting task" not in result.text
