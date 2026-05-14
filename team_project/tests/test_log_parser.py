"""Tests for the Airflow log normalization layer."""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from dag_triage.log_parser import LogRecord, parse_log


# ---------------------------------------------------------------------------
# Case 1: single INFO line
# ---------------------------------------------------------------------------


def test_single_info_line() -> None:
    raw = "[2024-01-15T10:30:45.123+0000] {dag.py:42} INFO - Starting task"
    records = parse_log(raw)

    assert len(records) == 1
    r = records[0]
    assert r.timestamp == "2024-01-15T10:30:45.123+0000"
    assert r.level == "INFO"
    assert r.source == "dag.py:42"
    assert r.message == "Starting task"
    assert r.traceback is None


# ---------------------------------------------------------------------------
# Case 2: multi-line Python traceback attached to the ERROR record
# ---------------------------------------------------------------------------


def test_error_with_multiline_traceback() -> None:
    raw = (
        "[2024-01-15T10:30:47.500+0000] {task_runner.py:88} ERROR - Task execution failed\n"
        "Traceback (most recent call last):\n"
        '  File "task_runner.py", line 88, in execute\n'
        "    result = self.hook.run()\n"
        "AttributeError: 'NoneType' object has no attribute 'run'"
    )
    records = parse_log(raw)

    assert len(records) == 1
    r = records[0]
    assert r.level == "ERROR"
    assert r.message == "Task execution failed"
    assert r.traceback is not None
    assert "Traceback (most recent call last):" in r.traceback
    assert "AttributeError" in r.traceback


# ---------------------------------------------------------------------------
# Case 3: mixed log — INFO, WARNING, ERROR all parsed correctly
# ---------------------------------------------------------------------------


def test_mixed_log_levels() -> None:
    raw = (
        "[2024-01-15T10:30:45.000+0000] {dag.py:1} INFO - Starting pipeline\n"
        "[2024-01-15T10:30:45.800+0000] {dag.py:55} WARNING - Slow query detected\n"
        "[2024-01-15T10:30:46.200+0000] {dag.py:99} ERROR - Connection refused"
    )
    records = parse_log(raw)

    assert len(records) == 3
    assert records[0].level == "INFO"
    assert records[0].message == "Starting pipeline"
    assert records[1].level == "WARNING"
    assert records[1].message == "Slow query detected"
    assert records[2].level == "ERROR"
    assert records[2].message == "Connection refused"
    assert records[0].traceback is None
    assert records[1].traceback is None


# ---------------------------------------------------------------------------
# Case 4: malformed line between valid records is silently skipped
# ---------------------------------------------------------------------------


def test_malformed_line_silently_skipped() -> None:
    raw = (
        "[2024-01-15T10:30:45.000+0000] {dag.py:1} INFO - First good line\n"
        "!!! this line has no timestamp and is not after an ERROR !!!\n"
        "[2024-01-15T10:30:46.000+0000] {dag.py:2} INFO - Second good line"
    )
    records = parse_log(raw)

    assert len(records) == 2
    assert records[0].message == "First good line"
    assert records[1].message == "Second good line"
    assert records[0].traceback is None
