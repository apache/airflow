"""Airflow task-instance log normalization layer."""

from __future__ import annotations

import re
from dataclasses import dataclass

# Matches: [YYYY-MM-DDTHH:MM:SS.fff+0000] {filename:line} LEVEL - message
_HEADER = re.compile(
    r"^\[(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+[+-]\d{4})\]"
    r"\s+\{([^}]+)\}"
    r"\s+([A-Z]+)"
    r"\s+-\s+(.*)"
)


@dataclass
class LogRecord:
    timestamp: str
    level: str
    source: str
    message: str
    traceback: str | None = None


def parse_log(raw_log: str) -> list[LogRecord]:
    """Parse an Airflow task-instance log string into a list of LogRecords.

    Lines matching the standard Airflow header format become new records.
    Non-header lines that immediately follow an ERROR record are collected
    into that record's ``traceback`` field.  All other non-header lines
    (malformed lines, or continuations after a non-ERROR record) are
    silently skipped.
    """
    records: list[LogRecord] = []
    tb_lines: list[str] = []

    def _flush_traceback() -> None:
        if tb_lines and records:
            records[-1].traceback = "\n".join(tb_lines)
        tb_lines.clear()

    for line in raw_log.splitlines():
        m = _HEADER.match(line)
        if m:
            _flush_traceback()
            timestamp, source, level, message = m.groups()
            records.append(
                LogRecord(timestamp=timestamp, level=level, source=source, message=message)
            )
        elif records and records[-1].level == "ERROR":
            tb_lines.append(line)
        # else: malformed or non-traceback continuation — skip silently

    _flush_traceback()
    return records
