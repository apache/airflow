# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""
Read and format Airflow task logs for display in pytest output on failure.

Task logs are structlog JSON-lines written by the deployed Airflow stack; they land on the host via
the ``./logs:/opt/airflow/logs`` bind mount in ``docker-compose.yaml``. Rather than requiring someone
to unzip the CI log artifact and open a specific ``attempt=N.log`` file, the warning/error highlights
are printed straight into the failing test's captured output.
"""

from __future__ import annotations

import json
import re
from collections.abc import Iterable
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from rich.console import Console

LOG_LEVELS_OF_INTEREST = frozenset({"warning", "error", "critical"})
MAX_HIGHLIGHT_LINES = 100

_ATTEMPT_NUMBER_PATTERN = re.compile(r"attempt=(\d+)\.log$")


def format_log_line(record: dict) -> str:
    """Render one parsed structlog record as a compact, human-readable line."""
    level = str(record.get("level", "info")).upper()
    event = record.get("event", "")
    lines = [f"[{level}] {event}"]
    for exc in record.get("error_detail") or []:
        exc_type = exc.get("exc_type", "Exception")
        exc_value = exc.get("exc_value", "")
        lines.append(f"    {exc_type}: {exc_value}")
        frames = exc.get("frames") or []
        if frames:
            frame = frames[-1]
            lines.append(f"    at {Path(frame['filename']).name}:{frame['lineno']} in {frame['name']}")
    return "\n".join(lines)


def filter_log_highlights(raw_lines: Iterable[str], max_lines: int = MAX_HIGHLIGHT_LINES) -> list[str]:
    """Parse structlog JSON lines and keep only warning/error/critical ones, formatted for display."""
    highlights = []
    for raw_line in raw_lines:
        stripped_line = raw_line.strip()
        if not stripped_line:
            continue
        try:
            record = json.loads(stripped_line)
        except json.JSONDecodeError:
            # Task logs aren't guaranteed to be 100% JSON (e.g. stray print output) — skip those lines.
            continue
        if record.get("level") in LOG_LEVELS_OF_INTEREST:
            highlights.append(format_log_line(record))
    if not highlights:
        return ["(no warning/error lines in this task's log)"]
    if len(highlights) > max_lines:
        dropped = len(highlights) - max_lines
        highlights = highlights[:max_lines] + [f"... ({dropped} more lines suppressed)"]
    return highlights


def _attempt_number(path: Path) -> int:
    match = _ATTEMPT_NUMBER_PATTERN.search(path.name)
    return int(match.group(1)) if match else -1


def find_log_paths(logs_folder: Path, dag_id: str, run_id: str, task_id: str) -> list[Path]:
    """Latest-attempt log file(s) for a task — one per map index if the task is dynamically mapped."""
    task_root = logs_folder / f"dag_id={dag_id}" / f"run_id={run_id}" / f"task_id={task_id}"
    latest_by_dir: dict[Path, Path] = {}
    for attempt_path in task_root.glob("**/attempt=*.log"):
        current = latest_by_dir.get(attempt_path.parent)
        if current is None or _attempt_number(attempt_path) > _attempt_number(current):
            latest_by_dir[attempt_path.parent] = attempt_path
    return sorted(latest_by_dir.values())


def print_task_log(console: Console, logs_folder: Path, dag_id: str, run_id: str, task_id: str) -> None:
    """Print the warning/error highlights of a task's log(s), or a note if none are found."""
    console.print(f"[bold cyan]--- {dag_id} / {task_id} / START---[/]")
    log_paths = find_log_paths(logs_folder, dag_id, run_id, task_id)
    if not log_paths:
        console.print(
            f"[yellow](no log file found under dag_id={dag_id}/run_id={run_id}/task_id={task_id})[/]"
        )
        return
    for log_path in log_paths:
        if len(log_paths) > 1:
            console.print(f"[cyan]{log_path.relative_to(logs_folder)}[/]")
        for line in filter_log_highlights(log_path.read_text().splitlines()):
            console.print(line)
    console.print(f"[bold cyan]--- {dag_id} / {task_id} / END---[/]")
