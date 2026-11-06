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
Print the OpenLineage validation errors of Airflow task logs in pytest output on failure.

Task logs are structlog JSON-lines written by the deployed Airflow stack; they land on the host via
the ``./logs:/opt/airflow/logs`` bind mount in ``docker-compose.yaml``. Full logs are too long for the CI
output, so only the lines tagged with ``VALIDATION_ERROR_TAG`` are printed: ``OpenLineageTestOperator``
tags every reason its event validation failed, as do the other helpers of the system test Dags. The full
logs stay in the CI log artifact.
"""

from __future__ import annotations

import json
import re
from collections.abc import Iterable
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from rich.console import Console

# Prefix of the log lines describing OpenLineage validation errors, keep in sync with VALIDATION_ERROR_TAG in
# providers/openlineage/tests/system/openlineage/operator.py
VALIDATION_ERROR_TAG = "[OL_VALIDATION_ERROR]"
MAX_ERROR_LINES = 100

_ATTEMPT_NUMBER_PATTERN = re.compile(r"attempt=(\d+)\.log$")


def extract_validation_errors(raw_lines: Iterable[str], max_lines: int = MAX_ERROR_LINES) -> list[str]:
    """Parse structlog JSON lines and return the tagged validation errors, without the tag."""
    errors = []
    for raw_line in raw_lines:
        stripped_line = raw_line.strip()
        if not stripped_line:
            continue
        try:
            record = json.loads(stripped_line)
        except json.JSONDecodeError:
            # Task logs aren't guaranteed to be 100% JSON (e.g. stray print output) — skip those lines.
            continue
        for line in str(record.get("event", "")).splitlines():
            if VALIDATION_ERROR_TAG in line:
                errors.append(line.split(VALIDATION_ERROR_TAG, 1)[1].strip())
    if len(errors) > max_lines:
        dropped = len(errors) - max_lines
        errors = errors[:max_lines] + [f"... ({dropped} more lines suppressed)"]
    return errors


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
    """Print the OpenLineage validation errors of a task's log(s), or a note if there are none."""
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
        errors = extract_validation_errors(log_path.read_text().splitlines())
        for line in errors or ["(no OpenLineage validation errors in this task's log)"]:
            console.print(line, markup=False, highlight=False)
    console.print(f"[bold cyan]--- {dag_id} / {task_id} / END---[/]")
