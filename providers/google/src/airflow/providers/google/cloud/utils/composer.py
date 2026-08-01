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
"""Shared helpers for Cloud Composer sensors and triggers."""

from __future__ import annotations

from collections.abc import Iterable
from datetime import datetime, timezone
from typing import Any

from dateutil import parser


def composer_dag_run_date_field(composer_airflow_version: int) -> str:
    """Return the Dag-run date field name for the Composer Airflow major version."""
    return "execution_date" if composer_airflow_version < 3 else "logical_date"


def normalize_to_utc(value: datetime) -> datetime:
    """Normalize a datetime to UTC; naive values are treated as UTC."""
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def parse_composer_airflow_datetime(value: Any) -> datetime | None:
    """
    Parse a datetime from Composer Airflow REST API payloads.

    Returns ``None`` when the value is missing/null so callers can skip those
    runs (Airflow 3 can emit null ``logical_date`` for some run types).

    Naive datetimes are treated as UTC so comparisons against window bounds do
    not silently depend on the worker's local timezone.
    """
    if value is None or value == "":
        return None
    parsed = value if isinstance(value, datetime) else parser.parse(value)
    return normalize_to_utc(parsed)


def is_in_execution_window(
    run_date: datetime,
    start_date: datetime,
    end_date: datetime,
) -> bool:
    """
    Return whether ``run_date`` falls in the half-open window ``[start_date, end_date)``.

    Start is inclusive so schedule-aligned runs at ``start_date`` are detected.
    End remains exclusive.
    """
    run_ts = normalize_to_utc(run_date).timestamp()
    start_ts = normalize_to_utc(start_date).timestamp()
    end_ts = normalize_to_utc(end_date).timestamp()
    return start_ts <= run_ts < end_ts


def check_dag_run_states_in_window(
    dag_runs: list[dict],
    *,
    start_date: datetime,
    end_date: datetime,
    allowed_states: Iterable[str] | None,
    composer_airflow_version: int,
) -> bool:
    """
    Return True when at least one in-window Dag run is allowed.

    Success requires at least one Dag run in the window and every in-window run
    in an allowed state.

    - Runs with a missing/null date field are skipped.
    - An empty window yields False (keep waiting).
    - ``allowed_states`` of ``None`` or empty defaults to ``["success"]`` so an
      empty list does not treat every in-window run as a failure.
    """
    allowed = list(allowed_states) if allowed_states else ["success"]
    date_field = composer_dag_run_date_field(composer_airflow_version)
    found_runs_in_window = False
    for dag_run in dag_runs:
        run_dt = parse_composer_airflow_datetime(dag_run.get(date_field))
        if run_dt is None:
            continue
        if is_in_execution_window(run_dt, start_date, end_date):
            found_runs_in_window = True
            if dag_run.get("state") not in allowed:
                return False
    return found_runs_in_window
