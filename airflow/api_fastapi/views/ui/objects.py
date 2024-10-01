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
from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

from fastapi import Depends
from flask import (
    abort,
)
from pendulum.parsing.exceptions import ParserError
from sqlalchemy import func, select
from sqlalchemy.orm import Session
from typing_extensions import Annotated

from airflow.models.dagrun import DagRun, DagRunType
from airflow.models.taskinstance import TaskInstance
from airflow.utils.state import DagRunState, TaskInstanceState

if TYPE_CHECKING:
    from sqlalchemy.orm import Session
from airflow.api_fastapi.db.common import get_session
from airflow.api_fastapi.views.router import AirflowRouter
from airflow.utils import timezone

object_router = AirflowRouter(tags=["Object"])


@object_router.get("/object/historical_metrics_data", include_in_schema=False)
async def historical_metrics_data(
    start_date: str,
    end_date: str,
    session: Annotated[Session, Depends(get_session)],
):
    """Return cluster activity historical metrics."""
    safe_start_date: datetime | None = _safe_parse_datetime(start_date)
    safe_end_date: datetime | None = _safe_parse_datetime(end_date)

    # DagRuns
    dag_run_types = session.execute(
        select(DagRun.run_type, func.count(DagRun.run_id))
        .where(
            DagRun.start_date >= safe_start_date,
            func.coalesce(DagRun.end_date, timezone.utcnow()) <= safe_end_date,
        )
        .group_by(DagRun.run_type)
    ).all()

    dag_run_states = session.execute(
        select(DagRun.state, func.count(DagRun.run_id))
        .where(
            DagRun.start_date >= safe_start_date,
            func.coalesce(DagRun.end_date, timezone.utcnow()) <= safe_end_date,
        )
        .group_by(DagRun.state)
    ).all()

    # TaskInstances
    task_instance_states = session.execute(
        select(TaskInstance.state, func.count(TaskInstance.run_id))
        .join(TaskInstance.dag_run)
        .where(
            DagRun.start_date >= safe_start_date,
            func.coalesce(DagRun.end_date, timezone.utcnow()) <= safe_end_date,
        )
        .group_by(TaskInstance.state)
    ).all()

    data = {
        "dag_run_types": {
            **{dag_run_type.value: 0 for dag_run_type in DagRunType},
            **dict(dag_run_types),
        },
        "dag_run_states": {
            **{dag_run_state.value: 0 for dag_run_state in DagRunState},
            **dict(dag_run_states),
        },
        "task_instance_states": {
            "no_status": 0,
            **{ti_state.value: 0 for ti_state in TaskInstanceState},
            **{ti_state or "no_status": sum_value for ti_state, sum_value in task_instance_states},
        },
    }

    return data


def _safe_parse_datetime(date_to_check, allow_empty=False, strict=True) -> datetime | None:
    """
    Parse datetime and return error message for invalid dates.

    :param date_to_check: the string value to be parsed
    :param allow_empty: Set True to return none if empty str or None
    :param strict: if False, it will fall back on the dateutil parser if unable to parse with pendulum
    """
    if allow_empty is True and not date_to_check:
        return None
    try:
        return timezone.parse(date_to_check, strict=strict)
    except (TypeError, ParserError):
        abort(400, f"Invalid datetime: {date_to_check!r}")
