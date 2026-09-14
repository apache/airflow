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

from collections.abc import Collection
from http import HTTPStatus
from typing import TYPE_CHECKING, Any, cast

from sqlalchemy import func, select, tuple_

from airflow.models import DagRun, TaskInstance
from airflow.models.serialized_dag import SerializedDagModel
from airflow.providers.standard.version_compat import AIRFLOW_V_3_0_PLUS
from airflow.utils.session import NEW_SESSION, provide_session

if TYPE_CHECKING:
    from datetime import datetime

    from sqlalchemy.orm import Session
    from sqlalchemy.sql import Select


@provide_session
def _get_count(
    dttm_filter,
    external_task_ids,
    external_task_group_id,
    external_dag_id,
    states,
    *,
    session: Session = NEW_SESSION,
) -> int:
    """
    Get the count of records against dttm filter and states.

    :param dttm_filter: date time filter for logical date
    :param external_task_ids: The list of task_ids
    :param external_task_group_id: The ID of the external task group
    :param external_dag_id: The ID of the external DAG.
    :param states: task or dag states
    :param session: airflow session object
    """
    TI = TaskInstance
    DR = DagRun
    if not dttm_filter:
        return 0

    if external_task_ids:
        count = (
            session.scalar(
                _count_stmt(TI, states, dttm_filter, external_dag_id).where(TI.task_id.in_(external_task_ids))
            )
            or 0
        ) / len(external_task_ids)
    elif external_task_group_id:
        external_task_group_task_ids = _get_external_task_group_task_ids(
            dttm_filter, external_task_group_id, external_dag_id, session
        )
        if not external_task_group_task_ids:
            count = 0
        else:
            count = (
                (
                    session.scalar(
                        _count_stmt(TI, states, dttm_filter, external_dag_id).where(
                            tuple_(TI.task_id, TI.map_index).in_(external_task_group_task_ids)
                        )
                    )
                    or 0
                )
                / len(external_task_group_task_ids)
                * len(dttm_filter)
            )
    else:
        count = session.scalar(_count_stmt(DR, states, dttm_filter, external_dag_id)) or 0
    return cast("int", count)


def _count_stmt(
    model: type[DagRun] | type[TaskInstance], states: list[str], dttm_filter: list[Any], external_dag_id: str
) -> Select[tuple[int]]:
    """
    Get the count of records against dttm filter and states.

    :param model: The SQLAlchemy model representing the relevant table.
    :param states: task or dag states
    :param dttm_filter: date time filter for logical date
    :param external_dag_id: The ID of the external DAG.
    """
    date_field = model.logical_date if AIRFLOW_V_3_0_PLUS else model.execution_date

    return select(func.count()).where(
        model.dag_id == external_dag_id, model.state.in_(states), date_field.in_(dttm_filter)
    )


def _get_external_task_group_task_ids(
    dttm_filter: list[Any], external_task_group_id: str, external_dag_id: str, session: Session
) -> list[tuple[str, int]]:
    """
    Get the count of records against dttm filter and states.

    :param dttm_filter: date time filter for logical date
    :param external_task_group_id: The ID of the external task group
    :param external_dag_id: The ID of the external DAG.
    :param session: airflow session object
    """
    refreshed_dag_info = SerializedDagModel.get_dag(external_dag_id, session=session)
    if not refreshed_dag_info:
        return [(external_task_group_id, -1)]
    task_group = refreshed_dag_info.task_group_dict.get(external_task_group_id)

    if task_group:
        date_field = TaskInstance.logical_date if AIRFLOW_V_3_0_PLUS else TaskInstance.execution_date

        group_tasks = session.scalars(
            select(TaskInstance).filter(
                TaskInstance.dag_id == external_dag_id,
                TaskInstance.task_id.in_(task.task_id for task in task_group),
                date_field.in_(dttm_filter),
            )
        )

        return [(t.task_id, t.map_index) for t in group_tasks]

    # returning default task_id as group_id itself, this will avoid any failure in case of
    # 'check_existence=False' and will fail on timeout
    return [(external_task_group_id, -1)]


def _get_count_by_matched_states(
    run_id_task_state_map: dict[str, dict[str, Any]],
    states: Collection[str],
):
    count = 0
    for _, task_states in run_id_task_state_map.items():
        # Create this list such that len() can be checked in the conditional (to handle empty inner)
        matched_states: list = [state in states for state in task_states.values()]

        # An empty inner, such as {"r": {}}, results in count NOT being incremented
        if len(matched_states) > 0 and all(matched_states):
            count += 1

    return count


def _not_found_message(error: Any) -> str | None:
    """Return the execution API's message when ``error`` wraps a 404 response, ``None`` for any other error."""
    detail = error.error.detail or {}
    if detail.get("status_code") != HTTPStatus.NOT_FOUND:
        return None
    # The supervisor forwards the server's JSON body under ``detail``; FastAPI nests an
    # ``HTTPException`` detail under a ``detail`` key of its own.
    payload = detail.get("detail")
    while isinstance(payload, dict) and "message" not in payload and isinstance(payload.get("detail"), dict):
        payload = payload["detail"]
    if isinstance(payload, dict) and isinstance(payload.get("message"), str):
        return payload["message"]
    return str(detail.get("message") or "not found")


def _check_external_task_existence(
    api: Any,
    *,
    external_dag_id: str,
    external_task_ids: Collection[str] | None,
    external_task_group_id: str | None,
    logical_dates: Collection[datetime] | None = None,
    run_ids: Collection[str] | None = None,
) -> bool:
    """
    Verify that the awaited tasks or task group exist in the awaited Dag runs, through the execution API.

    The execution API answers ``task-instances/states`` with 404 when the Dag version an existing
    run resolves to defines neither the requested tasks nor the requested task group and the run
    has no task instance for them (apache/airflow#73086). A normal answer means they exist for
    that run, even while their task instances have not been created yet, so nothing is inferred
    from task-instance counts. Nothing can be concluded about a run that does not exist yet,
    which is why the caller repeats the check until this function returns True. API servers
    without that validation answer normally for unknown tasks, in which case the sensor keeps
    waiting as it did before.

    :param api: an object exposing ``get_dr_count`` and ``get_task_states`` the way
        ``RuntimeTaskInstance`` does: the running task instance, or the class itself.
    :param external_dag_id: The ID of the external Dag.
    :param external_task_ids: The task IDs that must exist in every awaited run.
    :param external_task_group_id: The task group ID that must exist in every awaited run.
    :param logical_dates: Logical dates identifying the awaited runs, used when ``run_ids`` is empty.
    :param run_ids: Run IDs identifying the awaited runs.
    :return: True once every awaited run exists and passed the check, False while at least one
        awaited run does not exist yet.
    :raises ExternalTaskNotFoundError: when the API reports one of the tasks missing from an awaited run.
    :raises ExternalTaskGroupNotFoundError: when the API reports the task group missing from an awaited run.
    """
    from airflow.providers.standard.exceptions import (
        ExternalTaskGroupNotFoundError,
        ExternalTaskNotFoundError,
    )
    from airflow.sdk.exceptions import AirflowRuntimeError

    awaited_runs: list[tuple[str, dict[str, list[Any]]]]
    if run_ids:
        awaited_runs = [(run_id, {"run_ids": [run_id]}) for run_id in run_ids]
    else:
        awaited_runs = [(dt.isoformat(), {"logical_dates": [dt]}) for dt in logical_dates or []]

    all_runs_checked = True
    for run_label, run_filter in awaited_runs:
        if api.get_dr_count(dag_id=external_dag_id, **run_filter) == 0:
            all_runs_checked = False
            continue

        if external_task_ids:
            try:
                api.get_task_states(dag_id=external_dag_id, task_ids=list(external_task_ids), **run_filter)
            except AirflowRuntimeError as e:
                if (message := _not_found_message(e)) is None:
                    raise
                raise ExternalTaskNotFoundError(
                    f"The external tasks {list(external_task_ids)} in Dag {external_dag_id} "
                    f"do not all exist for run {run_label}: {message}"
                ) from None

        if external_task_group_id:
            try:
                api.get_task_states(
                    dag_id=external_dag_id, task_group_id=external_task_group_id, **run_filter
                )
            except AirflowRuntimeError as e:
                if (message := _not_found_message(e)) is None:
                    raise
                raise ExternalTaskGroupNotFoundError(
                    f"The external task group '{external_task_group_id}' in Dag '{external_dag_id}' "
                    f"does not exist for run {run_label}: {message}"
                ) from None

    return all_runs_checked
