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
from typing import TYPE_CHECKING, Any

import structlog


async def get_ti_count(
    dag_id: str,
    map_index: int | None = None,
    task_ids: list[str] | None = None,
    task_group_id: str | None = None,
    logical_dates: list[datetime] | None = None,
    run_ids: list[str] | None = None,
    states: list[str] | None = None,
) -> int:
    """Return the number of task instances matching the given criteria."""
    from airflow.sdk.execution_time.comms import GetTICount, TICount
    from airflow.sdk.execution_time.task_runner import SUPERVISOR_COMMS

    log = structlog.get_logger(logger_name="task")

    async with SUPERVISOR_COMMS.lock:
        SUPERVISOR_COMMS.send_request(
            log=log,
            msg=GetTICount(
                dag_id=dag_id,
                map_index=map_index,
                task_ids=task_ids,
                task_group_id=task_group_id,
                logical_dates=logical_dates,
                run_ids=run_ids,
                states=states,
            ),
        )
        response = SUPERVISOR_COMMS.get_message()

    if TYPE_CHECKING:
        assert isinstance(response, TICount)

    return response.count


async def get_dr_count(
    dag_id: str,
    logical_dates: list[datetime] | None = None,
    run_ids: list[str] | None = None,
    states: list[str] | None = None,
) -> int:
    """Return the number of DAG runs matching the given criteria."""
    from airflow.sdk.execution_time.comms import DRCount, GetDRCount
    from airflow.sdk.execution_time.task_runner import SUPERVISOR_COMMS

    log = structlog.get_logger(logger_name="task")

    async with SUPERVISOR_COMMS.lock:
        SUPERVISOR_COMMS.send_request(
            log=log,
            msg=GetDRCount(
                dag_id=dag_id,
                logical_dates=logical_dates,
                run_ids=run_ids,
                states=states,
            ),
        )
        response = SUPERVISOR_COMMS.get_message()

    if TYPE_CHECKING:
        assert isinstance(response, DRCount)

    return response.count


async def get_dagrun_state(dag_id: str, run_id: str) -> str:
    """Return the state of the DAG run with the given Run ID."""
    from airflow.sdk.execution_time.comms import DagRunStateResult, GetDagRunState
    from airflow.sdk.execution_time.task_runner import SUPERVISOR_COMMS

    log = structlog.get_logger(logger_name="task")
    async with SUPERVISOR_COMMS.lock:
        SUPERVISOR_COMMS.send_request(log=log, msg=GetDagRunState(dag_id=dag_id, run_id=run_id))
        response = SUPERVISOR_COMMS.get_message()

    if TYPE_CHECKING:
        assert isinstance(response, DagRunStateResult)

    return response.state


async def get_task_states(
    dag_id: str,
    map_index: int | None = None,
    task_ids: list[str] | None = None,
    task_group_id: str | None = None,
    logical_dates: list[datetime] | None = None,
    run_ids: list[str] | None = None,
) -> dict[str, Any]:
    """Return the task states matching the given criteria."""
    from airflow.sdk.execution_time.comms import GetTaskStates, TaskStatesResult
    from airflow.sdk.execution_time.task_runner import SUPERVISOR_COMMS

    log = structlog.get_logger(logger_name="task")

    async with SUPERVISOR_COMMS.lock:
        SUPERVISOR_COMMS.send_request(
            log=log,
            msg=GetTaskStates(
                dag_id=dag_id,
                map_index=map_index,
                task_ids=task_ids,
                task_group_id=task_group_id,
                logical_dates=logical_dates,
                run_ids=run_ids,
            ),
        )
        response = SUPERVISOR_COMMS.get_message()

    if TYPE_CHECKING:
        assert isinstance(response, TaskStatesResult)

    return response.task_states
