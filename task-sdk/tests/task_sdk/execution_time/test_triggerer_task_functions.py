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

from unittest import mock

import pytest

from airflow.sdk.execution_time.comms import (
    DagRunStateResult,
    DRCount,
    GetDagRunState,
    GetDRCount,
    GetTaskStates,
    GetTICount,
    TaskStatesResult,
    TICount,
)
from airflow.sdk.execution_time.triggerer_task_functions import (
    get_dagrun_state,
    get_dr_count,
    get_task_states,
    get_ti_count,
)
from airflow.utils import timezone


@pytest.mark.asyncio
async def test_get_ti_count(mock_supervisor_comms):
    """Test that async get_ti_count sends the correct request and returns the count."""

    mock_supervisor_comms.get_message.return_value = TICount(count=2)

    count = await get_ti_count(
        dag_id="test_dag",
        task_ids=["task1", "task2"],
        task_group_id="group1",
        logical_dates=[timezone.datetime(2024, 1, 1)],
        run_ids=["run1"],
        states=["success", "failed"],
    )

    mock_supervisor_comms.send_request.assert_called_once_with(
        log=mock.ANY,
        msg=GetTICount(
            dag_id="test_dag",
            task_ids=["task1", "task2"],
            task_group_id="group1",
            logical_dates=[timezone.datetime(2024, 1, 1)],
            run_ids=["run1"],
            states=["success", "failed"],
        ),
    )
    assert count == 2


@pytest.mark.asyncio
async def test_get_dr_count(mock_supervisor_comms):
    """Test that async get_dr_count sends the correct request and returns the count."""
    mock_supervisor_comms.get_message.return_value = DRCount(count=2)

    count = await get_dr_count(
        dag_id="test_dag",
        logical_dates=[timezone.datetime(2024, 1, 1)],
        run_ids=["run1"],
        states=["success", "failed"],
    )

    mock_supervisor_comms.send_request.assert_called_once_with(
        log=mock.ANY,
        msg=GetDRCount(
            dag_id="test_dag",
            logical_dates=[timezone.datetime(2024, 1, 1)],
            run_ids=["run1"],
            states=["success", "failed"],
        ),
    )
    assert count == 2


@pytest.mark.asyncio
async def test_get_dagrun_state(mock_supervisor_comms):
    """Test that async get_dagrun_state sends the correct request and returns the state."""

    mock_supervisor_comms.get_message.return_value = DagRunStateResult(state="running")

    state = await get_dagrun_state(
        dag_id="test_dag",
        run_id="run1",
    )

    mock_supervisor_comms.send_request.assert_called_once_with(
        log=mock.ANY,
        msg=GetDagRunState(
            dag_id="test_dag",
            run_id="run1",
        ),
    )
    assert state == "running"


@pytest.mark.asyncio
async def test_get_task_states(mock_supervisor_comms):
    """Test that async get_task_states sends the correct request and returns the states."""
    mock_supervisor_comms.get_message.return_value = TaskStatesResult(
        task_states={"run1": {"task1": "running"}}
    )

    states = await get_task_states(
        dag_id="test_dag",
        task_ids=["task1"],
        run_ids=["run1"],
    )

    mock_supervisor_comms.send_request.assert_called_once_with(
        log=mock.ANY,
        msg=GetTaskStates(
            dag_id="test_dag",
            task_ids=["task1"],
            run_ids=["run1"],
        ),
    )
    assert states == {"run1": {"task1": "running"}}
