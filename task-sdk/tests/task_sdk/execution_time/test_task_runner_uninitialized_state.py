#
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

from unittest.mock import MagicMock, patch

import pytest

from airflow.sdk.api.datamodels._generated import TaskInstanceState
from airflow.sdk.exceptions import DagRunTriggerException
from airflow.sdk.execution_time.task_runner import run


def test_run_uninitialized_state_on_handle_trigger_dag_run_failure():
    """
    Test that if _handle_trigger_dag_run raises an exception (e.g. COMMS failure),
    the finally block does not crash with UnboundLocalError for 'state'.
    """
    ti = MagicMock()
    ti.dag_id = "test_dag"
    ti.task_id = "test_task"
    ti.run_id = "test_run"
    ti.map_index = -1
    ti._ti_context_from_server = None
    ti.rendered_map_index = None

    context = MagicMock()
    log = MagicMock()

    # Mock _execute_task to raise DagRunTriggerException
    # Mock _handle_trigger_dag_run to raise an exception itself
    exception = DagRunTriggerException(
        trigger_dag_id="target",
        dag_run_id="run",
        logical_date=None,
        conf=None,
        reset_dag_run=False,
        skip_when_already_exists=False,
        wait_for_completion=False,
        allowed_states=None,
        failed_states=None,
        poke_interval=60,
        deferrable=False,
        note=None,
    )
    with (
        patch("airflow.sdk.execution_time.task_runner._prepare", return_value=None),
        patch("airflow.sdk.execution_time.task_runner._execute_task", side_effect=exception),
        patch(
            "airflow.sdk.execution_time.task_runner._handle_trigger_dag_run",
            side_effect=RuntimeError("COMMS failure"),
        ),
        patch(
            "airflow.sdk.execution_time.task_runner._handle_current_task_failed",
            side_effect=RuntimeError("Handler failure"),
        ),
        patch("airflow.sdk.execution_time.task_runner.stats") as mock_stats,
    ):
        with pytest.raises(RuntimeError, match="Handler failure"):
            run(ti, context, log)

        # Verify that state was FAILED (the default) in the stats.incr calls
        mock_stats.incr.assert_any_call(
            "ti.finish",
            tags={"dag_id": "test_dag", "task_id": "test_task", "state": TaskInstanceState.FAILED},
        )
