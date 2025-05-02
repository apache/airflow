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

from typing import TYPE_CHECKING

import pytest
import structlog

from airflow.sdk.execution_time.comms import SucceedTask, TaskState

if TYPE_CHECKING:
    from airflow.sdk.definitions.dag import DAG


@pytest.fixture
def run_ti(create_runtime_ti, mock_supervisor_comms):
    def run(dag: DAG, task_id: str, map_index: int):
        """Run the task and return the state that the SDK sent as the result for easier asserts"""
        from airflow.sdk.execution_time.task_runner import run

        log = structlog.get_logger(__name__)

        mock_supervisor_comms.send_request.reset_mock()
        ti = create_runtime_ti(dag.task_dict[task_id], map_index=map_index)
        run(ti, ti.get_template_context(), log)

        for call in mock_supervisor_comms.send_request.mock_calls:
            msg = call.kwargs["msg"]
            if isinstance(msg, (TaskState, SucceedTask)):
                return msg.state
        raise RuntimeError("Unable to find call to TaskState")

    return run
