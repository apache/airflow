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

import datetime
import uuid

import pytest

from airflowctl.api.client import ClientKind
from airflowctl.api.datamodels.generated import TaskInstanceCollectionResponse, TaskInstanceResponse
from airflowctl.ctl import cli_parser
from airflowctl.ctl.commands import task_command


def _make_task_instance(task_id: str, state: str) -> TaskInstanceResponse:
    return TaskInstanceResponse(
        id=uuid.uuid4(),
        task_id=task_id,
        dag_id="test_dag",
        dag_run_id="test_run",
        map_index=-1,
        run_after=datetime.datetime(2025, 1, 1, 0, 0, 0),
        try_number=1,
        max_tries=1,
        task_display_name=task_id,
        dag_display_name="test_dag",
        pool="default_pool",
        pool_slots=1,
        executor_config="{}",
        state=state,
    )


class TestTaskCommand:
    parser = cli_parser.get_parser()
    dag_id = "test_dag"
    dag_run_id = "test_run"

    collection_response = TaskInstanceCollectionResponse(
        task_instances=[
            _make_task_instance("task_one", "success"),
            _make_task_instance("task_two", "failed"),
        ],
        total_entries=2,
    )

    def test_states_for_dag_run(self, api_client_maker):
        api_client = api_client_maker(
            path=f"/api/v2/dags/{self.dag_id}/dagRuns/{self.dag_run_id}/taskInstances",
            response_json=self.collection_response.model_dump(mode="json"),
            expected_http_status_code=200,
            kind=ClientKind.CLI,
        )
        result = task_command.states_for_dag_run(
            self.parser.parse_args(["tasks", "states-for-dag-run", self.dag_id, self.dag_run_id]),
            api_client=api_client,
        )
        assert result == [
            {"task_id": "task_one", "state": "success"},
            {"task_id": "task_two", "state": "failed"},
        ]

    def test_states_for_dag_run_not_found(self, api_client_maker):
        api_client = api_client_maker(
            path=f"/api/v2/dags/{self.dag_id}/dagRuns/{self.dag_run_id}/taskInstances",
            response_json={"detail": "DAG run not found"},
            expected_http_status_code=404,
            kind=ClientKind.CLI,
        )
        with pytest.raises(SystemExit):
            task_command.states_for_dag_run(
                self.parser.parse_args(["tasks", "states-for-dag-run", self.dag_id, self.dag_run_id]),
                api_client=api_client,
            )
