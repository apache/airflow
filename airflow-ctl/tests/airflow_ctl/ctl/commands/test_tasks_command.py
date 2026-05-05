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
"""Tests for the airflowctl `tasks` command group.

Implements tests for issues #66174, #66175, and #66176.
"""
from __future__ import annotations

import datetime
import uuid

import pytest

from airflowctl.api.client import ClientKind
from airflowctl.api.datamodels.generated import (
    TaskInstanceResponse,
    TaskInstanceState,
)
from airflowctl.ctl import cli_parser
from airflowctl.ctl.commands import tasks_command


class TestTasksCommands:
    parser = cli_parser.get_parser()
    dag_id = "test_dag"
    task_id = "test_task"
    dag_run_id = "manual__2025-01-01T00:00:00+00:00"

    task_instance_response = TaskInstanceResponse(
        id=uuid.UUID("00000000-0000-0000-0000-000000000001"),
        dag_id=dag_id,
        task_id=task_id,
        dag_run_id=dag_run_id,
        map_index=-1,
        run_after=datetime.datetime(2025, 1, 1, 0, 0, 0, tzinfo=datetime.timezone.utc),
        state=TaskInstanceState.SUCCESS,
        start_date=datetime.datetime(2025, 1, 1, 0, 0, 0, tzinfo=datetime.timezone.utc),
        end_date=datetime.datetime(2025, 1, 1, 0, 5, 0, tzinfo=datetime.timezone.utc),
        try_number=1,
        max_tries=3,
        task_display_name=task_id,
        dag_display_name=dag_id,
        pool="default_pool",
        pool_slots=1,
        executor_config="{}",
    )

    def test_tasks_state(self, api_client_maker):
        api_client = api_client_maker(
            path=(
                f"/api/v2/dags/{self.dag_id}/dagRuns/{self.dag_run_id}"
                f"/taskInstances/{self.task_id}"
            ),
            response_json=self.task_instance_response.model_dump(mode="json"),
            expected_http_status_code=200,
            kind=ClientKind.CLI,
        )
        tasks_command.state(
            self.parser.parse_args([
                "tasks", "state",
                self.dag_id, self.task_id, self.dag_run_id,
            ]),
            api_client=api_client,
        )

    def test_tasks_state_not_found(self, api_client_maker):
        api_client = api_client_maker(
            path=(
                f"/api/v2/dags/{self.dag_id}/dagRuns/{self.dag_run_id}"
                f"/taskInstances/{self.task_id}"
            ),
            response_json={"detail": "Task instance not found"},
            expected_http_status_code=404,
            kind=ClientKind.CLI,
        )
        with pytest.raises(SystemExit):
            tasks_command.state(
                self.parser.parse_args([
                    "tasks", "state",
                    self.dag_id, self.task_id, self.dag_run_id,
                ]),
                api_client=api_client,
            )
