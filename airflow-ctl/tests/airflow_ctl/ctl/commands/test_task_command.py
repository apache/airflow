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

from unittest import mock

import pytest

from airflowctl.ctl import cli_parser
from airflowctl.ctl.commands import task_command


class TestTaskCommands:
    parser = cli_parser.get_parser()

    def test_clear_tasks(self):
        api_client = mock.Mock()
        response_data = {"task_instances": [{"task_id": "extract"}], "total_entries": 1}
        api_client.post.return_value.json.return_value = response_data

        args = self.parser.parse_args(
            [
                "tasks",
                "clear",
                "--dag-id",
                "example_dag",
                "--dag-run-id",
                "manual__2026-05-02T00:00:00+00:00",
                "--task-ids",
                "extract,transform",
                "--only-failed",
                "--upstream",
                "--dry-run",
                "--output",
                "json",
            ]
        )

        with mock.patch("airflowctl.ctl.commands.task_command.AirflowConsole") as console:
            result = task_command.task_clear(args, api_client=api_client)

        assert result == response_data
        api_client.post.assert_called_once_with(
            "dags/example_dag/clearTaskInstances",
            json={
                "dag_run_id": "manual__2026-05-02T00:00:00+00:00",
                "task_ids": ["extract", "transform"],
                "only_failed": True,
                "only_running": False,
                "include_upstream": True,
                "include_downstream": False,
                "dry_run": True,
            },
        )
        console.return_value.print_as.assert_called_once_with(data=[response_data], output="json")

    def test_clear_tasks_filters_none_task_ids(self):
        api_client = mock.Mock()
        response_data = {"task_instances": [], "total_entries": 0}
        api_client.post.return_value.json.return_value = response_data

        args = self.parser.parse_args(["tasks", "clear", "--dag-id", "example_dag", "--dag-run-id", "run_1"])

        task_command.task_clear(args, api_client=api_client)

        api_client.post.assert_called_once_with(
            "dags/example_dag/clearTaskInstances",
            json={
                "dag_run_id": "run_1",
                "only_failed": False,
                "only_running": False,
                "include_upstream": False,
                "include_downstream": False,
                "dry_run": False,
            },
        )

    def test_clear_tasks_rejects_only_failed_and_only_running(self):
        args = self.parser.parse_args(
            [
                "tasks",
                "clear",
                "--dag-id",
                "example_dag",
                "--dag-run-id",
                "run_1",
                "--only-failed",
                "--only-running",
            ]
        )

        with pytest.raises(SystemExit, match="--only-failed and --only-running cannot both be set"):
            task_command.task_clear(args, api_client=mock.Mock())
