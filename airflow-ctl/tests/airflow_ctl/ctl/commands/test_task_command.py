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

from airflowctl.api.client import ClientKind
from airflowctl.ctl import cli_parser
from airflowctl.ctl.commands import task_command


class TestTaskCommands:
    parser = cli_parser.get_parser()
    dag_id = "test_dag"
    task_id = "test_task"

    def _make_clear_response(self):
        """Build a minimal response payload for the clear endpoint."""
        return {
            "task_instances": [],
            "total_entries": 0,
        }

    def test_clear_success(self, api_client_maker):
        response_json = self._make_clear_response()
        api_client = api_client_maker(
            path=f"/api/v2/dags/{self.dag_id}/clearTaskInstances",
            response_json=response_json,
            expected_http_status_code=200,
            kind=ClientKind.CLI,
        )
        with mock.patch("airflowctl.ctl.commands.task_command.AirflowConsole") as mock_console_cls:
            task_command.clear(
                self.parser.parse_args(["tasks", "clear", self.dag_id, self.task_id]),
                api_client=api_client,
            )

        mock_console_cls.return_value.print_as.assert_called_once_with(data=[], output="json")

    def test_clear_with_dates(self, api_client_maker):
        response_json = self._make_clear_response()
        api_client = api_client_maker(
            path=f"/api/v2/dags/{self.dag_id}/clearTaskInstances",
            response_json=response_json,
            expected_http_status_code=200,
            kind=ClientKind.CLI,
        )
        with mock.patch("airflowctl.ctl.commands.task_command.AirflowConsole") as mock_console_cls:
            task_command.clear(
                self.parser.parse_args(
                    [
                        "tasks",
                        "clear",
                        self.dag_id,
                        self.task_id,
                        "--start-date",
                        "2026-01-01T00:00:00",
                        "--end-date",
                        "2026-01-02T00:00:00",
                    ]
                ),
                api_client=api_client,
            )

        mock_console_cls.return_value.print_as.assert_called_once_with(data=[], output="json")

    def test_clear_fail(self, api_client_maker):
        api_client = api_client_maker(
            path=f"/api/v2/dags/{self.dag_id}/clearTaskInstances",
            response_json={"detail": "DAG not found"},
            expected_http_status_code=404,
            kind=ClientKind.CLI,
        )
        with pytest.raises(SystemExit):
            task_command.clear(
                self.parser.parse_args(["tasks", "clear", self.dag_id, self.task_id]),
                api_client=api_client,
            )
