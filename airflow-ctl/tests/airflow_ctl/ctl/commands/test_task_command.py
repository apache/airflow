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

import json
from typing import Any, cast

import httpx
import pytest

from airflowctl.api.client import Client, ClientKind
from airflowctl.ctl import cli_parser
from airflowctl.ctl.commands import task_command


class TestTaskCommands:
    parser = cli_parser.get_parser()
    dag_id = "test_dag"
    dag_run_id = "test_run"

    @pytest.mark.parametrize(
        ("extra_cli_args", "expected_checks"),
        [
            pytest.param(
                [],
                {
                    "_no_task_ids": True,
                    "dag_run_id": "test_run",
                    "dry_run": False,
                    "only_failed": True,
                    "only_running": False,
                    "include_upstream": False,
                    "include_downstream": False,
                },
                id="defaults",
            ),
            pytest.param(
                [
                    "--task-ids",
                    "a, b",
                    "--no-only-failed",
                    "--only-running",
                    "--upstream",
                    "--downstream",
                    "--dry-run",
                ],
                {
                    "_no_task_ids": False,
                    "dag_run_id": "test_run",
                    "dry_run": True,
                    "only_failed": False,
                    "only_running": True,
                    "include_upstream": True,
                    "include_downstream": True,
                    "task_ids": ["a", "b"],
                },
                id="with_flags",
            ),
        ],
    )
    def test_tasks_clear(self, extra_cli_args: list[str], expected_checks: dict[str, Any]):
        received: dict[str, object] = {}

        def handle_request(request: httpx.Request) -> httpx.Response:
            assert request.method == "POST"
            assert request.url.path == f"/api/v2/dags/{self.dag_id}/clearTaskInstances"
            received["body"] = json.loads(request.content)
            return httpx.Response(200, json={"task_instances": [], "total_entries": 0})

        api_client = Client(
            base_url="http://localhost:8080",
            transport=httpx.MockTransport(handle_request),
            token="",
            kind=ClientKind.CLI,
        )

        task_command.clear(
            self.parser.parse_args(
                [
                    "tasks",
                    "clear",
                    "--dag-id",
                    self.dag_id,
                    "--dag-run-id",
                    self.dag_run_id,
                    *extra_cli_args,
                ]
            ),
            api_client=api_client,
        )

        body = cast(dict[str, Any], received["body"])
        checks = dict(expected_checks)
        no_task_ids = checks.pop("_no_task_ids")
        if no_task_ids:
            assert body.get("task_ids") is None
        for key, value in checks.items():
            assert body[key] == value

    def test_tasks_clear_api_error(self, api_client_maker):
        api_client = api_client_maker(
            path=f"/api/v2/dags/{self.dag_id}/clearTaskInstances",
            response_json={"detail": "DAG not found"},
            expected_http_status_code=404,
            kind=ClientKind.CLI,
        )
        with pytest.raises(SystemExit):
            task_command.clear(
                self.parser.parse_args(
                    [
                        "tasks",
                        "clear",
                        "--dag-id",
                        self.dag_id,
                        "--dag-run-id",
                        self.dag_run_id,
                    ]
                ),
                api_client=api_client,
            )
