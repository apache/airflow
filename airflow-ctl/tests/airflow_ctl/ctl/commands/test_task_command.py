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

import httpx
import pytest

from airflowctl.api.client import Client, ClientKind
from airflowctl.ctl import cli_parser
from airflowctl.ctl.commands import task_command


class TestTaskCommands:
    parser = cli_parser.get_parser()
    dag_id = "test_dag"
    dag_run_id = "test_run"

    def test_tasks_clear(self):
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
                ]
            ),
            api_client=api_client,
        )

        body = received["body"]
        assert body["dag_run_id"] == self.dag_run_id
        assert body["dry_run"] is False
        assert body["only_failed"] is True
        assert body["only_running"] is False
        assert body["include_upstream"] is False
        assert body["include_downstream"] is False
        assert body.get("task_ids") is None

    def test_tasks_clear_with_flags(self):
        received: dict[str, object] = {}

        def handle_request(request: httpx.Request) -> httpx.Response:
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
                    "--task-ids",
                    "a, b",
                    "--no-only-failed",
                    "--only-running",
                    "--upstream",
                    "--downstream",
                    "--dry-run",
                ]
            ),
            api_client=api_client,
        )

        body = received["body"]
        assert body["dag_run_id"] == self.dag_run_id
        assert body["dry_run"] is True
        assert body["only_failed"] is False
        assert body["only_running"] is True
        assert body["include_upstream"] is True
        assert body["include_downstream"] is True
        assert body["task_ids"] == ["a", "b"]

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
