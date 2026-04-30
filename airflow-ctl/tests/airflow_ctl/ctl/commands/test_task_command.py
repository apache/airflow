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

from urllib.parse import parse_qs
from uuid import UUID

import httpx
import pytest

from airflowctl.api.client import Client, ClientKind
from airflowctl.ctl import cli_parser
from airflowctl.ctl.commands import task_command


def _minimal_task_instance_json(*, task_id: str, state: str, ti_uuid: str | None = None) -> dict:
    run_after = "2025-01-01T00:00:00+00:00"
    return {
        "id": ti_uuid or "550e8400-e29b-41d4-a716-446655440000",
        "task_id": task_id,
        "dag_id": "test_dag",
        "dag_run_id": "test_run",
        "map_index": -1,
        "run_after": run_after,
        "try_number": 1,
        "max_tries": 0,
        "task_display_name": task_id,
        "dag_display_name": "test_dag",
        "pool": "default_pool",
        "pool_slots": 1,
        "executor_config": "{}",
        "state": state,
    }


class TestTaskCommands:
    parser = cli_parser.get_parser()
    dag_id = "test_dag"
    dag_run_id = "test_run"

    def test_states_for_dag_run(self, capsys):
        path = f"/api/v2/dags/{self.dag_id}/dagRuns/{self.dag_run_id}/taskInstances"
        payload = {
            "task_instances": [
                _minimal_task_instance_json(
                    task_id="task_a", state="success", ti_uuid="550e8400-e29b-41d4-a716-446655440010"
                ),
                _minimal_task_instance_json(
                    task_id="task_b",
                    state="failed",
                    ti_uuid="550e8400-e29b-41d4-a716-446655440011",
                ),
            ],
            "total_entries": 2,
        }

        def handle_request(request: httpx.Request) -> httpx.Response:
            assert request.method == "GET"
            assert request.url.path == path
            query = parse_qs(request.url.query.decode())
            assert query.get("limit") == ["100"]
            assert query.get("offset", ["0"]) == ["0"]
            return httpx.Response(200, json=payload)

        api_client = Client(
            base_url="http://localhost:8080",
            transport=httpx.MockTransport(handle_request),
            token="",
            kind=ClientKind.CLI,
        )

        task_command.states_for_dag_run(
            self.parser.parse_args(
                [
                    "tasks",
                    "states-for-dag-run",
                    "--dag-id",
                    self.dag_id,
                    "--dag-run-id",
                    self.dag_run_id,
                    "--output",
                    "plain",
                ]
            ),
            api_client=api_client,
        )
        out = capsys.readouterr().out
        assert "task_a" in out
        assert "success" in out
        assert "task_b" in out
        assert "failed" in out

    def test_states_for_dag_run_paginates(self):
        path = f"/api/v2/dags/{self.dag_id}/dagRuns/{self.dag_run_id}/taskInstances"
        calls: list[httpx.Request] = []

        first_page = [
            _minimal_task_instance_json(
                task_id=f"t{i}",
                state="success",
                ti_uuid=str(UUID(int=i + 1)),
            )
            for i in range(100)
        ]
        second_page = [
            _minimal_task_instance_json(
                task_id="t100",
                state="running",
                ti_uuid=str(UUID(int=101)),
            )
        ]

        def handle_request(request: httpx.Request) -> httpx.Response:
            calls.append(request)
            assert request.method == "GET"
            assert request.url.path == path
            query = parse_qs(request.url.query.decode())
            assert query.get("limit") == ["100"]
            offset = int(query.get("offset", ["0"])[0])
            if offset == 0:
                return httpx.Response(
                    200,
                    json={
                        "task_instances": first_page,
                        "total_entries": 101,
                    },
                )
            if offset == 100:
                return httpx.Response(
                    200,
                    json={
                        "task_instances": second_page,
                        "total_entries": 101,
                    },
                )
            return httpx.Response(200, json={"task_instances": [], "total_entries": 101})

        api_client = Client(
            base_url="http://localhost:8080",
            transport=httpx.MockTransport(handle_request),
            token="",
            kind=ClientKind.CLI,
        )

        task_command.states_for_dag_run(
            self.parser.parse_args(
                [
                    "tasks",
                    "states-for-dag-run",
                    "--dag-id",
                    self.dag_id,
                    "--dag-run-id",
                    self.dag_run_id,
                    "--output",
                    "plain",
                ]
            ),
            api_client=api_client,
        )
        assert len(calls) == 2

    def test_states_for_dag_run_api_error(self, api_client_maker):
        path = f"/api/v2/dags/{self.dag_id}/dagRuns/{self.dag_run_id}/taskInstances"
        api_client = api_client_maker(
            path=path,
            response_json={"detail": "Not found"},
            expected_http_status_code=404,
            kind=ClientKind.CLI,
        )
        with pytest.raises(SystemExit):
            task_command.states_for_dag_run(
                self.parser.parse_args(
                    [
                        "tasks",
                        "states-for-dag-run",
                        "--dag-id",
                        self.dag_id,
                        "--dag-run-id",
                        self.dag_run_id,
                    ]
                ),
                api_client=api_client,
            )
