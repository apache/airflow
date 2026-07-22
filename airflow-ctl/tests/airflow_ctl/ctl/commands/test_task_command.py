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
import json
import uuid
from unittest import mock

import httpx
import pytest

from airflowctl.api.client import ClientKind
from airflowctl.api.datamodels.generated import (
    TaskInstanceCollectionResponse,
    TaskInstanceResponse,
    TaskInstanceState,
)
from airflowctl.api.operations import ServerResponseError
from airflowctl.ctl import cli_parser
from airflowctl.ctl.commands import task_command


def _make_server_error(status_code: int) -> ServerResponseError:
    request = httpx.Request("GET", "http://testserver/api/v2/dags/test_dag/dagRuns/test_run")
    response = httpx.Response(status_code, request=request, json={"detail": "boom"})
    return ServerResponseError(message="boom", request=request, response=response)


def _normalize_rich_output(text: str) -> str:
    return " ".join(text.split())


class TestStatesForDagRun:
    parser = cli_parser.get_parser()
    dag_id = "test_dag"
    run_id = "test_run"
    logical_date = datetime.datetime(2025, 1, 1, tzinfo=datetime.timezone.utc)

    def _make_task_instance(
        self,
        task_id: str,
        *,
        map_index: int = -1,
        state: TaskInstanceState | None = TaskInstanceState.SUCCESS,
        start_date: datetime.datetime | None = None,
        end_date: datetime.datetime | None = None,
    ) -> TaskInstanceResponse:
        return TaskInstanceResponse(
            id=uuid.uuid4(),
            task_id=task_id,
            dag_id=self.dag_id,
            dag_run_id=self.run_id,
            map_index=map_index,
            logical_date=self.logical_date,
            run_after=self.logical_date,
            state=state,
            start_date=start_date,
            end_date=end_date,
            try_number=1,
            max_tries=0,
            task_display_name=task_id,
            dag_display_name=self.dag_id,
            pool="default_pool",
            pool_slots=1,
            executor_config="{}",
        )

    def _make_api_client(self, task_instances: list[TaskInstanceResponse]) -> mock.MagicMock:
        api_client = mock.MagicMock()
        api_client.dag_runs.list.return_value.dag_runs = [mock.MagicMock(dag_run_id=self.run_id)]
        api_client.task_instances.list.return_value = TaskInstanceCollectionResponse(
            task_instances=task_instances,
            total_entries=len(task_instances),
        )
        return api_client

    @mock.patch("airflowctl.ctl.commands.task_command.AirflowConsole")
    def test_states_for_dag_run_by_run_id(self, mock_console_cls):
        start_date = datetime.datetime(2025, 1, 1, 1, tzinfo=datetime.timezone.utc)
        end_date = datetime.datetime(2025, 1, 1, 2, tzinfo=datetime.timezone.utc)
        api_client = self._make_api_client(
            [
                self._make_task_instance("task_a", start_date=start_date, end_date=end_date),
                self._make_task_instance("task_b", state=None),
            ]
        )

        task_command.states_for_dag_run(
            self.parser.parse_args(["tasks", "states-for-dag-run", self.dag_id, self.run_id]),
            api_client=api_client,
        )

        api_client.dag_runs.list.assert_not_called()
        api_client.task_instances.list.assert_called_once_with(dag_id=self.dag_id, dag_run_id=self.run_id)
        mock_console_cls.return_value.print_as.assert_called_once_with(
            data=[
                {
                    "dag_id": self.dag_id,
                    "logical_date": "2025-01-01T00:00:00+00:00",
                    "task_id": "task_a",
                    "state": "success",
                    "start_date": "2025-01-01T01:00:00+00:00",
                    "end_date": "2025-01-01T02:00:00+00:00",
                },
                {
                    "dag_id": self.dag_id,
                    "logical_date": "2025-01-01T00:00:00+00:00",
                    "task_id": "task_b",
                    "state": "",
                    "start_date": "",
                    "end_date": "",
                },
            ],
            output="json",
        )

    @mock.patch("airflowctl.ctl.commands.task_command.AirflowConsole")
    def test_states_for_dag_run_by_logical_date(self, mock_console_cls):
        api_client = self._make_api_client([self._make_task_instance("task_a")])

        task_command.states_for_dag_run(
            self.parser.parse_args(
                ["tasks", "states-for-dag-run", self.dag_id, "--logical-date", self.logical_date.isoformat()]
            ),
            api_client=api_client,
        )

        api_client.dag_runs.list.assert_called_once_with(
            dag_id=self.dag_id,
            logical_date_gte=self.logical_date,
            logical_date_lte=self.logical_date,
            order_by="-id",
            limit=1,
            suppress_error_log=True,
        )
        api_client.task_instances.list.assert_called_once_with(dag_id=self.dag_id, dag_run_id=self.run_id)
        mock_console_cls.return_value.print_as.assert_called_once_with(
            data=[
                {
                    "dag_id": self.dag_id,
                    "logical_date": "2025-01-01T00:00:00+00:00",
                    "task_id": "task_a",
                    "state": "success",
                    "start_date": "",
                    "end_date": "",
                },
            ],
            output="json",
        )

    @mock.patch("airflowctl.ctl.commands.task_command.AirflowConsole")
    def test_states_for_dag_run_includes_map_index_for_mapped_instances(self, mock_console_cls):
        api_client = self._make_api_client(
            [
                self._make_task_instance("plain_task"),
                self._make_task_instance("mapped_task", map_index=0),
                self._make_task_instance("mapped_task", map_index=1),
            ]
        )

        task_command.states_for_dag_run(
            self.parser.parse_args(["tasks", "states-for-dag-run", self.dag_id, self.run_id]),
            api_client=api_client,
        )

        rows = mock_console_cls.return_value.print_as.call_args.kwargs["data"]
        assert [row["map_index"] for row in rows] == ["", "0", "1"]

    @pytest.mark.parametrize(
        "extra_args",
        [
            [],
            ["test_run", "--logical-date", "2025-01-01T00:00:00+00:00"],
        ],
        ids=["neither", "both"],
    )
    def test_states_for_dag_run_requires_exactly_one_of_run_id_and_logical_date(self, extra_args, capsys):
        api_client = mock.MagicMock()

        with pytest.raises(SystemExit, match="1"):
            task_command.states_for_dag_run(
                self.parser.parse_args(["tasks", "states-for-dag-run", self.dag_id, *extra_args]),
                api_client=api_client,
            )

        api_client.task_instances.list.assert_not_called()
        assert _normalize_rich_output(capsys.readouterr().out) == (
            "Provide either run_id or --logical-date, but not both"
        )

    @pytest.mark.parametrize(
        ("logical_date", "expected_message"),
        [
            ("not-a-date", "Invalid --logical-date: 'not-a-date'"),
            ("2025-01-01T00:00:00", "--logical-date must include a timezone offset"),
        ],
        ids=["unparsable", "naive"],
    )
    def test_states_for_dag_run_rejects_bad_logical_date(self, logical_date, expected_message, capsys):
        api_client = mock.MagicMock()

        with pytest.raises(SystemExit, match="1"):
            task_command.states_for_dag_run(
                self.parser.parse_args(
                    ["tasks", "states-for-dag-run", self.dag_id, "--logical-date", logical_date]
                ),
                api_client=api_client,
            )

        api_client.dag_runs.list.assert_not_called()
        assert _normalize_rich_output(capsys.readouterr().out) == expected_message

    @pytest.mark.parametrize("list_failure", ["no_matching_run", "dag_not_found_404"])
    def test_states_for_dag_run_dag_run_not_found_by_logical_date(self, list_failure, capsys):
        api_client = mock.MagicMock()
        if list_failure == "no_matching_run":
            api_client.dag_runs.list.return_value.dag_runs = []
        else:
            api_client.dag_runs.list.side_effect = _make_server_error(404)

        with pytest.raises(SystemExit, match="1"):
            task_command.states_for_dag_run(
                self.parser.parse_args(
                    [
                        "tasks",
                        "states-for-dag-run",
                        self.dag_id,
                        "--logical-date",
                        self.logical_date.isoformat(),
                    ]
                ),
                api_client=api_client,
            )

        api_client.task_instances.list.assert_not_called()
        assert _normalize_rich_output(capsys.readouterr().out) == (
            "Dag run for test_dag with logical date '2025-01-01T00:00:00+00:00' not found"
        )

    def test_states_for_dag_run_dag_run_not_found(self, capsys):
        api_client = mock.MagicMock()
        api_client.task_instances.list.side_effect = _make_server_error(404)

        with pytest.raises(SystemExit, match="1"):
            task_command.states_for_dag_run(
                self.parser.parse_args(["tasks", "states-for-dag-run", self.dag_id, self.run_id]),
                api_client=api_client,
            )

        assert _normalize_rich_output(capsys.readouterr().out) == (
            "Dag run 'test_run' of Dag 'test_dag' not found"
        )

    @pytest.mark.parametrize(
        "failing_call",
        ["dag_runs_list", "task_instances_list"],
    )
    def test_states_for_dag_run_propagates_non_404_api_error(self, failing_call):
        api_client = mock.MagicMock()
        error = _make_server_error(500)
        if failing_call == "dag_runs_list":
            api_client.dag_runs.list.side_effect = error
            argv = ["tasks", "states-for-dag-run", self.dag_id, "--logical-date", "2025-01-01T00:00:00+00:00"]
        else:
            api_client.task_instances.list.side_effect = error
            argv = ["tasks", "states-for-dag-run", self.dag_id, self.run_id]

        with pytest.raises(ServerResponseError) as ctx:
            task_command.states_for_dag_run(self.parser.parse_args(argv), api_client=api_client)

        assert ctx.value is error


class TestTaskCommands:
    parser = cli_parser.get_parser()
    dag_id = "example_dag"
    dag_run_id = "manual__2024-01-01T00:00:00+00:00"
    task_id = "my_task"

    task_instance_response = TaskInstanceResponse(
        id=uuid.uuid4(),
        task_id=task_id,
        dag_id=dag_id,
        dag_run_id=dag_run_id,
        map_index=-1,
        run_after=datetime.datetime(2024, 1, 1, 0, 0, 0),
        try_number=1,
        max_tries=1,
        task_display_name=task_id,
        dag_display_name=dag_id,
        pool="default_pool",
        pool_slots=1,
        executor_config="{}",
        state=TaskInstanceState.SUCCESS,
    )

    def test_task_state(self, api_client_maker, capsys):
        api_client = api_client_maker(
            path=f"/api/v2/dags/{self.dag_id}/dagRuns/{self.dag_run_id}/taskInstances/{self.task_id}",
            response_json=self.task_instance_response.model_dump(mode="json"),
            expected_http_status_code=200,
            kind=ClientKind.CLI,
        )
        task_command.task_state(
            self.parser.parse_args(
                [
                    "tasks",
                    "state",
                    self.dag_id,
                    self.dag_run_id,
                    self.task_id,
                ]
            ),
            api_client=api_client,
        )
        assert json.loads(capsys.readouterr().out) == [{"state": "success"}]

    def test_task_state_not_found(self, api_client_maker):
        api_client = api_client_maker(
            path=f"/api/v2/dags/{self.dag_id}/dagRuns/{self.dag_run_id}/taskInstances/{self.task_id}",
            response_json={"detail": "Task instance not found"},
            expected_http_status_code=404,
            kind=ClientKind.CLI,
        )
        with pytest.raises(ServerResponseError):
            task_command.task_state(
                self.parser.parse_args(
                    [
                        "tasks",
                        "state",
                        self.dag_id,
                        self.dag_run_id,
                        self.task_id,
                    ]
                ),
                api_client=api_client,
            )

    @pytest.mark.parametrize("map_index", [0, 1, 7])
    def test_task_state_mapped(self, api_client_maker, capsys, map_index):
        mapped_response = self.task_instance_response.model_copy(update={"map_index": map_index})
        api_client = api_client_maker(
            path=(
                f"/api/v2/dags/{self.dag_id}/dagRuns/{self.dag_run_id}"
                f"/taskInstances/{self.task_id}/{map_index}"
            ),
            response_json=mapped_response.model_dump(mode="json"),
            expected_http_status_code=200,
            kind=ClientKind.CLI,
        )
        task_command.task_state(
            self.parser.parse_args(
                [
                    "tasks",
                    "state",
                    self.dag_id,
                    self.dag_run_id,
                    self.task_id,
                    f"--map-index={map_index}",
                ]
            ),
            api_client=api_client,
        )
        assert json.loads(capsys.readouterr().out) == [{"state": "success"}]
