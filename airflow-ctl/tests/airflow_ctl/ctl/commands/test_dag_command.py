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
from unittest import mock

import httpx
import pytest

from airflowctl.api.client import ClientKind
from airflowctl.api.datamodels.generated import DAGResponse
from airflowctl.api.operations import ServerResponseError
from airflowctl.ctl import cli_parser
from airflowctl.ctl.commands import dag_command


def _server_error(status_code: int) -> ServerResponseError:
    request = httpx.Request("GET", "http://testserver/api/v2/dags/test_dag/dagRuns/test_run")
    response = httpx.Response(status_code, request=request, json={"detail": "boom"})
    return ServerResponseError(message="boom", request=request, response=response)


class TestDagCommands:
    parser = cli_parser.get_parser()
    dag_id = "test_dag"
    dag_display_name = "dag_display_name"
    dag_response_paused = DAGResponse(
        dag_id=dag_id,
        dag_display_name=dag_display_name,
        is_paused=False,
        last_parsed_time=datetime.datetime(2024, 12, 31, 23, 59, 59),
        last_expired=datetime.datetime(2025, 1, 1, 0, 0, 0),
        fileloc="fileloc",
        relative_fileloc="relative_fileloc",
        description="description",
        timetable_summary="timetable_summary",
        timetable_description="timetable_description",
        timetable_partitioned=False,
        timetable_periodic=True,
        tags=[],
        max_active_tasks=1,
        max_active_runs=1,
        max_consecutive_failed_dag_runs=1,
        has_task_concurrency_limits=True,
        has_import_errors=True,
        next_dagrun_logical_date=datetime.datetime(2025, 1, 1, 0, 0, 0),
        next_dagrun_data_interval_start=datetime.datetime(2025, 1, 1, 0, 0, 0),
        next_dagrun_data_interval_end=datetime.datetime(2025, 1, 1, 0, 0, 0),
        next_dagrun_run_after=datetime.datetime(2025, 1, 1, 0, 0, 0),
        owners=["apache-airflow"],
        is_backfillable=True,
        file_token="file_token",
        bundle_name="bundle_name",
        is_stale=False,
    )

    dag_response_unpaused = DAGResponse(
        dag_id=dag_id,
        dag_display_name=dag_display_name,
        is_paused=True,
        last_parsed_time=datetime.datetime(2024, 12, 31, 23, 59, 59),
        last_expired=datetime.datetime(2025, 1, 1, 0, 0, 0),
        fileloc="fileloc",
        relative_fileloc="relative_fileloc",
        description="description",
        timetable_summary="timetable_summary",
        timetable_description="timetable_description",
        timetable_partitioned=False,
        timetable_periodic=True,
        tags=[],
        max_active_tasks=1,
        max_active_runs=1,
        max_consecutive_failed_dag_runs=1,
        has_task_concurrency_limits=True,
        has_import_errors=True,
        next_dagrun_logical_date=datetime.datetime(2025, 1, 1, 0, 0, 0),
        next_dagrun_data_interval_start=datetime.datetime(2025, 1, 1, 0, 0, 0),
        next_dagrun_data_interval_end=datetime.datetime(2025, 1, 1, 0, 0, 0),
        next_dagrun_run_after=datetime.datetime(2025, 1, 1, 0, 0, 0),
        owners=["apache-airflow"],
        is_backfillable=True,
        file_token="file_token",
        bundle_name="bundle_name",
        is_stale=False,
    )

    dag_response_no_schedule = DAGResponse(
        dag_id=dag_id,
        dag_display_name=dag_display_name,
        is_paused=True,
        last_parsed_time=datetime.datetime(2024, 12, 31, 23, 59, 59),
        last_expired=datetime.datetime(2025, 1, 1, 0, 0, 0),
        fileloc="fileloc",
        relative_fileloc="relative_fileloc",
        description="description",
        timetable_summary=None,
        timetable_description=None,
        timetable_partitioned=False,
        timetable_periodic=False,
        tags=[],
        max_active_tasks=1,
        max_active_runs=1,
        max_consecutive_failed_dag_runs=1,
        has_task_concurrency_limits=False,
        has_import_errors=False,
        next_dagrun_logical_date=None,
        next_dagrun_data_interval_start=None,
        next_dagrun_data_interval_end=None,
        next_dagrun_run_after=None,
        owners=["apache-airflow"],
        is_backfillable=False,
        file_token="file_token",
        bundle_name="bundle_name",
        is_stale=False,
    )

    def test_pause_dag(self, api_client_maker, monkeypatch):
        api_client = api_client_maker(
            path=f"/api/v2/dags/{self.dag_id}",
            response_json=self.dag_response_paused.model_dump(mode="json"),
            expected_http_status_code=200,
            kind=ClientKind.CLI,
        )
        assert self.dag_response_paused.is_paused is False
        dag_response_dict = dag_command.pause(
            self.parser.parse_args(["dags", "pause", self.dag_id]),
            api_client=api_client,
        )
        assert dag_response_dict["is_paused"] is False

    def test_pause_fail(self, api_client_maker, monkeypatch):
        api_client = api_client_maker(
            path=f"/api/v2/dags/{self.dag_id}",
            response_json={"detail": "DAG not found"},
            expected_http_status_code=404,
            kind=ClientKind.CLI,
        )
        with pytest.raises(SystemExit):
            dag_command.pause(
                self.parser.parse_args(["dags", "pause", self.dag_id]),
                api_client=api_client,
            )

    def test_unpause_dag(self, api_client_maker, monkeypatch):
        api_client = api_client_maker(
            path=f"/api/v2/dags/{self.dag_id}",
            response_json=self.dag_response_unpaused.model_dump(mode="json"),
            expected_http_status_code=200,
            kind=ClientKind.CLI,
        )
        assert self.dag_response_unpaused.is_paused is True
        dag_response_dict = dag_command.unpause(
            self.parser.parse_args(["dags", "unpause", self.dag_id]),
            api_client=api_client,
        )
        assert dag_response_dict["is_paused"] is True

    def test_unpause_fail(self, api_client_maker, monkeypatch):
        api_client = api_client_maker(
            path=f"/api/v2/dags/{self.dag_id}",
            response_json={"detail": "DAG not found"},
            expected_http_status_code=404,
            kind=ClientKind.CLI,
        )
        with pytest.raises(SystemExit):
            dag_command.unpause(
                self.parser.parse_args(["dags", "unpause", self.dag_id]),
                api_client=api_client,
            )

    def test_next_execution(self, api_client_maker):
        api_client = api_client_maker(
            path=f"/api/v2/dags/{self.dag_id}",
            response_json=self.dag_response_paused.model_dump(mode="json"),
            expected_http_status_code=200,
            kind=ClientKind.CLI,
        )
        result = dag_command.next_execution(
            self.parser.parse_args(["dags", "next-execution", self.dag_id]),
            api_client=api_client,
        )
        assert result["next_dagrun_logical_date"] == datetime.datetime(2025, 1, 1, 0, 0, 0)
        assert result["next_dagrun_data_interval_start"] == datetime.datetime(2025, 1, 1, 0, 0, 0)
        assert result["next_dagrun_data_interval_end"] == datetime.datetime(2025, 1, 1, 0, 0, 0)
        assert result["next_dagrun_run_after"] == datetime.datetime(2025, 1, 1, 0, 0, 0)

    def test_next_execution_no_schedule(self, api_client_maker):
        api_client = api_client_maker(
            path=f"/api/v2/dags/{self.dag_id}",
            response_json=self.dag_response_no_schedule.model_dump(mode="json"),
            expected_http_status_code=200,
            kind=ClientKind.CLI,
        )
        result = dag_command.next_execution(
            self.parser.parse_args(["dags", "next-execution", self.dag_id]),
            api_client=api_client,
        )
        assert result is None

    def test_next_execution_fail(self, api_client_maker):
        api_client = api_client_maker(
            path=f"/api/v2/dags/{self.dag_id}",
            response_json={"detail": "DAG not found"},
            expected_http_status_code=404,
            kind=ClientKind.CLI,
        )
        with pytest.raises(SystemExit):
            dag_command.next_execution(
                self.parser.parse_args(["dags", "next-execution", self.dag_id]),
                api_client=api_client,
            )

    def test_state_by_run_id(self, capsys):
        api_client = mock.MagicMock()
        api_client.dag_runs.get.return_value = mock.MagicMock(state="success", conf={})

        dag_command.state(
            self.parser.parse_args(["dags", "state", self.dag_id, "test_run"]),
            api_client=api_client,
        )

        assert capsys.readouterr().out.strip() == "success"
        api_client.dag_runs.get.assert_called_once_with(
            dag_id=self.dag_id,
            dag_run_id="test_run",
            suppress_error_log=True,
        )
        api_client.dag_runs.list.assert_not_called()

    def test_state_by_logical_date(self, capsys):
        api_client = mock.MagicMock()
        logical_date = datetime.datetime(2025, 1, 1, tzinfo=datetime.timezone.utc)
        api_client.dag_runs.get.side_effect = _server_error(404)
        api_client.dag_runs.list.return_value.dag_runs = [
            mock.MagicMock(state="failed", conf={"reason": "[red]test[/red]"})
        ]

        dag_command.state(
            self.parser.parse_args(["dags", "state", self.dag_id, logical_date.isoformat()]),
            api_client=api_client,
        )

        assert capsys.readouterr().out.strip() == 'failed, {"reason": "[red]test[/red]"}'
        api_client.dag_runs.list.assert_called_once_with(
            dag_id=self.dag_id,
            logical_date_gte=logical_date,
            logical_date_lte=logical_date,
            order_by="-id",
            limit=1,
        )

    @pytest.mark.parametrize(
        ("value", "expected_list_kwargs"),
        [
            pytest.param("missing_run", {"dag_id": dag_id, "limit": 1}, id="run-id"),
            pytest.param(
                "2025-01-01T00:00:00+00:00",
                {
                    "dag_id": dag_id,
                    "logical_date_gte": datetime.datetime(2025, 1, 1, tzinfo=datetime.timezone.utc),
                    "logical_date_lte": datetime.datetime(2025, 1, 1, tzinfo=datetime.timezone.utc),
                    "order_by": "-id",
                    "limit": 1,
                },
                id="logical-date",
            ),
        ],
    )
    @mock.patch("rich.print")
    def test_state_missing_run_prints_message(self, mock_rich_print, value, expected_list_kwargs):
        api_client = mock.MagicMock()
        api_client.dag_runs.get.side_effect = _server_error(404)
        api_client.dag_runs.list.return_value.dag_runs = []

        dag_command.state(
            self.parser.parse_args(["dags", "state", self.dag_id, value]),
            api_client=api_client,
        )

        mock_rich_print.assert_called_once_with("[yellow]No matching Dag run found.[/yellow]")
        api_client.dag_runs.list.assert_called_once_with(**expected_list_kwargs)

    def test_state_missing_dag_propagates_api_error(self):
        api_client = mock.MagicMock()
        api_client.dag_runs.get.side_effect = _server_error(404)
        api_client.dag_runs.list.side_effect = error = _server_error(404)

        with pytest.raises(ServerResponseError) as ctx:
            dag_command.state(
                self.parser.parse_args(["dags", "state", self.dag_id, "missing_run"]),
                api_client=api_client,
            )

        assert ctx.value is error

    def test_state_rejects_naive_logical_date(self):
        api_client = mock.MagicMock()
        api_client.dag_runs.get.side_effect = _server_error(404)

        with pytest.raises(SystemExit, match="Logical date must include a timezone offset"):
            dag_command.state(
                self.parser.parse_args(["dags", "state", self.dag_id, "2025-01-01T00:00:00"]),
                api_client=api_client,
            )

        api_client.dag_runs.list.assert_not_called()

    def test_state_propagates_non_404_api_error(self):
        api_client = mock.MagicMock()
        api_client.dag_runs.get.side_effect = error = _server_error(500)

        with pytest.raises(ServerResponseError) as ctx:
            dag_command.state(
                self.parser.parse_args(["dags", "state", self.dag_id, "test_run"]),
                api_client=api_client,
            )

        assert ctx.value is error
        api_client.dag_runs.list.assert_not_called()
