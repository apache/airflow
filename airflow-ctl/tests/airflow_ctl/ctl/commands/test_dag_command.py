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
from types import SimpleNamespace
from unittest.mock import Mock, call

import pytest

from airflowctl.api.client import ClientKind
from airflowctl.api.datamodels.generated import DAGResponse
from airflowctl.ctl import cli_parser
from airflowctl.ctl.commands import dag_command


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

    @staticmethod
    def _dag_run(
        dag_run_id: str,
        *,
        logical_date: datetime.datetime | None = datetime.datetime(2025, 1, 1, 0, 0, 0),
        partition_key: str | None = None,
        partition_date: datetime.datetime | None = datetime.datetime(2025, 1, 1, 0, 0, 0),
    ):
        return SimpleNamespace(
            dag_run_id=dag_run_id,
            logical_date=logical_date,
            partition_key=partition_key,
            partition_date=partition_date,
        )

    @staticmethod
    def _api_client_mock():
        api_client = Mock(spec_set=["dag_runs"])
        api_client.dag_runs = Mock(spec_set=["get", "list", "_clear_task_instances"])
        return api_client

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

    def test_clear_by_run_id(self):
        api_client = self._api_client_mock()
        api_client.dag_runs.get.return_value = self._dag_run("scheduled__2025-01-01")
        api_client.dag_runs._clear_task_instances.return_value = SimpleNamespace(total_entries=2)

        result = dag_command.clear(
            self.parser.parse_args(
                ["dags", "clear", self.dag_id, "--run-id", "scheduled__2025-01-01", "--yes"]
            ),
            api_client=api_client,
        )

        assert result == {"dag_run_count": 1, "cleared_task_instances": 2}
        api_client.dag_runs.get.assert_called_once_with(
            dag_id=self.dag_id, dag_run_id="scheduled__2025-01-01"
        )
        api_client.dag_runs._clear_task_instances.assert_called_once_with(
            dag_id=self.dag_id,
            dag_run_id="scheduled__2025-01-01",
            dry_run=False,
            only_failed=False,
            only_running=False,
        )

    def test_clear_by_partition_key_filters_exact_match_and_paginates(self):
        api_client = self._api_client_mock()
        api_client.dag_runs.list.side_effect = [
            SimpleNamespace(
                dag_runs=[
                    self._dag_run(
                        "scheduled__2025-01-01",
                        logical_date=datetime.datetime(2025, 1, 1, 0, 0, 0),
                        partition_key="customer-a",
                    ),
                    self._dag_run(
                        "scheduled__2025-01-02",
                        logical_date=datetime.datetime(2025, 1, 2, 0, 0, 0),
                        partition_key="customer-a-suffix",
                    ),
                ],
                total_entries=3,
            ),
            SimpleNamespace(
                dag_runs=[
                    self._dag_run(
                        "scheduled__2025-01-03",
                        logical_date=datetime.datetime(2025, 1, 3, 0, 0, 0),
                        partition_key="customer-a",
                    )
                ],
                total_entries=3,
            ),
        ]
        api_client.dag_runs._clear_task_instances.side_effect = [
            SimpleNamespace(total_entries=1),
            SimpleNamespace(total_entries=2),
        ]

        result = dag_command.clear(
            self.parser.parse_args(["dags", "clear", self.dag_id, "--partition-key", "customer-a", "--yes"]),
            api_client=api_client,
        )

        assert result == {"dag_run_count": 2, "cleared_task_instances": 3}
        assert api_client.dag_runs.list.call_args_list == [
            call(
                dag_id=self.dag_id,
                offset=0,
                order_by="partition_date",
                partition_key_pattern="customer-a",
            ),
            call(
                dag_id=self.dag_id,
                offset=2,
                order_by="partition_date",
                partition_key_pattern="customer-a",
            ),
        ]
        assert api_client.dag_runs._clear_task_instances.call_args_list == [
            call(
                dag_id=self.dag_id,
                dag_run_id="scheduled__2025-01-01",
                dry_run=False,
                only_failed=False,
                only_running=False,
            ),
            call(
                dag_id=self.dag_id,
                dag_run_id="scheduled__2025-01-03",
                dry_run=False,
                only_failed=False,
                only_running=False,
            ),
        ]

    def test_clear_by_partition_date_uses_partition_date_filters(self):
        api_client = self._api_client_mock()
        api_client.dag_runs.list.return_value = SimpleNamespace(
            dag_runs=[self._dag_run("scheduled__2025-01-01")],
            total_entries=1,
        )
        api_client.dag_runs._clear_task_instances.return_value = SimpleNamespace(total_entries=1)

        result = dag_command.clear(
            self.parser.parse_args(
                [
                    "dags",
                    "clear",
                    self.dag_id,
                    "--partition-date-start",
                    "2025-01-01",
                    "--partition-date-end",
                    "2025-01-02",
                    "--only-running",
                    "--yes",
                ]
            ),
            api_client=api_client,
        )

        assert result == {"dag_run_count": 1, "cleared_task_instances": 1}
        api_client.dag_runs.list.assert_called_once_with(
            dag_id=self.dag_id,
            offset=0,
            order_by="partition_date",
            partition_date_start=datetime.date(2025, 1, 1),
            partition_date_end=datetime.date(2025, 1, 2),
        )
        api_client.dag_runs._clear_task_instances.assert_called_once_with(
            dag_id=self.dag_id,
            dag_run_id="scheduled__2025-01-01",
            dry_run=False,
            only_failed=False,
            only_running=True,
        )

    def test_clear_by_partition_date_uses_calendar_dates_from_datetimes(self):
        api_client = self._api_client_mock()
        api_client.dag_runs.list.return_value = SimpleNamespace(
            dag_runs=[self._dag_run("scheduled__2025-01-01")],
            total_entries=1,
        )
        api_client.dag_runs._clear_task_instances.return_value = SimpleNamespace(total_entries=1)

        result = dag_command.clear(
            self.parser.parse_args(
                [
                    "dags",
                    "clear",
                    self.dag_id,
                    "--partition-date-start",
                    "2025-01-01T08:00:00+08:00",
                    "--partition-date-end",
                    "2025-01-02T17:00:00+08:00",
                    "--yes",
                ]
            ),
            api_client=api_client,
        )

        assert result == {"dag_run_count": 1, "cleared_task_instances": 1}
        api_client.dag_runs.list.assert_called_once_with(
            dag_id=self.dag_id,
            offset=0,
            order_by="partition_date",
            partition_date_start=datetime.date(2025, 1, 1),
            partition_date_end=datetime.date(2025, 1, 2),
        )

    def test_clear_by_partition_date_accepts_naive_datetime_as_calendar_date(self):
        api_client = self._api_client_mock()
        api_client.dag_runs.list.return_value = SimpleNamespace(
            dag_runs=[self._dag_run("scheduled__2025-01-01")],
            total_entries=1,
        )
        api_client.dag_runs._clear_task_instances.return_value = SimpleNamespace(total_entries=1)

        result = dag_command.clear(
            self.parser.parse_args(
                [
                    "dags",
                    "clear",
                    self.dag_id,
                    "--partition-date-start",
                    "2025-01-01T00:00:00",
                    "--partition-date-end",
                    "2025-01-02T00:00:00",
                    "--yes",
                ]
            ),
            api_client=api_client,
        )

        assert result == {"dag_run_count": 1, "cleared_task_instances": 1}
        api_client.dag_runs.list.assert_called_once_with(
            dag_id=self.dag_id,
            offset=0,
            order_by="partition_date",
            partition_date_start=datetime.date(2025, 1, 1),
            partition_date_end=datetime.date(2025, 1, 2),
        )

    def test_clear_prompts_before_clearing(self, monkeypatch):
        api_client = self._api_client_mock()
        api_client.dag_runs.get.return_value = self._dag_run("scheduled__2025-01-01")
        monkeypatch.setattr("builtins.input", lambda _: "n")

        result = dag_command.clear(
            self.parser.parse_args(["dags", "clear", self.dag_id, "--run-id", "scheduled__2025-01-01"]),
            api_client=api_client,
        )

        assert result == {"dag_run_count": 1, "cleared_task_instances": 0, "cancelled": True}
        api_client.dag_runs._clear_task_instances.assert_not_called()

    @pytest.mark.parametrize(
        "command",
        [
            ["dags", "clear", dag_id],
            ["dags", "clear", dag_id, "--run-id", "run", "--partition-key", "key"],
            [
                "dags",
                "clear",
                dag_id,
                "--partition-date-start",
                "2025-01-01",
            ],
            [
                "dags",
                "clear",
                dag_id,
                "--run-id",
                "run",
                "--only-failed",
                "--only-running",
            ],
        ],
    )
    def test_clear_validates_selectors(self, command):
        with pytest.raises(SystemExit):
            dag_command.clear(self.parser.parse_args(command), api_client=self._api_client_mock())
