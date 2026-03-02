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
