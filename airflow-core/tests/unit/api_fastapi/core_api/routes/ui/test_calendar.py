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

from datetime import datetime

import pytest

from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.utils.session import provide_session
from airflow.utils.state import DagRunState

from tests_common.test_utils.asserts import assert_queries_count
from tests_common.test_utils.db import clear_db_dags, clear_db_runs

pytestmark = pytest.mark.db_test


class TestCalendar:
    DAG_NAME = "test_dag1"

    @pytest.fixture(autouse=True)
    @provide_session
    def setup_dag_runs(self, dag_maker, session=None) -> None:
        clear_db_runs()
        clear_db_dags()
        with dag_maker(
            self.DAG_NAME,
            schedule="0 0,1 * * *",
            start_date=datetime(2025, 1, 1),
            end_date=datetime(2025, 1, 3, 2),
            catchup=True,
            serialized=True,
            session=session,
        ):
            EmptyOperator(task_id="test_task1")
        dag_maker.create_dagrun(run_id="run_1", state=DagRunState.FAILED, logical_date=datetime(2025, 1, 1))
        dag_maker.create_dagrun(
            run_id="run_2",
            state=DagRunState.SUCCESS,
            logical_date=datetime(2025, 1, 1, 1),
        )
        dag_maker.create_dagrun(run_id="run_3", state=DagRunState.RUNNING, logical_date=datetime(2025, 1, 2))

        dag_maker.sync_dagbag_to_db()

        session.commit()

    def teardown_method(self) -> None:
        clear_db_runs()
        clear_db_dags()

    @pytest.mark.parametrize(
        ("query_params", "result"),
        [
            (
                {},
                {
                    "total_entries": 5,
                    "dag_runs": [
                        {"date": "2025-01-01T00:00:00Z", "state": "failed", "count": 1},
                        {"date": "2025-01-01T00:00:00Z", "state": "success", "count": 1},
                        {"date": "2025-01-02T00:00:00Z", "state": "running", "count": 1},
                        {"date": "2025-01-02T00:00:00Z", "state": "planned", "count": 1},
                        {"date": "2025-01-03T00:00:00Z", "state": "planned", "count": 2},
                    ],
                },
            ),
            (
                {"logical_date_gte": "2025-01-01T00:00:00Z", "logical_date_lte": "2025-01-01T23:23:59Z"},
                {
                    "total_entries": 2,
                    "dag_runs": [
                        {"date": "2025-01-01T00:00:00Z", "state": "failed", "count": 1},
                        {"date": "2025-01-01T00:00:00Z", "state": "success", "count": 1},
                    ],
                },
            ),
            (
                {"logical_date_gte": "2025-01-02T00:00:00Z", "logical_date_lte": "2025-01-02T23:23:59Z"},
                {
                    "total_entries": 2,
                    "dag_runs": [
                        {"date": "2025-01-02T00:00:00Z", "state": "running", "count": 1},
                        {"date": "2025-01-02T00:00:00Z", "state": "planned", "count": 1},
                    ],
                },
            ),
        ],
    )
    def test_daily_calendar(self, test_client, query_params, result):
        with assert_queries_count(4):
            response = test_client.get(f"/calendar/{self.DAG_NAME}", params=query_params)
        assert response.status_code == 200
        body = response.json()
        print(body)

        assert body == result

    @pytest.mark.parametrize(
        ("query_params", "result"),
        [
            (
                {"granularity": "hourly"},
                {
                    "total_entries": 6,
                    "dag_runs": [
                        {"date": "2025-01-01T00:00:00Z", "state": "failed", "count": 1},
                        {"date": "2025-01-01T01:00:00Z", "state": "success", "count": 1},
                        {"date": "2025-01-02T00:00:00Z", "state": "running", "count": 1},
                        {"date": "2025-01-02T01:00:00Z", "state": "planned", "count": 1},
                        {"date": "2025-01-03T00:00:00Z", "state": "planned", "count": 1},
                        {"date": "2025-01-03T01:00:00Z", "state": "planned", "count": 1},
                    ],
                },
            ),
            (
                {
                    "granularity": "hourly",
                    "logical_date_gte": "2025-01-02T00:00:00Z",
                    "logical_date_lte": "2025-01-02T23:23:59Z",
                },
                {
                    "total_entries": 2,
                    "dag_runs": [
                        {"date": "2025-01-02T00:00:00Z", "state": "running", "count": 1},
                        {"date": "2025-01-02T01:00:00Z", "state": "planned", "count": 1},
                    ],
                },
            ),
            (
                {
                    "granularity": "hourly",
                    "logical_date_gte": "2025-01-02T00:00:00Z",
                    "logical_date_lte": "2025-01-02T23:23:59Z",
                    "logical_date_gt": "2025-01-02T00:00:00Z",
                    "logical_date_lt": "2025-01-02T23:23:59Z",
                },
                {
                    "total_entries": 0,
                    "dag_runs": [],
                },
            ),
            (
                {
                    "granularity": "hourly",
                    "logical_date_gte": "2025-01-02T00:00:00Z",
                    "logical_date_lte": "2025-01-02T23:23:59Z",
                    "logical_date_gt": "2025-01-01T23:00:00Z",
                    "logical_date_lt": "2025-01-03T00:00:00Z",
                },
                {
                    "total_entries": 2,
                    "dag_runs": [
                        {"date": "2025-01-02T00:00:00Z", "state": "running", "count": 1},
                        {"date": "2025-01-02T01:00:00Z", "state": "planned", "count": 1},
                    ],
                },
            ),
        ],
    )
    def test_hourly_calendar(self, setup_dag_runs, test_client, query_params, result):
        with assert_queries_count(4):
            response = test_client.get(f"/calendar/{self.DAG_NAME}", params=query_params)
        assert response.status_code == 200
        body = response.json()

        assert body == result
