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

from datetime import datetime, timedelta

import pytest

from airflow.models.dag import DagModel
from airflow.models.dagrun import DagRun
from airflow.utils import timezone
from airflow.utils.state import DagRunState
from airflow.utils.types import DagRunType

from tests_common.test_utils.db import clear_db_dags, clear_db_runs, clear_db_serialized_dags

pytestmark = pytest.mark.db_test

DAG1_ID = "test_dag1"
DAG2_ID = "test_dag2"
DAG3_ID = "test_dag3"
TASK_ID = "op1"
API_PREFIX = "/public/dagStats"


class TestDagStatsEndpoint:
    default_time = "2020-06-11T18:00:00+00:00"

    @staticmethod
    def _clear_db():
        clear_db_runs()
        clear_db_dags()
        clear_db_serialized_dags()

    def _create_dag_and_runs(self, session=None):
        dag_1 = DagModel(
            dag_id=DAG1_ID,
            fileloc="/tmp/dag_stats_1.py",
            timetable_summary="2 2 * * *",
            is_active=False,
            is_paused=True,
            owners="test_owner,another_test_owner",
            next_dagrun=datetime(2021, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
        )
        dag_1_run_1 = DagRun(
            dag_id=DAG1_ID,
            run_id="test_dag_run_id_1",
            run_type=DagRunType.MANUAL,
            logical_date=timezone.parse(self.default_time),
            start_date=timezone.parse(self.default_time),
            external_trigger=True,
            state="running",
        )
        dag_1_run_2 = DagRun(
            dag_id=dag_1.dag_id,
            run_id="test_dag_run_id_2",
            run_type=DagRunType.MANUAL,
            logical_date=timezone.parse(self.default_time) + timedelta(days=1),
            start_date=timezone.parse(self.default_time),
            external_trigger=True,
            state="failed",
        )
        dag_2 = DagModel(
            dag_id=DAG2_ID,
            fileloc="/tmp/dag_stats_2.py",
            timetable_summary="2 2 * * *",
            is_active=False,
            is_paused=True,
            owners="test_owner,another_test_owner",
            next_dagrun=datetime(2021, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
        )
        dag_2_run_1 = DagRun(
            dag_id=dag_2.dag_id,
            run_id="test_dag_2_run_id_1",
            run_type=DagRunType.MANUAL,
            logical_date=timezone.parse(self.default_time),
            start_date=timezone.parse(self.default_time),
            external_trigger=True,
            state="queued",
        )
        dag_3 = DagModel(
            dag_id=DAG3_ID,
            fileloc="/tmp/dag_stats_3.py",
            timetable_summary="2 2 * * *",
            is_active=False,
            is_paused=True,
            owners="test_owner,another_test_owner",
            next_dagrun=datetime(2021, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
        )
        dag_3_run_1 = DagRun(
            dag_id=dag_3.dag_id,
            run_id="test_dag_3_run_id_1",
            run_type=DagRunType.MANUAL,
            logical_date=timezone.parse(self.default_time),
            start_date=timezone.parse(self.default_time),
            external_trigger=True,
            state="success",
        )
        entities = (
            dag_1,
            dag_1_run_1,
            dag_1_run_2,
            dag_2,
            dag_2_run_1,
            dag_3,
            dag_3_run_1,
        )
        session.add_all(entities)
        session.commit()

    @pytest.fixture(autouse=True)
    def setup(self) -> None:
        self._clear_db()

    def teardown_method(self) -> None:
        self._clear_db()


class TestGetDagStats(TestDagStatsEndpoint):
    """Unit tests for Get DAG Stats."""

    def test_should_respond_200(self, client, session):
        self._create_dag_and_runs(session)
        exp_payload = {
            "dags": [
                {
                    "dag_id": DAG1_ID,
                    "stats": [
                        {
                            "state": DagRunState.QUEUED,
                            "count": 0,
                        },
                        {
                            "state": DagRunState.RUNNING,
                            "count": 1,
                        },
                        {
                            "state": DagRunState.SUCCESS,
                            "count": 0,
                        },
                        {
                            "state": DagRunState.FAILED,
                            "count": 1,
                        },
                    ],
                },
                {
                    "dag_id": DAG2_ID,
                    "stats": [
                        {
                            "state": DagRunState.QUEUED,
                            "count": 1,
                        },
                        {
                            "state": DagRunState.RUNNING,
                            "count": 0,
                        },
                        {
                            "state": DagRunState.SUCCESS,
                            "count": 0,
                        },
                        {
                            "state": DagRunState.FAILED,
                            "count": 0,
                        },
                    ],
                },
            ],
            "total_entries": 2,
        }

        response = client().get(f"{API_PREFIX}?dag_ids={DAG1_ID}&dag_ids={DAG2_ID}")
        assert response.status_code == 200
        res_json = response.json()
        assert res_json["total_entries"] == len(res_json["dags"])
        assert res_json == exp_payload

    def test_all_dags_should_respond_200(self, client, session):
        self._create_dag_and_runs(session)
        exp_payload = {
            "dags": [
                {
                    "dag_id": DAG1_ID,
                    "stats": [
                        {
                            "state": DagRunState.QUEUED,
                            "count": 0,
                        },
                        {
                            "state": DagRunState.RUNNING,
                            "count": 1,
                        },
                        {
                            "state": DagRunState.SUCCESS,
                            "count": 0,
                        },
                        {
                            "state": DagRunState.FAILED,
                            "count": 1,
                        },
                    ],
                },
                {
                    "dag_id": DAG2_ID,
                    "stats": [
                        {
                            "state": DagRunState.QUEUED,
                            "count": 1,
                        },
                        {
                            "state": DagRunState.RUNNING,
                            "count": 0,
                        },
                        {
                            "state": DagRunState.SUCCESS,
                            "count": 0,
                        },
                        {
                            "state": DagRunState.FAILED,
                            "count": 0,
                        },
                    ],
                },
                {
                    "dag_id": DAG3_ID,
                    "stats": [
                        {
                            "state": DagRunState.QUEUED,
                            "count": 0,
                        },
                        {
                            "state": DagRunState.RUNNING,
                            "count": 0,
                        },
                        {
                            "state": DagRunState.SUCCESS,
                            "count": 1,
                        },
                        {
                            "state": DagRunState.FAILED,
                            "count": 0,
                        },
                    ],
                },
            ],
            "total_entries": 3,
        }

        response = client().get(API_PREFIX)
        assert response.status_code == 200
        res_json = response.json()
        assert res_json["total_entries"] == len(res_json["dags"])
        assert res_json == exp_payload

    @pytest.mark.parametrize(
        "url, params, exp_payload",
        [
            (
                API_PREFIX,
                [
                    ("dag_ids", DAG1_ID),
                    ("dag_ids", DAG3_ID),
                    ("dag_ids", DAG2_ID),
                ],
                {
                    "dags": [
                        {
                            "dag_id": DAG1_ID,
                            "stats": [
                                {
                                    "state": DagRunState.QUEUED,
                                    "count": 0,
                                },
                                {
                                    "state": DagRunState.RUNNING,
                                    "count": 1,
                                },
                                {
                                    "state": DagRunState.SUCCESS,
                                    "count": 0,
                                },
                                {
                                    "state": DagRunState.FAILED,
                                    "count": 1,
                                },
                            ],
                        },
                        {
                            "dag_id": DAG2_ID,
                            "stats": [
                                {
                                    "state": DagRunState.QUEUED,
                                    "count": 1,
                                },
                                {
                                    "state": DagRunState.RUNNING,
                                    "count": 0,
                                },
                                {
                                    "state": DagRunState.SUCCESS,
                                    "count": 0,
                                },
                                {
                                    "state": DagRunState.FAILED,
                                    "count": 0,
                                },
                            ],
                        },
                        {
                            "dag_id": DAG3_ID,
                            "stats": [
                                {
                                    "state": DagRunState.QUEUED,
                                    "count": 0,
                                },
                                {
                                    "state": DagRunState.RUNNING,
                                    "count": 0,
                                },
                                {
                                    "state": DagRunState.SUCCESS,
                                    "count": 1,
                                },
                                {
                                    "state": DagRunState.FAILED,
                                    "count": 0,
                                },
                            ],
                        },
                    ],
                    "total_entries": 3,
                },
            ),
            (
                API_PREFIX,
                [("dag_ids", DAG1_ID)],
                {
                    "dags": [
                        {
                            "dag_id": DAG1_ID,
                            "stats": [
                                {
                                    "state": DagRunState.QUEUED,
                                    "count": 0,
                                },
                                {
                                    "state": DagRunState.RUNNING,
                                    "count": 1,
                                },
                                {
                                    "state": DagRunState.SUCCESS,
                                    "count": 0,
                                },
                                {
                                    "state": DagRunState.FAILED,
                                    "count": 1,
                                },
                            ],
                        }
                    ],
                    "total_entries": 1,
                },
            ),
            (
                API_PREFIX,
                [("dag_ids", DAG3_ID)],
                {
                    "dags": [
                        {
                            "dag_id": DAG3_ID,
                            "stats": [
                                {
                                    "state": DagRunState.QUEUED,
                                    "count": 0,
                                },
                                {
                                    "state": DagRunState.RUNNING,
                                    "count": 0,
                                },
                                {
                                    "state": DagRunState.SUCCESS,
                                    "count": 1,
                                },
                                {
                                    "state": DagRunState.FAILED,
                                    "count": 0,
                                },
                            ],
                        },
                    ],
                    "total_entries": 1,
                },
            ),
        ],
    )
    def test_single_dag_in_dag_ids(self, client, session, url, params, exp_payload):
        self._create_dag_and_runs(session)
        response = client().get(url, params=params)
        assert response.status_code == 200
        res_json = response.json()
        assert res_json["total_entries"] == len(res_json["dags"])
        assert res_json == exp_payload
