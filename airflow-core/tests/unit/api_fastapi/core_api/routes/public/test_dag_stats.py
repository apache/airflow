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

from airflow._shared.timezones import timezone
from airflow.models.dag import DagModel
from airflow.models.dagrun import DagRun
from airflow.utils.state import DagRunState
from airflow.utils.types import DagRunType

from tests_common.test_utils.asserts import assert_queries_count
from tests_common.test_utils.db import (
    clear_db_dag_bundles,
    clear_db_dags,
    clear_db_runs,
    clear_db_serialized_dags,
)

pytestmark = pytest.mark.db_test

DAG1_ID = "test_dag1"
DAG1_DISPLAY_NAME = "test_dag1"
DAG2_ID = "test_dag2"
DAG2_DISPLAY_NAME = "test_dag2"
DAG3_ID = "test_dag3"
DAG3_DISPLAY_NAME = "test_dag3"
TASK_ID = "op1"
API_PREFIX = "/dagStats"


class TestDagStatsEndpoint:
    default_time = "2020-06-11T18:00:00+00:00"

    @staticmethod
    def _clear_db():
        clear_db_runs()
        clear_db_dags()
        clear_db_dag_bundles()
        clear_db_serialized_dags()

    def _create_dag_and_runs(self, session=None):
        dag_1 = DagModel(
            dag_id=DAG1_ID,
            bundle_name="testing",
            fileloc="/tmp/dag_stats_1.py",
            timetable_summary="2 2 * * *",
            is_stale=True,
            is_paused=True,
            owners="test_owner,another_test_owner",
            next_dagrun=datetime(2021, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
        )
        dag_1._dag_display_property_value = DAG1_DISPLAY_NAME
        dag_1_run_1 = DagRun(
            dag_id=DAG1_ID,
            run_id="test_dag_run_id_1",
            run_type=DagRunType.MANUAL,
            logical_date=timezone.parse(self.default_time),
            start_date=timezone.parse(self.default_time),
            state="running",
        )
        dag_1_run_2 = DagRun(
            dag_id=dag_1.dag_id,
            run_id="test_dag_run_id_2",
            run_type=DagRunType.MANUAL,
            logical_date=timezone.parse(self.default_time) + timedelta(days=1),
            start_date=timezone.parse(self.default_time),
            state="failed",
        )
        dag_2 = DagModel(
            dag_id=DAG2_ID,
            bundle_name="testing",
            fileloc="/tmp/dag_stats_2.py",
            timetable_summary="2 2 * * *",
            is_stale=True,
            is_paused=True,
            owners="test_owner,another_test_owner",
            next_dagrun=datetime(2021, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
        )
        dag_2._dag_display_property_value = DAG2_DISPLAY_NAME
        dag_2_run_1 = DagRun(
            dag_id=dag_2.dag_id,
            run_id="test_dag_2_run_id_1",
            run_type=DagRunType.MANUAL,
            logical_date=timezone.parse(self.default_time),
            start_date=timezone.parse(self.default_time),
            state="queued",
        )
        dag_3 = DagModel(
            dag_id=DAG3_ID,
            bundle_name="testing",
            fileloc="/tmp/dag_stats_3.py",
            timetable_summary="2 2 * * *",
            is_stale=True,
            is_paused=True,
            owners="test_owner,another_test_owner",
            next_dagrun=datetime(2021, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
        )
        dag_3._dag_display_property_value = DAG3_DISPLAY_NAME
        dag_3_run_1 = DagRun(
            dag_id=dag_3.dag_id,
            run_id="test_dag_3_run_id_1",
            run_type=DagRunType.MANUAL,
            logical_date=timezone.parse(self.default_time),
            start_date=timezone.parse(self.default_time),
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

    def test_should_respond_200(self, test_client, session, testing_dag_bundle):
        self._create_dag_and_runs(session)
        exp_payload = {
            "dags": [
                {
                    "dag_id": DAG1_ID,
                    "dag_display_name": DAG1_DISPLAY_NAME,
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
                    "dag_display_name": DAG2_DISPLAY_NAME,
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

        with assert_queries_count(2):
            response = test_client.get(f"{API_PREFIX}?dag_ids={DAG1_ID}&dag_ids={DAG2_ID}")
        assert response.status_code == 200
        res_json = response.json()
        assert res_json["total_entries"] == len(res_json["dags"])
        assert res_json == exp_payload

    def test_should_respond_401(self, unauthenticated_test_client):
        response = unauthenticated_test_client.get(f"{API_PREFIX}?dag_ids={DAG1_ID}&dag_ids={DAG2_ID}")
        assert response.status_code == 401

    def test_should_respond_403(self, unauthorized_test_client):
        response = unauthorized_test_client.get(f"{API_PREFIX}?dag_ids={DAG1_ID}&dag_ids={DAG2_ID}")
        assert response.status_code == 403

    def test_all_dags_should_respond_200(self, test_client, session, testing_dag_bundle):
        self._create_dag_and_runs(session)
        exp_payload = {
            "dags": [
                {
                    "dag_id": DAG1_ID,
                    "dag_display_name": DAG1_DISPLAY_NAME,
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
                    "dag_display_name": DAG2_DISPLAY_NAME,
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
                    "dag_display_name": DAG3_DISPLAY_NAME,
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

        with assert_queries_count(2):
            response = test_client.get(API_PREFIX)
        assert response.status_code == 200
        res_json = response.json()
        assert res_json["total_entries"] == len(res_json["dags"])
        assert res_json == exp_payload

    @pytest.mark.parametrize(
        ("url", "params", "exp_payload"),
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
                            "dag_display_name": DAG1_DISPLAY_NAME,
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
                            "dag_display_name": DAG2_DISPLAY_NAME,
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
                            "dag_display_name": DAG3_DISPLAY_NAME,
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
                            "dag_display_name": DAG1_DISPLAY_NAME,
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
                            "dag_display_name": DAG3_DISPLAY_NAME,
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
    def test_single_dag_in_dag_ids(self, test_client, session, testing_dag_bundle, url, params, exp_payload):
        self._create_dag_and_runs(session)

        with assert_queries_count(2):
            response = test_client.get(url, params=params)
        assert response.status_code == 200
        res_json = response.json()
        assert res_json["total_entries"] == len(res_json["dags"])
        assert res_json == exp_payload
