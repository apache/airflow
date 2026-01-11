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

from datetime import datetime, timezone

import pendulum
import pytest

from airflow.models.dag import DagModel, DagTag
from airflow.models.dagrun import DagRun
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.utils.session import provide_session
from airflow.utils.state import DagRunState
from airflow.utils.types import DagRunTriggeredByType, DagRunType

from tests_common.test_utils.asserts import assert_queries_count
from tests_common.test_utils.db import (
    clear_db_dag_bundles,
    clear_db_dags,
    clear_db_runs,
    clear_db_serialized_dags,
)

pytestmark = pytest.mark.db_test

DAG1_ID = "test_dag1"
DAG1_DISPLAY_NAME = "display1"
DAG2_ID = "test_dag2"
DAG2_START_DATE = datetime(2021, 6, 15, tzinfo=timezone.utc)
DAG3_ID = "test_dag3"
DAG4_ID = "test_dag4"
DAG4_DISPLAY_NAME = "display4"
DAG5_ID = "test_dag5"
DAG5_DISPLAY_NAME = "display5"
TASK_ID = "op1"
UTC_JSON_REPR = "UTC" if pendulum.__version__.startswith("3") else "Timezone('UTC')"
API_PREFIX = "/dags"


class TestDagEndpoint:
    """Common class for /dags related unit tests."""

    @staticmethod
    def _clear_db():
        clear_db_runs()
        clear_db_dags()
        clear_db_dag_bundles()
        clear_db_serialized_dags()

    def _create_deactivated_paused_dag(self, session=None):
        dag_model = DagModel(
            dag_id=DAG3_ID,
            bundle_name="dag_maker",
            fileloc="/tmp/dag_del_1.py",
            timetable_summary="2 2 * * *",
            is_stale=True,
            is_paused=True,
            owners="test_owner,another_test_owner",
            next_dagrun=datetime(2021, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
        )

        dagrun_failed = DagRun(
            dag_id=DAG3_ID,
            run_id="run1",
            logical_date=datetime(2018, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
            start_date=datetime(2018, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
            run_type=DagRunType.SCHEDULED,
            state=DagRunState.FAILED,
            triggered_by=DagRunTriggeredByType.TEST,
        )

        dagrun_success = DagRun(
            dag_id=DAG3_ID,
            run_id="run2",
            logical_date=datetime(2019, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
            start_date=datetime(2019, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
            run_type=DagRunType.MANUAL,
            state=DagRunState.SUCCESS,
            triggered_by=DagRunTriggeredByType.TEST,
        )

        session.add(dag_model)
        session.add(dagrun_failed)
        session.add(dagrun_success)

    def _create_dag_tags(self, session=None):
        session.add(DagTag(dag_id=DAG1_ID, name="tag_2"))
        session.add(DagTag(dag_id=DAG2_ID, name="tag_1"))
        session.add(DagTag(dag_id=DAG3_ID, name="tag_1"))

    @pytest.fixture(autouse=True)
    @provide_session
    def setup(self, dag_maker, session=None) -> None:
        self._clear_db()

        with dag_maker(
            DAG1_ID,
            dag_display_name=DAG1_DISPLAY_NAME,
            schedule=None,
            start_date=datetime(2018, 6, 15, 0, 0, tzinfo=timezone.utc),
            doc_md="details",
            params={"foo": 1},
            tags=["example"],
        ):
            EmptyOperator(task_id=TASK_ID)

        dag_maker.sync_dagbag_to_db()
        dag_maker.create_dagrun(state=DagRunState.FAILED)

        with dag_maker(
            DAG2_ID,
            schedule=None,
            start_date=DAG2_START_DATE,
            doc_md="details",
            params={"foo": 1},
            max_active_tasks=16,
            max_active_runs=16,
        ):
            EmptyOperator(task_id=TASK_ID)

        self._create_deactivated_paused_dag(session)
        self._create_dag_tags(session)

        dag_maker.sync_dagbag_to_db()
        dag_maker.dag_model.has_task_concurrency_limits = True
        session.merge(dag_maker.dag_model)
        session.commit()

    def teardown_method(self) -> None:
        self._clear_db()


class TestDagTags(TestDagEndpoint):
    """Unit tests for Get DAG Tags."""

    @pytest.mark.parametrize(
        ("query_params", "expected_status_code", "expected_dag_tags", "expected_total_entries"),
        [
            # test with offset, limit, and without any tag_name_pattern
            (
                {},
                200,
                [
                    "example",
                    "tag_1",
                    "tag_2",
                ],
                3,
            ),
            (
                {"offset": 1},
                200,
                [
                    "tag_1",
                    "tag_2",
                ],
                3,
            ),
            (
                {"limit": 2},
                200,
                [
                    "example",
                    "tag_1",
                ],
                3,
            ),
            (
                {"offset": 1, "limit": 2},
                200,
                [
                    "tag_1",
                    "tag_2",
                ],
                3,
            ),
            # test with tag_name_pattern
            (
                {"tag_name_pattern": "invalid"},
                200,
                [],
                0,
            ),
            (
                {"tag_name_pattern": "1"},
                200,
                ["tag_1"],
                1,
            ),
            (
                {"tag_name_pattern": "tag%"},
                200,
                ["tag_1", "tag_2"],
                2,
            ),
            # test order_by
            (
                {"order_by": "-name"},
                200,
                ["tag_2", "tag_1", "example"],
                3,
            ),
            # test all query params
            (
                {"tag_name_pattern": "t%", "order_by": "-name", "offset": 1, "limit": 1},
                200,
                ["tag_1"],
                2,
            ),
            (
                {"tag_name_pattern": "~", "offset": 1, "limit": 2},
                200,
                ["tag_1", "tag_2"],
                3,
            ),
            # test invalid query params
            (
                {"order_by": "dag_id"},
                400,
                None,
                None,
            ),
            (
                {"order_by": "-dag_id"},
                400,
                None,
                None,
            ),
        ],
    )
    def test_get_dag_tags(
        self, test_client, query_params, expected_status_code, expected_dag_tags, expected_total_entries
    ):
        with assert_queries_count(3 if expected_status_code == 200 else 2):
            response = test_client.get("/dagTags", params=query_params)
        assert response.status_code == expected_status_code
        if expected_status_code != 200:
            return

        res_json = response.json()
        expected = {
            "tags": expected_dag_tags,
            "total_entries": expected_total_entries,
        }
        assert res_json == expected

    def test_should_respond_401(self, unauthenticated_test_client):
        response = unauthenticated_test_client.get("/dagTags")
        assert response.status_code == 401

    def test_should_respond_403(self, unauthorized_test_client):
        response = unauthorized_test_client.get("/dagTags")
        assert response.status_code == 403
