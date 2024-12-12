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

from unittest import mock

import pytest

from airflow.models import DagModel
from airflow.models.backfill import Backfill
from airflow.utils import timezone
from airflow.utils.session import provide_session

from tests_common.test_utils.db import (
    clear_db_backfills,
    clear_db_dags,
    clear_db_runs,
    clear_db_serialized_dags,
)
from tests_common.test_utils.format_datetime import from_datetime_to_zulu

pytestmark = pytest.mark.db_test

DAG_ID = "test_dag"
TASK_ID = "op1"
DAG2_ID = "test_dag2"
DAG3_ID = "test_dag3"


def _clean_db():
    clear_db_backfills()
    clear_db_runs()
    clear_db_dags()
    clear_db_serialized_dags()


@pytest.fixture(autouse=True)
def clean_db():
    _clean_db()
    yield
    _clean_db()


class TestBackfillEndpoint:
    @provide_session
    def _create_dag_models(self, *, count=3, dag_id_prefix="TEST_DAG", is_paused=False, session=None):
        dags = []
        for num in range(1, count + 1):
            dag_model = DagModel(
                dag_id=f"{dag_id_prefix}_{num}",
                fileloc=f"/tmp/dag_{num}.py",
                is_active=True,
                timetable_summary="0 0 * * *",
                is_paused=is_paused,
            )
            session.add(dag_model)
            dags.append(dag_model)
        return dags


class TestListBackfills(TestBackfillEndpoint):
    @pytest.mark.parametrize(
        "test_params, response_params, total_entries",
        [
            ({}, ["backfill1", "backfill2", "backfill3"], 3),
            ({"active": True}, ["backfill2", "backfill3"], 2),
            ({"active": False}, ["backfill1"], 1),
            ({"dag_id": "", "active": True}, ["backfill2", "backfill3"], 2),
            ({"dag_id": "", "active": False}, ["backfill1"], 1),
            ({"dag_id": ""}, ["backfill1", "backfill2", "backfill3"], 3),
            ({"dag_id": "TEST_DAG_1", "active": True}, [], 0),
            ({"dag_id": "TEST_DAG_1", "active": False}, ["backfill1"], 1),
            ({"dag_id": "TEST_DAG_1"}, ["backfill1"], 1),
        ],
    )
    def test_list_backfill(self, test_params, response_params, total_entries, test_client, session):
        dags = self._create_dag_models()
        from_date = timezone.utcnow()
        to_date = timezone.utcnow()
        completed_at = timezone.utcnow()
        backfill0 = Backfill(
            dag_id=dags[0].dag_id, from_date=from_date, to_date=to_date, completed_at=completed_at
        )
        backfill1 = Backfill(dag_id=dags[1].dag_id, from_date=from_date, to_date=to_date)
        backfill2 = Backfill(dag_id=dags[2].dag_id, from_date=from_date, to_date=to_date, is_paused=True)
        backfills = [backfill0, backfill1, backfill2]
        session.add_all(backfills)
        session.commit()
        backfill_responses = {
            "backfill1": {
                "completed_at": from_datetime_to_zulu(completed_at),
                "created_at": mock.ANY,
                "dag_id": "TEST_DAG_1",
                "dag_run_conf": {},
                "from_date": from_datetime_to_zulu(from_date),
                "id": backfills[0].id,
                "is_paused": False,
                "reprocess_behavior": "none",
                "max_active_runs": 10,
                "to_date": from_datetime_to_zulu(to_date),
                "updated_at": mock.ANY,
            },
            "backfill2": {
                "completed_at": None,
                "created_at": mock.ANY,
                "dag_id": "TEST_DAG_2",
                "dag_run_conf": {},
                "from_date": from_datetime_to_zulu(from_date),
                "id": backfills[1].id,
                "is_paused": False,
                "reprocess_behavior": "none",
                "max_active_runs": 10,
                "to_date": from_datetime_to_zulu(to_date),
                "updated_at": mock.ANY,
            },
            "backfill3": {
                "completed_at": None,
                "created_at": mock.ANY,
                "dag_id": "TEST_DAG_3",
                "dag_run_conf": {},
                "from_date": from_datetime_to_zulu(from_date),
                "id": backfills[2].id,
                "is_paused": True,
                "reprocess_behavior": "none",
                "max_active_runs": 10,
                "to_date": from_datetime_to_zulu(to_date),
                "updated_at": mock.ANY,
            },
        }
        expected_response = []
        for backfill in response_params:
            expected_response.append(backfill_responses[backfill])
        response = test_client.get("/ui/backfills", params=test_params)
        assert response.status_code == 200
        assert response.json() == {
            "backfills": expected_response,
            "total_entries": total_entries,
        }
