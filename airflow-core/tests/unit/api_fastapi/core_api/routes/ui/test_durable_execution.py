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

import json

import pytest
from sqlalchemy import select

from airflow._shared.timezones import timezone
from airflow.models.dagrun import DagRun
from airflow.models.task_state_store import TaskStateStoreModel
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.utils.types import DagRunType

from tests_common.test_utils.db import clear_db_dag_bundles, clear_db_dags, clear_db_runs

pytestmark = pytest.mark.db_test

DAG_ID = "test_dag"
TASK_ID = "test_task"
LOGICAL_DATE = timezone.datetime(2026, 1, 1)
RUN_ID = DagRun.generate_run_id(run_type=DagRunType.MANUAL, logical_date=LOGICAL_DATE, run_after=LOGICAL_DATE)
URL = f"/dags/{DAG_ID}/dagRuns/{RUN_ID}/taskInstances/{TASK_ID}/durable"

RECORD = {
    "tries": [
        {
            "try_number": 1,
            "decision": "submitted",
            "external_id": "job-1",
            "status": None,
            "submitted_at": "2026-01-01T00:00:00+00:00",
            "saved_seconds": None,
        },
        {
            "try_number": 2,
            "decision": "reconnected",
            "external_id": "job-1",
            "status": "success",
            "submitted_at": "2026-01-01T00:00:00+00:00",
            "saved_seconds": 123.0,
        },
    ],
}


class TestGetDurableExecution:
    @pytest.fixture(autouse=True)
    def setup(self, dag_maker, session):
        with dag_maker(DAG_ID, schedule=None, start_date=LOGICAL_DATE):
            EmptyOperator(task_id=TASK_ID)
        dag_maker.create_dagrun(run_id=RUN_ID, run_type=DagRunType.MANUAL, logical_date=LOGICAL_DATE)
        dag_maker.sync_dagbag_to_db()
        session.merge(dag_maker.dag_model)
        session.commit()
        self.dag_run = session.scalar(select(DagRun).where(DagRun.run_id == RUN_ID))
        self.session = session
        yield
        clear_db_dags()
        clear_db_runs()
        clear_db_dag_bundles()

    def _store(self, key: str, value: object) -> None:
        self.session.add(
            TaskStateStoreModel(
                dag_run_id=self.dag_run.id,
                dag_id=DAG_ID,
                run_id=RUN_ID,
                task_id=TASK_ID,
                region_index=-1,
                key=key,
                value=json.dumps(value),
            )
        )
        self.session.commit()

    def test_returns_typed_record(self, test_client):
        self._store("__airflow_durable__", RECORD)

        response = test_client.get(URL)

        assert response.status_code == 200
        assert response.json() == {
            "tries": [
                {**t, "submitted_at": "2026-01-01T00:00:00Z", "previous_external_id": None}
                for t in RECORD["tries"]
            ],
        }

    def test_returns_empty_when_task_not_durable(self, test_client):
        self._store("job_id", "job-1")

        response = test_client.get(URL)

        assert response.status_code == 200
        assert response.json() == {"tries": []}

    @pytest.mark.parametrize(
        "value",
        [
            pytest.param({"__airflow_state_ref__": "s3://bucket/key"}, id="offloaded-ref"),
            pytest.param({"tries": "not-a-list"}, id="invalid-record"),
        ],
    )
    def test_returns_empty_when_record_unreadable(self, test_client, value):
        self._store("__airflow_durable__", value)

        response = test_client.get(URL)

        assert response.status_code == 200
        assert response.json() == {"tries": []}

    def test_unauthorized_returns_401(self, unauthenticated_test_client):
        assert unauthenticated_test_client.get(URL).status_code == 401
