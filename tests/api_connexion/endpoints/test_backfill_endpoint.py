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

import os
from datetime import datetime
from unittest import mock
from urllib.parse import urlencode

import pendulum
import pytest

from airflow.models import DagBag, DagModel
from airflow.models.backfill import Backfill
from airflow.models.dag import DAG
from airflow.models.serialized_dag import SerializedDagModel
from airflow.operators.empty import EmptyOperator
from airflow.utils import timezone
from airflow.utils.session import provide_session
from tests.test_utils.api_connexion_utils import create_user, delete_user
from tests.test_utils.db import clear_db_backfills, clear_db_dags, clear_db_runs, clear_db_serialized_dags

pytestmark = [pytest.mark.db_test]


DAG_ID = "test_dag"
TASK_ID = "op1"
DAG2_ID = "test_dag2"
DAG3_ID = "test_dag3"
UTC_JSON_REPR = "UTC" if pendulum.__version__.startswith("3") else "Timezone('UTC')"


@pytest.fixture(scope="module")
def configured_app(minimal_app_for_api):
    app = minimal_app_for_api

    create_user(
        app,
        username="test",
        role_name="admin",
    )
    create_user(app, username="test_no_permissions", role_name=None)

    with DAG(
        DAG_ID,
        schedule=None,
        start_date=datetime(2020, 6, 15),
        doc_md="details",
        params={"foo": 1},
        tags=["example"],
    ) as dag:
        EmptyOperator(task_id=TASK_ID)

    with DAG(DAG2_ID, schedule=None, start_date=datetime(2020, 6, 15)) as dag2:  # no doc_md
        EmptyOperator(task_id=TASK_ID)

    with DAG(DAG3_ID, schedule=None) as dag3:  # DAG start_date set to None
        EmptyOperator(task_id=TASK_ID, start_date=datetime(2019, 6, 12))

    dag_bag = DagBag(os.devnull, include_examples=False)
    dag_bag.dags = {dag.dag_id: dag, dag2.dag_id: dag2, dag3.dag_id: dag3}

    app.dag_bag = dag_bag

    yield app

    delete_user(app, username="test")
    delete_user(app, username="test_no_permissions")


class TestBackfillEndpoint:
    @staticmethod
    def clean_db():
        clear_db_backfills()
        clear_db_runs()
        clear_db_dags()
        clear_db_serialized_dags()

    @pytest.fixture(autouse=True)
    def setup_attrs(self, configured_app) -> None:
        self.clean_db()
        self.app = configured_app
        self.client = self.app.test_client()  # type:ignore
        self.dag_id = DAG_ID
        self.dag2_id = DAG2_ID
        self.dag3_id = DAG3_ID

    def teardown_method(self) -> None:
        self.clean_db()

    @provide_session
    def _create_dag_models(self, *, count=1, dag_id_prefix="TEST_DAG", is_paused=False, session=None):
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

    @provide_session
    def _create_deactivated_dag(self, session=None):
        dag_model = DagModel(
            dag_id="TEST_DAG_DELETED_1",
            fileloc="/tmp/dag_del_1.py",
            schedule_interval="2 2 * * *",
            is_active=False,
        )
        session.add(dag_model)


class TestListBackfills(TestBackfillEndpoint):
    def test_should_respond_200(self, session):
        (dag,) = self._create_dag_models()
        from_date = timezone.utcnow()
        to_date = timezone.utcnow()
        b = Backfill(dag_id=dag.dag_id, from_date=from_date, to_date=to_date)
        session.add(b)
        session.commit()
        response = self.client.get(
            f"/api/v1/backfills?dag_id={dag.dag_id}",
            environ_overrides={"REMOTE_USER": "test"},
        )
        assert response.status_code == 200
        assert response.json == {
            "backfills": [
                {
                    "completed_at": mock.ANY,
                    "created_at": mock.ANY,
                    "dag_id": "TEST_DAG_1",
                    "dag_run_conf": None,
                    "from_date": from_date.isoformat(),
                    "id": b.id,
                    "is_paused": False,
                    "max_active_runs": 10,
                    "to_date": to_date.isoformat(),
                    "updated_at": mock.ANY,
                }
            ],
            "total_entries": 1,
        }

    @pytest.mark.parametrize(
        "user, expected",
        [
            ("test_no_permissions", 403),
            ("test", 200),
            (None, 401),
        ],
    )
    def test_should_respond_200_with_granular_dag_access(self, user, expected, session):
        (dag,) = self._create_dag_models()
        from_date = timezone.utcnow()
        to_date = timezone.utcnow()
        b = Backfill(
            dag_id=dag.dag_id,
            from_date=from_date,
            to_date=to_date,
        )

        session.add(b)
        session.commit()
        kwargs = {}
        if user:
            kwargs.update(environ_overrides={"REMOTE_USER": user})
        response = self.client.get("/api/v1/backfills?dag_id=TEST_DAG_1", **kwargs)
        assert response.status_code == expected


class TestGetBackfill(TestBackfillEndpoint):
    def test_should_respond_200(self, session):
        (dag,) = self._create_dag_models()
        from_date = timezone.utcnow()
        to_date = timezone.utcnow()
        backfill = Backfill(dag_id=dag.dag_id, from_date=from_date, to_date=to_date)
        session.add(backfill)
        session.commit()
        response = self.client.get(
            f"/api/v1/backfills/{backfill.id}",
            environ_overrides={"REMOTE_USER": "test"},
        )
        assert response.status_code == 200
        assert response.json == {
            "completed_at": mock.ANY,
            "created_at": mock.ANY,
            "dag_id": "TEST_DAG_1",
            "dag_run_conf": None,
            "from_date": from_date.isoformat(),
            "id": backfill.id,
            "is_paused": False,
            "max_active_runs": 10,
            "to_date": to_date.isoformat(),
            "updated_at": mock.ANY,
        }

    def test_no_exist(self, session):
        response = self.client.get(
            f"/api/v1/backfills/{23198409834208}",
            environ_overrides={"REMOTE_USER": "test"},
        )
        assert response.status_code == 404
        assert response.json.get("title") == "Backfill not found"

    @pytest.mark.parametrize(
        "user, expected",
        [
            ("test_no_permissions", 403),
            ("test", 200),
            (None, 401),
        ],
    )
    def test_should_respond_200_with_granular_dag_access(self, user, expected, session):
        (dag,) = self._create_dag_models()
        from_date = timezone.utcnow()
        to_date = timezone.utcnow()
        backfill = Backfill(
            dag_id=dag.dag_id,
            from_date=from_date,
            to_date=to_date,
        )
        session.add(backfill)
        session.commit()
        kwargs = {}
        if user:
            kwargs.update(environ_overrides={"REMOTE_USER": user})
        response = self.client.get(f"/api/v1/backfills/{backfill.id}", **kwargs)
        assert response.status_code == expected


class TestCreateBackfill(TestBackfillEndpoint):
    @pytest.mark.parametrize(
        "user, expected",
        [
            ("test_no_permissions", 403),
            ("test", 200),
            (None, 401),
        ],
    )
    def test_create_backfill(self, user, expected, session, dag_maker):
        with dag_maker(session=session, dag_id="TEST_DAG_1", schedule="0 * * * *") as dag:
            EmptyOperator(task_id="mytask")
        session.add(SerializedDagModel(dag))
        session.commit()
        session.query(DagModel).all()
        from_date = pendulum.parse("2024-01-01")
        from_date_iso = from_date.isoformat()
        to_date = pendulum.parse("2024-02-01")
        to_date_iso = to_date.isoformat()
        max_active_runs = 5
        query = urlencode(
            query={
                "dag_id": dag.dag_id,
                "from_date": f"{from_date_iso}",
                "to_date": f"{to_date_iso}",
                "max_active_runs": max_active_runs,
                "reverse": False,
            }
        )
        kwargs = {}
        if user:
            kwargs.update(environ_overrides={"REMOTE_USER": user})

        response = self.client.post(
            f"/api/v1/backfills?{query}",
            **kwargs,
        )
        assert response.status_code == expected
        if expected < 300:
            assert response.json == {
                "completed_at": mock.ANY,
                "created_at": mock.ANY,
                "dag_id": "TEST_DAG_1",
                "dag_run_conf": None,
                "from_date": from_date_iso,
                "id": mock.ANY,
                "is_paused": False,
                "max_active_runs": 5,
                "to_date": to_date_iso,
                "updated_at": mock.ANY,
            }


class TestPauseBackfill(TestBackfillEndpoint):
    def test_should_respond_200(self, session):
        (dag,) = self._create_dag_models()
        from_date = timezone.utcnow()
        to_date = timezone.utcnow()
        backfill = Backfill(dag_id=dag.dag_id, from_date=from_date, to_date=to_date)
        session.add(backfill)
        session.commit()
        response = self.client.post(
            f"/api/v1/backfills/{backfill.id}/pause",
            environ_overrides={"REMOTE_USER": "test"},
        )
        assert response.status_code == 200
        assert response.json == {
            "completed_at": mock.ANY,
            "created_at": mock.ANY,
            "dag_id": "TEST_DAG_1",
            "dag_run_conf": None,
            "from_date": from_date.isoformat(),
            "id": backfill.id,
            "is_paused": True,
            "max_active_runs": 10,
            "to_date": to_date.isoformat(),
            "updated_at": mock.ANY,
        }

    @pytest.mark.parametrize(
        "user, expected",
        [
            ("test_no_permissions", 403),
            ("test", 200),
            (None, 401),
        ],
    )
    def test_should_respond_200_with_granular_dag_access(self, user, expected, session):
        (dag,) = self._create_dag_models()
        from_date = timezone.utcnow()
        to_date = timezone.utcnow()
        backfill = Backfill(
            dag_id=dag.dag_id,
            from_date=from_date,
            to_date=to_date,
        )
        session.add(backfill)
        session.commit()
        kwargs = {}
        if user:
            kwargs.update(environ_overrides={"REMOTE_USER": user})
        response = self.client.post(f"/api/v1/backfills/{backfill.id}/pause", **kwargs)
        assert response.status_code == expected


class TestCancelBackfill(TestBackfillEndpoint):
    def test_should_respond_200(self, session):
        (dag,) = self._create_dag_models()
        from_date = timezone.utcnow()
        to_date = timezone.utcnow()
        backfill = Backfill(dag_id=dag.dag_id, from_date=from_date, to_date=to_date)
        session.add(backfill)
        session.commit()
        response = self.client.post(
            f"/api/v1/backfills/{backfill.id}/cancel",
            environ_overrides={"REMOTE_USER": "test"},
        )
        assert response.status_code == 200
        assert response.json == {
            "completed_at": mock.ANY,
            "created_at": mock.ANY,
            "dag_id": "TEST_DAG_1",
            "dag_run_conf": None,
            "from_date": from_date.isoformat(),
            "id": backfill.id,
            "is_paused": True,
            "max_active_runs": 10,
            "to_date": to_date.isoformat(),
            "updated_at": mock.ANY,
        }
        assert pendulum.parse(response.json["completed_at"])
        # now it is marked as completed
        assert pendulum.parse(response.json["completed_at"])

        # get conflict when canceling already-canceled backfill
        response = self.client.post(
            f"/api/v1/backfills/{backfill.id}/cancel", environ_overrides={"REMOTE_USER": "test"}
        )
        assert response.status_code == 409

    @pytest.mark.parametrize(
        "user, expected",
        [
            ("test_no_permissions", 403),
            ("test", 200),
            (None, 401),
        ],
    )
    def test_should_respond_200_with_granular_dag_access(self, user, expected, session):
        (dag,) = self._create_dag_models()
        from_date = timezone.utcnow()
        to_date = timezone.utcnow()
        backfill = Backfill(
            dag_id=dag.dag_id,
            from_date=from_date,
            to_date=to_date,
        )
        session.add(backfill)
        session.commit()
        kwargs = {}
        if user:
            kwargs.update(environ_overrides={"REMOTE_USER": user})
        response = self.client.post(f"/api/v1/backfills/{backfill.id}/cancel", **kwargs)
        assert response.status_code == expected
        if response.status_code < 300:
            # now it is marked as completed
            assert pendulum.parse(response.json["completed_at"])

            # get conflict when canceling already-canceled backfill
            response = self.client.post(f"/api/v1/backfills/{backfill.id}/cancel", **kwargs)
            assert response.status_code == 409
