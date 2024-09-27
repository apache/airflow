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
from contextlib import nullcontext
from datetime import datetime
from unittest import mock
from urllib.parse import urlencode

import pendulum
import pytest
from sqlalchemy import select

from airflow.api_connexion.endpoints.backfill_endpoint import AlreadyRunningBackfill, _create_backfill
from airflow.models import DagBag, DagModel, DagRun
from airflow.models.backfill import Backfill, BackfillDagRun
from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.security import permissions
from airflow.utils import timezone
from airflow.utils.session import provide_session
from tests.test_utils.api_connexion_utils import create_user, delete_user
from tests.test_utils.db import clear_db_backfills, clear_db_dags, clear_db_runs, clear_db_serialized_dags

pytestmark = [pytest.mark.db_test, pytest.mark.need_serialized_dag]


DAG_ID = "test_dag"
TASK_ID = "op1"
DAG2_ID = "test_dag2"
DAG3_ID = "test_dag3"
UTC_JSON_REPR = "UTC" if pendulum.__version__.startswith("3") else "Timezone('UTC')"


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


@pytest.fixture(scope="module")
def configured_app(minimal_app_for_api):
    app = minimal_app_for_api

    create_user(
        app,  # type: ignore
        username="test",
        role_name="Test",
        permissions=[
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG),
            (permissions.ACTION_CAN_EDIT, permissions.RESOURCE_DAG),
            (permissions.ACTION_CAN_DELETE, permissions.RESOURCE_DAG),
        ],
    )
    create_user(app, username="test_no_permissions", role_name="TestNoPermissions")  # type: ignore
    create_user(app, username="test_granular_permissions", role_name="TestGranularDag")  # type: ignore
    app.appbuilder.sm.sync_perm_for_dag(  # type: ignore
        "TEST_DAG_1",
        access_control={
            "TestGranularDag": {
                permissions.RESOURCE_DAG: {permissions.ACTION_CAN_EDIT, permissions.ACTION_CAN_READ}
            },
        },
    )

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

    delete_user(app, username="test")  # type: ignore
    delete_user(app, username="test_no_permissions")  # type: ignore
    delete_user(app, username="test_granular_permissions")  # type: ignore


class TestBackfillEndpoint:
    @pytest.fixture(autouse=True)
    def setup_attrs(self, configured_app) -> None:
        self.app = configured_app
        self.client = self.app.test_client()  # type:ignore
        self.dag_id = DAG_ID
        self.dag2_id = DAG2_ID
        self.dag3_id = DAG3_ID

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
            ("test_granular_permissions", 200),
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
            ("test_granular_permissions", 200),
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
            ("test_granular_permissions", 200),
            ("test_no_permissions", 403),
            ("test", 200),
            (None, 401),
        ],
    )
    def test_create_backfill(self, user, expected, session, dag_maker):
        with dag_maker(session=session, dag_id="TEST_DAG_1", schedule="0 * * * *") as dag:
            EmptyOperator(task_id="mytask")
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
            ("test_granular_permissions", 200),
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
            ("test_granular_permissions", 200),
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


@pytest.mark.parametrize("dep_on_past", [True, False])
def test_reverse_and_depends_on_past_fails(dep_on_past, dag_maker, session):
    with dag_maker() as dag:
        PythonOperator(task_id="hi", python_callable=print, depends_on_past=dep_on_past)
    session.commit()
    cm = nullcontext()
    if dep_on_past:
        cm = pytest.raises(ValueError, match="cannot be run in reverse")
    b = None
    with cm:
        b = _create_backfill(
            dag_id=dag.dag_id,
            from_date=pendulum.parse("2021-01-01"),
            to_date=pendulum.parse("2021-01-05"),
            max_active_runs=2,
            reverse=True,
            dag_run_conf={},
        )
    if dep_on_past:
        assert b is None
    else:
        assert b is not None


@pytest.mark.parametrize("reverse", [True, False])
def test_simple(reverse, dag_maker, session):
    """
    Verify simple case behavior.

    This test verifies that runs in the range are created according
    to schedule intervals, and the sort ordinal is correct.
    """
    with dag_maker(schedule="@daily") as dag:
        PythonOperator(task_id="hi", python_callable=print)
    b = _create_backfill(
        dag_id=dag.dag_id,
        from_date=pendulum.parse("2021-01-01"),
        to_date=pendulum.parse("2021-01-05"),
        max_active_runs=2,
        reverse=reverse,
        dag_run_conf={},
    )
    query = (
        select(DagRun)
        .join(BackfillDagRun.dag_run)
        .where(BackfillDagRun.backfill_id == b.id)
        .order_by(BackfillDagRun.sort_ordinal)
    )
    dag_runs = session.scalars(query).all()
    dates = [str(x.logical_date.date()) for x in dag_runs]
    expected_dates = ["2021-01-01", "2021-01-02", "2021-01-03", "2021-01-04", "2021-01-05"]
    if reverse:
        expected_dates = list(reversed(expected_dates))
    assert dates == expected_dates


def test_params_stored_correctly(dag_maker, session):
    with dag_maker(schedule="@daily") as dag:
        PythonOperator(task_id="hi", python_callable=print)
    b = _create_backfill(
        dag_id=dag.dag_id,
        from_date=pendulum.parse("2021-01-01"),
        to_date=pendulum.parse("2021-01-05"),
        max_active_runs=263,
        reverse=False,
        dag_run_conf={"this": "param"},
    )
    session.expunge_all()
    b_stored = session.get(Backfill, b.id)
    assert all(
        (
            b_stored.dag_id == b.dag_id,
            b_stored.from_date == b.from_date,
            b_stored.to_date == b.to_date,
            b_stored.max_active_runs == b.max_active_runs,
            b_stored.dag_run_conf == b.dag_run_conf,
        )
    )


def test_active_dag_run(dag_maker, session):
    with dag_maker(schedule="@daily") as dag:
        PythonOperator(task_id="hi", python_callable=print)
    b1 = _create_backfill(
        dag_id=dag.dag_id,
        from_date=pendulum.parse("2021-01-01"),
        to_date=pendulum.parse("2021-01-05"),
        max_active_runs=10,
        reverse=False,
        dag_run_conf={"this": "param"},
    )
    assert b1 is not None
    with pytest.raises(AlreadyRunningBackfill, match="Another backfill is running for dag"):
        _create_backfill(
            dag_id=dag.dag_id,
            from_date=pendulum.parse("2021-02-01"),
            to_date=pendulum.parse("2021-02-05"),
            max_active_runs=10,
            reverse=False,
            dag_run_conf={"this": "param"},
        )
