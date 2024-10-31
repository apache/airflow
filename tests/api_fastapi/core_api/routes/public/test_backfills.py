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

import pendulum
import pytest
from sqlalchemy import select

from airflow.models import DagBag, DagModel, DagRun
from airflow.models.backfill import Backfill, BackfillDagRun, ReprocessBehavior, _create_backfill
from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.utils import timezone
from airflow.utils.session import provide_session
from airflow.utils.state import DagRunState

from tests_common.test_utils.db import (
    clear_db_backfills,
    clear_db_dags,
    clear_db_runs,
    clear_db_serialized_dags,
)

pytestmark = [pytest.mark.db_test, pytest.mark.need_serialized_dag]


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


def make_dags():
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


def to_iso(val):
    return pendulum.instance(val).to_iso8601_string()


class TestBackfillEndpoint:
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


class TestListBackfills(TestBackfillEndpoint):
    def test_list_backfill(self, test_client, session):
        (dag,) = self._create_dag_models()
        from_date = timezone.utcnow()
        to_date = timezone.utcnow()
        b = Backfill(dag_id=dag.dag_id, from_date=from_date, to_date=to_date)
        session.add(b)
        session.commit()
        response = test_client.get(f"/public/backfills?dag_id={dag.dag_id}")
        assert response.status_code == 200
        assert response.json() == {
            "backfills": [
                {
                    "completed_at": mock.ANY,
                    "created_at": mock.ANY,
                    "dag_id": "TEST_DAG_1",
                    "dag_run_conf": {},
                    "from_date": to_iso(from_date),
                    "id": b.id,
                    "is_paused": False,
                    "reprocess_behavior": "none",
                    "max_active_runs": 10,
                    "to_date": to_iso(to_date),
                    "updated_at": mock.ANY,
                }
            ],
            "total_entries": 1,
        }


class TestGetBackfill(TestBackfillEndpoint):
    def test_get_backfill(self, session, test_client):
        (dag,) = self._create_dag_models()
        from_date = timezone.utcnow()
        to_date = timezone.utcnow()
        backfill = Backfill(dag_id=dag.dag_id, from_date=from_date, to_date=to_date)
        session.add(backfill)
        session.commit()
        response = test_client.get(f"/public/backfills/{backfill.id}")
        assert response.status_code == 200
        assert response.json() == {
            "completed_at": mock.ANY,
            "created_at": mock.ANY,
            "dag_id": "TEST_DAG_1",
            "dag_run_conf": {},
            "from_date": to_iso(from_date),
            "id": backfill.id,
            "is_paused": False,
            "reprocess_behavior": "none",
            "max_active_runs": 10,
            "to_date": to_iso(to_date),
            "updated_at": mock.ANY,
        }

    def test_no_exist(self, session, test_client):
        response = test_client.get(f"/public/backfills/{231984098}")
        assert response.status_code == 404
        assert response.json().get("detail") == "Backfill not found"


class TestCreateBackfill(TestBackfillEndpoint):
    @pytest.mark.parametrize(
        "repro_act, repro_exp",
        [
            (None, ReprocessBehavior.NONE),
            ("none", ReprocessBehavior.NONE),
            ("failed", ReprocessBehavior.FAILED),
            ("completed", ReprocessBehavior.COMPLETED),
        ],
    )
    def test_create_backfill(self, repro_act, repro_exp, session, dag_maker, test_client):
        with dag_maker(session=session, dag_id="TEST_DAG_1", schedule="0 * * * *") as dag:
            EmptyOperator(task_id="mytask")
        session.query(DagModel).all()
        session.commit()
        from_date = pendulum.parse("2024-01-01")
        from_date_iso = to_iso(from_date)
        to_date = pendulum.parse("2024-02-01")
        to_date_iso = to_iso(to_date)
        max_active_runs = 5
        data = {
            "dag_id": dag.dag_id,
            "from_date": f"{from_date_iso}",
            "to_date": f"{to_date_iso}",
            "max_active_runs": max_active_runs,
            "run_backwards": False,
            "dag_run_conf": {"param1": "val1", "param2": True},
        }
        if repro_act is not None:
            data["reprocess_behavior"] = repro_act
        response = test_client.post(
            url="/public/backfills",
            json=data,
        )
        assert response.status_code == 200
        assert response.json() == {
            "completed_at": mock.ANY,
            "created_at": mock.ANY,
            "dag_id": "TEST_DAG_1",
            "dag_run_conf": {"param1": "val1", "param2": True},
            "from_date": from_date_iso,
            "id": mock.ANY,
            "is_paused": False,
            "reprocess_behavior": repro_exp,
            "max_active_runs": 5,
            "to_date": to_date_iso,
            "updated_at": mock.ANY,
        }


class TestCancelBackfill(TestBackfillEndpoint):
    def test_cancel_backfill(self, session, test_client):
        (dag,) = self._create_dag_models()
        from_date = timezone.utcnow()
        to_date = timezone.utcnow()
        backfill = Backfill(dag_id=dag.dag_id, from_date=from_date, to_date=to_date)
        session.add(backfill)
        session.commit()
        response = test_client.put(
            f"/public/backfills/{backfill.id}/cancel",
        )
        assert response.status_code == 200
        assert response.json() == {
            "completed_at": mock.ANY,
            "created_at": mock.ANY,
            "dag_id": "TEST_DAG_1",
            "dag_run_conf": {},
            "from_date": to_iso(from_date),
            "id": backfill.id,
            "is_paused": True,
            "reprocess_behavior": "none",
            "max_active_runs": 10,
            "to_date": to_iso(to_date),
            "updated_at": mock.ANY,
        }
        assert pendulum.parse(response.json()["completed_at"])
        # now it is marked as completed
        assert pendulum.parse(response.json()["completed_at"])

        # get conflict when canceling already-canceled backfill
        response = test_client.put(f"/public/backfills/{backfill.id}/cancel")
        assert response.status_code == 409

    def test_cancel_backfill_end_states(self, dag_maker, session, test_client):
        """
        Queued runs should be marked *failed*.
        Every other dag run should be left alone.
        """
        with dag_maker(schedule="@daily") as dag:
            PythonOperator(task_id="hi", python_callable=print)
        b = _create_backfill(
            dag_id=dag.dag_id,
            from_date=timezone.datetime(2021, 1, 1),
            to_date=timezone.datetime(2021, 1, 5),
            max_active_runs=2,
            reverse=False,
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
        assert dates == expected_dates
        assert all(x.state == DagRunState.QUEUED for x in dag_runs)
        dag_runs[0].state = "running"
        session.commit()
        response = test_client.put(f"/public/backfills/{b.id}/cancel")
        assert response.status_code == 200
        session.expunge_all()
        dag_runs = session.scalars(query).all()
        states = [x.state for x in dag_runs]
        assert states == ["running", "failed", "failed", "failed", "failed"]


class TestPauseBackfill(TestBackfillEndpoint):
    def test_pause_backfill(self, session, test_client):
        (dag,) = self._create_dag_models()
        from_date = timezone.utcnow()
        to_date = timezone.utcnow()
        backfill = Backfill(dag_id=dag.dag_id, from_date=from_date, to_date=to_date)
        session.add(backfill)
        session.commit()
        response = test_client.put(f"/public/backfills/{backfill.id}/pause")
        assert response.status_code == 200
        assert response.json() == {
            "completed_at": mock.ANY,
            "created_at": mock.ANY,
            "dag_id": "TEST_DAG_1",
            "dag_run_conf": {},
            "from_date": to_iso(from_date),
            "id": backfill.id,
            "is_paused": True,
            "reprocess_behavior": "none",
            "max_active_runs": 10,
            "to_date": to_iso(to_date),
            "updated_at": mock.ANY,
        }
