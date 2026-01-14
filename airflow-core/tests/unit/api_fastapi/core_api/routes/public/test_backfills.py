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
from datetime import datetime, timedelta
from unittest import mock

import pendulum
import pytest
from sqlalchemy import and_, func, select

from airflow._shared.timezones import timezone
from airflow.dag_processing.dagbag import DagBag
from airflow.models import DagModel, DagRun
from airflow.models.backfill import Backfill, BackfillDagRun, ReprocessBehavior, _create_backfill
from airflow.models.dag import DAG
from airflow.models.dagbundle import DagBundleModel
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.utils.session import provide_session
from airflow.utils.state import DagRunState

from tests_common.test_utils.asserts import assert_queries_count
from tests_common.test_utils.db import (
    clear_db_backfills,
    clear_db_dag_bundles,
    clear_db_dags,
    clear_db_logs,
    clear_db_runs,
    clear_db_serialized_dags,
)
from tests_common.test_utils.logs import check_last_log

pytestmark = [pytest.mark.db_test, pytest.mark.need_serialized_dag]


DAG_ID = "test_dag"
TASK_ID = "op1"
DAG2_ID = "test_dag2"
DAG3_ID = "test_dag3"


def _clean_db():
    clear_db_backfills()
    clear_db_runs()
    clear_db_dags()
    clear_db_dag_bundles()
    clear_db_serialized_dags()
    clear_db_logs()


@pytest.fixture(autouse=True)
def clean_db():
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
        bundle_name = "dags-folder"
        orm_dag_bundle = DagBundleModel(name=bundle_name)
        session.add(orm_dag_bundle)
        session.flush()

        dags = []
        for num in range(1, count + 1):
            dag_model = DagModel(
                dag_id=f"{dag_id_prefix}_{num}",
                bundle_name=bundle_name,
                fileloc=f"/tmp/dag_{num}.py",
                is_stale=False,
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

        with assert_queries_count(2):
            response = test_client.get(f"/backfills?dag_id={dag.dag_id}")

        assert response.status_code == 200
        assert response.json() == {
            "backfills": [
                {
                    "completed_at": mock.ANY,
                    "created_at": mock.ANY,
                    "dag_display_name": "TEST_DAG_1",
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
        response = test_client.get(f"/backfills/{backfill.id}")
        assert response.status_code == 200
        assert response.json() == {
            "completed_at": mock.ANY,
            "created_at": mock.ANY,
            "dag_display_name": "TEST_DAG_1",
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
        response = test_client.get(f"/backfills/{231984098}")
        assert response.status_code == 404
        assert response.json().get("detail") == "Backfill not found"

    def test_invalid_id(self, test_client):
        response = test_client.get("/backfills/invalid_id")
        assert response.status_code == 422
        response_detail = response.json()["detail"][0]
        assert response_detail["input"] == "invalid_id"
        assert response_detail["loc"] == ["path", "backfill_id"]
        assert (
            response_detail["msg"] == "Input should be a valid integer, unable to parse string as an integer"
        )


class TestCreateBackfill(TestBackfillEndpoint):
    @pytest.mark.parametrize(
        ("repro_act", "repro_exp"),
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
        session.scalars(select(DagModel)).all()
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
            url="/backfills",
            json=data,
        )
        assert response.status_code == 200
        assert response.json() == {
            "completed_at": mock.ANY,
            "created_at": mock.ANY,
            "dag_display_name": "TEST_DAG_1",
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
        check_last_log(session, dag_id="TEST_DAG_1", event="create_backfill", logical_date=None)

    def test_dag_not_exist(self, session, test_client):
        session.scalars(select(DagModel)).all()
        session.commit()
        from_date = pendulum.parse("2024-01-01")
        from_date_iso = to_iso(from_date)
        to_date = pendulum.parse("2024-02-01")
        to_date_iso = to_iso(to_date)
        max_active_runs = 5
        data = {
            "dag_id": "DAG_NOT_EXIST",
            "from_date": f"{from_date_iso}",
            "to_date": f"{to_date_iso}",
            "max_active_runs": max_active_runs,
            "run_backwards": False,
            "dag_run_conf": {"param1": "val1", "param2": True},
            "reprocess_behavior": ReprocessBehavior.NONE,
        }
        response = test_client.post(
            url="/backfills",
            json=data,
        )
        assert response.status_code == 404
        assert response.json().get("detail") == "Could not find dag DAG_NOT_EXIST"

    def test_no_schedule_dag(self, session, dag_maker, test_client):
        with dag_maker(session=session, dag_id="TEST_DAG_1", schedule="None") as dag:
            EmptyOperator(task_id="mytask")
        session.scalars(select(DagModel)).all()
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
            "reprocess_behavior": ReprocessBehavior.NONE,
        }
        response = test_client.post(
            url="/backfills",
            json=data,
        )
        assert response.status_code == 422
        assert response.json().get("detail") == f"{dag.dag_id} has no schedule"

    @pytest.mark.parametrize(
        ("repro_act", "repro_exp", "run_backwards", "status_code"),
        [
            ("none", ReprocessBehavior.NONE, False, 422),
            ("completed", ReprocessBehavior.COMPLETED, False, 200),
            ("completed", ReprocessBehavior.COMPLETED, True, 422),
        ],
    )
    def test_create_backfill_with_depends_on_past(
        self, repro_act, repro_exp, run_backwards, status_code, session, dag_maker, test_client
    ):
        with dag_maker(session=session, dag_id="TEST_DAG_1", schedule="0 * * * *") as dag:
            EmptyOperator(task_id="mytask", depends_on_past=True)
        session.scalars(select(DagModel)).all()
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
            "run_backwards": run_backwards,
            "dag_run_conf": {"param1": "val1", "param2": True},
            "reprocess_behavior": repro_act,
        }
        response = test_client.post(
            url="/backfills",
            json=data,
        )
        assert response.status_code == status_code

        if response.status_code != 200:
            if run_backwards:
                assert (
                    response.json().get("detail")
                    == "Backfill cannot be run in reverse when the DAG has tasks where depends_on_past=True."
                )
            else:
                assert (
                    response.json().get("detail")
                    == "DAG has tasks for which depends_on_past=True. You must set reprocess behavior to reprocess completed or reprocess failed."
                )

    @pytest.mark.parametrize(
        "run_backwards",
        [
            (False),
            (True),
        ],
    )
    def test_create_backfill_future_dates(self, session, dag_maker, test_client, run_backwards):
        with dag_maker(session=session, dag_id="TEST_DAG_1", schedule="0 * * * *") as dag:
            EmptyOperator(task_id="mytask")
        session.scalars(select(DagModel)).all()
        session.commit()
        from_date = timezone.utcnow() + timedelta(days=1)
        to_date = timezone.utcnow() + timedelta(days=1)
        max_active_runs = 5
        data = {
            "dag_id": dag.dag_id,
            "from_date": f"{to_iso(from_date)}",
            "to_date": f"{to_iso(to_date)}",
            "max_active_runs": max_active_runs,
            "run_backwards": run_backwards,
            "dag_run_conf": {"param1": "val1", "param2": True},
        }

        response = test_client.post(
            url="/backfills",
            json=data,
        )
        assert response.status_code == 422
        assert response.json().get("detail") == "Backfill cannot be executed for future dates."

    @pytest.mark.parametrize(
        "run_backwards",
        [
            (False),
            (True),
        ],
    )
    def test_create_backfill_past_future_dates(self, session, dag_maker, test_client, run_backwards):
        with dag_maker(session=session, dag_id="TEST_DAG_1", schedule="@daily") as dag:
            EmptyOperator(task_id="mytask")
        session.scalars(select(DagModel)).all()
        session.commit()
        from_date = timezone.utcnow() - timedelta(days=2)
        to_date = timezone.utcnow() + timedelta(days=1)
        max_active_runs = 1
        data = {
            "dag_id": dag.dag_id,
            "from_date": f"{to_iso(from_date)}",
            "to_date": f"{to_iso(to_date)}",
            "max_active_runs": max_active_runs,
            "run_backwards": run_backwards,
            "dag_run_conf": {"param1": "val1", "param2": True},
        }

        response = test_client.post(
            url="/backfills",
            json=data,
        )
        assert response.status_code == 200
        backfill_dag_run_count = select(func.count()).where(
            and_(BackfillDagRun.backfill_id == response.json()["id"], BackfillDagRun.logical_date == to_date)
        )
        count = session.execute(backfill_dag_run_count).scalar()
        assert count == 0

    # todo: AIP-83 amendment must fix
    @pytest.mark.parametrize(
        ("reprocess_behavior", "expected_dates"),
        [
            (
                "none",
                [
                    "2024-01-01T00:00:00+00:00",
                    "2024-01-04T00:00:00+00:00",
                    "2024-01-05T00:00:00+00:00",
                ],
            ),
            (
                "failed",
                [
                    "2024-01-01T00:00:00+00:00",
                    "2024-01-03T00:00:00+00:00",
                    "2024-01-04T00:00:00+00:00",
                    "2024-01-05T00:00:00+00:00",
                ],
            ),
            (
                "completed",
                [
                    "2024-01-01T00:00:00+00:00",
                    "2024-01-02T00:00:00+00:00",
                    "2024-01-03T00:00:00+00:00",
                    "2024-01-04T00:00:00+00:00",
                    "2024-01-05T00:00:00+00:00",
                ],
            ),
        ],
    )
    def test_create_backfill_with_existing_runs(
        self,
        session,
        dag_maker,
        test_client,
        reprocess_behavior,
        expected_dates,
    ):
        """
        Verify behavior when there's existing runs in the range

        If the there is a run in the range, depending on the reprocess behavior,
        it should clear the existing dag run and update it to be associated with the backfill.
        """
        with dag_maker(
            session=session,
            dag_id="TEST_DAG_2",
            schedule="0 0 * * *",
            start_date=pendulum.parse("2024-01-01"),
        ) as dag:
            EmptyOperator(task_id="mytask")

        session.commit()

        existing_dagruns = [
            {"logical_date": pendulum.parse("2024-01-02"), "state": DagRunState.SUCCESS},  # Completed dag run
            {"logical_date": pendulum.parse("2024-01-03"), "state": DagRunState.FAILED},  # Failed dag run
        ]
        for dagrun in existing_dagruns:
            session.add(
                DagRun(
                    dag_id=dag.dag_id,
                    run_id=f"manual__{dagrun['logical_date'].isoformat()}",
                    logical_date=dagrun["logical_date"],
                    state=dagrun["state"],
                    run_type="scheduled",
                )
            )
        session.commit()

        from_date = pendulum.parse("2024-01-01")
        from_date_iso = to_iso(from_date)
        to_date = pendulum.parse("2024-01-05")
        to_date_iso = to_iso(to_date)

        data = {
            "dag_id": dag.dag_id,
            "from_date": from_date_iso,
            "to_date": to_date_iso,
            "max_active_runs": 5,
            "run_backwards": False,
            "dag_run_conf": {"param1": "val1", "param2": True},
            "reprocess_behavior": reprocess_behavior,
        }

        response = test_client.post(
            url="/backfills",
            json=data,
        )

        assert response.status_code == 200
        response_json = response.json()
        assert response_json["from_date"] == "2024-01-01T00:00:00Z"
        assert response_json["to_date"] == "2024-01-05T00:00:00Z"
        backfill_id = response_json["id"]

        result = session.scalars(select(DagRun).where(DagRun.dag_id == dag.dag_id))
        dag_runs = sorted(((x.logical_date.isoformat(), x.backfill_id) for x in result), key=lambda x: x[0])

        all_dates = [
            "2024-01-01T00:00:00+00:00",
            "2024-01-02T00:00:00+00:00",
            "2024-01-03T00:00:00+00:00",
            "2024-01-04T00:00:00+00:00",
            "2024-01-05T00:00:00+00:00",
        ]

        # verify which runs are associated with the backfill now
        expected = []
        for date in all_dates:
            if date in expected_dates:
                expected.append((date, backfill_id))
            else:
                expected.append((date, None))
        assert dag_runs == expected

        # verify which logical dates have a non-create exception
        result = session.scalars(select(BackfillDagRun).where(BackfillDagRun.backfill_id == backfill_id))
        actual = sorted((x.logical_date.isoformat(), x.exception_reason) for x in result)
        expected = []
        for date in all_dates:
            if date in expected_dates:
                expected.append((date, None))
            else:
                expected.append((date, "already exists"))
        assert actual == expected

        # verify all dag runs associated with the backfill have backfill run type
        actual = list(session.scalars(select(DagRun.run_type).where(DagRun.backfill_id == backfill_id)))
        assert actual == ["backfill"] * len(expected_dates)

    def test_should_respond_401(self, unauthenticated_test_client, dag_maker, session):
        with dag_maker(session=session, dag_id="TEST_DAG_1", schedule="0 * * * *") as dag:
            EmptyOperator(task_id="mytask")
        session.scalars(select(DagModel)).all()
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
        response = unauthenticated_test_client.post("/backfills", json=data)
        assert response.status_code == 401

    def test_should_respond_403(self, unauthorized_test_client, dag_maker, session):
        with dag_maker(session=session, dag_id="TEST_DAG_1", schedule="0 * * * *") as dag:
            EmptyOperator(task_id="mytask")
        session.scalars(select(DagModel)).all()
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
        response = unauthorized_test_client.post("/backfills", json=data)
        assert response.status_code == 403


class TestCreateBackfillDryRun(TestBackfillEndpoint):
    @pytest.mark.parametrize(
        ("reprocess_behavior", "expected_dates"),
        [
            (
                "none",
                [
                    {"logical_date": "2024-01-01T00:00:00Z"},
                    {"logical_date": "2024-01-04T00:00:00Z"},
                    {"logical_date": "2024-01-05T00:00:00Z"},
                ],
            ),
            (
                "failed",
                [
                    {"logical_date": "2024-01-01T00:00:00Z"},
                    {"logical_date": "2024-01-03T00:00:00Z"},  # Reprocess failed
                    {"logical_date": "2024-01-04T00:00:00Z"},
                    {"logical_date": "2024-01-05T00:00:00Z"},
                ],
            ),
            (
                "completed",
                [
                    {"logical_date": "2024-01-01T00:00:00Z"},
                    {"logical_date": "2024-01-02T00:00:00Z"},  # Reprocess all
                    {"logical_date": "2024-01-03T00:00:00Z"},
                    {"logical_date": "2024-01-04T00:00:00Z"},
                    {"logical_date": "2024-01-05T00:00:00Z"},
                ],
            ),
        ],
    )
    def test_create_backfill_dry_run(
        self, session, dag_maker, test_client, reprocess_behavior, expected_dates
    ):
        with dag_maker(
            session=session,
            dag_id="TEST_DAG_2",
            schedule="0 0 * * *",
            start_date=pendulum.parse("2024-01-01"),
        ) as dag:
            EmptyOperator(task_id="mytask")

        session.commit()

        existing_dagruns = [
            {"logical_date": pendulum.parse("2024-01-02"), "state": DagRunState.SUCCESS},  # Completed dag run
            {"logical_date": pendulum.parse("2024-01-03"), "state": DagRunState.FAILED},  # Failed dag run
        ]
        for dagrun in existing_dagruns:
            session.add(
                DagRun(
                    dag_id=dag.dag_id,
                    run_id=f"manual__{dagrun['logical_date'].isoformat()}",
                    logical_date=dagrun["logical_date"],
                    state=dagrun["state"],
                    run_type="scheduled",
                )
            )
        session.commit()

        from_date = pendulum.parse("2024-01-01")
        from_date_iso = to_iso(from_date)
        to_date = pendulum.parse("2024-01-05")
        to_date_iso = to_iso(to_date)

        data = {
            "dag_id": dag.dag_id,
            "from_date": from_date_iso,
            "to_date": to_date_iso,
            "max_active_runs": 5,
            "run_backwards": False,
            "dag_run_conf": {"param1": "val1", "param2": True},
            "reprocess_behavior": reprocess_behavior,
        }

        response = test_client.post(
            url="/backfills/dry_run",
            json=data,
        )

        assert response.status_code == 200
        response_json = response.json()
        assert response_json["backfills"] == expected_dates

    @pytest.mark.parametrize(
        ("repro_act", "repro_exp", "run_backwards", "status_code"),
        [
            ("none", ReprocessBehavior.NONE, False, 422),
            ("completed", ReprocessBehavior.COMPLETED, False, 200),
            ("completed", ReprocessBehavior.COMPLETED, True, 422),
        ],
    )
    def test_create_backfill_dry_run_with_depends_on_past(
        self, repro_act, repro_exp, run_backwards, status_code, session, dag_maker, test_client
    ):
        with dag_maker(session=session, dag_id="TEST_DAG_1", schedule="0 * * * *") as dag:
            EmptyOperator(task_id="mytask", depends_on_past=True)
        session.scalars(select(DagModel)).all()
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
            "run_backwards": run_backwards,
            "dag_run_conf": {"param1": "val1", "param2": True},
            "reprocess_behavior": repro_act,
        }
        response = test_client.post(
            url="/backfills/dry_run",
            json=data,
        )
        assert response.status_code == status_code

        if response.status_code != 200:
            if run_backwards:
                assert (
                    response.json().get("detail")
                    == "Backfill cannot be run in reverse when the DAG has tasks where depends_on_past=True."
                )
            else:
                assert (
                    response.json().get("detail")
                    == "DAG has tasks for which depends_on_past=True. You must set reprocess behavior to reprocess completed or reprocess failed."
                )


class TestCancelBackfill(TestBackfillEndpoint):
    def test_cancel_backfill(self, session, test_client):
        (dag,) = self._create_dag_models()
        from_date = timezone.utcnow()
        to_date = timezone.utcnow()
        backfill = Backfill(dag_id=dag.dag_id, from_date=from_date, to_date=to_date)
        session.add(backfill)
        session.commit()
        response = test_client.put(
            f"/backfills/{backfill.id}/cancel",
        )
        assert response.status_code == 200
        assert response.json() == {
            "completed_at": mock.ANY,
            "created_at": mock.ANY,
            "dag_display_name": "TEST_DAG_1",
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
        response = test_client.put(f"/backfills/{backfill.id}/cancel")
        assert response.status_code == 409
        check_last_log(session, dag_id=None, event="cancel_backfill", logical_date=None)

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
            triggering_user_name="test_user",
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
        response = test_client.put(f"/backfills/{b.id}/cancel")
        assert response.status_code == 200
        session.expunge_all()
        dag_runs = session.scalars(query).all()
        states = [x.state for x in dag_runs]
        assert states == ["running", "failed", "failed", "failed", "failed"]

    def test_invalid_id(self, test_client):
        response = test_client.put("/backfills/invalid_id/cancel")
        assert response.status_code == 422
        response_detail = response.json()["detail"][0]
        assert response_detail["input"] == "invalid_id"
        assert response_detail["loc"] == ["path", "backfill_id"]
        assert (
            response_detail["msg"] == "Input should be a valid integer, unable to parse string as an integer"
        )


class TestPauseBackfill(TestBackfillEndpoint):
    def test_pause_backfill(self, session, test_client):
        (dag,) = self._create_dag_models()
        from_date = timezone.utcnow()
        to_date = timezone.utcnow()
        backfill = Backfill(dag_id=dag.dag_id, from_date=from_date, to_date=to_date)
        session.add(backfill)
        session.commit()
        response = test_client.put(f"/backfills/{backfill.id}/pause")
        assert response.status_code == 200
        assert response.json() == {
            "completed_at": mock.ANY,
            "created_at": mock.ANY,
            "dag_display_name": "TEST_DAG_1",
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
        check_last_log(session, dag_id=None, event="pause_backfill", logical_date=None)

    def test_pause_backfill_401(self, session, unauthenticated_test_client):
        (dag,) = self._create_dag_models()
        from_date = timezone.utcnow()
        to_date = timezone.utcnow()
        backfill = Backfill(dag_id=dag.dag_id, from_date=from_date, to_date=to_date)
        session.add(backfill)
        session.commit()
        response = unauthenticated_test_client.put(f"/backfills/{backfill.id}/pause")
        assert response.status_code == 401

    def test_pause_backfill_403(self, session, unauthorized_test_client):
        (dag,) = self._create_dag_models()
        from_date = timezone.utcnow()
        to_date = timezone.utcnow()
        backfill = Backfill(dag_id=dag.dag_id, from_date=from_date, to_date=to_date)
        session.add(backfill)
        session.commit()
        response = unauthorized_test_client.put(f"/backfills/{backfill.id}/pause")
        assert response.status_code == 403

    def test_invalid_id(self, test_client):
        response = test_client.put("/backfills/invalid_id/pause")
        assert response.status_code == 422
        response_detail = response.json()["detail"][0]
        assert response_detail["input"] == "invalid_id"
        assert response_detail["loc"] == ["path", "backfill_id"]
        assert (
            response_detail["msg"] == "Input should be a valid integer, unable to parse string as an integer"
        )


class TestUnpauseBackfill(TestBackfillEndpoint):
    def test_unpause_backfill(self, session, test_client):
        (dag,) = self._create_dag_models()
        from_date = timezone.utcnow()
        to_date = timezone.utcnow()
        backfill = Backfill(dag_id=dag.dag_id, from_date=from_date, to_date=to_date)
        session.add(backfill)
        session.commit()

        test_client.put(f"/backfills/{backfill.id}/pause")
        response = test_client.put(f"/backfills/{backfill.id}/unpause")
        assert response.status_code == 200
        assert response.json() == {
            "completed_at": mock.ANY,
            "created_at": mock.ANY,
            "dag_display_name": "TEST_DAG_1",
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
        check_last_log(session, dag_id=None, event="unpause_backfill", logical_date=None)

    def test_invalid_id(self, test_client):
        response = test_client.put("/backfills/invalid_id/unpause")
        assert response.status_code == 422
        response_detail = response.json()["detail"][0]
        assert response_detail["input"] == "invalid_id"
        assert response_detail["loc"] == ["path", "backfill_id"]
        assert (
            response_detail["msg"] == "Input should be a valid integer, unable to parse string as an integer"
        )
