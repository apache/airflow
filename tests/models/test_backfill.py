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

from contextlib import nullcontext

import pendulum
import pytest
from sqlalchemy import select

from airflow.models import DagRun
from airflow.models.backfill import (
    AlreadyRunningBackfill,
    Backfill,
    BackfillDagRun,
    _cancel_backfill,
    _create_backfill,
)
from airflow.operators.python import PythonOperator
from airflow.utils.state import DagRunState
from tests_common.test_utils.db import (
    clear_db_backfills,
    clear_db_dags,
    clear_db_runs,
    clear_db_serialized_dags,
)

pytestmark = [pytest.mark.db_test, pytest.mark.need_serialized_dag]


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
def test_create_backfill_simple(reverse, dag_maker, session):
    """
    Verify simple case behavior.

    This test verifies that runs in the range are created according
    to schedule intervals, and the sort ordinal is correct. Also verifies
    that dag runs are created in the queued state.
    """
    with dag_maker(schedule="@daily") as dag:
        PythonOperator(task_id="hi", python_callable=print)
    expected_run_conf = {"param1": "valABC"}
    b = _create_backfill(
        dag_id=dag.dag_id,
        from_date=pendulum.parse("2021-01-01"),
        to_date=pendulum.parse("2021-01-05"),
        max_active_runs=2,
        reverse=reverse,
        dag_run_conf=expected_run_conf,
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
    assert all(x.state == DagRunState.QUEUED for x in dag_runs)
    assert all(x.conf == expected_run_conf for x in dag_runs)


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
    session.commit()
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


def test_cancel_backfill(dag_maker, session):
    """
    Queued runs should be marked *failed*.
    Every other dag run should be left alone.
    """
    with dag_maker(schedule="@daily") as dag:
        PythonOperator(task_id="hi", python_callable=print)
    b = _create_backfill(
        dag_id=dag.dag_id,
        from_date=pendulum.parse("2021-01-01"),
        to_date=pendulum.parse("2021-01-05"),
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
    _cancel_backfill(backfill_id=b.id)
    session.expunge_all()
    dag_runs = session.scalars(query).all()
    states = [x.state for x in dag_runs]
    assert states == ["running", "failed", "failed", "failed", "failed"]
