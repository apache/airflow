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
"""
End-to-end semantics for backfilling a subset of a Dag's tasks (issue #70027).

These tests drive the real path the scheduler takes for a subset backfill run:
``DBDagBag.get_dag_for_run`` prunes the run's Dag to the selected tasks, and that
pruned Dag then feeds ``verify_integrity`` (task-instance creation) and
``task_instance_scheduling_decisions`` (dependency evaluation). Together they prove
that only the selected tasks are scheduled and that a selected task is not blocked
by an unselected upstream.
"""

from __future__ import annotations

import pendulum
import pytest
from sqlalchemy import select

from airflow.models.backfill import _create_backfill
from airflow.models.dag_version import DagVersion
from airflow.models.dagbag import DBDagBag
from airflow.models.dagrun import DagRun
from airflow.providers.standard.operators.empty import EmptyOperator
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


def _first_backfill_run(backfill_id, session) -> DagRun:
    return session.scalar(select(DagRun).where(DagRun.backfill_id == backfill_id).limit(1))


def test_subset_backfill_creates_only_selected_tis(dag_maker, session):
    """verify_integrity on a subset backfill run creates task instances only for selected tasks."""
    with dag_maker(schedule="@daily") as dag:
        load = EmptyOperator(task_id="load")
        transform_x = EmptyOperator(task_id="transform_x")
        publish_x = EmptyOperator(task_id="publish_x")
        load >> transform_x >> publish_x
    session.commit()

    backfill = _create_backfill(
        dag_id=dag.dag_id,
        from_date=pendulum.parse("2021-01-01"),
        to_date=pendulum.parse("2021-01-02"),
        max_active_runs=2,
        reverse=False,
        triggering_user_name="pytest",
        dag_run_conf={},
        task_id_pattern="_x",
    )
    assert set(backfill.selected_task_ids) == {"transform_x", "publish_x"}

    run = _first_backfill_run(backfill.id, session)
    # Attach the pruned Dag exactly like the scheduler does.
    run.dag = DBDagBag().get_dag_for_run(run, session=session)
    assert set(run.dag.task_ids) == {"transform_x", "publish_x"}

    dag_version_id = DagVersion.get_latest_version(dag_id=dag.dag_id, session=session).id
    run.verify_integrity(dag_version_id=dag_version_id, session=session)
    session.flush()

    created = {ti.task_id for ti in run.get_task_instances(session=session)}
    assert created == {"transform_x", "publish_x"}


def test_subset_backfill_selected_task_not_blocked_by_unselected_upstream(dag_maker, session):
    """A selected task whose upstream is unselected is schedulable (the upstream dep is ignored)."""
    with dag_maker(schedule="@daily") as dag:
        load = EmptyOperator(task_id="load")
        transform_x = EmptyOperator(task_id="transform_x")
        publish_x = EmptyOperator(task_id="publish_x")
        load >> transform_x >> publish_x
    session.commit()

    backfill = _create_backfill(
        dag_id=dag.dag_id,
        from_date=pendulum.parse("2021-01-01"),
        to_date=pendulum.parse("2021-01-02"),
        max_active_runs=2,
        reverse=False,
        triggering_user_name="pytest",
        dag_run_conf={},
        task_id_pattern="_x",
    )

    run = _first_backfill_run(backfill.id, session)
    run.dag = DBDagBag().get_dag_for_run(run, session=session)
    run.state = DagRunState.RUNNING

    dag_version_id = DagVersion.get_latest_version(dag_id=dag.dag_id, session=session).id
    run.verify_integrity(dag_version_id=dag_version_id, session=session)
    session.flush()

    decision = run.task_instance_scheduling_decisions(session=session)
    schedulable = {ti.task_id for ti in decision.schedulable_tis}
    # transform_x's only upstream (load) was pruned away, so it is immediately schedulable;
    # publish_x still waits on the not-yet-run transform_x.
    assert schedulable == {"transform_x"}
