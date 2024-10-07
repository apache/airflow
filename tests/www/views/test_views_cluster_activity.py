#
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

from datetime import timedelta

import pendulum
import pytest

from airflow.models import DagBag
from airflow.operators.empty import EmptyOperator
from airflow.utils.state import DagRunState, TaskInstanceState
from airflow.utils.types import DagRunType

from tests_common.test_utils.db import clear_db_runs

pytestmark = pytest.mark.db_test


@pytest.fixture(autouse=True, scope="module")
def examples_dag_bag():
    # Speed up: We don't want example dags for this module

    return DagBag(include_examples=False, read_dags_from_db=True)


@pytest.fixture(autouse=True)
def _clean():
    clear_db_runs()
    yield
    clear_db_runs()


# freeze time fixture so that it is applied before `_make_dag_runs` is!
@pytest.fixture
def _freeze_time_for_dagruns(time_machine):
    time_machine.move_to("2023-05-02T00:00:00+00:00", tick=False)


@pytest.fixture
def _make_dag_runs(dag_maker, session, time_machine):
    with dag_maker(
        dag_id="test_dag_id",
        serialized=True,
        session=session,
        start_date=pendulum.DateTime(2023, 2, 1, 0, 0, 0, tzinfo=pendulum.UTC),
    ):
        EmptyOperator(task_id="task_1") >> EmptyOperator(task_id="task_2")

    date = dag_maker.dag.start_date

    run1 = dag_maker.create_dagrun(
        run_id="run_1",
        state=DagRunState.SUCCESS,
        run_type=DagRunType.SCHEDULED,
        execution_date=date,
        start_date=date,
    )

    run2 = dag_maker.create_dagrun(
        run_id="run_2",
        state=DagRunState.FAILED,
        run_type=DagRunType.ASSET_TRIGGERED,
        execution_date=date + timedelta(days=1),
        start_date=date + timedelta(days=1),
    )

    run3 = dag_maker.create_dagrun(
        run_id="run_3",
        state=DagRunState.RUNNING,
        run_type=DagRunType.SCHEDULED,
        execution_date=pendulum.DateTime(2023, 2, 3, 0, 0, 0, tzinfo=pendulum.UTC),
        start_date=pendulum.DateTime(2023, 2, 3, 0, 0, 0, tzinfo=pendulum.UTC),
    )
    run3.end_date = None

    for ti in run1.task_instances:
        ti.state = TaskInstanceState.SUCCESS

    for ti in run2.task_instances:
        ti.state = TaskInstanceState.FAILED

    time_machine.move_to("2023-07-02T00:00:00+00:00", tick=False)

    session.flush()


@pytest.mark.usefixtures("_freeze_time_for_dagruns", "_make_dag_runs")
def test_historical_metrics_data(admin_client, session, time_machine):
    resp = admin_client.get(
        "/object/historical_metrics_data?start_date=2023-01-01T00:00&end_date=2023-08-02T00:00",
        follow_redirects=True,
    )
    assert resp.status_code == 200
    assert resp.json == {
        "dag_run_states": {"failed": 1, "queued": 0, "running": 1, "success": 1},
        "dag_run_types": {"backfill": 0, "dataset_triggered": 1, "manual": 0, "scheduled": 2},
        "task_instance_states": {
            "deferred": 0,
            "failed": 2,
            "no_status": 2,
            "queued": 0,
            "removed": 0,
            "restarting": 0,
            "running": 0,
            "scheduled": 0,
            "skipped": 0,
            "success": 2,
            "up_for_reschedule": 0,
            "up_for_retry": 0,
            "upstream_failed": 0,
        },
    }


@pytest.mark.usefixtures("_freeze_time_for_dagruns", "_make_dag_runs")
def test_historical_metrics_data_date_filters(admin_client, session):
    resp = admin_client.get(
        "/object/historical_metrics_data?start_date=2023-02-02T00:00&end_date=2023-06-02T00:00",
        follow_redirects=True,
    )
    assert resp.status_code == 200
    assert resp.json == {
        "dag_run_states": {"failed": 1, "queued": 0, "running": 0, "success": 0},
        "dag_run_types": {"backfill": 0, "dataset_triggered": 1, "manual": 0, "scheduled": 0},
        "task_instance_states": {
            "deferred": 0,
            "failed": 2,
            "no_status": 0,
            "queued": 0,
            "removed": 0,
            "restarting": 0,
            "running": 0,
            "scheduled": 0,
            "skipped": 0,
            "success": 0,
            "up_for_reschedule": 0,
            "up_for_retry": 0,
            "upstream_failed": 0,
        },
    }
