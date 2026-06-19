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
from typing import TYPE_CHECKING

import pytest

from airflow._shared.timezones import timezone
from airflow.jobs.job import Job
from airflow.jobs.scheduler_job_runner import SchedulerJobRunner
from airflow.models.dag import DagModel
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.timetables.base import DataInterval
from airflow.utils.state import State
from airflow.utils.types import DagRunType

from tests_common.test_utils.mock_executor import MockExecutor
from unit.listeners import skipped_intervals_listener

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

    from tests_common.pytest_plugin import DagMaker

pytestmark = [pytest.mark.db_test, pytest.mark.need_serialized_dag]

DEFAULT_DATE = timezone.datetime(2016, 1, 1)


@pytest.fixture(autouse=True)
def clean_listener_state():
    yield
    skipped_intervals_listener.clear()


def _configure_gap_dag_model(dag_maker: DagMaker, session: Session):
    prev_start = DEFAULT_DATE
    prev_end = DEFAULT_DATE + timedelta(days=1)
    dag_maker.create_dagrun(
        run_type=DagRunType.SCHEDULED,
        logical_date=prev_start,
        data_interval=DataInterval(start=prev_start, end=prev_end),
        state=State.SUCCESS,
    )

    new_start = DEFAULT_DATE + timedelta(days=4)
    new_end = DEFAULT_DATE + timedelta(days=5)
    dag_model = DagModel.get_dagmodel(dag_maker.dag.dag_id, session=session)
    if dag_model is None:
        raise RuntimeError(f"DagModel for {dag_maker.dag.dag_id} not found")
    dag_model.next_dagrun = new_start
    dag_model.next_dagrun_data_interval = DataInterval(start=new_start, end=new_end)
    dag_model.next_dagrun_create_after = new_end
    session.merge(dag_model)
    session.commit()
    return dag_model, prev_end, new_start


def test_listener_notified_when_intervals_skipped(session, dag_maker, listener_manager):
    listener_manager(skipped_intervals_listener)

    with dag_maker(
        dag_id="test_listener_skipped_intervals",
        schedule=timedelta(days=1),
        start_date=DEFAULT_DATE,
        catchup=False,
        session=session,
    ):
        EmptyOperator(task_id="dummy")

    dag_model, prev_end, new_start = _configure_gap_dag_model(dag_maker, session)

    job_runner = SchedulerJobRunner(job=Job(), executors=[MockExecutor(do_update=False)])
    job_runner._create_dag_runs([dag_model], session)
    session.commit()

    assert len(skipped_intervals_listener.events) == 1
    dag_id, summary = skipped_intervals_listener.events[0]
    assert dag_id == dag_maker.dag.dag_id
    assert summary.skipped_interval_count == 3
    assert summary.skipped_range.start == prev_end
    assert summary.skipped_range.end == new_start
