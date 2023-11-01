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

from unittest.mock import Mock, patch

import pytest
import time_machine

from airflow import settings
from airflow.models import DagRun, TaskInstance
from airflow.ti_deps.deps.runnable_exec_date_dep import RunnableExecDateDep
from airflow.utils.timezone import datetime
from airflow.utils.types import DagRunType

pytestmark = pytest.mark.db_test


@pytest.fixture(autouse=True, scope="function")
def clean_db(session):
    yield
    session.query(DagRun).delete()
    session.query(TaskInstance).delete()


@time_machine.travel("2016-11-01")
@pytest.mark.parametrize(
    "allow_trigger_in_future,schedule,execution_date,is_met",
    [
        (True, None, datetime(2016, 11, 3), True),
        (True, "@daily", datetime(2016, 11, 3), False),
        (False, None, datetime(2016, 11, 3), False),
        (False, "@daily", datetime(2016, 11, 3), False),
        (False, "@daily", datetime(2016, 11, 1), True),
        (False, None, datetime(2016, 11, 1), True),
    ],
)
def test_exec_date_dep(
    dag_maker,
    session,
    create_dummy_dag,
    allow_trigger_in_future,
    schedule,
    execution_date,
    is_met,
):
    """
    If the dag's execution date is in the future but (allow_trigger_in_future=False or not schedule)
    this dep should fail
    """
    with patch.object(settings, "ALLOW_FUTURE_EXEC_DATES", allow_trigger_in_future):
        create_dummy_dag(
            "test_localtaskjob_heartbeat",
            start_date=datetime(2015, 1, 1),
            end_date=datetime(2016, 11, 5),
            schedule=schedule,
            with_dagrun_type=DagRunType.MANUAL,
            session=session,
        )
        (ti,) = dag_maker.create_dagrun(execution_date=execution_date).task_instances
        assert RunnableExecDateDep().is_met(ti=ti) == is_met


@time_machine.travel("2016-01-01")
def test_exec_date_after_end_date(session, dag_maker, create_dummy_dag):
    """
    If the dag's execution date is in the future this dep should fail
    """
    create_dummy_dag(
        "test_localtaskjob_heartbeat",
        start_date=datetime(2015, 1, 1),
        end_date=datetime(2016, 11, 5),
        schedule=None,
        with_dagrun_type=DagRunType.MANUAL,
        session=session,
    )
    (ti,) = dag_maker.create_dagrun(execution_date=datetime(2016, 11, 2)).task_instances
    assert not RunnableExecDateDep().is_met(ti=ti)


class TestRunnableExecDateDep:
    def _get_task_instance(self, execution_date, dag_end_date=None, task_end_date=None):
        dag = Mock(end_date=dag_end_date)
        dagrun = Mock(execution_date=execution_date)
        task = Mock(dag=dag, end_date=task_end_date)
        return Mock(task=task, get_dagrun=Mock(return_value=dagrun))

    def test_exec_date_after_task_end_date(self):
        """
        If the task instance execution date is after the tasks end date
        this dep should fail
        """
        ti = self._get_task_instance(
            dag_end_date=datetime(2016, 1, 3),
            task_end_date=datetime(2016, 1, 1),
            execution_date=datetime(2016, 1, 2),
        )
        assert not RunnableExecDateDep().is_met(ti=ti)

    def test_exec_date_after_dag_end_date(self):
        """
        If the task instance execution date is after the dag's end date
        this dep should fail
        """
        ti = self._get_task_instance(
            dag_end_date=datetime(2016, 1, 1),
            task_end_date=datetime(2016, 1, 3),
            execution_date=datetime(2016, 1, 2),
        )
        assert not RunnableExecDateDep().is_met(ti=ti)

    def test_all_deps_met(self):
        """
        Test to make sure all the conditions for the dep are met
        """
        ti = self._get_task_instance(
            dag_end_date=datetime(2016, 1, 2),
            task_end_date=datetime(2016, 1, 2),
            execution_date=datetime(2016, 1, 1),
        )
        assert RunnableExecDateDep().is_met(ti=ti)
