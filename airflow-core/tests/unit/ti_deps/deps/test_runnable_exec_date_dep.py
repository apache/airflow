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

from unittest.mock import Mock

import pytest
import time_machine

from airflow._shared.timezones.timezone import datetime
from airflow.models import DagRun, TaskInstance
from airflow.ti_deps.deps.runnable_exec_date_dep import RunnableExecDateDep
from airflow.utils.types import DagRunType

pytestmark = pytest.mark.db_test


@pytest.fixture(autouse=True)
def clean_db(session):
    yield
    session.query(DagRun).delete()
    session.query(TaskInstance).delete()


@time_machine.travel("2016-11-01")
@pytest.mark.parametrize(
    ("logical_date", "is_met"),
    [
        (datetime(2016, 11, 3), False),
        (datetime(2016, 11, 1), True),
    ],
)
def test_logical_date_dep(
    dag_maker,
    session,
    create_dummy_dag,
    logical_date,
    is_met,
):
    """
    If the dag's logical date is in the future, this dep should fail
    """
    create_dummy_dag(
        "test_localtaskjob_heartbeat",
        start_date=datetime(2015, 1, 1),
        end_date=datetime(2016, 11, 5),
        schedule=None,
        with_dagrun_type=DagRunType.MANUAL,
        session=session,
    )
    (ti,) = dag_maker.create_dagrun(run_id="scheduled", logical_date=logical_date).task_instances
    assert RunnableExecDateDep().is_met(ti=ti) == is_met


@time_machine.travel("2016-01-01")
def test_logical_date_after_end_date(session, dag_maker, create_dummy_dag):
    """
    If the dag's logical date is in the future this dep should fail
    """
    create_dummy_dag(
        "test_localtaskjob_heartbeat",
        start_date=datetime(2015, 1, 1),
        end_date=datetime(2016, 11, 5),
        schedule=None,
        with_dagrun_type=DagRunType.MANUAL,
        session=session,
    )
    (ti,) = dag_maker.create_dagrun(logical_date=datetime(2016, 11, 2)).task_instances
    assert not RunnableExecDateDep().is_met(ti=ti)


class TestRunnableExecDateDep:
    def _get_task_instance(self, logical_date, dag_end_date=None, task_end_date=None):
        dag = Mock(end_date=dag_end_date)
        dagrun = Mock(logical_date=logical_date)
        task = Mock(dag=dag, end_date=task_end_date)
        return Mock(task=task, get_dagrun=Mock(return_value=dagrun))

    def test_logical_date_after_task_end_date(self):
        """
        If the task instance logical date is after the tasks end date
        this dep should fail
        """
        ti = self._get_task_instance(
            dag_end_date=datetime(2016, 1, 3),
            task_end_date=datetime(2016, 1, 1),
            logical_date=datetime(2016, 1, 2),
        )
        assert not RunnableExecDateDep().is_met(ti=ti)

    def test_exec_date_after_dag_end_date(self):
        """
        If the task instance logical date is after the dag's end date
        this dep should fail
        """
        ti = self._get_task_instance(
            dag_end_date=datetime(2016, 1, 1),
            task_end_date=datetime(2016, 1, 3),
            logical_date=datetime(2016, 1, 2),
        )
        assert not RunnableExecDateDep().is_met(ti=ti)

    def test_all_deps_met(self):
        """
        Test to make sure all the conditions for the dep are met
        """
        ti = self._get_task_instance(
            dag_end_date=datetime(2016, 1, 2),
            task_end_date=datetime(2016, 1, 2),
            logical_date=datetime(2016, 1, 1),
        )
        assert RunnableExecDateDep().is_met(ti=ti)
