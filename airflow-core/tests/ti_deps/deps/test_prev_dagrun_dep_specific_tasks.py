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

import pytest

from airflow.exceptions import AirflowException
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.ti_deps.deps.prev_dagrun_dep import PrevDagrunDep
from airflow.utils.state import DagRunState, State
from airflow.utils.timezone import datetime
from airflow.utils.types import DagRunType

pytestmark = pytest.mark.db_test


def test_depends_on_previous_task_ids_requires_depends_on_past():
    """
    Test that an AirflowException is raised if depends_on_previous_task_ids is set
    without depends_on_past=True.
    """
    with pytest.raises(AirflowException):
        PythonOperator(
            task_id="test_task",
            python_callable=lambda: None,
            depends_on_past=False,
            depends_on_previous_task_ids=["another_task"],
        )


def test_dependency_met_on_first_run(session):
    """Test that the dependency is met on the first DAG run."""
    with DAG("test_dag", start_date=datetime(2022, 1, 1), schedule_interval="@daily") as dag:
        task_a = PythonOperator(task_id="task_a", python_callable=lambda: None)
        PythonOperator(
            task_id="task_b",
            python_callable=lambda: None,
            depends_on_past=True,
            depends_on_previous_task_ids=["task_a"],
        )

    dr = dag.create_dagrun(
        run_type=DagRunType.SCHEDULED,
        state=DagRunState.RUNNING,
        execution_date=datetime(2022, 1, 1),
        session=session,
    )
    ti_a = dr.get_task_instance("task_a", session=session)
    ti_a.set_state(State.SUCCESS, session=session)
    ti_b = dr.get_task_instance("task_b", session=session)

    dep_status = next(PrevDagrunDep()._get_dep_statuses(ti_b, session, dep_context=None))
    assert dep_status.passed


def test_dependency_met_when_previous_tasks_succeeded(session):
    """Test that the dependency is met when the specified tasks in the previous run succeeded."""
    with DAG("test_dag", start_date=datetime(2022, 1, 1), schedule_interval="@daily") as dag:
        task_a = PythonOperator(task_id="task_a", python_callable=lambda: None)
        task_b = PythonOperator(task_id="task_b", python_callable=lambda: None)
        PythonOperator(
            task_id="task_c",
            python_callable=lambda: None,
            depends_on_past=True,
            depends_on_previous_task_ids=["task_a", "task_b"],
        )

    # Create previous DAG run and set task states to SUCCESS
    prev_dr = dag.create_dagrun(
        run_type=DagRunType.SCHEDULED,
        state=DagRunState.SUCCESS,
        execution_date=datetime(2022, 1, 1),
        session=session,
    )
    prev_ti_a = prev_dr.get_task_instance("task_a", session=session)
    prev_ti_a.set_state(State.SUCCESS, session=session)
    prev_ti_b = prev_dr.get_task_instance("task_b", session=session)
    prev_ti_b.set_state(State.SUCCESS, session=session)

    # Create current DAG run
    current_dr = dag.create_dagrun(
        run_type=DagRunType.SCHEDULED,
        state=DagRunState.RUNNING,
        execution_date=datetime(2022, 1, 2),
        session=session,
    )
    ti_c = current_dr.get_task_instance("task_c", session=session)

    dep_status = next(PrevDagrunDep()._get_dep_statuses(ti_c, session, dep_context=None))
    assert dep_status.passed


def test_dependency_failed_when_one_previous_task_failed(session):
    """Test that the dependency is not met when one of the specified tasks in the previous run failed."""
    with DAG("test_dag", start_date=datetime(2022, 1, 1), schedule_interval="@daily") as dag:
        task_a = PythonOperator(task_id="task_a", python_callable=lambda: None)
        task_b = PythonOperator(task_id="task_b", python_callable=lambda: None)
        PythonOperator(
            task_id="task_c",
            python_callable=lambda: None,
            depends_on_past=True,
            depends_on_previous_task_ids=["task_a", "task_b"],
        )

    # Create previous DAG run and set one task to FAILED
    prev_dr = dag.create_dagrun(
        run_type=DagRunType.SCHEDULED,
        state=DagRunState.FAILED,
        execution_date=datetime(2022, 1, 1),
        session=session,
    )
    prev_ti_a = prev_dr.get_task_instance("task_a", session=session)
    prev_ti_a.set_state(State.SUCCESS, session=session)
    prev_ti_b = prev_dr.get_task_instance("task_b", session=session)
    prev_ti_b.set_state(State.FAILED, session=session)

    # Create current DAG run
    current_dr = dag.create_dagrun(
        run_type=DagRunType.SCHEDULED,
        state=DagRunState.RUNNING,
        execution_date=datetime(2022, 1, 2),
        session=session,
    )
    ti_c = current_dr.get_task_instance("task_c", session=session)

    dep_status = next(PrevDagrunDep()._get_dep_statuses(ti_c, session, dep_context=None))
    assert not dep_status.passed
