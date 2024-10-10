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
from sqlalchemy import select

from airflow.models import DagRun
from airflow.operators.python import PythonOperator
from airflow.utils.module_loading import import_string
from airflow.utils.state import DagRunState
from airflow.utils.trigger_rule import TriggerRule

from dev.tests_common.test_utils.system_tests import get_test_run
from dev.tests_common.test_utils.system_tests_class import SystemTest


def fail():
    raise ValueError


def get_dag_success(dag_maker):
    with dag_maker(
        dag_id="test_dagrun_states_success",
        schedule=timedelta(days=1),
    ) as dag:
        dag4_task1 = PythonOperator(
            task_id="test_dagrun_fail",
            python_callable=fail,
        )
        dag4_task2 = PythonOperator(
            task_id="test_dagrun_succeed", trigger_rule=TriggerRule.ALL_FAILED, python_callable=print
        )
        dag4_task2.set_upstream(dag4_task1)
    return dag


def get_dag_fail(dag_maker):
    with dag_maker(
        dag_id="test_dagrun_states_fail",
        schedule=timedelta(days=1),
    ) as dag:
        dag3_task1 = PythonOperator(task_id="to_fail", python_callable=fail)
        dag3_task2 = PythonOperator(task_id="to_succeed", python_callable=print)
        dag3_task2.set_upstream(dag3_task1)
    return dag


def get_dag_fail_root(dag_maker):
    with dag_maker(
        dag_id="test_dagrun_states_root_fail",
        schedule=timedelta(days=1),
    ) as dag:
        PythonOperator(task_id="test_dagrun_succeed", python_callable=print)
        PythonOperator(
            task_id="test_dagrun_fail",
            python_callable=fail,
        )
    return dag


@pytest.mark.system("core")
class TestExampleDagsSystem(SystemTest):
    @pytest.mark.parametrize(
        "module",
        ["example_bash_operator", "example_branch_operator", "tutorial_dag", "example_dag_decorator"],
    )
    def test_dag_example(self, module):
        test_run = import_string(f"airflow.example_dags.{module}.test_run")
        test_run()

    @pytest.mark.parametrize(
        "factory, expected",
        [
            (get_dag_fail, "failed"),
            (get_dag_fail_root, "failed"),
            (get_dag_success, "success"),
        ],
    )
    def test_dag_run_final_state(self, factory, expected, dag_maker, session):
        """
        These tests are migrated tests that were added in PR #1289
        which was fixing issue #1225.

        I would be very surprised if these things were not covered elsewhere already
        but, just in case, I'm migrating them to system tests.
        """
        dag = factory(dag_maker)
        run = get_test_run(dag)
        with pytest.raises(AssertionError, match="The system test failed"):
            run()
        dr = session.scalar(select(DagRun))
        assert dr.state == "failed"

    def test_dag_root_task_start_date_future(self, dag_maker, session):
        """
        These tests are migrated tests that were added in PR #1289
        which was fixing issue #1225.

        This one tests what happens when there's a dag with a root task with future start date.

        The dag should run, but no TI should be created for the task where start date in future.
        """
        exec_date = pendulum.datetime(2021, 1, 1)
        fut_start_date = pendulum.datetime(2021, 2, 1)
        with dag_maker(
            dag_id="dagrun_states_root_future",
            schedule=timedelta(days=1),
            catchup=False,
        ) as dag:
            PythonOperator(
                task_id="current",
                python_callable=lambda: print("hello"),
            )
            PythonOperator(
                task_id="future",
                python_callable=lambda: print("hello"),
                start_date=fut_start_date,
            )
        run = get_test_run(dag, execution_date=exec_date)
        run()
        dr = session.scalar(select(DagRun))
        tis = dr.task_instances
        assert dr.state == DagRunState.SUCCESS
        assert len(tis) == 1
        assert tis[0].task_id == "current"
