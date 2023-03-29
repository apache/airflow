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

from airflow import AirflowException
from airflow.decorators import setup, task, task_group, teardown
from airflow.operators.bash import BashOperator


class TestSetupTearDownTask:
    def test_marking_functions_as_setup_task(self, dag_maker):
        @setup
        def mytask():
            print("I am a setup task")

        with dag_maker() as dag:
            mytask()

        assert len(dag.task_group.children) == 1
        setup_task = dag.task_group.children["mytask"]
        assert setup_task._is_setup

    def test_marking_functions_as_teardown_task(self, dag_maker):
        @teardown
        def mytask():
            print("I am a teardown task")

        with dag_maker() as dag:
            mytask()

        assert len(dag.task_group.children) == 1
        teardown_task = dag.task_group.children["mytask"]
        assert teardown_task._is_teardown

    def test_marking_decorated_functions_as_setup_task(self, dag_maker):
        @setup
        @task
        def mytask():
            print("I am a setup task")

        with dag_maker() as dag:
            mytask()

        assert len(dag.task_group.children) == 1
        setup_task = dag.task_group.children["mytask"]
        assert setup_task._is_setup

    def test_marking_operator_as_setup_task(self, dag_maker):
        with dag_maker() as dag:
            BashOperator.as_setup(task_id="mytask", bash_command='echo "I am a setup task"')

        assert len(dag.task_group.children) == 1
        setup_task = dag.task_group.children["mytask"]
        assert setup_task._is_setup

    def test_marking_decorated_functions_as_teardown_task(self, dag_maker):
        @teardown
        @task
        def mytask():
            print("I am a teardown task")

        with dag_maker() as dag:
            mytask()

        assert len(dag.task_group.children) == 1
        teardown_task = dag.task_group.children["mytask"]
        assert teardown_task._is_teardown

    def test_marking_operator_as_teardown_task(self, dag_maker):
        with dag_maker() as dag:
            BashOperator.as_teardown(task_id="mytask", bash_command='echo "I am a setup task"')

        assert len(dag.task_group.children) == 1
        teardown_task = dag.task_group.children["mytask"]
        assert teardown_task._is_teardown

    def test_setup_taskgroup_decorator(self, dag_maker):
        with dag_maker():
            with pytest.raises(
                expected_exception=AirflowException,
                match="Task groups cannot be marked as setup or teardown.",
            ):

                @setup
                @task_group
                def mygroup():
                    @task
                    def mytask():
                        print("I am a setup task")

                    mytask()

                mygroup()

    def test_teardown_taskgroup_decorator(self, dag_maker):

        with dag_maker():
            with pytest.raises(
                expected_exception=AirflowException,
                match="Task groups cannot be marked as setup or teardown.",
            ):

                @teardown
                @task_group
                def mygroup():
                    @task
                    def mytask():
                        print("I am a teardown task")

                    mytask()

                mygroup()

    @pytest.mark.parametrize("on_failure_fail_dagrun", [True, False])
    def test_teardown_task_decorators_works_with_on_failure_fail_dagrun(
        self, on_failure_fail_dagrun, dag_maker
    ):
        @teardown(on_failure_fail_dagrun=on_failure_fail_dagrun)
        def mytask():
            print("I am a teardown task")

        with dag_maker() as dag:
            mytask()
        teardown_task = dag.task_group.children["mytask"]
        assert teardown_task._is_teardown
        assert teardown_task._on_failure_fail_dagrun is on_failure_fail_dagrun
        assert len(dag.task_group.children) == 1

    @pytest.mark.parametrize("on_failure_fail_dagrun", [True, False])
    def test_classic_teardown_task_works_with_on_failure_fail_dagrun(self, on_failure_fail_dagrun, dag_maker):
        with dag_maker() as dag:
            BashOperator.as_teardown(
                task_id="mytask",
                bash_command='echo "I am a teardown task"',
                on_failure_fail_dagrun=on_failure_fail_dagrun,
            )

        teardown_task = dag.task_group.children["mytask"]
        assert teardown_task._is_teardown
        assert teardown_task._on_failure_fail_dagrun is on_failure_fail_dagrun
        assert len(dag.task_group.children) == 1

    def test_setup_task_can_be_overriden(self, dag_maker):
        @setup
        def mytask():
            print("I am a setup task")

        with dag_maker() as dag:
            mytask.override(task_id="mytask2")()
        assert len(dag.task_group.children) == 1
        setup_task = dag.task_group.children["mytask2"]
        assert setup_task._is_setup

    def test_setup_teardown_mixed_up_in_a_dag(self, dag_maker):
        @setup
        def setuptask():
            print("setup")

        @setup
        def setuptask2():
            print("setup")

        @teardown
        def teardowntask():
            print("teardown")

        @teardown
        def teardowntask2():
            print("teardown")

        @task()
        def mytask():
            print("mytask")

        @task()
        def mytask2():
            print("mytask")

        with dag_maker() as dag:
            setuptask()
            teardowntask()
            setuptask2()
            teardowntask2()
            mytask()
            mytask2()

        assert len(dag.task_group.children) == 6
        assert [x for x in dag.tasks if not x.downstream_list]  # no deps have been set
        assert dag.task_group.children["setuptask"]._is_setup
        assert dag.task_group.children["teardowntask"]._is_teardown
        assert dag.task_group.children["setuptask2"]._is_setup
        assert dag.task_group.children["teardowntask2"]._is_teardown
        assert dag.task_group.children["mytask"]._is_setup is False
        assert dag.task_group.children["mytask"]._is_teardown is False
        assert dag.task_group.children["mytask2"]._is_setup is False
        assert dag.task_group.children["mytask2"]._is_teardown is False
