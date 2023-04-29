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
from airflow.settings import _ENABLE_AIP_52


@pytest.mark.skipif(not _ENABLE_AIP_52, reason="AIP-52 is disabled")
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

    def test_setup_teardown_as_context_manager_normal_tasks_rel_set_downstream(self, dag_maker):
        """
        Test that setup >> teardown tasks are set up correctly when used as context managers
        and the normal tasks are set up with >> relations.
        """

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
            with setuptask() >> teardowntask():
                with setuptask2() >> teardowntask2():
                    mytask() >> mytask2()

        assert len(dag.task_group.children) == 6
        assert not dag.task_group.children["setuptask"].upstream_task_ids
        assert dag.task_group.children["setuptask"].downstream_task_ids == {"teardowntask", "setuptask2"}
        assert dag.task_group.children["setuptask2"].upstream_task_ids == {"setuptask"}
        assert dag.task_group.children["setuptask2"].downstream_task_ids == {"teardowntask2", "mytask"}
        assert dag.task_group.children["mytask"].upstream_task_ids == {"setuptask2"}
        assert dag.task_group.children["mytask"].downstream_task_ids == {"mytask2"}
        assert dag.task_group.children["mytask2"].upstream_task_ids == {"mytask"}
        assert dag.task_group.children["mytask2"].downstream_task_ids == {"teardowntask2"}
        assert dag.task_group.children["teardowntask"].upstream_task_ids == {"setuptask", "teardowntask2"}
        assert not dag.task_group.children["teardowntask"].downstream_task_ids
        assert dag.task_group.children["teardowntask2"].upstream_task_ids == {"setuptask2", "mytask2"}
        assert dag.task_group.children["teardowntask2"].downstream_task_ids == {"teardowntask"}

    def test_setup_teardown_as_context_manager_normal_tasks_rel_set_upstream(self, dag_maker):
        """
        Test that setup >> teardown tasks are set up correctly when used as context managers
        and the normal tasks are set up with << relations.
        """

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
            with setuptask() >> teardowntask():
                with setuptask2() >> teardowntask2():
                    mytask() << mytask2()

        assert len(dag.task_group.children) == 6
        assert not dag.task_group.children["setuptask"].upstream_task_ids
        assert dag.task_group.children["setuptask"].downstream_task_ids == {"teardowntask", "setuptask2"}
        assert dag.task_group.children["setuptask2"].upstream_task_ids == {"setuptask"}
        assert dag.task_group.children["setuptask2"].downstream_task_ids == {"teardowntask2", "mytask2"}
        assert dag.task_group.children["mytask"].upstream_task_ids == {"mytask2"}
        assert dag.task_group.children["mytask"].downstream_task_ids == {"teardowntask2"}
        assert dag.task_group.children["mytask2"].upstream_task_ids == {"setuptask2"}
        assert dag.task_group.children["mytask2"].downstream_task_ids == {"mytask"}
        assert dag.task_group.children["teardowntask"].upstream_task_ids == {"setuptask", "teardowntask2"}
        assert not dag.task_group.children["teardowntask"].downstream_task_ids
        assert dag.task_group.children["teardowntask2"].upstream_task_ids == {"setuptask2", "mytask"}
        assert dag.task_group.children["teardowntask2"].downstream_task_ids == {"teardowntask"}

    def test_normal_task_raises_when_used_as_context_managers(self, dag_maker):
        @task()
        def mytask():
            print("mytask")

        with dag_maker():
            with pytest.raises(
                AirflowException, match="Only setup/teardown tasks can be used as context managers."
            ):
                with mytask():
                    pass

    def test_only_setup(self, dag_maker):
        @setup
        def setuptask():
            print("setup")

        @task()
        def mytask():
            print("mytask")

        @task()
        def mytask2():
            print("mytask")

        with dag_maker() as dag:
            with setuptask():
                mytask() >> mytask2()

        assert len(dag.task_group.children) == 3
        assert not dag.task_group.children["setuptask"].upstream_task_ids
        assert dag.task_group.children["setuptask"].downstream_task_ids == {"mytask"}
        assert dag.task_group.children["mytask"].upstream_task_ids == {"setuptask"}
        assert dag.task_group.children["mytask"].downstream_task_ids == {"mytask2"}
        assert dag.task_group.children["mytask2"].upstream_task_ids == {"mytask"}
        assert not dag.task_group.children["mytask2"].downstream_task_ids

    def test_only_teardown(self, dag_maker):
        @teardown
        def teardowntask():
            print("teardown")

        @task()
        def mytask():
            print("mytask")

        @task()
        def mytask2():
            print("mytask")

        with dag_maker("foo") as dag:
            with teardowntask():
                mytask() >> mytask2()

        assert len(dag.task_group.children) == 3
        assert not dag.task_group.children["mytask"].upstream_task_ids
        assert dag.task_group.children["mytask"].downstream_task_ids == {"mytask2"}
        assert dag.task_group.children["mytask2"].upstream_task_ids == {"mytask"}
        assert dag.task_group.children["mytask2"].downstream_task_ids == {"teardowntask"}
        assert dag.task_group.children["teardowntask"].upstream_task_ids == {"mytask2"}
        assert not dag.task_group.children["teardowntask"].downstream_task_ids

    def test_nested_only_setup(self, dag_maker):
        @setup
        def setuptask():
            print("setup")

        @setup
        def setuptask2():
            print("setup")

        @teardown
        def teardowntask():
            print("teardown")

        @task()
        def mytask():
            print("mytask")

        @task()
        def mytask2():
            print("mytask")

        with dag_maker() as dag:
            with setuptask() >> teardowntask():
                with setuptask2():
                    mytask() << mytask2()

        assert len(dag.task_group.children) == 5
        assert not dag.task_group.children["setuptask"].upstream_task_ids
        assert dag.task_group.children["setuptask"].downstream_task_ids == {"teardowntask", "setuptask2"}
        assert dag.task_group.children["setuptask2"].upstream_task_ids == {"setuptask"}
        assert dag.task_group.children["setuptask2"].downstream_task_ids == {"mytask2"}
        assert dag.task_group.children["mytask"].upstream_task_ids == {"mytask2"}
        assert dag.task_group.children["mytask"].downstream_task_ids == {"teardowntask"}
        assert dag.task_group.children["mytask2"].upstream_task_ids == {"setuptask2"}
        assert dag.task_group.children["mytask2"].downstream_task_ids == {"mytask"}
        assert dag.task_group.children["teardowntask"].upstream_task_ids == {"setuptask", "mytask"}
        assert not dag.task_group.children["teardowntask"].downstream_task_ids

    def test_task_in_different_setup_context(self, dag_maker):
        @setup
        def setuptask():
            print("setup")

        @setup
        def setuptask2():
            print("setup")

        @task()
        def mytask():
            print("mytask")

        @task()
        def mytask2():
            print("mytask")

        @task()
        def mytask3():
            print("mytask")

        @task
        def mytask4():
            print("mytask")

        with dag_maker() as dag:
            with setuptask():
                t1 = mytask()
                t2 = mytask2()
                t1 >> t2
            with setuptask2():
                t3 = mytask3()
                t4 = mytask4()
                t2 >> t3 >> t4

        assert len(dag.task_group.children) == 6
        assert not dag.task_group.children["setuptask"].upstream_task_ids
        assert not dag.task_group.children["setuptask2"].upstream_task_ids
        assert dag.task_group.children["setuptask"].downstream_task_ids == {"mytask"}
        assert dag.task_group.children["setuptask2"].downstream_task_ids == {"mytask3"}
        assert dag.task_group.children["mytask"].upstream_task_ids == {"setuptask"}
        assert dag.task_group.children["mytask"].downstream_task_ids == {"mytask2"}
        assert dag.task_group.children["mytask2"].upstream_task_ids == {"mytask"}
        assert dag.task_group.children["mytask2"].downstream_task_ids == {"mytask3"}
        assert dag.task_group.children["mytask3"].upstream_task_ids == {"mytask2", "setuptask2"}
        assert dag.task_group.children["mytask3"].downstream_task_ids == {"mytask4"}
        assert dag.task_group.children["mytask4"].upstream_task_ids == {"mytask3"}
        assert not dag.task_group.children["mytask4"].downstream_task_ids

    def test_task_in_different_setup_context_2(self, dag_maker):
        @setup
        def setuptask():
            print("setup")

        @setup
        def setuptask2():
            print("setup")

        @task()
        def mytask():
            print("mytask")

        @task()
        def mytask2():
            print("mytask")

        @task()
        def mytask3():
            print("mytask")

        @task
        def mytask4():
            print("mytask")

        with dag_maker() as dag:
            with setuptask():
                t1 = mytask()
                t2 = mytask2()
                t1 >> t2
                with setuptask2():
                    t3 = mytask3()
                    t4 = mytask4()
                    t2 >> t3 >> t4

        assert len(dag.task_group.children) == 6
        assert not dag.task_group.children["setuptask"].upstream_task_ids
        assert dag.task_group.children["setuptask2"].upstream_task_ids == {"setuptask"}
        assert dag.task_group.children["setuptask"].downstream_task_ids == {"mytask", "setuptask2"}
        assert dag.task_group.children["setuptask2"].downstream_task_ids == {"mytask3"}
        assert dag.task_group.children["mytask"].upstream_task_ids == {"setuptask"}
        assert dag.task_group.children["mytask"].downstream_task_ids == {"mytask2"}
        assert dag.task_group.children["mytask2"].upstream_task_ids == {"mytask"}
        assert dag.task_group.children["mytask2"].downstream_task_ids == {"mytask3"}
        assert dag.task_group.children["mytask3"].upstream_task_ids == {"mytask2", "setuptask2"}
        assert dag.task_group.children["mytask3"].downstream_task_ids == {"mytask4"}
        assert dag.task_group.children["mytask4"].upstream_task_ids == {"mytask3"}
        assert not dag.task_group.children["mytask4"].downstream_task_ids

    def test_setup_teardown_as_context_manager_with_work_task_rel_not_set(self, dag_maker):
        """
        Test that setup >> teardown tasks are set up correctly when used as context managers
        and the normal tasks are set up even if they don't have a relationship set
        """

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
            with setuptask() >> teardowntask():
                with setuptask2() >> teardowntask2():
                    mytask()
                    mytask2()

        assert len(dag.task_group.children) == 6
        assert not dag.task_group.children["setuptask"].upstream_task_ids
        assert dag.task_group.children["setuptask"].downstream_task_ids == {"teardowntask", "setuptask2"}
        assert dag.task_group.children["setuptask2"].upstream_task_ids == {"setuptask"}
        assert dag.task_group.children["setuptask2"].downstream_task_ids == {
            "teardowntask2",
            "mytask",
            "mytask2",
        }
        assert dag.task_group.children["mytask"].upstream_task_ids == {"setuptask2"}
        assert dag.task_group.children["mytask"].downstream_task_ids == {"teardowntask2"}
        assert dag.task_group.children["mytask2"].upstream_task_ids == {"setuptask2"}
        assert dag.task_group.children["mytask2"].downstream_task_ids == {"teardowntask2"}
        assert dag.task_group.children["teardowntask"].upstream_task_ids == {"setuptask", "teardowntask2"}
        assert not dag.task_group.children["teardowntask"].downstream_task_ids
        assert dag.task_group.children["teardowntask2"].upstream_task_ids == {
            "setuptask2",
            "mytask",
            "mytask2",
        }
        assert dag.task_group.children["teardowntask2"].downstream_task_ids == {"teardowntask"}

    def test_classic_setup_teardown_as_context_manager_normal_tasks_rel_set_downstream(self, dag_maker):
        """
        Test that setup >> teardown tasks are set up correctly when used as context managers
        and the normal tasks are set up with >> relations.
        """

        @task()
        def mytask():
            print("mytask")

        @task()
        def mytask2():
            print("mytask")

        with dag_maker() as dag:
            setuptask = BashOperator.as_setup(task_id="setuptask", bash_command="echo 1")
            setuptask2 = BashOperator.as_setup(task_id="setuptask2", bash_command="echo 1")

            teardowntask = BashOperator.as_teardown(task_id="teardowntask", bash_command="echo 1")
            teardowntask2 = BashOperator.as_teardown(task_id="teardowntask2", bash_command="echo 1")
            with setuptask >> teardowntask:
                with setuptask2 >> teardowntask2:
                    mytask() >> mytask2()

        assert len(dag.task_group.children) == 6
        assert not dag.task_group.children["setuptask"].upstream_task_ids
        assert dag.task_group.children["setuptask"].downstream_task_ids == {"teardowntask", "setuptask2"}
        assert dag.task_group.children["setuptask2"].upstream_task_ids == {"setuptask"}
        assert dag.task_group.children["setuptask2"].downstream_task_ids == {"teardowntask2", "mytask"}
        assert dag.task_group.children["mytask"].upstream_task_ids == {"setuptask2"}
        assert dag.task_group.children["mytask"].downstream_task_ids == {"mytask2"}
        assert dag.task_group.children["mytask2"].upstream_task_ids == {"mytask"}
        assert dag.task_group.children["mytask2"].downstream_task_ids == {"teardowntask2"}
        assert dag.task_group.children["teardowntask"].upstream_task_ids == {"setuptask", "teardowntask2"}
        assert not dag.task_group.children["teardowntask"].downstream_task_ids
        assert dag.task_group.children["teardowntask2"].upstream_task_ids == {"setuptask2", "mytask2"}
        assert dag.task_group.children["teardowntask2"].downstream_task_ids == {"teardowntask"}

    def test_classic_setup_teardown_as_context_manager_normal_tasks_rel_set_upstream(self, dag_maker):
        """
        Test that setup >> teardown tasks are set up correctly when used as context managers
        and the normal tasks are set up with << relations.
        """

        @task()
        def mytask():
            print("mytask")

        @task()
        def mytask2():
            print("mytask")

        with dag_maker() as dag:
            setuptask = BashOperator.as_setup(task_id="setuptask", bash_command="echo 1")
            setuptask2 = BashOperator.as_setup(task_id="setuptask2", bash_command="echo 1")

            teardowntask = BashOperator.as_teardown(task_id="teardowntask", bash_command="echo 1")
            teardowntask2 = BashOperator.as_teardown(task_id="teardowntask2", bash_command="echo 1")
            with setuptask >> teardowntask:
                with setuptask2 >> teardowntask2:
                    mytask() << mytask2()

        assert len(dag.task_group.children) == 6
        assert not dag.task_group.children["setuptask"].upstream_task_ids
        assert dag.task_group.children["setuptask"].downstream_task_ids == {"teardowntask", "setuptask2"}
        assert dag.task_group.children["setuptask2"].upstream_task_ids == {"setuptask"}
        assert dag.task_group.children["setuptask2"].downstream_task_ids == {"teardowntask2", "mytask2"}
        assert dag.task_group.children["mytask"].upstream_task_ids == {"mytask2"}
        assert dag.task_group.children["mytask"].downstream_task_ids == {"teardowntask2"}
        assert dag.task_group.children["mytask2"].upstream_task_ids == {"setuptask2"}
        assert dag.task_group.children["mytask2"].downstream_task_ids == {"mytask"}
        assert dag.task_group.children["teardowntask"].upstream_task_ids == {"setuptask", "teardowntask2"}
        assert not dag.task_group.children["teardowntask"].downstream_task_ids
        assert dag.task_group.children["teardowntask2"].upstream_task_ids == {"setuptask2", "mytask"}
        assert dag.task_group.children["teardowntask2"].downstream_task_ids == {"teardowntask"}

    def test_only_setup_classic(self, dag_maker):
        @task()
        def mytask():
            print("mytask")

        @task()
        def mytask2():
            print("mytask")

        with dag_maker() as dag:
            setuptask = BashOperator.as_setup(task_id="setuptask", bash_command="echo 1")
            with setuptask:
                mytask() >> mytask2()

        assert len(dag.task_group.children) == 3
        assert not dag.task_group.children["setuptask"].upstream_task_ids
        assert dag.task_group.children["setuptask"].downstream_task_ids == {"mytask"}
        assert dag.task_group.children["mytask"].upstream_task_ids == {"setuptask"}
        assert dag.task_group.children["mytask"].downstream_task_ids == {"mytask2"}
        assert dag.task_group.children["mytask2"].upstream_task_ids == {"mytask"}
        assert not dag.task_group.children["mytask2"].downstream_task_ids

    def test_only_teardown_classic(self, dag_maker):
        @task()
        def mytask():
            print("mytask")

        @task()
        def mytask2():
            print("mytask")

        with dag_maker("foo") as dag:
            teardowntask = BashOperator.as_teardown(task_id="teardowntask", bash_command="echo 1")
            with teardowntask:
                mytask() >> mytask2()

        assert len(dag.task_group.children) == 3
        assert not dag.task_group.children["mytask"].upstream_task_ids
        assert dag.task_group.children["mytask"].downstream_task_ids == {"mytask2"}
        assert dag.task_group.children["mytask2"].upstream_task_ids == {"mytask"}
        assert dag.task_group.children["mytask2"].downstream_task_ids == {"teardowntask"}
        assert dag.task_group.children["teardowntask"].upstream_task_ids == {"mytask2"}
        assert not dag.task_group.children["teardowntask"].downstream_task_ids

    def test_nested_only_setup_classic(self, dag_maker):
        @task()
        def mytask():
            print("mytask")

        @task()
        def mytask2():
            print("mytask")

        with dag_maker() as dag:
            setuptask = BashOperator.as_setup(task_id="setuptask", bash_command="echo 1")
            setuptask2 = BashOperator.as_setup(task_id="setuptask2", bash_command="echo 1")

            teardowntask = BashOperator.as_teardown(task_id="teardowntask", bash_command="echo 1")
            with setuptask >> teardowntask:
                with setuptask2:
                    mytask() << mytask2()

        assert len(dag.task_group.children) == 5
        assert not dag.task_group.children["setuptask"].upstream_task_ids
        assert dag.task_group.children["setuptask"].downstream_task_ids == {"teardowntask", "setuptask2"}
        assert dag.task_group.children["setuptask2"].upstream_task_ids == {"setuptask"}
        assert dag.task_group.children["setuptask2"].downstream_task_ids == {"mytask2"}
        assert dag.task_group.children["mytask"].upstream_task_ids == {"mytask2"}
        assert dag.task_group.children["mytask"].downstream_task_ids == {"teardowntask"}
        assert dag.task_group.children["mytask2"].upstream_task_ids == {"setuptask2"}
        assert dag.task_group.children["mytask2"].downstream_task_ids == {"mytask"}
        assert dag.task_group.children["teardowntask"].upstream_task_ids == {"setuptask", "mytask"}
        assert not dag.task_group.children["teardowntask"].downstream_task_ids

    def test_classic_setup_teardown_task_in_different_setup_context(self, dag_maker):
        @task()
        def mytask():
            print("mytask")

        @task()
        def mytask2():
            print("mytask")

        @task()
        def mytask3():
            print("mytask")

        @task
        def mytask4():
            print("mytask")

        with dag_maker() as dag:
            setuptask = BashOperator.as_setup(task_id="setuptask", bash_command="echo 1")
            setuptask2 = BashOperator.as_setup(task_id="setuptask2", bash_command="echo 1")
            with setuptask:
                t1 = mytask()
                t2 = mytask2()
                t1 >> t2
            with setuptask2:
                t3 = mytask3()
                t4 = mytask4()
                t2 >> t3 >> t4

        assert len(dag.task_group.children) == 6
        assert not dag.task_group.children["setuptask"].upstream_task_ids
        assert not dag.task_group.children["setuptask2"].upstream_task_ids
        assert dag.task_group.children["setuptask"].downstream_task_ids == {"mytask"}
        assert dag.task_group.children["setuptask2"].downstream_task_ids == {"mytask3"}
        assert dag.task_group.children["mytask"].upstream_task_ids == {"setuptask"}
        assert dag.task_group.children["mytask"].downstream_task_ids == {"mytask2"}
        assert dag.task_group.children["mytask2"].upstream_task_ids == {"mytask"}
        assert dag.task_group.children["mytask2"].downstream_task_ids == {"mytask3"}
        assert dag.task_group.children["mytask3"].upstream_task_ids == {"mytask2", "setuptask2"}
        assert dag.task_group.children["mytask3"].downstream_task_ids == {"mytask4"}
        assert dag.task_group.children["mytask4"].upstream_task_ids == {"mytask3"}
        assert not dag.task_group.children["mytask4"].downstream_task_ids

    def test_classic_setup_teardown_task_in_different_setup_context_2(self, dag_maker):
        @task()
        def mytask():
            print("mytask")

        @task()
        def mytask2():
            print("mytask")

        @task()
        def mytask3():
            print("mytask")

        @task
        def mytask4():
            print("mytask")

        with dag_maker() as dag:
            setuptask = BashOperator.as_setup(task_id="setuptask", bash_command="echo 1")
            setuptask2 = BashOperator.as_setup(task_id="setuptask2", bash_command="echo 1")
            with setuptask:
                t1 = mytask()
                t2 = mytask2()
                t1 >> t2
                with setuptask2:
                    t3 = mytask3()
                    t4 = mytask4()
                    t2 >> t3 >> t4

        assert len(dag.task_group.children) == 6
        assert not dag.task_group.children["setuptask"].upstream_task_ids
        assert dag.task_group.children["setuptask2"].upstream_task_ids == {"setuptask"}
        assert dag.task_group.children["setuptask"].downstream_task_ids == {"mytask", "setuptask2"}
        assert dag.task_group.children["setuptask2"].downstream_task_ids == {"mytask3"}
        assert dag.task_group.children["mytask"].upstream_task_ids == {"setuptask"}
        assert dag.task_group.children["mytask"].downstream_task_ids == {"mytask2"}
        assert dag.task_group.children["mytask2"].upstream_task_ids == {"mytask"}
        assert dag.task_group.children["mytask2"].downstream_task_ids == {"mytask3"}
        assert dag.task_group.children["mytask3"].upstream_task_ids == {"mytask2", "setuptask2"}
        assert dag.task_group.children["mytask3"].downstream_task_ids == {"mytask4"}
        assert dag.task_group.children["mytask4"].upstream_task_ids == {"mytask3"}
        assert not dag.task_group.children["mytask4"].downstream_task_ids

    def test_classic_setup_teardown_as_context_manager_with_work_task_rel_not_set(self, dag_maker):
        """
        Test that setup >> teardown tasks are set up correctly when used as context managers
        and the normal tasks are set up even if they don't have a relationship set
        """

        @task()
        def mytask():
            print("mytask")

        @task()
        def mytask2():
            print("mytask")

        with dag_maker() as dag:
            setuptask = BashOperator.as_setup(task_id="setuptask", bash_command="echo 1")
            setuptask2 = BashOperator.as_setup(task_id="setuptask2", bash_command="echo 1")

            teardowntask = BashOperator.as_teardown(task_id="teardowntask", bash_command="echo 1")
            teardowntask2 = BashOperator.as_teardown(task_id="teardowntask2", bash_command="echo 1")
            with setuptask >> teardowntask:
                with setuptask2 >> teardowntask2:
                    mytask()
                    mytask2()

        assert len(dag.task_group.children) == 6
        assert not dag.task_group.children["setuptask"].upstream_task_ids
        assert dag.task_group.children["setuptask"].downstream_task_ids == {"teardowntask", "setuptask2"}
        assert dag.task_group.children["setuptask2"].upstream_task_ids == {"setuptask"}
        assert dag.task_group.children["setuptask2"].downstream_task_ids == {
            "teardowntask2",
            "mytask",
            "mytask2",
        }
        assert dag.task_group.children["mytask"].upstream_task_ids == {"setuptask2"}
        assert dag.task_group.children["mytask"].downstream_task_ids == {"teardowntask2"}
        assert dag.task_group.children["mytask2"].upstream_task_ids == {"setuptask2"}
        assert dag.task_group.children["mytask2"].downstream_task_ids == {"teardowntask2"}
        assert dag.task_group.children["teardowntask"].upstream_task_ids == {"setuptask", "teardowntask2"}
        assert not dag.task_group.children["teardowntask"].downstream_task_ids
        assert dag.task_group.children["teardowntask2"].upstream_task_ids == {
            "setuptask2",
            "mytask",
            "mytask2",
        }
        assert dag.task_group.children["teardowntask2"].downstream_task_ids == {"teardowntask"}
