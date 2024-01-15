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

from airflow.decorators import setup, task, task_group, teardown
from airflow.decorators.setup_teardown import context_wrapper
from airflow.exceptions import AirflowException
from airflow.operators.bash import BashOperator

pytestmark = pytest.mark.db_test


def make_task(name, type_, setup_=False, teardown_=False):
    if type_ == "classic" and setup_:
        return BashOperator(task_id=name, bash_command="echo 1").as_setup()
    elif type_ == "classic" and teardown_:
        return BashOperator(task_id=name, bash_command="echo 1").as_teardown()
    elif type_ == "classic":
        return BashOperator(task_id=name, bash_command="echo 1")
    elif setup_:

        @setup
        def setuptask():
            pass

        return setuptask.override(task_id=name)()
    elif teardown_:

        @teardown
        def teardowntask():
            pass

        return teardowntask.override(task_id=name)()
    else:

        @task
        def my_task():
            pass

        return my_task.override(task_id=name)()


class TestSetupTearDownTask:
    def test_marking_functions_as_setup_task(self, dag_maker):
        @setup
        def mytask():
            print("I am a setup task")

        with dag_maker() as dag:
            mytask()

        assert len(dag.task_group.children) == 1
        setup_task = dag.task_group.children["mytask"]
        assert setup_task.is_setup

    def test_marking_functions_as_teardown_task(self, dag_maker):
        @teardown
        def mytask():
            print("I am a teardown task")

        with dag_maker() as dag:
            mytask()

        assert len(dag.task_group.children) == 1
        teardown_task = dag.task_group.children["mytask"]
        assert teardown_task.is_teardown

    def test_marking_decorated_functions_as_setup_task(self, dag_maker):
        @setup
        @task
        def mytask():
            print("I am a setup task")

        with dag_maker() as dag:
            mytask()

        assert len(dag.task_group.children) == 1
        setup_task = dag.task_group.children["mytask"]
        assert setup_task.is_setup

    def test_marking_operator_as_setup_task(self, dag_maker):
        with dag_maker() as dag:
            BashOperator(task_id="mytask", bash_command='echo "I am a setup task"').as_setup()

        assert len(dag.task_group.children) == 1
        setup_task = dag.task_group.children["mytask"]
        assert setup_task.is_setup

    def test_marking_decorated_functions_as_teardown_task(self, dag_maker):
        @teardown
        @task
        def mytask():
            print("I am a teardown task")

        with dag_maker() as dag:
            mytask()

        assert len(dag.task_group.children) == 1
        teardown_task = dag.task_group.children["mytask"]
        assert teardown_task.is_teardown

    def test_marking_operator_as_teardown_task(self, dag_maker):
        with dag_maker() as dag:
            BashOperator(task_id="mytask", bash_command='echo "I am a setup task"').as_teardown()

        assert len(dag.task_group.children) == 1
        teardown_task = dag.task_group.children["mytask"]
        assert teardown_task.is_teardown

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
        assert teardown_task.is_teardown
        assert teardown_task.on_failure_fail_dagrun is on_failure_fail_dagrun
        assert len(dag.task_group.children) == 1

    @pytest.mark.parametrize("on_failure_fail_dagrun", [True, False])
    def test_classic_teardown_task_works_with_on_failure_fail_dagrun(self, on_failure_fail_dagrun, dag_maker):
        with dag_maker() as dag:
            BashOperator(
                task_id="mytask",
                bash_command='echo "I am a teardown task"',
            ).as_teardown(on_failure_fail_dagrun=on_failure_fail_dagrun)

        teardown_task = dag.task_group.children["mytask"]
        assert teardown_task.is_teardown
        assert teardown_task.on_failure_fail_dagrun is on_failure_fail_dagrun
        assert len(dag.task_group.children) == 1

    def test_setup_task_can_be_overridden(self, dag_maker):
        @setup
        def mytask():
            print("I am a setup task")

        with dag_maker() as dag:
            mytask.override(task_id="mytask2")()
        assert len(dag.task_group.children) == 1
        setup_task = dag.task_group.children["mytask2"]
        assert setup_task.is_setup

    def test_teardown_on_failure_fail_dagrun_can_be_overridden(self, dag_maker):
        @teardown
        def mytask():
            print("I am a teardown task")

        with dag_maker() as dag:
            mytask.override(on_failure_fail_dagrun=True)()
        assert len(dag.task_group.children) == 1
        teardown_task = dag.task_group.children["mytask"]
        assert teardown_task.is_teardown
        assert teardown_task.on_failure_fail_dagrun

    def test_retain_on_failure_fail_dagrun_when_other_attrs_are_overridden(self, dag_maker):
        @teardown(on_failure_fail_dagrun=True)
        def mytask():
            print("I am a teardown task")

        with dag_maker() as dag:
            mytask.override(task_id="mytask2")()
        assert len(dag.task_group.children) == 1
        teardown_task = dag.task_group.children["mytask2"]
        assert teardown_task.is_teardown
        assert teardown_task.on_failure_fail_dagrun

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
        assert sum(1 for x in dag.tasks if not x.downstream_list) == 6
        assert dag.task_group.children["setuptask"].is_setup
        assert dag.task_group.children["teardowntask"].is_teardown
        assert dag.task_group.children["setuptask2"].is_setup
        assert dag.task_group.children["teardowntask2"].is_teardown
        assert dag.task_group.children["mytask"].is_setup is False
        assert dag.task_group.children["mytask"].is_teardown is False
        assert dag.task_group.children["mytask2"].is_setup is False
        assert dag.task_group.children["mytask2"].is_teardown is False

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
            setuptask = BashOperator(task_id="setuptask", bash_command="echo 1").as_setup()
            setuptask2 = BashOperator(task_id="setuptask2", bash_command="echo 1").as_setup()

            teardowntask = BashOperator(task_id="teardowntask", bash_command="echo 1").as_teardown()
            teardowntask2 = BashOperator(task_id="teardowntask2", bash_command="echo 1").as_teardown()
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
            setuptask = BashOperator(task_id="setuptask", bash_command="echo 1").as_setup()
            setuptask2 = BashOperator(task_id="setuptask2", bash_command="echo 1").as_setup()

            teardowntask = BashOperator(task_id="teardowntask", bash_command="echo 1").as_teardown()
            teardowntask2 = BashOperator(task_id="teardowntask2", bash_command="echo 1").as_teardown()
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
            setuptask = BashOperator(task_id="setuptask", bash_command="echo 1").as_setup()
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
            teardowntask = BashOperator(task_id="teardowntask", bash_command="echo 1").as_teardown()
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
            setuptask = BashOperator(task_id="setuptask", bash_command="echo 1").as_setup()
            setuptask2 = BashOperator(task_id="setuptask2", bash_command="echo 1").as_setup()

            teardowntask = BashOperator(task_id="teardowntask", bash_command="echo 1").as_teardown()
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
            setuptask = BashOperator(task_id="setuptask", bash_command="echo 1").as_setup()
            setuptask2 = BashOperator(task_id="setuptask2", bash_command="echo 1").as_setup()
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
            setuptask = BashOperator(task_id="setuptask", bash_command="echo 1").as_setup()
            setuptask2 = BashOperator(task_id="setuptask2", bash_command="echo 1").as_setup()
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
            setuptask = BashOperator(task_id="setuptask", bash_command="echo 1").as_setup()
            setuptask2 = BashOperator(task_id="setuptask2", bash_command="echo 1").as_setup()

            teardowntask = BashOperator(task_id="teardowntask", bash_command="echo 1").as_teardown()
            teardowntask2 = BashOperator(task_id="teardowntask2", bash_command="echo 1").as_teardown()
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

    def test_setup_decorator_context_manager_with_list_on_left(self, dag_maker):
        @setup
        def setuptask():
            print("setup")

        @setup
        def setuptask2():
            print("setup")

        @task()
        def mytask():
            print("mytask")

        @teardown
        def teardowntask():
            print("teardown")

        with dag_maker() as dag:
            with [setuptask(), setuptask2()] >> teardowntask():
                mytask()

        assert len(dag.task_group.children) == 4
        assert not dag.task_group.children["setuptask"].upstream_task_ids
        assert not dag.task_group.children["setuptask2"].upstream_task_ids
        assert dag.task_group.children["setuptask"].downstream_task_ids == {"mytask", "teardowntask"}
        assert dag.task_group.children["setuptask2"].downstream_task_ids == {"mytask", "teardowntask"}
        assert dag.task_group.children["mytask"].upstream_task_ids == {"setuptask", "setuptask2"}
        assert dag.task_group.children["mytask"].downstream_task_ids == {"teardowntask"}
        assert dag.task_group.children["teardowntask"].upstream_task_ids == {
            "setuptask",
            "setuptask2",
            "mytask",
        }

    def test_setup_decorator_context_manager_with_list_on_right(self, dag_maker):
        @setup
        def setuptask():
            print("setup")

        @setup
        def setuptask2():
            print("setup")

        @task()
        def mytask():
            print("mytask")

        @teardown
        def teardowntask():
            print("teardown")

        with dag_maker() as dag:
            with teardowntask() << context_wrapper([setuptask(), setuptask2()]):
                mytask()

        assert len(dag.task_group.children) == 4
        assert not dag.task_group.children["setuptask"].upstream_task_ids
        assert not dag.task_group.children["setuptask2"].upstream_task_ids
        assert dag.task_group.children["setuptask"].downstream_task_ids == {"mytask", "teardowntask"}
        assert dag.task_group.children["setuptask2"].downstream_task_ids == {"mytask", "teardowntask"}
        assert dag.task_group.children["mytask"].upstream_task_ids == {"setuptask", "setuptask2"}
        assert dag.task_group.children["mytask"].downstream_task_ids == {"teardowntask"}
        assert dag.task_group.children["teardowntask"].upstream_task_ids == {
            "setuptask",
            "setuptask2",
            "mytask",
        }

    def test_setup_decorator_context_manager_errors_with_mixed_up_tasks(self, dag_maker):
        @setup
        def setuptask():
            print("setup")

        @setup
        def setuptask2():
            print("setup")

        @task()
        def mytask():
            print("mytask")

        @teardown
        def teardowntask():
            print("teardown")

        with pytest.raises(ValueError, match="All tasks in the list must be either setup or teardown tasks"):
            with dag_maker():
                with setuptask() << context_wrapper([teardowntask(), setuptask2()]):
                    mytask()

    def test_teardown_decorator_context_manager_with_list_on_left(self, dag_maker):
        @setup
        def setuptask():
            print("setup")

        @task()
        def mytask():
            print("mytask")

        @teardown
        def teardowntask():
            print("teardown")

        @teardown
        def teardowntask2():
            print("teardown")

        with dag_maker() as dag:
            with [teardowntask(), teardowntask2()] << setuptask():
                mytask()

        assert len(dag.task_group.children) == 4
        assert not dag.task_group.children["setuptask"].upstream_task_ids
        assert dag.task_group.children["setuptask"].downstream_task_ids == {
            "mytask",
            "teardowntask",
            "teardowntask2",
        }
        assert dag.task_group.children["mytask"].upstream_task_ids == {"setuptask"}
        assert dag.task_group.children["mytask"].downstream_task_ids == {"teardowntask", "teardowntask2"}
        assert dag.task_group.children["teardowntask"].upstream_task_ids == {
            "setuptask",
            "mytask",
        }
        assert dag.task_group.children["teardowntask2"].upstream_task_ids == {
            "setuptask",
            "mytask",
        }

    def test_teardown_decorator_context_manager_with_list_on_right(self, dag_maker):
        @setup
        def setuptask():
            print("setup")

        @task()
        def mytask():
            print("mytask")

        @teardown
        def teardowntask():
            print("teardown")

        @teardown
        def teardowntask2():
            print("teardown")

        with dag_maker() as dag:
            with setuptask() >> context_wrapper([teardowntask(), teardowntask2()]):
                mytask()

        assert len(dag.task_group.children) == 4
        assert not dag.task_group.children["setuptask"].upstream_task_ids
        assert dag.task_group.children["setuptask"].downstream_task_ids == {
            "mytask",
            "teardowntask",
            "teardowntask2",
        }
        assert dag.task_group.children["mytask"].upstream_task_ids == {"setuptask"}
        assert dag.task_group.children["mytask"].downstream_task_ids == {"teardowntask", "teardowntask2"}
        assert dag.task_group.children["teardowntask"].upstream_task_ids == {
            "setuptask",
            "mytask",
        }
        assert dag.task_group.children["teardowntask2"].upstream_task_ids == {
            "setuptask",
            "mytask",
        }

    def test_classic_operator_context_manager_with_list_on_left(self, dag_maker):
        @task()
        def mytask():
            print("mytask")

        with dag_maker() as dag:
            teardowntask = BashOperator(task_id="teardowntask", bash_command="echo 1").as_teardown()
            teardowntask2 = BashOperator(task_id="teardowntask2", bash_command="echo 1").as_teardown()
            setuptask = BashOperator(task_id="setuptask", bash_command="echo 1").as_setup()
            with [teardowntask, teardowntask2] << setuptask:
                mytask()

        assert len(dag.task_group.children) == 4
        assert not dag.task_group.children["setuptask"].upstream_task_ids
        assert dag.task_group.children["setuptask"].downstream_task_ids == {
            "mytask",
            "teardowntask",
            "teardowntask2",
        }
        assert dag.task_group.children["mytask"].upstream_task_ids == {"setuptask"}
        assert dag.task_group.children["mytask"].downstream_task_ids == {"teardowntask", "teardowntask2"}
        assert dag.task_group.children["teardowntask"].upstream_task_ids == {
            "setuptask",
            "mytask",
        }
        assert dag.task_group.children["teardowntask2"].upstream_task_ids == {
            "setuptask",
            "mytask",
        }

    def test_classic_operator_context_manager_with_list_on_right(self, dag_maker):
        @task()
        def mytask():
            print("mytask")

        with dag_maker() as dag:
            teardowntask = BashOperator(task_id="teardowntask", bash_command="echo 1").as_teardown()
            teardowntask2 = BashOperator(task_id="teardowntask2", bash_command="echo 1").as_teardown()
            setuptask = BashOperator(task_id="setuptask", bash_command="echo 1").as_setup()
            with setuptask >> context_wrapper([teardowntask, teardowntask2]):
                mytask()

        assert len(dag.task_group.children) == 4
        assert not dag.task_group.children["setuptask"].upstream_task_ids
        assert dag.task_group.children["setuptask"].downstream_task_ids == {
            "mytask",
            "teardowntask",
            "teardowntask2",
        }
        assert dag.task_group.children["mytask"].upstream_task_ids == {"setuptask"}
        assert dag.task_group.children["mytask"].downstream_task_ids == {"teardowntask", "teardowntask2"}
        assert dag.task_group.children["teardowntask"].upstream_task_ids == {
            "setuptask",
            "mytask",
        }
        assert dag.task_group.children["teardowntask2"].upstream_task_ids == {
            "setuptask",
            "mytask",
        }

    def test_tasks_decorators_called_outside_context_manager_can_link_up_with_scope(self, dag_maker):
        @setup
        def setuptask():
            print("setup")

        @task()
        def mytask():
            print("mytask")

        @teardown
        def teardowntask():
            print("teardown")

        with dag_maker() as dag:
            task1 = mytask()
            with setuptask() >> teardowntask() as scope:
                scope.add_task(task1)

        assert len(dag.task_group.children) == 3
        assert not dag.task_group.children["setuptask"].upstream_task_ids
        assert dag.task_group.children["setuptask"].downstream_task_ids == {"mytask", "teardowntask"}
        assert dag.task_group.children["mytask"].upstream_task_ids == {"setuptask"}
        assert dag.task_group.children["mytask"].downstream_task_ids == {"teardowntask"}
        assert dag.task_group.children["teardowntask"].upstream_task_ids == {"mytask", "setuptask"}
        assert not dag.task_group.children["teardowntask"].downstream_task_ids

    def test_classic_tasks_called_outside_context_manager_can_link_up_with_scope(self, dag_maker):
        with dag_maker() as dag:
            setuptask = BashOperator(task_id="setuptask", bash_command="echo 1").as_setup()
            teardowntask = BashOperator(task_id="teardowntask", bash_command="echo 1").as_teardown()
            mytask = BashOperator(task_id="mytask", bash_command="echo 1")
            with setuptask >> teardowntask as scope:
                scope.add_task(mytask)

        assert len(dag.task_group.children) == 3
        assert not dag.task_group.children["setuptask"].upstream_task_ids
        assert dag.task_group.children["setuptask"].downstream_task_ids == {"mytask", "teardowntask"}
        assert dag.task_group.children["mytask"].upstream_task_ids == {"setuptask"}
        assert dag.task_group.children["mytask"].downstream_task_ids == {"teardowntask"}
        assert dag.task_group.children["teardowntask"].upstream_task_ids == {"mytask", "setuptask"}
        assert not dag.task_group.children["teardowntask"].downstream_task_ids

    def test_tasks_decorators_called_outside_context_manager_can_link_up_with_scope_op(self, dag_maker):
        """Here we test that XComArg.add_ctx_task can take an Operator as argument"""

        @setup
        def setuptask():
            print("setup")

        @teardown
        def teardowntask():
            print("teardown")

        with dag_maker() as dag:
            task1 = BashOperator(task_id="mytask", bash_command="echo 1")
            with setuptask() >> teardowntask() as scope:
                scope.add_task(task1)

        assert len(dag.task_group.children) == 3
        assert not dag.task_group.children["setuptask"].upstream_task_ids
        assert dag.task_group.children["setuptask"].downstream_task_ids == {"mytask", "teardowntask"}
        assert dag.task_group.children["mytask"].upstream_task_ids == {"setuptask"}
        assert dag.task_group.children["mytask"].downstream_task_ids == {"teardowntask"}
        assert dag.task_group.children["teardowntask"].upstream_task_ids == {"mytask", "setuptask"}
        assert not dag.task_group.children["teardowntask"].downstream_task_ids

    def test_classic_tasks_called_outside_context_manager_can_link_up_with_scope_xcomarg(self, dag_maker):
        """Here we test we can add xcom arg task to a scope using the BaseOperator.add_ctx_task method"""

        @task
        def mytask():
            return 1

        with dag_maker() as dag:
            setuptask = BashOperator(task_id="setuptask", bash_command="echo 1").as_setup()
            teardowntask = BashOperator(task_id="teardowntask", bash_command="echo 1").as_teardown()
            with setuptask >> teardowntask as scope:
                scope.add_task(mytask())

        assert len(dag.task_group.children) == 3
        assert not dag.task_group.children["setuptask"].upstream_task_ids
        assert dag.task_group.children["setuptask"].downstream_task_ids == {"mytask", "teardowntask"}
        assert dag.task_group.children["mytask"].upstream_task_ids == {"setuptask"}
        assert dag.task_group.children["mytask"].downstream_task_ids == {"teardowntask"}
        assert dag.task_group.children["teardowntask"].upstream_task_ids == {"mytask", "setuptask"}
        assert not dag.task_group.children["teardowntask"].downstream_task_ids

    def test_add_tasks_to_context_for_different_context_level(self, dag_maker):
        @setup
        def setuptask():
            print("setup")

        @teardown
        def teardowntask():
            print("teardown")

        @task
        def mytask():
            return 1

        with dag_maker() as dag:
            task1 = mytask()
            setuptask2 = BashOperator(task_id="setuptask2", bash_command="echo 1").as_setup()
            teardowntask2 = BashOperator(task_id="teardowntask2", bash_command="echo 1").as_teardown()
            task2 = BashOperator(task_id="mytask2", bash_command="echo 1")

            with setuptask() >> teardowntask() as scope:
                scope.add_task(task1)
                with setuptask2 >> teardowntask2 as scope2:
                    scope2.add_task(task2)

        assert len(dag.task_group.children) == 6
        assert not dag.task_group.children["setuptask"].upstream_task_ids
        assert dag.task_group.children["setuptask"].downstream_task_ids == {
            "setuptask2",
            "mytask",
            "teardowntask",
        }
        assert dag.task_group.children["mytask"].upstream_task_ids == {"setuptask"}
        assert dag.task_group.children["mytask"].downstream_task_ids == {"teardowntask"}
        assert dag.task_group.children["teardowntask"].upstream_task_ids == {
            "mytask",
            "setuptask",
            "teardowntask2",
        }
        assert dag.task_group.children["setuptask2"].upstream_task_ids == {"setuptask"}
        assert dag.task_group.children["setuptask2"].downstream_task_ids == {"mytask2", "teardowntask2"}
        assert dag.task_group.children["mytask2"].upstream_task_ids == {"setuptask2"}
        assert dag.task_group.children["mytask2"].downstream_task_ids == {"teardowntask2"}
        assert dag.task_group.children["teardowntask2"].upstream_task_ids == {"mytask2", "setuptask2"}

    def test_check_for_circular_dependency(self, dag_maker):
        with dag_maker() as dag:
            s1 = make_task("s1", type_="classic", setup_=True)
            s2 = make_task("s2", type_="classic", setup_=True)
            t1 = make_task("t1", type_="classic", teardown_=True)
            t2 = make_task("t2", type_="classic", teardown_=True)

            s1 >> s2
            s1 >> t1
            s2 >> t1
            s1 >> t2
            s2 >> t2
            with t1, t2:
                make_task("work_task", type_="classic")

        dag.validate()

        assert dag.task_group.children.keys() == {"s1", "s2", "t1", "t2", "work_task"}
        assert dag.task_group.children["s1"].downstream_task_ids == {"s2", "work_task", "t1", "t2"}
        assert dag.task_group.children["s2"].downstream_task_ids == {"work_task", "t1", "t2"}
        assert dag.task_group.children["t2"].downstream_task_ids == {"t1"}

    def test_mixing_construct_with_add_task(self, dag_maker):
        with dag_maker() as dag:
            s1 = make_task("s1", type_="classic")
            s2 = make_task("s2", type_="classic")
            t1 = make_task("t1", type_="classic")
            t2 = make_task("t2", type_="classic")
            t1.as_teardown(setups=s1)
            t2.as_teardown(setups=s2)
            with t1:
                work = make_task("work", type_="classic")
            with t2 as scope:
                scope.add_task(work)

        assert dag.task_group.children.keys() == {"s1", "s2", "t1", "t2", "work"}
        assert dag.task_group.children["s1"].downstream_task_ids == {"work", "t1"}
        assert dag.task_group.children["s2"].downstream_task_ids == {"work", "t2"}
        assert not dag.task_group.children["t1"].downstream_task_ids
        assert not dag.task_group.children["t2"].downstream_task_ids
        assert dag.task_group.children["work"].downstream_task_ids == {"t1", "t2"}
