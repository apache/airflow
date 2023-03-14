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
from airflow.models.baseoperator import BaseOperator
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup


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
        from airflow.operators.bash import BashOperator

        with dag_maker() as dag:
            BashOperator.as_teardown(task_id="mytask", bash_command='echo "I am a setup task"')

        assert len(dag.task_group.children) == 1
        teardown_task = dag.task_group.children["mytask"]
        assert teardown_task._is_teardown

    def test_setup_taskgroup_decorator(self, dag_maker):
        @setup
        @task_group
        def mygroup():
            @task
            def mytask():
                print("I am a setup task")

            mytask()

        with dag_maker() as dag:
            mygroup()

        assert len(dag.task_group.children) == 1
        setup_task_group = dag.task_group.children["mygroup"]
        assert len(setup_task_group.children) == 1
        setup_task = setup_task_group.children["mygroup.mytask"]
        assert setup_task._is_setup

    def test_teardown_taskgroup_decorator(self, dag_maker):
        @teardown
        @task_group
        def mygroup():
            @task
            def mytask():
                print("I am a teardown task")

            mytask()

        with dag_maker() as dag:
            mygroup()

        assert len(dag.task_group.children) == 1
        teardown_task_group = dag.task_group.children["mygroup"]
        assert len(teardown_task_group.children) == 1
        teardown_task = teardown_task_group.children["mygroup.mytask"]
        assert teardown_task._is_teardown

    def test_setup_taskgroup_classic(self, dag_maker):
        with dag_maker() as dag:
            with TaskGroup("mygroup", setup=True):
                BashOperator(task_id="mytask", bash_command='echo "I am a setup task"')

        assert len(dag.task_group.children) == 1
        setup_task_group = dag.task_group.children["mygroup"]
        assert len(setup_task_group.children) == 1
        setup_task = setup_task_group.children["mygroup.mytask"]
        assert setup_task._is_setup

    def test_teardown_taskgroup_classic(self, dag_maker):
        with dag_maker() as dag:
            with TaskGroup("mygroup", teardown=True):
                BashOperator(task_id="mytask", bash_command='echo "I am a setup task"')

        assert len(dag.task_group.children) == 1
        teardown_task_group = dag.task_group.children["mygroup"]
        assert len(teardown_task_group.children) == 1
        teardown_task = teardown_task_group.children["mygroup.mytask"]
        assert teardown_task._is_teardown

    def test_setup_taskgroup_decorator_with_subgroup(self, dag_maker):
        @setup
        @task_group
        def mygroup():
            @task
            def mytask():
                print("I am a setup task")

            @task_group
            def subgroup():
                @task
                def mytask2():
                    print("I am a task")

                mytask2()

            mytask()
            subgroup()

        with dag_maker() as dag:
            mygroup()

        assert len(dag.task_group.children) == 1
        setup_task_group = dag.task_group.children["mygroup"]
        assert len(setup_task_group.children) == 2
        setup_task = setup_task_group.children["mygroup.mytask"]
        assert setup_task._is_setup
        subgroup_task_group = setup_task_group.children["mygroup.subgroup"]
        assert len(subgroup_task_group.children) == 1
        subgroup_task = subgroup_task_group.children["mygroup.subgroup.mytask2"]
        assert subgroup_task._is_setup

    def test_teardown_taskgroup_decorator_with_subgroup(self, dag_maker):
        @teardown
        @task_group
        def mygroup():
            @task
            def mytask():
                print("I am a teardown task")

            @task_group
            def subgroup():
                @task
                def mytask2():
                    print("I am a task")

                mytask2()

            mytask()
            subgroup()

        with dag_maker() as dag:
            mygroup()

        assert len(dag.task_group.children) == 1
        teardown_task_group = dag.task_group.children["mygroup"]
        assert len(teardown_task_group.children) == 2
        teardown_task = teardown_task_group.children["mygroup.mytask"]
        assert teardown_task._is_teardown
        subgroup_task_group = teardown_task_group.children["mygroup.subgroup"]
        assert len(subgroup_task_group.children) == 1
        subgroup_task = subgroup_task_group.children["mygroup.subgroup.mytask2"]
        assert subgroup_task._is_teardown

    def test_teardown_taskgroup_with_subgroup_classic(self, dag_maker):
        with dag_maker() as dag:
            with TaskGroup("mygroup", teardown=True):

                @task
                def mytask():
                    print("I am a teardown task")

                with TaskGroup("subgroup"):

                    @task
                    def mytask2():
                        print("I am a teardown task")

                    mytask2()

                mytask()

        assert len(dag.task_group.children) == 1
        teardown_task_group = dag.task_group.children["mygroup"]
        assert len(teardown_task_group.children) == 2
        teardown_task = teardown_task_group.children["mygroup.mytask"]
        assert teardown_task._is_teardown
        subgroup_task_group = teardown_task_group.children["mygroup.subgroup"]
        assert len(subgroup_task_group.children) == 1
        subgroup_task = subgroup_task_group.children["mygroup.subgroup.mytask2"]
        assert subgroup_task._is_teardown

    def test_setup_taskgroup_with_subgroup_classic(self, dag_maker):
        with dag_maker() as dag:
            with TaskGroup("mygroup", setup=True):

                @task
                def mytask():
                    print("I am a setup task")

                with TaskGroup("subgroup"):

                    @task
                    def mytask2():
                        print("I am a setup task")

                    mytask2()

                mytask()

        assert len(dag.task_group.children) == 1
        setup_task_group = dag.task_group.children["mygroup"]
        assert len(setup_task_group.children) == 2
        setup_task = setup_task_group.children["mygroup.mytask"]
        assert setup_task._is_setup
        subgroup_task_group = setup_task_group.children["mygroup.subgroup"]
        assert len(subgroup_task_group.children) == 1
        subgroup_task = subgroup_task_group.children["mygroup.subgroup.mytask2"]
        assert subgroup_task._is_setup

    def test_setup_taskgroup_with_subgroup_being_the_setup_decorated(self, dag_maker):
        """Here, the subgroup is the setup taskgroup while the parent is not"""
        with dag_maker() as dag:

            @task_group
            def mygroup():
                @task
                def mytask():
                    print("I am a task")

                @setup
                @task_group
                def subgroup():
                    @task
                    def mytask2():
                        print("I am a setup task")

                    mytask2()

                subgroup()
                mytask()

            mygroup()
        assert len(dag.task_group.children) == 1
        normal_task_group = dag.task_group.children["mygroup"]
        assert isinstance(normal_task_group, TaskGroup)
        assert len(normal_task_group.children) == 2
        normal_task = normal_task_group.children["mygroup.mytask"]
        assert isinstance(normal_task, BaseOperator)
        assert not normal_task._is_setup
        setup_task_group = normal_task_group.children["mygroup.subgroup"]
        assert isinstance(setup_task_group, TaskGroup)
        assert len(setup_task_group.children) == 1
        subgroup_task = setup_task_group.children["mygroup.subgroup.mytask2"]
        assert isinstance(subgroup_task, BaseOperator)
        assert subgroup_task._is_setup

    def test_teardown_taskgroup_with_subgroup_being_the_teardown_decorated(self, dag_maker):
        """Here, the subgroup is the teardown taskgroup while the parent is not"""

        @task_group
        def mygroup():
            @task
            def mytask():
                print("I am not a teardown task")

            @teardown
            @task_group
            def subgroup():
                @task
                def mytask2():
                    print("I am a teardown task")

                mytask2()

            mytask()
            subgroup()

        with dag_maker() as dag:
            mygroup()

        assert len(dag.task_group.children) == 1
        normal_task_group = dag.task_group.children["mygroup"]
        assert isinstance(normal_task_group, TaskGroup)
        assert len(normal_task_group.children) == 2
        normal_task = normal_task_group.children["mygroup.mytask"]
        assert isinstance(normal_task, BaseOperator)
        assert not normal_task._is_teardown
        teardown_task_group = normal_task_group.children["mygroup.subgroup"]
        assert isinstance(teardown_task_group, TaskGroup)
        assert len(teardown_task_group.children) == 1
        subgroup_task = teardown_task_group.children["mygroup.subgroup.mytask2"]
        assert isinstance(subgroup_task, BaseOperator)
        assert subgroup_task._is_teardown

    def test_setup_taskgroup_with_subgroup_being_the_setup_classic(self, dag_maker):
        """Here, the subgroup is the setup taskgroup while the parent is not"""
        with dag_maker() as dag:
            with TaskGroup("mygroup"):

                @task
                def mytask():
                    print("I am not a setup task")

                with TaskGroup("subgroup", setup=True):

                    @task
                    def mytask2():
                        print("I am a setup task")

                    mytask2()

                mytask()

        assert len(dag.task_group.children) == 1
        normal_task_group = dag.task_group.children["mygroup"]
        assert isinstance(normal_task_group, TaskGroup)
        assert len(normal_task_group.children) == 2
        normal_task = normal_task_group.children["mygroup.mytask"]
        assert isinstance(normal_task, BaseOperator)
        assert not normal_task._is_setup
        setup_task_group = normal_task_group.children["mygroup.subgroup"]
        assert isinstance(setup_task_group, TaskGroup)
        assert len(setup_task_group.children) == 1
        subgroup_task = setup_task_group.children["mygroup.subgroup.mytask2"]
        assert isinstance(subgroup_task, BaseOperator)
        assert subgroup_task._is_setup

    def test_teardown_taskgroup_with_subgroup_being_the_teardown_classic(self, dag_maker):
        """Here, the subgroup is the teardown taskgroup while the parent is not"""
        with dag_maker() as dag:
            with TaskGroup("mygroup"):

                @task
                def mytask():
                    print("I am not a teardown task")

                with TaskGroup("subgroup", teardown=True):

                    @task
                    def mytask2():
                        print("I am a teardown task")

                    mytask2()

                mytask()

        assert len(dag.task_group.children) == 1
        normal_task_group = dag.task_group.children["mygroup"]
        assert isinstance(normal_task_group, TaskGroup)
        assert len(normal_task_group.children) == 2
        normal_task = normal_task_group.children["mygroup.mytask"]
        assert isinstance(normal_task, BaseOperator)
        assert not normal_task._is_teardown
        teardown_task_group = normal_task_group.children["mygroup.subgroup"]
        assert isinstance(teardown_task_group, TaskGroup)
        assert len(teardown_task_group.children) == 1
        subgroup_task = teardown_task_group.children["mygroup.subgroup.mytask2"]
        assert isinstance(subgroup_task, BaseOperator)
        assert subgroup_task._is_teardown

    def test_setup_taskgroup_using_alternative_syntax(self, dag_maker):
        """Here, the subgroup is the setup taskgroup while the parent is not"""
        with dag_maker() as dag:
            tg1 = TaskGroup("mygroup")

            @task(task_group=tg1)
            def mytask():
                print("I am not a setup task")

            tg2 = TaskGroup("subgroup", setup=True, parent_group=tg1)

            @task(task_group=tg2)
            def mytask2():
                print("I am a setup task")

            mytask2()

            mytask()

        assert len(dag.task_group.children) == 1
        normal_task_group = dag.task_group.children["mygroup"]
        assert isinstance(normal_task_group, TaskGroup)
        assert len(normal_task_group.children) == 2
        normal_task = normal_task_group.children["mygroup.mytask"]
        assert isinstance(normal_task, BaseOperator)
        assert not normal_task._is_setup
        setup_task_group = normal_task_group.children["mygroup.subgroup"]
        assert isinstance(setup_task_group, TaskGroup)
        assert len(setup_task_group.children) == 1
        subgroup_task = setup_task_group.children["mygroup.subgroup.mytask2"]
        assert isinstance(subgroup_task, BaseOperator)
        assert subgroup_task._is_setup

    def test_setup_teardown_are_mutually_exclusive_on_taskgroup(self, dag_maker):
        """Test that setup and teardown are mutually exclusive on TaskGroup"""
        with dag_maker():
            with pytest.raises(AirflowException, match="Cannot set both setup and teardown to True"):
                TaskGroup("mygroup", setup=True, teardown=True)
