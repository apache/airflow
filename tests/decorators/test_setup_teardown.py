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

from airflow.decorators import setup, task, task_group, teardown


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
        from airflow.operators.bash import BashOperator

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

    def test_setup_taskgroup(self, dag_maker):
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

    def test_teardown_taskgroup(self, dag_maker):
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
