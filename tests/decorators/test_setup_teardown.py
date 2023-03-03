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

import collections

from airflow.decorators import setup, task, task_group, teardown
from airflow.utils.setup_teardown import SetupTeardown


class TestSetupTearDownTask:
    def test_marking_functions_as_setup_task(self, dag_maker):
        @setup
        def mytask():
            print("I am a setup task")

        with dag_maker() as dag:
            mytask()

        setup_task = dag.task_group.all_children_by_kind[SetupTeardown.setup]["mytask"]
        assert setup_task.setup_teardown == SetupTeardown.setup

        counter = collections.Counter(dag.task_group.all_children_by_kind)
        assert counter == collections.Counter({None: 0, SetupTeardown.setup: 1, SetupTeardown.teardown: 0})

    def test_marking_functions_as_teardown_task(self, dag_maker):
        @teardown
        def mytask():
            print("I am a teardown task")

        with dag_maker() as dag:
            mytask()

        teardown_task = dag.task_group.all_children_by_kind[SetupTeardown.teardown]["mytask"]
        assert teardown_task.setup_teardown == SetupTeardown.teardown

        counter = collections.Counter(dag.task_group.all_children_by_kind)
        assert counter == collections.Counter({None: 0, SetupTeardown.setup: 0, SetupTeardown.teardown: 1})

    def test_marking_decorated_functions_as_setup_task(self, dag_maker):
        @setup
        @task
        def mytask():
            print("I am a setup task")

        with dag_maker() as dag:
            mytask()

        setup_task = dag.task_group.all_children_by_kind[SetupTeardown.setup]["mytask"]
        assert setup_task.setup_teardown == SetupTeardown.setup

        counter = collections.Counter(dag.task_group.all_children_by_kind)
        assert counter == collections.Counter({None: 0, SetupTeardown.setup: 1, SetupTeardown.teardown: 0})

    def test_marking_operator_as_setup_task(self, dag_maker):
        from airflow.operators.bash import BashOperator

        with dag_maker() as dag:
            BashOperator.as_setup(task_id="mytask", bash_command='echo "I am a setup task"')

        setup_task = dag.task_group.all_children_by_kind[SetupTeardown.setup]["mytask"]
        assert setup_task.setup_teardown == SetupTeardown.setup

        counter = collections.Counter(dag.task_group.all_children_by_kind)
        assert counter == collections.Counter({None: 0, SetupTeardown.setup: 1, SetupTeardown.teardown: 0})

    def test_marking_decorated_functions_as_teardown_task(self, dag_maker):
        @teardown
        @task
        def mytask():
            print("I am a teardown task")

        with dag_maker() as dag:
            mytask()

        teardown_task = dag.task_group.all_children_by_kind[SetupTeardown.teardown]["mytask"]
        assert teardown_task.setup_teardown == SetupTeardown.teardown

        counter = collections.Counter(dag.task_group.all_children_by_kind)
        assert counter == collections.Counter({None: 0, SetupTeardown.setup: 0, SetupTeardown.teardown: 1})

    def test_marking_operator_as_teardown_task(self, dag_maker):
        from airflow.operators.bash import BashOperator

        with dag_maker() as dag:
            BashOperator.as_teardown(task_id="mytask", bash_command='echo "I am a setup task"')

        teardown_task = dag.task_group.all_children_by_kind[SetupTeardown.teardown]["mytask"]
        assert teardown_task.setup_teardown == SetupTeardown.teardown

        counter = collections.Counter(dag.task_group.all_children_by_kind)
        assert counter == collections.Counter({None: 0, SetupTeardown.setup: 0, SetupTeardown.teardown: 1})

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

        counter = collections.Counter(dag.task_group.all_children_by_kind)
        assert counter == collections.Counter({None: 0, SetupTeardown.setup: 1, SetupTeardown.teardown: 0})

        setup_task_group = dag.task_group.all_children_by_kind[SetupTeardown.setup]["mygroup"]
        counter = collections.Counter(setup_task_group.all_children_by_kind)
        assert counter == collections.Counter({None: 0, SetupTeardown.setup: 1, SetupTeardown.teardown: 0})

        setup_task = setup_task_group.all_children_by_kind[SetupTeardown.setup]["mygroup.mytask"]
        assert setup_task.setup_teardown == SetupTeardown.setup

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

        counter = collections.Counter(dag.task_group.all_children_by_kind)
        assert counter == collections.Counter({None: 0, SetupTeardown.setup: 0, SetupTeardown.teardown: 1})

        teardown_task_group = dag.task_group.all_children_by_kind[SetupTeardown.teardown]["mygroup"]
        counter = collections.Counter(teardown_task_group.all_children_by_kind)
        assert counter == collections.Counter({None: 0, SetupTeardown.setup: 0, SetupTeardown.teardown: 1})

        teardown_task = teardown_task_group.all_children_by_kind[SetupTeardown.teardown]["mygroup.mytask"]
        assert teardown_task.setup_teardown == SetupTeardown.teardown
