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

import typing
from typing import TYPE_CHECKING

import pytest

from airflow.sdk import TaskInstanceState
from airflow.sdk.definitions.dag import DAG
from airflow.sdk.definitions.decorators import task

if TYPE_CHECKING:
    from airflow.sdk.definitions.context import Context


def test_skip_if(run_task):
    with DAG(dag_id="test_skip_if", schedule=None) as dag:

        @task.skip_if(lambda context: context["task_instance"].task_id == "do_skip")
        @task.python()
        def f(): ...

        f.override(task_id="do_skip")()
        f.override(task_id="do_not_skip")()

    do_skip = dag.get_task("do_skip")
    do_not_skip = dag.get_task("do_not_skip")

    run_task(do_skip)
    assert run_task.state == TaskInstanceState.SKIPPED

    run_task(do_not_skip)
    assert run_task.state == TaskInstanceState.SUCCESS


def test_run_if(run_task):
    with DAG(dag_id="test_run_if", schedule=None) as dag:

        @task.run_if(lambda context: context["task_instance"].task_id == "do_run")
        @task.python()
        def f(): ...

        f.override(task_id="do_run")()
        f.override(task_id="do_not_run")()

    do_run = dag.get_task("do_run")
    do_not_run = dag.get_task("do_not_run")

    run_task(do_run)
    assert run_task.state == TaskInstanceState.SUCCESS

    run_task(do_not_run)
    assert run_task.state == TaskInstanceState.SKIPPED


def test_skip_if_with_non_task_error():
    with pytest.raises(TypeError):

        @task.skip_if(lambda _: True)
        def f(): ...


def test_run_if_with_non_task_error():
    with pytest.raises(TypeError):

        @task.run_if(lambda _: True)
        def f(): ...


def test_skip_if_with_other_pre_execute(run_task):
    def setup_conf(context: Context) -> None:
        if typing.TYPE_CHECKING:
            assert context["dag_run"].conf
        context["dag_run"].conf = {"some_key": "some_value"}

    with DAG(dag_id="test_skip_if_with_other_pre_execute", schedule=None) as dag:

        @task.skip_if(lambda context: context["dag_run"].conf.get("some_key") == "some_value")
        @task.python(pre_execute=setup_conf)
        def f(): ...

        f()

    t = dag.get_task("f")
    run_task(t)

    assert run_task.state == TaskInstanceState.SKIPPED


def test_run_if_with_other_pre_execute(run_task):
    def setup_conf(context: Context) -> None:
        if typing.TYPE_CHECKING:
            assert context["dag_run"].conf
        context["dag_run"].conf = {"some_key": "some_value"}

    with DAG(dag_id="test_run_if_with_other_pre_execute", schedule=None) as dag:

        @task.run_if(lambda context: context["dag_run"].conf.get("some_key") == "some_value")
        @task.python(pre_execute=setup_conf)
        def f(): ...

        f()

    t = dag.get_task("f")
    run_task(t)

    assert run_task.state == TaskInstanceState.SUCCESS
