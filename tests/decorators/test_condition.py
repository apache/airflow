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

from typing import TYPE_CHECKING, Any

import pytest

from airflow.decorators import task
from airflow.utils.state import TaskInstanceState

if TYPE_CHECKING:
    from airflow.models.dagrun import DagRun
    from airflow.models.taskinstance import TaskInstance
    from airflow.utils.context import Context

pytestmark = pytest.mark.db_test


def test_skip_if(dag_maker, session):
    def condition(context: Context) -> bool:
        return context["task_instance"].task_id == "do_skip"

    with dag_maker(session=session):

        @task.skip_if(condition)
        @task.python()
        def f(): ...

        f.override(task_id="do_skip")()
        f.override(task_id="do_not_skip")()

    dag_run: DagRun = dag_maker.create_dagrun()
    do_skip_ti: TaskInstance = dag_run.get_task_instance(task_id="do_skip", session=session)
    do_not_skip_ti: TaskInstance = dag_run.get_task_instance(task_id="do_not_skip", session=session)
    do_skip_ti.run(session=session)
    do_not_skip_ti.run(session=session)

    assert do_skip_ti.state == TaskInstanceState.SKIPPED
    assert do_not_skip_ti.state == TaskInstanceState.SUCCESS


def test_run_if(dag_maker, session):
    def condition(context: Context) -> bool:
        return context["task_instance"].task_id == "do_run"

    with dag_maker(session=session):

        @task.run_if(condition)
        @task.python()
        def f(): ...

        f.override(task_id="do_run")()
        f.override(task_id="do_not_run")()

    dag_run: DagRun = dag_maker.create_dagrun()
    do_run_ti: TaskInstance = dag_run.get_task_instance(task_id="do_run", session=session)
    do_not_run_ti: TaskInstance = dag_run.get_task_instance(task_id="do_not_run", session=session)
    do_run_ti.run(session=session)
    do_not_run_ti.run(session=session)

    assert do_run_ti.state == TaskInstanceState.SUCCESS
    assert do_not_run_ti.state == TaskInstanceState.SKIPPED


def test_skip_if_with_non_task_error():
    with pytest.raises(TypeError):

        @task.skip_if(lambda _: True)
        def f(): ...


def test_run_if_with_non_task_error():
    with pytest.raises(TypeError):

        @task.run_if(lambda _: True)
        def f(): ...


def test_skip_if_with_other_pre_execute(dag_maker, session):
    def setup_conf(context: Context) -> None:
        context["dag_run"].conf["some_key"] = "some_value"

    def condition(context: Context) -> bool:
        return context["dag_run"].conf.get("some_key") == "some_value"

    with dag_maker(session=session):

        @task.skip_if(condition)
        @task.python(pre_execute=setup_conf)
        def f(): ...

        f()

    dag_run: DagRun = dag_maker.create_dagrun()
    ti: TaskInstance = dag_run.get_task_instance(task_id="f", session=session)
    ti.run(session=session)

    assert ti.state == TaskInstanceState.SKIPPED


def test_run_if_with_other_pre_execute(dag_maker, session):
    def setup_conf(context: Context) -> None:
        context["dag_run"].conf["some_key"] = "some_value"

    def condition(context: Context) -> bool:
        return context["dag_run"].conf.get("some_key") == "some_value"

    with dag_maker(session=session):

        @task.run_if(condition)
        @task.python(pre_execute=setup_conf)
        def f(): ...

        f()

    dag_run: DagRun = dag_maker.create_dagrun()
    ti: TaskInstance = dag_run.get_task_instance(task_id="f", session=session)
    ti.run(session=session)

    assert ti.state == TaskInstanceState.SUCCESS


def test_skip_if_custom_msg(dag_maker, session):
    catched_args = []

    def catch_info(*args: Any, **kwargs: Any) -> None:
        catched_args.extend(args)

    def condition(context: Context) -> tuple[bool, str]:
        return context["task_instance"].task_id == "do_skip", "custom_msg"

    with dag_maker(session=session):

        @task.skip_if(condition)
        @task.python()
        def f(): ...

        f.override(task_id="do_skip")()

    dag_run: DagRun = dag_maker.create_dagrun()
    do_skip_ti: TaskInstance = dag_run.get_task_instance(task_id="do_skip", session=session)
    do_skip_ti.log.info = catch_info
    do_skip_ti.run(session=session)

    assert do_skip_ti.state == TaskInstanceState.SKIPPED
    assert catched_args
    assert catched_args[0] == "custom_msg"


def test_run_if_custom_msg(dag_maker, session):
    catched_args = []

    def catch_info(*args: Any, **kwargs: Any) -> None:
        catched_args.extend(args)

    def condition(context: Context) -> tuple[bool, str]:
        return context["task_instance"].task_id == "do_run", "custom_msg"

    with dag_maker(session=session):

        @task.skip_if(condition)
        @task.python()
        def f(): ...

        f.override(task_id="do_not_run")()

    dag_run: DagRun = dag_maker.create_dagrun()
    do_not_run_ti: TaskInstance = dag_run.get_task_instance(task_id="do_not_run", session=session)
    do_not_run_ti.log.info = catch_info
    do_not_run_ti.run(session=session)

    assert do_not_run_ti.state == TaskInstanceState.SKIPPED
    assert catched_args
    assert catched_args[0] == "custom_msg"
