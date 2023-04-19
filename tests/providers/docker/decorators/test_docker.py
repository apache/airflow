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

from airflow.decorators import setup, task, teardown
from airflow.exceptions import AirflowException
from airflow.models import TaskInstance
from airflow.models.dag import DAG
from airflow.settings import _ENABLE_AIP_52
from airflow.utils import timezone
from airflow.utils.state import TaskInstanceState

DEFAULT_DATE = timezone.datetime(2021, 9, 1)


class TestDockerDecorator:
    def test_basic_docker_operator(self, dag_maker):
        @task.docker(image="python:3.9-slim", auto_remove="force")
        def f():
            import random

            return [random.random() for _ in range(100)]

        with dag_maker():
            ret = f()

        dr = dag_maker.create_dagrun()
        ret.operator.run(start_date=dr.execution_date, end_date=dr.execution_date)
        ti = dr.get_task_instances()[0]
        assert len(ti.xcom_pull()) == 100

    def test_basic_docker_operator_with_param(self, dag_maker):
        @task.docker(image="python:3.9-slim", auto_remove="force")
        def f(num_results):
            import random

            return [random.random() for _ in range(num_results)]

        with dag_maker():
            ret = f(50)

        dr = dag_maker.create_dagrun()
        ret.operator.run(start_date=dr.execution_date, end_date=dr.execution_date)
        ti = dr.get_task_instances()[0]
        result = ti.xcom_pull()
        assert isinstance(result, list)
        assert len(result) == 50

    def test_basic_docker_operator_with_template_fields(self, dag_maker):
        @task.docker(image="python:3.9-slim", container_name="python_{{dag_run.dag_id}}", auto_remove="force")
        def f():
            raise RuntimeError("Should not executed")

        with dag_maker():
            ret = f()

        dr = dag_maker.create_dagrun()
        ti = TaskInstance(task=ret.operator, run_id=dr.run_id)
        rendered = ti.render_templates()
        assert rendered.container_name == f"python_{dr.dag_id}"

    def test_basic_docker_operator_multiple_output(self, dag_maker):
        @task.docker(image="python:3.9-slim", multiple_outputs=True, auto_remove="force")
        def return_dict(number: int):
            return {"number": number + 1, "43": 43}

        test_number = 10
        with dag_maker():
            ret = return_dict(test_number)

        dr = dag_maker.create_dagrun()
        ret.operator.run(start_date=dr.execution_date, end_date=dr.execution_date)

        ti = dr.get_task_instances()[0]
        assert ti.xcom_pull(key="number") == test_number + 1
        assert ti.xcom_pull(key="43") == 43
        assert ti.xcom_pull() == {"number": test_number + 1, "43": 43}

    def test_no_return(self, dag_maker):
        @task.docker(image="python:3.9-slim", auto_remove="force")
        def f():
            pass

        with dag_maker():
            ret = f()

        dr = dag_maker.create_dagrun()
        ret.operator.run(start_date=dr.execution_date, end_date=dr.execution_date)
        ti = dr.get_task_instances()[0]
        assert ti.xcom_pull() is None

    def test_call_decorated_multiple_times(self):
        """Test calling decorated function 21 times in a DAG"""

        @task.docker(image="python:3.9-slim", network_mode="bridge", api_version="auto", auto_remove="force")
        def do_run():
            return 4

        with DAG("test", start_date=DEFAULT_DATE) as dag:
            do_run()
            for _ in range(20):
                do_run()

        assert len(dag.task_ids) == 21
        assert dag.task_ids[-1] == "do_run__20"

    @pytest.mark.parametrize(
        "extra_kwargs, actual_exit_code, expected_state",
        [
            (None, 99, TaskInstanceState.FAILED),
            ({"skip_on_exit_code": 100}, 100, TaskInstanceState.SKIPPED),
            ({"skip_on_exit_code": 100}, 101, TaskInstanceState.FAILED),
            ({"skip_on_exit_code": None}, 0, TaskInstanceState.SUCCESS),
        ],
    )
    def test_skip_docker_operator(self, extra_kwargs, actual_exit_code, expected_state, dag_maker):
        @task.docker(image="python:3.9-slim", auto_remove="force", **(extra_kwargs if extra_kwargs else {}))
        def f(exit_code):
            raise SystemExit(exit_code)

        with dag_maker():
            ret = f(actual_exit_code)

        dr = dag_maker.create_dagrun()
        if expected_state == TaskInstanceState.FAILED:
            with pytest.raises(AirflowException):
                ret.operator.run(start_date=dr.execution_date, end_date=dr.execution_date)
        else:
            ret.operator.run(start_date=dr.execution_date, end_date=dr.execution_date)
            ti = dr.get_task_instances()[0]
            assert ti.state == expected_state

    @pytest.mark.skipif(not _ENABLE_AIP_52, reason="AIP-52 is disabled")
    def test_setup_decorator_with_decorated_docker_task(self, dag_maker):
        @setup
        @task.docker(image="python:3.9-slim", auto_remove="force")
        def f():
            pass

        with dag_maker() as dag:
            f()

        assert len(dag.task_group.children) == 1
        setup_task = dag.task_group.children["f"]
        assert setup_task._is_setup

    @pytest.mark.skipif(not _ENABLE_AIP_52, reason="AIP-52 is disabled")
    def test_teardown_decorator_with_decorated_docker_task(self, dag_maker):
        @teardown
        @task.docker(image="python:3.9-slim", auto_remove="force")
        def f():
            pass

        with dag_maker() as dag:
            f()

        assert len(dag.task_group.children) == 1
        teardown_task = dag.task_group.children["f"]
        assert teardown_task._is_teardown

    @pytest.mark.skipif(not _ENABLE_AIP_52, reason="AIP-52 is disabled")
    @pytest.mark.parametrize("on_failure_fail_dagrun", [True, False])
    def test_teardown_decorator_with_decorated_docker_task_and_on_failure_fail_arg(
        self, dag_maker, on_failure_fail_dagrun
    ):
        @teardown(on_failure_fail_dagrun=on_failure_fail_dagrun)
        @task.docker(image="python:3.9-slim", auto_remove="force")
        def f():
            pass

        with dag_maker() as dag:
            f()

        assert len(dag.task_group.children) == 1
        teardown_task = dag.task_group.children["f"]
        assert teardown_task._is_teardown
        assert teardown_task._on_failure_fail_dagrun is on_failure_fail_dagrun
