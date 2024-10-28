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

import logging
from importlib.util import find_spec
from io import StringIO as StringBuffer

import pytest

from airflow.decorators import setup, task, teardown
from airflow.exceptions import AirflowException, AirflowProviderDeprecationWarning
from airflow.models import TaskInstance
from airflow.models.dag import DAG
from airflow.utils import timezone
from airflow.utils.state import TaskInstanceState

pytestmark = pytest.mark.db_test


DEFAULT_DATE = timezone.datetime(2021, 9, 1)
DILL_INSTALLED = find_spec("dill") is not None
DILL_MARKER = pytest.mark.skipif(not DILL_INSTALLED, reason="`dill` is not installed")
CLOUDPICKLE_INSTALLED = find_spec("cloudpickle") is not None
CLOUDPICKLE_MARKER = pytest.mark.skipif(
    not CLOUDPICKLE_INSTALLED, reason="`cloudpickle` is not installed"
)


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
        @task.docker(
            image="python:3.9-slim",
            container_name="python_{{dag_run.dag_id}}",
            auto_remove="force",
        )
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

        @task.docker(
            image="python:3.9-slim",
            network_mode="bridge",
            api_version="auto",
            auto_remove="force",
        )
        def do_run():
            return 4

        with DAG("test", schedule=None, start_date=DEFAULT_DATE) as dag:
            do_run()
            for _ in range(20):
                do_run()

        assert len(dag.task_ids) == 21
        assert dag.task_ids[-1] == "do_run__20"

    @pytest.mark.parametrize(
        "kwargs, actual_exit_code, expected_state",
        [
            ({}, 0, TaskInstanceState.SUCCESS),
            ({}, 100, TaskInstanceState.FAILED),
            ({}, 101, TaskInstanceState.FAILED),
            ({"skip_on_exit_code": None}, 0, TaskInstanceState.SUCCESS),
            ({"skip_on_exit_code": None}, 100, TaskInstanceState.FAILED),
            ({"skip_on_exit_code": None}, 101, TaskInstanceState.FAILED),
            ({"skip_on_exit_code": 100}, 0, TaskInstanceState.SUCCESS),
            ({"skip_on_exit_code": 100}, 100, TaskInstanceState.SKIPPED),
            ({"skip_on_exit_code": 100}, 101, TaskInstanceState.FAILED),
            ({"skip_on_exit_code": 0}, 0, TaskInstanceState.SKIPPED),
            ({"skip_on_exit_code": [100]}, 0, TaskInstanceState.SUCCESS),
            ({"skip_on_exit_code": [100]}, 100, TaskInstanceState.SKIPPED),
            ({"skip_on_exit_code": [100]}, 101, TaskInstanceState.FAILED),
            ({"skip_on_exit_code": [100, 102]}, 101, TaskInstanceState.FAILED),
            ({"skip_on_exit_code": (100,)}, 0, TaskInstanceState.SUCCESS),
            ({"skip_on_exit_code": (100,)}, 100, TaskInstanceState.SKIPPED),
            ({"skip_on_exit_code": (100,)}, 101, TaskInstanceState.FAILED),
        ],
    )
    def test_skip_docker_operator(
        self, kwargs, actual_exit_code, expected_state, dag_maker
    ):
        @task.docker(image="python:3.9-slim", auto_remove="force", **kwargs)
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

    def test_setup_decorator_with_decorated_docker_task(self, dag_maker):
        @setup
        @task.docker(image="python:3.9-slim", auto_remove="force")
        def f():
            pass

        with dag_maker() as dag:
            f()

        assert len(dag.task_group.children) == 1
        setup_task = dag.task_group.children["f"]
        assert setup_task.is_setup

    def test_teardown_decorator_with_decorated_docker_task(self, dag_maker):
        @teardown
        @task.docker(image="python:3.9-slim", auto_remove="force")
        def f():
            pass

        with dag_maker() as dag:
            f()

        assert len(dag.task_group.children) == 1
        teardown_task = dag.task_group.children["f"]
        assert teardown_task.is_teardown

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
        assert teardown_task.is_teardown
        assert teardown_task.on_failure_fail_dagrun is on_failure_fail_dagrun

    @pytest.mark.parametrize(
        "serializer",
        [
            pytest.param("pickle", id="pickle"),
            pytest.param("dill", marks=DILL_MARKER, id="dill"),
            pytest.param("cloudpickle", marks=CLOUDPICKLE_MARKER, id="cloudpickle"),
            pytest.param(None, id="default"),
        ],
    )
    def test_deepcopy_with_python_operator(self, dag_maker, serializer):
        import copy

        from airflow.providers.docker.decorators.docker import _DockerDecoratedOperator

        @task.docker(image="python:3.9-slim", auto_remove="force", serializer=serializer)
        def f():
            import logging

            logger = logging.getLogger("airflow.task")
            logger.info("info log in docker")

        @task.python()
        def g():
            import logging

            logger = logging.getLogger("airflow.task")
            logger.info("info log in python")

        with dag_maker() as dag:
            docker_task = f()
            python_task = g()
            _ = python_task >> docker_task

        docker_operator = getattr(docker_task, "operator", None)
        assert isinstance(docker_operator, _DockerDecoratedOperator)
        task_id = docker_operator.task_id

        assert isinstance(dag, DAG)
        assert hasattr(dag, "task_dict")
        assert isinstance(dag.task_dict, dict)
        assert task_id in dag.task_dict

        some_task = dag.task_dict[task_id]
        clone_of_docker_operator = copy.deepcopy(docker_operator)
        assert isinstance(some_task, _DockerDecoratedOperator)
        assert isinstance(clone_of_docker_operator, _DockerDecoratedOperator)
        assert some_task.command == clone_of_docker_operator.command
        assert some_task.expect_airflow == clone_of_docker_operator.expect_airflow
        assert some_task.serializer == clone_of_docker_operator.serializer
        assert some_task.use_dill == clone_of_docker_operator.use_dill
        assert some_task.pickling_library is clone_of_docker_operator.pickling_library

    def test_respect_docker_host_env(self, monkeypatch, dag_maker):
        monkeypatch.setenv("DOCKER_HOST", "tcp://docker-host-from-env:2375")

        @task.docker(image="python:3.9-slim", auto_remove="force")
        def f():
            pass

        with dag_maker():
            ret = f()

        assert ret.operator.docker_url == "tcp://docker-host-from-env:2375"

    def test_docker_host_env_empty(self, monkeypatch, dag_maker):
        monkeypatch.setenv("DOCKER_HOST", "")

        @task.docker(image="python:3.9-slim", auto_remove="force")
        def f():
            pass

        with dag_maker():
            ret = f()

        # The docker CLI ignores the empty string and defaults to unix://var/run/docker.sock
        # We want to ensure the same behavior.
        assert ret.operator.docker_url == "unix://var/run/docker.sock"

    def test_docker_host_env_unset(self, monkeypatch, dag_maker):
        monkeypatch.delenv("DOCKER_HOST", raising=False)

        @task.docker(image="python:3.9-slim", auto_remove="force")
        def f():
            pass

        with dag_maker():
            ret = f()

        assert ret.operator.docker_url == "unix://var/run/docker.sock"

    def test_failing_task(self, dag_maker):
        """Test regression #39319

        Check the log content of the DockerOperator when the task fails.
        """

        @task.docker(image="python:3.9-slim", auto_remove="force")
        def f():
            raise ValueError("This task is expected to fail")

        docker_operator_logger_name = "airflow.task.operators"

        docker_operator_logger = logging.getLogger(docker_operator_logger_name)
        log_capture_string = StringBuffer()
        ch = logging.StreamHandler(log_capture_string)
        docker_operator_logger.addHandler(ch)
        with dag_maker():
            ret = f()

        dr = dag_maker.create_dagrun()
        with pytest.raises(AirflowException):
            ret.operator.run(start_date=dr.execution_date, end_date=dr.execution_date)
        ti = dr.get_task_instances()[0]
        assert ti.state == TaskInstanceState.FAILED

        log_content = str(log_capture_string.getvalue())
        assert 'with open(sys.argv[4], "w") as file:' not in log_content
        last_line_of_docker_operator_log = log_content.splitlines()[-1]
        assert (
            "ValueError: This task is expected to fail"
            in last_line_of_docker_operator_log
        )

    @pytest.mark.parametrize(
        "serializer",
        [
            pytest.param("pickle", id="pickle"),
            pytest.param("dill", marks=DILL_MARKER, id="dill"),
            pytest.param("cloudpickle", marks=CLOUDPICKLE_MARKER, id="cloudpickle"),
        ],
    )
    def test_ambiguous_serializer(self, dag_maker, serializer):
        @task.docker(
            image="python:3.9-slim",
            auto_remove="force",
            use_dill=True,
            serializer=serializer,
        )
        def f():
            pass

        with dag_maker():
            with pytest.warns(
                AirflowProviderDeprecationWarning,
                match="`use_dill` is deprecated and will be removed",
            ):
                with pytest.raises(
                    AirflowException,
                    match="Both 'use_dill' and 'serializer' parameters are set",
                ):
                    f()

    def test_invalid_serializer(self, dag_maker):
        @task.docker(image="python:3.9-slim", auto_remove="force", serializer="airflow")
        def f():
            """Ensure dill is correctly installed."""
            import dill  # noqa: F401

        with dag_maker():
            with pytest.raises(
                AirflowException, match="Unsupported serializer 'airflow'"
            ):
                f()

    @pytest.mark.parametrize(
        "serializer",
        [
            pytest.param(
                "dill",
                marks=pytest.mark.skipif(
                    DILL_INSTALLED,
                    reason="For this test case `dill` shouldn't be installed",
                ),
                id="dill",
            ),
            pytest.param(
                "cloudpickle",
                marks=pytest.mark.skipif(
                    CLOUDPICKLE_INSTALLED,
                    reason="For this test case `cloudpickle` shouldn't be installed",
                ),
                id="cloudpickle",
            ),
        ],
    )
    def test_advanced_serializer_not_installed(self, dag_maker, serializer, caplog):
        """Test case for check raising an error if dill/cloudpickle is not installed."""

        @task.docker(image="python:3.9-slim", auto_remove="force", serializer=serializer)
        def f(): ...

        with dag_maker():
            with pytest.raises(ModuleNotFoundError):
                f()
        assert f"Unable to import `{serializer}` module." in caplog.text

    @CLOUDPICKLE_MARKER
    def test_add_cloudpickle(self, dag_maker):
        @task.docker(
            image="python:3.9-slim", auto_remove="force", serializer="cloudpickle"
        )
        def f():
            """Ensure cloudpickle is correctly installed."""
            import cloudpickle  # noqa: F401

        with dag_maker():
            f()

    @DILL_MARKER
    def test_add_dill(self, dag_maker):
        @task.docker(image="python:3.9-slim", auto_remove="force", serializer="dill")
        def f():
            """Ensure dill is correctly installed."""
            import dill  # noqa: F401

        with dag_maker():
            f()

    @DILL_MARKER
    def test_add_dill_use_dill(self, dag_maker):
        @task.docker(image="python:3.9-slim", auto_remove="force", use_dill=True)
        def f():
            """Ensure dill is correctly installed."""
            import dill  # noqa: F401

        with dag_maker():
            with pytest.warns(
                AirflowProviderDeprecationWarning,
                match="`use_dill` is deprecated and will be removed",
            ):
                f()
