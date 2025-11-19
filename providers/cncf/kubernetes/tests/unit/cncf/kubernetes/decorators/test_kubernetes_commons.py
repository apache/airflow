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

from collections.abc import Callable
from unittest import mock

import pytest

from tests_common.test_utils.version_compat import AIRFLOW_V_3_0_PLUS

if AIRFLOW_V_3_0_PLUS:
    from airflow.sdk import setup, task, teardown
else:
    from airflow.decorators import setup, task, teardown  # type: ignore[attr-defined,no-redef]

from airflow.utils import timezone

from tests_common.test_utils.db import clear_db_dags, clear_db_runs, clear_rendered_ti_fields

TASK_FUNCTION_NAME_ID = "task_function_name"
DEFAULT_DATE = timezone.datetime(2023, 1, 1)
DAG_ID = "k8s_deco_test_dag"


def _kubernetes_func():
    return {"key1": "value1", "key2": "value2"}


def _kubernetes_cmd_func():
    return ["echo", "Hello world!"]


def _get_decorator_func(decorator_name: str) -> Callable:
    if decorator_name == "kubernetes":
        return _kubernetes_func
    if decorator_name == "kubernetes_cmd":
        return _kubernetes_cmd_func
    raise ValueError(f"Unknown decorator {decorator_name}")


def _prepare_task(
    task_decorator: Callable,
    decorator_name: str,
    **decorator_kwargs,
) -> Callable:
    func_to_use = _get_decorator_func(decorator_name)

    @task_decorator(
        image="python:3.10-slim-buster",
        in_cluster=False,
        cluster_context="default",
        config_file="/tmp/fake_file",
        **decorator_kwargs,
    )
    def task_function_name():
        return func_to_use()

    return task_function_name


KPO_MODULE = "airflow.providers.cncf.kubernetes.operators.pod"
POD_MANAGER_CLASS = "airflow.providers.cncf.kubernetes.utils.pod_manager.PodManager"
HOOK_CLASS = "airflow.providers.cncf.kubernetes.operators.pod.KubernetesHook"


@pytest.mark.db_test
class TestKubernetesDecoratorsBase:
    @pytest.fixture(autouse=True)
    def setup(self, dag_maker):
        self.dag_maker = dag_maker

        with dag_maker(dag_id=DAG_ID):
            ...

        self.dag = self.dag_maker.dag

        self.mock_create_pod = mock.patch(f"{POD_MANAGER_CLASS}.create_pod").start()
        self.mock_await_pod_start = mock.patch(f"{POD_MANAGER_CLASS}.await_pod_start").start()
        self.mock_watch_pod_events = mock.patch(f"{POD_MANAGER_CLASS}.watch_pod_events").start()
        self.mock_await_xcom_sidecar_container_start = mock.patch(
            f"{POD_MANAGER_CLASS}.await_xcom_sidecar_container_start"
        ).start()

        self.mock_extract_xcom = mock.patch(f"{POD_MANAGER_CLASS}.extract_xcom").start()
        self.mock_extract_xcom.return_value = '{"key1": "value1", "key2": "value2"}'

        self.mock_await_pod_completion = mock.patch(f"{POD_MANAGER_CLASS}.await_pod_completion").start()
        self.mock_await_pod_completion.return_value = mock.MagicMock(**{"status.phase": "Succeeded"})
        self.mock_hook = mock.patch(HOOK_CLASS).start()

        # Without this patch each time pod manager would try to extract logs from the pod
        # and log an error about it's inability to get containers for the log
        # {pod_manager.py:572} ERROR - Could not retrieve containers for the pod: ...
        self.mock_fetch_logs = mock.patch(f"{POD_MANAGER_CLASS}.fetch_requested_container_logs").start()
        self.mock_fetch_logs.return_value = "logs"

        try:
            yield
        except Exception:
            pass
        mock.patch.stopall()

    def teardown_method(self):
        clear_db_runs()
        clear_db_dags()
        clear_rendered_ti_fields()

    def execute_task(self, task):
        session = self.dag_maker.session
        dag_run = self.dag_maker.create_dagrun(run_id=f"k8s_decorator_test_{DEFAULT_DATE.date()}")
        ti = dag_run.get_task_instance(task.operator.task_id, session=session)
        return_val = task.operator.execute(context=ti.get_template_context(session=session))

        return ti, return_val


def parametrize_kubernetes_decorators_commons(cls):
    for name, method in cls.__dict__.items():
        if not name.startswith("test_") or not callable(method):
            continue
        new_method = pytest.mark.parametrize(
            ("task_decorator", "decorator_name"),
            [
                (task.kubernetes, "kubernetes"),
                (task.kubernetes_cmd, "kubernetes_cmd"),
            ],
            ids=["kubernetes", "kubernetes_cmd"],
        )(method)
        setattr(cls, name, new_method)

    return cls


@parametrize_kubernetes_decorators_commons
class TestKubernetesDecoratorsCommons(TestKubernetesDecoratorsBase):
    def test_k8s_decorator_init(self, task_decorator, decorator_name):
        """Test the initialization of the @task.kubernetes[_cmd] decorated task."""

        with self.dag_maker:

            @task_decorator(
                image="python:3.10-slim-buster",
                in_cluster=False,
                cluster_context="default",
            )
            def k8s_task_function() -> list[str]:
                return ["return", "value"]

            k8s_task = k8s_task_function()

        assert k8s_task.operator.task_id == "k8s_task_function"
        assert k8s_task.operator.image == "python:3.10-slim-buster"

        expected_cmds = ["placeholder-command"] if decorator_name == "kubernetes" else []
        assert k8s_task.operator.cmds == expected_cmds
        assert k8s_task.operator.random_name_suffix is True

    def test_decorators_with_marked_as_setup(self, task_decorator, decorator_name):
        """Test the @task.kubernetes[_cmd] decorated task works with setup decorator."""
        with self.dag_maker:
            task_function_name = setup(_prepare_task(task_decorator, decorator_name))
            task_function_name()

        assert len(self.dag.task_group.children) == 1
        setup_task = self.dag.task_group.children[TASK_FUNCTION_NAME_ID]
        assert setup_task.is_setup

    def test_decorators_with_marked_as_teardown(self, task_decorator, decorator_name):
        """Test the @task.kubernetes[_cmd] decorated task works with teardown decorator."""
        with self.dag_maker:
            task_function_name = teardown(_prepare_task(task_decorator, decorator_name))
            task_function_name()

        assert len(self.dag.task_group.children) == 1
        teardown_task = self.dag.task_group.children[TASK_FUNCTION_NAME_ID]
        assert teardown_task.is_teardown

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "name",
        ["no_name_in_args", None, "test_task_name"],
        ids=["no_name_in_args", "name_set_to_None", "with_name"],
    )
    @pytest.mark.parametrize(
        "random_name_suffix",
        [True, False],
        ids=["rand_suffix", "no_rand_suffix"],
    )
    def test_pod_naming(
        self,
        task_decorator,
        decorator_name,
        name: str | None,
        random_name_suffix: bool,
    ) -> None:
        """
        Idea behind this test is to check naming conventions are respected in various
        decorator arguments combinations scenarios.

        @task.kubernetes[_cmd] differs from KubernetesPodOperator in a way that it distinguishes
        between no name argument was provided and name was set to None.
        In the first case, the operator name is generated from the python_callable name,
        in the second case default KubernetesPodOperator behavior is preserved.
        """
        extra_kwargs = {"name": name}
        if name == "no_name_in_args":
            extra_kwargs.pop("name")

        decorator_kwargs = {
            "random_name_suffix": random_name_suffix,
            "namespace": "default",
            **extra_kwargs,
        }

        with self.dag_maker:
            task_function_name = _prepare_task(
                task_decorator,
                decorator_name,
                **decorator_kwargs,
            )

            k8s_task = task_function_name()

        task_id = TASK_FUNCTION_NAME_ID
        op = self.dag.get_task(task_id)
        if name is not None:
            assert isinstance(op.name, str)

        # If name was explicitly set to None, we expect the operator name to be None
        if name is None:
            assert op.name is None
        # If name was not provided in decorator, it would be generated:
        # f"k8s-airflow-pod-{python_callable.__name__}"
        elif name == "no_name_in_args":
            assert op.name == f"k8s-airflow-pod-{task_id}"
        # Otherwise, we expect the name to be exactly the same as provided
        else:
            assert op.name == name

        self.execute_task(k8s_task)
        pod_meta = self.mock_create_pod.call_args.kwargs["pod"].metadata
        assert isinstance(pod_meta.name, str)

        # After execution pod names should not contain underscores
        task_id_normalized = task_id.replace("_", "-")

        def check_op_name(name_arg: str | None) -> str:
            if name_arg is None:
                assert op.name is None
                return task_id_normalized

            assert isinstance(op.name, str)
            if name_arg == "no_name_in_args":
                generated_name = f"k8s-airflow-pod-{task_id_normalized}"
                assert op.name == generated_name
                return generated_name

            normalized_name = name_arg.replace("_", "-")
            assert op.name == normalized_name

            return normalized_name

        def check_pod_name(name_base: str):
            if random_name_suffix:
                assert pod_meta.name.startswith(f"{name_base}")
                assert pod_meta.name != name_base
            else:
                assert pod_meta.name == name_base

        pod_name = check_op_name(name)
        check_pod_name(pod_name)
