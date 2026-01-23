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

import contextlib

import pytest

from airflow.providers.common.compat.sdk import AirflowSkipException

from tests_common.test_utils.version_compat import AIRFLOW_V_3_0_PLUS

if AIRFLOW_V_3_0_PLUS:
    from airflow.sdk import task
else:
    from airflow.decorators import task  # type: ignore[attr-defined,no-redef]
from unit.cncf.kubernetes.decorators.test_kubernetes_commons import DAG_ID, TestKubernetesDecoratorsBase

XCOM_IMAGE = "XCOM_IMAGE"


class TestKubernetesCmdDecorator(TestKubernetesDecoratorsBase):
    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "args_only",
        [True, False],
    )
    def test_basic_kubernetes(self, args_only: bool):
        """Test basic proper KubernetesPodOperator creation from @task.kubernetes_cmd decorator"""
        expected = ["echo", "Hello world!"]
        with self.dag_maker:

            @task.kubernetes_cmd(
                image="python:3.10-slim-buster",
                in_cluster=False,
                cluster_context="default",
                config_file="/tmp/fake_file",
                namespace="default",
                args_only=args_only,
            )
            def hello():
                return expected

            k8s_task = hello()

        self.execute_task(k8s_task)

        self.mock_hook.assert_called_once_with(
            conn_id="kubernetes_default",
            in_cluster=False,
            cluster_context="default",
            config_file="/tmp/fake_file",
        )
        assert self.mock_create_pod.call_count == 1

        containers = self.mock_create_pod.call_args.kwargs["pod"].spec.containers
        assert len(containers) == 1

        expected_command = expected
        expected_args = []
        if args_only:
            expected_args = expected_command
            expected_command = []

        assert containers[0].command == expected_command
        assert containers[0].args == expected_args

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("func_return", "exception"),
        [
            pytest.param("string", TypeError, id="iterable_str"),
            pytest.param(True, TypeError, id="bool"),
            pytest.param(42, TypeError, id="int"),
            pytest.param(None, TypeError, id="None"),
            pytest.param(("a", "b"), TypeError, id="tuple"),
            pytest.param([], ValueError, id="empty_list"),
            pytest.param(["echo", 123], TypeError, id="mixed_list"),
            pytest.param(["echo", "Hello world!"], None, id="valid_list"),
        ],
    )
    def test_kubernetes_cmd_wrong_cmd(
        self,
        func_return,
        exception,
    ):
        """
        Test that @task.kubernetes_cmd raises an error if the python_callable returns
        an invalid value.
        """
        with self.dag_maker:

            @task.kubernetes_cmd(
                image="python:3.10-slim-buster",
                in_cluster=False,
                cluster_context="default",
                config_file="/tmp/fake_file",
                namespace="default",
            )
            def hello():
                return func_return

            k8s_task = hello()

        context_manager = pytest.raises(exception) if exception else contextlib.nullcontext()
        with context_manager:
            self.execute_task(k8s_task)

    @pytest.mark.asyncio
    def test_kubernetes_cmd_with_input_output(self):
        """Verify @task.kubernetes_cmd will run XCom container if do_xcom_push is set."""
        with self.dag_maker:

            @task.kubernetes_cmd(
                image="python:3.10-slim-buster",
                in_cluster=False,
                cluster_context="default",
                config_file="/tmp/fake_file",
                namespace="default",
            )
            def f(arg1: str, arg2: str, kwarg1: str | None = None, kwarg2: str | None = None):
                return [
                    "echo",
                    f"arg1={arg1}",
                    f"arg2={arg2}",
                    f"kwarg1={kwarg1}",
                    f"kwarg2={kwarg2}",
                ]

            k8s_task = f.override(task_id="my_task_id", do_xcom_push=True)("arg1", "arg2", kwarg1="kwarg1")

        self.mock_hook.return_value.get_xcom_sidecar_container_image.return_value = XCOM_IMAGE
        self.mock_hook.return_value.get_xcom_sidecar_container_resources.return_value = {
            "requests": {"cpu": "1m", "memory": "10Mi"},
            "limits": {"cpu": "1m", "memory": "50Mi"},
        }
        self.execute_task(k8s_task)

        self.mock_hook.assert_called_once_with(
            conn_id="kubernetes_default",
            in_cluster=False,
            cluster_context="default",
            config_file="/tmp/fake_file",
        )
        assert self.mock_create_pod.call_count == 1
        assert self.mock_hook.return_value.get_xcom_sidecar_container_image.call_count == 1
        assert self.mock_hook.return_value.get_xcom_sidecar_container_resources.call_count == 1

        containers = self.mock_create_pod.call_args.kwargs["pod"].spec.containers

        # First container is main one with command
        assert len(containers) == 2
        assert containers[0].command == ["echo", "arg1=arg1", "arg2=arg2", "kwarg1=kwarg1", "kwarg2=None"]
        assert len(containers[0].args) == 0

        # Second container is xcom image
        assert containers[1].image == XCOM_IMAGE
        assert containers[1].volume_mounts[0].mount_path == "/airflow/xcom"

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "cmds",
        [None, ["ignored_cmd"], "ignored_cmd"],
    )
    @pytest.mark.parametrize(
        "arguments",
        [None, ["ignored_arg"], "ignored_arg"],
    )
    @pytest.mark.parametrize(
        "args_only",
        [True, False],
    )
    def test_ignored_decorator_parameters(
        self,
        cmds: list[str] | None,
        arguments: list[str] | None,
        args_only: bool,
    ) -> None:
        """
        Test setting `cmds` or `arguments` from decorator does not affect the operator.
        And the warning is shown only if `cmds` or `arguments` are not None.
        """
        context_manager = pytest.warns(UserWarning, match="The `cmds` and `arguments` are unused")
        # Don't warn if both `cmds` and `arguments` are None
        if cmds is None and arguments is None:
            context_manager = contextlib.nullcontext()  # type: ignore

        expected = ["func", "return"]
        with self.dag_maker:
            # We need to suppress the warning about `cmds` and `arguments` being unused
            with context_manager:

                @task.kubernetes_cmd(
                    image="python:3.10-slim-buster",
                    in_cluster=False,
                    cluster_context="default",
                    config_file="/tmp/fake_file",
                    namespace="default",
                    cmds=cmds,
                    arguments=arguments,
                    args_only=args_only,
                )
                def hello():
                    return expected

                hello_task = hello()

        assert hello_task.operator.cmds == []
        assert hello_task.operator.arguments == []

        self.execute_task(hello_task)
        containers = self.mock_create_pod.call_args.kwargs["pod"].spec.containers
        assert len(containers) == 1

        expected_command = expected
        expected_args = []
        if args_only:
            expected_args = expected_command
            expected_command = []
        assert containers[0].command == expected_command
        assert containers[0].args == expected_args

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        argnames=("command", "op_arg", "expected_command"),
        argvalues=[
            pytest.param(
                ["echo", "hello"],
                "world",
                ["echo", "hello", "world"],
                id="not_templated",
            ),
            pytest.param(
                ["echo", "{{ ti.task_id }}"], "{{ ti.dag_id }}", ["echo", "hello", DAG_ID], id="templated"
            ),
        ],
    )
    def test_rendering_kubernetes_cmd(
        self,
        command: list[str],
        op_arg: str,
        expected_command: list[str],
    ):
        """Test that templating works in function return value"""
        with self.dag_maker:

            @task.kubernetes_cmd(
                image="python:3.10-slim-buster",
                in_cluster=False,
                cluster_context="default",
                config_file="/tmp/fake_file",
                namespace="default",
            )
            def hello(add_to_command: str):
                return command + [add_to_command]

            hello_task = hello(op_arg)

        self.execute_task(hello_task)

        self.mock_hook.assert_called_once_with(
            conn_id="kubernetes_default",
            in_cluster=False,
            cluster_context="default",
            config_file="/tmp/fake_file",
        )
        containers = self.mock_create_pod.call_args.kwargs["pod"].spec.containers
        assert len(containers) == 1

        assert containers[0].command == expected_command
        assert containers[0].args == []

    @pytest.mark.asyncio
    def test_basic_context_works(self):
        """Test that decorator works with context as kwargs unpcacked in function arguments"""
        with self.dag_maker:

            @task.kubernetes_cmd(
                image="python:3.10-slim-buster",
                in_cluster=False,
                cluster_context="default",
                config_file="/tmp/fake_file",
                namespace="default",
            )
            def hello(**context):
                return ["echo", context["ti"].task_id, context["dag_run"].dag_id]

            hello_task = hello()

        self.execute_task(hello_task)

        self.mock_hook.assert_called_once_with(
            conn_id="kubernetes_default",
            in_cluster=False,
            cluster_context="default",
            config_file="/tmp/fake_file",
        )
        containers = self.mock_create_pod.call_args.kwargs["pod"].spec.containers
        assert len(containers) == 1

        assert containers[0].command == ["echo", "hello", DAG_ID]
        assert containers[0].args == []

    @pytest.mark.asyncio
    def test_named_context_variables(self):
        """Test that decorator works with specific context variable as kwargs in function arguments"""
        with self.dag_maker:

            @task.kubernetes_cmd(
                image="python:3.10-slim-buster",
                in_cluster=False,
                cluster_context="default",
                config_file="/tmp/fake_file",
                namespace="default",
            )
            def hello(ti=None, dag_run=None):
                return ["echo", ti.task_id, dag_run.dag_id]

            hello_task = hello()

        self.execute_task(hello_task)

        self.mock_hook.assert_called_once_with(
            conn_id="kubernetes_default",
            in_cluster=False,
            cluster_context="default",
            config_file="/tmp/fake_file",
        )
        containers = self.mock_create_pod.call_args.kwargs["pod"].spec.containers
        assert len(containers) == 1

        assert containers[0].command == ["echo", "hello", DAG_ID]
        assert containers[0].args == []

    @pytest.mark.asyncio
    def test_rendering_kubernetes_cmd_decorator_params(self):
        """Test that templating works in decorator parameters"""
        with self.dag_maker:

            @task.kubernetes_cmd(
                image="python:{{ dag.dag_id }}",
                in_cluster=False,
                cluster_context="default",
                config_file="/tmp/fake_file",
                namespace="default",
                kubernetes_conn_id="kubernetes_{{ dag.dag_id }}",
            )
            def hello():
                return ["echo", "Hello world!"]

            hello_task = hello()

        self.execute_task(hello_task)

        self.mock_hook.assert_called_once_with(
            conn_id="kubernetes_" + DAG_ID,
            in_cluster=False,
            cluster_context="default",
            config_file="/tmp/fake_file",
        )
        containers = self.mock_create_pod.call_args.kwargs["pod"].spec.containers
        assert len(containers) == 1

        assert containers[0].image == f"python:{DAG_ID}"

    def test_airflow_skip(self):
        """Test that the operator is skipped if the task is skipped"""
        with self.dag_maker:

            @task.kubernetes_cmd(
                image="python:3.10-slim-buster",
                in_cluster=False,
                cluster_context="default",
                config_file="/tmp/fake_file",
                namespace="default",
            )
            def hello():
                raise AirflowSkipException("This task should be skipped")

            hello_task = hello()

        with pytest.raises(AirflowSkipException):
            self.execute_task(hello_task)
        self.mock_hook.assert_not_called()
        self.mock_create_pod.assert_not_called()

    def test_kubernetes_cmd_template_fields_include_taskflow_args(self):
        """Test that kubernetes_cmd operator has op_args and op_kwargs in template_fields"""
        with self.dag_maker:

            @task.kubernetes_cmd(
                image="python:3.10-slim-buster",
                in_cluster=False,
                cluster_context="default",
                config_file="/tmp/fake_file",
                namespace="default",
            )
            def hello(name: str) -> list[str]:
                return ["echo", name]

            hello_task = hello("world")

        op = hello_task.operator

        assert "op_args" in op.template_fields
        assert "op_kwargs" in op.template_fields
        assert "cmds" in op.template_fields
        assert "arguments" in op.template_fields
