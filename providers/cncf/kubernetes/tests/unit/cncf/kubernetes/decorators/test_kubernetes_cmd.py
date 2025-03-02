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
import warnings
from unittest import mock

import pytest

from airflow.decorators import task

pytestmark = pytest.mark.db_test


XCOM_IMAGE = "XCOM_IMAGE"


@pytest.mark.parametrize(
    "args_only",
    [True, False],
)
def test_basic_kubernetes_cmd(
    dag_maker,
    session,
    args_only: bool,
    mock_create_pod: mock.Mock,
    mock_hook: mock.Mock,
) -> None:
    expected = ["echo", "Hello world!"]
    with dag_maker(session=session) as dag:

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

        hello()

    dr = dag_maker.create_dagrun()
    (ti,) = dr.task_instances
    session.add(ti)
    session.commit()
    dag.get_task("hello").execute(context=ti.get_template_context(session=session))
    mock_hook.assert_called_once_with(
        conn_id="kubernetes_default",
        in_cluster=False,
        cluster_context="default",
        config_file="/tmp/fake_file",
    )
    assert mock_create_pod.call_count == 1

    containers = mock_create_pod.call_args.kwargs["pod"].spec.containers
    assert len(containers) == 1

    expected_command = expected
    expected_args = []
    if args_only:
        expected_args = expected_command
        expected_command = []

    assert containers[0].command == expected_command
    assert containers[0].args == expected_args


@pytest.mark.parametrize(
    "func_return, exception",
    [
        ("string", TypeError),
        (42, TypeError),
        (None, TypeError),
        (("a", "b"), TypeError),
        ([], ValueError),
        (["echo", 123], TypeError),
        (["echo", "Hello world!"], None),
    ],
    ids=[
        "iterable_str",
        "int",
        "None",
        "tuple",
        "empty_list",
        "mixed_list",
        "valid_list",
    ],
)
def test_kubernetes_cmd_wrong_cmd(
    dag_maker,
    session,
    func_return,
    exception,
    mock_create_pod: mock.Mock,
    mock_hook: mock.Mock,
) -> None:
    with dag_maker(session=session) as dag:

        @task.kubernetes_cmd(
            image="python:3.10-slim-buster",
            in_cluster=False,
            cluster_context="default",
            config_file="/tmp/fake_file",
            namespace="default",
        )
        def hello():
            return func_return

        hello()

    dr = dag_maker.create_dagrun()
    (ti,) = dr.task_instances
    session.add(ti)
    session.commit()

    context_manager = pytest.raises(exception) if exception else contextlib.nullcontext()
    with context_manager:
        dag.get_task("hello").execute(context=ti.get_template_context(session=session))


def test_kubernetes_cmd_with_input_output(
    dag_maker, session, mock_create_pod: mock.Mock, mock_hook: mock.Mock
) -> None:
    with dag_maker(session=session) as dag:

        @task.kubernetes_cmd(
            image="python:3.10-slim-buster",
            in_cluster=False,
            cluster_context="default",
            config_file="/tmp/fake_file",
            namespace="default",
        )
        def f(arg1, arg2, kwarg1=None, kwarg2=None):
            return [
                "echo",
                f"arg1={arg1}",
                f"arg2={arg2}",
                f"kwarg1={kwarg1}",
                f"kwarg2={kwarg2}",
            ]

        f.override(task_id="my_task_id", do_xcom_push=True)("arg1", "arg2", kwarg1="kwarg1")

    mock_hook.return_value.get_xcom_sidecar_container_image.return_value = XCOM_IMAGE
    mock_hook.return_value.get_xcom_sidecar_container_resources.return_value = {
        "requests": {"cpu": "1m", "memory": "10Mi"},
        "limits": {"cpu": "1m", "memory": "50Mi"},
    }

    dr = dag_maker.create_dagrun()
    (ti,) = dr.task_instances
    session.add(dr)
    session.commit()
    dag.get_task("my_task_id").execute(context=ti.get_template_context(session=session))

    mock_hook.assert_called_once_with(
        conn_id="kubernetes_default",
        in_cluster=False,
        cluster_context="default",
        config_file="/tmp/fake_file",
    )
    assert mock_create_pod.call_count == 1
    assert mock_hook.return_value.get_xcom_sidecar_container_image.call_count == 1
    assert mock_hook.return_value.get_xcom_sidecar_container_resources.call_count == 1

    containers = mock_create_pod.call_args.kwargs["pod"].spec.containers

    # First container is main one with command
    assert len(containers) == 2
    assert containers[0].command == ["echo", "arg1=arg1", "arg2=arg2", "kwarg1=kwarg1", "kwarg2=None"]
    assert len(containers[0].args) == 0

    # Second container is xcom image
    assert containers[1].image == XCOM_IMAGE
    assert containers[1].volume_mounts[0].mount_path == "/airflow/xcom"


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
    dag_maker,
    session,
    cmds: list[str],
    arguments: list[str],
    args_only: bool,
    mock_create_pod: mock.Mock,
    mock_hook: mock.Mock,
) -> None:
    """Verify setting `cmds` or `arguments` for a @task.kubernetes_cmd function is ignored."""

    expected = ["func", "return"]
    with dag_maker(session=session) as dag:

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

    with warnings.catch_warnings():
        warnings.simplefilter("error", category=UserWarning)
        dr = dag_maker.create_dagrun()
        (ti,) = dr.task_instances
        session.add(ti)
        session.commit()
        dag.get_task("hello").execute(context=ti.get_template_context(session=session))

    containers = mock_create_pod.call_args.kwargs["pod"].spec.containers
    assert len(containers) == 1

    expected_command = expected
    expected_args = []
    if args_only:
        expected_args = expected_command
        expected_command = []
    assert containers[0].command == expected_command
    assert containers[0].args == expected_args


@pytest.mark.parametrize(
    argnames=["command", "expected_command", "expected_return_val"],
    argvalues=[
        pytest.param(
            ["echo", "hello", "world"],
            ["echo", "hello", "world"],
            ["echo", "hello", "world"],
            id="not_templated",
        ),
        pytest.param(["echo", "{{ ti.task_id }}"], ["echo", "hello"], ["echo", "hello"], id="templated"),
    ],
)
def test_rendering_kubernetes_cmd(
    dag_maker,
    session,
    command,
    expected_command,
    expected_return_val,
    mock_create_pod: mock.Mock,
    mock_hook: mock.Mock,
) -> None:
    with dag_maker(session=session) as dag:

        @task.kubernetes_cmd(
            image="python:3.10-slim-buster",
            in_cluster=False,
            cluster_context="default",
            config_file="/tmp/fake_file",
            namespace="default",
        )
        def hello():
            return command

        hello()

    dr = dag_maker.create_dagrun()
    (ti,) = dr.task_instances
    session.add(ti)
    session.commit()
    retr = dag.get_task("hello").execute(context=ti.get_template_context(session=session))
    mock_hook.assert_called_once_with(
        conn_id="kubernetes_default",
        in_cluster=False,
        cluster_context="default",
        config_file="/tmp/fake_file",
    )
    containers = mock_create_pod.call_args.kwargs["pod"].spec.containers
    assert len(containers) == 1

    assert containers[0].command == expected_command
    assert retr == expected_return_val
