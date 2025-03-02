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

from typing import Callable
from unittest import mock

import pytest

from airflow.decorators import setup, task, teardown

pytestmark = pytest.mark.db_test


TASK_FUNCTION_NAME_ID = "task_function_name"


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


@pytest.mark.parametrize(
    "task_decorator,decorator_name",
    [
        (task.kubernetes, "kubernetes"),
        (task.kubernetes_cmd, "kubernetes_cmd"),
    ],
    ids=["kubernetes", "kubernetes_cmd"],
)
def test_decorators_with_marked_as_setup(
    task_decorator,
    decorator_name,
    dag_maker,
    session,
    mock_create_pod: mock.Mock,
    mock_hook: mock.Mock,
) -> None:
    with dag_maker(session=session) as dag:
        task_function_name = setup(_prepare_task(task_decorator, decorator_name))
        task_function_name()

    assert len(dag.task_group.children) == 1
    setup_task = dag.task_group.children[TASK_FUNCTION_NAME_ID]
    assert setup_task.is_setup


@pytest.mark.parametrize(
    "task_decorator,decorator_name",
    [
        (task.kubernetes, "kubernetes"),
        (task.kubernetes_cmd, "kubernetes_cmd"),
    ],
    ids=["kubernetes", "kubernetes_cmd"],
)
def test_decorators_with_marked_as_teardown(
    task_decorator,
    decorator_name,
    dag_maker,
    session,
    mock_create_pod: mock.Mock,
    mock_hook: mock.Mock,
) -> None:
    with dag_maker(session=session) as dag:
        task_function_name = teardown(_prepare_task(task_decorator, decorator_name))
        task_function_name()

    assert len(dag.task_group.children) == 1
    teardown_task = dag.task_group.children[TASK_FUNCTION_NAME_ID]
    assert teardown_task.is_teardown


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
@pytest.mark.parametrize(
    "task_decorator,decorator_name",
    [
        (task.kubernetes, "kubernetes"),
        (task.kubernetes_cmd, "kubernetes_cmd"),
    ],
    ids=["kubernetes", "kubernetes_cmd"],
)
def test_pod_naming(
    task_decorator,
    decorator_name,
    dag_maker,
    session,
    mock_create_pod: mock.Mock,
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

    with dag_maker(session=session) as dag:
        task_function_name = _prepare_task(
            task_decorator,
            decorator_name,
            **decorator_kwargs,
        )

        task_function_name()

    dr = dag_maker.create_dagrun()
    (ti,) = dr.task_instances
    session.add(ti)
    session.commit()

    task_id = TASK_FUNCTION_NAME_ID
    op = dag.get_task(task_id)
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

    op.execute(context=ti.get_template_context(session=session))
    pod_meta = mock_create_pod.call_args.kwargs["pod"].metadata
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
