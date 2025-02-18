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

import base64
import pickle
from unittest import mock

import pytest

from airflow.decorators import setup, task, teardown
from airflow.utils import timezone

pytestmark = pytest.mark.db_test


DEFAULT_DATE = timezone.datetime(2021, 9, 1)

KPO_MODULE = "airflow.providers.cncf.kubernetes.operators.pod"
POD_MANAGER_CLASS = "airflow.providers.cncf.kubernetes.utils.pod_manager.PodManager"
HOOK_CLASS = "airflow.providers.cncf.kubernetes.operators.pod.KubernetesHook"
XCOM_IMAGE = "XCOM_IMAGE"


@pytest.fixture(autouse=True)
def mock_create_pod() -> mock.Mock:
    return mock.patch(f"{POD_MANAGER_CLASS}.create_pod").start()


@pytest.fixture(autouse=True)
def mock_await_pod_start() -> mock.Mock:
    return mock.patch(f"{POD_MANAGER_CLASS}.await_pod_start").start()


@pytest.fixture(autouse=True)
def await_xcom_sidecar_container_start() -> mock.Mock:
    return mock.patch(f"{POD_MANAGER_CLASS}.await_xcom_sidecar_container_start").start()


@pytest.fixture(autouse=True)
def extract_xcom() -> mock.Mock:
    f = mock.patch(f"{POD_MANAGER_CLASS}.extract_xcom").start()
    f.return_value = '{"key1": "value1", "key2": "value2"}'
    return f


@pytest.fixture(autouse=True)
def mock_await_pod_completion() -> mock.Mock:
    f = mock.patch(f"{POD_MANAGER_CLASS}.await_pod_completion").start()
    f.return_value = mock.MagicMock(**{"status.phase": "Succeeded"})
    return f


@pytest.fixture(autouse=True)
def mock_hook():
    return mock.patch(HOOK_CLASS).start()


# Without this patch each time pod manager would try to extract logs from the pod
# and log an error about it's inability to get containers for the log
# {pod_manager.py:572} ERROR - Could not retrieve containers for the pod: ...
@pytest.fixture(autouse=True)
def mock_fetch_logs() -> mock.Mock:
    f = mock.patch(f"{POD_MANAGER_CLASS}.fetch_requested_container_logs").start()
    f.return_value = "logs"
    return f


def test_basic_kubernetes(dag_maker, session, mock_create_pod: mock.Mock, mock_hook: mock.Mock) -> None:
    with dag_maker(session=session) as dag:

        @task.kubernetes(
            image="python:3.10-slim-buster",
            in_cluster=False,
            cluster_context="default",
            config_file="/tmp/fake_file",
            namespace="default",
        )
        def f():
            import random

            return [random.random() for _ in range(100)]

        f()

    dr = dag_maker.create_dagrun()
    (ti,) = dr.task_instances
    session.add(ti)
    session.commit()
    dag.get_task("f").execute(context=ti.get_template_context(session=session))
    mock_hook.assert_called_once_with(
        conn_id="kubernetes_default",
        in_cluster=False,
        cluster_context="default",
        config_file="/tmp/fake_file",
    )
    assert mock_create_pod.call_count == 1

    containers = mock_create_pod.call_args.kwargs["pod"].spec.containers
    assert len(containers) == 1
    assert containers[0].command[0] == "bash"
    assert len(containers[0].args) == 0
    assert containers[0].env[0].name == "__PYTHON_SCRIPT"
    assert containers[0].env[0].value
    assert containers[0].env[1].name == "__PYTHON_INPUT"

    # Ensure we pass input through a b64 encoded env var
    decoded_input = pickle.loads(base64.b64decode(containers[0].env[1].value))
    assert decoded_input == {"args": [], "kwargs": {}}


def test_kubernetes_with_input_output(
    dag_maker, session, mock_create_pod: mock.Mock, mock_hook: mock.Mock
) -> None:
    with dag_maker(session=session) as dag:

        @task.kubernetes(
            image="python:3.10-slim-buster",
            in_cluster=False,
            cluster_context="default",
            config_file="/tmp/fake_file",
            namespace="default",
        )
        def f(arg1, arg2, kwarg1=None, kwarg2=None):
            return {"key1": "value1", "key2": "value2"}

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

    # First container is Python script
    assert len(containers) == 2
    assert containers[0].command[0] == "bash"
    assert len(containers[0].args) == 0

    assert containers[0].env[0].name == "__PYTHON_SCRIPT"
    assert containers[0].env[0].value
    assert containers[0].env[1].name == "__PYTHON_INPUT"
    assert containers[0].env[1].value

    # Ensure we pass input through a b64 encoded env var
    decoded_input = pickle.loads(base64.b64decode(containers[0].env[1].value))
    assert decoded_input == {"args": ("arg1", "arg2"), "kwargs": {"kwarg1": "kwarg1"}}

    # Second container is xcom image
    assert containers[1].image == XCOM_IMAGE
    assert containers[1].volume_mounts[0].mount_path == "/airflow/xcom"


def test_kubernetes_with_marked_as_setup(
    dag_maker, session, mock_create_pod: mock.Mock, mock_hook: mock.Mock
) -> None:
    with dag_maker(session=session) as dag:

        @setup
        @task.kubernetes(
            image="python:3.10-slim-buster",
            in_cluster=False,
            cluster_context="default",
            config_file="/tmp/fake_file",
        )
        def f():
            return {"key1": "value1", "key2": "value2"}

        f()

    assert len(dag.task_group.children) == 1
    setup_task = dag.task_group.children["f"]
    assert setup_task.is_setup


def test_kubernetes_with_marked_as_teardown(
    dag_maker, session, mock_create_pod: mock.Mock, mock_hook: mock.Mock
) -> None:
    with dag_maker(session=session) as dag:

        @teardown
        @task.kubernetes(
            image="python:3.10-slim-buster",
            in_cluster=False,
            cluster_context="default",
            config_file="/tmp/fake_file",
        )
        def f():
            return {"key1": "value1", "key2": "value2"}

        f()

    assert len(dag.task_group.children) == 1
    teardown_task = dag.task_group.children["f"]
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
def test_pod_naming(
    dag_maker,
    session,
    mock_create_pod: mock.Mock,
    name: str | None,
    random_name_suffix: bool,
) -> None:
    """
    Idea behind this test is to check naming conventions are respected in various
    decorator arguments combinations scenarios.

    @task.kubernetes differs from KubernetesPodOperator in a way that it distinguishes
    between no name argument was provided and name was set to None.
    In the first case, the operator name is generated from the python_callable name,
    in the second case default KubernetesPodOperator behavior is preserved.
    """
    extra_kwargs = {"name": name}
    if name == "no_name_in_args":
        extra_kwargs.pop("name")

    with dag_maker(session=session) as dag:

        @task.kubernetes(
            image="python:3.10-slim-buster",
            in_cluster=False,
            cluster_context="default",
            config_file="/tmp/fake_file",
            random_name_suffix=random_name_suffix,
            namespace="default",
            **extra_kwargs,  # type: ignore
        )
        def task_function_name():
            return 42

        task_function_name()

    dr = dag_maker.create_dagrun()
    (ti,) = dr.task_instances
    session.add(ti)
    session.commit()

    task_id = "task_function_name"
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
