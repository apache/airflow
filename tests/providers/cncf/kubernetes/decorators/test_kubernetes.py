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


def test_basic_kubernetes(dag_maker, session, mock_create_pod: mock.Mock, mock_hook: mock.Mock) -> None:
    with dag_maker(session=session) as dag:

        @task.kubernetes(
            image="python:3.10-slim-buster",
            in_cluster=False,
            cluster_context="default",
            config_file="/tmp/fake_file",
        )
        def f():
            import random

            return [random.random() for _ in range(100)]

        f()

    dr = dag_maker.create_dagrun()
    (ti,) = dr.task_instances
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


def test_kubernetes_with_mini_scheduler(
    dag_maker, session, mock_create_pod: mock.Mock, mock_hook: mock.Mock
) -> None:
    with dag_maker(session=session):

        @task.kubernetes(
            image="python:3.10-slim-buster",
            in_cluster=False,
            cluster_context="default",
            config_file="/tmp/fake_file",
        )
        def f(arg1, arg2, kwarg1=None, kwarg2=None):
            return {"key1": "value1", "key2": "value2"}

        f1 = f.override(task_id="my_task_id", do_xcom_push=True)("arg1", "arg2", kwarg1="kwarg1")
        f.override(task_id="my_task_id2", do_xcom_push=False)("arg1", "arg2", kwarg1=f1)

    dr = dag_maker.create_dagrun()
    (ti, _) = dr.task_instances

    # check that mini-scheduler works
    ti.schedule_downstream_tasks()
