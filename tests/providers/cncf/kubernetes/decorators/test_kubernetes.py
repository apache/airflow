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

from unittest import mock

import pytest

from airflow.decorators import task
from airflow.utils import timezone

DEFAULT_DATE = timezone.datetime(2021, 9, 1)

KPO_MODULE = "airflow.providers.cncf.kubernetes.operators.kubernetes_pod"
POD_MANAGER_CLASS = "airflow.providers.cncf.kubernetes.utils.pod_manager.PodManager"
HOOK_CLASS = "airflow.providers.cncf.kubernetes.operators.kubernetes_pod.KubernetesHook"


@pytest.fixture(autouse=True)
def mock_create_pod() -> mock.Mock:
    return mock.patch(f"{POD_MANAGER_CLASS}.create_pod").start()


@pytest.fixture(autouse=True)
def mock_await_pod_start() -> mock.Mock:
    return mock.patch(f"{POD_MANAGER_CLASS}.await_pod_start").start()


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
        conn_id=None,
        in_cluster=False,
        cluster_context="default",
        config_file="/tmp/fake_file",
    )
    assert mock_create_pod.call_count == 1

    containers = mock_create_pod.call_args[1]["pod"].spec.containers
    assert len(containers) == 1
    assert containers[0].command == ["bash"]

    assert len(containers[0].args) == 2
    assert containers[0].args[0] == "-cx"
    assert containers[0].args[1].endswith("/tmp/script.py")

    assert containers[0].env[-1].name == "__PYTHON_SCRIPT"
    assert containers[0].env[-1].value
