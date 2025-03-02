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

from unittest import mock

import pytest

pytest_plugins = "tests_common.pytest_plugin"

KPO_MODULE = "airflow.providers.cncf.kubernetes.operators.pod"
POD_MANAGER_CLASS = "airflow.providers.cncf.kubernetes.utils.pod_manager.PodManager"
HOOK_CLASS = "airflow.providers.cncf.kubernetes.operators.pod.KubernetesHook"


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
