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

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from airflow.providers.cncf.kubernetes.utils.container import (
    container_is_running,
    container_is_succeeded,
    container_is_terminated,
)


@pytest.mark.parametrize(
    ("container_state", "expected_is_terminated"),
    [("waiting", False), ("running", False), ("terminated", True)],
)
def test_container_is_terminated_with_waiting_state(container_state, expected_is_terminated):
    container_status = MagicMock()
    container_status.configure_mock(
        **{
            "name": "base",
            "state.waiting": True if container_state == "waiting" else None,
            "state.running": True if container_state == "running" else None,
            "state.terminated": True if container_state == "terminated" else None,
        }
    )
    pod_info = MagicMock()
    pod_info.status.container_statuses = [container_status]
    assert container_is_terminated(pod_info, "base") == expected_is_terminated


def params_for_test_container_is_running():
    """The `container_is_running` method is designed to handle an assortment of bad objects
    returned from `read_pod`.  E.g. a None object, an object `e` such that `e.status` is None,
    an object `e` such that `e.status.container_statuses` is None, and so on.  This function
    emits params used in `test_container_is_running` to verify this behavior.

    We create mock classes not derived from MagicMock because with an instance `e` of MagicMock,
    tests like `e.hello is not None` are always True.
    """

    class RemotePodMock:
        pass

    class ContainerStatusMock:
        def __init__(self, name):
            self.name = name

    def remote_pod(running=None, not_running=None):
        e = RemotePodMock()
        e.status = RemotePodMock()
        e.status.container_statuses = []
        e.status.init_container_statuses = []
        for r in not_running or []:
            e.status.container_statuses.append(container(r, False))
        for r in running or []:
            e.status.container_statuses.append(container(r, True))
        return e

    def container(name, running):
        c = ContainerStatusMock(name)
        c.state = RemotePodMock()
        c.state.running = {"a": "b"} if running else None
        return c

    pod_mock_list = []
    pod_mock_list.append(pytest.param(None, False, id="None remote_pod"))
    p = RemotePodMock()
    p.status = None
    pod_mock_list.append(pytest.param(p, False, id="None remote_pod.status"))
    p = RemotePodMock()
    p.status = RemotePodMock()
    p.status.container_statuses = []
    p.status.init_container_statuses = []
    pod_mock_list.append(pytest.param(p, False, id="empty remote_pod.status.container_statuses"))
    pod_mock_list.append(pytest.param(remote_pod(), False, id="filter empty"))
    pod_mock_list.append(pytest.param(remote_pod(None, ["base"]), False, id="filter 0 running"))
    pod_mock_list.append(pytest.param(remote_pod(["hello"], ["base"]), False, id="filter 1 not running"))
    pod_mock_list.append(pytest.param(remote_pod(["base"], ["hello"]), True, id="filter 1 running"))
    return pod_mock_list


@pytest.mark.parametrize(("remote_pod", "result"), params_for_test_container_is_running())
def test_container_is_running(remote_pod, result):
    """The `container_is_running` function is designed to handle an assortment of bad objects
    returned from `read_pod`.  E.g. a None object, an object `e` such that `e.status` is None,
    an object `e` such that `e.status.container_statuses` is None, and so on.  This test
    verifies the expected behavior."""
    assert container_is_running(remote_pod, "base") is result


def params_for_test_container_is_succeeded():
    """The `container_is_succeeded` method is designed to handle an assortment of bad objects
    returned from `read_pod`.  E.g. a None object, an object `e` such that `e.status` is None,
    an object `e` such that `e.status.container_statuses` is None, and so on.  This function
    emits params used in `test_container_is_succeeded` to verify this behavior.
    We create mock classes not derived from MagicMock because with an instance `e` of MagicMock,
    tests like `e.hello is not None` are always True.
    """

    class RemotePodMock:
        pass

    class ContainerStatusMock:
        def __init__(self, name):
            self.name = name

    def remote_pod(succeeded=None, not_succeeded=None):
        e = RemotePodMock()
        e.status = RemotePodMock()
        e.status.container_statuses = []
        e.status.init_container_statuses = []
        for r in not_succeeded or []:
            e.status.container_statuses.append(container(r, False))
        for r in succeeded or []:
            e.status.container_statuses.append(container(r, True))
        return e

    def container(name, succeeded):
        c = ContainerStatusMock(name)
        c.state = RemotePodMock()
        c.state.terminated = SimpleNamespace(**{"exit_code": 0}) if succeeded else None
        return c

    pod_mock_list = []
    pod_mock_list.append(pytest.param(None, False, id="None remote_pod"))
    p = RemotePodMock()
    p.status = None
    pod_mock_list.append(pytest.param(p, False, id="None remote_pod.status"))
    p = RemotePodMock()
    p.status = RemotePodMock()
    p.status.container_statuses = None
    p.status.init_container_statuses = []

    pod_mock_list.append(pytest.param(p, False, id="None remote_pod.status.container_statuses"))
    p = RemotePodMock()
    p.status = RemotePodMock()
    p.status.container_statuses = []
    p.status.init_container_statuses = []
    pod_mock_list.append(pytest.param(p, False, id="empty remote_pod.status.container_statuses"))
    pod_mock_list.append(pytest.param(remote_pod(), False, id="filter empty"))
    pod_mock_list.append(pytest.param(remote_pod(None, ["base"]), False, id="filter 0 succeeded"))
    pod_mock_list.append(pytest.param(remote_pod(["hello"], ["base"]), False, id="filter 1 not succeeded"))
    pod_mock_list.append(pytest.param(remote_pod(["base"], ["hello"]), True, id="filter 1 succeeded"))
    return pod_mock_list


@pytest.mark.parametrize(("remote_pod", "result"), params_for_test_container_is_succeeded())
def test_container_is_succeeded(remote_pod, result):
    """The `container_is_succeeded` function is designed to handle an assortment of bad objects
    returned from `read_pod`.  E.g. a None object, an object `e` such that `e.status` is None,
    an object `e` such that `e.status.container_statuses` is None, and so on.  This test
    verifies the expected behavior."""
    assert container_is_succeeded(remote_pod, "base") is result
