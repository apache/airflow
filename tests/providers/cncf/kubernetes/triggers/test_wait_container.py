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

from unittest.mock import MagicMock

from airflow.providers.cncf.kubernetes.triggers.wait_container import WaitContainerTrigger

TRIGGER_CLASS = "airflow.providers.cncf.kubernetes.triggers.wait_container.WaitContainerTrigger"
READ_NAMESPACED_POD_PATH = "kubernetes_asyncio.client.CoreV1Api.read_namespaced_pod"


def get_read_pod_mock_phases(phases_to_emit=None):
    """emit pods with given phases sequentially"""

    async def mock_read_namespaced_pod(*args, **kwargs):
        event_mock = MagicMock()
        event_mock.status.phase = phases_to_emit.pop(0)
        return event_mock

    return mock_read_namespaced_pod


def get_read_pod_mock_containers(statuses_to_emit=None):
    """
    Emit pods with given phases sequentially.
    `statuses_to_emit` should be a list of bools indicating running or not.
    """

    async def mock_read_namespaced_pod(*args, **kwargs):
        container_mock = MagicMock()
        container_mock.state.running = statuses_to_emit.pop(0)
        event_mock = MagicMock()
        event_mock.status.container_statuses = [container_mock]
        return event_mock

    return mock_read_namespaced_pod


class TestWaitContainerTrigger:
    def test_serialize(self):
        """
        Asserts that the Trigger correctly serializes its arguments
        and classpath.
        """
        expected_kwargs = {
            "kubernetes_conn_id": None,
            "hook_params": {
                "cluster_context": "cluster_context",
                "config_file": "config_file",
                "in_cluster": "in_cluster",
            },
            "pod_name": "pod_name",
            "container_name": "container_name",
            "pod_namespace": "pod_namespace",
            "pending_phase_timeout": 120,
            "poll_interval": 5,
            "logging_interval": None,
            "last_log_time": None,
        }
        trigger = WaitContainerTrigger(**expected_kwargs)
        classpath, actual_kwargs = trigger.serialize()
        assert classpath == TRIGGER_CLASS
        assert actual_kwargs == expected_kwargs
