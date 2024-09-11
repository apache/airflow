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

import asyncio
import datetime
import logging
from asyncio import Future
from unittest import mock
from unittest.mock import MagicMock

import pytest
from kubernetes.client import models as k8s
from pendulum import DateTime

from airflow.providers.cncf.kubernetes.triggers.pod import ContainerState, KubernetesPodTrigger
from airflow.providers.cncf.kubernetes.utils.pod_manager import PodPhase
from airflow.triggers.base import TriggerEvent

TRIGGER_PATH = "airflow.providers.cncf.kubernetes.triggers.pod.KubernetesPodTrigger"
HOOK_PATH = "airflow.providers.cncf.kubernetes.hooks.kubernetes.AsyncKubernetesHook"
POD_NAME = "test-pod-name"
NAMESPACE = "default"
CONN_ID = "test_kubernetes_conn_id"
POLL_INTERVAL = 2
CLUSTER_CONTEXT = "test-context"
CONFIG_DICT = {"a": "b"}
IN_CLUSTER = False
GET_LOGS = True
STARTUP_TIMEOUT_SECS = 120
TRIGGER_START_TIME = datetime.datetime.now(tz=datetime.timezone.utc)
FAILED_RESULT_MSG = "Test message that appears when trigger have failed event."
BASE_CONTAINER_NAME = "base"
ON_FINISH_ACTION = "delete_pod"


@pytest.fixture
def trigger():
    return KubernetesPodTrigger(
        pod_name=POD_NAME,
        pod_namespace=NAMESPACE,
        base_container_name=BASE_CONTAINER_NAME,
        kubernetes_conn_id=CONN_ID,
        poll_interval=POLL_INTERVAL,
        cluster_context=CLUSTER_CONTEXT,
        config_dict=CONFIG_DICT,
        in_cluster=IN_CLUSTER,
        get_logs=GET_LOGS,
        startup_timeout=STARTUP_TIMEOUT_SECS,
        trigger_start_time=TRIGGER_START_TIME,
        on_finish_action=ON_FINISH_ACTION,
    )


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


class TestKubernetesPodTrigger:
    @staticmethod
    def _mock_pod_result(result_to_mock):
        f = Future()
        f.set_result(result_to_mock)
        return f

    def test_serialize(self, trigger):
        classpath, kwargs_dict = trigger.serialize()

        assert classpath == TRIGGER_PATH
        assert kwargs_dict == {
            "pod_name": POD_NAME,
            "pod_namespace": NAMESPACE,
            "base_container_name": BASE_CONTAINER_NAME,
            "kubernetes_conn_id": CONN_ID,
            "poll_interval": POLL_INTERVAL,
            "cluster_context": CLUSTER_CONTEXT,
            "config_dict": CONFIG_DICT,
            "in_cluster": IN_CLUSTER,
            "get_logs": GET_LOGS,
            "startup_timeout": STARTUP_TIMEOUT_SECS,
            "startup_check_interval": 5,
            "trigger_start_time": TRIGGER_START_TIME,
            "on_finish_action": ON_FINISH_ACTION,
            "should_delete_pod": ON_FINISH_ACTION == "delete_pod",
            "last_log_time": None,
            "logging_interval": None,
        }

    @pytest.mark.asyncio
    @mock.patch(f"{TRIGGER_PATH}._wait_for_pod_start")
    async def test_run_loop_return_success_event(self, mock_wait_pod, trigger):
        mock_wait_pod.return_value = ContainerState.TERMINATED

        expected_event = TriggerEvent(
            {
                "status": "success",
                "namespace": "default",
                "name": "test-pod-name",
                "message": "All containers inside pod have started successfully.",
            }
        )
        actual_event = await trigger.run().asend(None)

        assert actual_event == expected_event

    @pytest.mark.asyncio
    @mock.patch(f"{TRIGGER_PATH}._wait_for_pod_start")
    @mock.patch(f"{TRIGGER_PATH}.define_container_state")
    @mock.patch(f"{TRIGGER_PATH}.hook")
    async def test_run_loop_return_waiting_event(
        self, mock_hook, mock_method, mock_wait_pod, trigger, caplog
    ):
        mock_hook.get_pod.return_value = self._mock_pod_result(mock.MagicMock())
        mock_method.return_value = ContainerState.WAITING

        caplog.set_level(logging.INFO)

        task = asyncio.create_task(trigger.run().__anext__())
        await asyncio.sleep(0.5)

        assert not task.done()
        assert "Container is not completed and still working."
        assert f"Sleeping for {POLL_INTERVAL} seconds."

    @pytest.mark.asyncio
    @mock.patch(f"{TRIGGER_PATH}._wait_for_pod_start")
    @mock.patch(f"{TRIGGER_PATH}.define_container_state")
    @mock.patch(f"{TRIGGER_PATH}.hook")
    async def test_run_loop_return_running_event(
        self, mock_hook, mock_method, mock_wait_pod, trigger, caplog
    ):
        mock_hook.get_pod.return_value = self._mock_pod_result(mock.MagicMock())
        mock_method.return_value = ContainerState.RUNNING

        caplog.set_level(logging.INFO)

        task = asyncio.create_task(trigger.run().__anext__())
        await asyncio.sleep(0.5)

        assert not task.done()
        assert "Container is not completed and still working."
        assert f"Sleeping for {POLL_INTERVAL} seconds."

    @pytest.mark.asyncio
    @mock.patch(f"{TRIGGER_PATH}._wait_for_pod_start")
    @mock.patch(f"{TRIGGER_PATH}.define_container_state")
    @mock.patch(f"{TRIGGER_PATH}.hook")
    async def test_run_loop_return_failed_event(self, mock_hook, mock_method, mock_wait_pod, trigger):
        mock_hook.get_pod.return_value = self._mock_pod_result(
            mock.MagicMock(
                status=mock.MagicMock(
                    message=FAILED_RESULT_MSG,
                )
            )
        )
        mock_method.return_value = ContainerState.FAILED

        expected_event = TriggerEvent(
            {
                "status": "failed",
                "namespace": "default",
                "name": "test-pod-name",
                "message": "Container state failed",
                "last_log_time": None,
            }
        )
        actual_event = await trigger.run().asend(None)

        assert actual_event == expected_event

    @pytest.mark.asyncio
    @mock.patch(f"{TRIGGER_PATH}._wait_for_pod_start")
    @mock.patch(f"{TRIGGER_PATH}.hook")
    async def test_logging_in_trigger_when_exception_should_execute_successfully(
        self, mock_hook, mock_wait_pod, trigger, caplog
    ):
        """
        Test that KubernetesPodTrigger fires the correct event in case of an error.
        """

        mock_hook.get_pod.side_effect = Exception("Test exception")

        generator = trigger.run()
        actual = await generator.asend(None)
        actual_stack_trace = actual.payload.pop("stack_trace")
        assert (
            TriggerEvent(
                {"name": POD_NAME, "namespace": NAMESPACE, "status": "error", "message": "Test exception"}
            )
            == actual
        )
        assert actual_stack_trace.startswith("Traceback (most recent call last):")

    @pytest.mark.asyncio
    @mock.patch(f"{TRIGGER_PATH}.define_container_state")
    @mock.patch(f"{TRIGGER_PATH}.hook")
    async def test_logging_in_trigger_when_fail_should_execute_successfully(
        self, mock_hook, mock_method, trigger, caplog
    ):
        """
        Test that KubernetesPodTrigger fires the correct event in case of fail.
        """

        mock_hook.get_pod.return_value = self._mock_pod_result(mock.MagicMock())
        mock_method.return_value = ContainerState.FAILED
        caplog.set_level(logging.INFO)

        generator = trigger.run()
        await generator.asend(None)
        assert "Container logs:"

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "logging_interval, exp_event",
        [
            pytest.param(
                0,
                {
                    "status": "running",
                    "last_log_time": DateTime(2022, 1, 1),
                    "name": POD_NAME,
                    "namespace": NAMESPACE,
                },
                id="short_interval",
            ),
        ],
    )
    @mock.patch(f"{TRIGGER_PATH}.define_container_state")
    @mock.patch(f"{TRIGGER_PATH}._wait_for_pod_start")
    @mock.patch("airflow.providers.cncf.kubernetes.triggers.pod.AsyncKubernetesHook.get_pod")
    async def test_running_log_interval(
        self, mock_get_pod, mock_wait_pod, define_container_state, logging_interval, exp_event
    ):
        """
        If log interval given, should emit event with running status and last log time.
        Otherwise, should make it to second loop and emit "done" event.
        For this test we emit container status "running, running not".
        The first "running" status gets us out of wait_for_pod_start.
        The second "running" will fire a "running" event when logging interval is non-None.  When logging
        interval is None, the second "running" status will just result in continuation of the loop.  And
        when in the next loop we get a non-running status, the trigger fires a "done" event.
        """
        define_container_state.return_value = "running"
        trigger = KubernetesPodTrigger(
            pod_name=POD_NAME,
            pod_namespace=NAMESPACE,
            trigger_start_time=datetime.datetime.now(tz=datetime.timezone.utc),
            base_container_name=BASE_CONTAINER_NAME,
            startup_timeout=5,
            poll_interval=1,
            logging_interval=1,
            last_log_time=DateTime(2022, 1, 1),
        )
        assert await trigger.run().__anext__() == TriggerEvent(exp_event)

    @pytest.mark.parametrize(
        "container_state, expected_state",
        [
            (
                {"running": k8s.V1ContainerStateRunning(), "terminated": None, "waiting": None},
                ContainerState.RUNNING,
            ),
            (
                {"running": None, "terminated": k8s.V1ContainerStateTerminated(exit_code=0), "waiting": None},
                ContainerState.TERMINATED,
            ),
            (
                {"running": None, "terminated": None, "waiting": k8s.V1ContainerStateWaiting()},
                ContainerState.WAITING,
            ),
        ],
    )
    def test_define_container_state_should_execute_successfully(
        self, trigger, container_state, expected_state
    ):
        pod = k8s.V1Pod(
            metadata=k8s.V1ObjectMeta(name="base", namespace="default"),
            status=k8s.V1PodStatus(
                container_statuses=[
                    k8s.V1ContainerStatus(
                        name="base",
                        image="alpine",
                        image_id="1",
                        ready=True,
                        restart_count=1,
                        state=k8s.V1ContainerState(**container_state),
                    )
                ]
            ),
        )

        assert expected_state == trigger.define_container_state(pod)

    @pytest.mark.asyncio
    @pytest.mark.parametrize("container_state", [ContainerState.WAITING, ContainerState.UNDEFINED])
    @mock.patch(f"{TRIGGER_PATH}.define_container_state")
    @mock.patch(f"{TRIGGER_PATH}.hook")
    async def test_run_loop_return_timeout_event(
        self, mock_hook, mock_method, trigger, caplog, container_state
    ):
        trigger.trigger_start_time = TRIGGER_START_TIME - datetime.timedelta(minutes=2)
        mock_hook.get_pod.return_value = self._mock_pod_result(
            mock.MagicMock(
                status=mock.MagicMock(
                    phase=PodPhase.PENDING,
                )
            )
        )
        mock_method.return_value = container_state

        caplog.set_level(logging.INFO)

        generator = trigger.run()
        actual = await generator.asend(None)
        assert (
            TriggerEvent(
                {
                    "name": POD_NAME,
                    "namespace": NAMESPACE,
                    "status": "timeout",
                    "message": "Pod did not leave 'Pending' phase within specified timeout",
                }
            )
            == actual
        )

    @pytest.mark.asyncio
    @mock.patch(f"{TRIGGER_PATH}.define_container_state")
    @mock.patch(f"{TRIGGER_PATH}.hook")
    async def test_run_loop_return_success_for_completed_pod_after_timeout(
        self, mock_hook, mock_method, trigger, caplog
    ):
        """
        Test that the trigger correctly recognizes the pod is not pending even after the timeout has been
        reached. This may happen when a new triggerer process takes over the trigger, the pod already left
        pending state and the timeout has been reached.
        """
        trigger.trigger_start_time = TRIGGER_START_TIME - datetime.timedelta(minutes=2)
        mock_hook.get_pod.return_value = self._mock_pod_result(
            mock.MagicMock(
                status=mock.MagicMock(
                    phase=PodPhase.SUCCEEDED,
                )
            )
        )
        mock_method.return_value = ContainerState.TERMINATED

        caplog.set_level(logging.INFO)

        generator = trigger.run()
        actual = await generator.asend(None)
        assert (
            TriggerEvent(
                {
                    "name": POD_NAME,
                    "namespace": NAMESPACE,
                    "message": "All containers inside pod have started successfully.",
                    "status": "success",
                }
            )
            == actual
        )
