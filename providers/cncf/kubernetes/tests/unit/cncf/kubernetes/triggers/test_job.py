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
from contextlib import asynccontextmanager
from dataclasses import dataclass
from unittest import mock

import pytest
from kubernetes.client.rest import ApiException

from airflow.providers.cncf.kubernetes.triggers.job import KubernetesJobTrigger
from airflow.triggers.base import TriggerEvent

TRIGGER_PATH = "airflow.providers.cncf.kubernetes.triggers.job.{}"
TRIGGER_CLASS = TRIGGER_PATH.format("KubernetesJobTrigger")
HOOK_PATH = "airflow.providers.cncf.kubernetes.hooks.kubernetes.AsyncKubernetesHook"
JOB_NAME = "test-job-name"
POD_NAME = "test-pod-name"
CONTAINER_NAME = "test-container-name"
NAMESPACE = "default"
CONN_ID = "test_kubernetes_conn_id"
POLL_INTERVAL = 2
CLUSTER_CONTEXT = "test-context"
CONFIG_FILE = "/path/to/config/file"
IN_CLUSTER = False
GET_LOGS = True
XCOM_PUSH = False


@dataclass(slots=True)
class TriggerHarness:
    """Convenience wrapper for trigger tests with patched hook and pod manager."""

    trigger: KubernetesJobTrigger
    hook: mock.MagicMock
    pod_manager: mock.MagicMock

    async def run_once(self) -> TriggerEvent:
        return await asyncio.wait_for(self.trigger.run().__anext__(), timeout=0.2)


def _make_pod(name: str, namespace: str = "default") -> mock.MagicMock:
    pod = mock.MagicMock()
    pod.metadata.name = name
    pod.metadata.namespace = namespace
    return pod


@asynccontextmanager
async def _pending_job_task():
    """Create and clean up a job task that never finishes."""

    async def never_finishes():
        await asyncio.Future()

    job_task = asyncio.create_task(never_finishes())
    try:
        yield job_task
    finally:
        job_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await job_task


@pytest.fixture
def trigger():
    return KubernetesJobTrigger(
        job_name=JOB_NAME,
        job_namespace=NAMESPACE,
        pod_names=[
            POD_NAME,
        ],
        pod_namespace=NAMESPACE,
        base_container_name=CONTAINER_NAME,
        kubernetes_conn_id=CONN_ID,
        poll_interval=POLL_INTERVAL,
        cluster_context=CLUSTER_CONTEXT,
        config_file=CONFIG_FILE,
        in_cluster=IN_CLUSTER,
        get_logs=GET_LOGS,
        do_xcom_push=XCOM_PUSH,
    )


@pytest.fixture
def parallelism_gt_completions_trigger():
    """Fixture for issue #64867: parallelism > completions snapshot can contain extra pods."""
    return KubernetesJobTrigger(
        job_name="job",
        job_namespace="default",
        pod_names=["pod-1", "pod-2"],  # parallelism=2, completions=1
        pod_namespace="default",
        base_container_name="base",
        do_xcom_push=True,
        get_logs=False,
        poll_interval=0.01,
    )


@pytest.fixture
def parallelism_gt_completions_harness(parallelism_gt_completions_trigger):
    with (
        mock.patch(
            "airflow.providers.cncf.kubernetes.triggers.job.KubernetesJobTrigger.pod_manager"
        ) as pod_manager,
        mock.patch("airflow.providers.cncf.kubernetes.triggers.job.KubernetesJobTrigger.hook") as hook,
    ):
        mock_job = mock.MagicMock()
        mock_job.metadata.name = "job"
        mock_job.metadata.namespace = "default"
        mock_job.to_dict.return_value = {"status": {"succeeded": 1}}
        hook.wait_until_job_complete = mock.AsyncMock(return_value=mock_job)
        hook.is_job_failed.return_value = False
        yield TriggerHarness(
            trigger=parallelism_gt_completions_trigger,
            hook=hook,
            pod_manager=pod_manager,
        )


class TestKubernetesJobTrigger:
    @pytest.mark.parametrize("invalid_poll_interval", [0, -0.1])
    def test_init_raises_on_non_positive_poll_interval(self, invalid_poll_interval):
        with pytest.raises(
            ValueError, match="Invalid value for poll_interval. Expected value greater than 0"
        ):
            KubernetesJobTrigger(
                job_name=JOB_NAME,
                job_namespace=NAMESPACE,
                pod_names=[POD_NAME],
                pod_namespace=NAMESPACE,
                base_container_name=CONTAINER_NAME,
                poll_interval=invalid_poll_interval,
            )

    def test_serialize(self, trigger):
        classpath, kwargs_dict = trigger.serialize()

        assert classpath == TRIGGER_CLASS
        assert kwargs_dict == {
            "job_name": JOB_NAME,
            "job_namespace": NAMESPACE,
            "pod_names": [
                POD_NAME,
            ],
            "pod_namespace": NAMESPACE,
            "base_container_name": CONTAINER_NAME,
            "kubernetes_conn_id": CONN_ID,
            "poll_interval": POLL_INTERVAL,
            "cluster_context": CLUSTER_CONTEXT,
            "config_file": CONFIG_FILE,
            "in_cluster": IN_CLUSTER,
            "get_logs": GET_LOGS,
            "do_xcom_push": XCOM_PUSH,
        }

    @pytest.mark.asyncio
    @mock.patch(f"{TRIGGER_CLASS}.hook")
    async def test_run_success(self, mock_hook, trigger):
        mock_job = mock.MagicMock()
        mock_job.metadata.name = JOB_NAME
        mock_job.metadata.namespace = NAMESPACE
        mock_hook.wait_until_job_complete.side_effect = mock.AsyncMock(return_value=mock_job)

        mock_pod = mock.MagicMock()
        mock_pod.metadata.name = POD_NAME
        mock_pod.metadata.namespace = NAMESPACE
        mock_hook.get_pod.side_effect = mock.AsyncMock(return_value=mock_pod)

        mock_is_job_failed = mock_hook.is_job_failed
        mock_is_job_failed.return_value = False

        mock_job_dict = mock_job.to_dict.return_value

        event_actual = await trigger.run().asend(None)

        mock_hook.wait_until_job_complete.assert_called_once_with(
            name=JOB_NAME,
            namespace=NAMESPACE,
            poll_interval=POLL_INTERVAL,
        )
        mock_job.to_dict.assert_called_once()
        mock_is_job_failed.assert_called_once_with(job=mock_job)
        assert event_actual == TriggerEvent(
            {
                "name": JOB_NAME,
                "namespace": NAMESPACE,
                "pod_names": [
                    POD_NAME,
                ],
                "pod_namespace": NAMESPACE,
                "status": "success",
                "message": "Job completed successfully",
                "job": mock_job_dict,
                "xcom_result": None,
            }
        )

    @pytest.mark.asyncio
    @mock.patch(f"{TRIGGER_CLASS}.hook")
    async def test_run_fail(self, mock_hook, trigger):
        mock_job = mock.MagicMock()
        mock_job.metadata.name = JOB_NAME
        mock_job.metadata.namespace = NAMESPACE
        mock_hook.wait_until_job_complete.side_effect = mock.AsyncMock(return_value=mock_job)

        mock_pod = mock.MagicMock()
        mock_pod.metadata.name = POD_NAME
        mock_pod.metadata.namespace = NAMESPACE
        mock_hook.get_pod.side_effect = mock.AsyncMock(return_value=mock_pod)

        mock_is_job_failed = mock_hook.is_job_failed
        mock_is_job_failed.return_value = "Error"

        mock_job_dict = mock_job.to_dict.return_value

        event_actual = await trigger.run().asend(None)

        mock_hook.wait_until_job_complete.assert_called_once_with(
            name=JOB_NAME,
            namespace=NAMESPACE,
            poll_interval=POLL_INTERVAL,
        )
        mock_job.to_dict.assert_called_once()
        mock_is_job_failed.assert_called_once_with(job=mock_job)
        assert event_actual == TriggerEvent(
            {
                "name": JOB_NAME,
                "namespace": NAMESPACE,
                "pod_names": [
                    POD_NAME,
                ],
                "pod_namespace": NAMESPACE,
                "status": "error",
                "message": "Job failed with error: Error",
                "job": mock_job_dict,
                "xcom_result": None,
            }
        )

    @mock.patch(TRIGGER_PATH.format("AsyncKubernetesHook"))
    def test_hook(self, mock_hook, trigger):
        hook_expected = mock_hook.return_value

        hook_actual = trigger.hook

        mock_hook.assert_called_once_with(
            conn_id=CONN_ID,
            in_cluster=IN_CLUSTER,
            config_file=CONFIG_FILE,
            cluster_context=CLUSTER_CONTEXT,
        )
        assert hook_actual == hook_expected

    @pytest.mark.asyncio
    async def test_run_completes_when_job_is_done_even_if_some_snapshot_pods_never_complete(
        self, parallelism_gt_completions_harness
    ):
        """Regression test for #64867."""
        pod_1 = _make_pod("pod-1")
        pod_2 = _make_pod("pod-2")

        async def get_pod(name, namespace):
            return pod_1 if name == "pod-1" else pod_2

        async def wait_until_container_complete(name, namespace, container_name, poll_interval):
            if name == "pod-1":
                return None
            # emulate stale pod from snapshot that never reaches completed state
            await asyncio.Future()

        async def wait_until_container_started(name, namespace, container_name, poll_interval):
            return None

        harness = parallelism_gt_completions_harness
        harness.hook.get_pod.side_effect = get_pod
        harness.hook.wait_until_container_complete.side_effect = wait_until_container_complete
        harness.hook.wait_until_container_started.side_effect = wait_until_container_started
        harness.pod_manager.extract_xcom.return_value = '{"ok": true}'

        event_actual = await harness.run_once()

        harness.hook.wait_until_job_complete.assert_called_once_with(
            name="job",
            namespace="default",
            poll_interval=0.01,
        )
        assert event_actual == TriggerEvent(
            {
                "name": "job",
                "namespace": "default",
                "pod_names": None,
                "pod_namespace": None,
                "status": "success",
                "message": "Job completed successfully",
                "job": {"status": {"succeeded": 1}},
                "xcom_result": ['{"ok": true}', '{"ok": true}'],
            }
        )

    @pytest.mark.asyncio
    async def test_run_skips_deleted_snapshot_pod_and_completes_when_job_is_done(
        self, parallelism_gt_completions_harness
    ):
        """Regression test for #64867."""
        pod = _make_pod("pod-1")

        async def get_pod(name, namespace):
            if name == "pod-2":
                raise ApiException(status=404, reason="Not Found")
            return pod

        harness = parallelism_gt_completions_harness
        harness.hook.get_pod.side_effect = get_pod
        harness.hook.wait_until_container_complete = mock.AsyncMock(return_value=None)
        harness.hook.wait_until_container_started = mock.AsyncMock(return_value=None)
        harness.pod_manager.extract_xcom.return_value = '{"ok": true}'

        event_actual = await harness.run_once()

        assert event_actual.payload["status"] == "success"
        assert event_actual.payload["xcom_result"] == ['{"ok": true}']

    @pytest.mark.asyncio
    async def test_run_collects_later_pod_xcom_best_effort_after_job_done(
        self, parallelism_gt_completions_harness
    ):
        """Regression test for #64867: continue collecting from remaining pods after job completion."""
        pod_1 = _make_pod("pod-1")
        pod_2 = _make_pod("pod-2")

        async def get_pod(name, namespace):
            return pod_1 if name == "pod-1" else pod_2

        async def wait_until_container_complete(name, namespace, container_name, poll_interval):
            if name == "pod-1":
                await asyncio.Future()
            return None

        def extract_xcom(pod):
            if pod.metadata.name == "pod-1":
                raise RuntimeError("pod-1 xcom unavailable")
            return '{"from":"pod-2"}'

        harness = parallelism_gt_completions_harness
        harness.hook.get_pod.side_effect = get_pod
        harness.hook.wait_until_container_complete.side_effect = wait_until_container_complete
        harness.hook.wait_until_container_started = mock.AsyncMock(return_value=None)
        harness.pod_manager.extract_xcom.side_effect = extract_xcom

        event_actual = await harness.run_once()

        assert event_actual.payload["status"] == "success"
        assert event_actual.payload["xcom_result"] == ['{"from":"pod-2"}']
        harness.hook.wait_until_container_complete.assert_called_once()
        harness.hook.wait_until_container_started.assert_not_called()

    @pytest.mark.asyncio
    async def test_wait_until_container_state_or_job_done_does_not_restart_wait_task(self, trigger):
        # Polling loop must not cancel/recreate wait_method on every tick.
        # If it does, slow/loaded clusters can starve readiness detection forever.
        trigger.poll_interval = 1
        wait_method_calls = 0
        wait_loop_calls = 0
        # Gate that allows us to complete wait_method exactly when we want.
        wait_gate: asyncio.Future[None] = asyncio.Future()

        async def slow_wait_method(name, namespace, container_name, poll_interval):
            nonlocal wait_method_calls
            wait_method_calls += 1
            await wait_gate
            return None

        async def fake_asyncio_wait(tasks, timeout=None, return_when=None):
            nonlocal wait_loop_calls
            wait_loop_calls += 1
            # Simulate a few poll ticks where nothing finishes yet.
            if wait_loop_calls < 3:
                return set(), set(tasks)

            # Then release wait_method and let event loop settle task completion.
            if not wait_gate.done():
                wait_gate.set_result(None)
            await asyncio.sleep(0)
            done = {task for task in tasks if task.done()}
            return done, set(tasks) - done

        # Job task stays pending; readiness must come from wait_method task itself.
        async with _pending_job_task() as job_task:
            with mock.patch(
                "airflow.providers.cncf.kubernetes.triggers.job.asyncio.wait",
                side_effect=fake_asyncio_wait,
            ):
                outcome = await asyncio.wait_for(
                    trigger._wait_until_container_state_or_job_done(
                        pod_name=POD_NAME,
                        container_name=CONTAINER_NAME,
                        wait_method=slow_wait_method,
                        job_task=job_task,
                        state_label="completed",
                    ),
                    timeout=0.3,
                )

        assert outcome == "ready"
        # Key assertion: wait_method coroutine is started once and stays alive across poll ticks.
        assert wait_method_calls == 1
        assert wait_loop_calls >= 3
