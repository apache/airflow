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

import json
from contextlib import nullcontext
from unittest import mock

import pytest
import tenacity
from kubernetes.client import CoreV1Api, models as k8s
from kubernetes.client.rest import ApiException

from airflow.providers.cncf.kubernetes.hooks.kubernetes import AsyncKubernetesHook, KubernetesHook
from airflow.providers.cncf.kubernetes.triggers.kueue import (
    KubernetesKueueJobTrigger,
    KueueJobState,
    build_job_event,
    build_pod_label_selector,
    collect_xcom_results,
    get_job_state,
    list_job_pods,
)
from airflow.providers.cncf.kubernetes.utils.pod_manager import PodManager, XComRetrievalError
from airflow.triggers.base import TriggerEvent

TRIGGER_PATH = "airflow.providers.cncf.kubernetes.triggers.kueue.{}"
JOB_NAME = "job-test"
NAMESPACE = "team-a"
JOB_UID = "job-uid"
POD_LABEL_SELECTOR = "batch.kubernetes.io/controller-uid=job-uid"
TRIGGER_KWARGS = {
    "job_name": JOB_NAME,
    "job_namespace": NAMESPACE,
    "base_container_name": "base",
    "kubernetes_conn_id": "kubernetes_default",
    "poll_interval": 0,
    "cluster_context": "cluster",
    "config_file": "/path/to/config",
    "in_cluster": False,
    "do_xcom_push": True,
}


def build_job(suspend: bool, condition: str | None = None, succeeded: int | None = None) -> k8s.V1Job:
    conditions = (
        [k8s.V1JobCondition(type=condition, status="True", reason="BackoffLimitExceeded")]
        if condition
        else []
    )
    return k8s.V1Job(
        metadata=k8s.V1ObjectMeta(name=JOB_NAME, namespace=NAMESPACE, uid=JOB_UID),
        spec=k8s.V1JobSpec(
            suspend=suspend,
            template=k8s.V1PodTemplateSpec(),
            selector=k8s.V1LabelSelector(match_labels={"batch.kubernetes.io/controller-uid": "job-uid"}),
        ),
        status=k8s.V1JobStatus(conditions=conditions, succeeded=succeeded),
    )


def build_pod(
    name: str,
    base: str | None = None,
    sidecar: str | None = None,
    phase: str = "Running",
    owner_uid: str | None = JOB_UID,
) -> k8s.V1Pod:
    """
    Build a pod of the Job, with its base and XCom sidecar containers "running", "terminated" or None.

    ``owner_uid`` is the uid of the Job that owns the pod, None for a pod without owner.
    """
    owner_references = (
        [k8s.V1OwnerReference(api_version="batch/v1", kind="Job", name=JOB_NAME, uid=owner_uid)]
        if owner_uid
        else None
    )
    statuses = [
        k8s.V1ContainerStatus(
            name=container_name,
            image="",
            image_id="",
            ready=state == "running",
            restart_count=0,
            state=k8s.V1ContainerState(
                running=k8s.V1ContainerStateRunning() if state == "running" else None,
                terminated=k8s.V1ContainerStateTerminated(exit_code=0) if state == "terminated" else None,
            ),
        )
        for container_name, state in (("base", base), ("airflow-xcom-sidecar", sidecar))
        if state
    ]
    return k8s.V1Pod(
        metadata=k8s.V1ObjectMeta(
            name=name,
            namespace=NAMESPACE,
            labels={"try_number": "1", "batch.kubernetes.io/controller-uid": "job-uid"},
            owner_references=owner_references,
        ),
        status=k8s.V1PodStatus(phase=phase, container_statuses=statuses),
    )


@pytest.mark.parametrize(
    ("job", "expected_state"),
    [
        (build_job(True), KueueJobState.SUSPENDED),
        (build_job(False), KueueJobState.RUNNING),
        (build_job(False, "Complete"), KueueJobState.FINISHED),
        (build_job(False, "Failed"), KueueJobState.FINISHED),
    ],
    ids=["suspended", "running", "complete", "failed"],
)
def test_get_job_state(job, expected_state):
    assert get_job_state(job) == expected_state


@pytest.mark.parametrize(
    ("selector", "expected_label_selector"),
    [
        pytest.param(
            k8s.V1LabelSelector(match_labels={"batch.kubernetes.io/controller-uid": "job-uid"}),
            POD_LABEL_SELECTOR,
            id="generated-selector",
        ),
        pytest.param(
            k8s.V1LabelSelector(
                match_labels={"team": "a", "app": "train"},
                match_expressions=[
                    k8s.V1LabelSelectorRequirement(key="tier", operator="In", values=["gpu", "cpu"]),
                    k8s.V1LabelSelectorRequirement(key="env", operator="NotIn", values=["dev"]),
                    k8s.V1LabelSelectorRequirement(key="run", operator="Exists"),
                    k8s.V1LabelSelectorRequirement(key="skip", operator="DoesNotExist"),
                ],
            ),
            "app=train,team=a,tier in (gpu,cpu),env notin (dev),run,!skip",
            id="manual-selector",
        ),
    ],
)
def test_build_pod_label_selector(selector, expected_label_selector):
    job = k8s.V1Job(spec=k8s.V1JobSpec(template=k8s.V1PodTemplateSpec(), selector=selector))

    assert build_pod_label_selector(job) == expected_label_selector


class TestCollectXcomResults:
    def setup_method(self):
        self.client = mock.create_autospec(CoreV1Api, instance=True)
        self.pod_manager = mock.create_autospec(PodManager, instance=True)

    def test_reads_only_new_pods_with_terminated_base_and_running_sidecar(self):
        ready = build_pod("ready", base="terminated", sidecar="running")
        self.client.list_namespaced_pod.return_value = k8s.V1PodList(
            items=[
                build_pod("pending"),
                build_pod("running", base="running", sidecar="running"),
                ready,
                build_pod("already-read", base="terminated", sidecar="running"),
            ]
        )
        self.pod_manager.extract_xcom.return_value = '{"a": 1}'
        xcom_results = {"already-read": "{}"}

        collect_xcom_results(self.client, self.pod_manager, build_job(False), "base", xcom_results)

        self.client.list_namespaced_pod.assert_called_once_with(
            namespace=NAMESPACE, label_selector=POD_LABEL_SELECTOR
        )
        self.pod_manager.extract_xcom.assert_called_once_with(ready, ignore_kill_failure=False)
        assert xcom_results == {"already-read": "{}", "ready": '{"a": 1}'}

    @pytest.mark.parametrize(
        ("error", "read_pod_error", "expectation"),
        [
            (XComRetrievalError("sidecar stopped"), None, nullcontext()),
            (ApiException(status=404), None, nullcontext()),
            (ApiException(status=0), ApiException(status=404), nullcontext()),
            (ApiException(status=0), None, pytest.raises(ApiException)),
            (ApiException(status=403), None, pytest.raises(ApiException)),
            (json.JSONDecodeError("Unterminated string", '{"a', 1), None, nullcontext()),
        ],
        ids=[
            "sidecar-stopped",
            "pod-deleted",
            "exec-failed-on-deleted-pod",
            "exec-failed-on-existing-pod",
            "other-api-error",
            "partial-xcom-json",
        ],
    )
    def test_skips_pods_stopped_or_deleted_by_preemption(self, error, read_pod_error, expectation):
        self.client.list_namespaced_pod.return_value = k8s.V1PodList(
            items=[build_pod("preempted", base="terminated", sidecar="running")]
        )
        self.client.read_namespaced_pod.side_effect = read_pod_error
        self.pod_manager.extract_xcom.side_effect = error
        xcom_results: dict[str, str] = {}

        with expectation:
            collect_xcom_results(self.client, self.pod_manager, build_job(False), "base", xcom_results)
        assert xcom_results == {}


@mock.patch.object(list_job_pods.retry, "wait", tenacity.wait_none())
def test_list_job_pods_retries_transient_api_errors():
    client = mock.create_autospec(CoreV1Api, instance=True)
    pods = [build_pod("pod")]
    client.list_namespaced_pod.side_effect = [ApiException(status=503), k8s.V1PodList(items=pods)]

    assert list_job_pods(client, NAMESPACE, POD_LABEL_SELECTOR, JOB_UID) == pods
    assert client.list_namespaced_pod.call_count == 2


def test_list_job_pods_keeps_only_pods_owned_by_the_job():
    """
    With manualSelector, a selector that is not unique may also match the pods of other Jobs, or
    orphan pods the Job controller has not adopted yet.
    """
    client = mock.create_autospec(CoreV1Api, instance=True)
    owned = build_pod("owned")
    client.list_namespaced_pod.return_value = k8s.V1PodList(
        items=[owned, build_pod("other-job", owner_uid="other-job-uid"), build_pod("orphan", owner_uid=None)]
    )

    assert list_job_pods(client, NAMESPACE, POD_LABEL_SELECTOR, JOB_UID) == [owned]
    client.list_namespaced_pod.assert_called_once_with(namespace=NAMESPACE, label_selector=POD_LABEL_SELECTOR)


class TestKubernetesKueueJobTrigger:
    def test_serialize(self):
        assert KubernetesKueueJobTrigger(**TRIGGER_KWARGS).serialize() == (
            "airflow.providers.cncf.kubernetes.triggers.kueue.KubernetesKueueJobTrigger",
            TRIGGER_KWARGS,
        )

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("do_xcom_push", "expected_collect_calls", "expected_xcom_result"),
        [
            pytest.param(True, 2, {"pod": "{}"}, id="xcom-read-while-running"),
            pytest.param(False, 0, {}, id="without-xcom"),
        ],
    )
    @mock.patch(TRIGGER_PATH.format("collect_xcom_results"), autospec=True)
    @mock.patch(
        TRIGGER_PATH.format("KubernetesKueueJobTrigger.sync_hook"),
        new_callable=lambda: mock.create_autospec(KubernetesHook, instance=True),
    )
    @mock.patch(
        TRIGGER_PATH.format("KubernetesKueueJobTrigger.hook"),
        new_callable=lambda: mock.create_autospec(AsyncKubernetesHook, instance=True),
    )
    async def test_run_waits_through_preemption(
        self,
        mock_hook,
        mock_sync_hook,
        mock_collect,
        do_xcom_push,
        expected_collect_calls,
        expected_xcom_result,
    ):
        """Kueue admits the Job, preempts it, admits it again, then the Job completes."""
        jobs = [
            build_job(True),
            build_job(False),
            build_job(True),
            build_job(False),
            build_job(False, "Complete"),
        ]
        mock_hook.get_job_status.side_effect = jobs

        def read_xcom(client, pod_manager, job, base_container_name, xcom_results):
            xcom_results["pod"] = "{}"

        mock_collect.side_effect = read_xcom
        trigger = KubernetesKueueJobTrigger(**TRIGGER_KWARGS)
        trigger.do_xcom_push = do_xcom_push

        events = [event async for event in trigger.run()]

        assert events == [TriggerEvent(build_job_event(jobs[-1], expected_xcom_result))]
        mock_hook.get_job_status.assert_awaited_with(name=JOB_NAME, namespace=NAMESPACE)
        assert mock_collect.call_count == expected_collect_calls

    @pytest.mark.asyncio
    @mock.patch(TRIGGER_PATH.format("AsyncKubernetesHook.close"), autospec=True)
    async def test_cleanup_closes_hook(self, mock_close):
        trigger = KubernetesKueueJobTrigger(**TRIGGER_KWARGS)

        await trigger.cleanup()

        mock_close.assert_awaited_once_with(trigger.hook)
