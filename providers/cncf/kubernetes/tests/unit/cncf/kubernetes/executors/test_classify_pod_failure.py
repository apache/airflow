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

import logging
from queue import Queue

import pytest
from kubernetes import client as k8s

from airflow.providers.cncf.kubernetes.executors.kubernetes_executor_utils import (
    KubernetesJobWatcher,
    classify_pod_failure,
    collect_pod_failure_details,
)
from airflow.providers.cncf.kubernetes.kube_config import KubeConfig

from tests_common.test_utils.version_compat import AIRFLOW_V_3_4_PLUS

pytestmark = pytest.mark.skipif(
    not AIRFLOW_V_3_4_PLUS,
    reason="TaskFailureKind failure-context classification is Airflow 3.4+ only",
)


class TestClassifyPodFailure:
    @pytest.mark.parametrize(
        ("pod_reason", "container_reason", "expected_kind"),
        [
            ("Evicted", None, None),
            ("Preempting", None, "infra"),
            ("NodeLost", None, "infra"),
            ("Terminated", None, "infra"),
            ("NodeShutdown", None, None),
            ("PodDeleted", None, None),
            (None, "OOMKilled", None),
            (None, "Error", None),
            (None, "ContainerCannotRun", None),
            ("Evicted", "OOMKilled", None),
            (None, "Preempting", None),
            (None, "Terminated", None),
            (None, "NodeLost", None),
        ],
    )
    def test_kind_classification(self, pod_reason, container_reason, expected_kind):
        details = {"pod_status": "Failed", "pod_reason": pod_reason, "container_reason": container_reason}
        result = classify_pod_failure(details)
        assert result is not None
        failure_kind, infra_reason = result
        assert failure_kind == expected_kind
        assert infra_reason == (pod_reason or container_reason)

    def test_none_when_nothing_to_classify(self):
        assert classify_pod_failure(None) is None
        assert classify_pod_failure({}) is None

    def test_oomkilled_pod_keeps_reason_without_infra_kind(self):
        pod = k8s.V1Pod(
            metadata=k8s.V1ObjectMeta(name="aip97-oom"),
            status=k8s.V1PodStatus(
                phase="Failed",
                container_statuses=[
                    k8s.V1ContainerStatus(
                        name="base",
                        image="python:3.11-slim",
                        image_id="",
                        ready=False,
                        restart_count=0,
                        state=k8s.V1ContainerState(
                            terminated=k8s.V1ContainerStateTerminated(reason="OOMKilled", exit_code=137)
                        ),
                    )
                ],
            ),
        )
        details = collect_pod_failure_details(pod, logging.getLogger("test"))
        assert details is not None
        assert details["container_reason"] == "OOMKilled"
        assert details["exit_code"] == 137

        failure_kind, infra_reason = classify_pod_failure(details)
        assert failure_kind is None
        assert infra_reason == "OOMKilled"

    def test_evicted_pod_without_disruption_evidence_is_unclassified(self):
        pod = k8s.V1Pod(
            metadata=k8s.V1ObjectMeta(name="aip97-evicted"),
            status=k8s.V1PodStatus(
                phase="Failed", reason="Evicted", message="The node was low on resource: memory."
            ),
        )
        details = collect_pod_failure_details(pod, logging.getLogger("test"))
        assert details is not None
        assert details["pod_reason"] == "Evicted"

        failure_kind, infra_reason = classify_pod_failure(details)
        assert failure_kind is None
        assert infra_reason == "Evicted"

    def test_node_pressure_eviction_keeps_its_confirmed_disruption(self):
        details = {
            "pod_status": "Failed",
            "pod_reason": "Evicted",
            "disruption_reason": "TerminationByKubelet",
        }
        assert classify_pod_failure(details) == ("infra", "TerminationByKubelet")

    def test_generic_pod_deletion_is_unclassified(self):
        failure_kind, infra_reason = classify_pod_failure(
            {"pod_status": "Running", "pod_reason": "PodDeleted"}
        )
        assert failure_kind is None
        assert infra_reason == "PodDeleted"


class TestDisruptionTargetCondition:
    """A control-plane disruption is only visible in the DisruptionTarget condition."""

    @pytest.mark.parametrize(
        "disruption_reason",
        [
            "PreemptionByScheduler",
            "DeletionByTaintManager",
            "TerminationByKubelet",
            "DeletionByPodGC",
            "DeletionByDeviceTaintManager",
        ],
    )
    def test_disruption_condition_is_infra(self, disruption_reason):
        details = {
            "pod_status": "Failed",
            "pod_reason": None,
            "container_reason": "Error",
            "disruption_reason": disruption_reason,
        }
        failure_kind, infra_reason = classify_pod_failure(details)
        assert failure_kind == "infra"
        assert infra_reason == disruption_reason

    def test_unrelated_condition_reason_stays_unclassified(self):
        details = {"pod_status": "Failed", "container_reason": "Error", "disruption_reason": "SomethingElse"}
        failure_kind, reason = classify_pod_failure(details)
        assert failure_kind is None
        assert reason == "SomethingElse"

    @pytest.mark.parametrize(
        ("disruption_reason", "pod_name"),
        [("DeletionByTaintManager", "aip97-victim"), ("PreemptionByScheduler", "aip97-victim2")],
    )
    def test_end_to_end_live_shape_is_infra(self, disruption_reason, pod_name):
        pod = k8s.V1Pod(
            metadata=k8s.V1ObjectMeta(name=pod_name, deletion_timestamp="2026-07-29T19:21:36Z"),
            status=k8s.V1PodStatus(
                phase="Failed",
                reason=None,
                conditions=[
                    k8s.V1PodCondition(type="DisruptionTarget", status="True", reason=disruption_reason)
                ],
                container_statuses=[
                    k8s.V1ContainerStatus(
                        name="base",
                        image="busybox:1.36",
                        image_id="",
                        ready=False,
                        restart_count=0,
                        state=k8s.V1ContainerState(
                            terminated=k8s.V1ContainerStateTerminated(reason="Error", exit_code=143)
                        ),
                    )
                ],
            ),
        )
        details = collect_pod_failure_details(pod, logging.getLogger("test"))
        assert details is not None
        assert details["pod_reason"] is None
        assert details["container_reason"] == "Error"
        assert details["disruption_reason"] == disruption_reason

        failure_kind, infra_reason = classify_pod_failure(details)
        assert failure_kind == "infra"
        assert infra_reason == disruption_reason

    @pytest.mark.parametrize(
        "conditions",
        [
            pytest.param(
                [k8s.V1PodCondition(type="Ready", status="False", reason="DeletionByTaintManager")],
                id="reason-on-a-different-condition-type",
            ),
            pytest.param(
                [
                    k8s.V1PodCondition(
                        type="DisruptionTarget", status="False", reason="DeletionByTaintManager"
                    )
                ],
                id="disruption-condition-no-longer-true",
            ),
        ],
    )
    def test_only_a_true_disruption_target_counts(self, conditions):
        pod = k8s.V1Pod(
            metadata=k8s.V1ObjectMeta(name="aip97-negative"),
            status=k8s.V1PodStatus(phase="Failed", reason=None, conditions=conditions),
        )
        details = collect_pod_failure_details(pod, logging.getLogger("test"))
        assert details is not None
        assert details["disruption_reason"] is None
        assert classify_pod_failure(details) is None

    @pytest.mark.parametrize(
        ("reason", "condition_status", "expected"),
        [
            ("PreemptionByScheduler", "True", ("infra", "PreemptionByScheduler")),
            ("DeletionByTaintManager", "True", ("infra", "DeletionByTaintManager")),
            ("OtherDisruption", "True", (None, "OtherDisruption")),
            ("PreemptionByScheduler", "False", (None, None)),
            (None, "True", (None, None)),
        ],
    )
    def test_running_deletion_preserves_only_observed_disruption_cause(
        self, reason, condition_status, expected
    ):
        pod = k8s.V1Pod(
            metadata=k8s.V1ObjectMeta(
                name="deleted-worker",
                namespace="default",
                labels={},
                deletion_timestamp="2026-09-16T00:00:00Z",
            ),
            status=k8s.V1PodStatus(
                phase="Running",
                conditions=[
                    k8s.V1PodCondition(type="DisruptionTarget", status=condition_status, reason=reason)
                ],
            ),
        )
        watcher_queue = Queue()
        watcher = KubernetesJobWatcher(
            namespace="default",
            watcher_queue=watcher_queue,
            resource_version="1",
            scheduler_job_id="1",
            kube_config=KubeConfig(),
        )
        watcher.process_status(
            pod_name=pod.metadata.name,
            namespace=pod.metadata.namespace,
            status=pod.status.phase,
            annotations={"dag_id": "dag", "task_id": "task", "run_id": "manual__test", "try_number": "1"},
            resource_version="2",
            event={"type": "DELETED", "object": pod},
        )
        result = watcher_queue.get_nowait()
        assert (classify_pod_failure(result.failure_details) or (None, None)) == expected
