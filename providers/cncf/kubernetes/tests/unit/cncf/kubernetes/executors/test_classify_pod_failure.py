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

import pytest
from kubernetes import client as k8s

from airflow.providers.cncf.kubernetes.executors.kubernetes_executor_utils import (
    classify_pod_failure,
    collect_pod_failure_details,
)

from tests_common.test_utils.version_compat import AIRFLOW_V_3_4_PLUS

pytestmark = pytest.mark.skipif(
    not AIRFLOW_V_3_4_PLUS,
    reason="TaskFailureKind failure-context classification is Airflow 3.4+ only",
)


class TestClassifyPodFailure:
    """The K8s bridge maps a pod/container failure to a (failure_kind, infra_reason) the retry decision can trust."""

    @pytest.mark.parametrize(
        ("pod_reason", "container_reason", "expected_kind"),
        [
            # Node/platform ended the pod -> infra (earns a refund).
            ("Evicted", None, "infra"),
            ("Preempting", None, "infra"),
            ("NodeShutdown", None, "infra"),
            ("NodeLost", None, "infra"),
            ("DisruptionTarget", None, "infra"),
            ("TerminationByKubelet", None, "infra"),
            # A running task's pod removed by the platform (drain, preempt, spot reclaim,
            # force-delete) -> infra. The task didn't fail on its own; its pod was taken.
            ("PodDeleted", None, "infra"),
            # Container ended on its own -> user (no refund). The key one: an OOM against
            # the container's OWN limit is the app's memory problem, not an infra disruption.
            (None, "OOMKilled", "application"),
            (None, "Error", "application"),
            (None, "ContainerCannotRun", "application"),
            # A node eviction that also shows a container OOM is still infra (the node acted).
            ("Evicted", "OOMKilled", "infra"),
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

    def test_end_to_end_oomkilled_pod_is_application(self):
        # Mirrors the live kind result: a real OOMKilled container (exit 137) flows through
        # collect_pod_failure_details -> classify_pod_failure and classifies as application, not infra,
        # so an app OOM does not earn an infra refund.
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
        assert failure_kind == "application"
        assert infra_reason == "OOMKilled"

    def test_end_to_end_evicted_pod_is_infra(self):
        # A real node-pressure eviction: the kubelet sets the pod phase=Failed with
        # status.reason=Evicted (the pod object persists). This flows through
        # collect_pod_failure_details -> classify_pod_failure and classifies as infra,
        # so the disruption earns a refund. This is the positive-infra path the
        # conservative default requires (an unclassified death does not refund).
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
        assert failure_kind == "infra"
        assert infra_reason == "Evicted"

    def test_pod_deleted_while_running_is_infra(self):
        # A pod force-deleted / drained / preempted while its task was running has no
        # container-exit status; the watcher emits {pod_reason: PodDeleted}. It must
        # classify infra so the disruption earns a refund (the scheduler still gates on
        # the TI being non-terminal, so an Airflow-initiated stop is excluded upstream).
        failure_kind, infra_reason = classify_pod_failure(
            {"pod_status": "Running", "pod_reason": "PodDeleted"}
        )
        assert failure_kind == "infra"
        assert infra_reason == "PodDeleted"

    """The executor stashes a (failure_kind, infra_reason); the scheduler reads it once."""

    def test_base_executor_failure_info_round_trips_once(self):
        from airflow.executors.local_executor import LocalExecutor

        ex = LocalExecutor()
        key = ("dag", "task", "run", 1, -1)
        classified = classify_pod_failure({"pod_status": "Failed", "pod_reason": "Evicted"})
        ex.task_failure_info[key] = classified
        # first read returns it, and clears it so a later event can't reuse stale context
        assert ex.get_task_failure_info(key) is classified
        assert ex.get_task_failure_info(key) is None
