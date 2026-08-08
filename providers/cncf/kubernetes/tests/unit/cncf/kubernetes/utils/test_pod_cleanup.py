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

from datetime import datetime, timezone

from kubernetes.client import models as k8s

from airflow.providers.cncf.kubernetes.pod_generator import make_safe_label_value
from airflow.providers.cncf.kubernetes.utils.pod_cleanup import cleanup_kpo_zombie_pods


class FakeSession:
    def __init__(self, rows):
        self.rows = rows

    def execute(self, _statement):
        return self.rows


class FakeKubeClient:
    def __init__(self):
        self.deleted = []

    def delete_namespaced_pod(self, name, namespace, body, **kwargs):
        self.deleted.append((name, namespace, body, kwargs))


def make_kpo_pod(
    name,
    *,
    dag_id="dag",
    task_id="task",
    run_id="run",
    try_number="1",
    map_index=None,
    created_at=None,
):
    labels = {
        "dag_id": make_safe_label_value(dag_id),
        "task_id": make_safe_label_value(task_id),
        "run_id": make_safe_label_value(run_id),
        "try_number": try_number,
        "kubernetes_pod_operator": "True",
    }
    if map_index is not None:
        labels["map_index"] = str(map_index)
    return k8s.V1Pod(
        metadata=k8s.V1ObjectMeta(
            name=name, namespace="default", labels=labels, creation_timestamp=created_at
        )
    )


def make_task_instance_row(*, dag_id="dag", task_id="task", run_id="run", map_index=-1, try_number=1):
    return dag_id, task_id, run_id, map_index, try_number


def run_cleanup(pods, rows, *, max_deletes=100):
    kube_client = FakeKubeClient()
    result = cleanup_kpo_zombie_pods(
        list_pods=lambda _query_kwargs: pods,
        kube_client=kube_client,
        max_deletes=max_deletes,
        grace_period_seconds=5,
        delete_options={"propagation_policy": "Foreground"},
        kube_client_request_args={"request_timeout": 10},
        session=FakeSession(rows),
    )
    return result, kube_client


def test_cleanup_deletes_pod_without_active_task_instance():
    pod = make_kpo_pod("zombie")

    result, kube_client = run_cleanup([pod], [])

    assert result.scanned == 1
    assert result.zombies == 1
    assert result.deleted == 1
    name, namespace, body, kwargs = kube_client.deleted[0]
    assert name == "zombie"
    assert namespace == "default"
    assert body.grace_period_seconds == 5
    assert body.propagation_policy == "Foreground"
    assert kwargs == {"request_timeout": 10}


def test_cleanup_keeps_pod_with_active_matching_task_instance():
    pod = make_kpo_pod("active")

    result, kube_client = run_cleanup([pod], [make_task_instance_row()])

    assert result.zombies == 0
    assert result.deleted == 0
    assert kube_client.deleted == []


def test_cleanup_deletes_old_retry_pod():
    pod = make_kpo_pod("old-try", try_number="1")

    result, kube_client = run_cleanup([pod], [make_task_instance_row(try_number=2)])

    assert result.zombies == 1
    assert result.deleted == 1
    assert kube_client.deleted[0][0] == "old-try"


def test_cleanup_normalizes_active_task_instance_labels_before_matching():
    long_run_id = "scheduled__" + "a" * 100 + ":with:colons"
    pod = make_kpo_pod("active-long-run", run_id=long_run_id)

    result, kube_client = run_cleanup([pod], [make_task_instance_row(run_id=long_run_id)])

    assert result.zombies == 0
    assert result.deleted == 0
    assert kube_client.deleted == []


def test_cleanup_skips_pods_with_incomplete_labels():
    pod = make_kpo_pod("missing-try")
    del pod.metadata.labels["try_number"]

    result, kube_client = run_cleanup([pod], [])

    assert result.scanned == 1
    assert result.candidates == 0
    assert result.zombies == 0
    assert result.skipped == 1
    assert kube_client.deleted == []


def test_cleanup_respects_max_deletes_and_deletes_oldest_pods_first():
    newer = make_kpo_pod("newer", created_at=datetime(2024, 1, 2, tzinfo=timezone.utc))
    older = make_kpo_pod("older", created_at=datetime(2024, 1, 1, tzinfo=timezone.utc))

    result, kube_client = run_cleanup([newer, older], [], max_deletes=1)

    assert result.zombies == 2
    assert result.deleted == 1
    assert result.skipped == 1
    assert kube_client.deleted[0][0] == "older"
