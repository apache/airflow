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
"""Clean up KubernetesPodOperator pods that no longer belong to active task instances."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING

from kubernetes import client
from kubernetes.client.rest import ApiException
from sqlalchemy import select

from airflow.models.taskinstance import TaskInstance
from airflow.providers.cncf.kubernetes.pod_generator import make_safe_label_value
from airflow.utils.session import NEW_SESSION, provide_session
from airflow.utils.state import State

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable

    from kubernetes.client import models as k8s
    from sqlalchemy.orm import Session

log = logging.getLogger(__name__)

KPO_LABEL_SELECTOR = "kubernetes_pod_operator=True"


@dataclass(frozen=True)
class _KpoPodKey:
    dag_id: str
    task_id: str
    run_id: str
    map_index: int


@dataclass(frozen=True)
class _KpoPodRef:
    key: _KpoPodKey
    try_number: int
    pod: k8s.V1Pod


@dataclass(frozen=True)
class KpoZombiePodCleanupResult:
    """Summary of one KPO zombie pod cleanup pass."""

    scanned: int
    candidates: int
    zombies: int
    deleted: int
    skipped: int


def _extract_kpo_pod_ref(pod: k8s.V1Pod) -> _KpoPodRef | None:
    labels = pod.metadata.labels or {}
    try:
        dag_id = labels["dag_id"]
        task_id = labels["task_id"]
        run_id = labels["run_id"]
        try_number = int(labels["try_number"])
    except (KeyError, TypeError, ValueError):
        return None

    try:
        map_index = int(labels.get("map_index", "-1"))
    except (TypeError, ValueError):
        return None

    return _KpoPodRef(
        key=_KpoPodKey(dag_id=dag_id, task_id=task_id, run_id=run_id, map_index=map_index),
        try_number=try_number,
        pod=pod,
    )


def _build_active_task_instance_try_numbers(
    pod_refs: Iterable[_KpoPodRef], *, session: Session
) -> dict[_KpoPodKey, int]:
    pod_refs = list(pod_refs)
    if not pod_refs:
        return {}

    rows = session.execute(
        select(
            TaskInstance.dag_id,
            TaskInstance.task_id,
            TaskInstance.run_id,
            TaskInstance.map_index,
            TaskInstance.try_number,
        )
        .where(TaskInstance.state.in_(State.unfinished))
        .where(TaskInstance.map_index.in_({pod_ref.key.map_index for pod_ref in pod_refs}))
    )

    active_try_numbers: dict[_KpoPodKey, int] = {}
    for dag_id, task_id, run_id, map_index, try_number in rows:
        active_try_numbers[
            _KpoPodKey(
                dag_id=make_safe_label_value(dag_id),
                task_id=make_safe_label_value(task_id),
                run_id=make_safe_label_value(run_id),
                map_index=map_index,
            )
        ] = try_number
    return active_try_numbers


def _get_pod_creation_timestamp(pod_ref: _KpoPodRef):
    return pod_ref.pod.metadata.creation_timestamp is None, pod_ref.pod.metadata.creation_timestamp


def _find_zombie_pods(pod_refs: list[_KpoPodRef], *, session: Session) -> list[k8s.V1Pod]:
    active_try_numbers = _build_active_task_instance_try_numbers(pod_refs, session=session)
    zombie_refs = [
        pod_ref
        for pod_ref in pod_refs
        if pod_ref.key not in active_try_numbers or pod_ref.try_number < active_try_numbers[pod_ref.key]
    ]
    return [pod_ref.pod for pod_ref in sorted(zombie_refs, key=_get_pod_creation_timestamp)]


def _delete_pod(
    kube_client: client.CoreV1Api,
    pod: k8s.V1Pod,
    *,
    grace_period_seconds: int,
    delete_options: dict,
    kube_client_request_args: dict,
) -> bool:
    pod_name = pod.metadata.name
    namespace = pod.metadata.namespace
    try:
        kube_client.delete_namespaced_pod(
            pod_name,
            namespace,
            body=client.V1DeleteOptions(**{**delete_options, "grace_period_seconds": grace_period_seconds}),
            **kube_client_request_args,
        )
    except ApiException as e:
        if e.status == 404:
            return False
        log.warning(
            "Failed to delete zombie KubernetesPodOperator pod %s in namespace %s: %s", pod_name, namespace, e
        )
        return False
    return True


@provide_session
def cleanup_kpo_zombie_pods(
    *,
    list_pods: Callable[[dict], list[k8s.V1Pod]],
    kube_client: client.CoreV1Api,
    max_deletes: int,
    grace_period_seconds: int,
    delete_options: dict | None = None,
    kube_client_request_args: dict | None = None,
    session: Session = NEW_SESSION,
) -> KpoZombiePodCleanupResult:
    """Delete KubernetesPodOperator pods without an active matching task instance."""
    max_deletes = max(0, max_deletes)
    query_kwargs = {"label_selector": KPO_LABEL_SELECTOR}
    pods = list_pods(query_kwargs)
    pod_refs = [pod_ref for pod in pods if (pod_ref := _extract_kpo_pod_ref(pod))]
    zombie_pods = _find_zombie_pods(pod_refs, session=session)
    deleted = 0
    for pod in zombie_pods[:max_deletes]:
        if _delete_pod(
            kube_client,
            pod,
            grace_period_seconds=grace_period_seconds,
            delete_options=delete_options or {},
            kube_client_request_args=kube_client_request_args or {},
        ):
            deleted += 1

    skipped = len(pods) - len(pod_refs) + max(0, len(zombie_pods) - max_deletes)
    result = KpoZombiePodCleanupResult(
        scanned=len(pods),
        candidates=len(pod_refs),
        zombies=len(zombie_pods),
        deleted=deleted,
        skipped=skipped,
    )
    log.info(
        "KPO zombie pod cleanup scanned=%s candidates=%s zombies=%s deleted=%s skipped=%s",
        result.scanned,
        result.candidates,
        result.zombies,
        result.deleted,
        result.skipped,
    )
    return result
