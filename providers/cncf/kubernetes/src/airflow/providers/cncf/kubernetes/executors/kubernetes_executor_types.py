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

from typing import TYPE_CHECKING, Any, Literal, NamedTuple, TypedDict

from airflow.providers.cncf.kubernetes.version_compat import AIRFLOW_V_3_3_PLUS  # noqa: TC001

if TYPE_CHECKING:
    from collections.abc import Sequence
    from typing import TypeAlias

    from airflow.models.taskinstance import TaskInstanceKey
    from airflow.utils.state import TaskInstanceState

    # On Airflow 3.3+ a workload key/state may also be a callback (or connection
    # test) key/state, not just a task one.  Widen the executor NamedTuple fields
    # accordingly while falling back to the task-only types on older Airflow.
    if AIRFLOW_V_3_3_PLUS:
        from airflow.executors.workloads import ExecuteCallback, ExecuteTask
        from airflow.executors.workloads.types import (
            WorkloadKey as _WorkloadKey,
            WorkloadState as _WorkloadState,
        )

        WorkloadKey: TypeAlias = _WorkloadKey
        WorkloadState: TypeAlias = _WorkloadState
        # KubernetesJob.command carries either the legacy Airflow-2 argv
        # (Sequence[str]) or, on Airflow 3, a single-element list wrapping the
        # workload object (ExecuteTask or ExecuteCallback).
        WorkloadCommand: TypeAlias = Sequence[str] | Sequence[ExecuteTask] | Sequence[ExecuteCallback]
    else:
        WorkloadKey: TypeAlias = TaskInstanceKey  # type: ignore[no-redef, misc]
        WorkloadState: TypeAlias = TaskInstanceState  # type: ignore[no-redef, misc]
        WorkloadCommand: TypeAlias = Sequence[str]  # type: ignore[no-redef, misc]


ADOPTED = "adopted"


class FailureDetails(TypedDict, total=False):
    """Detailed information about pod/container failure."""

    pod_status: str | None
    pod_reason: str | None
    pod_message: str | None
    container_state: str | None
    container_reason: str | None
    container_message: str | None
    exit_code: int | None
    container_type: Literal["init", "main"] | None
    container_name: str | None


class KubernetesResults(NamedTuple):
    """Results from Kubernetes task execution."""

    key: WorkloadKey
    state: WorkloadState | str | None
    pod_name: str
    namespace: str
    resource_version: str
    failure_details: FailureDetails | None


class KubernetesWatch(NamedTuple):
    """Watch event data from Kubernetes pods."""

    pod_name: str
    namespace: str
    state: WorkloadState | str | None
    annotations: dict[str, str]
    resource_version: str
    failure_details: FailureDetails | None


# TODO: Remove after Airflow 2 support is removed
CommandType = "Sequence[str]"


class KubernetesJob(NamedTuple):
    """Job definition for Kubernetes execution."""

    key: WorkloadKey
    command: WorkloadCommand
    kube_executor_config: Any
    pod_template_file: str | None


ALL_NAMESPACES = "ALL_NAMESPACES"
POD_EXECUTOR_DONE_KEY = "airflow_executor_done"

POD_REVOKED_KEY = "airflow_pod_revoked"
"""Label to indicate pod revoked by executor.

When executor the executor revokes a task, the pod deletion is the result of
the revocation.  So we don't want it to process that as an external deletion.
So we want events on a revoked pod to be ignored.

:meta private:
"""

CALLBACK_WORKLOAD_TYPE_KEY = "airflow-workload-type"
"""Label key used to mark callback pods.

Callback pods carry ``CALLBACK_WORKLOAD_TYPE_KEY=callback`` so the watcher
can distinguish them from task pods and route annotations correctly.
"""

CALLBACK_POD_ANNOTATION_KEY = "callback_id"
"""Annotation key that stores the callback UUID on callback pods.

The watcher reads this annotation to reconstruct a ``CallbackKey`` instead of
a ``TaskInstanceKey``.
"""
