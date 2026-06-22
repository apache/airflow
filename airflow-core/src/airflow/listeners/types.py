#
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

from typing import TYPE_CHECKING, Any

import attrs

if TYPE_CHECKING:
    from pydantic import JsonValue

    from airflow.serialization.definitions.assets import SerializedAsset, SerializedAssetAlias


@attrs.define
class AssetEvent:
    """Asset event representation for asset listener hooks."""

    asset: SerializedAsset
    extra: dict[str, JsonValue]
    source_dag_id: str | None
    source_task_id: str | None
    source_run_id: str | None
    source_map_index: int | None
    source_aliases: list[SerializedAssetAlias]
    partition_key: str | None


@attrs.define(frozen=True)
class FailureDetails:
    """
    Structured infrastructure-side failure context for ``on_task_instance_failed``.

    Foundation for AIP-97 (Infrastructure-Aware Task Execution). Today the
    listener only sees the worker-side ``error`` exception; failure causes
    that originate outside the worker process — pod ``OOMKilled`` /
    ``PodEvicted`` on Kubernetes, ``WorkerLost`` / ``SoftTimeLimit`` on
    Celery, oom-killer ``SIGKILL`` on the local executor, preemption /
    eviction on resource-managed clusters — are visible only to the
    executor and never reach the listener.

    This type is the executor-agnostic shape every executor populates when
    surfacing infrastructure failure causes. The ``executor_kind`` is the
    canonical executor name (``"kubernetes"``, ``"celery"``,
    ``"local_executor"``, etc.); ``infra_reason`` is the executor's
    short categorical token; ``infra_metadata`` carries any structured
    payload the executor wants to ship (pod status conditions, exit
    codes, evicted-by reasons, queue names, etc.).

    Wiring per executor is out of scope for the foundation: this type
    only defines the shape so each executor's surfacing PR can iterate
    against a fixed contract.
    """

    executor_kind: str
    infra_reason: str | None = None
    infra_metadata: dict[str, Any] = attrs.field(factory=dict)
