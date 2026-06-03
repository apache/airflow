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

from typing import Any

from pydantic import JsonValue

from airflow.api_fastapi.core_api.base import BaseModel, StrictBaseModel


class TriggerLoadBody(StrictBaseModel):
    """Body for requesting triggers to be assigned to and loaded for a triggerer."""

    triggerer_id: int
    """Id of the requesting triggerer's Job row."""
    capacity: int
    """Maximum number of triggers the triggerer can run concurrently."""
    health_check_threshold: float
    """Seconds since last heartbeat after which a triggerer is considered dead."""
    queues: list[str] | None = None
    """Optional set of trigger queues this triggerer serves."""
    team_name: str | None = None
    """Optional team this triggerer serves (multi-team setups)."""


class TriggerIdsResponse(BaseModel):
    """Response listing the trigger IDs currently assigned to a triggerer."""

    trigger_ids: list[int]


class TriggerWorkloadsBody(StrictBaseModel):
    """Body for requesting runnable workloads for a set of trigger IDs."""

    trigger_ids: list[int]


class TriggerWorkloadsResponse(BaseModel):
    """Response carrying serialized ``RunTrigger`` workloads."""

    workloads: list[dict[str, Any]]
    """Each entry is a ``RunTrigger.model_dump(mode="json")``."""


class TriggerEventBody(StrictBaseModel):
    """Body for submitting a trigger event payload."""

    payload: JsonValue = None
    """
    The event payload to resume a deferred task instance with.

    This must be an ``airflow.sdk.serde.serialize(...)`` output, **not** arbitrary JSON. The
    ``/triggers/{trigger_id}/event`` route stores it **without deserializing** -- it is spliced
    straight into the task instance's ``next_kwargs``, and the worker deserializes it on resume.
    The server never reconstructs the worker-supplied object, so passing raw JSON that was never
    serde-serialized will deserialize into the wrong shape on the worker.
    """


class TriggerFailureBody(StrictBaseModel):
    """Body for submitting a trigger failure."""

    error: list[str] | None = None
    """Traceback lines (``traceback.format_exception`` output), kept as a list end-to-end."""
