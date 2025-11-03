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

import uuid
from datetime import timedelta
from enum import Enum
from typing import Annotated, Any, Literal

from pydantic import (
    AwareDatetime,
    Discriminator,
    Field,
    JsonValue,
    Tag,
    TypeAdapter,
    WithJsonSchema,
)

from airflow.api_fastapi.common.types import UtcDateTime
from airflow.api_fastapi.core_api.base import BaseModel, StrictBaseModel
from airflow.api_fastapi.execution_api.datamodels.asset import AssetProfile
from airflow.api_fastapi.execution_api.datamodels.connection import ConnectionResponse
from airflow.api_fastapi.execution_api.datamodels.variable import VariableResponse
from airflow.utils.state import (
    DagRunState,
    IntermediateTIState,
    TaskInstanceState as TIState,
    TerminalTIState,
)
from airflow.utils.types import DagRunType

AwareDatetimeAdapter = TypeAdapter(AwareDatetime)


class TIEnterRunningPayload(StrictBaseModel):
    """Schema for updating TaskInstance to 'RUNNING' state with minimal required fields."""

    state: Annotated[
        Literal[TIState.RUNNING],
        # Specify a default in the schema, but not in code.
        WithJsonSchema({"type": "string", "enum": [TIState.RUNNING], "default": TIState.RUNNING}),
    ]
    hostname: str
    """Hostname where this task has started"""
    unixname: str
    """Local username of the process where this task has started"""
    pid: int
    """Process Identifier on `hostname`"""
    start_date: UtcDateTime
    """When the task started executing"""


# Create an enum to give a nice name in the generated datamodels
class TerminalStateNonSuccess(str, Enum):
    """TaskInstance states that can be reported without extra information."""

    FAILED = TerminalTIState.FAILED
    SKIPPED = TerminalTIState.SKIPPED
    REMOVED = TerminalTIState.REMOVED
    UPSTREAM_FAILED = TerminalTIState.UPSTREAM_FAILED


class TITerminalStatePayload(StrictBaseModel):
    """Schema for updating TaskInstance to a terminal state except SUCCESS state."""

    state: TerminalStateNonSuccess

    end_date: UtcDateTime
    """When the task completed executing"""
    rendered_map_index: str | None = None


class TISuccessStatePayload(StrictBaseModel):
    """Schema for updating TaskInstance to success state."""

    state: Annotated[
        Literal[TerminalTIState.SUCCESS],
        # Specify a default in the schema, but not in code, so Pydantic marks it as required.
        WithJsonSchema(
            {
                "type": "string",
                "enum": [TerminalTIState.SUCCESS],
                "default": TerminalTIState.SUCCESS,
            }
        ),
    ]

    end_date: UtcDateTime
    """When the task completed executing"""

    task_outlets: Annotated[list[AssetProfile], Field(default_factory=list)]
    outlet_events: Annotated[list[dict[str, Any]], Field(default_factory=list)]
    rendered_map_index: str | None = None


class TITargetStatePayload(StrictBaseModel):
    """Schema for updating TaskInstance to a target state, excluding terminal and running states."""

    state: IntermediateTIState


class TIDeferredStatePayload(StrictBaseModel):
    """Schema for updating TaskInstance to a deferred state."""

    state: Annotated[
        Literal[IntermediateTIState.DEFERRED],
        # Specify a default in the schema, but not in code, so Pydantic marks it as required.
        WithJsonSchema(
            {
                "type": "string",
                "enum": [IntermediateTIState.DEFERRED],
                "default": IntermediateTIState.DEFERRED,
            }
        ),
    ]
    classpath: str
    trigger_kwargs: Annotated[dict[str, Any] | str, Field(default_factory=dict)]
    """
    Kwargs to pass to the trigger constructor, either a plain dict or an encrypted string.

    Both forms will be passed along to the trigger, the server will not handle either.
    """

    trigger_timeout: timedelta | None = None
    next_method: str
    """The name of the method on the operator to call in the worker after the trigger has fired."""
    next_kwargs: Annotated[dict[str, Any] | str, Field(default_factory=dict)]
    """
    Kwargs to pass to the above method, either a plain dict or an encrypted string.

    Both forms will be passed along to the TaskSDK upon resume, the server will not handle either.
    """
    rendered_map_index: str | None = None


class TIRescheduleStatePayload(StrictBaseModel):
    """Schema for updating TaskInstance to a up_for_reschedule state."""

    state: Annotated[
        Literal[IntermediateTIState.UP_FOR_RESCHEDULE],
        # Specify a default in the schema, but not in code, so Pydantic marks it as required.
        WithJsonSchema(
            {
                "type": "string",
                "enum": [IntermediateTIState.UP_FOR_RESCHEDULE],
                "default": IntermediateTIState.UP_FOR_RESCHEDULE,
            }
        ),
    ]
    reschedule_date: UtcDateTime
    end_date: UtcDateTime


class TIRetryStatePayload(StrictBaseModel):
    """Schema for updating TaskInstance to up_for_retry."""

    state: Annotated[
        Literal[IntermediateTIState.UP_FOR_RETRY],
        # Specify a default in the schema, but not in code, so Pydantic marks it as required.
        WithJsonSchema(
            {
                "type": "string",
                "enum": [IntermediateTIState.UP_FOR_RETRY],
                "default": IntermediateTIState.UP_FOR_RETRY,
            }
        ),
    ]
    end_date: UtcDateTime
    rendered_map_index: str | None = None


class TISkippedDownstreamTasksStatePayload(StrictBaseModel):
    """Schema for updating downstream tasks to a skipped state."""

    tasks: list[str | tuple[str, int]]


def ti_state_discriminator(v: dict[str, str] | StrictBaseModel) -> str:
    """
    Determine the discriminator key for TaskInstance state transitions.

    This function serves as a discriminator for the TIStateUpdate union schema,
    categorizing the payload based on the ``state`` attribute in the input data.
    It returns a key that directs FastAPI to the appropriate subclass (schema)
    based on the requested state.
    """
    if isinstance(v, dict):
        state = v.get("state")
    else:
        state = getattr(v, "state", None)

    if state == TIState.SUCCESS:
        return "success"
    if state in set(TerminalTIState):
        return "_terminal_"
    if state == TIState.DEFERRED:
        return "deferred"
    if state == TIState.UP_FOR_RESCHEDULE:
        return "up_for_reschedule"
    if state == TIState.UP_FOR_RETRY:
        return "up_for_retry"
    return "_other_"


# It is called "_terminal_" to avoid future conflicts if we added an actual state named "terminal"
# and "_other_" is a catch-all for all other states that are not covered by the other schemas.
TIStateUpdate = Annotated[
    Annotated[TITerminalStatePayload, Tag("_terminal_")]
    | Annotated[TISuccessStatePayload, Tag("success")]
    | Annotated[TITargetStatePayload, Tag("_other_")]
    | Annotated[TIDeferredStatePayload, Tag("deferred")]
    | Annotated[TIRescheduleStatePayload, Tag("up_for_reschedule")]
    | Annotated[TIRetryStatePayload, Tag("up_for_retry")],
    Discriminator(ti_state_discriminator),
]


class TIHeartbeatInfo(StrictBaseModel):
    """Schema for TaskInstance heartbeat endpoint."""

    hostname: str
    pid: int


# This model is not used in the API, but it is included in generated OpenAPI schema
# for use in the client SDKs.
class TaskInstance(BaseModel):
    """Schema for TaskInstance model with minimal required fields needed for Runtime."""

    id: uuid.UUID

    task_id: str
    dag_id: str
    run_id: str
    try_number: int
    dag_version_id: uuid.UUID
    map_index: int = -1
    hostname: str | None = None
    context_carrier: dict | None = None


class AssetReferenceAssetEventDagRun(StrictBaseModel):
    """Schema for AssetModel used in AssetEventDagRunReference."""

    name: str
    uri: str
    extra: dict[str, JsonValue]


class AssetAliasReferenceAssetEventDagRun(StrictBaseModel):
    """Schema for AssetAliasModel used in AssetEventDagRunReference."""

    name: str


class AssetEventDagRunReference(StrictBaseModel):
    """Schema for AssetEvent model used in DagRun."""

    asset: AssetReferenceAssetEventDagRun
    extra: dict[str, JsonValue]
    source_task_id: str | None
    source_dag_id: str | None
    source_run_id: str | None
    source_map_index: int | None
    source_aliases: list[AssetAliasReferenceAssetEventDagRun]
    timestamp: UtcDateTime


class DagRun(StrictBaseModel):
    """Schema for DagRun model with minimal required fields needed for Runtime."""

    # TODO: `dag_id` and `run_id` are duplicated from TaskInstance
    #   See if we can avoid sending these fields from API server and instead
    #   use the TaskInstance data to get the DAG run information in the client (Task Execution Interface).
    dag_id: str
    run_id: str

    logical_date: UtcDateTime | None
    data_interval_start: UtcDateTime | None
    data_interval_end: UtcDateTime | None
    run_after: UtcDateTime
    start_date: UtcDateTime
    end_date: UtcDateTime | None
    clear_number: int = 0
    run_type: DagRunType
    state: DagRunState
    conf: dict[str, Any] | None = None
    triggering_user_name: str | None = None
    consumed_asset_events: list[AssetEventDagRunReference]


class TIRunContext(BaseModel):
    """Response schema for TaskInstance run context."""

    dag_run: DagRun
    """DAG run information for the task instance."""

    task_reschedule_count: int = 0
    """How many times the task has been rescheduled."""

    max_tries: int
    """Maximum number of tries for the task instance (from DB)."""

    variables: Annotated[list[VariableResponse], Field(default_factory=list)]
    """Variables that can be accessed by the task instance."""

    connections: Annotated[list[ConnectionResponse], Field(default_factory=list)]
    """Connections that can be accessed by the task instance."""

    upstream_map_indexes: dict[str, int | list[int] | None] | None = None

    next_method: str | None = None
    """Method to call. Set when task resumes from a trigger."""
    next_kwargs: dict[str, Any] | str | None = None
    """
    Args to pass to ``next_method``.

    Can either be a "decorated" dict, or a string encrypted with the shared Fernet key.
    """

    xcom_keys_to_clear: Annotated[list[str], Field(default_factory=list)]
    """List of Xcom keys that need to be cleared and purged on by the worker."""

    should_retry: bool = False
    """If the ti encounters an error, whether it should enter retry or failed state."""


class PrevSuccessfulDagRunResponse(BaseModel):
    """Schema for response with previous successful DagRun information for Task Template Context."""

    data_interval_start: UtcDateTime | None = None
    data_interval_end: UtcDateTime | None = None
    start_date: UtcDateTime | None = None
    end_date: UtcDateTime | None = None


class TaskStatesResponse(BaseModel):
    """Response for task states with run_id, task and state."""

    task_states: dict[str, Any]


class InactiveAssetsResponse(BaseModel):
    """Response for inactive assets."""

    inactive_assets: Annotated[list[AssetProfile], Field(default_factory=list)]
