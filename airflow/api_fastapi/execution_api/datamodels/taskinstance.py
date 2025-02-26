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
from typing import Annotated, Any, Literal, Union

from pydantic import (
    AwareDatetime,
    Discriminator,
    Field,
    Tag,
    TypeAdapter,
    WithJsonSchema,
)

from airflow.api_fastapi.common.types import UtcDateTime
from airflow.api_fastapi.core_api.base import BaseModel, StrictBaseModel
from airflow.api_fastapi.execution_api.datamodels.asset import AssetProfile
from airflow.api_fastapi.execution_api.datamodels.connection import ConnectionResponse
from airflow.api_fastapi.execution_api.datamodels.variable import VariableResponse
from airflow.utils.state import IntermediateTIState, TaskInstanceState as TIState, TerminalTIState
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
    FAIL_WITHOUT_RETRY = TerminalTIState.FAIL_WITHOUT_RETRY


class TITerminalStatePayload(StrictBaseModel):
    """Schema for updating TaskInstance to a terminal state except SUCCESS state."""

    state: TerminalStateNonSuccess

    end_date: UtcDateTime
    """When the task completed executing"""


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
    outlet_events: Annotated[list[Any], Field(default_factory=list)]


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
    elif state in set(TerminalTIState):
        return "_terminal_"
    elif state == TIState.DEFERRED:
        return "deferred"
    elif state == TIState.UP_FOR_RESCHEDULE:
        return "up_for_reschedule"
    return "_other_"


# It is called "_terminal_" to avoid future conflicts if we added an actual state named "terminal"
# and "_other_" is a catch-all for all other states that are not covered by the other schemas.
TIStateUpdate = Annotated[
    Union[
        Annotated[TITerminalStatePayload, Tag("_terminal_")],
        Annotated[TISuccessStatePayload, Tag("success")],
        Annotated[TITargetStatePayload, Tag("_other_")],
        Annotated[TIDeferredStatePayload, Tag("deferred")],
        Annotated[TIRescheduleStatePayload, Tag("up_for_reschedule")],
    ],
    Discriminator(ti_state_discriminator),
]


class TIHeartbeatInfo(StrictBaseModel):
    """Schema for TaskInstance heartbeat endpoint."""

    hostname: str
    pid: int


# This model is not used in the API, but it is included in generated OpenAPI schema
# for use in the client SDKs.
class TaskInstance(StrictBaseModel):
    """Schema for TaskInstance model with minimal required fields needed for Runtime."""

    id: uuid.UUID

    task_id: str
    dag_id: str
    run_id: str
    try_number: int
    map_index: int = -1
    hostname: str | None = None


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
    conf: Annotated[dict[str, Any], Field(default_factory=dict)]


class TIRunContext(BaseModel):
    """Response schema for TaskInstance run context."""

    dag_run: DagRun
    """DAG run information for the task instance."""

    task_reschedule_count: Annotated[int, Field(default=0)]
    """How many times the task has been rescheduled."""

    max_tries: int
    """Maximum number of tries for the task instance (from DB)."""

    variables: Annotated[list[VariableResponse], Field(default_factory=list)]
    """Variables that can be accessed by the task instance."""

    connections: Annotated[list[ConnectionResponse], Field(default_factory=list)]
    """Connections that can be accessed by the task instance."""

    upstream_map_indexes: dict[str, int] | None = None

    next_method: str | None = None
    """Method to call. Set when task resumes from a trigger."""
    next_kwargs: dict[str, Any] | str | None = None
    """
    Args to pass to ``next_method``.

    Can either be a "decorated" dict, or a string encrypted with the shared Fernet key.
    """


class PrevSuccessfulDagRunResponse(BaseModel):
    """Schema for response with previous successful DagRun information for Task Template Context."""

    data_interval_start: UtcDateTime | None = None
    data_interval_end: UtcDateTime | None = None
    start_date: UtcDateTime | None = None
    end_date: UtcDateTime | None = None


class TIRuntimeCheckPayload(StrictBaseModel):
    """Payload for performing Runtime checks on the TaskInstance model as requested by the SDK."""

    inlets: list[AssetProfile] | None = None
    outlets: list[AssetProfile] | None = None
