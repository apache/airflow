# generated by datamodel-codegen:
#   filename:  http://0.0.0.0:8080/execution/openapi.json
#   version:   0.28.2

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

from datetime import timedelta
from enum import Enum
from typing import Annotated, Any, Final, Literal
from uuid import UUID

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, JsonValue

API_VERSION: Final[str] = "2025-03-26"


class AssetProfile(BaseModel):
    """
    Profile of an asset-like object.

    Asset will have name, uri defined, with type set to 'Asset'.
    AssetNameRef will have name defined, type set to 'AssetNameRef'.
    AssetUriRef will have uri defined, type set to 'AssetUriRef'.
    AssetAlias will have name defined, type set to 'AssetAlias'.

    Note that 'type' here is distinct from 'asset_type' the user declares on an
    Asset (or subclass). This field is for distinguishing between different
    asset-related types (Asset, AssetRef, or AssetAlias).
    """

    model_config = ConfigDict(
        extra="forbid",
    )
    name: Annotated[str | None, Field(title="Name")] = None
    uri: Annotated[str | None, Field(title="Uri")] = None
    type: Annotated[str, Field(title="Type")]


class AssetResponse(BaseModel):
    """
    Asset schema for responses with fields that are needed for Runtime.
    """

    name: Annotated[str, Field(title="Name")]
    uri: Annotated[str, Field(title="Uri")]
    group: Annotated[str, Field(title="Group")]
    extra: Annotated[dict[str, Any] | None, Field(title="Extra")] = None


class ConnectionResponse(BaseModel):
    """
    Connection schema for responses with fields that are needed for Runtime.
    """

    conn_id: Annotated[str, Field(title="Conn Id")]
    conn_type: Annotated[str, Field(title="Conn Type")]
    host: Annotated[str | None, Field(title="Host")] = None
    schema_: Annotated[str | None, Field(alias="schema", title="Schema")] = None
    login: Annotated[str | None, Field(title="Login")] = None
    password: Annotated[str | None, Field(title="Password")] = None
    port: Annotated[int | None, Field(title="Port")] = None
    extra: Annotated[str | None, Field(title="Extra")] = None


class DagRunAssetReference(BaseModel):
    """
    DagRun serializer for asset responses.
    """

    model_config = ConfigDict(
        extra="forbid",
    )
    run_id: Annotated[str, Field(title="Run Id")]
    dag_id: Annotated[str, Field(title="Dag Id")]
    logical_date: Annotated[AwareDatetime | None, Field(title="Logical Date")] = None
    start_date: Annotated[AwareDatetime, Field(title="Start Date")]
    end_date: Annotated[AwareDatetime | None, Field(title="End Date")] = None
    state: Annotated[str, Field(title="State")]
    data_interval_start: Annotated[AwareDatetime | None, Field(title="Data Interval Start")] = None
    data_interval_end: Annotated[AwareDatetime | None, Field(title="Data Interval End")] = None


class DagRunState(str, Enum):
    """
    All possible states that a DagRun can be in.

    These are "shared" with TaskInstanceState in some parts of the code,
    so please ensure that their values always match the ones with the
    same name in TaskInstanceState.
    """

    QUEUED = "queued"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"


class DagRunStateResponse(BaseModel):
    """
    Schema for DAG Run State response.
    """

    state: DagRunState


class DagRunType(str, Enum):
    """
    Class with DagRun types.
    """

    BACKFILL = "backfill"
    SCHEDULED = "scheduled"
    MANUAL = "manual"
    ASSET_TRIGGERED = "asset_triggered"


class IntermediateTIState(str, Enum):
    """
    States that a Task Instance can be in that indicate it is not yet in a terminal or running state.
    """

    SCHEDULED = "scheduled"
    QUEUED = "queued"
    RESTARTING = "restarting"
    UP_FOR_RETRY = "up_for_retry"
    UP_FOR_RESCHEDULE = "up_for_reschedule"
    UPSTREAM_FAILED = "upstream_failed"
    DEFERRED = "deferred"


class PrevSuccessfulDagRunResponse(BaseModel):
    """
    Schema for response with previous successful DagRun information for Task Template Context.
    """

    data_interval_start: Annotated[AwareDatetime | None, Field(title="Data Interval Start")] = None
    data_interval_end: Annotated[AwareDatetime | None, Field(title="Data Interval End")] = None
    start_date: Annotated[AwareDatetime | None, Field(title="Start Date")] = None
    end_date: Annotated[AwareDatetime | None, Field(title="End Date")] = None


class TIDeferredStatePayload(BaseModel):
    """
    Schema for updating TaskInstance to a deferred state.
    """

    model_config = ConfigDict(
        extra="forbid",
    )
    state: Annotated[Literal["deferred"] | None, Field(title="State")] = "deferred"
    classpath: Annotated[str, Field(title="Classpath")]
    trigger_kwargs: Annotated[dict[str, Any] | str | None, Field(title="Trigger Kwargs")] = None
    trigger_timeout: Annotated[timedelta | None, Field(title="Trigger Timeout")] = None
    next_method: Annotated[str, Field(title="Next Method")]
    next_kwargs: Annotated[dict[str, Any] | str | None, Field(title="Next Kwargs")] = None


class TIEnterRunningPayload(BaseModel):
    """
    Schema for updating TaskInstance to 'RUNNING' state with minimal required fields.
    """

    model_config = ConfigDict(
        extra="forbid",
    )
    state: Annotated[Literal["running"] | None, Field(title="State")] = "running"
    hostname: Annotated[str, Field(title="Hostname")]
    unixname: Annotated[str, Field(title="Unixname")]
    pid: Annotated[int, Field(title="Pid")]
    start_date: Annotated[AwareDatetime, Field(title="Start Date")]


class TIHeartbeatInfo(BaseModel):
    """
    Schema for TaskInstance heartbeat endpoint.
    """

    model_config = ConfigDict(
        extra="forbid",
    )
    hostname: Annotated[str, Field(title="Hostname")]
    pid: Annotated[int, Field(title="Pid")]


class TIRescheduleStatePayload(BaseModel):
    """
    Schema for updating TaskInstance to a up_for_reschedule state.
    """

    model_config = ConfigDict(
        extra="forbid",
    )
    state: Annotated[Literal["up_for_reschedule"] | None, Field(title="State")] = "up_for_reschedule"
    reschedule_date: Annotated[AwareDatetime, Field(title="Reschedule Date")]
    end_date: Annotated[AwareDatetime, Field(title="End Date")]


class TIRetryStatePayload(BaseModel):
    """
    Schema for updating TaskInstance to up_for_retry.
    """

    model_config = ConfigDict(
        extra="forbid",
    )
    state: Annotated[Literal["up_for_retry"] | None, Field(title="State")] = "up_for_retry"
    end_date: Annotated[AwareDatetime, Field(title="End Date")]


class TISkippedDownstreamTasksStatePayload(BaseModel):
    """
    Schema for updating downstream tasks to a skipped state.
    """

    model_config = ConfigDict(
        extra="forbid",
    )
    tasks: Annotated[list[str | tuple[str, int]], Field(title="Tasks")]


class TISuccessStatePayload(BaseModel):
    """
    Schema for updating TaskInstance to success state.
    """

    model_config = ConfigDict(
        extra="forbid",
    )
    state: Annotated[Literal["success"] | None, Field(title="State")] = "success"
    end_date: Annotated[AwareDatetime, Field(title="End Date")]
    task_outlets: Annotated[list[AssetProfile] | None, Field(title="Task Outlets")] = None
    outlet_events: Annotated[list[dict[str, Any]] | None, Field(title="Outlet Events")] = None


class TITargetStatePayload(BaseModel):
    """
    Schema for updating TaskInstance to a target state, excluding terminal and running states.
    """

    model_config = ConfigDict(
        extra="forbid",
    )
    state: IntermediateTIState


class TerminalStateNonSuccess(str, Enum):
    """
    TaskInstance states that can be reported without extra information.
    """

    FAILED = "failed"
    SKIPPED = "skipped"
    REMOVED = "removed"


class TriggerDAGRunPayload(BaseModel):
    """
    Schema for Trigger DAG Run API request.
    """

    model_config = ConfigDict(
        extra="forbid",
    )
    logical_date: Annotated[AwareDatetime | None, Field(title="Logical Date")] = None
    conf: Annotated[dict[str, Any] | None, Field(title="Conf")] = None
    reset_dag_run: Annotated[bool | None, Field(title="Reset Dag Run")] = False


class ValidationError(BaseModel):
    loc: Annotated[list[str | int], Field(title="Location")]
    msg: Annotated[str, Field(title="Message")]
    type: Annotated[str, Field(title="Error Type")]


class VariablePostBody(BaseModel):
    """
    Request body schema for creating variables.
    """

    model_config = ConfigDict(
        extra="forbid",
    )
    val: Annotated[str | None, Field(title="Val")] = None
    description: Annotated[str | None, Field(title="Description")] = None


class VariableResponse(BaseModel):
    """
    Variable schema for responses with fields that are needed for Runtime.
    """

    model_config = ConfigDict(
        extra="forbid",
    )
    key: Annotated[str, Field(title="Key")]
    value: Annotated[str | None, Field(title="Value")] = None


class XComResponse(BaseModel):
    """
    XCom schema for responses with fields that are needed for Runtime.
    """

    key: Annotated[str, Field(title="Key")]
    value: JsonValue


class TaskInstance(BaseModel):
    """
    Schema for TaskInstance model with minimal required fields needed for Runtime.
    """

    model_config = ConfigDict(
        extra="forbid",
    )
    id: Annotated[UUID, Field(title="Id")]
    task_id: Annotated[str, Field(title="Task Id")]
    dag_id: Annotated[str, Field(title="Dag Id")]
    run_id: Annotated[str, Field(title="Run Id")]
    try_number: Annotated[int, Field(title="Try Number")]
    map_index: Annotated[int | None, Field(title="Map Index")] = -1
    hostname: Annotated[str | None, Field(title="Hostname")] = None
    context_carrier: Annotated[dict | None, Field(title="Context Carrier")] = None


class BundleInfo(BaseModel):
    """
    Schema for telling task which bundle to run with.
    """

    name: Annotated[str, Field(title="Name")]
    version: Annotated[str | None, Field(title="Version")] = None


class TerminalTIState(str, Enum):
    SUCCESS = "success"
    FAILED = "failed"
    SKIPPED = "skipped"
    REMOVED = "removed"


class AssetEventResponse(BaseModel):
    """
    Asset event schema with fields that are needed for Runtime.
    """

    id: Annotated[int, Field(title="Id")]
    timestamp: Annotated[AwareDatetime, Field(title="Timestamp")]
    extra: Annotated[dict[str, Any] | None, Field(title="Extra")] = None
    asset: AssetResponse
    created_dagruns: Annotated[list[DagRunAssetReference], Field(title="Created Dagruns")]
    source_task_id: Annotated[str | None, Field(title="Source Task Id")] = None
    source_dag_id: Annotated[str | None, Field(title="Source Dag Id")] = None
    source_run_id: Annotated[str | None, Field(title="Source Run Id")] = None
    source_map_index: Annotated[int | None, Field(title="Source Map Index")] = -1


class AssetEventsResponse(BaseModel):
    """
    Collection of AssetEventResponse.
    """

    asset_events: Annotated[list[AssetEventResponse], Field(title="Asset Events")]


class DagRun(BaseModel):
    """
    Schema for DagRun model with minimal required fields needed for Runtime.
    """

    model_config = ConfigDict(
        extra="forbid",
    )
    dag_id: Annotated[str, Field(title="Dag Id")]
    run_id: Annotated[str, Field(title="Run Id")]
    logical_date: Annotated[AwareDatetime | None, Field(title="Logical Date")] = None
    data_interval_start: Annotated[AwareDatetime | None, Field(title="Data Interval Start")] = None
    data_interval_end: Annotated[AwareDatetime | None, Field(title="Data Interval End")] = None
    run_after: Annotated[AwareDatetime, Field(title="Run After")]
    start_date: Annotated[AwareDatetime, Field(title="Start Date")]
    end_date: Annotated[AwareDatetime | None, Field(title="End Date")] = None
    clear_number: Annotated[int | None, Field(title="Clear Number")] = 0
    run_type: DagRunType
    conf: Annotated[dict[str, Any] | None, Field(title="Conf")] = None


class HTTPValidationError(BaseModel):
    detail: Annotated[list[ValidationError] | None, Field(title="Detail")] = None


class TIRunContext(BaseModel):
    """
    Response schema for TaskInstance run context.
    """

    dag_run: DagRun
    task_reschedule_count: Annotated[int | None, Field(title="Task Reschedule Count")] = 0
    max_tries: Annotated[int, Field(title="Max Tries")]
    variables: Annotated[list[VariableResponse] | None, Field(title="Variables")] = None
    connections: Annotated[list[ConnectionResponse] | None, Field(title="Connections")] = None
    upstream_map_indexes: Annotated[dict[str, int] | None, Field(title="Upstream Map Indexes")] = None
    next_method: Annotated[str | None, Field(title="Next Method")] = None
    next_kwargs: Annotated[dict[str, Any] | str | None, Field(title="Next Kwargs")] = None
    xcom_keys_to_clear: Annotated[list[str] | None, Field(title="Xcom Keys To Clear")] = None
    should_retry: Annotated[bool, Field(title="Should Retry")]


class TITerminalStatePayload(BaseModel):
    """
    Schema for updating TaskInstance to a terminal state except SUCCESS state.
    """

    model_config = ConfigDict(
        extra="forbid",
    )
    state: TerminalStateNonSuccess
    end_date: Annotated[AwareDatetime, Field(title="End Date")]
