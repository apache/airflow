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
r"""
Communication protocol between the Supervisor and the task process
==================================================================

* All communication is done over stdout/stdin in the form of "JSON lines" (each
  message is a single JSON document terminated by `\n` character)
* Messages from the subprocess are all log messages and are sent directly to the log
* No messages are sent to task process except in response to a request. (This is because the task process will
  be running user's code, so we can't read from stdin until we enter our code, such as when requesting an XCom
  value etc.)

The reason this communication protocol exists, rather than the task process speaking directly to the Task
Execution API server is because:

1. To reduce the number of concurrent HTTP connections on the API server.

   The supervisor already has to speak to that to heartbeat the running Task, so having the task speak to its
   parent process and having all API traffic go through that means that the number of HTTP connections is
   "halved". (Not every task will make API calls, so it's not always halved, but it is reduced.)

2. This means that the user Task code doesn't ever directly see the task identity JWT token.

   This is a short lived token tied to one specific task instance try, so it being leaked/exfiltrated is not a
   large risk, but it's easy to not give it to the user code, so lets do that.
"""  # noqa: D400, D205

from __future__ import annotations

from datetime import datetime
from typing import Annotated, Any, Literal, Union
from uuid import UUID

from fastapi import Body
from pydantic import BaseModel, ConfigDict, Field, JsonValue, field_serializer

from airflow.sdk.api.datamodels._generated import (
    AssetEventsResponse,
    AssetResponse,
    BundleInfo,
    ConnectionResponse,
    DagRunStateResponse,
    PrevSuccessfulDagRunResponse,
    TaskInstance,
    TerminalTIState,
    TIDeferredStatePayload,
    TIRescheduleStatePayload,
    TIRetryStatePayload,
    TIRunContext,
    TIRuntimeCheckPayload,
    TISkippedDownstreamTasksStatePayload,
    TISuccessStatePayload,
    TriggerDAGRunPayload,
    VariableResponse,
    XComResponse,
)
from airflow.sdk.exceptions import ErrorType


class StartupDetails(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    ti: TaskInstance
    dag_rel_path: str
    bundle_info: BundleInfo
    requests_fd: int
    start_date: datetime
    """
    The channel for the task to send requests over.

    Responses will come back on stdin
    """
    ti_context: TIRunContext
    type: Literal["StartupDetails"] = "StartupDetails"


class AssetResult(AssetResponse):
    """Response to ReadXCom request."""

    type: Literal["AssetResult"] = "AssetResult"

    @classmethod
    def from_asset_response(cls, asset_response: AssetResponse) -> AssetResult:
        """
        Get AssetResult from AssetResponse.

        AssetResponse is autogenerated from the API schema, so we need to convert it to AssetResult
        for communication between the Supervisor and the task process.
        """
        # Exclude defaults to avoid sending unnecessary data
        # Pass the type as AssetResult explicitly so we can then call model_dump_json with exclude_unset=True
        # to avoid sending unset fields (which are defaults in our case).
        return cls(**asset_response.model_dump(exclude_defaults=True), type="AssetResult")


class AssetEventsResult(AssetEventsResponse):
    """Response to GetAssetEvent request."""

    type: Literal["AssetEventsResult"] = "AssetEventsResult"

    @classmethod
    def from_asset_events_response(cls, asset_events_response: AssetEventsResponse) -> AssetEventsResult:
        """
        Get AssetEventsResult from AssetEventsResponse.

        AssetEventsResponse is autogenerated from the API schema, so we need to convert it to AssetEventsResponse
        for communication between the Supervisor and the task process.
        """
        # Exclude defaults to avoid sending unnecessary data
        # Pass the type as AssetEventsResult explicitly so we can then call model_dump_json with exclude_unset=True
        # to avoid sending unset fields (which are defaults in our case).
        return cls(
            **asset_events_response.model_dump(exclude_defaults=True),
            type="AssetEventsResult",
        )


class XComResult(XComResponse):
    """Response to ReadXCom request."""

    type: Literal["XComResult"] = "XComResult"

    @classmethod
    def from_xcom_response(cls, xcom_response: XComResponse) -> XComResult:
        """
        Get XComResult from XComResponse.

        XComResponse is autogenerated from the API schema, so we need to convert it to XComResult
        for communication between the Supervisor and the task process.
        """
        return cls(**xcom_response.model_dump())


class XComCountResponse(BaseModel):
    len: int
    type: Literal["XComLengthResponse"] = "XComLengthResponse"


class ConnectionResult(ConnectionResponse):
    type: Literal["ConnectionResult"] = "ConnectionResult"

    @classmethod
    def from_conn_response(cls, connection_response: ConnectionResponse) -> ConnectionResult:
        """
        Get ConnectionResult from ConnectionResponse.

        ConnectionResponse is autogenerated from the API schema, so we need to convert it to ConnectionResult
        for communication between the Supervisor and the task process.
        """
        # Exclude defaults to avoid sending unnecessary data
        # Pass the type as ConnectionResult explicitly so we can then call model_dump_json with exclude_unset=True
        # to avoid sending unset fields (which are defaults in our case).
        return cls(
            **connection_response.model_dump(exclude_defaults=True, by_alias=True), type="ConnectionResult"
        )


class VariableResult(VariableResponse):
    type: Literal["VariableResult"] = "VariableResult"

    @classmethod
    def from_variable_response(cls, variable_response: VariableResponse) -> VariableResult:
        """
        Get VariableResult from VariableResponse.

        VariableResponse is autogenerated from the API schema, so we need to convert it to VariableResult
        for communication between the Supervisor and the task process.
        """
        return cls(**variable_response.model_dump(exclude_defaults=True), type="VariableResult")


class DagRunStateResult(DagRunStateResponse):
    type: Literal["DagRunStateResult"] = "DagRunStateResult"

    # TODO: Create a convert api_response to result classes so we don't need to do this
    #   for all the classes above
    @classmethod
    def from_api_response(cls, dr_state_response: DagRunStateResponse) -> DagRunStateResult:
        """
        Create result class from API Response.

        API Response is autogenerated from the API schema, so we need to convert it to Result
        for communication between the Supervisor and the task process since it needs a
        discriminator field.
        """
        return cls(**dr_state_response.model_dump(exclude_defaults=True), type="DagRunStateResult")


class PrevSuccessfulDagRunResult(PrevSuccessfulDagRunResponse):
    type: Literal["PrevSuccessfulDagRunResult"] = "PrevSuccessfulDagRunResult"

    @classmethod
    def from_dagrun_response(cls, prev_dag_run: PrevSuccessfulDagRunResponse) -> PrevSuccessfulDagRunResult:
        """
        Get a result object from response object.

        PrevSuccessfulDagRunResponse is autogenerated from the API schema, so we need to convert it to
        PrevSuccessfulDagRunResult for communication between the Supervisor and the task process.
        """
        return cls(**prev_dag_run.model_dump(exclude_defaults=True), type="PrevSuccessfulDagRunResult")


class ErrorResponse(BaseModel):
    error: ErrorType = ErrorType.GENERIC_ERROR
    detail: dict | None = None
    type: Literal["ErrorResponse"] = "ErrorResponse"


class OKResponse(BaseModel):
    ok: bool
    type: Literal["OKResponse"] = "OKResponse"


ToTask = Annotated[
    Union[
        AssetResult,
        AssetEventsResult,
        ConnectionResult,
        DagRunStateResult,
        ErrorResponse,
        PrevSuccessfulDagRunResult,
        StartupDetails,
        VariableResult,
        XComResult,
        XComCountResponse,
        OKResponse,
    ],
    Field(discriminator="type"),
]


class TaskState(BaseModel):
    """
    Update a task's state.

    If a process exits without sending one of these the state will be derived from the exit code:
    - 0 = SUCCESS
    - anything else = FAILED
    """

    state: Literal[
        TerminalTIState.FAILED,
        TerminalTIState.SKIPPED,
        TerminalTIState.REMOVED,
    ]
    end_date: datetime | None = None
    type: Literal["TaskState"] = "TaskState"


class SucceedTask(TISuccessStatePayload):
    """Update a task's state to success. Includes task_outlets and outlet_events for registering asset events."""

    type: Literal["SucceedTask"] = "SucceedTask"


class DeferTask(TIDeferredStatePayload):
    """Update a task instance state to deferred."""

    type: Literal["DeferTask"] = "DeferTask"

    @field_serializer("trigger_kwargs", "next_kwargs", check_fields=True)
    def _serde_kwarg_fields(self, val: str | dict[str, Any] | None, _info):
        from airflow.serialization.serialized_objects import BaseSerialization

        if not isinstance(val, dict):
            # None, or an encrypted string
            return val

        if val.keys() == {"__type", "__var"}:
            # Already encoded.
            return val
        return BaseSerialization.serialize(val or {})


class RetryTask(TIRetryStatePayload):
    """Update a task instance state to up_for_retry."""

    type: Literal["RetryTask"] = "RetryTask"


class RescheduleTask(TIRescheduleStatePayload):
    """Update a task instance state to reschedule/up_for_reschedule."""

    type: Literal["RescheduleTask"] = "RescheduleTask"


class SkipDownstreamTasks(TISkippedDownstreamTasksStatePayload):
    """Update state of downstream tasks within a task instance to 'skipped', while updating current task to success state."""

    type: Literal["SkipDownstreamTasks"] = "SkipDownstreamTasks"


class RuntimeCheckOnTask(TIRuntimeCheckPayload):
    type: Literal["RuntimeCheckOnTask"] = "RuntimeCheckOnTask"


class GetXCom(BaseModel):
    key: str
    dag_id: str
    run_id: str
    task_id: str
    map_index: int | None = None
    type: Literal["GetXCom"] = "GetXCom"


class GetXComCount(BaseModel):
    """Get the number of (mapped) XCom values available."""

    key: str
    dag_id: str
    run_id: str
    task_id: str
    type: Literal["GetNumberXComs"] = "GetNumberXComs"


class SetXCom(BaseModel):
    key: str
    value: Annotated[
        # JsonValue can handle non JSON stringified dicts, lists and strings, which is better
        # for the task intuitibe to send to the supervisor
        JsonValue,
        Body(
            description="A JSON-formatted string representing the value to set for the XCom.",
            openapi_examples={
                "simple_value": {
                    "summary": "Simple value",
                    "value": "value1",
                },
                "dict_value": {
                    "summary": "Dictionary value",
                    "value": {"key2": "value2"},
                },
                "list_value": {
                    "summary": "List value",
                    "value": ["value1"],
                },
            },
        ),
    ]
    dag_id: str
    run_id: str
    task_id: str
    map_index: int | None = None
    mapped_length: int | None = None
    type: Literal["SetXCom"] = "SetXCom"


class DeleteXCom(BaseModel):
    key: str
    dag_id: str
    run_id: str
    task_id: str
    map_index: int | None = None
    type: Literal["DeleteXCom"] = "DeleteXCom"


class GetConnection(BaseModel):
    conn_id: str
    type: Literal["GetConnection"] = "GetConnection"


class GetVariable(BaseModel):
    key: str
    type: Literal["GetVariable"] = "GetVariable"


class PutVariable(BaseModel):
    key: str
    value: str | None
    description: str | None
    type: Literal["PutVariable"] = "PutVariable"


class SetRenderedFields(BaseModel):
    """Payload for setting RTIF for a task instance."""

    # We are using a BaseModel here compared to server using RootModel because we
    # have a discriminator running with "type", and RootModel doesn't support type

    rendered_fields: dict[str, JsonValue]
    type: Literal["SetRenderedFields"] = "SetRenderedFields"


class TriggerDagRun(TriggerDAGRunPayload):
    dag_id: str
    run_id: Annotated[str, Field(title="Dag Run Id")]
    type: Literal["TriggerDagRun"] = "TriggerDagRun"


class GetDagRunState(BaseModel):
    dag_id: str
    run_id: str
    type: Literal["GetDagRunState"] = "GetDagRunState"


class GetAssetByName(BaseModel):
    name: str
    type: Literal["GetAssetByName"] = "GetAssetByName"


class GetAssetByUri(BaseModel):
    uri: str
    type: Literal["GetAssetByUri"] = "GetAssetByUri"


class GetAssetEventByAsset(BaseModel):
    name: str | None
    uri: str | None
    type: Literal["GetAssetEventByAsset"] = "GetAssetEventByAsset"


class GetAssetEventByAssetAlias(BaseModel):
    alias_name: str
    type: Literal["GetAssetEventByAssetAlias"] = "GetAssetEventByAssetAlias"


class GetPrevSuccessfulDagRun(BaseModel):
    ti_id: UUID
    type: Literal["GetPrevSuccessfulDagRun"] = "GetPrevSuccessfulDagRun"


ToSupervisor = Annotated[
    Union[
        SucceedTask,
        DeferTask,
        GetAssetByName,
        GetAssetByUri,
        GetAssetEventByAsset,
        GetAssetEventByAssetAlias,
        GetConnection,
        GetDagRunState,
        GetPrevSuccessfulDagRun,
        GetVariable,
        GetXCom,
        GetXComCount,
        PutVariable,
        RescheduleTask,
        RetryTask,
        SkipDownstreamTasks,
        SetRenderedFields,
        SetXCom,
        TaskState,
        TriggerDagRun,
        RuntimeCheckOnTask,
        DeleteXCom,
    ],
    Field(discriminator="type"),
]
