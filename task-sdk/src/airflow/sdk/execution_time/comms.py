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

* All communication is done over the subprocesses stdin in the form of a binary length-prefixed msgpack frame
  (4 byte, big-endian length, followed by the msgpack-encoded _RequestFrame.) Each side uses this same
  encoding
* Log Messages from the subprocess are sent over the dedicated logs socket (which is line-based JSON)
* No messages are sent to task process except in response to a request. (This is because the task process will
  be running user's code, so we can't read from stdin until we enter our code, such as when requesting an XCom
  value etc.)
* Every request returns a response, even if the frame is otherwise empty.
* Requests are written by the subprocess to fd0/stdin. This is making use of the fact that stdin is a
  bi-directional socket, and thus we can write to it and don't need a dedicated extra socket for sending
  requests.

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

import itertools
from collections.abc import Iterator
from datetime import datetime
from functools import cached_property
from pathlib import Path
from socket import socket
from typing import TYPE_CHECKING, Annotated, Any, ClassVar, Generic, Literal, TypeVar, Union, overload
from uuid import UUID

import attrs
import msgspec
import structlog
from fastapi import Body
from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, JsonValue, TypeAdapter, field_serializer

from airflow.sdk.api.datamodels._generated import (
    AssetEventDagRunReference,
    AssetEventResponse,
    AssetEventsResponse,
    AssetResponse,
    BundleInfo,
    ConnectionResponse,
    DagRunStateResponse,
    InactiveAssetsResponse,
    PrevSuccessfulDagRunResponse,
    TaskInstance,
    TaskInstanceState,
    TaskStatesResponse,
    TIDeferredStatePayload,
    TIRescheduleStatePayload,
    TIRetryStatePayload,
    TIRunContext,
    TISkippedDownstreamTasksStatePayload,
    TISuccessStatePayload,
    TriggerDAGRunPayload,
    VariableResponse,
    XComResponse,
    XComSequenceIndexResponse,
    XComSequenceSliceResponse,
)
from airflow.sdk.exceptions import ErrorType

try:
    from socket import recv_fds
except ImportError:
    # Available on Unix and Windows (so "everywhere") but lets be safe
    recv_fds = None  # type: ignore[assignment]

if TYPE_CHECKING:
    from structlog.typing import FilteringBoundLogger as Logger

SendMsgType = TypeVar("SendMsgType", bound=BaseModel)
ReceiveMsgType = TypeVar("ReceiveMsgType", bound=BaseModel)


def _msgpack_enc_hook(obj: Any) -> Any:
    import pendulum

    if isinstance(obj, pendulum.DateTime):
        # convert the pendulm Datetime subclass into a raw datetime so that msgspec can use it's native
        # encoding
        return datetime(
            obj.year, obj.month, obj.day, obj.hour, obj.minute, obj.second, obj.microsecond, tzinfo=obj.tzinfo
        )
    if isinstance(obj, Path):
        return str(obj)
    if isinstance(obj, BaseModel):
        return obj.model_dump(exclude_unset=True)

    # Raise a NotImplementedError for other types
    raise NotImplementedError(f"Objects of type {type(obj)} are not supported")


def _new_encoder() -> msgspec.msgpack.Encoder:
    return msgspec.msgpack.Encoder(enc_hook=_msgpack_enc_hook)


class _RequestFrame(msgspec.Struct, array_like=True, frozen=True, omit_defaults=True):
    id: int
    """
    The request id, set by the sender.

    This is used to allow "pipeling" of requests and to be able to tie response to requests, which is
    particularly useful in the Triggerer where multiple async tasks can send a requests concurrently.
    """
    body: dict[str, Any] | None

    req_encoder: ClassVar[msgspec.msgpack.Encoder] = _new_encoder()

    def as_bytes(self) -> bytearray:
        # https://jcristharif.com/msgspec/perf-tips.html#length-prefix-framing for inspiration
        buffer = bytearray(256)

        self.req_encoder.encode_into(self, buffer, 4)

        n = len(buffer) - 4
        if n >= 2**32:
            raise OverflowError(f"Cannot send messages larger than 4GiB {n=}")
        buffer[:4] = n.to_bytes(4, byteorder="big")

        return buffer


class _ResponseFrame(_RequestFrame, frozen=True):
    id: int
    """
    The id of the request this is a response to
    """
    body: dict[str, Any] | None = None
    error: dict[str, Any] | None = None


@attrs.define()
class CommsDecoder(Generic[ReceiveMsgType, SendMsgType]):
    """Handle communication between the task in this process and the supervisor parent process."""

    log: Logger = attrs.field(repr=False, factory=structlog.get_logger)
    socket: socket = attrs.field(factory=lambda: socket(fileno=0))

    resp_decoder: msgspec.msgpack.Decoder[_ResponseFrame] = attrs.field(
        factory=lambda: msgspec.msgpack.Decoder(_ResponseFrame), repr=False
    )

    id_counter: Iterator[int] = attrs.field(factory=itertools.count)

    # We could be "clever" here and set the default to this based type parameters and a custom
    # `__class_getitem__`, but that's a lot of code the one subclass we've got currently. So we'll just use a
    # "sort of wrong default"
    body_decoder: TypeAdapter[ReceiveMsgType] = attrs.field(factory=lambda: TypeAdapter(ToTask), repr=False)

    err_decoder: TypeAdapter[ErrorResponse] = attrs.field(factory=lambda: TypeAdapter(ToTask), repr=False)

    def send(self, msg: SendMsgType) -> ReceiveMsgType | None:
        """Send a request to the parent and block until the response is received."""
        frame = _RequestFrame(id=next(self.id_counter), body=msg.model_dump())
        bytes = frame.as_bytes()

        self.socket.sendall(bytes)
        if isinstance(msg, ResendLoggingFD):
            if recv_fds is None:
                return None
            # We need special handling here! The server can't send us the fd number, as the number on the
            # supervisor will be different to in this process, so we have to mutate the message ourselves here.
            frame, fds = self._read_frame(maxfds=1)
            resp = self._from_frame(frame)
            if TYPE_CHECKING:
                assert isinstance(resp, SentFDs)
            resp.fds = fds
            # Since we know this is an expliclt SendFDs, and since this class is generic SendFDs might not
            # always be in the return type union
            return resp  # type: ignore[return-value]

        return self._get_response()

    @overload
    def _read_frame(self, maxfds: None = None) -> _ResponseFrame: ...

    @overload
    def _read_frame(self, maxfds: int) -> tuple[_ResponseFrame, list[int]]: ...

    def _read_frame(self, maxfds: int | None = None) -> tuple[_ResponseFrame, list[int]] | _ResponseFrame:
        """
        Get a message from the parent.

        This will block until the message has been received.
        """
        if self.socket:
            self.socket.setblocking(True)
        fds = None
        if maxfds:
            len_bytes, fds, flag, address = recv_fds(self.socket, 4, maxfds)
        else:
            len_bytes = self.socket.recv(4)

        if len_bytes == b"":
            raise EOFError("Request socket closed before length")

        len = int.from_bytes(len_bytes, byteorder="big")

        buffer = bytearray(len)
        nread = self.socket.recv_into(buffer)
        if nread != len:
            raise RuntimeError(
                f"unable to read full response in child. (We read {nread}, but expected {len})"
            )
        if nread == 0:
            raise EOFError(f"Request socket closed before response was complete ({self.id_counter=})")

        resp = self.resp_decoder.decode(buffer)
        if maxfds:
            return resp, fds or []
        return resp

    def _from_frame(self, frame) -> ReceiveMsgType | None:
        from airflow.sdk.exceptions import AirflowRuntimeError

        if frame.error is not None:
            err = self.err_decoder.validate_python(frame.error)
            raise AirflowRuntimeError(error=err)

        if frame.body is None:
            return None

        try:
            return self.body_decoder.validate_python(frame.body)
        except Exception:
            self.log.exception("Unable to decode message")
            raise

    def _get_response(self) -> ReceiveMsgType | None:
        frame = self._read_frame()
        return self._from_frame(frame)


class StartupDetails(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    ti: TaskInstance
    dag_rel_path: str
    bundle_info: BundleInfo
    start_date: datetime
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


@attrs.define(kw_only=True)
class AssetEventSourceTaskInstance:
    """Used in AssetEventResult."""

    dag_id: str
    task_id: str
    run_id: str
    map_index: int

    def xcom_pull(
        self,
        *,
        key: str = "return_value",  # TODO: Make this a constant; see RuntimeTaskInstance.
        default: Any = None,
    ) -> Any:
        from airflow.sdk.execution_time.xcom import XCom

        if (value := XCom.get_value(ti_key=self, key=key)) is None:
            return default
        return value


class AssetEventResult(AssetEventResponse):
    """Used in AssetEventsResult."""

    @classmethod
    def from_asset_event_response(cls, asset_event_response: AssetEventResponse) -> AssetEventResult:
        return cls(**asset_event_response.model_dump(exclude_defaults=True))

    @cached_property
    def source_task_instance(self) -> AssetEventSourceTaskInstance | None:
        if not (self.source_task_id and self.source_dag_id and self.source_run_id):
            return None
        if self.source_map_index is None:
            return None

        return AssetEventSourceTaskInstance(
            dag_id=self.source_dag_id,
            task_id=self.source_task_id,
            run_id=self.source_run_id,
            map_index=self.source_map_index,
        )


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

    def iter_asset_event_results(self) -> Iterator[AssetEventResult]:
        return (AssetEventResult.from_asset_event_response(event) for event in self.asset_events)


class AssetEventDagRunReferenceResult(AssetEventDagRunReference):
    @classmethod
    def from_asset_event_dag_run_reference(
        cls,
        asset_event_dag_run_reference: AssetEventDagRunReference,
    ) -> AssetEventDagRunReferenceResult:
        return cls(**asset_event_dag_run_reference.model_dump(exclude_defaults=True))

    @cached_property
    def source_task_instance(self) -> AssetEventSourceTaskInstance | None:
        if not (self.source_task_id and self.source_dag_id and self.source_run_id):
            return None
        if self.source_map_index is None:
            return None

        return AssetEventSourceTaskInstance(
            dag_id=self.source_dag_id,
            task_id=self.source_task_id,
            run_id=self.source_run_id,
            map_index=self.source_map_index,
        )


class InactiveAssetsResult(InactiveAssetsResponse):
    """Response of InactiveAssets requests."""

    type: Literal["InactiveAssetsResult"] = "InactiveAssetsResult"

    @classmethod
    def from_inactive_assets_response(
        cls, inactive_assets_response: InactiveAssetsResponse
    ) -> InactiveAssetsResult:
        """
        Get InactiveAssetsResponse from InactiveAssetsResult.

        InactiveAssetsResponse is autogenerated from the API schema, so we need to convert it to InactiveAssetsResult
        for communication between the Supervisor and the task process.
        """
        return cls(**inactive_assets_response.model_dump(exclude_defaults=True), type="InactiveAssetsResult")


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
        return cls(**xcom_response.model_dump(exclude_defaults=True), type="XComResult")


class XComCountResponse(BaseModel):
    len: int
    type: Literal["XComLengthResponse"] = "XComLengthResponse"


class XComSequenceIndexResult(BaseModel):
    root: JsonValue
    type: Literal["XComSequenceIndexResult"] = "XComSequenceIndexResult"

    @classmethod
    def from_response(cls, response: XComSequenceIndexResponse) -> XComSequenceIndexResult:
        return cls(root=response.root, type="XComSequenceIndexResult")


class XComSequenceSliceResult(BaseModel):
    root: list[JsonValue]
    type: Literal["XComSequenceSliceResult"] = "XComSequenceSliceResult"

    @classmethod
    def from_response(cls, response: XComSequenceSliceResponse) -> XComSequenceSliceResult:
        return cls(root=response.root, type="XComSequenceSliceResult")


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


class TaskRescheduleStartDate(BaseModel):
    """Response containing the first reschedule date for a task instance."""

    start_date: AwareDatetime | None
    type: Literal["TaskRescheduleStartDate"] = "TaskRescheduleStartDate"


class TICount(BaseModel):
    """Response containing count of Task Instances matching certain filters."""

    count: int
    type: Literal["TICount"] = "TICount"


class TaskStatesResult(TaskStatesResponse):
    type: Literal["TaskStatesResult"] = "TaskStatesResult"

    @classmethod
    def from_api_response(cls, task_states_response: TaskStatesResponse) -> TaskStatesResult:
        """
        Create result class from API Response.

        API Response is autogenerated from the API schema, so we need to convert it to Result
        for communication between the Supervisor and the task process since it needs a
        discriminator field.
        """
        return cls(**task_states_response.model_dump(exclude_defaults=True), type="TaskStatesResult")


class DRCount(BaseModel):
    """Response containing count of DAG Runs matching certain filters."""

    count: int
    type: Literal["DRCount"] = "DRCount"


class ErrorResponse(BaseModel):
    error: ErrorType = ErrorType.GENERIC_ERROR
    detail: dict | None = None
    type: Literal["ErrorResponse"] = "ErrorResponse"


class OKResponse(BaseModel):
    ok: bool
    type: Literal["OKResponse"] = "OKResponse"


class SentFDs(BaseModel):
    type: Literal["SentFDs"] = "SentFDs"
    fds: list[int]


ToTask = Annotated[
    Union[
        AssetResult,
        AssetEventsResult,
        ConnectionResult,
        DagRunStateResult,
        DRCount,
        ErrorResponse,
        PrevSuccessfulDagRunResult,
        SentFDs,
        StartupDetails,
        TaskRescheduleStartDate,
        TICount,
        TaskStatesResult,
        VariableResult,
        XComCountResponse,
        XComResult,
        XComSequenceIndexResult,
        XComSequenceSliceResult,
        InactiveAssetsResult,
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
        TaskInstanceState.FAILED,
        TaskInstanceState.SKIPPED,
        TaskInstanceState.REMOVED,
    ]
    end_date: datetime | None = None
    type: Literal["TaskState"] = "TaskState"
    rendered_map_index: str | None = None


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


class GetXCom(BaseModel):
    key: str
    dag_id: str
    run_id: str
    task_id: str
    map_index: int | None = None
    include_prior_dates: bool = False
    type: Literal["GetXCom"] = "GetXCom"


class GetXComCount(BaseModel):
    """Get the number of (mapped) XCom values available."""

    key: str
    dag_id: str
    run_id: str
    task_id: str
    type: Literal["GetNumberXComs"] = "GetNumberXComs"


class GetXComSequenceItem(BaseModel):
    key: str
    dag_id: str
    run_id: str
    task_id: str
    offset: int
    type: Literal["GetXComSequenceItem"] = "GetXComSequenceItem"


class GetXComSequenceSlice(BaseModel):
    key: str
    dag_id: str
    run_id: str
    task_id: str
    start: int | None
    stop: int | None
    step: int | None
    type: Literal["GetXComSequenceSlice"] = "GetXComSequenceSlice"


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


class DeleteVariable(BaseModel):
    key: str
    type: Literal["DeleteVariable"] = "DeleteVariable"


class ResendLoggingFD(BaseModel):
    type: Literal["ResendLoggingFD"] = "ResendLoggingFD"


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


class ValidateInletsAndOutlets(BaseModel):
    ti_id: UUID
    type: Literal["ValidateInletsAndOutlets"] = "ValidateInletsAndOutlets"


class GetPrevSuccessfulDagRun(BaseModel):
    ti_id: UUID
    type: Literal["GetPrevSuccessfulDagRun"] = "GetPrevSuccessfulDagRun"


class GetTaskRescheduleStartDate(BaseModel):
    ti_id: UUID
    try_number: int = 1
    type: Literal["GetTaskRescheduleStartDate"] = "GetTaskRescheduleStartDate"


class GetTICount(BaseModel):
    dag_id: str
    map_index: int | None = None
    task_ids: list[str] | None = None
    task_group_id: str | None = None
    logical_dates: list[AwareDatetime] | None = None
    run_ids: list[str] | None = None
    states: list[str] | None = None
    type: Literal["GetTICount"] = "GetTICount"


class GetTaskStates(BaseModel):
    dag_id: str
    map_index: int | None = None
    task_ids: list[str] | None = None
    task_group_id: str | None = None
    logical_dates: list[AwareDatetime] | None = None
    run_ids: list[str] | None = None
    type: Literal["GetTaskStates"] = "GetTaskStates"


class GetDRCount(BaseModel):
    dag_id: str
    logical_dates: list[AwareDatetime] | None = None
    run_ids: list[str] | None = None
    states: list[str] | None = None
    type: Literal["GetDRCount"] = "GetDRCount"


ToSupervisor = Annotated[
    Union[
        DeferTask,
        DeleteXCom,
        GetAssetByName,
        GetAssetByUri,
        GetAssetEventByAsset,
        GetAssetEventByAssetAlias,
        GetConnection,
        GetDagRunState,
        GetDRCount,
        GetPrevSuccessfulDagRun,
        GetTaskRescheduleStartDate,
        GetTICount,
        GetTaskStates,
        GetVariable,
        GetXCom,
        GetXComCount,
        GetXComSequenceItem,
        GetXComSequenceSlice,
        PutVariable,
        RescheduleTask,
        RetryTask,
        SetRenderedFields,
        SetXCom,
        SkipDownstreamTasks,
        SucceedTask,
        ValidateInletsAndOutlets,
        TaskState,
        TriggerDagRun,
        DeleteVariable,
        ResendLoggingFD,
    ],
    Field(discriminator="type"),
]
