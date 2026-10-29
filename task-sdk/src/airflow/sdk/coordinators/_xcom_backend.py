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
"""
The XCom backend for the tasks of a language SDK runtime.

A language SDK runtime sends its XCom requests to the supervisor, which runs the configured XCom backend
on them before it forwards them to the API server. The backend runs in a helper process, started per task
the way the supervisor starts a Python task process, so the long-lived supervisor process keeps none of
the backend's state, such as a cached filesystem that a task process forked from it later could not use.

The supervisor services the helper process from the selector of the task it serves, so a backend call
never blocks that task's request loop, which goes on heartbeating and enforcing ``execution_timeout``.
"""

from __future__ import annotations

import contextlib
import selectors
import signal
import time
from collections import deque
from collections.abc import Callable
from typing import TYPE_CHECKING, Annotated, ClassVar, Literal
from uuid import UUID

import attrs
import structlog
from pydantic import BaseModel, Field, JsonValue, TypeAdapter

from airflow.sdk.bases.xcom import BaseXCom, XComCoordinate
from airflow.sdk.execution_time.comms import (
    ConnectionResult,
    ErrorResponse,
    GetConnection,
    GetVariable,
    MaskSecret,
    VariableResult,
    XComResult,
)
from airflow.sdk.execution_time.supervisor import (
    ResponseSent,
    WatchedSubprocess,
    register_request_method,
    task_process_uses_exec,
)

if TYPE_CHECKING:
    from socket import socket
    from typing import Self

    from structlog.typing import FilteringBoundLogger

    from airflow.sdk.api.client import Client

log: FilteringBoundLogger = structlog.get_logger(logger_name="xcom_backend")

_EXIT_TIMEOUT = 10.0


class XComBackendError(RuntimeError):
    """
    An XCom backend call failed.

    For an exception raised in the helper process, the message holds its type and message, since the
    exception itself does not cross the process boundary.
    """


class _Coordinate(BaseModel):
    dag_id: str
    run_id: str
    task_id: str
    ti_id: UUID | None = None
    region_id: UUID | None = None
    region_index: int | None = None


class XComBackendCall(BaseModel):
    """
    A call of an XCom backend hook, sent to the helper process.

    ``value`` is a value as the runtime sends it for ``store``, the XCom row for ``load`` and ``purge``,
    and a list of rows for ``load_many``.
    """

    hook: Literal["store", "load", "load_many", "purge", "exit"]
    key: str = ""
    value: JsonValue = None
    coordinate: _Coordinate | None = None
    map_index: int = -1
    type: Literal["XComBackendCall"] = "XComBackendCall"

    @classmethod
    def store(cls, coordinate: XComCoordinate, map_index: int, key: str, value: JsonValue) -> Self:
        return cls(
            hook="store",
            key=key,
            value=value,
            coordinate=_Coordinate.model_validate(attrs.asdict(coordinate)),
            map_index=map_index,
        )


class XComBackendResult(BaseModel):
    """The outcome of an XCom backend call, sent by the helper process."""

    value: JsonValue = None
    error: str | None = None
    type: Literal["XComBackendResult"] = "XComBackendResult"


class AwaitXComBackendCall(BaseModel):
    """Sent by the helper process when it is ready for its first call."""

    type: Literal["AwaitXComBackendCall"] = "AwaitXComBackendCall"


_ToXComBackendProcess = Annotated[
    XComBackendCall | ConnectionResult | VariableResult | ErrorResponse,
    Field(discriminator="type"),
]
_FromXComBackendProcess = Annotated[
    AwaitXComBackendCall | XComBackendResult | GetConnection | GetVariable | MaskSecret,
    Field(discriminator="type"),
]


def runs_in_helper(xcom: type[BaseXCom]) -> bool:
    """Whether task XComs go through the helper process, which runs every backend but the default one."""
    return xcom is not BaseXCom


_PendingCall = tuple[XComBackendCall, Callable[[XComBackendResult], None]]


@attrs.define(kw_only=True)
class XComBackendProcess(WatchedSubprocess):
    """
    The helper process that runs XCom backend calls for the task of a language SDK runtime.

    The process asks for a call, runs it, and answers with its result, which is also how it asks for the
    next call. Each call carries the callback that takes its result, so a caller never has to match an
    answer to its own call. Connection and Variable lookups of the backend reach the API server through
    ``client``.
    """

    client: Client
    decoder: ClassVar[TypeAdapter[_FromXComBackendProcess]] = TypeAdapter(_FromXComBackendProcess)

    _call_request_id: int | None = attrs.field(default=None, init=False)
    _queued_calls: deque[_PendingCall] = attrs.field(factory=deque, init=False)
    _running_call: Callable[[XComBackendResult], None] | None = attrs.field(default=None, init=False)

    @classmethod
    def start(  # type: ignore[override]
        cls,
        *,
        id: UUID,
        client: Client,
        logger: FilteringBoundLogger,
        selector: selectors.BaseSelector,
    ) -> Self:
        return super().start(
            id=id,
            client=client,
            # The selector of the task this process serves, so that the task's request loop services both.
            selector=selector,
            logger=logger,
            target=run_xcom_backend,
            use_exec=task_process_uses_exec(),
        )

    @property
    def can_run_a_call(self) -> bool:
        """Whether the process can still be sent a call, which a process that has exited cannot."""
        return self.stdin in self._open_sockets

    def send_call(self, call: XComBackendCall, finish: Callable[[XComBackendResult], None]) -> None:
        """
        Send ``call`` to the process, which answers it by calling ``finish`` with its result.

        The process runs one call at a time, so a call waits its turn. ``finish`` also takes the error of
        a call the process never answers, such as when it exits while the call is waiting.
        """
        self._queued_calls.append((call, finish))
        self._send_next_call()

    def _send_next_call(self) -> None:
        """Hand the oldest waiting call to the process, if it has asked for one and runs none."""
        if self._call_request_id is None or self._running_call is not None or not self._queued_calls:
            return
        call, self._running_call = self._queued_calls.popleft()
        request_id, self._call_request_id = self._call_request_id, None
        self.send_msg(call, request_id=request_id)

    def _fail_waiting_calls(self, error: str) -> None:
        """Answer every call the process has not answered with ``error``."""
        running, self._running_call = self._running_call, None
        waiting = [running] if running is not None else []
        waiting += [finish for _, finish in self._queued_calls]
        self._queued_calls.clear()
        for finish in waiting:
            finish(XComBackendResult(error=error))

    def stop(self) -> None:
        """Let the helper process exit, and kill it if it does not exit in time."""
        # The caller gives up on the calls it has not answered, so they are dropped rather than failed.
        self._queued_calls.clear()
        self._running_call = None
        if self._exit_code is None and self._call_request_id is not None:
            # The process may be dead already, such as when the task it serves was killed.
            with contextlib.suppress(OSError):
                self.send_msg(XComBackendCall(hook="exit"), request_id=self._call_request_id)
            self._call_request_id = None
        deadline = time.monotonic() + _EXIT_TIMEOUT
        while self._open_sockets and (now := time.monotonic()) < deadline:
            self._service_subprocess(max_wait_time=deadline - now)
        if self._exit_code is None:
            with contextlib.suppress(self._process.TimeoutExpired):
                self._exit_code = self._process.wait(timeout=max(0.0, deadline - time.monotonic()))
        if self._exit_code is None:
            self.kill(signal.SIGTERM, force=True)
        # Only this process's own sockets, since the selector belongs to the task it serves.
        for sock in list(self._open_sockets):
            self._on_socket_closed(sock)
            sock.close()

    def _on_socket_closed(self, sock: socket) -> None:
        super()._on_socket_closed(sock)
        if sock is self.stdin:
            # The process has exited, or is exiting, so it answers none of the calls it was still to run.
            self._fail_waiting_calls("The XCom backend process exited before it answered")

    def _handle_await_call(
        self, msg: AwaitXComBackendCall, log: FilteringBoundLogger, req_id: int
    ) -> ResponseSent:
        self._call_request_id = req_id
        self._send_next_call()
        return ResponseSent.ALREADY_SENT

    def _handle_result(self, msg: XComBackendResult, log: FilteringBoundLogger, req_id: int) -> ResponseSent:
        self._call_request_id = req_id
        finish, self._running_call = self._running_call, None
        self._send_next_call()
        if finish is not None:
            finish(msg)
        return ResponseSent.ALREADY_SENT

    _request_handlers = {
        **WatchedSubprocess._get_shared_request_handlers(GetConnection, GetVariable, MaskSecret),
        **dict(
            [
                register_request_method(AwaitXComBackendCall, _handle_await_call),
                register_request_method(XComBackendResult, _handle_result),
            ]
        ),
    }


def run_xcom_backend() -> None:
    """Run the XCom backend calls that the supervisor sends, until it sends ``exit``."""
    # Lazy: these load the task runtime and the configured backend, which only the helper process needs.
    from airflow.sdk.execution_time import task_runner
    from airflow.sdk.execution_time.comms import CommsDecoder
    from airflow.sdk.execution_time.xcom import XCom

    comms = CommsDecoder[_ToXComBackendProcess, _FromXComBackendProcess](
        log=log, body_decoder=TypeAdapter(_ToXComBackendProcess)
    )
    # The secrets backends of a task process look up Connections and Variables through SUPERVISOR_COMMS.
    task_runner.SUPERVISOR_COMMS = comms  # type: ignore[assignment]
    runner = _HookRunner(XCom)
    call = comms.send(AwaitXComBackendCall())
    while isinstance(call, XComBackendCall) and call.hook != "exit":
        call = comms.send(runner.run(call))


@attrs.define
class _HookRunner:
    """Runs the hooks of an XCom backend that the supervisor's calls name, one method per hook."""

    xcom: type[BaseXCom]

    def run(self, call: XComBackendCall) -> XComBackendResult:
        try:
            if (hook := getattr(self, call.hook, None)) is None:
                raise ValueError(f"Unknown XCom backend hook {call.hook!r}")
            return hook(call)
        except Exception as e:
            log.exception("XCom backend call failed", hook=call.hook, key=call.key)
            return XComBackendResult(error=f"{type(e).__name__}: {e}")

    def store(self, call: XComBackendCall) -> XComBackendResult:
        # Lazy: only the helper process decodes and encodes XCom values.
        from airflow.sdk.serde import deserialize

        if call.coordinate is None:
            raise ValueError("A store call needs the coordinate of the XCom")
        # A value the backend cannot be given fails the push. Keeping it in the XCom row instead would
        # put the payload in the database, which is what the backend is configured to avoid.
        value = deserialize(call.value)  # type: ignore[type-var]
        coordinate = XComCoordinate(**call.coordinate.model_dump())
        return XComBackendResult(value=self.xcom._store(coordinate, call.map_index, call.key, value))

    def load(self, call: XComBackendCall) -> XComBackendResult:
        return XComBackendResult(value=self._load_row(call.key, call.value))

    def load_many(self, call: XComBackendCall) -> XComBackendResult:
        rows = call.value if isinstance(call.value, list) else []
        return XComBackendResult(value=[self._load_row(call.key, row) for row in rows])

    def purge(self, call: XComBackendCall) -> XComBackendResult:
        self.xcom.purge(XComResult(key=call.key, value=call.value))
        return XComBackendResult()

    def _load_row(self, key: str, row: JsonValue) -> JsonValue:
        """Return what the runtime receives for ``row``: what the default backend would keep in the row."""
        from airflow.sdk.serde import serialize

        # A row the backend cannot load, or a value this process cannot encode, fails the read rather
        # than reaching the runtime as the raw row, which would hide the backend being unusable.
        return serialize(self.xcom._load(XComResult(key=key, value=row)))  # type: ignore[return-value]
