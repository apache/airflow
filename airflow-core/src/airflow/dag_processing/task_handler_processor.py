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
"""Ask the Lang-SDK runtime of a coordinator which task handlers an artifact registers."""

from __future__ import annotations

import functools
import os
import selectors
import signal
import time
from pathlib import Path
from socket import socket
from typing import TYPE_CHECKING, Annotated, Any, ClassVar, Literal, cast, get_args

import attrs
import msgspec
from pydantic import BaseModel, Field, TypeAdapter
from uuid6 import uuid7

from airflow import settings
from airflow.configuration import conf
from airflow.dag_processing.processor import (
    BaseDagFileProcessorProcess,
    DagFileParsingResult,
    TaskHandlerParseRequest,
    TaskHandlerParsingResult,
    ToManager,
)
from airflow.sdk.coordinators._subprocess import _is_connection_from_pid, _start_server
from airflow.sdk.exceptions import AirflowRuntimeError
from airflow.sdk.execution_time import supervisor, task_runner
from airflow.sdk.execution_time.comms import CommsDecoder, ErrorResponse, MaskSecret, _RequestFrame
from airflow.sdk.execution_time.coordinator import get_coordinator_manager
from airflow.sdk.execution_time.supervisor import (
    ResponseSent,
    length_prefixed_frame_reader,
    make_buffered_socket_reader,
    process_log_messages_from_subprocess,
    register_request_method,
)

if TYPE_CHECKING:
    from collections.abc import Generator

    from structlog.typing import FilteringBoundLogger

    from airflow.sdk.api.client import Client
    from airflow.sdk.execution_time.supervisor import RequestHandler, RequestResult
    from airflow.typing_compat import Self

# How long a runtime may keep running after its parse result, as Node does while a handle stays open.
_EXIT_GRACE_PERIOD = 5.0


# StartTaskHandlerRuntime, LangSDKRuntimeSchemaVersion and LangSDKRuntimeStartFailed pass only between
# the probe and its forked child before the exec, so they are not part of the supervisor schema the
# runtimes speak.


class StartTaskHandlerRuntime(BaseModel):
    """Ask the parse child to exec the runtime of the coordinator configured under *coordinator*."""

    file: str
    bundle_path: Path
    coordinator: str
    """The coordinator's key in ``[sdk] coordinators``."""
    comm_address: tuple[str, int]
    logs_address: tuple[str, int]
    type: Literal["StartTaskHandlerRuntime"] = "StartTaskHandlerRuntime"


class LangSDKRuntimeSchemaVersion(BaseModel):
    """The schema version and the import timeout of the runtime the parse child is about to exec."""

    schema_version: str | None
    import_timeout: float | None = None
    """Seconds from the start of the parse; ``None`` means no timeout."""
    type: Literal["LangSDKRuntimeSchemaVersion"] = "LangSDKRuntimeSchemaVersion"


class LangSDKRuntimeStartFailed(BaseModel):
    """Why the parse child could not start the runtime."""

    error: str
    type: Literal["LangSDKRuntimeStartFailed"] = "LangSDKRuntimeStartFailed"


def _get_import_timeout(path: str) -> float | None:
    """Return the ``get_dagbag_import_timeout`` policy's timeout for *path*; ``None`` means none."""
    timeout = settings.get_dagbag_import_timeout(path)
    if not isinstance(timeout, (int, float)):
        raise TypeError(f"Value ({timeout}) from get_dagbag_import_timeout must be int or float")
    return timeout if timeout > 0 else None


def _start_task_handler_runtime_entrypoint() -> None:
    """Exec the runtime that probes the artifact named by the start request, or report why it cannot start."""
    os.environ["_AIRFLOW_PROCESS_CONTEXT"] = "client"
    # fd 0 becomes the runtime's stdin, so the request channel moves to a close-on-exec copy.
    comms = CommsDecoder[StartTaskHandlerRuntime, LangSDKRuntimeSchemaVersion | LangSDKRuntimeStartFailed](
        socket=socket(fileno=os.dup(0)),
        body_decoder=TypeAdapter(StartTaskHandlerRuntime),
    )
    devnull = os.open(os.devnull, os.O_RDONLY)
    os.dup2(devnull, 0)
    os.close(devnull)

    msg = comms._get_response()
    if not isinstance(msg, StartTaskHandlerRuntime):
        raise RuntimeError(f"Required first message to be a StartTaskHandlerRuntime, it was {msg}")

    def report_schema_version(schema_version: str | None) -> None:
        comms.send(LangSDKRuntimeSchemaVersion(schema_version=schema_version, import_timeout=import_timeout))

    try:
        # The policy is user code: it runs in this child, where a failure is only this file's import error.
        import_timeout = _get_import_timeout(msg.file)
        get_coordinator_manager().get_coordinator(msg.coordinator).parse_task_handler(
            path=Path(msg.file),
            bundle_path=msg.bundle_path,
            comm_address=msg.comm_address,
            logs_address=msg.logs_address,
            report_schema_version=report_schema_version,
        )
    except Exception as e:
        comms.send(LangSDKRuntimeStartFailed(error=f"{type(e).__name__}: {e}"))


_Channel = Literal["comm", "logs"]


# The requests relayed to the Dag processor without a client: those it answers, but not the Dag parse's
# result. MaskSecret is not relayed: its handler masks the secret here, and mask_secret sends it on to a
# parent.
_PARENT_REQUESTS = frozenset(BaseDagFileProcessorProcess._request_handlers) - {
    DagFileParsingResult,
    MaskSecret,
}


@attrs.define(kw_only=True)
class LangSDKTaskHandlerProcessorProcess(BaseDagFileProcessorProcess):
    """
    Ask a coordinator's Lang-SDK runtime for every task handler an artifact registers.

    The forked parse child finds the coordinator, reports the runtime's schema version and execs the
    runtime. The runtime connects back to two listeners this process owns and answers the
    ``TaskHandlerParseRequest`` itself, so the request is sent once it has connected. A failed start,
    a missing result, an invalid frame or message, or a timeout is an import error on the result,
    keyed by the artifact's path in its Dag bundle.

    The runtime's requests are answered with :attr:`client`. Without one, as in a Dag-parsing child,
    they are relayed up the supervisor channel of the process this runs in, or get an error when there
    is none. On Linux the runtime is killed when the thread that started this process exits, so start
    it from a thread that outlives the probe.
    """

    client: Client | None = None  # type: ignore[assignment]
    """Answers the runtime's requests; without one, they are relayed to the parent process if there is one."""

    parsing_result: TaskHandlerParsingResult | None = None  # type: ignore[assignment]

    decoder = TypeAdapter(
        Annotated[
            LangSDKRuntimeSchemaVersion | LangSDKRuntimeStartFailed | get_args(ToManager)[0],
            Field(discriminator="type"),
        ]
    )

    coordinator: str
    """The coordinator's key in ``[sdk] coordinators``."""

    _listeners: dict[_Channel, socket]
    _parse_request: TaskHandlerParseRequest
    _runtime_schema_version: str | None = attrs.field(default=None, init=False)
    _import_timeout: float | None = attrs.field(default=None, init=False)
    _schema_version_reported: bool = attrs.field(default=False, init=False)
    _parsing_result_monotonic: float | None = attrs.field(default=None, init=False)
    _unverified_connections: list[tuple[socket, _Channel]] = attrs.field(factory=list, init=False)

    @classmethod
    def start(  # type: ignore[override]
        cls,
        *,
        coordinator: str,
        path: str | os.PathLike[str],
        bundle_path: Path,
        bundle_name: str,
        artifact_rel_path: str,
        **kwargs,
    ) -> Self:
        """
        Start probing the artifact at *path* for every task handler it registers.

        *bundle_path* and *bundle_name* are those of the Dag bundle holding the artifact, and
        *artifact_rel_path* is the artifact's path in it.
        """
        listeners: dict[_Channel, socket] = {"comm": _start_server(), "logs": _start_server()}
        try:
            for listener in listeners.values():
                listener.setblocking(False)
            parse_request = TaskHandlerParseRequest(
                file=os.fspath(path), bundle_path=bundle_path, bundle_name=bundle_name
            )
            proc = super().start(
                target=_start_task_handler_runtime_entrypoint,
                use_exec=supervisor._should_use_exec(),
                new_process_group=True,
                coordinator=coordinator,
                bundle_name=bundle_name,
                dag_file_rel_path=artifact_rel_path,
                listeners=listeners,
                parse_request=parse_request,
                **kwargs,
            )
        except BaseException:
            for listener in listeners.values():
                listener.close()
            raise
        for channel, listener in listeners.items():
            proc._open_sockets[listener] = f"{channel}-listener"
            proc.selector.register(
                listener,
                selectors.EVENT_READ,
                (functools.partial(proc._accept_connection, channel=channel), proc._on_socket_closed),
            )
        proc.send_msg(
            StartTaskHandlerRuntime(
                file=parse_request.file,
                bundle_path=bundle_path,
                coordinator=coordinator,
                comm_address=listeners["comm"].getsockname()[:2],
                logs_address=listeners["logs"].getsockname()[:2],
            ),
            request_id=0,
        )
        return proc

    @classmethod
    def run(
        cls,
        *,
        coordinator: str,
        path: str | os.PathLike[str],
        bundle_path: Path,
        bundle_name: str,
        artifact_rel_path: str,
        logger: FilteringBoundLogger,
    ) -> TaskHandlerParsingResult:
        """
        Probe the artifact at *path* as :meth:`start` does, and wait for the result.

        The artifact's ``get_dagbag_import_timeout`` bounds the probe, and
        ``[dag_processor] dag_file_processor_timeout`` until the parse child resolves it.
        """
        return cls._run_to_completion(
            coordinator=coordinator,
            path=path,
            bundle_path=bundle_path,
            bundle_name=bundle_name,
            artifact_rel_path=artifact_rel_path,
            logger=logger,
        )

    @classmethod
    def _run_to_completion(
        cls, *, logger: FilteringBoundLogger, **start_kwargs: Any
    ) -> TaskHandlerParsingResult:
        """
        Probe outside a caller's selector loop and wait for the result.

        The import timeout the parse child reports bounds the probe, and
        ``[dag_processor] dag_file_processor_timeout`` until it is reported.
        """
        processor_timeout = conf.getfloat("dag_processor", "dag_file_processor_timeout")
        with selectors.DefaultSelector() as selector:
            proc = cls.start(id=uuid7(), selector=selector, logger=logger, **start_kwargs)
            try:
                while not proc.is_ready:
                    timeout = proc._import_timeout if proc._schema_version_reported else processor_timeout
                    if timeout is not None and time.monotonic() - proc.start_time > timeout:
                        # Unlike is_ready, this does not wait for an exited runtime's leftover processes,
                        # which can hold its sockets open. close() closes them.
                        proc._time_out(timeout)
                        break
                    proc._service_subprocess(max_wait_time=0.1)
            except BaseException:
                proc._kill_runtime()
                raise
            finally:
                proc.close()
        return cast("TaskHandlerParsingResult", proc.parsing_result)

    def _accept_connection(self, listener: socket, *, channel: _Channel) -> bool:
        try:
            conn, _ = listener.accept()
        except (BlockingIOError, InterruptedError):
            return True
        conn.setblocking(True)
        self._unverified_connections.append((conn, channel))
        self._verify_connections()
        return True

    def _verify_connections(self) -> None:
        """
        Use each accepted connection once it is confirmed to come from the runtime.

        A connection that is not visible yet stays pending and is checked again on the next
        ``is_ready`` poll, so the caller's loop never waits here.
        """
        pending = []
        for conn, channel in self._unverified_connections:
            if channel not in self._listeners:
                # The runtime already connected this channel.
                conn.close()
                continue
            try:
                owned = _is_connection_from_pid(conn, self.pid)
            except OSError:
                conn.close()
                continue
            if not owned:
                pending.append((conn, channel))
                continue
            self._close_listener(channel)
            if channel == "comm":
                self._register_comm(conn)
            else:
                self._register_logs(conn)
        self._unverified_connections = pending

    def _close_listener(self, channel: _Channel) -> None:
        if (listener := self._listeners.pop(channel, None)) is not None:
            self._on_socket_closed(listener)
            listener.close()

    def _close_listeners(self) -> None:
        """Close the listeners of a runtime that did not connect, and connections never verified."""
        for channel in list(self._listeners):
            self._close_listener(channel)
        for conn, _ in self._unverified_connections:
            conn.close()
        self._unverified_connections = []

    def _register_comm(self, conn: socket) -> None:
        self.stdin = conn
        self._open_sockets[conn] = "requests"
        read_frame, on_close = length_prefixed_frame_reader(
            self._handle_valid_requests(), on_close=self._on_socket_closed
        )

        def read_valid_frame(sock: socket) -> bool:
            # A frame that does not decode would otherwise escape the caller's selector loop.
            try:
                return read_frame(sock)
            except msgspec.DecodeError as e:
                self._fail_on_invalid_message(f"The Lang-SDK runtime sent an invalid frame: {e}")
                return False

        self.selector.register(conn, selectors.EVENT_READ, (read_valid_frame, on_close))
        # The parse child reports the version and waits for the reply before it execs the runtime,
        # so the version is known here. It is set only now, so the child's messages are not migrated.
        self._subprocess_schema_version = self._runtime_schema_version
        self.send_msg(self._parse_request, request_id=0)

    def _handle_valid_requests(self) -> Generator[None, _RequestFrame, None]:
        """
        Pass each request on to ``handle_requests``, or kill the runtime at one that does not validate.

        ``handle_requests`` would only log such a request, and the runtime would wait for a reply. The
        runtime speaks ``ToManager`` only; the start messages come from the parse child.
        """
        requests = self.handle_requests(self.process_log)
        next(requests)
        while True:
            frame = yield
            try:
                BaseDagFileProcessorProcess.decoder.validate_python(self._deserialize_request(frame.body))
            except ValueError as e:
                self._fail_on_invalid_message(
                    f"The Lang-SDK runtime sent a message that does not validate: {e}"
                )
                return
            requests.send(frame)

    def _fail_on_invalid_message(self, message: str) -> None:
        """Kill the runtime; *message* is the import error unless a parse result was already received."""
        if self.parsing_result is None:
            self._set_import_error(message)
        else:
            self.process_log.warning(
                "Ignoring an invalid message from the Lang-SDK runtime after its parse result", error=message
            )
        self._kill_runtime()

    def _register_logs(self, conn: socket) -> None:
        self._open_sockets[conn] = "logs"
        self.selector.register(
            conn,
            selectors.EVENT_READ,
            make_buffered_socket_reader(
                process_log_messages_from_subprocess(self._get_target_loggers()),
                on_close=self._on_socket_closed,
            ),
        )

    def _set_import_error(self, message: str) -> None:
        self.parsing_result = TaskHandlerParsingResult(
            fileloc=self._parse_request.file,
            task_handlers={},
            import_errors={self.dag_file_rel_path: message},
        )

    def _handle_runtime_schema_version(
        self, msg: LangSDKRuntimeSchemaVersion, log: FilteringBoundLogger, req_id: int
    ) -> RequestResult | ResponseSent:
        if self._schema_version_reported:
            self._reject_request(msg, log, req_id)
            return ResponseSent.ALREADY_SENT
        self._runtime_schema_version = msg.schema_version
        self._import_timeout = msg.import_timeout
        self._schema_version_reported = True
        return None, {}

    def _handle_start_failed(
        self, msg: LangSDKRuntimeStartFailed, log: FilteringBoundLogger, req_id: int
    ) -> RequestResult | ResponseSent:
        self._set_import_error(f"Cannot start the Lang-SDK runtime: {msg.error}")
        return None, {}

    def _handle_parsing_result(  # type: ignore[override]
        self, msg: TaskHandlerParsingResult, log: FilteringBoundLogger, req_id: int
    ) -> RequestResult | ResponseSent:
        if self.parsing_result is not None:
            log.warning("Ignoring another parse result from the Lang-SDK runtime", fileloc=msg.fileloc)
            self.send_msg(
                None,
                request_id=req_id,
                error=ErrorResponse(detail={"message": "A parse result was already received"}),
            )
            return ResponseSent.ALREADY_SENT
        self.parsing_result = msg
        self._parsing_result_monotonic = time.monotonic()
        return None, {}

    _request_handlers: ClassVar[dict[type[BaseModel], RequestHandler[Any]]] = {
        **{
            message_type: handler
            for message_type, handler in BaseDagFileProcessorProcess._request_handlers.items()
            if message_type is not DagFileParsingResult
        },
        **dict(
            [
                register_request_method(LangSDKRuntimeSchemaVersion, _handle_runtime_schema_version),
                register_request_method(LangSDKRuntimeStartFailed, _handle_start_failed),
                register_request_method(TaskHandlerParsingResult, _handle_parsing_result),
            ]
        ),
    }

    def _handle_request(self, msg, log: FilteringBoundLogger, req_id: int) -> None:
        if self.client is None and type(msg) in _PARENT_REQUESTS:
            self._relay_request(msg, req_id)
            return
        super()._handle_request(msg, log, req_id)

    def _relay_request(self, msg: BaseModel, req_id: int) -> None:
        """Answer the runtime's request through the supervisor channel of this process, if it has one."""
        comms = getattr(task_runner, "SUPERVISOR_COMMS", None)
        if comms is None:
            self.send_msg(
                None,
                request_id=req_id,
                error=ErrorResponse(
                    detail={"message": f"{type(msg).__name__} is answered only in the Dag processor"}
                ),
            )
            return
        try:
            response = comms.send(msg)
        except AirflowRuntimeError as e:
            self.send_msg(None, request_id=req_id, error=e.error)
            return
        # Only the fields the parent sent, under their wire names, so the runtime gets the same body.
        self.send_msg(response, request_id=req_id, exclude_unset=True, by_alias=True)

    @property
    def is_ready(self) -> bool:
        self._verify_connections()
        if (
            self._parsing_result_monotonic is not None
            and self._exit_code is None
            and time.monotonic() - self._parsing_result_monotonic > _EXIT_GRACE_PERIOD
        ):
            self.process_log.warning("The Lang-SDK runtime did not exit after its parse result; killing it")
            self._kill_runtime()
        if (
            self._import_timeout is not None
            and self.parsing_result is None
            and self._exit_code is None
            and time.monotonic() - self.start_time > self._import_timeout
        ):
            self._time_out(self._import_timeout)
        if self._check_subprocess_exit() is None:
            return False
        self._close_listeners()
        if not super().is_ready:
            return False
        if self.parsing_result is None:
            self._set_import_error(
                f"The Lang-SDK runtime exited with code {self._exit_code} without a parse result"
            )
        return True

    def _time_out(self, timeout: float) -> None:
        if self.parsing_result is None:
            self._set_import_error(
                f"The Lang-SDK runtime did not parse {self._parse_request.file} within {timeout}s"
            )
        self._kill_runtime()

    def _kill_runtime(self) -> None:
        """Kill the runtime and wait for it, without servicing its sockets, whose handler may have failed."""
        if self._exit_code is not None:
            return
        try:
            self._signal_subprocess(signal.SIGKILL)
            self._exit_code = self._process.wait(timeout=None)
        except (self._process.ProcessNotFound, ProcessLookupError):
            self._exit_code = -1

    def close(self) -> None:
        # A listener has nothing to drain, and cleanup would call its accept handler forever.
        self._close_listeners()
        super().close()
