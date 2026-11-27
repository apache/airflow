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
"""Supervised execution of callback workloads."""

from __future__ import annotations

import os
import signal
import sys
import time
from collections.abc import Callable
from concurrent.futures import Future
from contextlib import suppress
from contextvars import copy_context
from functools import partial
from http import HTTPStatus
from importlib import import_module
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path
from threading import Event
from typing import TYPE_CHECKING, Annotated, Any, BinaryIO, ClassVar, Literal, Protocol, TypeVar, cast
from uuid import UUID

import attrs
import structlog
from pydantic import BaseModel, ConfigDict, Field, TypeAdapter

from airflow.sdk._shared.module_loading import UNUSUAL_MODULE_PREFIX, accepts_context, accepts_keyword_args
from airflow.sdk._shared.observability.metrics import stats
from airflow.sdk.api.client import ServerResponseError, callback_request_budget
from airflow.sdk.api.datamodels._generated import (
    BundleInfo,
    CallbackTerminalState,
    DagCallbackContext,
    EmailCallbackContext,
    TaskCallbackContext,
)
from airflow.sdk.execution_time.comms import (
    DeleteVariable,
    GetAssetsByAlias,
    GetConnection,
    GetPreviousDagRun,
    GetPreviousTI,
    GetPrevSuccessfulDagRun,
    GetTaskStates,
    GetTICount,
    GetVariable,
    GetVariableKeys,
    GetXCom,
    GetXComCount,
    GetXComSequenceItem,
    GetXComSequenceSlice,
    MaskSecret,
    PrevSuccessfulDagRunResult,
    PutVariable,
    ToSupervisor,
    ToTask,
    _ResponseFrame,
)
from airflow.sdk.execution_time.supervisor import (
    MIN_HEARTBEAT_INTERVAL,
    SOCKET_CLEANUP_TIMEOUT,
    ResponseSent,
    WatchedSubprocess,
    _CallbackIOThread,
    _ensure_client,
    _has_live_callback_io,
    _import_task_runner_before_fork,
    _make_process_nondumpable,
    _register_request_handler,
    _should_use_exec,
)

if TYPE_CHECKING:
    from typing import Self

    from structlog.typing import FilteringBoundLogger

    from airflow.sdk.api.client import Client
    from airflow.sdk.execution_time.supervisor import RequestHandler, RequestResult

    # Core (airflow.executors.workloads.base.BundleInfo) and SDK (airflow.sdk.api.datamodels._generated.BundleInfo)
    # are structurally identical, but MyPy treats them as different types. This Protocol makes MyPy happy.
    class _BundleInfoLike(Protocol):
        name: str
        version: str | None
        version_data: dict[str, Any] | None


__all__ = ["CallbackSubprocess", "supervise_callback"]

log: FilteringBoundLogger = structlog.get_logger(logger_name="callback_supervisor")


# The set of messages that a callback subprocess can send to the supervisor: the requests a Dag
# processor answers for the callbacks it runs, so a Dag or task callback has the same context on a
# worker, plus asset alias resolution for the callback context and MaskSecret for the secrets masker.
_CALLBACK_REQUEST_TYPES: tuple[type[BaseModel], ...] = (
    DeleteVariable,
    GetAssetsByAlias,
    GetConnection,
    GetPrevSuccessfulDagRun,
    GetPreviousDagRun,
    GetPreviousTI,
    GetTICount,
    GetTaskStates,
    GetVariable,
    GetVariableKeys,
    GetXCom,
    GetXComCount,
    GetXComSequenceItem,
    GetXComSequenceSlice,
    MaskSecret,
    PutVariable,
)
CallbackToSupervisor = Annotated[
    DeleteVariable
    | GetAssetsByAlias
    | GetConnection
    | GetPrevSuccessfulDagRun
    | GetPreviousDagRun
    | GetPreviousTI
    | GetTICount
    | GetTaskStates
    | GetVariable
    | GetVariableKeys
    | GetXCom
    | GetXComCount
    | GetXComSequenceItem
    | GetXComSequenceSlice
    | MaskSecret
    | PutVariable,
    Field(discriminator="type"),
]


def _add_xcom_pull_to_context(callback_kwargs: dict) -> None:
    """
    Add an ``xcom_pull`` helper bound to the callback's Dag run to its context.

    Callbacks have no task instance, so the helper takes ``dag_id`` and ``dag_run_id`` from the
    ``dag_run`` in the context and the callback only names the task to pull from.
    """
    context = callback_kwargs.get("context")
    if not isinstance(context, dict):
        return
    dag_run = context.get("dag_run")
    if not isinstance(dag_run, dict) or not dag_run.get("dag_id") or not dag_run.get("dag_run_id"):
        log.warning("Callback context has no Dag run, so xcom_pull is not available")
        return

    def xcom_pull(
        task_id: str, *, key: str = "return_value", map_index: int | None = None, default: Any = None
    ) -> Any:
        from airflow.sdk.execution_time.xcom import XCom

        value = XCom.get_one(
            key=key,
            dag_id=dag_run["dag_id"],
            run_id=dag_run["dag_run_id"],
            task_id=task_id,
            map_index=map_index,
        )
        return default if value is None else value

    context["xcom_pull"] = xcom_pull


class _UnsupportedRequest(BaseModel):
    """
    A request a callback can't make, decoded so it gets an error response.

    The child waits for a response to every request, so one dropped as undecodable would block the
    callback until its timeout.
    """

    model_config = ConfigDict(extra="allow")

    type: str


SERDE_KWARGS_ENCODING = "serde"
DAG_ATTRIBUTE_FETCH_METHOD = "dag_attribute"


class CallbackStartupDetails(BaseModel):
    dag_rel_path: Path
    bundle_info: BundleInfo | None = None
    context: (
        Annotated[
            DagCallbackContext | TaskCallbackContext | EmailCallbackContext, Field(discriminator="type")
        ]
        | None
    ) = None
    callback_path: str = ""
    callback_kwargs: dict[str, Any] = Field(default_factory=dict)
    callback_kwargs_encoding: str | None = None
    type: Literal["CallbackStartupDetails"] = "CallbackStartupDetails"


def _run_callback_entrypoint() -> None:
    from airflow.sdk.execution_time import task_runner
    from airflow.sdk.execution_time.comms import CommsDecoder

    _log = structlog.get_logger(logger_name="callback_runner")
    comms = CommsDecoder[CallbackStartupDetails, CallbackToSupervisor](
        body_decoder=TypeAdapter(CallbackStartupDetails), log=_log
    )
    msg = comms._get_response()
    if not isinstance(msg, CallbackStartupDetails):
        raise RuntimeError(f"Required first message to be CallbackStartupDetails, got {type(msg).__name__}")
    task_runner.SUPERVISOR_COMMS = CommsDecoder[ToTask, ToSupervisor](socket=comms.socket, log=_log)
    if msg.context is not None:
        _run_dag_callback(msg.context, msg.dag_rel_path, msg.bundle_info, _log)
    else:
        _run_import_callback(
            msg.callback_path,
            msg.callback_kwargs,
            msg.callback_kwargs_encoding,
            msg.dag_rel_path,
            msg.bundle_info,
            _log,
        )


def _run_dag_callback(
    context: DagCallbackContext | TaskCallbackContext | EmailCallbackContext,
    dag_rel_path: os.PathLike[str],
    bundle_info: _BundleInfoLike | None,
    log: FilteringBoundLogger,
) -> None:
    from airflow.sdk.execution_time.dag_attribute_callbacks import run_dag_attribute_callbacks

    if bundle_info is None:
        log.error("A Dag-attribute callback needs the bundle of its Dag")
        sys.exit(1)
    if errors := run_dag_attribute_callbacks(
        context, dag_rel_path=dag_rel_path, bundle_info=cast("BundleInfo", bundle_info), log=log
    ):
        log.error("Callback failed", errors=errors)
        sys.exit(1)


def _run_import_callback(
    callback_path: str,
    callback_kwargs: dict,
    callback_kwargs_encoding: str | None,
    dag_rel_path: os.PathLike[str],
    bundle_info: _BundleInfoLike | None,
    log: FilteringBoundLogger,
) -> None:
    bundle_path = None

    # If bundle info is provided, initialize the bundle and ensure its path is importable.
    # This is needed for user-defined callbacks that live inside a DAG bundle rather than
    # in an installed package or the plugins directory.
    if bundle_info and bundle_info.name:
        try:
            from airflow.dag_processing.bundles.manager import DagBundlesManager  # noqa: SDK002

            bundle = DagBundlesManager().get_bundle(
                name=bundle_info.name,
                version=bundle_info.version,
                version_data=bundle_info.version_data,
            )
            bundle.initialize()
            if (bundle_path := str(bundle.path)) not in sys.path:
                sys.path.append(bundle_path)
                log.debug("Added bundle path to sys.path", bundle_name=bundle_info.name, path=bundle_path)
        except Exception:
            log.warning(
                "Failed to initialize DAG bundle for callback",
                bundle_name=bundle_info.name,
                exc_info=True,
            )

    _add_xcom_pull_to_context(callback_kwargs)
    success, error_msg = execute_callback(
        callback_path=callback_path,
        callback_kwargs=callback_kwargs,
        dag_rel_path=dag_rel_path,
        bundle_path=bundle_path,
        log=log,
        callback_kwargs_encoding=callback_kwargs_encoding,
    )
    if not success:
        log.error("Callback failed", error=error_msg)
        sys.exit(1)


def decode_callback_kwargs(callback_kwargs: dict[str, Any], encoding: str | None) -> dict[str, Any]:
    """
    Decode the kwargs of a callback, right before the callback is called.

    ``encoding`` is ``None`` for kwargs that are passed as they are (callbacks stored by
    earlier versions), or ``"serde"`` for serde-encoded kwargs. ``context`` is added by the
    scheduler and is never encoded.
    """
    if encoding is None:
        return callback_kwargs
    if encoding != SERDE_KWARGS_ENCODING:
        raise ValueError(f"Unsupported callback kwargs encoding: {encoding!r}")

    from airflow.sdk.serde import deserialize

    return {key: value if key == "context" else deserialize(value) for key, value in callback_kwargs.items()}


def execute_callback(
    callback_path: str,
    callback_kwargs: dict,
    dag_rel_path: os.PathLike[str],
    bundle_path: os.PathLike[str] | str | None,
    log,
    callback_kwargs_encoding: str | None = None,
) -> tuple[bool, str | None]:
    """
    Execute a callback function by importing and calling it, returning the success state.

    Supports two patterns:
    1. Functions - called directly with kwargs
    2. Classes that return callable instances (like BaseNotifier) - instantiated then called with context

    Example:
        # Function callback
        execute_callback("my_module.alert_func", {"msg": "Alert!", "context": {...}}, log)

        # Notifier callback
        execute_callback("airflow.providers.slack...SlackWebhookNotifier", {"text": "Alert!"}, log)

    :param callback_path: Dot-separated import path to the callback function or class.
    :param callback_kwargs: Keyword arguments to pass to the callback.
    :param dag_rel_path: Relative path to the DAG file.
    :param bundle_path: Path to the bundle file.
    :param log: Logger instance for recording execution.
    :param callback_kwargs_encoding: How ``callback_kwargs`` is encoded, see :func:`decode_callback_kwargs`.
    :return: Tuple of (success: bool, error_message: str | None)
    """
    if not callback_path:
        return False, "Callback path not found."

    try:
        # Import the callback callable
        # Expected format: "module.path.to.function_or_class"
        module_path, function_name = callback_path.rsplit(".", 1)
        # If the callback is defined within the Dag module, the module path is modified during DAG serialization.
        # Attempt to import it using the path of the Dag file.
        if module_path.startswith(UNUSUAL_MODULE_PREFIX):
            if not dag_rel_path:
                return False, "Dag relative path not found."
            if not bundle_path:
                return False, "Bundle path not found."
            abs_path = Path(bundle_path) / Path(dag_rel_path)
            spec = spec_from_file_location(module_path, abs_path)
            if spec is None:
                return False, f"Could not create module spec for {module_path}"
            if spec.loader is None:
                return False, f"Module spec has no loader for {module_path}"
            module = module_from_spec(spec)
            spec.loader.exec_module(module)
        else:
            module = import_module(module_path)
        callback_callable = getattr(module, function_name)
        callback_kwargs = decode_callback_kwargs(callback_kwargs, callback_kwargs_encoding)

        log.debug("Executing callback", callback_path=callback_path, callback_kwargs=callback_kwargs)

        kwargs_without_context = {k: v for k, v in callback_kwargs.items() if k != "context"}

        # Call the callable with all kwargs if it accepts context, otherwise strip context.
        if accepts_context(callback_callable):
            result = callback_callable(**callback_kwargs)
        else:
            result = callback_callable(**kwargs_without_context)

        # If the callback was a class then it is now instantiated and callable, call it.
        # The constructor already received the full kwargs above; the __call__ method
        # typically only needs context (e.g. BaseNotifier.__call__(self, *args)).
        # Some callables (like BaseNotifier.__call__) only accept positional args,
        # so check the signature first rather than catching a broad TypeError.
        if callable(result):
            context = callback_kwargs.get("context", {})
            if accepts_keyword_args(result):
                result = result(context=context) if accepts_context(result) else result()
            else:
                # Positional-only callable (e.g. BaseNotifier.__call__(self, *args))
                result = result(context)

        log.info("Callback executed successfully", callback_path=callback_path)
        return True, None

    except Exception as e:
        error_msg = f"Callback execution failed: {type(e).__name__}: {str(e)}"
        log.exception(
            "Callback execution failed",
            callback_path=callback_path,
            callback_kwargs=callback_kwargs,
            error_msg=error_msg,
        )
        return False, error_msg


def _handle_previous_successful_dag_run_request(
    process: CallbackSubprocess, msg: GetPrevSuccessfulDagRun, log: FilteringBoundLogger, req_id: int
) -> RequestResult:
    # A task asks the task-instance endpoint for the task instance its process id names; a callback's
    # process id names the callback, so it asks the callback endpoint.
    response = process.client.callbacks.get_previous_successful_dagrun(process.id)
    return PrevSuccessfulDagRunResult.from_dagrun_response(response), {"exclude_unset": True}


def _check_callback_start_deadline(deadline: float | None, timeout: float | None) -> None:
    if deadline is not None and time.monotonic() >= deadline:
        stats.incr("callback.timeouts")
        raise TimeoutError(f"Callback timed out after {timeout} seconds during startup")


@attrs.define
class _CallbackHeartbeat:
    """Keep a deadline callback's worker lease alive even when it has no execution time limit."""

    timeout: int
    last_success: float
    last_attempt: float

    def renew_if_due(
        self, client: Client, callback_id: UUID, *, execution_deadline: float | None = None
    ) -> float:
        """Renew if needed and return how long the supervisor can wait for the next attempt."""
        interval = min(max(1, MIN_HEARTBEAT_INTERVAL), self.timeout / 3)
        now = time.monotonic()
        if (remaining := self.last_attempt + interval - now) > 0:
            return remaining

        timeout = self.last_success + self.timeout - now
        if timeout <= 0:
            raise TimeoutError("Lost the callback worker heartbeat")
        if execution_deadline is not None:
            timeout = min(timeout, execution_deadline - now)
            if timeout <= 0:
                return 0
        try:
            client.callbacks.heartbeat(callback_id, timeout=timeout)
        except Exception as e:
            if isinstance(e, ServerResponseError) and e.response.status_code in (
                HTTPStatus.UNAUTHORIZED,
                HTTPStatus.FORBIDDEN,
                HTTPStatus.NOT_FOUND,
                HTTPStatus.CONFLICT,
                HTTPStatus.GONE,
            ):
                raise
            if time.monotonic() - self.last_success >= self.timeout:
                raise TimeoutError("Lost the callback worker heartbeat") from e
            log.warning("Failed to heartbeat deadline callback", callback_id=callback_id, exc_info=True)
        else:
            self.last_success = now
        self.last_attempt = now
        return max(0.0, now + interval - time.monotonic())


_T = TypeVar("_T")


@attrs.define
class _CallbackIO:
    """Keep supervision on the calling thread while a blocking callback API or setup operation runs."""

    client: Client
    callback_id: UUID
    heartbeat: _CallbackHeartbeat | None
    deadline: float | None
    startup_timeout: float | None = None
    interrupted: Exception | None = None

    def _expire(self) -> TimeoutError:
        if self.startup_timeout is not None:
            stats.incr("callback.timeouts")
            return TimeoutError(f"Callback timed out after {self.startup_timeout} seconds during startup")
        return TimeoutError("Callback execution deadline passed")

    def call(
        self,
        operation: Callable[[], _T],
        *,
        discard: Callable[[_T], None] | None = None,
        reporting: bool = False,
    ) -> _T:
        if self.deadline is None and self.heartbeat is None:
            return operation()

        if self.deadline is not None and self.deadline <= time.monotonic():
            self.interrupted = self._expire()
            raise self.interrupted

        result: Future[_T] = Future()
        cancelled = Event()

        def run():
            token = callback_request_budget.set((self.deadline, cancelled))
            try:
                result.set_result(operation())
            except BaseException as e:
                result.set_exception(e)
            finally:
                callback_request_budget.reset(token)

        # A transport may still be inside a system call at the deadline. Its response is discarded
        # and cancelled prevents subsequent attempts. Only the supervisor may start user code.
        worker = _CallbackIOThread(target=copy_context().run, args=(run,), name="callback-io", daemon=True)
        worker.start()
        try:
            while not result.done():
                remaining = None if self.deadline is None else self.deadline - time.monotonic()
                if remaining is not None and remaining <= 0:
                    raise self._expire()
                if self.heartbeat is not None:
                    try:
                        due = self.heartbeat.renew_if_due(
                            self.client, self.callback_id, execution_deadline=self.deadline
                        )
                    except Exception:
                        if not reporting:
                            raise
                        # The server can acknowledge the terminal write before its HTTP response arrives.
                        worker.join()
                        break
                    remaining = due if remaining is None else min(remaining, due)
                worker.join(remaining)
            worker.join()  # Join before the supervisor forks a callback subprocess.
        except Exception as e:
            self.interrupted = e
            cancelled.set()
            if discard is not None:

                def discard_late_result(done: Future[_T]) -> None:
                    if done.exception() is None:
                        discard(done.result())

                result.add_done_callback(discard_late_result)
            raise
        try:
            return result.result()
        except TimeoutError:
            if self.deadline is not None and time.monotonic() >= self.deadline:
                self.interrupted = self._expire()
                raise self.interrupted from None
            raise


@attrs.define(kw_only=True)
class CallbackSubprocess(WatchedSubprocess):
    """
    Supervised subprocess for executing callbacks.

    Uses the WatchedSubprocess infrastructure for fork/monitor/signal handling
    while keeping a simple lifecycle: start, run callback, exit.

    Provides the comms channels a Dag processor gives the callbacks it runs (Connections,
    Variables, XComs, and task and Dag run state) so that callback code can access runtime
    services like ``Connection.get()`` and ``Variable.get()`` via the supervisor's API client.
    """

    client: Client  # The HTTP client to use for communication with the API server.
    execution_timeout: float | None = None
    """Seconds the callback may run; ``None`` reads ``[callbacks] callback_execution_timeout``."""
    execution_deadline: float | None = None
    """Monotonic deadline shared with routed callback setup, including the token exchange."""
    heartbeat: _CallbackHeartbeat | None = None
    is_dag_attribute: bool = False
    timed_out: bool = attrs.field(default=False, init=False)
    _io: _CallbackIO | None = attrs.field(default=None, init=False)

    decoder: ClassVar[TypeAdapter[CallbackToSupervisor | _UnsupportedRequest]] = TypeAdapter(
        Annotated[CallbackToSupervisor | _UnsupportedRequest, Field(union_mode="left_to_right")]
    )
    _request_handlers: ClassVar[dict[type[BaseModel], RequestHandler[Any]]] = dict(
        [
            *WatchedSubprocess._get_shared_request_handlers(*_CALLBACK_REQUEST_TYPES).items(),
            _register_request_handler(GetPrevSuccessfulDagRun, _handle_previous_successful_dag_run_request),
        ]
    )

    def _handle_request(self, msg, log: FilteringBoundLogger, req_id: int) -> None:
        if not self.is_dag_attribute and not isinstance(
            msg, (GetConnection, GetVariable, GetVariableKeys, GetXCom, MaskSecret)
        ):
            self._reject_request(msg, log, req_id)
            return
        super()._handle_request(msg, log, req_id)

    def _call_request_handler(
        self, handler: RequestHandler[Any], msg: BaseModel, log: FilteringBoundLogger, req_id: int
    ) -> RequestResult | ResponseSent:
        if self._io is None:
            return super()._call_request_handler(handler, msg, log, req_id)
        if self._io.interrupted is not None:
            return ResponseSent.ALREADY_SENT
        try:
            result = self._io.call(partial(handler, self, msg, log, req_id))
            if self._io.deadline is not None and time.monotonic() >= self._io.deadline:
                self._io.interrupted = TimeoutError("Callback execution deadline passed")
                return ResponseSent.ALREADY_SENT
            return result
        except Exception:
            if self._io.interrupted is None:
                raise
            # The monitor will terminate the child. Returning an IPC error would let it resume.
            return ResponseSent.ALREADY_SENT

    @classmethod
    def start(  # type: ignore[override]
        cls,
        *,
        id: str,
        callback_path: str,
        callback_kwargs: dict,
        callback_kwargs_encoding: str | None = None,
        dag_rel_path: os.PathLike[str],
        bundle_info: _BundleInfoLike | None = None,
        context: DagCallbackContext | TaskCallbackContext | EmailCallbackContext | None = None,
        client: Client,
        logger: FilteringBoundLogger | None = None,
        **kwargs,
    ) -> Self:
        """
        Start a new subprocess to execute the given callback.

        With a ``context``, the callback is a Dag or task attribute: the subprocess parses the Dag
        file at ``bundle_info`` and runs the callbacks the context names instead of ``callback_path``.
        """
        callback_id = UUID(id) if not isinstance(id, UUID) else id
        if context is not None:
            kwargs["is_dag_attribute"] = True
        deadline = kwargs.get("execution_deadline")
        startup_io = _CallbackIO(
            client, callback_id, kwargs.get("heartbeat"), deadline, kwargs.get("execution_timeout")
        )
        if (context is not None and _should_use_exec()) or _has_live_callback_io():
            msg = CallbackStartupDetails(
                dag_rel_path=Path(dag_rel_path),
                bundle_info=BundleInfo.model_validate(bundle_info, from_attributes=True)
                if bundle_info
                else None,
                context=context,
                callback_path=callback_path if context is None else "",
                callback_kwargs=callback_kwargs if context is None else {},
                callback_kwargs_encoding=callback_kwargs_encoding if context is None else None,
            )
            # Serialize before spawning so an invalid payload cannot leave a child waiting for startup.
            startup_frame = startup_io.call(lambda: _ResponseFrame(id=0, body=msg.model_dump()).as_bytes())
            _check_callback_start_deadline(kwargs.get("execution_deadline"), kwargs.get("execution_timeout"))
            proc = super().start(
                id=callback_id,
                client=client,
                target=_run_callback_entrypoint,
                logger=logger,
                use_exec=True,
                new_process_group=True,
                **kwargs,
            )
            try:
                if deadline is not None:
                    proc.stdin.settimeout(max(0.0, deadline - time.monotonic()))
                proc.stdin.sendall(startup_frame)
                if deadline is not None:
                    proc.stdin.settimeout(None)
            except Exception:
                expired = deadline is not None and time.monotonic() >= deadline
                proc.kill(signal.SIGKILL)
                proc._cleanup_open_sockets()
                proc.selector.close()
                if expired:
                    raise startup_io._expire() from None
                raise
            return proc

        # Use a closure to pass callback data to the child process.  Note that this
        # ONLY works because WatchedSubprocess.start() uses os.fork(), so the child
        # inherits the parent's memory space and the variables are available directly.
        def _target():
            from airflow.sdk.execution_time import task_runner
            from airflow.sdk.execution_time.comms import CommsDecoder, ToTask

            _log = structlog.get_logger(logger_name="callback_runner")
            task_runner.SUPERVISOR_COMMS = CommsDecoder[ToTask, CallbackToSupervisor](log=_log)

            if context is not None:
                _run_dag_callback(context, dag_rel_path, bundle_info, _log)
                return

            _run_import_callback(
                callback_path, callback_kwargs, callback_kwargs_encoding, dag_rel_path, bundle_info, _log
            )

        startup_io.call(_import_task_runner_before_fork)
        _check_callback_start_deadline(kwargs.get("execution_deadline"), kwargs.get("execution_timeout"))
        return super().start(
            id=callback_id,
            client=client,
            target=_target,
            new_process_group=True,
            logger=logger,
            **kwargs,
        )

    def wait(self) -> int:
        """
        Wait for the callback subprocess to complete.

        Deadline callbacks renew their worker heartbeat while the child runs.
        """
        if self._exit_code is not None:
            return self._exit_code

        try:
            self._monitor_subprocess()
        finally:
            self.selector.close()

        self._exit_code = self._exit_code if self._exit_code is not None else 1
        return self._exit_code

    def _get_callback_execution_timeout(self) -> float:
        """Return the timeout the scheduler stamped on the callback, or the configured one."""
        if self.execution_timeout is not None:
            return self.execution_timeout

        from airflow.sdk.configuration import conf

        return conf.getint("callbacks", "callback_execution_timeout", fallback=0)

    def _upload_logs(self):
        from airflow.sdk.execution_time.supervisor import _remote_logging_conn
        from airflow.sdk.log import upload_to_remote

        try:
            with _remote_logging_conn(self.client):
                upload_to_remote(self.process_log)
        except Exception:
            log.exception(
                "Failed to upload callback logs to remote storage", callback_id=self.id, pid=self.pid
            )

    def _terminate_callback(self) -> None:
        # kill() stops escalating when the child exits. Descendants may still be in its process
        # group (e.g. a helper ignoring SIGTERM); finish that group when the callback loses ownership.
        pgid = None
        if self._new_process_group:
            with suppress(ProcessLookupError):
                if os.getpgid(self.pid) == self.pid:
                    pgid = self.pid
        self.kill(signal.SIGTERM, escalation_delay=5.0, force=True)
        if pgid is not None:
            with suppress(ProcessLookupError):
                os.killpg(pgid, signal.SIGKILL)

    def _monitor_subprocess(self):
        """
        Monitor the subprocess until it exits.

        If ``[callbacks] callback_execution_timeout`` is set to a positive value, the subprocess
        is killed after that many seconds. Routed callbacks retain the deadline from worker setup.
        """
        timeout = self._get_callback_execution_timeout()
        deadline = self.execution_deadline
        if deadline is None and timeout > 0:
            deadline = time.monotonic() + timeout

        self._io = _CallbackIO(self.client, self.id, self.heartbeat, deadline)
        while self._exit_code is None or self._open_sockets:
            max_wait_time: float = MIN_HEARTBEAT_INTERVAL
            if self.heartbeat is not None:
                try:
                    max_wait_time = min(
                        max_wait_time,
                        self.heartbeat.renew_if_due(
                            self.client,
                            self.id,
                            execution_deadline=deadline if self._exit_code is None else None,
                        ),
                    )
                except Exception:
                    self._terminate_callback()
                    raise
            if deadline is not None and self._exit_code is None:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    if self._check_subprocess_exit() is not None:
                        continue
                    log.warning(
                        "Callback execution timeout exceeded; terminating subprocess",
                        pid=self.pid,
                        timeout_seconds=timeout,
                        elapsed_seconds=timeout - remaining,
                    )
                    self._terminate_callback()
                    self.timed_out = True
                    break
                max_wait_time = min(max_wait_time, remaining)

            self._service_subprocess(max_wait_time=max_wait_time)
            if self._io.interrupted is not None:
                self._terminate_callback()
                if deadline is not None and time.monotonic() >= deadline:
                    self.timed_out = True
                    break
                raise self._io.interrupted

            # If the process has exited but sockets remain open, apply a timeout
            # to prevent hanging indefinitely on stuck sockets.
            if self._exit_code is not None and self._open_sockets:
                if (
                    self._process_exit_monotonic
                    and time.monotonic() - self._process_exit_monotonic > SOCKET_CLEANUP_TIMEOUT
                ):
                    log.warning(
                        "Process exited with open sockets; cleaning up after timeout",
                        pid=self.pid,
                        exit_code=self._exit_code,
                        socket_types=list(self._open_sockets.values()),
                        timeout_seconds=SOCKET_CLEANUP_TIMEOUT,
                    )
                    self._cleanup_open_sockets()


def _configure_logging(log_path: str, client: Client) -> tuple[FilteringBoundLogger, BinaryIO]:
    """Configure file-based logging for the callback subprocess."""
    from airflow.sdk.execution_time.supervisor import _remote_logging_conn
    from airflow.sdk.log import init_log_file, logging_processors

    log_file = init_log_file(log_path)
    log_file_descriptor: BinaryIO = log_file.open("ab")
    try:
        underlying_logger = structlog.BytesLogger(log_file_descriptor)
        with _remote_logging_conn(client):
            processors = logging_processors(json_output=True)
        logger = structlog.wrap_logger(
            underlying_logger, processors=processors, logger_name="callback"
        ).bind()
    except BaseException:
        log_file_descriptor.close()
        raise

    return logger, log_file_descriptor


def _report_callback_state(
    client: Client, callback_id: str, state: CallbackTerminalState, output: str | None = None
) -> bool:
    try:
        client.callbacks.finish(UUID(callback_id), state=state, output=output)
    except Exception:
        # The executor's event for the workload still records the outcome.
        log.warning(
            "Failed to report the callback state", callback_id=callback_id, state=state, exc_info=True
        )
        return False
    return True


def supervise_callback(
    *,
    id: str,
    callback_path: str,
    callback_kwargs: dict,
    dag_rel_path: os.PathLike[str],
    callback_kwargs_encoding: str | None = None,
    log_path: str | None = None,
    bundle_info: _BundleInfoLike | None = None,
    token: str = "",
    server: str | None = None,
    client: Client | None = None,
    fetch_method: str | None = None,
    execution_timeout: float | None = None,
) -> int:
    """
    Run a single callback execution to completion in a supervised subprocess.

    :param id: Unique identifier for this callback execution.
    :param callback_path: Dot-separated import path to the callback function or class.
    :param callback_kwargs: Keyword arguments to pass to the callback.
    :param dag_rel_path: Relative path to the DAG file.
    :param callback_kwargs_encoding: How ``callback_kwargs`` is encoded, see :func:`decode_callback_kwargs`.
    :param log_path: Path to write logs, if required.
    :param bundle_info: When provided, the bundle's path is added to sys.path so callbacks in Dag Bundles are importable.
    :param token: Authentication token for the API client.
    :param server: Base URL of the API server.
    :param client: Optional preconfigured client for communication with the server (mostly for tests).
    :param fetch_method: ``"dag_attribute"`` for a Dag or task callback, or an email, that the scheduler
        routed from the Dag processor: its context is read from the API server and its callbacks from
        the parsed Dag. Otherwise the callback is imported from ``callback_path``.
    :param execution_timeout: Seconds the callback may run, overriding ``[callbacks] callback_execution_timeout``.
    :return: Zero when the callback succeeded or the API acknowledged its failure.
    """
    _make_process_nondumpable()

    start = time.monotonic()
    deadline = (
        start + execution_timeout
        if fetch_method == DAG_ATTRIBUTE_FETCH_METHOD
        and execution_timeout is not None
        and execution_timeout > 0
        else None
    )

    logger: FilteringBoundLogger
    log_file_descriptor: BinaryIO | None = None

    with _ensure_client(server, token, client=client) as client:
        # Swap the single-use callback token for an execution token before any other request: the
        # callback token can't read the connection that logging setup reads for remote logging.
        callback_id = UUID(id)
        io = _CallbackIO(client, callback_id, None, deadline, execution_timeout)
        heartbeat_timeout = io.call(partial(client.callbacks.run, callback_id))
        heartbeat = (
            _CallbackHeartbeat(heartbeat_timeout, last_success=time.monotonic(), last_attempt=start)
            if heartbeat_timeout
            else None
        )

        io.heartbeat = heartbeat
        terminal_state = CallbackTerminalState.FAILED
        terminal_output: str | None = None
        supervisor_error: Exception | None = None
        process = None

        try:
            _check_callback_start_deadline(deadline, execution_timeout)
            if log_path:
                logger, log_file_descriptor = io.call(
                    partial(_configure_logging, log_path, client),
                    discard=lambda configured: configured[1].close(),
                )
            else:
                logger = structlog.get_logger(logger_name="callback").bind()

            _check_callback_start_deadline(deadline, execution_timeout)
            context = None
            if fetch_method == DAG_ATTRIBUTE_FETCH_METHOD:
                context = io.call(partial(client.callbacks.get_context, callback_id)).context
            _check_callback_start_deadline(deadline, execution_timeout)
            if heartbeat is not None:
                heartbeat.renew_if_due(client, UUID(id))
            process = CallbackSubprocess.start(
                id=id,
                callback_path=callback_path,
                callback_kwargs=callback_kwargs,
                callback_kwargs_encoding=callback_kwargs_encoding,
                dag_rel_path=dag_rel_path,
                bundle_info=bundle_info,
                context=context,
                client=client,
                logger=logger,
                subprocess_logs_to_stdout=True,
                execution_timeout=execution_timeout,
                execution_deadline=deadline,
                heartbeat=heartbeat,
            )

            exit_code = process.wait()
            end = time.monotonic()
            log.info(
                "Workload finished",
                workload_type="ExecutorCallback",
                workload_id=id,
                exit_code=exit_code,
                duration=end - start,
            )
            if process.timed_out:
                stats.incr("callback.timeouts")
                terminal_output = (
                    f"Callback timed out after {process._get_callback_execution_timeout()} seconds"
                )
            elif exit_code != 0:
                terminal_output = f"Callback subprocess exited with code {exit_code}"
            else:
                terminal_state = CallbackTerminalState.SUCCESS
        except Exception as e:
            supervisor_error = e
            terminal_output = f"Callback supervisor error: {type(e).__name__}: {e}"
            log.exception("Callback supervisor failed", callback_id=id)
        finally:
            report_io = _CallbackIO(client, callback_id, heartbeat, None)
            reported = report_io.call(
                partial(_report_callback_state, client, id, terminal_state, terminal_output), reporting=True
            )
            # Report first so a slow log upload cannot use up the scheduler's timeout grace period.
            if process is not None:
                process._upload_logs()

            if log_path and log_file_descriptor:
                log_file_descriptor.close()

    # Executors such as ECS and Batch retry a nonzero exit as a launch failure. Once the API
    # acknowledges the outcome, the executor's task is complete even if the callback failed.
    if terminal_state == CallbackTerminalState.FAILED and not reported:
        raise supervisor_error or RuntimeError(terminal_output)
    return 0
