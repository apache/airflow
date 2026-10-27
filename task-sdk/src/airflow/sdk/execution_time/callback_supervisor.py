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
from importlib import import_module
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path
from typing import TYPE_CHECKING, Annotated, Any, BinaryIO, ClassVar, Literal, Protocol, cast
from uuid import UUID

import attrs
import structlog
from pydantic import BaseModel, ConfigDict, Field, TypeAdapter

from airflow.sdk._shared.module_loading import UNUSUAL_MODULE_PREFIX, accepts_context, accepts_keyword_args
from airflow.sdk._shared.observability.metrics import stats
from airflow.sdk.api.datamodels._generated import (
    BundleInfo,
    CallbackState,
    DagCallbackContext,
    EmailCallbackContext,
    TaskCallbackContext,
)
from airflow.sdk.execution_time.comms import (
    DeleteVariable,
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
    WatchedSubprocess,
    _ensure_client,
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
# worker, plus MaskSecret for the secrets masker.
_CALLBACK_REQUEST_TYPES: tuple[type[BaseModel], ...] = (
    DeleteVariable,
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


class DagCallbackStartupDetails(BaseModel):
    dag_rel_path: Path
    bundle_info: BundleInfo | None = None
    context: Annotated[
        DagCallbackContext | TaskCallbackContext | EmailCallbackContext, Field(discriminator="type")
    ]
    type: Literal["DagCallbackStartupDetails"] = "DagCallbackStartupDetails"


def _run_dag_callback_entrypoint() -> None:
    from airflow.sdk.execution_time import task_runner
    from airflow.sdk.execution_time.comms import CommsDecoder

    _log = structlog.get_logger(logger_name="callback_runner")
    comms = CommsDecoder[DagCallbackStartupDetails, CallbackToSupervisor](
        body_decoder=TypeAdapter(DagCallbackStartupDetails), log=_log
    )
    msg = comms._get_response()
    if not isinstance(msg, DagCallbackStartupDetails):
        raise RuntimeError(
            f"Required first message to be DagCallbackStartupDetails, got {type(msg).__name__}"
        )
    task_runner.SUPERVISOR_COMMS = CommsDecoder[ToTask, ToSupervisor](socket=comms.socket, log=_log)
    _run_dag_callback(msg.context, msg.dag_rel_path, msg.bundle_info, _log)


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
    bundle_path: os.PathLike[str] | None,
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
    timed_out: bool = attrs.field(default=False, init=False)

    decoder: ClassVar[TypeAdapter[CallbackToSupervisor | _UnsupportedRequest]] = TypeAdapter(
        Annotated[CallbackToSupervisor | _UnsupportedRequest, Field(union_mode="left_to_right")]
    )
    _request_handlers: ClassVar[dict[type[BaseModel], RequestHandler[Any]]] = dict(
        [
            *WatchedSubprocess._get_shared_request_handlers(*_CALLBACK_REQUEST_TYPES).items(),
            _register_request_handler(GetPrevSuccessfulDagRun, _handle_previous_successful_dag_run_request),
        ]
    )

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
        if context is not None and _should_use_exec():
            msg = DagCallbackStartupDetails(
                dag_rel_path=Path(dag_rel_path),
                bundle_info=BundleInfo.model_validate(bundle_info, from_attributes=True)
                if bundle_info
                else None,
                context=context,
            )
            # Serialize before spawning so an invalid payload cannot leave a child waiting for startup.
            startup_frame = _ResponseFrame(id=0, body=msg.model_dump()).as_bytes()
            proc = super().start(
                id=UUID(id) if not isinstance(id, UUID) else id,
                client=client,
                target=_run_dag_callback_entrypoint,
                logger=logger,
                use_exec=True,
                **kwargs,
            )
            try:
                proc.stdin.sendall(startup_frame)
            except Exception:
                proc.kill(signal.SIGKILL)
                proc._cleanup_open_sockets()
                proc.selector.close()
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
                        _log.debug(
                            "Added bundle path to sys.path", bundle_name=bundle_info.name, path=bundle_path
                        )
                except Exception:
                    _log.warning(
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
                log=_log,
                callback_kwargs_encoding=callback_kwargs_encoding,
            )
            if not success:
                _log.error("Callback failed", error=error_msg)
                sys.exit(1)

        _import_task_runner_before_fork()
        return super().start(
            id=UUID(id) if not isinstance(id, UUID) else id,
            client=client,
            target=_target,
            logger=logger,
            **kwargs,
        )

    def wait(self) -> int:
        """
        Wait for the callback subprocess to complete.

        Mirrors the structure of ActivitySubprocess.wait() but without heartbeating
        or task API state management.
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

    def _monitor_subprocess(self):
        """
        Monitor the subprocess until it exits.

        A simplified version of ActivitySubprocess._monitor_subprocess() without heartbeating
        or timeout handling, just process output monitoring and stuck-socket cleanup.

        If ``[callbacks] callback_execution_timeout`` is set to a positive value, the subprocess
        is killed after that many seconds.
        """
        timeout = self._get_callback_execution_timeout()
        start_monotonic = time.monotonic()

        while self._exit_code is None or self._open_sockets:
            self._service_subprocess(max_wait_time=MIN_HEARTBEAT_INTERVAL)

            # Enforce execution timeout if configured.
            if timeout > 0 and self._exit_code is None:
                elapsed = time.monotonic() - start_monotonic
                if elapsed > timeout:
                    log.warning(
                        "Callback execution timeout exceeded; terminating subprocess",
                        pid=self.pid,
                        timeout_seconds=timeout,
                        elapsed_seconds=elapsed,
                    )
                    self.kill(signal.SIGTERM, escalation_delay=5.0, force=True)
                    self.timed_out = True
                    break

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
    underlying_logger = structlog.BytesLogger(log_file_descriptor)
    with _remote_logging_conn(client):
        processors = logging_processors(json_output=True)
    logger = structlog.wrap_logger(underlying_logger, processors=processors, logger_name="callback").bind()

    return logger, log_file_descriptor


def _report_callback_state(
    client: Client, callback_id: str, state: CallbackState, output: str | None = None
) -> None:
    try:
        client.callbacks.update_state(UUID(callback_id), state, output)
    except Exception:
        # The executor's event for the workload still records the outcome.
        log.warning(
            "Failed to report the callback state", callback_id=callback_id, state=state, exc_info=True
        )


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
    :return: Exit code of the subprocess (0 = success).
    """
    _make_process_nondumpable()

    start = time.monotonic()

    logger: FilteringBoundLogger
    log_file_descriptor: BinaryIO | None = None

    with _ensure_client(server, token, client=client) as client:
        if log_path:
            logger, log_file_descriptor = _configure_logging(log_path, client)
        else:
            logger = structlog.get_logger(logger_name="callback").bind()

        # Swap the single-use callback token for an execution token before any context read.
        client.callbacks.run(UUID(id))

        try:
            context = None
            if fetch_method == DAG_ATTRIBUTE_FETCH_METHOD:
                context = client.callbacks.get_context(UUID(id)).context
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
            output = None
            if process.timed_out:
                stats.incr("callback.timeouts")
                output = f"Callback timed out after {process._get_callback_execution_timeout()} seconds"
            elif exit_code != 0:
                output = f"Callback subprocess exited with code {exit_code}"
            _report_callback_state(
                client, id, CallbackState.FAILED if output else CallbackState.SUCCESS, output
            )
            # Uploaded only once reported, as a task's are: the scheduler fails a routed callback that
            # has not reported within its timeout and a grace period, which a slow upload could use up.
            process._upload_logs()
            if output:
                raise RuntimeError(output)
            return exit_code
        finally:
            if log_path and log_file_descriptor:
                log_file_descriptor.close()
