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

import sys
import time
from importlib import import_module
from typing import TYPE_CHECKING, BinaryIO, ClassVar, Protocol
from uuid import UUID

import attrs
import structlog
from pydantic import TypeAdapter

from airflow.sdk._shared.module_loading import accepts_context, accepts_keyword_args
from airflow.sdk.execution_time.supervisor import (
    MIN_HEARTBEAT_INTERVAL,
    SOCKET_CLEANUP_TIMEOUT,
    WatchedSubprocess,
    _make_process_nondumpable,
)

if TYPE_CHECKING:
    from structlog.typing import FilteringBoundLogger
    from typing_extensions import Self

    # Core (airflow.executors.workloads.base.BundleInfo) and SDK (airflow.sdk.api.datamodels._generated.BundleInfo)
    # are structurally identical, but MyPy treats them as different types. This Protocol makes MyPy happy.
    class _BundleInfoLike(Protocol):
        name: str
        version: str | None


__all__ = ["CallbackSubprocess", "supervise_callback"]

log: FilteringBoundLogger = structlog.get_logger(logger_name="callback_supervisor")


def execute_callback(
    callback_path: str,
    callback_kwargs: dict,
    log,
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
    :param log: Logger instance for recording execution.
    :return: Tuple of (success: bool, error_message: str | None)
    """
    if not callback_path:
        return False, "Callback path not found."

    try:
        # Import the callback callable
        # Expected format: "module.path.to.function_or_class"
        module_path, function_name = callback_path.rsplit(".", 1)
        module = import_module(module_path)
        callback_callable = getattr(module, function_name)

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


# An empty message set; the callback subprocess doesn't currently communicate back to the
# supervisor. This means callback code cannot access runtime services like Connection.get()
# or Variable.get() which require the supervisor to pass requests to the API server.
# To enable this, add the needed message types here and implement _handle_request accordingly.
# See ActivitySubprocess.decoder in supervisor.py for the full task message set and examples.
_EmptyMessage: TypeAdapter[None] = TypeAdapter(None)


@attrs.define(kw_only=True)
class CallbackSubprocess(WatchedSubprocess):
    """
    Supervised subprocess for executing callbacks.

    Uses the WatchedSubprocess infrastructure for fork/monitor/signal handling
    while keeping a simple lifecycle: start, run callback, exit.
    """

    decoder: ClassVar[TypeAdapter] = _EmptyMessage

    @classmethod
    def start(  # type: ignore[override]
        cls,
        *,
        id: str,
        callback_path: str,
        callback_kwargs: dict,
        bundle_info: _BundleInfoLike | None = None,
        logger: FilteringBoundLogger | None = None,
        **kwargs,
    ) -> Self:
        """Fork and start a new subprocess to execute the given callback."""

        # Use a closure to pass callback data to the child process.  Note that this
        # ONLY works because WatchedSubprocess.start() uses os.fork(), so the child
        # inherits the parent's memory space and the variables are available directly.
        def _target():
            _log = structlog.get_logger(logger_name="callback_runner")

            # If bundle info is provided, initialize the bundle and ensure its path is importable.
            # This is needed for user-defined callbacks that live inside a DAG bundle rather than
            # in an installed package or the plugins directory.
            if bundle_info and bundle_info.name:
                try:
                    from airflow.dag_processing.bundles.manager import DagBundlesManager

                    bundle = DagBundlesManager().get_bundle(
                        name=bundle_info.name,
                        version=bundle_info.version,
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

            success, error_msg = execute_callback(callback_path, callback_kwargs, _log)
            if not success:
                _log.error("Callback failed", error=error_msg)
                sys.exit(1)

        return super().start(
            id=UUID(id) if not isinstance(id, UUID) else id,
            target=_target,
            logger=logger,
            **kwargs,
        )

    def wait(self) -> int:
        """
        Wait for the callback subprocess to complete.

        Mirrors the structure of ActivitySubprocess.wait() but without heartbeating,
        task API state management, or log uploading.
        """
        if self._exit_code is not None:
            return self._exit_code

        try:
            self._monitor_subprocess()
        finally:
            self.selector.close()

        self._exit_code = self._exit_code if self._exit_code is not None else 1
        return self._exit_code

    def _monitor_subprocess(self):
        """
        Monitor the subprocess until it exits.

        A simplified version of ActivitySubprocess._monitor_subprocess() without heartbeating
        or timeout handling, just process output monitoring and stuck-socket cleanup.
        """
        while self._exit_code is None or self._open_sockets:
            self._service_subprocess(max_wait_time=MIN_HEARTBEAT_INTERVAL)

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

    def _handle_request(self, msg, log: FilteringBoundLogger, req_id: int) -> None:
        """Handle incoming requests from the callback subprocess (currently none expected)."""
        log.warning("Unexpected request from callback subprocess", msg=msg)


def _configure_logging(log_path: str) -> tuple[FilteringBoundLogger, BinaryIO]:
    """Configure file-based logging for the callback subprocess."""
    from airflow.sdk.log import init_log_file, logging_processors

    log_file = init_log_file(log_path)
    log_file_descriptor: BinaryIO = log_file.open("ab")
    underlying_logger = structlog.BytesLogger(log_file_descriptor)
    processors = logging_processors(json_output=True)
    logger = structlog.wrap_logger(underlying_logger, processors=processors, logger_name="callback").bind()

    return logger, log_file_descriptor


def supervise_callback(
    *,
    id: str,
    callback_path: str,
    callback_kwargs: dict,
    log_path: str | None = None,
    bundle_info: _BundleInfoLike | None = None,
) -> int:
    """
    Run a single callback execution to completion in a supervised subprocess.

    :param id: Unique identifier for this callback execution.
    :param callback_path: Dot-separated import path to the callback function or class.
    :param callback_kwargs: Keyword arguments to pass to the callback.
    :param log_path: Path to write logs, if required.
    :param bundle_info: When provided, the bundle's path is added to sys.path so callbacks in Dag Bundles are importable.
    :return: Exit code of the subprocess (0 = success).
    """
    _make_process_nondumpable()

    start = time.monotonic()

    logger: FilteringBoundLogger
    log_file_descriptor: BinaryIO | None = None
    if log_path:
        logger, log_file_descriptor = _configure_logging(log_path)
    else:
        # When no log file is requested, still use a callback-specific logger
        # so logs are clearly separated from task logs.
        logger = structlog.get_logger(logger_name="callback").bind()

    try:
        process = CallbackSubprocess.start(
            id=id,
            callback_path=callback_path,
            callback_kwargs=callback_kwargs,
            bundle_info=bundle_info,
            logger=logger,
            subprocess_logs_to_stdout=True,
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
        if exit_code != 0:
            raise RuntimeError(f"Callback subprocess exited with code {exit_code}")
        return exit_code
    finally:
        if log_path and log_file_descriptor:
            log_file_descriptor.close()
