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
import time
from importlib import import_module
from typing import TYPE_CHECKING, BinaryIO, ClassVar

import attrs
import structlog
from pydantic import TypeAdapter

from airflow.sdk.execution_time.supervisor import WatchedSubprocess

if TYPE_CHECKING:
    from collections.abc import Callable

    from structlog.typing import FilteringBoundLogger
    from typing_extensions import Self

__all__ = ["CallbackSubprocess", "execute_callback", "supervise_callback"]

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

        log.debug("Executing callback %s(%s)...", callback_path, callback_kwargs)

        # If the callback is a callable, call it.  If it is a class, instantiate it.
        result = callback_callable(**callback_kwargs)

        # If the callback is a class then it is now instantiated and callable, call it.
        if callable(result):
            context = callback_kwargs.get("context", {})
            log.debug("Calling result with context for %s", callback_path)
            result = result(context)

        log.info("Callback %s executed successfully.", callback_path)
        return True, None

    except Exception as e:
        error_msg = f"Callback execution failed: {type(e).__name__}: {str(e)}"
        log.exception("Callback %s(%s) execution failed: %s", callback_path, callback_kwargs, error_msg)
        return False, error_msg


def _callback_subprocess_main():
    """
    Entry point for the callback subprocess, runs after fork.

    Reads the callback path and kwargs from environment variables,
    executes the callback, and exits with an appropriate code.
    """
    import json
    import sys

    log = structlog.get_logger(logger_name="callback_runner")

    callback_path = os.environ.get("_AIRFLOW_CALLBACK_PATH", "")
    callback_kwargs_json = os.environ.get("_AIRFLOW_CALLBACK_KWARGS", "{}")

    if not callback_path:
        print("No callback path found in environment", file=sys.stderr)
        sys.exit(1)

    try:
        callback_kwargs = json.loads(callback_kwargs_json)
    except Exception:
        log.exception("Failed to deserialize callback kwargs")
        sys.exit(1)

    success, error_msg = execute_callback(callback_path, callback_kwargs, log)
    if not success:
        log.error("Callback failed", error=error_msg)
        sys.exit(1)


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
        target: Callable[[], None] = _callback_subprocess_main,
        logger: FilteringBoundLogger | None = None,
        **kwargs,
    ) -> Self:
        """Fork and start a new subprocess to execute the given callback."""
        import json
        from datetime import date, datetime
        from uuid import UUID

        class _ExtendedEncoder(json.JSONEncoder):
            """Handle types that stdlib json can't serialize (UUID, datetime, etc.)."""

            def default(self, o):
                if isinstance(o, UUID):
                    return str(o)
                if isinstance(o, datetime):
                    return o.isoformat()
                if isinstance(o, date):
                    return o.isoformat()
                if hasattr(o, "__str__"):
                    return str(o)
                return super().default(o)

        # Pass the callback data to the child process via environment variables.
        # These are set before fork so the child inherits them, and cleaned up in the parent after.
        os.environ["_AIRFLOW_CALLBACK_PATH"] = callback_path
        os.environ["_AIRFLOW_CALLBACK_KWARGS"] = json.dumps(callback_kwargs, cls=_ExtendedEncoder)
        try:
            proc: Self = super().start(
                id=id,
                target=target,
                logger=logger,
                **kwargs,
            )
        finally:
            # Clean up the env vars in the parent process
            os.environ.pop("_AIRFLOW_CALLBACK_PATH", None)
            os.environ.pop("_AIRFLOW_CALLBACK_KWARGS", None)
        return proc

    def wait(self) -> int:
        """
        Wait for the callback subprocess to complete.

        A simplified monitor loop compared to ActivitySubprocess — no heartbeating,
        no task API state management. Just monitors process output and waits for exit.
        """
        if self._exit_code is not None:
            return self._exit_code

        try:
            while self._exit_code is None or self._open_sockets:
                self._service_subprocess(max_wait_time=5.0)
        finally:
            self.selector.close()

        self._exit_code = self._exit_code if self._exit_code is not None else 1
        return self._exit_code

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
) -> int:
    """
    Run a single callback execution to completion in a supervised subprocess.

    :param id: Unique identifier for this callback execution.
    :param callback_path: Dot-separated import path to the callback function or class.
    :param callback_kwargs: Keyword arguments to pass to the callback.
    :param log_path: Path to write logs, if required.
    :return: Exit code of the subprocess (0 = success).
    """
    start = time.monotonic()

    logger: FilteringBoundLogger | None = None
    log_file_descriptor: BinaryIO | None = None
    if log_path:
        logger, log_file_descriptor = _configure_logging(log_path)

    try:
        process = CallbackSubprocess.start(
            id=id,
            callback_path=callback_path,
            callback_kwargs=callback_kwargs,
            logger=logger,
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
