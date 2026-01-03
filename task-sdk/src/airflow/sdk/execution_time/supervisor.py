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
"""Supervise and run Tasks in a subprocess."""

from __future__ import annotations

import atexit
import contextlib
import io
import logging
import os
import selectors
import signal
import sys
import threading
import time
import warnings
import weakref
from collections import deque
from collections.abc import Callable, Generator
from contextlib import contextmanager, suppress
from datetime import datetime, timezone
from http import HTTPStatus
from socket import socket, socketpair
from typing import TYPE_CHECKING, BinaryIO, ClassVar, NoReturn, TextIO, cast
from urllib.parse import urlparse
from uuid import UUID

import attrs
import httpx
import msgspec
import psutil
import structlog
from pydantic import BaseModel, TypeAdapter

from airflow.sdk._shared.logging.structlog import reconfigure_logger
from airflow.sdk.api.client import Client, ServerResponseError
from airflow.sdk.api.datamodels._generated import (
    AssetResponse,
    ConnectionResponse,
    TaskInstance,
    TaskInstanceState,
    TaskStatesResponse,
    VariableResponse,
    XComSequenceIndexResponse,
)
from airflow.sdk.configuration import conf
from airflow.sdk.exceptions import ErrorType
from airflow.sdk.execution_time import comms
from airflow.sdk.execution_time.comms import (
    AssetEventsResult,
    AssetResult,
    ConnectionResult,
    CreateHITLDetailPayload,
    DagRunResult,
    DagRunStateResult,
    DeferTask,
    DeleteVariable,
    DeleteXCom,
    ErrorResponse,
    GetAssetByName,
    GetAssetByUri,
    GetAssetEventByAsset,
    GetAssetEventByAssetAlias,
    GetConnection,
    GetDagRun,
    GetDagRunState,
    GetDRCount,
    GetPreviousDagRun,
    GetPreviousTI,
    GetPrevSuccessfulDagRun,
    GetTaskBreadcrumbs,
    GetTaskRescheduleStartDate,
    GetTaskStates,
    GetTICount,
    GetVariable,
    GetXCom,
    GetXComCount,
    GetXComSequenceItem,
    GetXComSequenceSlice,
    HITLDetailRequestResult,
    InactiveAssetsResult,
    MaskSecret,
    PrevSuccessfulDagRunResult,
    PutVariable,
    RescheduleTask,
    ResendLoggingFD,
    RetryTask,
    SentFDs,
    SetRenderedFields,
    SetRenderedMapIndex,
    SetXCom,
    SkipDownstreamTasks,
    StartupDetails,
    SucceedTask,
    TaskBreadcrumbsResult,
    TaskState,
    TaskStatesResult,
    ToSupervisor,
    TriggerDagRun,
    ValidateInletsAndOutlets,
    VariableResult,
    XComResult,
    XComSequenceIndexResult,
    XComSequenceSliceResult,
    _RequestFrame,
    _ResponseFrame,
)
from airflow.sdk.log import mask_secret

try:
    from socket import send_fds
except ImportError:
    send_fds = None  # type: ignore[assignment]

if TYPE_CHECKING:
    from structlog.typing import FilteringBoundLogger, WrappedLogger
    from typing_extensions import Self

    from airflow.executors.workloads import BundleInfo
    from airflow.sdk.bases.secrets_backend import BaseSecretsBackend
    from airflow.sdk.definitions.connection import Connection
    from airflow.sdk.types import RuntimeTaskInstanceProtocol as RuntimeTI


__all__ = ["ActivitySubprocess", "WatchedSubprocess", "supervise"]

log: FilteringBoundLogger = structlog.get_logger(logger_name="supervisor")

HEARTBEAT_TIMEOUT: int = conf.getint("scheduler", "task_instance_heartbeat_timeout")
# Don't heartbeat more often than this
MIN_HEARTBEAT_INTERVAL: int = conf.getint("workers", "min_heartbeat_interval")
MAX_FAILED_HEARTBEATS: int = conf.getint("workers", "max_failed_heartbeats")

SOCKET_CLEANUP_TIMEOUT: float = conf.getfloat("workers", "socket_cleanup_timeout")

# Maximum possible time (in seconds) that task will have for execution of auxiliary processes
# like listeners after task is complete.
TASK_OVERTIME_THRESHOLD: float = conf.getfloat("core", "task_success_overtime")

SERVER_TERMINATED = "SERVER_TERMINATED"

# These are the task instance states that require some additional information to transition into.
# "Directly" here means that the PATCH API calls to transition into these states are
# made from _handle_request() itself and don't have to come all the way to wait().
STATES_SENT_DIRECTLY = [
    TaskInstanceState.DEFERRED,
    TaskInstanceState.UP_FOR_RESCHEDULE,
    TaskInstanceState.UP_FOR_RETRY,
    TaskInstanceState.SUCCESS,
    SERVER_TERMINATED,
]

# Setting a fair buffer size here to handle most message sizes. Intention is to enforce a buffer size
# that is big enough to handle small to medium messages while not enforcing hard latency issues
BUFFER_SIZE = 4096

SIGSEGV_MESSAGE = """
******************************************* Received SIGSEGV *******************************************
SIGSEGV (Segmentation Violation) signal indicates Segmentation Fault error which refers to
an attempt by a program/library to write or read outside its allocated memory.

In Python environment usually this signal refers to libraries which use low level C API.
Make sure that you use right libraries/Docker Images
for your architecture (Intel/ARM) and/or Operational System (Linux/macOS).

Suggested way to debug
======================
  - Set environment variable 'PYTHONFAULTHANDLER' to 'true'.
  - Start airflow services.
  - Restart failed airflow task.
  - Check 'scheduler' and 'worker' services logs for additional traceback
    which might contain information about module/library where actual error happen.

Known Issues
============

Note: Only Linux-based distros supported as "Production" execution environment for Airflow.

macOS
-----
 1. Due to limitations in Apple's libraries not every process might 'fork' safe.
    One of the general error is unable to query the macOS system configuration for network proxies.
    If your are not using a proxy you could disable it by set environment variable 'no_proxy' to '*'.
    See: https://github.com/python/cpython/issues/58037 and https://bugs.python.org/issue30385#msg293958
********************************************************************************************************"""


def _subprocess_main():
    from airflow.sdk.execution_time.task_runner import main

    main()


def _reset_signals():
    # Uninstall the rich etc. exception handler
    sys.excepthook = sys.__excepthook__
    signal.signal(signal.SIGINT, signal.SIG_DFL)
    signal.signal(signal.SIGTERM, signal.SIG_DFL)
    signal.signal(signal.SIGUSR2, signal.SIG_DFL)


def _configure_logs_over_json_channel(log_fd: int):
    # A channel that the task can send JSON-formatted logs over.
    #
    # JSON logs sent this way will be handled nicely
    from airflow.sdk.log import configure_logging, reset_logging

    log_io = os.fdopen(log_fd, "wb", buffering=0)
    reset_logging()
    configure_logging(json_output=True, output=log_io, sending_to_supervisor=True)


def _reopen_std_io_handles(child_stdin, child_stdout, child_stderr):
    # Ensure that sys.stdout et al (and the underlying filehandles for C libraries etc) are connected to the
    # pipes from the supervisor
    for handle_name, fd, sock, mode in (
        # Yes, we want to re-open stdin in write mode! This is cause it is a bi-directional socket, so we can
        # read and write to it.
        ("stdin", 0, child_stdin, "w"),
        ("stdout", 1, child_stdout, "w"),
        ("stderr", 2, child_stderr, "w"),
    ):
        os.dup2(sock.fileno(), fd)
        del sock

        # We open the socket/fd as binary, and then pass it to a TextIOWrapper so that it looks more like a
        # normal sys.stdout etc.
        binary = os.fdopen(fd, mode + "b")
        handle = io.TextIOWrapper(binary, line_buffering=True)
        setattr(sys, handle_name, handle)


def _get_last_chance_stderr() -> TextIO:
    stream = sys.__stderr__ or sys.stderr

    try:
        # We want to open another copy of the underlying filedescriptor if we can, to ensure it stays open!
        return os.fdopen(os.dup(stream.fileno()), "w", buffering=1)
    except Exception:
        # If that didn't work, do the best we can
        return stream


class BlockedDBSession:
    """:meta private:"""  # noqa: D400

    def __init__(self):
        raise RuntimeError("Direct database access via the ORM is not allowed in Airflow 3.0")

    def remove(*args, **kwargs):
        pass

    def get_bind(
        self,
        mapper=None,
        clause=None,
        bind=None,
        _sa_skip_events=None,
        _sa_skip_for_implicit_returning=False,
    ):
        pass


def block_orm_access():
    """
    Disable direct DB access as best as possible from task code.

    While we still don't have 100% code separation between TaskSDK and "core" Airflow, it is still possible to
    import the models and use them. This does what it can to disable that if it is not blocked at the network
    level
    """
    # A fake URL schema that might give users some clue what's going on. Hopefully
    conn = "airflow-db-not-allowed:///"
    if "airflow.settings" in sys.modules:
        from airflow import settings

        # This one needs to be from core, because we are checking if settings is loaded to disallow ORM
        # If settings is loaded, airflow.configuration will be too
        from airflow.configuration import conf

        to_block = frozenset(("engine", "async_engine", "Session", "AsyncSession", "NonScopedSession"))
        for attr in to_block:
            if hasattr(settings, attr):
                delattr(settings, attr)

        def configure_orm(*args, **kwargs):
            raise RuntimeError("Database access is disabled from Dags and Triggers")

        settings.configure_orm = configure_orm
        settings.Session = BlockedDBSession
        if conf.has_section("database"):
            conf.set("database", "sql_alchemy_conn", conn)
            conf.set("database", "sql_alchemy_conn_cmd", "/bin/false")
            conf.set("database", "sql_alchemy_conn_secret", "db-access-blocked")

        # This only gets called when the module does not already have an attribute, and for these values
        # lets give a custom error message
        def __getattr__(name: str):
            if name in to_block:
                raise AttributeError("Access to the Airflow Metadatabase from dags is not allowed!")
            raise AttributeError(f"module {settings.__name__!r} has no attribute {name!r}")

        settings.__getattr__ = __getattr__

        settings.SQL_ALCHEMY_CONN = conn
        settings.SQL_ALCHEMY_CONN_ASYNC = conn

    os.environ["AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"] = conn
    os.environ["AIRFLOW__CORE__SQL_ALCHEMY_CONN"] = conn


def _fork_main(
    requests: socket,
    child_stdout: socket,
    child_stderr: socket,
    log_fd: int,
    target: Callable[[], None],
) -> NoReturn:
    """
    "Entrypoint" of the child process.

    Ultimately this process will be running the user's code in the operators ``execute()`` function.

    The responsibility of this function is to:

    - Reset any signals handlers we inherited from the parent process (so they don't fire twice - once in
      parent, and once in child)
    - Set up the out/err handles to the streams created in the parent (to capture stdout and stderr for
      logging)
    - Configure the loggers in the child (both stdlib logging and Structlog) to send JSON logs back to the
      supervisor for processing/output.
    - Catch un-handled exceptions and attempt to show _something_ in case of error
    - Finally, run the actual task runner code (``target`` argument, defaults to ``.task_runner:main`)
    """
    # TODO: Make this process a session leader

    # Store original stderr for last-chance exception handling
    last_chance_stderr = _get_last_chance_stderr()

    _reset_signals()
    if log_fd:
        _configure_logs_over_json_channel(log_fd)
    _reopen_std_io_handles(requests, child_stdout, child_stderr)

    def exit(n: int) -> NoReturn:
        with suppress(ValueError, OSError):
            sys.stdout.flush()
        with suppress(ValueError, OSError):
            sys.stderr.flush()
        with suppress(ValueError, OSError):
            last_chance_stderr.flush()

        # Explicitly close the child-end of our supervisor sockets so
        # the parent sees EOF on "logs" channel.
        with suppress(OSError):
            os.close(log_fd)
        with suppress(OSError):
            os.close(requests.fileno())
        os._exit(n)

    if hasattr(atexit, "_clear"):
        # Since we're in a fork we want to try and clear them. If we can't do it cleanly, then we won't try
        # and run new atexit handlers.
        with suppress(Exception):
            atexit._clear()
            base_exit = exit

            def exit(n: int) -> NoReturn:
                # This will only run any atexit funcs registered after we've forked.
                atexit._run_exitfuncs()
                base_exit(n)

    try:
        block_orm_access()

        target()
        exit(0)
    except SystemExit as e:
        code = 1
        if isinstance(e.code, int):
            code = e.code
        elif e.code:
            print(e.code, file=sys.stderr)
        exit(code)
    except Exception:
        # Last ditch log attempt
        exc, v, tb = sys.exc_info()

        import traceback

        try:
            last_chance_stderr.write("--- Supervised process Last chance exception handler ---\n")
            traceback.print_exception(exc, value=v, tb=tb, file=last_chance_stderr)
            # Exit code 126 and 125 don't have any "special" meaning, they are only meant to serve as an
            # identifier that the task process died in a really odd way.
            exit(126)
        except Exception as e:
            with suppress(Exception):
                print(
                    f"--- Last chance exception handler failed --- {repr(str(e))}\n", file=last_chance_stderr
                )
            exit(125)


@attrs.define(kw_only=True)
class WatchedSubprocess:
    """
    Base class for managing subprocesses in Airflow's TaskSDK.

    This class handles common functionalities required for subprocess management, such as
    socket handling, process monitoring, and request handling.
    """

    id: UUID

    pid: int
    """The process ID of the child process"""

    stdin: socket
    """The handle connected to stdin of the child process"""

    decoder: ClassVar[TypeAdapter]
    """The decoder to use for incoming messages from the child process."""

    _process: psutil.Process = attrs.field(repr=False)
    """File descriptor for request handling."""

    _exit_code: int | None = attrs.field(default=None, init=False)
    _process_exit_monotonic: float | None = attrs.field(default=None, init=False)
    _open_sockets: weakref.WeakKeyDictionary[socket, str] = attrs.field(
        factory=weakref.WeakKeyDictionary, init=False
    )

    selector: selectors.BaseSelector = attrs.field(factory=selectors.DefaultSelector, repr=False)

    _frame_encoder: msgspec.msgpack.Encoder = attrs.field(factory=comms._new_encoder, repr=False)

    process_log: FilteringBoundLogger = attrs.field(repr=False)

    subprocess_logs_to_stdout: bool = False
    """Duplicate log messages to stdout, or only send them to ``self.process_log``."""

    start_time: float = attrs.field(factory=time.monotonic)
    """The start time of the child process."""

    @classmethod
    def start(
        cls,
        *,
        target: Callable[[], None] = _subprocess_main,
        logger: FilteringBoundLogger | None = None,
        **constructor_kwargs,
    ) -> Self:
        """Fork and start a new subprocess with the specified target function."""
        # Create socketpairs/"pipes" to connect to the stdin and out from the subprocess
        child_stdout, read_stdout = socketpair()
        child_stderr, read_stderr = socketpair()

        # Place for child to send requests/read responses, and the server side to read/respond
        child_requests, read_requests = socketpair()

        # Open the socketpair before forking off the child, so that it is open when we fork.
        child_logs, read_logs = socketpair()

        # Prevent on console:
        # DeprecationWarning: This process (pid=NNN) is multi-threaded, use of fork() may lead to deadlocks in the child.
        warnings.filterwarnings(
            "ignore",
            module="airflow.sdk.execution_time.supervisor",
            message=".*use of fork() may lead to deadlocks in the child.",
        )
        pid = os.fork()
        if pid == 0:
            # Close and delete of the parent end of the sockets.
            cls._close_unused_sockets(read_requests, read_stdout, read_stderr, read_logs)

            # Python GC should delete these for us, but lets make double sure that we don't keep anything
            # around in the forked processes, especially things that might involve open files or sockets!
            del constructor_kwargs
            del logger

            try:
                # Run the child entrypoint
                _fork_main(child_requests, child_stdout, child_stderr, child_logs.fileno(), target)
            except BaseException as e:
                import traceback

                with suppress(BaseException):
                    # We can't use log here, as if we except out of _fork_main something _weird_ went on.
                    print("Exception in _fork_main, exiting with code 124", file=sys.stderr)
                    traceback.print_exception(type(e), e, e.__traceback__, file=sys.stderr)

            # It's really super super important we never exit this block. We are in the forked child, and if we
            # do then _THINGS GET WEIRD_.. (Normally `_fork_main` itself will `_exit()` so we never get here)
            os._exit(124)

        # Close the remaining parent-end of the sockets we've passed to the child via fork. We still have the
        # other end of the pair open
        cls._close_unused_sockets(child_stdout, child_stderr, child_logs)

        logger = logger or cast("FilteringBoundLogger", structlog.get_logger(logger_name="task").bind())
        proc = cls(
            pid=pid,
            stdin=read_requests,
            process=psutil.Process(pid),
            process_log=logger,
            start_time=time.monotonic(),
            **constructor_kwargs,
        )

        proc._register_pipe_readers(
            stdout=read_stdout,
            stderr=read_stderr,
            requests=read_requests,
            logs=read_logs,
        )

        return proc

    def _register_pipe_readers(self, stdout: socket, stderr: socket, requests: socket, logs: socket):
        """Register handlers for subprocess communication channels."""
        # self.selector is a way of registering a handler/callback to be called when the given IO channel has
        # activity to read on (https://www.man7.org/linux/man-pages/man2/select.2.html etc, but better
        # alternatives are used automatically) -- this is a way of having "event-based" code, but without
        # needing full async, to read and process output from each socket as it is received.

        # Track the open sockets, and for debugging what type each one is
        self._open_sockets.update(
            (
                (stdout, "stdout"),
                (stderr, "stderr"),
                (logs, "logs"),
                (requests, "requests"),
            )
        )

        target_loggers: tuple[FilteringBoundLogger, ...] = (self.process_log,)

        if self.subprocess_logs_to_stdout:
            target_loggers += (log,)

        self.selector.register(
            stdout, selectors.EVENT_READ, self._create_log_forwarder(target_loggers, "task.stdout")
        )
        self.selector.register(
            stderr,
            selectors.EVENT_READ,
            self._create_log_forwarder(target_loggers, "task.stderr", log_level=logging.ERROR),
        )
        self.selector.register(
            logs,
            selectors.EVENT_READ,
            make_buffered_socket_reader(
                process_log_messages_from_subprocess(target_loggers), on_close=self._on_socket_closed
            ),
        )
        self.selector.register(
            requests,
            selectors.EVENT_READ,
            length_prefixed_frame_reader(self.handle_requests(log), on_close=self._on_socket_closed),
        )

    def _create_log_forwarder(self, loggers, name, log_level=logging.INFO) -> Callable[[socket], bool]:
        """Create a socket handler that forwards logs to a logger."""
        loggers = tuple(
            reconfigure_logger(
                log,
                structlog.processors.CallsiteParameterAdder,
            )
            for log in loggers
        )
        return make_buffered_socket_reader(
            forward_to_log(loggers, logger=name, level=log_level), on_close=self._on_socket_closed
        )

    def _on_socket_closed(self, sock: socket):
        # We want to keep servicing this process until we've read up to EOF from all the sockets.

        with suppress(KeyError):
            self.selector.unregister(sock)
            del self._open_sockets[sock]

    def send_msg(
        self, msg: BaseModel | None, request_id: int, error: ErrorResponse | None = None, **dump_opts
    ):
        """
        Send the msg as a length-prefixed response frame.

        ``request_id`` is the ID that the client sent in it's request, and has no meaning to the server

        """
        if msg:
            frame = _ResponseFrame(id=request_id, body=msg.model_dump(**dump_opts))
        else:
            err_resp = error.model_dump() if error else None
            frame = _ResponseFrame(id=request_id, error=err_resp)

        self.stdin.sendall(frame.as_bytes())

    def handle_requests(self, log: FilteringBoundLogger) -> Generator[None, _RequestFrame, None]:
        """Handle incoming requests from the task process, respond with the appropriate data."""
        while True:
            request = yield

            try:
                msg = self.decoder.validate_python(request.body)
            except Exception:
                log.exception("Unable to decode message", body=request.body)
                continue

            try:
                self._handle_request(msg, log, request.id)
            except ServerResponseError as e:
                error_details = e.response.json() if e.response else None
                log.error(
                    "API server error",
                    status_code=e.response.status_code,
                    detail=error_details,
                    message=str(e),
                )

                # Send error response back to task so that the error appears in the task logs
                self.send_msg(
                    msg=None,
                    error=ErrorResponse(
                        error=ErrorType.API_SERVER_ERROR,
                        detail={
                            "status_code": e.response.status_code,
                            "message": str(e),
                            "detail": error_details,
                        },
                    ),
                    request_id=request.id,
                )

    def _handle_request(self, msg, log: FilteringBoundLogger, req_id: int) -> None:
        raise NotImplementedError()

    @staticmethod
    def _close_unused_sockets(*sockets):
        """Close unused ends of sockets after fork."""
        for sock in sockets:
            sock.close()

    def _cleanup_open_sockets(self):
        """Force-close any sockets that never reported EOF."""
        # In extremely busy environments the selector can fail to deliver a
        # final read event before the subprocess exits. Without closing these
        # sockets the supervisor would wait forever thinking they are still
        # active. This cleanup ensures we always release resources and exit.
        stuck_sockets = []
        for sock, socket_type in self._open_sockets.items():
            fileno = "unknown"
            with suppress(Exception):
                fileno = sock.fileno()
                sock.close()
            stuck_sockets.append(f"{socket_type}(fd={fileno})")

        if stuck_sockets:
            log.warning("Force-closed stuck sockets", pid=self.pid, sockets=stuck_sockets)

        self.selector.close()
        self.stdin.close()

    def kill(
        self,
        signal_to_send: signal.Signals = signal.SIGINT,
        escalation_delay: float = 5.0,
        force: bool = False,
    ):
        """
        Attempt to terminate the subprocess with a given signal.

        If the process does not exit within `escalation_delay` seconds, escalate to SIGTERM and eventually SIGKILL if necessary.

        :param signal_to_send: The signal to send initially (default is SIGINT).
        :param escalation_delay: Time in seconds to wait before escalating to a stronger signal.
        :param force: If True, ensure escalation through all signals without skipping.
        """
        if self._exit_code is not None:
            return

        # Escalation sequence: SIGINT -> SIGTERM -> SIGKILL
        escalation_path: list[signal.Signals] = [signal.SIGINT, signal.SIGTERM, signal.SIGKILL]

        if force and signal_to_send in escalation_path:
            # Start from `signal_to_send` and escalate to the end of the escalation path
            escalation_path = escalation_path[escalation_path.index(signal_to_send) :]
        else:
            escalation_path = [signal_to_send]

        for sig in escalation_path:
            try:
                self._process.send_signal(sig)

                start = time.monotonic()
                end = start + escalation_delay
                now = start

                while now < end:
                    # Service subprocess events during the escalation delay. This will return as soon as it's
                    # read from any of the sockets, so we need to re-run it if the process is still alive
                    if (
                        exit_code := self._service_subprocess(
                            max_wait_time=end - now, raise_on_timeout=False, expect_signal=sig
                        )
                    ) is not None:
                        log.info("Process exited", pid=self.pid, exit_code=exit_code, signal_sent=sig.name)
                        return

                    now = time.monotonic()

                msg = "Process did not terminate in time"
                if sig != escalation_path[-1]:
                    msg += "; escalating"
                log.warning(msg, pid=self.pid, signal=sig.name)
            except psutil.NoSuchProcess:
                log.debug("Process already terminated", pid=self.pid)
                self._exit_code = -1
                return

        log.error("Failed to terminate process after full escalation", pid=self.pid)

    def wait(self) -> int:
        raise NotImplementedError()

    def __rich_repr__(self):
        yield "id", self.id
        yield "pid", self.pid
        # only include this if it's not the default (third argument)
        yield "exit_code", self._exit_code, None

    __rich_repr__.angular = True  # type: ignore[attr-defined]

    def __repr__(self) -> str:
        rep = f"<{type(self).__name__} id={self.id} pid={self.pid}"
        if self._exit_code is not None:
            rep += f" exit_code={self._exit_code}"
        return rep + " >"

    def _service_subprocess(
        self, max_wait_time: float, raise_on_timeout: bool = False, expect_signal: None | int = None
    ):
        """
        Service subprocess events by processing socket activity and checking for process exit.

        This method:
        - Waits for activity on the registered file objects (via `self.selector.select`).
        - Processes any events triggered on these file objects.
        - Checks if the subprocess has exited during the wait.

        :param max_wait_time: Maximum time to block while waiting for events, in seconds.
        :param raise_on_timeout: If True, raise an exception if the subprocess does not exit within the timeout.
        :param expect_signal: Signal not to log if the task exits with this code.
        :returns: The process exit code, or None if it's still alive
        """
        # Ensure minimum timeout to prevent CPU spike with tight loop when timeout is 0 or negative
        timeout = max(0.01, max_wait_time)
        events = self.selector.select(timeout=timeout)
        for key, _ in events:
            # Retrieve the handler responsible for processing this file object (e.g., stdout, stderr)
            socket_handler, on_close = key.data

            # Example of handler behavior:
            # If the subprocess writes "Hello, World!" to stdout:
            # - `socket_handler` reads and processes the message.
            # - If EOF is reached, the handler returns False to signal no more reads are expected.
            # - BrokenPipeError should be caught and treated as if the handler returned false, similar
            # to EOF case
            try:
                need_more = socket_handler(key.fileobj)
            except (BrokenPipeError, ConnectionResetError):
                need_more = False

            # If the handler signals that the file object is no longer needed (EOF, closed, etc.)
            # unregister it from the selector to stop monitoring; `wait()` blocks until all selectors
            # are removed.
            if not need_more:
                sock: socket = key.fileobj  # type: ignore[assignment]
                on_close(sock)
                sock.close()

        # Check if the subprocess has exited
        return self._check_subprocess_exit(raise_on_timeout=raise_on_timeout, expect_signal=expect_signal)

    def _check_subprocess_exit(
        self, raise_on_timeout: bool = False, expect_signal: None | int = None
    ) -> int | None:
        """Check if the subprocess has exited."""
        if self._exit_code is not None:
            return self._exit_code

        try:
            self._exit_code = self._process.wait(timeout=0)
        except psutil.TimeoutExpired:
            if raise_on_timeout:
                raise
        else:
            self._process_exit_monotonic = time.monotonic()

            if expect_signal is not None and self._exit_code == -expect_signal:
                # Bypass logging, the caller expected us to exit with this
                return self._exit_code

            # Put a message in the viewable task logs

            if self._exit_code == -signal.SIGSEGV:
                self.process_log.critical(SIGSEGV_MESSAGE)
            # psutil turns signal exit codes into an enum for us. Handy. (Otherwise it's a plain integer) if exit_code and (name := getattr(exit_code, "name")):
            elif name := getattr(self._exit_code, "name", None):
                message = "Process terminated by signal."
                level = logging.ERROR
                if self._exit_code == -signal.SIGKILL:
                    message += " Likely out of memory error (OOM)."
                    level = logging.CRITICAL
                message += " For more information, see https://airflow.apache.org/docs/apache-airflow/stable/troubleshooting.html#process-terminated-by-signal."
                self.process_log.log(level, message, signal=int(self._exit_code), signal_name=name)
            elif self._exit_code:
                # Run of the mill exit code (1, 42, etc).
                # Most task errors should be caught in the task runner and _that_ exits with 0.
                self.process_log.warning("Process exited abnormally", exit_code=self._exit_code)
        return self._exit_code


_REMOTE_LOGGING_CONN_CACHE: dict[str, Connection | None] = {}


def _fetch_remote_logging_conn(conn_id: str, client: Client) -> Connection | None:
    """
    Fetch and cache connection for remote logging.

    Args:
        conn_id: Connection ID to fetch
        client: API client for making requests

    Returns:
        Connection object or None if not found.
    """
    # Since we need to use the API Client directly, we can't use Connection.get as that would try to use
    # SUPERVISOR_COMMS

    # TODO: Store in the SecretsCache if its enabled - see #48858

    if conn_id in _REMOTE_LOGGING_CONN_CACHE:
        return _REMOTE_LOGGING_CONN_CACHE[conn_id]

    backends = ensure_secrets_backend_loaded()
    for secrets_backend in backends:
        try:
            conn = secrets_backend.get_connection(conn_id=conn_id)
            if conn:
                _REMOTE_LOGGING_CONN_CACHE[conn_id] = conn
                return conn
        except Exception:
            log.exception(
                "Unable to retrieve connection from secrets backend (%s). "
                "Checking subsequent secrets backend.",
                type(secrets_backend).__name__,
            )

    conn = client.connections.get(conn_id)
    if isinstance(conn, ConnectionResponse):
        conn_result = ConnectionResult.from_conn_response(conn)
        from airflow.sdk.definitions.connection import Connection

        result: Connection | None = Connection(**conn_result.model_dump(exclude={"type"}, by_alias=True))
    else:
        result = None

    _REMOTE_LOGGING_CONN_CACHE[conn_id] = result
    return result


@contextlib.contextmanager
def _remote_logging_conn(client: Client):
    """
    Pre-fetch the needed remote logging connection with caching.

    If a remote logger is in use, and has the logging/remote_logging option set, we try to fetch the
    connection it needs, now, directly from the API client, and store it in an env var, so that when the logging
    hook tries to get the connection it can find it easily from the env vars.

    This is needed as the BaseHook.get_connection looks for SUPERVISOR_COMMS, but we are still in the
    supervisor process when this is needed, so that doesn't exist yet.

    The connection details are fetched eagerly on every invocation to avoid retaining
    per-task API client instances in global caches.
    """
    from airflow.sdk.log import load_remote_conn_id, load_remote_log_handler

    if load_remote_log_handler() is None or not (conn_id := load_remote_conn_id()):
        # Nothing to do
        yield
        return

    # Fetch connection details on-demand without caching the entire API client instance
    conn = _fetch_remote_logging_conn(conn_id, client)

    if not conn:
        try:
            yield
        finally:
            # Ensure we don't leak the caller's client when no connection was fetched.
            del conn
            del client
        return

    key = f"AIRFLOW_CONN_{conn_id.upper()}"
    old_conn = os.getenv(key)
    old_context = os.getenv("_AIRFLOW_PROCESS_CONTEXT")

    os.environ[key] = conn.get_uri()
    # Set process context to "client" so that Connection deserialization uses SDK Connection class
    # which has from_uri() method, instead of core Connection class
    os.environ["_AIRFLOW_PROCESS_CONTEXT"] = "client"
    try:
        yield
    finally:
        if old_conn is None:
            del os.environ[key]
        else:
            os.environ[key] = old_conn

        if old_context is None:
            del os.environ["_AIRFLOW_PROCESS_CONTEXT"]
        else:
            os.environ["_AIRFLOW_PROCESS_CONTEXT"] = old_context

        # Explicitly drop local references so the caller's client can be garbage collected.
        del conn
        del client


@attrs.define(kw_only=True)
class ActivitySubprocess(WatchedSubprocess):
    client: Client
    """The HTTP client to use for communication with the API server."""

    _terminal_state: str | None = attrs.field(default=None, init=False)
    _final_state: str | None = attrs.field(default=None, init=False)

    _last_successful_heartbeat: float = attrs.field(default=0, init=False)
    _last_heartbeat_attempt: float = attrs.field(default=0, init=False)

    _should_retry: bool = attrs.field(default=False, init=False)
    """Whether the task should retry or not as decided by the API server."""

    # After the failure of a heartbeat, we'll increment this counter. If it reaches `MAX_FAILED_HEARTBEATS`, we
    # will kill theprocess. This is to handle temporary network issues etc. ensuring that the process
    # does not hang around forever.
    failed_heartbeats: int = attrs.field(default=0, init=False)

    _task_end_time_monotonic: float | None = attrs.field(default=None, init=False)
    _rendered_map_index: str | None = attrs.field(default=None, init=False)

    decoder: ClassVar[TypeAdapter[ToSupervisor]] = TypeAdapter(ToSupervisor)

    ti: RuntimeTI | None = None

    @classmethod
    def start(  # type: ignore[override]
        cls,
        *,
        what: TaskInstance,
        dag_rel_path: str | os.PathLike[str],
        bundle_info,
        client: Client,
        target: Callable[[], None] = _subprocess_main,
        logger: FilteringBoundLogger | None = None,
        sentry_integration: str = "",
        **kwargs,
    ) -> Self:
        """Fork and start a new subprocess to execute the given task."""
        proc: Self = super().start(id=what.id, client=client, target=target, logger=logger, **kwargs)
        # Tell the task process what it needs to do!
        proc._on_child_started(
            ti=what,
            dag_rel_path=dag_rel_path,
            bundle_info=bundle_info,
            sentry_integration=sentry_integration,
        )
        return proc

    def _on_child_started(
        self,
        *,
        ti: TaskInstance,
        dag_rel_path: str | os.PathLike[str],
        bundle_info,
        sentry_integration: str,
    ) -> None:
        """Send startup message to the subprocess."""
        self.ti = ti  # type: ignore[assignment]
        start_date = datetime.now(tz=timezone.utc)
        try:
            # We've forked, but the task won't start doing anything until we send it the StartupDetails
            # message. But before we do that, we need to tell the server it's started (so it has the chance to
            # tell us "no, stop!" for any reason)
            ti_context = self.client.task_instances.start(ti.id, self.pid, start_date)
            self._should_retry = ti_context.should_retry
            self._last_successful_heartbeat = time.monotonic()
        except Exception:
            # On any error kill that subprocess!
            self.kill(signal.SIGKILL)
            raise

        msg = StartupDetails.model_construct(
            ti=ti,
            dag_rel_path=os.fspath(dag_rel_path),
            bundle_info=bundle_info,
            ti_context=ti_context,
            start_date=start_date,
            sentry_integration=sentry_integration,
        )

        # Send the message to tell the process what it needs to execute
        log.debug("Sending", msg=msg)

        try:
            self.send_msg(msg, request_id=0)
        except (BrokenPipeError, ConnectionResetError):
            # Debug is fine, the process will have shown _something_ in it's last_chance exception handler
            log.debug("Couldn't send startup message to Subprocess - it died very early", pid=self.pid)

    def wait(self) -> int:
        if self._exit_code is not None:
            return self._exit_code

        try:
            self._monitor_subprocess()
        finally:
            self.selector.close()

        # self._monitor_subprocess() will set the exit code when the process has finished
        # If it hasn't, assume it's failed
        self._exit_code = self._exit_code if self._exit_code is not None else 1

        self.update_task_state_if_needed()

        # Now at the last possible moment, when all logs and comms with the subprocess has finished, lets
        # upload the remote logs
        self._upload_logs()

        return self._exit_code

    def update_task_state_if_needed(self):
        # If the process has finished non-directly patched state (directly means deferred, reschedule, etc.),
        # update the state of the TaskInstance to reflect the final state of the process.
        # For states like `deferred`, `up_for_reschedule`, the process will exit with 0, but the state will be updated
        # by the subprocess in the `handle_requests` method.
        if self.final_state not in STATES_SENT_DIRECTLY:
            self.client.task_instances.finish(
                id=self.id,
                state=self.final_state,
                when=datetime.now(tz=timezone.utc),
                rendered_map_index=self._rendered_map_index,
            )

    def _upload_logs(self):
        """
        Upload all log files found to the remote storage.

        We upload logs from here after the task has finished to give us the best possible chance of logs being
        uploaded in case the task task.
        """
        from airflow.sdk.log import upload_to_remote

        with _remote_logging_conn(self.client):
            upload_to_remote(self.process_log, self.ti)

    def _monitor_subprocess(self):
        """
        Monitor the subprocess until it exits.

        This function:

        - Waits for activity on file objects (e.g., subprocess stdout, stderr, logs, requests) using the selector.
        - Processes events triggered on the monitored file objects, such as data availability or EOF.
        - Sends heartbeats to ensure the process is alive and checks if the subprocess has exited.
        """
        while self._exit_code is None or self._open_sockets:
            last_heartbeat_ago = time.monotonic() - self._last_successful_heartbeat
            # Monitor the task to see if it's done. Wait in a syscall (`select`) for as long as possible
            # so we notice the subprocess finishing as quick as we can.
            max_wait_time = max(
                0,  # Make sure this value is never negative,
                min(
                    # Ensure we heartbeat _at most_ 75% through the task instance heartbeat timeout time
                    HEARTBEAT_TIMEOUT - last_heartbeat_ago * 0.75,
                    MIN_HEARTBEAT_INTERVAL,
                ),
            )
            # Block until events are ready or the timeout is reached
            # This listens for activity (e.g., subprocess output) on registered file objects
            alive = self._service_subprocess(max_wait_time=max_wait_time) is None

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

            if alive:
                # We don't need to heartbeat if the process has shutdown, as we are just finishing of reading the
                # logs
                self._send_heartbeat_if_needed()

                self._handle_process_overtime_if_needed()

    def _handle_process_overtime_if_needed(self):
        """Handle termination of auxiliary processes if the task exceeds the configured overtime."""
        # If the task has reached a terminal state, we can start monitoring the overtime
        if not self._terminal_state:
            return
        if (
            self._task_end_time_monotonic
            and (time.monotonic() - self._task_end_time_monotonic) > TASK_OVERTIME_THRESHOLD
        ):
            log.warning(
                "Task success overtime reached; terminating process. "
                "Modify `task_success_overtime` setting in [core] section of "
                "Airflow configuration to change this limit.",
                ti_id=self.id,
            )
            self.kill(signal.SIGTERM, force=True)

    def _send_heartbeat_if_needed(self):
        """Send a heartbeat to the client if heartbeat interval has passed."""
        # Respect the minimum interval between heartbeat attempts
        if (time.monotonic() - self._last_heartbeat_attempt) < MIN_HEARTBEAT_INTERVAL:
            return

        if self._terminal_state:
            # If the task has finished, and we are in "overtime" (running OL listeners etc) we shouldn't
            # heartbeat
            return

        self._last_heartbeat_attempt = time.monotonic()
        try:
            self.client.task_instances.heartbeat(self.id, pid=self._process.pid)
            # Update the last heartbeat time on success
            self._last_successful_heartbeat = time.monotonic()

            # Reset the counter on success
            self.failed_heartbeats = 0
        except ServerResponseError as e:
            if e.response.status_code in {HTTPStatus.NOT_FOUND, HTTPStatus.CONFLICT}:
                log.error(
                    "Server indicated the task shouldn't be running anymore",
                    detail=e.detail,
                    status_code=e.response.status_code,
                    ti_id=self.id,
                )
                self.process_log.error(
                    "Server indicated the task shouldn't be running anymore. Terminating process",
                    detail=e.detail,
                )
                self.kill(signal.SIGTERM, force=True)
                self.process_log.error("Task killed!")
                self._terminal_state = SERVER_TERMINATED
            else:
                # If we get any other error, we'll just log it and try again next time
                self._handle_heartbeat_failures(e)
        except Exception as e:
            self._handle_heartbeat_failures(e)

    def _handle_heartbeat_failures(self, exc: Exception):
        """Increment the failed heartbeats counter and kill the process if too many failures."""
        self.failed_heartbeats += 1
        log.warning(
            "Failed to send heartbeat. Will be retried",
            failed_heartbeats=self.failed_heartbeats,
            ti_id=self.id,
            max_retries=MAX_FAILED_HEARTBEATS,
            exc_info=exc,
        )
        # If we've failed to heartbeat too many times, kill the process
        if self.failed_heartbeats >= MAX_FAILED_HEARTBEATS:
            log.error(
                "Too many failed heartbeats; terminating process", failed_heartbeats=self.failed_heartbeats
            )
            self.kill(signal.SIGTERM, force=True)

    @property
    def final_state(self):
        """
        The final state of the TaskInstance.

        By default, this will be derived from the exit code of the task
        (0=success, failed otherwise) but can be changed by the subprocess
        sending a TaskState message, as long as the process exits with 0

        Not valid before the process has finished.
        """
        if self._exit_code == 0:
            return self._terminal_state or TaskInstanceState.SUCCESS
        if self._exit_code != 0 and self._terminal_state == SERVER_TERMINATED:
            return SERVER_TERMINATED

        # Any non zero exit code indicates a failure
        # If retries are configured, mark as UP_FOR_RETRY
        # Negative exit codes indicate signal kills (often transient)
        # Positive exit codes can also be transient failures like network issues in a task communicating to
        # external services
        if self._exit_code != 0 and self._should_retry:
            return TaskInstanceState.UP_FOR_RETRY

        return TaskInstanceState.FAILED

    def _handle_request(self, msg: ToSupervisor, log: FilteringBoundLogger, req_id: int):
        if isinstance(msg, MaskSecret):
            log.debug("Received message from task runner (body omitted)", msg=type(msg))
        else:
            log.debug("Received message from task runner", msg=msg)
        resp: BaseModel | None = None
        dump_opts = {}
        if isinstance(msg, TaskState):
            self._terminal_state = msg.state
            self._task_end_time_monotonic = time.monotonic()
            self._rendered_map_index = msg.rendered_map_index
        elif isinstance(msg, SucceedTask):
            self._terminal_state = msg.state
            self._task_end_time_monotonic = time.monotonic()
            self._rendered_map_index = msg.rendered_map_index
            self.client.task_instances.succeed(
                id=self.id,
                when=msg.end_date,
                task_outlets=msg.task_outlets,
                outlet_events=msg.outlet_events,
                rendered_map_index=self._rendered_map_index,
            )
        elif isinstance(msg, RetryTask):
            self._terminal_state = msg.state
            self._task_end_time_monotonic = time.monotonic()
            self._rendered_map_index = msg.rendered_map_index
            self.client.task_instances.retry(
                id=self.id,
                end_date=msg.end_date,
                rendered_map_index=self._rendered_map_index,
            )
        elif isinstance(msg, GetConnection):
            conn = self.client.connections.get(msg.conn_id)
            if isinstance(conn, ConnectionResponse):
                if conn.password:
                    mask_secret(conn.password)
                if conn.extra:
                    mask_secret(conn.extra)
                conn_result = ConnectionResult.from_conn_response(conn)
                resp = conn_result
                dump_opts = {"exclude_unset": True, "by_alias": True}
            else:
                resp = conn
        elif isinstance(msg, GetVariable):
            var = self.client.variables.get(msg.key)
            if isinstance(var, VariableResponse):
                if var.value:
                    mask_secret(var.value, var.key)
                var_result = VariableResult.from_variable_response(var)
                resp = var_result
                dump_opts = {"exclude_unset": True}
            else:
                resp = var
        elif isinstance(msg, GetXCom):
            xcom = self.client.xcoms.get(
                msg.dag_id, msg.run_id, msg.task_id, msg.key, msg.map_index, msg.include_prior_dates
            )
            xcom_result = XComResult.from_xcom_response(xcom)
            resp = xcom_result
        elif isinstance(msg, GetXComCount):
            resp = self.client.xcoms.head(msg.dag_id, msg.run_id, msg.task_id, msg.key)
        elif isinstance(msg, GetXComSequenceItem):
            xcom = self.client.xcoms.get_sequence_item(
                msg.dag_id, msg.run_id, msg.task_id, msg.key, msg.offset
            )
            if isinstance(xcom, XComSequenceIndexResponse):
                resp = XComSequenceIndexResult.from_response(xcom)
            else:
                resp = xcom
        elif isinstance(msg, GetXComSequenceSlice):
            xcoms = self.client.xcoms.get_sequence_slice(
                msg.dag_id,
                msg.run_id,
                msg.task_id,
                msg.key,
                msg.start,
                msg.stop,
                msg.step,
                msg.include_prior_dates,
            )
            resp = XComSequenceSliceResult.from_response(xcoms)
        elif isinstance(msg, DeferTask):
            self._terminal_state = TaskInstanceState.DEFERRED
            self._rendered_map_index = msg.rendered_map_index
            self.client.task_instances.defer(self.id, msg)
        elif isinstance(msg, RescheduleTask):
            self._terminal_state = TaskInstanceState.UP_FOR_RESCHEDULE
            self.client.task_instances.reschedule(self.id, msg)
        elif isinstance(msg, SkipDownstreamTasks):
            self.client.task_instances.skip_downstream_tasks(self.id, msg)
        elif isinstance(msg, SetXCom):
            self.client.xcoms.set(
                msg.dag_id, msg.run_id, msg.task_id, msg.key, msg.value, msg.map_index, msg.mapped_length
            )
        elif isinstance(msg, DeleteXCom):
            self.client.xcoms.delete(msg.dag_id, msg.run_id, msg.task_id, msg.key, msg.map_index)
        elif isinstance(msg, PutVariable):
            self.client.variables.set(msg.key, msg.value, msg.description)
        elif isinstance(msg, SetRenderedFields):
            self.client.task_instances.set_rtif(self.id, msg.rendered_fields)
        elif isinstance(msg, SetRenderedMapIndex):
            self.client.task_instances.set_rendered_map_index(self.id, msg.rendered_map_index)
        elif isinstance(msg, GetAssetByName):
            asset_resp = self.client.assets.get(name=msg.name)
            if isinstance(asset_resp, AssetResponse):
                asset_result = AssetResult.from_asset_response(asset_resp)
                resp = asset_result
                dump_opts = {"exclude_unset": True}
            else:
                resp = asset_resp
        elif isinstance(msg, GetAssetByUri):
            asset_resp = self.client.assets.get(uri=msg.uri)
            if isinstance(asset_resp, AssetResponse):
                asset_result = AssetResult.from_asset_response(asset_resp)
                resp = asset_result
                dump_opts = {"exclude_unset": True}
            else:
                resp = asset_resp
        elif isinstance(msg, GetAssetEventByAsset):
            asset_event_resp = self.client.asset_events.get(
                uri=msg.uri,
                name=msg.name,
                after=msg.after,
                before=msg.before,
                ascending=msg.ascending,
                limit=msg.limit,
            )
            asset_event_result = AssetEventsResult.from_asset_events_response(asset_event_resp)
            resp = asset_event_result
            dump_opts = {"exclude_unset": True}
        elif isinstance(msg, GetAssetEventByAssetAlias):
            asset_event_resp = self.client.asset_events.get(
                alias_name=msg.alias_name,
                after=msg.after,
                before=msg.before,
                ascending=msg.ascending,
                limit=msg.limit,
            )
            asset_event_result = AssetEventsResult.from_asset_events_response(asset_event_resp)
            resp = asset_event_result
            dump_opts = {"exclude_unset": True}
        elif isinstance(msg, GetPrevSuccessfulDagRun):
            dagrun_resp = self.client.task_instances.get_previous_successful_dagrun(self.id)
            dagrun_result = PrevSuccessfulDagRunResult.from_dagrun_response(dagrun_resp)
            resp = dagrun_result
            dump_opts = {"exclude_unset": True}
        elif isinstance(msg, TriggerDagRun):
            resp = self.client.dag_runs.trigger(
                msg.dag_id,
                msg.run_id,
                msg.conf,
                msg.logical_date,
                msg.reset_dag_run,
            )
        elif isinstance(msg, GetDagRun):
            dr_resp = self.client.dag_runs.get_detail(msg.dag_id, msg.run_id)
            resp = DagRunResult.from_api_response(dr_resp)
        elif isinstance(msg, GetDagRunState):
            dr_resp = self.client.dag_runs.get_state(msg.dag_id, msg.run_id)
            resp = DagRunStateResult.from_api_response(dr_resp)
        elif isinstance(msg, GetTaskRescheduleStartDate):
            resp = self.client.task_instances.get_reschedule_start_date(msg.ti_id, msg.try_number)
        elif isinstance(msg, GetTICount):
            resp = self.client.task_instances.get_count(
                dag_id=msg.dag_id,
                map_index=msg.map_index,
                task_ids=msg.task_ids,
                task_group_id=msg.task_group_id,
                logical_dates=msg.logical_dates,
                run_ids=msg.run_ids,
                states=msg.states,
            )
        elif isinstance(msg, GetTaskStates):
            task_states_map = self.client.task_instances.get_task_states(
                dag_id=msg.dag_id,
                map_index=msg.map_index,
                task_ids=msg.task_ids,
                task_group_id=msg.task_group_id,
                logical_dates=msg.logical_dates,
                run_ids=msg.run_ids,
            )
            if isinstance(task_states_map, TaskStatesResponse):
                resp = TaskStatesResult.from_api_response(task_states_map)
            else:
                resp = task_states_map
        elif isinstance(msg, GetTaskBreadcrumbs):
            api_resp = self.client.task_instances.get_task_breakcrumbs(dag_id=msg.dag_id, run_id=msg.run_id)
            resp = TaskBreadcrumbsResult.from_api_response(api_resp)
        elif isinstance(msg, GetDRCount):
            resp = self.client.dag_runs.get_count(
                dag_id=msg.dag_id,
                logical_dates=msg.logical_dates,
                run_ids=msg.run_ids,
                states=msg.states,
            )
        elif isinstance(msg, GetPreviousDagRun):
            resp = self.client.dag_runs.get_previous(
                dag_id=msg.dag_id,
                logical_date=msg.logical_date,
                state=msg.state,
            )
        elif isinstance(msg, GetPreviousTI):
            resp = self.client.task_instances.get_previous(
                dag_id=msg.dag_id,
                task_id=msg.task_id,
                logical_date=msg.logical_date,
                map_index=msg.map_index,
                state=msg.state,
            )
        elif isinstance(msg, DeleteVariable):
            resp = self.client.variables.delete(msg.key)
        elif isinstance(msg, ValidateInletsAndOutlets):
            inactive_assets_resp = self.client.task_instances.validate_inlets_and_outlets(msg.ti_id)
            resp = InactiveAssetsResult.from_inactive_assets_response(inactive_assets_resp)
            dump_opts = {"exclude_unset": True}
        elif isinstance(msg, ResendLoggingFD):
            # We need special handling here!
            if send_fds is not None:
                self._send_new_log_fd(req_id)
                # Since we've sent the message, return. Nothing else in this ifelse/switch should return directly
                return
        elif isinstance(msg, CreateHITLDetailPayload):
            hitl_detail_request = self.client.hitl.add_response(
                ti_id=msg.ti_id,
                options=msg.options,
                subject=msg.subject,
                body=msg.body,
                defaults=msg.defaults,
                params=msg.params,
                multiple=msg.multiple,
                assigned_users=msg.assigned_users,
            )
            resp = HITLDetailRequestResult.from_api_response(hitl_detail_request)
            dump_opts = {"exclude_unset": True}
        elif isinstance(msg, MaskSecret):
            mask_secret(msg.value, msg.name)
        else:
            log.error("Unhandled request", msg=msg)
            self.send_msg(
                None,
                request_id=req_id,
                error=ErrorResponse(
                    error=ErrorType.API_SERVER_ERROR,
                    detail={"status_code": 400, "message": "Unhandled request"},
                ),
            )
            return

        self.send_msg(resp, request_id=req_id, error=None, **dump_opts)

    def _send_new_log_fd(self, req_id: int) -> None:
        if send_fds is None:
            raise RuntimeError("send_fds is not available on this platform")
        child_logs, read_logs = socketpair()

        target_loggers: tuple[FilteringBoundLogger, ...] = (self.process_log,)
        if self.subprocess_logs_to_stdout:
            target_loggers += (log,)

        self.selector.register(
            read_logs,
            selectors.EVENT_READ,
            make_buffered_socket_reader(
                process_log_messages_from_subprocess(target_loggers), on_close=self._on_socket_closed
            ),
        )
        # We don't explicitly close the old log socket, that will get handled for us if/when the other end is
        # closed (such as `sudo` would do for us automatically.) This also means that this feature _can_ be
        # used outside of a exec context if it is useful, as we can then have multiple things sending us logs,
        # such as the task process and it's launched subprocess.

        frame = _ResponseFrame(id=req_id, body=SentFDs(fds=[child_logs.fileno()]).model_dump())
        send_fds(self.stdin, [frame.as_bytes()], [child_logs.fileno()])
        child_logs.close()  # Close this end now.


def in_process_api_server():
    from airflow.api_fastapi.execution_api.app import InProcessExecutionAPI

    api = InProcessExecutionAPI()
    return api


@attrs.define(kw_only=True)
class InProcessSupervisorComms:
    """In-process communication handler that uses deques instead of sockets."""

    log: FilteringBoundLogger = attrs.field(repr=False, factory=structlog.get_logger)
    supervisor: InProcessTestSupervisor
    messages: deque[BaseModel | None] = attrs.field(factory=deque)

    def _get_response(self) -> BaseModel | None:
        """Get a message from the supervisor. Blocks until a message is available."""
        return self.messages.popleft()

    def send(self, msg: BaseModel):
        """Send a request to the supervisor."""
        self.log.debug("Sending request", msg=msg)

        with set_supervisor_comms(None):
            self.supervisor._handle_request(msg, log, 0)  # type: ignore[arg-type]

        return self._get_response()


@attrs.define
class TaskRunResult:
    """Result of running a task via ``InProcessTestSupervisor``."""

    ti: RuntimeTI
    state: str
    msg: BaseModel | None
    error: BaseException | None


@attrs.define(kw_only=True)
class InProcessTestSupervisor(ActivitySubprocess):
    """A supervisor that runs tasks in-process for easier testing."""

    comms: InProcessSupervisorComms = attrs.field(init=False)

    stdin: socket = attrs.field(init=False)

    class _Client(Client):
        def request(self, *args, **kwargs):
            # Bypass the tenacity retries!
            return super().request.__wrapped__(self, *args, **kwargs)  # type: ignore[attr-defined]

    def _check_subprocess_exit(
        self, raise_on_timeout: bool = False, expect_signal: None | int = None
    ) -> int | None:
        # InProcessSupervisor has no subprocess, so we don't need to poll anything. This is called from
        # _handle_socket_comms, so we need to override it
        return None

    def _handle_socket_comms(self):
        while self._open_sockets:
            self._service_subprocess(1.0)

    @contextlib.contextmanager
    def _setup_subprocess_socket(self):
        thread = threading.Thread(target=self._handle_socket_comms, daemon=True)

        requests, child_sock = socketpair()

        self._open_sockets[requests] = "requests"
        self.stdin = requests

        self.selector.register(
            requests,
            selectors.EVENT_READ,
            length_prefixed_frame_reader(self.handle_requests(log), on_close=self._on_socket_closed),
        )
        os.set_inheritable(child_sock.fileno(), True)
        os.environ["__AIRFLOW_SUPERVISOR_FD"] = str(child_sock.fileno())

        try:
            thread.start()
            yield child_sock
        finally:
            requests.close()
            child_sock.close()
            self._on_socket_closed(requests)
            thread.join(0)
            os.environ.pop("__AIRFLOW_SUPERVISOR_FD", None)

    @classmethod
    def start(  # type: ignore[override]
        cls,
        *,
        what: TaskInstance,
        task,
        logger: FilteringBoundLogger | None = None,
        **kwargs,
    ) -> TaskRunResult:
        """
        Run a task in-process without spawning a new child process.

        This bypasses the standard `ActivitySubprocess.start()` behavior, which expects
        to launch a subprocess and communicate via stdin/stdout. Instead, it constructs
        the `RuntimeTaskInstance` directly — useful in contexts like `dag.test()` where the
        Dag is already parsed in memory.

        Supervisor state and communications are simulated in-memory via `InProcessSupervisorComms`.
        """
        # Create supervisor instance
        supervisor = cls(
            id=what.id,
            pid=os.getpid(),  # Use current process
            process=psutil.Process(),  # Current process
            process_log=logger or structlog.get_logger(logger_name="task").bind(),
            client=cls._api_client(task.dag),
            **kwargs,
        )

        from airflow.sdk.execution_time.task_runner import RuntimeTaskInstance, finalize, run

        supervisor.comms = InProcessSupervisorComms(supervisor=supervisor)
        with set_supervisor_comms(supervisor.comms):
            supervisor.ti = what  # type: ignore[assignment]

            # We avoid calling `task_runner.startup()` because we are already inside a
            # parsed Dag file (e.g. via dag.test()).
            # In normal execution, `startup()` parses the Dag based on info in a `StartupDetails` message.
            # By directly constructing the `RuntimeTaskInstance`,
            #   we skip re-parsing (`task_runner.parse()`) and avoid needing to set Dag Bundle config
            #   and run the task in-process.
            start_date = datetime.now(tz=timezone.utc)
            ti_context = supervisor.client.task_instances.start(supervisor.id, supervisor.pid, start_date)

            ti = RuntimeTaskInstance.model_construct(
                **what.model_dump(exclude_unset=True),
                task=task,
                _ti_context_from_server=ti_context,
                max_tries=ti_context.max_tries,
                start_date=start_date,
                state=TaskInstanceState.RUNNING,
            )

            # Create a socketpair preemptively, in case the task process runs VirtualEnv operator or run_as_user
            with supervisor._setup_subprocess_socket():
                context = ti.get_template_context()
                log = structlog.get_logger(logger_name="task")

                state, msg, error = run(ti, context, log)
                finalize(ti, state, context, log, error)

                # In the normal subprocess model, the task runner calls this before exiting.
                # Since we're running in-process, we manually notify the API server that
                # the task has finished—unless the terminal state was already sent explicitly.
                supervisor.update_task_state_if_needed()

        return TaskRunResult(ti=ti, state=state, msg=msg, error=error)

    @staticmethod
    def _api_client(dag=None):
        api = in_process_api_server()
        if dag is not None:
            from airflow.api_fastapi.common.dagbag import dag_bag_from_app
            from airflow.models.dagbag import DBDagBag

            # This is needed since the Execution API server uses the DBDagBag in its "state".
            # This `app.state.dag_bag` is used to get some Dag properties like `fail_fast`.
            dag_bag = DBDagBag()

            api.app.dependency_overrides[dag_bag_from_app] = lambda: dag_bag

        client = InProcessTestSupervisor._Client(
            base_url=None, token="", dry_run=True, transport=api.transport
        )
        # Mypy is wrong -- the setter accepts a string on the property setter! `URLType = URL | str`
        client.base_url = "http://in-process.invalid./"
        return client

    def send_msg(
        self, msg: BaseModel | None, request_id: int, error: ErrorResponse | None = None, **dump_opts
    ):
        """Override to use in-process comms."""
        self.comms.messages.append(msg)

    @classmethod
    def run_trigger_in_process(cls, *, trigger, ti):
        """
        Run a trigger in-process for testing, similar to how we run tasks.

        This creates a minimal supervisor instance specifically for trigger execution
        and ensures the trigger has access to SUPERVISOR_COMMS for connection access.
        """
        # Create a minimal supervisor instance for trigger execution
        supervisor = cls(
            id=ti.id,
            pid=os.getpid(),  # Use current process
            process=psutil.Process(),  # Current process - note the underscore prefix
            process_log=structlog.get_logger(logger_name="task").bind(),
            client=cls._api_client(),
        )

        supervisor.comms = InProcessSupervisorComms(supervisor=supervisor)

        # Run the trigger with supervisor comms available
        with set_supervisor_comms(supervisor.comms):
            # Run the trigger's async generator and get the first event
            import asyncio

            async def _run_trigger():
                return await anext(trigger.run(), None)

            return asyncio.run(_run_trigger())

    @property
    def final_state(self):
        """Override to use in-process comms."""
        # Since we're running in-process, we don't have a final state until the task has finished.
        # We also don't have a process exit code to determine success/failure.
        return self._terminal_state


@contextmanager
def set_supervisor_comms(temp_comms):
    """
    Temporarily override `SUPERVISOR_COMMS` in the `task_runner` module.

    This is used to simulate task-runner ↔ supervisor communication in-process,
    by injecting a test Comms implementation (e.g. `InProcessSupervisorComms`)
    in place of the real inter-process communication layer.

    Some parts of the code (e.g. models.Variable.get) check for the presence
    of `task_runner.SUPERVISOR_COMMS` to determine if the code is running in a Task SDK execution context.
    This override ensures those code paths behave correctly during in-process tests.
    """
    from airflow.sdk.execution_time import task_runner

    sentinel = object()
    old = getattr(task_runner, "SUPERVISOR_COMMS", sentinel)

    if temp_comms is not None:
        task_runner.SUPERVISOR_COMMS = temp_comms
    elif old is not sentinel:
        delattr(task_runner, "SUPERVISOR_COMMS")

    try:
        yield
    finally:
        if old is sentinel:
            if hasattr(task_runner, "SUPERVISOR_COMMS"):
                delattr(task_runner, "SUPERVISOR_COMMS")
        else:
            task_runner.SUPERVISOR_COMMS = old


def run_task_in_process(ti: TaskInstance, task) -> TaskRunResult:
    """Run a task in-process for testing."""
    # Run the task
    return InProcessTestSupervisor.start(what=ti, task=task)


# Sockets, even the `.makefile()` function don't correctly do line buffering on reading. If a chunk is read
# and it doesn't contain a new line character, `.readline()` will just return the chunk as is.
#
# This returns a callback suitable for attaching to a `selector` that reads in to a buffer, and yields lines
# to a (sync) generator
def make_buffered_socket_reader(
    gen: Generator[None, bytes | bytearray, None],
    on_close: Callable[[socket], None],
    buffer_size: int = 4096,
):
    buffer = bytearray()  # This will hold our accumulated binary data
    read_buffer = bytearray(buffer_size)  # Temporary buffer for each read

    # We need to start up the generator to get it to the point it's at waiting on the yield
    next(gen)

    def cb(sock: socket):
        nonlocal buffer, read_buffer
        # Read up to `buffer_size` bytes of data from the socket
        n_received = sock.recv_into(read_buffer)

        if not n_received:
            # If no data is returned, the connection is closed. Return whatever is left in the buffer
            if len(buffer):
                with suppress(StopIteration):
                    gen.send(buffer)
            return False

        buffer.extend(read_buffer[:n_received])

        # We could have read multiple lines in one go, yield them all
        while (newline_pos := buffer.find(b"\n")) != -1:
            line = buffer[: newline_pos + 1]
            try:
                gen.send(line)
            except StopIteration:
                return False
            buffer = buffer[newline_pos + 1 :]  # Update the buffer with remaining data

        return True

    return cb, on_close


def length_prefixed_frame_reader(
    gen: Generator[None, _RequestFrame, None], on_close: Callable[[socket], None]
):
    length_needed: int | None = None
    # This will hold our accumulated/partial binary frame if it doesn't come in a single read
    buffer: memoryview | None = None
    # position in the buffer to store next read
    pos = 0
    decoder = msgspec.msgpack.Decoder[_RequestFrame](_RequestFrame)

    # We need to start up the generator to get it to the point it's at waiting on the yield
    next(gen)

    def cb(sock: socket):
        nonlocal buffer, length_needed, pos

        if length_needed is None:
            # Read the 32bit length of the frame
            bytes = sock.recv(4)
            if bytes == b"":
                return False

            length_needed = int.from_bytes(bytes, byteorder="big")
            buffer = memoryview(bytearray(length_needed))
        if length_needed and buffer:
            n = sock.recv_into(buffer[pos:])
            if n == 0:
                # EOF
                return False
            pos += n

            if pos >= length_needed:
                request = decoder.decode(buffer)
                buffer = None
                pos = 0
                length_needed = None
                try:
                    gen.send(request)
                except StopIteration:
                    return False
        return True

    return cb, on_close


def process_log_messages_from_subprocess(
    loggers: tuple[FilteringBoundLogger, ...],
) -> Generator[None, bytes | bytearray, None]:
    from structlog.stdlib import NAME_TO_LEVEL

    loggers = tuple(
        reconfigure_logger(
            log,
            structlog.processors.CallsiteParameterAdder,
            # We need these logger to print _everything_ they are given. The subprocess itself does the level
            # filtering.
            level_override=logging.NOTSET,
        )
        for log in loggers
    )

    while True:
        # Generator receive syntax, values are "sent" in  by the `make_buffered_socket_reader` and returned to
        # the yield.
        line = yield

        try:
            event = msgspec.json.decode(line)
        except Exception:
            log.exception("Malformed json log line", line=line)
            continue

        if ts := event.get("timestamp"):
            # We use msgspec to decode the timestamp as it does it orders of magnitude quicker than
            # datetime.strptime cn
            event["timestamp"] = msgspec.json.decode(f'"{ts}"', type=datetime)

        if exc := event.pop("exception", None):
            # TODO: convert the dict back to a pretty stack trace
            event["error_detail"] = exc

        if level := NAME_TO_LEVEL.get(event.pop("level")):
            msg = event.pop("event", None)
            for target in loggers:
                target.log(level, msg, **event)


def forward_to_log(
    target_loggers: tuple[FilteringBoundLogger, ...], logger: str, level: int
) -> Generator[None, bytes | bytearray, None]:
    while True:
        line = yield
        # Strip off new line
        line = line.rstrip()
        try:
            msg = line.decode("utf-8", errors="replace")
        except UnicodeDecodeError:
            msg = line.decode("ascii", errors="replace")
        for log in target_loggers:
            log.log(level, msg, logger=logger)


def ensure_secrets_backend_loaded() -> list[BaseSecretsBackend]:
    """
    Initialize secrets backend with auto-detected context.

    Detection strategy:
    1. SUPERVISOR_COMMS exists and is set → client chain (ExecutionAPISecretsBackend)
    2. _AIRFLOW_PROCESS_CONTEXT=server env var → server chain (MetastoreBackend)
    3. Neither → fallback chain (only env vars + external backends, no MetastoreBackend)

    Client contexts: task runner in worker (has SUPERVISOR_COMMS)
    Server contexts: API server, scheduler (set _AIRFLOW_PROCESS_CONTEXT=server)
    Fallback contexts: supervisor, unknown contexts (no SUPERVISOR_COMMS, no env var)

    The fallback chain ensures supervisor can use external secrets (AWS Secrets Manager,
    Vault, etc.) while falling back to API client, without trying MetastoreBackend.
    """
    import os

    from airflow.sdk.configuration import ensure_secrets_loaded
    from airflow.sdk.execution_time.secrets import DEFAULT_SECRETS_SEARCH_PATH_WORKERS

    # 1. Check for client context (SUPERVISOR_COMMS)
    try:
        from airflow.sdk.execution_time import task_runner

        if hasattr(task_runner, "SUPERVISOR_COMMS") and task_runner.SUPERVISOR_COMMS is not None:
            # Client context: task runner with SUPERVISOR_COMMS
            return ensure_secrets_loaded(default_backends=DEFAULT_SECRETS_SEARCH_PATH_WORKERS)
    except (ImportError, AttributeError):
        pass

    # 2. Check for explicit server context
    if os.environ.get("_AIRFLOW_PROCESS_CONTEXT") == "server":
        # Server context: API server, scheduler
        # uses the default server list
        return ensure_secrets_loaded()

    # 3. Fallback for unknown contexts (supervisor, etc.)
    # Only env vars + external backends from config, no MetastoreBackend, no ExecutionAPISecretsBackend
    fallback_backends = [
        "airflow.secrets.environment_variables.EnvironmentVariablesBackend",
    ]
    return ensure_secrets_loaded(default_backends=fallback_backends)


def _configure_logging(log_path: str, client: Client) -> tuple[FilteringBoundLogger, BinaryIO | TextIO]:
    # If we are told to write logs to a file, redirect the task logger to it. Make sure we append to the
    # file though, otherwise when we resume we would lose the logs from the start->deferral segment if it
    # lands on the same node as before.
    from airflow.sdk.log import init_log_file, logging_processors

    log_file_descriptor: BinaryIO | TextIO | None = None

    log_file = init_log_file(log_path)

    json_logs = True
    if json_logs:
        log_file_descriptor = log_file.open("ab")
        underlying_logger: WrappedLogger = structlog.BytesLogger(cast("BinaryIO", log_file_descriptor))
    else:
        log_file_descriptor = log_file.open("a", buffering=1)
        underlying_logger = structlog.WriteLogger(cast("TextIO", log_file_descriptor))

    with _remote_logging_conn(client):
        processors = logging_processors(json_output=json_logs)
    logger = structlog.wrap_logger(underlying_logger, processors=processors, logger_name="task").bind()

    return logger, log_file_descriptor


def supervise(
    *,
    ti: TaskInstance,
    bundle_info: BundleInfo,
    dag_rel_path: str | os.PathLike[str],
    token: str,
    server: str | None = None,
    dry_run: bool = False,
    log_path: str | None = None,
    subprocess_logs_to_stdout: bool = False,
    client: Client | None = None,
    sentry_integration: str = "",
) -> int:
    """
    Run a single task execution to completion.

    :param ti: The task instance to run.
    :param bundle_info: Information of the Dag bundle to use for this task instance.
    :param dag_rel_path: The file path to the Dag.
    :param token: Authentication token for the API client.
    :param server: Base URL of the API server.
    :param dry_run: If True, execute without actual task execution (simulate run).
    :param log_path: Path to write logs, if required.
    :param subprocess_logs_to_stdout: Should task logs also be sent to stdout via the main logger.
    :param client: Optional preconfigured client for communication with the server (Mostly for tests).
    :param sentry_integration: If the executor has a Sentry integration, import
        path to a callable to initialize it (empty means no integration).
    :return: Exit code of the process.
    :raises ValueError: If server URL is empty or invalid.
    """
    # One or the other
    from airflow.sdk._shared.secrets_masker import reset_secrets_masker

    if not client:
        if dry_run and server:
            raise ValueError(f"Can only specify one of {server=} or {dry_run=}")

        if not dry_run:
            if not server:
                raise ValueError(
                    "Invalid execution API server URL. Please ensure that a valid URL is configured."
                )

            try:
                parsed_url = urlparse(server)
            except Exception as e:
                raise ValueError(
                    f"Invalid execution API server URL '{server}': {e}. "
                    "Please ensure that a valid URL is configured."
                ) from e

            if parsed_url.scheme not in ("http", "https"):
                raise ValueError(
                    f"Invalid execution API server URL '{server}': "
                    "URL must use http:// or https:// scheme. "
                    "Please ensure that a valid URL is configured."
                )

            if not parsed_url.netloc:
                raise ValueError(
                    f"Invalid execution API server URL '{server}': "
                    "URL must include a valid host. "
                    "Please ensure that a valid URL is configured."
                )

    if not dag_rel_path:
        raise ValueError("dag_path is required")

    close_client = False
    if not client:
        limits = httpx.Limits(max_keepalive_connections=1, max_connections=10)
        client = Client(base_url=server or "", limits=limits, dry_run=dry_run, token=token)
        close_client = True
        log.debug("Connecting to execution API server", server=server)

    start = time.monotonic()

    # TODO: Use logging providers to handle the chunked upload for us etc.
    logger: FilteringBoundLogger | None = None
    log_file_descriptor: BinaryIO | TextIO | None = None
    if log_path:
        logger, log_file_descriptor = _configure_logging(log_path, client)

    backends = ensure_secrets_backend_loaded()
    log.info(
        "Secrets backends loaded for worker",
        count=len(backends),
        backend_classes=[type(b).__name__ for b in backends],
    )

    reset_secrets_masker()

    try:
        process = ActivitySubprocess.start(
            dag_rel_path=dag_rel_path,
            what=ti,
            client=client,
            logger=logger,
            bundle_info=bundle_info,
            subprocess_logs_to_stdout=subprocess_logs_to_stdout,
            sentry_integration=sentry_integration,
        )

        exit_code = process.wait()
        end = time.monotonic()
        log.info(
            "Task finished",
            task_instance_id=str(ti.id),
            exit_code=exit_code,
            duration=end - start,
            final_state=process.final_state,
        )
        return exit_code
    finally:
        if log_path and log_file_descriptor:
            log_file_descriptor.close()
        if close_client and client:
            with suppress(Exception):
                client.close()
