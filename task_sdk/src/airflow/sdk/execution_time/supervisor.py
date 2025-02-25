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
import io
import logging
import os
import selectors
import signal
import sys
import time
from collections.abc import Generator
from contextlib import suppress
from datetime import datetime, timezone
from http import HTTPStatus
from socket import socket, socketpair
from typing import (
    TYPE_CHECKING,
    BinaryIO,
    Callable,
    ClassVar,
    Literal,
    NoReturn,
    TextIO,
    cast,
    overload,
)
from uuid import UUID

import attrs
import httpx
import msgspec
import psutil
import structlog
from pydantic import TypeAdapter

from airflow.sdk.api.client import Client, ServerResponseError
from airflow.sdk.api.datamodels._generated import (
    ConnectionResponse,
    IntermediateTIState,
    TaskInstance,
    TerminalTIState,
    VariableResponse,
)
from airflow.sdk.execution_time.comms import (
    AssetResult,
    ConnectionResult,
    DeferTask,
    GetAssetByName,
    GetAssetByUri,
    GetConnection,
    GetPrevSuccessfulDagRun,
    GetVariable,
    GetXCom,
    GetXComCount,
    PrevSuccessfulDagRunResult,
    PutVariable,
    RescheduleTask,
    RuntimeCheckOnTask,
    SetRenderedFields,
    SetXCom,
    StartupDetails,
    SucceedTask,
    TaskState,
    ToSupervisor,
    VariableResult,
    XComCountResponse,
    XComResult,
)

if TYPE_CHECKING:
    from structlog.typing import FilteringBoundLogger, WrappedLogger

    from airflow.executors.workloads import BundleInfo
    from airflow.typing_compat import Self


__all__ = ["ActivitySubprocess", "WatchedSubprocess", "supervise"]

log: FilteringBoundLogger = structlog.get_logger(logger_name="supervisor")

# TODO: Pull this from config
#  (previously `[scheduler] local_task_job_heartbeat_sec` with the following as fallback if it is 0:
#  `[scheduler] scheduler_zombie_task_threshold`)
HEARTBEAT_THRESHOLD: int = 30
# Don't heartbeat more often than this
MIN_HEARTBEAT_INTERVAL: int = 5
MAX_FAILED_HEARTBEATS: int = 3

# These are the task instance states that require some additional information to transition into.
# "Directly" here means that the PATCH API calls to transition into these states are
# made from _handle_request() itself and don't have to come all the way to wait().
STATES_SENT_DIRECTLY = [
    IntermediateTIState.DEFERRED,
    IntermediateTIState.UP_FOR_RESCHEDULE,
    TerminalTIState.SUCCESS,
]


@overload
def mkpipe() -> tuple[socket, socket]: ...


@overload
def mkpipe(remote_read: Literal[True]) -> tuple[socket, BinaryIO]: ...


def mkpipe(
    remote_read: bool = False,
) -> tuple[socket, socket | BinaryIO]:
    """
    Create a pair of connected sockets.

    The inheritable flag will be set correctly so that the end destined for the subprocess is kept open but
    the end for this process is closed automatically by the OS.
    """
    rsock, wsock = socketpair()
    local, remote = (wsock, rsock) if remote_read else (rsock, wsock)

    remote.set_inheritable(True)
    local.setblocking(False)

    io: BinaryIO | socket
    if remote_read:
        # If _we_ are writing, we don't want to buffer
        io = cast(BinaryIO, local.makefile("wb", buffering=0))
    else:
        io = local

    return remote, io


def _subprocess_main():
    from airflow.sdk.execution_time.task_runner import main

    main()


def _reset_signals():
    # Uninstall the rich etc. exception handler
    sys.excepthook = sys.__excepthook__
    signal.signal(signal.SIGINT, signal.SIG_DFL)
    signal.signal(signal.SIGUSR2, signal.SIG_DFL)


def _configure_logs_over_json_channel(log_fd: int):
    # A channel that the task can send JSON-formated logs over.
    #
    # JSON logs sent this way will be handled nicely
    from airflow.sdk.log import configure_logging

    log_io = os.fdopen(log_fd, "wb", buffering=0)
    configure_logging(enable_pretty_log=False, output=log_io, sending_to_supervisor=True)


def _reopen_std_io_handles(child_stdin, child_stdout, child_stderr):
    # Ensure that sys.stdout et al (and the underlying filehandles for C libraries etc) are connected to the
    # pipes from the supervisor

    for handle_name, fd, sock, mode in (
        ("stdin", 0, child_stdin, "r"),
        ("stdout", 1, child_stdout, "w"),
        ("stderr", 2, child_stderr, "w"),
    ):
        handle = getattr(sys, handle_name)
        handle.close()
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


def _fork_main(
    child_stdin: socket,
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
    _reopen_std_io_handles(child_stdin, child_stdout, child_stderr)

    def exit(n: int) -> NoReturn:
        with suppress(ValueError, OSError):
            sys.stdout.flush()
        with suppress(ValueError, OSError):
            sys.stderr.flush()
        with suppress(ValueError, OSError):
            last_chance_stderr.flush()
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
            last_chance_stderr.write("--- Last chance exception handler ---\n")
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

    stdin: BinaryIO
    """The handle connected to stdin of the child process"""

    decoder: ClassVar[TypeAdapter]
    """The decoder to use for incoming messages from the child process."""

    _process: psutil.Process
    _requests_fd: int
    """File descriptor for request handling."""

    _num_open_sockets: int = 4
    _exit_code: int | None = attrs.field(default=None, init=False)

    selector: selectors.BaseSelector = attrs.field(factory=selectors.DefaultSelector)

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
        child_stdin, feed_stdin = mkpipe(remote_read=True)
        child_stdout, read_stdout = mkpipe()
        child_stderr, read_stderr = mkpipe()

        # Open these socketpair before forking off the child, so that it is open when we fork.
        child_comms, read_msgs = mkpipe()
        child_logs, read_logs = mkpipe()

        pid = os.fork()
        if pid == 0:
            # Parent ends of the sockets are closed by the OS as they are set as non-inheritable

            # Python GC should delete these for us, but lets make double sure that we don't keep anything
            # around in the forked processes, especially things that might involve open files or sockets!
            del constructor_kwargs
            del logger

            try:
                # Run the child entrypoint
                _fork_main(child_stdin, child_stdout, child_stderr, child_logs.fileno(), target)
            except BaseException as e:
                try:
                    # We can't use log here, as if we except out of _fork_main something _weird_ went on.
                    print("Exception in _fork_main, exiting with code 124", e, file=sys.stderr)
                except BaseException as e:
                    pass

            # It's really super super important we never exit this block. We are in the forked child, and if we
            # do then _THINGS GET WEIRD_.. (Normally `_fork_main` itself will `_exit()` so we never get here)
            os._exit(124)

        requests_fd = child_comms.fileno()

        # Close the remaining parent-end of the sockets we've passed to the child via fork. We still have the
        # other end of the pair open
        cls._close_unused_sockets(child_stdin, child_stdout, child_stderr, child_comms, child_logs)

        proc = cls(
            pid=pid,
            stdin=feed_stdin,
            process=psutil.Process(pid),
            requests_fd=requests_fd,
            **constructor_kwargs,
        )

        logger = logger or cast("FilteringBoundLogger", structlog.get_logger(logger_name="task").bind())
        proc._register_pipe_readers(
            logger=logger,
            stdout=read_stdout,
            stderr=read_stderr,
            requests=read_msgs,
            logs=read_logs,
        )

        return proc

    def _register_pipe_readers(
        self, logger: FilteringBoundLogger, stdout: socket, stderr: socket, requests: socket, logs: socket
    ):
        """Register handlers for subprocess communication channels."""
        # self.selector is a way of registering a handler/callback to be called when the given IO channel has
        # activity to read on (https://www.man7.org/linux/man-pages/man2/select.2.html etc, but better
        # alternatives are used automatically) -- this is a way of having "event-based" code, but without
        # needing full async, to read and process output from each socket as it is received.

        self.selector.register(stdout, selectors.EVENT_READ, self._create_socket_handler(logger, "stdout"))
        self.selector.register(
            stderr,
            selectors.EVENT_READ,
            self._create_socket_handler(logger, "stderr", log_level=logging.ERROR),
        )
        self.selector.register(
            logs,
            selectors.EVENT_READ,
            make_buffered_socket_reader(
                process_log_messages_from_subprocess(logger), on_close=self._on_socket_closed
            ),
        )
        self.selector.register(
            requests,
            selectors.EVENT_READ,
            make_buffered_socket_reader(self.handle_requests(log), on_close=self._on_socket_closed),
        )

    def _create_socket_handler(self, logger, channel, log_level=logging.INFO) -> Callable[[socket], bool]:
        """Create a socket handler that forwards logs to a logger."""
        return make_buffered_socket_reader(
            forward_to_log(logger.bind(chan=channel), level=log_level), on_close=self._on_socket_closed
        )

    def _on_socket_closed(self):
        # We want to keep servicing this process until we've read up to EOF from all the sockets.
        self._num_open_sockets -= 1

    def handle_requests(self, log: FilteringBoundLogger) -> Generator[None, bytes, None]:
        """Handle incoming requests from the task process, respond with the appropriate data."""
        while True:
            line = yield

            try:
                msg = self.decoder.validate_json(line)
            except Exception:
                log.exception("Unable to decode message", line=line)
                continue

            self._handle_request(msg, log)

    def _handle_request(self, msg, log: FilteringBoundLogger) -> None:
        raise NotImplementedError()

    @staticmethod
    def _close_unused_sockets(*sockets):
        """Close unused ends of sockets after fork."""
        for sock in sockets:
            sock.close()

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
        escalation_path = [signal.SIGINT, signal.SIGTERM, signal.SIGKILL]

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
                        exit_code := self._service_subprocess(max_wait_time=end - now, raise_on_timeout=False)
                    ) is not None:
                        log.info("Process exited", pid=self.pid, exit_code=exit_code, signal=sig.name)
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

    def _service_subprocess(self, max_wait_time: float, raise_on_timeout: bool = False):
        """
        Service subprocess events by processing socket activity and checking for process exit.

        This method:
        - Waits for activity on the registered file objects (via `self.selector.select`).
        - Processes any events triggered on these file objects.
        - Checks if the subprocess has exited during the wait.

        :param max_wait_time: Maximum time to block while waiting for events, in seconds.
        :param raise_on_timeout: If True, raise an exception if the subprocess does not exit within the timeout.
        :returns: The process exit code, or None if it's still alive
        """
        events = self.selector.select(timeout=max_wait_time)
        for key, _ in events:
            # Retrieve the handler responsible for processing this file object (e.g., stdout, stderr)
            socket_handler = key.data

            # Example of handler behavior:
            # If the subprocess writes "Hello, World!" to stdout:
            # - `socket_handler` reads and processes the message.
            # - If EOF is reached, the handler returns False to signal no more reads are expected.
            need_more = socket_handler(key.fileobj)

            # If the handler signals that the file object is no longer needed (EOF, closed, etc.)
            # unregister it from the selector to stop monitoring; `wait()` blocks until all selectors
            # are removed.
            if not need_more:
                self.selector.unregister(key.fileobj)
                key.fileobj.close()  # type: ignore[union-attr]

        # Check if the subprocess has exited
        return self._check_subprocess_exit(raise_on_timeout=raise_on_timeout)

    def _check_subprocess_exit(self, raise_on_timeout: bool = False) -> int | None:
        """Check if the subprocess has exited."""
        if self._exit_code is None:
            try:
                self._exit_code = self._process.wait(timeout=0)
                log.debug("Workload process exited", exit_code=self._exit_code)
            except psutil.TimeoutExpired:
                if raise_on_timeout:
                    raise
        return self._exit_code


@attrs.define(kw_only=True)
class ActivitySubprocess(WatchedSubprocess):
    client: Client
    """The HTTP client to use for communication with the API server."""

    _terminal_state: str | None = attrs.field(default=None, init=False)
    _final_state: str | None = attrs.field(default=None, init=False)

    _last_successful_heartbeat: float = attrs.field(default=0, init=False)
    _last_heartbeat_attempt: float = attrs.field(default=0, init=False)

    # After the failure of a heartbeat, we'll increment this counter. If it reaches `MAX_FAILED_HEARTBEATS`, we
    # will kill the process. This is to handle temporary network issues etc. ensuring that the process
    # does not hang around forever.
    failed_heartbeats: int = attrs.field(default=0, init=False)

    # Maximum possible time (in seconds) that task will have for execution of auxiliary processes
    # like listeners after task is complete.
    # TODO: This should come from airflow.cfg: [core] task_success_overtime
    TASK_OVERTIME_THRESHOLD: ClassVar[float] = 20.0
    _task_end_time_monotonic: float | None = attrs.field(default=None, init=False)

    decoder: ClassVar[TypeAdapter[ToSupervisor]] = TypeAdapter(ToSupervisor)

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
        **kwargs,
    ) -> Self:
        """Fork and start a new subprocess to execute the given task."""
        proc: Self = super().start(id=what.id, client=client, target=target, logger=logger, **kwargs)
        # Tell the task process what it needs to do!
        proc._on_child_started(ti=what, dag_rel_path=dag_rel_path, bundle_info=bundle_info)
        return proc

    def _on_child_started(self, ti: TaskInstance, dag_rel_path: str | os.PathLike[str], bundle_info):
        """Send startup message to the subprocess."""
        start_date = datetime.now(tz=timezone.utc)
        try:
            # We've forked, but the task won't start doing anything until we send it the StartupDetails
            # message. But before we do that, we need to tell the server it's started (so it has the chance to
            # tell us "no, stop!" for any reason)
            ti_context = self.client.task_instances.start(ti.id, self.pid, start_date)
            self._last_successful_heartbeat = time.monotonic()
        except Exception:
            # On any error kill that subprocess!
            self.kill(signal.SIGKILL)
            raise

        msg = StartupDetails.model_construct(
            ti=ti,
            dag_rel_path=os.fspath(dag_rel_path),
            bundle_info=bundle_info,
            requests_fd=self._requests_fd,
            ti_context=ti_context,
            start_date=start_date,
        )

        # Send the message to tell the process what it needs to execute
        log.debug("Sending", msg=msg)

        try:
            self.stdin.write(msg.model_dump_json().encode())
            self.stdin.write(b"\n")
        except BrokenPipeError:
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

        # If the process has finished non-directly patched state (directly means deferred, reschedule, etc.),
        # update the state of the TaskInstance to reflect the final state of the process.
        # For states like `deferred`, `up_for_reschedule`, the process will exit with 0, but the state will be updated
        # by the subprocess in the `handle_requests` method.
        if self.final_state not in STATES_SENT_DIRECTLY:
            self.client.task_instances.finish(
                id=self.id, state=self.final_state, when=datetime.now(tz=timezone.utc)
            )
        return self._exit_code

    def _monitor_subprocess(self):
        """
        Monitor the subprocess until it exits.

        This function:

        - Waits for activity on file objects (e.g., subprocess stdout, stderr, logs, requests) using the selector.
        - Processes events triggered on the monitored file objects, such as data availability or EOF.
        - Sends heartbeats to ensure the process is alive and checks if the subprocess has exited.
        """
        while self._exit_code is None or self._num_open_sockets > 0:
            last_heartbeat_ago = time.monotonic() - self._last_successful_heartbeat
            # Monitor the task to see if it's done. Wait in a syscall (`select`) for as long as possible
            # so we notice the subprocess finishing as quick as we can.
            max_wait_time = max(
                0,  # Make sure this value is never negative,
                min(
                    # Ensure we heartbeat _at most_ 75% through time the zombie threshold time
                    HEARTBEAT_THRESHOLD - last_heartbeat_ago * 0.75,
                    MIN_HEARTBEAT_INTERVAL,
                ),
            )
            # Block until events are ready or the timeout is reached
            # This listens for activity (e.g., subprocess output) on registered file objects
            alive = self._service_subprocess(max_wait_time=max_wait_time) is None

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
            and (time.monotonic() - self._task_end_time_monotonic) > self.TASK_OVERTIME_THRESHOLD
        ):
            log.warning("Workload success overtime reached; terminating process", ti_id=self.id)
            self.kill(signal.SIGTERM, force=True)

    def _send_heartbeat_if_needed(self):
        """Send a heartbeat to the client if heartbeat interval has passed."""
        # Respect the minimum interval between heartbeat attempts
        if (time.monotonic() - self._last_heartbeat_attempt) < MIN_HEARTBEAT_INTERVAL:
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
                )
                self.kill(signal.SIGTERM, force=True)
            else:
                # If we get any other error, we'll just log it and try again next time
                self._handle_heartbeat_failures()
        except Exception:
            self._handle_heartbeat_failures()

    def _handle_heartbeat_failures(self):
        """Increment the failed heartbeats counter and kill the process if too many failures."""
        self.failed_heartbeats += 1
        log.warning(
            "Failed to send heartbeat. Will be retried",
            failed_heartbeats=self.failed_heartbeats,
            ti_id=self.id,
            max_retries=MAX_FAILED_HEARTBEATS,
            exc_info=True,
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
            return self._terminal_state or TerminalTIState.SUCCESS
        return TerminalTIState.FAILED

    def _handle_request(self, msg: ToSupervisor, log: FilteringBoundLogger):
        log.debug("Received message from task runner", msg=msg)
        resp = None
        if isinstance(msg, TaskState):
            self._terminal_state = msg.state
            self._task_end_time_monotonic = time.monotonic()
        elif isinstance(msg, RuntimeCheckOnTask):
            runtime_check_resp = self.client.task_instances.runtime_checks(id=self.id, msg=msg)
            resp = runtime_check_resp.model_dump_json().encode()
        elif isinstance(msg, SucceedTask):
            self._terminal_state = msg.state
            self._task_end_time_monotonic = time.monotonic()
            self.client.task_instances.succeed(
                id=self.id,
                when=msg.end_date,
                task_outlets=msg.task_outlets,
                outlet_events=msg.outlet_events,
            )
        elif isinstance(msg, GetConnection):
            conn = self.client.connections.get(msg.conn_id)
            if isinstance(conn, ConnectionResponse):
                conn_result = ConnectionResult.from_conn_response(conn)
                resp = conn_result.model_dump_json(exclude_unset=True, by_alias=True).encode()
            else:
                resp = conn.model_dump_json().encode()
        elif isinstance(msg, GetVariable):
            var = self.client.variables.get(msg.key)
            if isinstance(var, VariableResponse):
                var_result = VariableResult.from_variable_response(var)
                resp = var_result.model_dump_json(exclude_unset=True).encode()
            else:
                resp = var.model_dump_json().encode()
        elif isinstance(msg, GetXCom):
            xcom = self.client.xcoms.get(msg.dag_id, msg.run_id, msg.task_id, msg.key, msg.map_index)
            xcom_result = XComResult.from_xcom_response(xcom)
            resp = xcom_result.model_dump_json().encode()
        elif isinstance(msg, GetXComCount):
            len = self.client.xcoms.head(msg.dag_id, msg.run_id, msg.task_id, msg.key)
            resp = XComCountResponse(len=len).model_dump_json().encode()
        elif isinstance(msg, DeferTask):
            self._terminal_state = IntermediateTIState.DEFERRED
            self.client.task_instances.defer(self.id, msg)
        elif isinstance(msg, RescheduleTask):
            self._terminal_state = IntermediateTIState.UP_FOR_RESCHEDULE
            self.client.task_instances.reschedule(self.id, msg)
        elif isinstance(msg, SetXCom):
            self.client.xcoms.set(msg.dag_id, msg.run_id, msg.task_id, msg.key, msg.value, msg.map_index)
        elif isinstance(msg, PutVariable):
            self.client.variables.set(msg.key, msg.value, msg.description)
        elif isinstance(msg, SetRenderedFields):
            self.client.task_instances.set_rtif(self.id, msg.rendered_fields)
        elif isinstance(msg, GetAssetByName):
            asset_resp = self.client.assets.get(name=msg.name)
            asset_result = AssetResult.from_asset_response(asset_resp)
            resp = asset_result.model_dump_json(exclude_unset=True).encode()
        elif isinstance(msg, GetAssetByUri):
            asset_resp = self.client.assets.get(uri=msg.uri)
            asset_result = AssetResult.from_asset_response(asset_resp)
            resp = asset_result.model_dump_json(exclude_unset=True).encode()
        elif isinstance(msg, GetPrevSuccessfulDagRun):
            dagrun_resp = self.client.task_instances.get_previous_successful_dagrun(self.id)
            dagrun_result = PrevSuccessfulDagRunResult.from_dagrun_response(dagrun_resp)
            resp = dagrun_result.model_dump_json(exclude_unset=True).encode()
        else:
            log.error("Unhandled request", msg=msg)
            return

        if resp:
            self.stdin.write(resp + b"\n")


# Sockets, even the `.makefile()` function don't correctly do line buffering on reading. If a chunk is read
# and it doesn't contain a new line character, `.readline()` will just return the chunk as is.
#
# This returns a callback suitable for attaching to a `selector` that reads in to a buffer, and yields lines
# to a (sync) generator
def make_buffered_socket_reader(
    gen: Generator[None, bytes, None],
    on_close: Callable,
    buffer_size: int = 4096,
) -> Callable[[socket], bool]:
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
                gen.send(buffer)
            # Tell loop to close this selector
            on_close()
            return False

        buffer.extend(read_buffer[:n_received])

        # We could have read multiple lines in one go, yield them all
        while (newline_pos := buffer.find(b"\n")) != -1:
            line = buffer[: newline_pos + 1]
            gen.send(line)
            buffer = buffer[newline_pos + 1 :]  # Update the buffer with remaining data

        return True

    return cb


def process_log_messages_from_subprocess(log: FilteringBoundLogger) -> Generator[None, bytes, None]:
    from structlog.stdlib import NAME_TO_LEVEL

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
            #
            # We remove the timezone info here, as the json encoding has `+00:00`, and since the log came
            # from a subprocess we know that the timezone of the log message is the same, so having some
            # messages include tz (from subprocess) but others not (ones from supervisor process) is
            # confusing.
            event["timestamp"] = msgspec.json.decode(f'"{ts}"', type=datetime).replace(tzinfo=None)

        if exc := event.pop("exception", None):
            # TODO: convert the dict back to a pretty stack trace
            event["error_detail"] = exc
        log.log(NAME_TO_LEVEL[event.pop("level")], event.pop("event", None), **event)


def forward_to_log(target_log: FilteringBoundLogger, level: int) -> Generator[None, bytes, None]:
    while True:
        buf = yield
        line = bytes(buf)
        # Strip off new line
        line = line.rstrip()
        try:
            msg = line.decode("utf-8", errors="replace")
            target_log.log(level, msg)
        except UnicodeDecodeError:
            msg = line.decode("ascii", errors="replace")
            target_log.log(level, msg)


def supervise(
    *,
    ti: TaskInstance,
    bundle_info: BundleInfo,
    dag_rel_path: str | os.PathLike[str],
    token: str,
    server: str | None = None,
    dry_run: bool = False,
    log_path: str | None = None,
    client: Client | None = None,
) -> int:
    """
    Run a single task execution to completion.

    :param ti: The task instance to run.
    :param dr: Current DagRun of the task instance.
    :param dag_path: The file path to the DAG.
    :param token: Authentication token for the API client.
    :param server: Base URL of the API server.
    :param dry_run: If True, execute without actual task execution (simulate run).
    :param log_path: Path to write logs, if required.
    :param client: Optional preconfigured client for communication with the server (Mostly for tests).
    :return: Exit code of the process.
    """
    # One or the other
    if not client and ((not server) ^ dry_run):
        raise ValueError(f"Can only specify one of {server=} or {dry_run=}")

    if not dag_rel_path:
        raise ValueError("dag_path is required")

    if not client:
        limits = httpx.Limits(max_keepalive_connections=1, max_connections=10)
        client = Client(base_url=server or "", limits=limits, dry_run=dry_run, token=token)

    start = time.monotonic()

    # TODO: Use logging providers to handle the chunked upload for us etc.
    logger: FilteringBoundLogger | None = None
    if log_path:
        # If we are told to write logs to a file, redirect the task logger to it.
        from airflow.sdk.log import init_log_file, logging_processors

        log_file = init_log_file(log_path)

        pretty_logs = False
        if pretty_logs:
            underlying_logger: WrappedLogger = structlog.WriteLogger(log_file.open("w", buffering=1))
        else:
            underlying_logger = structlog.BytesLogger(log_file.open("wb"))
        processors = logging_processors(enable_pretty_log=pretty_logs)[0]
        logger = structlog.wrap_logger(underlying_logger, processors=processors, logger_name="task").bind()

    process = ActivitySubprocess.start(
        dag_rel_path=dag_rel_path,
        what=ti,
        client=client,
        logger=logger,
        bundle_info=bundle_info,
    )

    exit_code = process.wait()
    end = time.monotonic()
    log.info("Task finished", exit_code=exit_code, duration=end - start, final_state=process.final_state)
    return exit_code
