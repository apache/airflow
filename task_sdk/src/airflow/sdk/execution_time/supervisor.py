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
import weakref
from collections.abc import Generator
from contextlib import suppress
from datetime import datetime, timezone
from socket import socket, socketpair
from typing import TYPE_CHECKING, BinaryIO, Callable, ClassVar, Literal, NoReturn, cast, overload
from uuid import UUID

import attrs
import httpx
import msgspec
import psutil
import structlog
from pydantic import TypeAdapter

from airflow.sdk.api.client import Client
from airflow.sdk.api.datamodels._generated import TaskInstance, TerminalTIState
from airflow.sdk.execution_time.comms import (
    GetConnection,
    GetVariable,
    StartupDetails,
    ToSupervisor,
)

if TYPE_CHECKING:
    from structlog.typing import FilteringBoundLogger

    from airflow.sdk.api.datamodels.activities import ExecuteTaskActivity

__all__ = ["WatchedSubprocess", "supervise"]

log: FilteringBoundLogger = structlog.get_logger(logger_name="supervisor")

# TODO: Pull this from config
SLOWEST_HEARTBEAT_INTERVAL: int = 30
# Don't heartbeat more often than this
FASTEST_HEARTBEAT_INTERVAL: int = 5


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
    configure_logging(enable_pretty_log=False, output=log_io)


def _reopen_std_io_handles(child_stdin, child_stdout, child_stderr):
    if "PYTEST_CURRENT_TEST" in os.environ:
        # When we are running in pytest, it's output capturing messes us up. This works around it
        sys.stdout = sys.__stdout__
        sys.stderr = sys.__stderr__

    # Ensure that sys.stdout et al (and the underlying filehandles for C libraries etc) are connected to the
    # pipes from the supervisor

    for handle_name, sock, mode, close in (
        ("stdin", child_stdin, "r", True),
        ("stdout", child_stdout, "w", True),
        ("stderr", child_stderr, "w", False),
    ):
        handle = getattr(sys, handle_name)
        try:
            fd = handle.fileno()
            os.dup2(sock.fileno(), fd)
            if close:
                handle.close()
        except io.UnsupportedOperation:
            if "PYTEST_CURRENT_TEST" in os.environ:
                # When we're running under pytest, the stdin is not a real filehandle with an fd, so we need
                # to handle that differently
                fd = sock.fileno()
            else:
                raise
        # We can't open text mode fully unbuffered (python throws an exception if we try), but we can make it line buffered with `buffering=1`
        handle = os.fdopen(fd, mode, buffering=1)
        setattr(sys, handle_name, handle)


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
    last_chance_stderr = sys.__stderr__ or sys.stderr

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


@attrs.define()
class WatchedSubprocess:
    ti_id: UUID
    pid: int

    stdin: BinaryIO
    stdout: socket
    stderr: socket

    client: Client

    _process: psutil.Process
    _exit_code: int | None = None
    _terminal_state: str | None = None

    _last_heartbeat: float = 0

    selector: selectors.BaseSelector = attrs.field(factory=selectors.DefaultSelector)

    procs: ClassVar[weakref.WeakValueDictionary[int, WatchedSubprocess]] = weakref.WeakValueDictionary()

    def __attrs_post_init__(self):
        self.procs[self.pid] = self

    @classmethod
    def start(
        cls,
        path: str | os.PathLike[str],
        ti: TaskInstance,
        client: Client,
        target: Callable[[], None] = _subprocess_main,
    ) -> WatchedSubprocess:
        """Fork and start a new subprocess to execute the given task."""
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

            # Run the child entrypoint
            _fork_main(child_stdin, child_stdout, child_stderr, child_logs.fileno(), target)

        proc = cls(
            ti_id=ti.id,
            pid=pid,
            stdin=feed_stdin,
            stdout=read_stdout,
            stderr=read_stderr,
            process=psutil.Process(pid),
            client=client,
        )

        # We've forked, but the task won't start until we send it the StartupDetails message. But before we do
        # that, we need to tell the server it's started (so it has the chance to tell us "no, stop!" for any
        # reason)
        try:
            client.task_instances.start(ti.id, pid, datetime.now(tz=timezone.utc))
            proc._last_heartbeat = time.monotonic()
        except Exception:
            # On any error kill that subprocess!
            proc.kill(signal.SIGKILL)
            raise

        proc._register_pipes(read_msgs, read_logs)

        # Close the remaining parent-end of the sockets we've passed to the child via fork. We still have the
        # other end of the pair open
        proc._close_unused_sockets(child_stdout, child_stdin, child_comms, child_logs)

        # Tell the task process what it needs to do!
        proc._send_startup_message(ti, path, child_comms)
        return proc

    def _register_pipes(self, read_msgs, read_logs):
        """Register handlers for subprocess communication channels."""
        # self.selector is a way of registering a handler/callback to be called when the given IO channel has
        # activity to read on (https://www.man7.org/linux/man-pages/man2/select.2.html etc, but better
        # alternatives are used automatically) -- this is a way of having "event-based" code, but without
        # needing full async, to read and process output from each socket as it is received.

        # TODO: Use logging providers to handle the chunked upload for us
        logger: FilteringBoundLogger = structlog.get_logger(logger_name="task").bind()

        self.selector.register(
            self.stdout, selectors.EVENT_READ, self._create_socket_handler(logger, "stdout")
        )
        self.selector.register(
            self.stderr,
            selectors.EVENT_READ,
            self._create_socket_handler(logger, "stderr", log_level=logging.ERROR),
        )
        self.selector.register(
            read_logs,
            selectors.EVENT_READ,
            make_buffered_socket_reader(process_log_messages_from_subprocess(logger)),
        )
        self.selector.register(
            read_msgs, selectors.EVENT_READ, make_buffered_socket_reader(self.handle_requests(log))
        )

    @staticmethod
    def _create_socket_handler(logger, channel, log_level=logging.INFO) -> Callable[[socket], bool]:
        """Create a socket handler that forwards logs to a logger."""
        return make_buffered_socket_reader(forward_to_log(logger.bind(chan=channel), level=log_level))

    @staticmethod
    def _close_unused_sockets(*sockets):
        """Close unused ends of sockets after fork."""
        for sock in sockets:
            sock.close()

    def _send_startup_message(self, ti: TaskInstance, path: str | os.PathLike[str], child_comms: socket):
        """Send startup message to the subprocess."""
        msg = StartupDetails(
            ti=ti,
            file=str(path),
            requests_fd=child_comms.fileno(),
        )

        # Send the message to tell the process what it needs to execute
        log.debug("Sending", msg=msg)
        self.stdin.write(msg.model_dump_json().encode())
        self.stdin.write(b"\n")

    def kill(self, signal: signal.Signals = signal.SIGINT):
        if self._exit_code is not None:
            return

        with suppress(ProcessLookupError):
            os.kill(self.pid, signal)

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

        self.client.task_instances.finish(
            id=self.ti_id, state=self.final_state, when=datetime.now(tz=timezone.utc)
        )
        return self._exit_code

    def _monitor_subprocess(self):
        """
        Monitor the subprocess until it exits.

        This function:

        - Polls the subprocess for output
        - Sends heartbeats to the client to keep the task alive
        - Checks if the subprocess has exited
        """
        # Until we have a selector for the process, don't poll for more than 10s, just in case it exists but
        # doesn't produce any output
        max_poll_interval = 10

        while self._exit_code is None or len(self.selector.get_map()):
            last_heartbeat_ago = time.monotonic() - self._last_heartbeat
            # Monitor the task to see if it's done. Wait in a syscall (`select`) for as long as possible
            # so we notice the subprocess finishing as quick as we can.
            max_wait_time = max(
                0,  # Make sure this value is never negative,
                min(
                    # Ensure we heartbeat _at most_ 75% through time the zombie threshold time
                    SLOWEST_HEARTBEAT_INTERVAL - last_heartbeat_ago * 0.75,
                    max_poll_interval,
                ),
            )
            events = self.selector.select(timeout=max_wait_time)
            for key, _ in events:
                socket_handler = key.data
                need_more = socket_handler(key.fileobj)

                if not need_more:
                    self.selector.unregister(key.fileobj)
                    key.fileobj.close()  # type: ignore[union-attr]

            self._check_subprocess_exit()
            self._send_heartbeat_if_needed()

    def _check_subprocess_exit(self):
        """Check if the subprocess has exited."""
        if self._exit_code is None:
            try:
                self._exit_code = self._process.wait(timeout=0)
                log.debug("Task process exited", exit_code=self._exit_code)
            except psutil.TimeoutExpired:
                pass

    def _send_heartbeat_if_needed(self):
        """Send a heartbeat to the client if heartbeat interval has passed."""
        if time.monotonic() - self._last_heartbeat >= FASTEST_HEARTBEAT_INTERVAL:
            try:
                self.client.task_instances.heartbeat(self.ti_id, pid=self._process.pid)
                self._last_heartbeat = time.monotonic()
            except Exception:
                log.warning("Failed to send heartbeat", exc_info=True)
                # TODO: If we couldn't heartbeat for X times the interval, kill ourselves
                pass

    @property
    def final_state(self):
        """
        The final state of the TaskInstance.

        By default this will be derived from the exit code of the task
        (0=success, failed otherwise) but can be changed by the subprocess
        sending a TaskState message, as long as the process exits with 0

        Not valid before the process has finished.
        """
        if self._exit_code == 0:
            return self._terminal_state or TerminalTIState.SUCCESS
        return TerminalTIState.FAILED

    def __rich_repr__(self):
        yield "pid", self.pid
        yield "exit_code", self._exit_code, None

    __rich_repr__.angular = True  # type: ignore[attr-defined]

    def __repr__(self) -> str:
        rep = f"<WatchedSubprocess pid={self.pid}"
        if self._exit_code is not None:
            rep += f" exit_code={self._exit_code}"
        return rep + " >"

    def handle_requests(self, log: FilteringBoundLogger) -> Generator[None, bytes, None]:
        """Handle incoming requests from the task process, respond with the appropriate data."""
        decoder = TypeAdapter[ToSupervisor](ToSupervisor)

        while True:
            line = yield

            try:
                msg = decoder.validate_json(line)
            except Exception:
                log.exception("Unable to decode message", line=line)
                continue

            # if isinstance(msg, TaskState):
            #     self._terminal_state = msg.state
            # elif isinstance(msg, ReadXCom):
            #     resp = XComResponse(key="secret", value=True)
            #     encoder.encode_into(resp, buffer)
            #     self.stdin.write(buffer + b"\n")
            if isinstance(msg, GetConnection):
                conn = self.client.connections.get(msg.conn_id)
                resp = conn.model_dump_json(exclude_unset=True).encode()
            elif isinstance(msg, GetVariable):
                var = self.client.variables.get(msg.key)
                resp = var.model_dump_json(exclude_unset=True).encode()
            else:
                log.error("Unhandled request", msg=msg)
                continue

            self.stdin.write(resp + b"\n")


# Sockets, even the `.makefile()` function don't correctly do line buffering on reading. If a chunk is read
# and it doesn't contain a new line character, `.readline()` will just return the chunk as is.
#
# This returns a callback suitable for attaching to a `selector` that reads in to a buffer, and yields lines
# to a (sync) generator
def make_buffered_socket_reader(
    gen: Generator[None, bytes, None], buffer_size: int = 4096
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
            return False

        buffer.extend(read_buffer[:n_received])

        # We could have read multiple lines in one go, yield them all
        while (newline_pos := buffer.find(b"\n")) != -1:
            if TYPE_CHECKING:
                # We send in a memoryvuew, but pretend it's a bytes, as Buffer is only in 3.12+
                line = buffer[: newline_pos + 1]
            else:
                line = memoryview(buffer)[: newline_pos + 1]  # Include the newline character
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


def supervise(activity: ExecuteTaskActivity, server: str | None = None, dry_run: bool = False) -> int:
    """
    Run a single task execution to completion.

    Returns the exit code of the process
    """
    # One or the other
    if (server == "") ^ dry_run:
        raise ValueError(f"Can only specify one of {server=} or {dry_run=}")

    if not activity.path:
        raise ValueError("path filed of activity missing")

    limits = httpx.Limits(max_keepalive_connections=1, max_connections=10)
    client = Client(base_url=server or "", limits=limits, dry_run=dry_run, token=activity.token)

    start = time.monotonic()

    process = WatchedSubprocess.start(activity.path, activity.ti, client=client)

    exit_code = process.wait()
    end = time.monotonic()
    log.debug("Task finished", exit_code=exit_code, duration=end - start)
    return exit_code
