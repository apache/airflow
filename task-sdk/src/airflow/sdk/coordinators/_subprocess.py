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
Common subprocess coordinator scaffolding.

Coordinators that launch a subprocess and communicate with it over two TCP
sockets (``--comm`` and ``--logs``) — Java, native executables, and any
future runtime that follows the same wire convention — can subclass
:class:`SubprocessCoordinator` and reuse the resource-tracking, accept, and
draining machinery in this module rather than re-implementing it.
"""

from __future__ import annotations

import contextlib
import ipaddress
import itertools
import os
import selectors
import signal
import socket
import subprocess
import threading
import time
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, NoReturn, TypeVar, cast

import attrs
import psutil
import structlog

from airflow.sdk.api.datamodels._generated import TaskInstanceState
from airflow.sdk.configuration import conf
from airflow.sdk.execution_time.coordinator import BaseCoordinator, _warm_shutdown_signals
from airflow.sdk.execution_time.supervisor import (
    MIN_HEARTBEAT_INTERVAL,
    ActivitySubprocess,
    Heartbeater,
    NeverRaised,
    ProcessTracker,
)

if TYPE_CHECKING:
    import uuid
    from collections.abc import Generator, Sequence

    from structlog.typing import FilteringBoundLogger
    from typing_extensions import Self

    from airflow.sdk.api.client import Client
    from airflow.sdk.api.datamodels._generated import BundleInfo, TaskInstance, TIRunContext

    Tracked = TypeVar("Tracked", socket.socket, subprocess.Popen)

log: FilteringBoundLogger = structlog.get_logger(logger_name="coordinators.subprocess")


class SubprocessStartupError(RuntimeError):
    """
    The launched runtime never completed the startup handshake with the supervisor.

    :param exit_code: Exit code to report for the run — the runtime's own when it
        managed to exit before connecting, otherwise 1.
    """

    def __init__(self, reason: str, *, exit_code: int = 1) -> None:
        super().__init__(reason)
        self.exit_code = exit_code


@contextlib.contextmanager
def _heartbeat_until_monitored(
    client: Client, ti_id: uuid.UUID, pid: int, logger: FilteringBoundLogger
) -> Generator[None, None, None]:
    """
    Keep the run's heartbeat fresh while the worker is getting the runtime up.

    The task is RUNNING from before the runtime exists, but nothing heartbeats until
    :meth:`ActivitySubprocess.wait` starts monitoring, so a slow launch — a first-time
    Dag bundle clone, a generous ``task_startup_timeout`` — would look like a zombie to
    ``[scheduler] task_instance_heartbeat_timeout`` and be reaped mid-startup. The Python
    path gets this for free: its supervisor is already monitoring while the child does
    the same work.

    :meth:`ActivitySubprocess._monitor_subprocess` cannot cover this window — it services
    and reaps a child that does not exist yet — so a dedicated thread paces a
    :class:`~airflow.sdk.execution_time.supervisor.Heartbeater` instead, sharing the
    send and stop policy with the monitor loop's own heartbeats.

    Nothing here aborts the launch. A disowned run stops beating and is terminated by the
    monitor loop's own heartbeat moments later, which is the code that owns killing the
    runtime and recording SERVER_TERMINATED; with no process to kill,
    ``on_fatal_failures`` is left unset so a transient failure is simply retried.
    """
    stop = threading.Event()

    def _stop_beating(detail: Any) -> None:
        logger.error(
            "Server disowned this run while the runtime was starting; the monitor loop will terminate it",
            detail=detail,
        )
        stop.set()

    heartbeater = Heartbeater(client=client, ti_id=ti_id, pid=pid, on_server_terminated=_stop_beating)

    def _beat() -> None:
        # The blocking wait IS the pacing: exactly one attempt per interval, success or
        # failure, with the sleep doubling as the stop signal. The monitor loop's
        # if-needed gate and shrinking wait formula compensate for IO-driven select
        # wake-ups, which a dedicated timer thread doesn't have.
        while not stop.wait(MIN_HEARTBEAT_INTERVAL):
            heartbeater.send_heartbeat()

    thread = threading.Thread(target=_beat, name=f"startup-heartbeat-{ti_id}", daemon=True)
    thread.start()
    try:
        yield
    finally:
        stop.set()
        thread.join(MIN_HEARTBEAT_INTERVAL)


def _start_server() -> socket.socket:
    server = socket.socket()
    server.bind(("127.0.0.1", 0))
    server.setblocking(True)
    server.listen(1)  # Just need to listen to the child process.
    return server


def _socket_address(value: tuple | str) -> tuple[str, int] | None:
    if not isinstance(value, tuple) or len(value) < 2:
        return None
    host, port = value[:2]
    host = str(host)
    # Canonicalize an IPv4 address that a dual-stack client embeds in IPv6 so it matches
    # the AF_INET supervisor socket's plain-IPv4 address in the ownership check below. A
    # dual-stack JVM's loopback connection is rendered in two different forms depending on
    # the platform, and both must collapse to plain "127.0.0.1":
    #   * IPv4-mapped     "::ffff:127.0.0.1" -> "127.0.0.1"  (Linux, via /proc/net/tcp6)
    #   * IPv4-compatible "::127.0.0.1"      -> "127.0.0.1"  (macOS, via psutil)
    # Otherwise the JVM's connection fails the check and every Java task is rejected with
    # "process exited with 1 before connecting".
    try:
        parsed = ipaddress.ip_address(host)
    except ValueError:
        pass
    else:
        if isinstance(parsed, ipaddress.IPv6Address):
            if parsed.ipv4_mapped is not None:
                host = str(parsed.ipv4_mapped)
            elif 1 < int(parsed) <= 0xFFFFFFFF:
                # IPv4-compatible IPv6: ::/96 with the IPv4 in the low 32 bits. Exclude
                # "::" (unspecified) and "::1" (IPv6 loopback), which are not IPv4.
                host = str(ipaddress.IPv4Address(int(parsed)))
    return host, int(port)


def _connection_owned_by_process_tree(
    peer: tuple[str, int], local: tuple[str, int], proc: subprocess.Popen
) -> bool:
    """
    Return whether ``peer`` <-> ``local`` is an established connection in the child's process tree.

    The launched child may itself spawn the process that connects back to the
    supervisor — a JVM launcher, a shell wrapper, or any runtime that forks a
    worker — so the connecting peer can legitimately belong to a *descendant* of
    ``proc.pid`` rather than ``proc.pid`` itself. Every process in the subtree
    rooted at ``proc.pid`` is part of the task and is trusted; a process outside
    that subtree (e.g. an unrelated local process racing for the port) is not.
    """
    try:
        root = psutil.Process(proc.pid)
        processes = [root, *root.children(recursive=True)]
    except (psutil.AccessDenied, psutil.NoSuchProcess, psutil.ZombieProcess, OSError):
        return False
    for process in processes:
        try:
            connections = process.net_connections(kind="tcp")
        except (psutil.AccessDenied, psutil.NoSuchProcess, psutil.ZombieProcess, OSError):
            # A descendant may exit between enumeration and inspection — skip it
            # rather than failing verification for the whole tree.
            continue
        for connection in connections:
            if _socket_address(connection.laddr) == peer and _socket_address(connection.raddr) == local:
                return True
    return False


def _is_connection_from_process(
    conn: socket.socket,
    proc: subprocess.Popen,
    *,
    verify_timeout: float = 1.0,
    poll_interval: float = 0.05,
) -> bool:
    """
    Return whether the accepted TCP connection originates from the child process tree.

    The connection is trusted only if it belongs to ``proc.pid`` or one of its
    descendants. A freshly established connection is not always visible in
    ``/proc`` the instant it is accepted, so the lookup is retried for up to
    *verify_timeout* seconds before the connection is rejected.
    """
    peer = _socket_address(conn.getpeername())
    local = _socket_address(conn.getsockname())
    if peer is None or local is None:
        return False
    deadline = time.monotonic() + verify_timeout
    while True:
        if _connection_owned_by_process_tree(peer, local, proc):
            return True
        if time.monotonic() >= deadline:
            return False
        time.sleep(poll_interval)


def _accept_connections(
    servers: dict[str, socket.socket],
    drains: dict[str, socket.socket],
    proc: subprocess.Popen,
    *,
    max_wait: float = 10.0,
    drain_size: int = 4096,
    logger: FilteringBoundLogger | None = None,
) -> tuple[dict[socket.socket, socket.socket], dict[socket.socket, bytes]]:
    """Block until the subprocess connects to servers, draining stdout/stderr along the way."""
    task_log = logger or log
    accepted: dict[socket.socket, socket.socket] = {}
    drained: dict[socket.socket, bytes] = {s: b"" for s in drains.values()}

    def _give_up(reason: str, *, exit_code: int = 1) -> NoReturn:
        for s in accepted.values():
            s.close()
        # On the happy path these bytes reach the task log through
        # _register_pipe_readers. Emit them here too, or the runtime's own account of
        # why it never started dies with the drain buffers.
        for key, soc in drains.items():
            if output := drained[soc]:
                task_log.error(
                    "Runtime output before startup failure",
                    key=key,
                    output=output.decode(errors="replace"),
                )
        raise SubprocessStartupError(reason, exit_code=exit_code)

    with selectors.DefaultSelector() as sel:
        for key, soc in itertools.chain(servers.items(), drains.items()):
            sel.register(soc, selectors.EVENT_READ, data=key)
        deadline = time.monotonic() + max_wait
        while len(accepted) < len(servers):
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                _give_up("process did not connect within timeout")
            if proc.poll() is not None:
                _give_up(
                    f"process exited with {proc.returncode} before connecting",
                    exit_code=proc.returncode or 1,
                )
            for event, _ in sel.select(timeout=min(remaining, 1.0)):
                soc = cast("socket.socket", event.fileobj)
                if soc in drained:
                    if incoming := soc.recv(drain_size):
                        task_log.debug("Draining child process stream", key=event.data)
                        drained[soc] += incoming
                    else:
                        task_log.warning("Child stream closed before ready!", key=event.data)
                        sel.unregister(soc)
                else:
                    task_log.debug("Accepting child process connection", key=event.data)
                    conn, _ = soc.accept()
                    if not _is_connection_from_process(conn, proc):
                        task_log.warning(
                            "Rejected connection not owned by child process",
                            key=event.data,
                            pid=proc.pid,
                            peer=conn.getpeername(),
                        )
                        conn.close()
                        continue
                    sel.unregister(soc)
                    accepted[soc] = conn
    return accepted, drained


class PopenTracker(ProcessTracker):
    """
    Process tracker backed by :class:`subprocess.Popen`.

    :meta private:
    """

    ProcessNotFound = NeverRaised
    TimeoutExpired = subprocess.TimeoutExpired

    def __init__(self, impl: subprocess.Popen) -> None:
        self._impl = impl

    @property
    def pid(self) -> int:
        return self._impl.pid

    def send_signal(self, s: signal.Signals) -> None:
        self._impl.send_signal(s)

    def wait(self, timeout: float | None) -> int:
        return self._impl.wait(timeout)


@attrs.define(kw_only=True)
class _ResourceTracker:
    """
    Context manager that auto-closes tracked sockets and terminates tracked Popen objects.

    A subprocess startup is built up incrementally: bind sockets, spawn the
    child, accept its connections. If any step fails, the half-set-up state
    must be released. Calling :meth:`track` after each successful step records
    what to release; :meth:`untrack` removes ownership once another component
    (e.g. the activity subprocess instance) has taken over.
    """

    timeout: float
    tracked: dict[int, socket.socket | subprocess.Popen] = attrs.field(init=False, factory=dict)

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        for o in self.tracked.values():
            match o:
                case socket.socket():
                    o.close()
                case subprocess.Popen():
                    o.terminate()
                    try:
                        o.wait(self.timeout)
                    except subprocess.TimeoutExpired:
                        o.kill()

    def track(self, *objects: Tracked) -> tuple[Tracked, ...]:
        self.tracked.update((id(o), o) for o in objects)
        return objects

    def untrack(self, *objects: Tracked) -> tuple[Tracked, ...]:
        for o in objects:
            self.tracked.pop(id(o), None)
        return objects


@attrs.define(kw_only=True)
class _PopenActivitySubprocess(ActivitySubprocess):
    """
    Activity subprocess that talks to the parent over two TCP sockets.

    The subclass-supplied *command* is launched with ``--comm=<host:port>``
    and ``--logs=<host:port>`` appended; the subprocess MUST connect back to
    both ports before *startup_timeout* elapses. Anything the subprocess
    writes to stdout/stderr before connecting is drained and forwarded to
    :meth:`_register_pipe_readers` via the ``data=`` kwarg so log lines are
    not lost.
    """

    _comm_server: socket.socket
    _logs_server: socket.socket

    @classmethod
    def start(  # type: ignore[override]
        cls,
        *,
        what: TaskInstance,
        dag_rel_path: str | os.PathLike[str],
        bundle_info,
        ti_context: TIRunContext,
        running_since: datetime,
        logger: FilteringBoundLogger | None = None,
        sentry_integration: str = "",
        command: Sequence[str],
        subprocess_schema_version: str | None = None,
        startup_timeout: float = 10.0,
        **kwargs,
    ) -> Self:
        task_log = logger or structlog.get_logger(logger_name="task").bind()
        with _ResourceTracker(timeout=startup_timeout) as tracker:
            comm_server, logs_server = tracker.track(_start_server(), _start_server())
            stdout_r, stdout_w = tracker.track(*socket.socketpair())
            stderr_r, stderr_w = tracker.track(*socket.socketpair())

            # A language SDK runtime cannot read Airflow's config, so propagate the
            # resolved log levels via the environment at launch. StartupDetails
            # arrives too late, the logs might already be produced by then.
            env = {
                **os.environ,
                "AIRFLOW__LOGGING__LOGGING_LEVEL": conf.get("logging", "logging_level", fallback="INFO"),
                "AIRFLOW__LOGGING__NAMESPACE_LEVELS": conf.get("logging", "namespace_levels", fallback=""),
            }

            proc = subprocess.Popen(
                [
                    *command,
                    "--comm={0[0]}:{0[1]}".format(comm_server.getsockname()),
                    "--logs={0[0]}:{0[1]}".format(logs_server.getsockname()),
                ],
                stdout=stdout_w.fileno(),
                stderr=stderr_w.fileno(),
                env=env,
            )
            tracker.track(proc)
            for soc in tracker.untrack(stdout_w, stderr_w):
                soc.close()
            task_log.info("Starting subprocess", pid=proc.pid)

            socks, drained = _accept_connections(
                {"comm": comm_server, "logs": logs_server},
                {"stdout": stdout_r, "stderr": stderr_r},
                proc,
                max_wait=startup_timeout,
                logger=task_log,
            )
            tracker.track(*socks.values())

            self = cls(
                id=what.id,
                pid=proc.pid,
                process=PopenTracker(proc),
                process_log=task_log,
                start_time=time.monotonic(),
                stdin=socks[comm_server],
                subprocess_schema_version=subprocess_schema_version,
                comm_server=comm_server,
                logs_server=logs_server,
                **kwargs,
            )
            self._register_pipe_readers(
                *tracker.untrack(stdout_r, stderr_r, socks[comm_server], socks[logs_server]),
                data=drained,
            )
            self._on_child_started(
                ti=what,
                dag_rel_path=dag_rel_path,
                bundle_info=bundle_info,
                sentry_integration=sentry_integration,
                ti_context=ti_context,
                running_since=running_since,
            )

            # Untrack everything left. 'self' keeps track of these and closes
            # the servers when the subprocess exits in 'wait'.
            tracker.untrack(comm_server, logs_server, proc)

        return self

    def wait(self) -> int:
        code = super().wait()
        self._close_unused_sockets(self._comm_server, self._logs_server)
        return code


@attrs.define(kw_only=True)
class SubprocessCoordinator(BaseCoordinator):
    """
    Abstract base for coordinators that launch a subprocess and IPC over TCP sockets.

    Subclasses provide the per-task subprocess command and the supervisor
    wire-schema version via :meth:`_build_execute_task_command`. The rest of
    the socket lifecycle — listening, spawning the child, accepting
    connections, draining startup output, and tearing everything down on
    failure — is handled here.

    The task is reported RUNNING before :meth:`_build_execute_task_command` runs,
    so everything a subclass does to locate its artifacts — walking a directory,
    materializing a Dag bundle — is charged to the task's runtime rather than to
    its queued time, and failing it fails the task instead of leaving the run
    QUEUED for ``[scheduler] task_queued_timeout`` to pick up.

    :param task_startup_timeout: Maximum time the coordinator waits for the
        subprocess to connect to both servers, in seconds. The default is 10
        seconds. The wait happens with the task already RUNNING.
    """

    task_startup_timeout: float = 10.0

    def _build_execute_task_command(self, *, what: TaskInstance) -> tuple[list[str], str | None]:
        """
        Build the subprocess command and resolve its supervisor wire-schema version for *what*.

        Returns a ``(command, subprocess_schema_version)`` pair. *command*
        MUST NOT include the ``--comm`` / ``--logs`` flags — those are
        appended by :class:`_PopenActivitySubprocess` once the listening
        sockets have been bound. A ``None`` schema version disables schema
        migration; messages are then exchanged at the runtime's native wire
        format.
        """
        raise NotImplementedError

    def execute_task(
        self,
        *,
        what: TaskInstance,
        dag_rel_path: str | os.PathLike[str],
        bundle_info: BundleInfo,
        client: Client,
        logger: FilteringBoundLogger | None = None,
        sentry_integration: str = "",
        subprocess_logs_to_stdout: bool,
        **kwargs,
    ) -> BaseCoordinator.ExecutionResult:
        task_log = logger or structlog.get_logger(logger_name="task").bind()
        # Hold the warm-shutdown handlers across the RUNNING window, as the Python
        # coordinator does, so a SIGTERM here cannot orphan a task the server has
        # been told is running.
        with _warm_shutdown_signals():
            # Report RUNNING before any preparation work (see the class docstring), so
            # `_build_execute_task_command` runs on the task's clock rather than the
            # queue's. A failure here is not ours to report: there is no run in RUNNING
            # yet, and the redelivery it usually means (TaskAlreadyRunningError) is for
            # the executor to swallow.
            # The runtime does not exist yet, so this supervisor's own pid becomes the
            # run's ownership token; the runtime's pid goes to the task log instead
            # ("Starting subprocess"). Every later heartbeat must present the same
            # value, so it is threaded into the supervised process too (see
            # ActivitySubprocess.reported_pid).
            reported_pid = os.getpid()
            running_since = datetime.now(tz=timezone.utc)
            ti_context = client.task_instances.start(what.id, reported_pid, running_since)
            try:
                with _heartbeat_until_monitored(client, what.id, reported_pid, task_log):
                    command, subprocess_schema_version = self._build_execute_task_command(what=what)
                    process = _PopenActivitySubprocess.start(
                        what=what,
                        dag_rel_path=dag_rel_path,
                        bundle_info=bundle_info,
                        client=client,
                        ti_context=ti_context,
                        running_since=running_since,
                        reported_pid=reported_pid,
                        logger=task_log,
                        subprocess_logs_to_stdout=subprocess_logs_to_stdout,
                        sentry_integration=sentry_integration,
                        command=command,
                        subprocess_schema_version=subprocess_schema_version,
                        startup_timeout=self.task_startup_timeout,
                    )
            except SubprocessStartupError as error:
                task_log.error("Task runtime failed to start", reason=str(error))
                return self._finish_failed_startup(
                    client=client,
                    what=what,
                    ti_context=ti_context,
                    logger=task_log,
                    exit_code=error.exit_code,
                )
            except Exception:
                task_log.exception("Failed to launch the task runtime")
                return self._finish_failed_startup(
                    client=client, what=what, ti_context=ti_context, logger=task_log
                )
            exit_code = process.wait()
            return self.ExecutionResult(exit_code, process.final_state)

    def _finish_failed_startup(
        self,
        *,
        client: Client,
        what: TaskInstance,
        ti_context: TIRunContext,
        logger: FilteringBoundLogger,
        exit_code: int = 1,
    ) -> BaseCoordinator.ExecutionResult:
        """
        Report the terminal state for a run that never got a runtime to monitor.

        :class:`ActivitySubprocess` only reports a terminal state out of ``wait()``,
        which needs the very process this failure prevented, so without this the run
        would sit RUNNING until zombie detection reaped it.

        :raises Exception: whatever the terminal-state call raised. The run is RUNNING
            on the server and this is the only thing that was going to move it, so the
            failure has to escape and let the executor report it for the scheduler to
            reconcile — returning a state we did not manage to record would put the run
            right back in the RUNNING-until-reaped hole this method exists to close.
        """
        when = datetime.now(tz=timezone.utc)
        state = TaskInstanceState.UP_FOR_RETRY if ti_context.should_retry else TaskInstanceState.FAILED
        try:
            if state is TaskInstanceState.UP_FOR_RETRY:
                # UP_FOR_RETRY is not a TerminalStateNonSuccess, so it has its own endpoint.
                client.task_instances.retry(
                    id=what.id,
                    end_date=when,
                    rendered_map_index=None,
                    retry_reason="Task runtime failed to start",
                )
            else:
                client.task_instances.finish(
                    id=what.id,
                    state=state,
                    when=when,
                    rendered_map_index=None,
                )
        except Exception:
            logger.exception("Failed to report the task runtime startup failure", state=state)
            raise
        return self.ExecutionResult(exit_code, state)
