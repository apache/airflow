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

import ipaddress
import itertools
import os
import selectors
import signal
import socket
import subprocess
import time
from typing import TYPE_CHECKING, TypeVar, cast

import attrs
import psutil
import structlog

from airflow.sdk.execution_time.coordinator import BaseCoordinator
from airflow.sdk.execution_time.supervisor import ActivitySubprocess, NeverRaised, ProcessTracker

if TYPE_CHECKING:
    from collections.abc import Sequence

    from structlog.typing import FilteringBoundLogger
    from typing_extensions import Self

    from airflow.sdk.api.client import Client
    from airflow.sdk.api.datamodels._generated import BundleInfo, TaskInstance

    Tracked = TypeVar("Tracked", socket.socket, subprocess.Popen)

log: FilteringBoundLogger = structlog.get_logger(logger_name="coordinators.subprocess")


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
    # Canonicalize IPv4-mapped IPv6 ("::ffff:127.0.0.1" -> "127.0.0.1") so a dual-stack
    # client (e.g. the JVM, shown v4-mapped in /proc/net/tcp6) matches the AF_INET
    # supervisor socket's plain-IPv4 address in the ownership check below.
    try:
        parsed = ipaddress.ip_address(host)
    except ValueError:
        pass
    else:
        if isinstance(parsed, ipaddress.IPv6Address) and parsed.ipv4_mapped is not None:
            host = str(parsed.ipv4_mapped)
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
) -> tuple[dict[socket.socket, socket.socket], dict[socket.socket, bytes]]:
    """Block until the subprocess connects to servers, draining stdout/stderr along the way."""
    accepted: dict[socket.socket, socket.socket] = {}
    drained: dict[socket.socket, bytes] = {s: b"" for s in drains.values()}
    with selectors.DefaultSelector() as sel:
        for key, soc in itertools.chain(servers.items(), drains.items()):
            sel.register(soc, selectors.EVENT_READ, data=key)
        deadline = time.monotonic() + max_wait
        while len(accepted) < len(servers):
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                for s in accepted.values():
                    s.close()
                raise TimeoutError("process did not connect within timeout")
            if proc.poll() is not None:
                for s in accepted.values():
                    s.close()
                raise RuntimeError(f"process exited with {proc.returncode} before connecting")
            for event, _ in sel.select(timeout=min(remaining, 1.0)):
                soc = cast("socket.socket", event.fileobj)
                if soc in drained:
                    if incoming := soc.recv(drain_size):
                        log.debug("Draining child process stream", key=event.data)
                        drained[soc] += incoming
                    else:
                        log.warning("Child stream closed before ready!", key=event.data)
                        sel.unregister(soc)
                else:
                    log.debug("Accepting child process connection", key=event.data)
                    conn, _ = soc.accept()
                    if not _is_connection_from_process(conn, proc):
                        log.warning(
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
        logger: FilteringBoundLogger | None = None,
        sentry_integration: str = "",
        command: Sequence[str],
        subprocess_schema_version: str | None = None,
        startup_timeout: float = 10.0,
        **kwargs,
    ) -> Self:
        with _ResourceTracker(timeout=startup_timeout) as tracker:
            comm_server, logs_server = tracker.track(_start_server(), _start_server())
            stdout_r, stdout_w = tracker.track(*socket.socketpair())
            stderr_r, stderr_w = tracker.track(*socket.socketpair())

            proc = subprocess.Popen(
                [
                    *command,
                    "--comm={0[0]}:{0[1]}".format(comm_server.getsockname()),
                    "--logs={0[0]}:{0[1]}".format(logs_server.getsockname()),
                ],
                stdout=stdout_w.fileno(),
                stderr=stderr_w.fileno(),
            )
            tracker.track(proc)
            for soc in tracker.untrack(stdout_w, stderr_w):
                soc.close()
            log.info("Starting subprocess", pid=proc.pid)

            socks, drained = _accept_connections(
                {"comm": comm_server, "logs": logs_server},
                {"stdout": stdout_r, "stderr": stderr_r},
                proc,
                max_wait=startup_timeout,
            )
            tracker.track(*socks.values())

            self = cls(
                id=what.id,
                pid=proc.pid,
                process=PopenTracker(proc),
                process_log=logger or structlog.get_logger(logger_name="task").bind(),
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

    :param task_startup_timeout: Maximum time the coordinator waits for the
        subprocess to connect to both servers, in seconds. The default is 10
        seconds.
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
        command, subprocess_schema_version = self._build_execute_task_command(what=what)
        process = _PopenActivitySubprocess.start(
            what=what,
            dag_rel_path=dag_rel_path,
            bundle_info=bundle_info,
            client=client,
            logger=logger,
            subprocess_logs_to_stdout=subprocess_logs_to_stdout,
            sentry_integration=sentry_integration,
            command=command,
            subprocess_schema_version=subprocess_schema_version,
            startup_timeout=self.task_startup_timeout,
        )
        exit_code = process.wait()
        return self.ExecutionResult(exit_code, process.final_state)
