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
import enum
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

from airflow.sdk.configuration import conf
from airflow.sdk.execution_time.coordinator import BaseCoordinator
from airflow.sdk.execution_time.supervisor import ActivitySubprocess, NeverRaised, ProcessTracker

if TYPE_CHECKING:
    import pathlib
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


class _ArtifactSource(enum.Enum):
    """How a subprocess coordinator locates the compiled task artifacts."""

    EXPLICIT_ROOT = enum.auto()
    """An explicit filesystem root (``jars_root`` / ``executables_root`` / ``bundles_root``)."""
    NAMED_BUNDLE = enum.auto()
    """``dag_bundle_name`` names a configured Dag bundle; its latest version is used."""
    TASK_BUNDLE = enum.auto()
    """Neither is set: the task's own (co-located) bundle, pinned to the run's version."""


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
    :param dag_bundle_name: Locate artifacts through a configured Dag bundle rather
        than an explicit root. Mutually exclusive with the subclass's explicit root;
        if neither is set, the task's own bundle is used. A named bundle resolves to
        its latest version; the task's own bundle is pinned to the run's version.
    """

    task_startup_timeout: float = 10.0
    dag_bundle_name: str | None = None

    # Classified once at construction by :meth:`_classify_artifact_source` and
    # dispatched on by :meth:`_init_root_source` at execute time.
    _artifact_source: _ArtifactSource | None = attrs.field(init=False, default=None)
    # The subclass's explicit root, recorded at construction so the base can
    # resolve roots without knowing the subclass field name.
    _configured_roots: list[pathlib.Path] = attrs.field(init=False, factory=list)
    # The task's own bundle, bound for the duration of a single :meth:`execute_task`
    # call by :meth:`_set_current_bundle` so :meth:`_init_root_source` can resolve
    # co-located artifacts.
    _active_bundle_info: BundleInfo | None = attrs.field(init=False, default=None)

    def _classify_artifact_source(self, configured: Sequence[pathlib.Path], *, root_kwarg: str) -> None:
        """
        Classify and validate how this coordinator locates artifacts (construction time).

        Subclasses call this from ``__attrs_post_init__`` with their own root
        field. It rejects setting both an explicit root and ``dag_bundle_name``,
        fails fast when ``dag_bundle_name`` names a bundle that is not configured,
        and records the resulting :class:`_ArtifactSource` and explicit root.
        """
        if configured and self.dag_bundle_name is not None:
            raise ValueError(
                f"Set at most one of {root_kwarg!r} or 'dag_bundle_name': {root_kwarg!r} for an "
                f"explicit path, 'dag_bundle_name' for a configured Dag bundle, or leave both "
                f"unset to scan the task's own bundle."
            )
        if configured:
            source = _ArtifactSource.EXPLICIT_ROOT
            self._configured_roots = list(configured)
        elif self.dag_bundle_name is not None:
            source = _ArtifactSource.NAMED_BUNDLE
            from airflow.dag_processing.bundles.manager import DagBundlesManager  # noqa: SDK002

            if not DagBundlesManager.is_bundle_configured(self.dag_bundle_name):
                raise ValueError(
                    f"Coordinator 'dag_bundle_name' references unconfigured Dag bundle "
                    f"{self.dag_bundle_name!r}."
                )
        else:
            source = _ArtifactSource.TASK_BUNDLE

        self._artifact_source = source
        details: dict[str, str | list[str]] = {"mode": source.name}
        if self.dag_bundle_name is not None:
            details["dag_bundle_name"] = self.dag_bundle_name
        if self._configured_roots:
            details["configured_roots"] = [str(root) for root in self._configured_roots]
        log.debug("Coordinator artifact source selected", **details)

    def _init_root_source(self) -> list[pathlib.Path]:
        """
        Resolve the directories to scan for artifacts for the current task.

        Dispatches on the :class:`_ArtifactSource` classified at construction.
        An explicit root is returned as-is (no Dag bundle is resolved); otherwise
        the root is a Dag bundle's materialized path — the named
        ``dag_bundle_name`` bundle at its latest version, or the task's own
        bundle pinned to the run's version. Called by :meth:`execute_task`, which
        forwards the result to :meth:`_build_execute_task_command`.
        """
        if self._artifact_source is _ArtifactSource.EXPLICIT_ROOT:
            return self._configured_roots

        if self._artifact_source is _ArtifactSource.NAMED_BUNDLE:
            from airflow.sdk.api.datamodels._generated import BundleInfo

            # NAMED_BUNDLE implies dag_bundle_name is set.
            target = BundleInfo(name=cast("str", self.dag_bundle_name))
        elif self._artifact_source is _ArtifactSource.TASK_BUNDLE:
            if self._active_bundle_info is None:
                raise RuntimeError("_init_root_source requires an active task; call it during execute_task.")
            target = self._active_bundle_info
        else:
            raise RuntimeError(
                "Coordinator artifact source was not classified; call _classify_artifact_source first."
            )

        # Lazy import: task_runner is a heavy module and importing it at module
        # load would risk an import cycle through the supervisor.
        from airflow.sdk.execution_time.task_runner import initialize_ti_bundle

        bundle = initialize_ti_bundle(target, log)
        path = bundle.path
        if not path.exists():
            raise FileNotFoundError(f"Dag bundle {target.name!r} resolved to {path}, which does not exist.")
        return [path]

    def _build_execute_task_command(
        self, *, what: TaskInstance, roots: list[pathlib.Path]
    ) -> tuple[list[str], str | None]:
        """
        Build the subprocess command and resolve its supervisor wire-schema version for *what*.

        *roots* are the directories to scan for artifacts, already resolved by
        :meth:`_init_root_source` from the coordinator's configured source.
        Returns a ``(command, subprocess_schema_version)`` pair. *command* MUST
        NOT include the ``--comm`` / ``--logs`` flags — those are appended by
        :class:`_PopenActivitySubprocess` once the listening sockets have been
        bound. A ``None`` schema version disables schema migration; messages are
        then exchanged at the runtime's native wire format.
        """
        raise NotImplementedError

    @contextlib.contextmanager
    def _set_current_bundle(self, bundle_info: BundleInfo):
        """
        Bind *bundle_info* as the active task for the duration of the block, clearing it on exit.

        Rejects a second concurrent bind: this coordinator runs one blocking task
        per process and is not re-entrant.
        """
        if self._active_bundle_info is not None:
            raise RuntimeError("SubprocessCoordinator.execute_task is not re-entrant.")
        self._active_bundle_info = bundle_info
        try:
            yield
        finally:
            self._active_bundle_info = None

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
        with self._set_current_bundle(bundle_info):
            roots = self._init_root_source()
            command, subprocess_schema_version = self._build_execute_task_command(what=what, roots=roots)
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
