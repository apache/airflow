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
import ctypes
import ipaddress
import itertools
import os
import selectors
import signal
import socket
import subprocess
import sys
import time
from typing import TYPE_CHECKING, NoReturn, TypeVar, cast

import attrs
import psutil
import structlog

from airflow.dag_processing.bundles.base import BundleVersionLock, unpack_bundle_version  # noqa: SDK002
from airflow.sdk.api.datamodels._generated import BundleInfo
from airflow.sdk.configuration import conf
from airflow.sdk.coordinators._bundle_metadata import walk_files
from airflow.sdk.execution_time.bundles import initialize_ti_bundle
from airflow.sdk.execution_time.coordinator import BaseCoordinator
from airflow.sdk.execution_time.schema import get_schema_version_migrator
from airflow.sdk.execution_time.supervisor import ActivitySubprocess, NeverRaised, ProcessTracker

if TYPE_CHECKING:
    import pathlib
    from collections.abc import Callable, Sequence

    from structlog.typing import FilteringBoundLogger
    from typing_extensions import Self

    from airflow.dag_processing.bundles.base import BaseDagBundle  # noqa: SDK002
    from airflow.sdk.api.client import Client
    from airflow.sdk.api.datamodels._generated import TaskInstance
    from airflow.sdk.execution_time.coordinator import TaskHandlerCandidate

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


def _connection_owned_by_process_tree(peer: tuple[str, int], local: tuple[str, int], pid: int) -> bool:
    """
    Return whether ``peer`` <-> ``local`` is an established connection in the child's process tree.

    The launched child may itself spawn the process that connects back to the
    supervisor — a JVM launcher, a shell wrapper, or any runtime that forks a
    worker — so the connecting peer can legitimately belong to a *descendant* of
    ``pid`` rather than ``pid`` itself. Every process in the subtree rooted at
    ``pid`` is part of the task and is trusted; a process outside that subtree
    (e.g. an unrelated local process racing for the port) is not.
    """
    try:
        root = psutil.Process(pid)
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
        if _connection_owned_by_process_tree(peer, local, proc.pid):
            return True
        if time.monotonic() >= deadline:
            return False
        time.sleep(poll_interval)


def _is_connection_from_pid(conn: socket.socket, pid: int) -> bool:
    """
    Return whether the accepted TCP connection comes from ``pid``'s process tree, checking once.

    A connection that is not visible yet is reported as not owned, so the caller retries later
    instead of waiting here.
    """
    peer = _socket_address(conn.getpeername())
    local = _socket_address(conn.getsockname())
    return peer is not None and local is not None and _connection_owned_by_process_tree(peer, local, pid)


def _set_close_on_exec_above_stderr() -> None:
    """Mark every file descriptor above 2 close-on-exec, as ``subprocess.Popen(close_fds=True)`` does."""
    fd_dir = "/proc/self/fd" if os.path.isdir("/proc/self/fd") else "/dev/fd"
    for name in os.listdir(fd_dir):
        if (fd := int(name)) > 2:
            # The descriptor listing the directory is already closed.
            with contextlib.suppress(OSError):
                os.set_inheritable(fd, False)


def _build_runtime_env() -> dict[str, str]:
    """Return the environment for a language SDK runtime, with the Airflow config it reads resolved."""
    # A language SDK runtime cannot read Airflow's config, so the options it needs are resolved here.
    # An option without a config default gets the fallback Python uses. StartupDetails arrives too
    # late: logs may already be produced.
    return {
        **os.environ,
        "AIRFLOW__LOGGING__LOGGING_LEVEL": conf.get("logging", "logging_level", fallback="INFO"),
        "AIRFLOW__LOGGING__NAMESPACE_LEVELS": conf.get("logging", "namespace_levels", fallback=""),
    }


# From <linux/prctl.h>
_PR_SET_PDEATHSIG = 1


def _set_parent_death_signal() -> None:
    """
    On Linux, have the kernel SIGKILL this process when the thread that forked it exits.

    The setting survives the exec, so the runtime dies with the process that reads its sockets,
    even when that process is killed without the chance to kill the runtime first.
    """
    if sys.platform != "linux":
        return
    try:
        libc = ctypes.CDLL(None, use_errno=True)
        if libc.prctl(_PR_SET_PDEATHSIG, signal.SIGKILL, 0, 0, 0) != 0:
            log.warning("Failed to set PR_SET_PDEATHSIG", errno=ctypes.get_errno())
    except Exception:
        log.warning("Unable to set PR_SET_PDEATHSIG", exc_info=True)


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
                env=_build_runtime_env(),
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


def _initialize_pinned_bundle(target: BundleInfo, logger: FilteringBoundLogger) -> BaseDagBundle:
    """
    Materialize *target* at a concrete version, so the tree handed to the subprocess is lockable.

    A bundle resolved without a version points at the bundle's shared, mutable
    checkout: another task refreshing the same bundle resets it underneath a running
    subprocess, and ``BundleVersionLock`` cannot protect it because a version-less
    lock is a no-op. Re-resolving at the version current now yields a private
    ``versions/<version>`` tree that the lock does cover.

    Bundles that do not track versions have nothing to pin and keep their single path.
    """
    bundle = initialize_ti_bundle(target)
    if bundle.version is not None:
        return bundle

    version, version_data = unpack_bundle_version(bundle.get_current_version(), bundle)
    if version is None:
        return bundle
    logger.debug("Pinning Dag bundle to its current version", bundle=target.name, version=version)
    return initialize_ti_bundle(BundleInfo(name=target.name, version=version, version_data=version_data))


@attrs.define(kw_only=True)
class SubprocessCoordinator(BaseCoordinator):
    """
    Abstract base for coordinators that launch a subprocess and IPC over TCP sockets.

    Subclasses provide the per-task subprocess command and the supervisor
    wire-schema version via :meth:`_build_execute_task_command`. The rest of
    the socket lifecycle — listening, spawning the child, accepting
    connections, draining startup output, and tearing everything down on
    failure — is handled here.

    A subclass that also reports its artifacts' task handlers implements
    :meth:`_build_parse_task_handler_command`, and :meth:`_read_task_handler_candidate`
    to tell its artifacts apart from the other files of a Dag bundle.

    :param task_startup_timeout: Maximum time the coordinator waits for the
        subprocess to connect to both servers, in seconds. The default is 10
        seconds.
    :param task_handler_bundle_name: Name of the Dag bundle that holds the compiled
        task handlers. It must be registered in ``[dag_processor] dag_bundle_config_list``.
        If unset, the task's own Dag bundle is used. A named bundle resolves to the
        version current when the task starts; the task's own bundle uses the run's
        version. Either way the resolved version is pinned for the whole task.
    """

    task_startup_timeout: float = 10.0
    task_handler_bundle_name: str | None = None

    _active_scan_roots: tuple[pathlib.Path, ...] | None = attrs.field(init=False, default=None)

    def _resolve_artifact_bundle(
        self, bundle_info: BundleInfo, logger: FilteringBoundLogger
    ) -> BaseDagBundle:
        """
        Materialize the Dag bundle holding the artifacts for a task of *bundle_info*.

        That is the bundle named by ``task_handler_bundle_name``, or the task's own
        bundle when it is unset. *logger* is the task logger, so materialization
        failures surface in the task log.
        """
        if self.task_handler_bundle_name is None:
            target = bundle_info
        else:
            target = BundleInfo(name=self.task_handler_bundle_name)

        bundle = _initialize_pinned_bundle(target, logger)
        path = bundle.path
        if not path.exists():
            raise FileNotFoundError(f"Dag bundle {target.name!r} resolved to {path}, which does not exist.")
        return bundle

    def _get_scan_roots(self) -> tuple[pathlib.Path, ...]:
        """Return the artifact roots resolved for the active task or parse."""
        if self._active_scan_roots is None:
            raise RuntimeError(
                "_get_scan_roots requires an active task or parse; "
                "call it during execute_task or parse_task_handler."
            )
        return self._active_scan_roots

    def _build_execute_task_command(self, *, what: TaskInstance) -> tuple[list[str], str | None]:
        """
        Build the subprocess command and resolve its supervisor wire-schema version for *what*.

        Subclasses can retrieve the directories to scan for artifacts with
        :meth:`_get_scan_roots`.
        Returns a ``(command, subprocess_schema_version)`` pair. *command* MUST
        NOT include the ``--comm`` / ``--logs`` flags — those are appended by
        :class:`_PopenActivitySubprocess` once the listening sockets have been
        bound. A ``None`` schema version disables schema migration; messages are
        then exchanged at the runtime's native wire format.
        """
        raise NotImplementedError

    def _build_parse_task_handler_command(self, *, path: pathlib.Path) -> tuple[list[str], str | None]:
        """
        Build the command that reports the task handlers of the artifact at *path*, with its schema version.

        Subclasses can retrieve the directories to scan for artifacts with
        :meth:`_get_scan_roots`; for a parse they are the root of the Dag bundle holding
        *path*. The contract is that of :meth:`_build_execute_task_command`: *command*
        MUST NOT include the ``--comm`` / ``--logs`` flags.
        """
        raise NotImplementedError(f"{type(self).__name__} does not parse task handlers")

    def parse_task_handler(
        self,
        *,
        path: pathlib.Path,
        bundle_path: pathlib.Path,
        comm_address: tuple[str, int],
        logs_address: tuple[str, int],
        report_schema_version: Callable[[str | None], None],
    ) -> NoReturn:
        """
        Replace the current process with the runtime that reports the task handlers of the artifact at *path*.

        Call this in a child process whose standard streams are already set up; the runtime
        inherits them and no other file descriptor. The command and its supervisor wire-schema
        version are resolved against *bundle_path*, and the version is passed to
        *report_schema_version* just before the exec. The runtime connects back to
        *comm_address* and *logs_address*. On Linux the runtime dies with the thread that forked
        the calling process, and it is not started once that parent has exited.

        :raises Exception: when the command cannot be resolved or started, or the parent has
            exited; the process is then unchanged.
        """
        with self._set_scan_roots([bundle_path]):
            command, schema_version = self._build_parse_task_handler_command(path=path)
        if schema_version is not None:
            get_schema_version_migrator().resolve_version(schema_version)
        argv = [
            *command,
            f"--comm={comm_address[0]}:{comm_address[1]}",
            f"--logs={logs_address[0]}:{logs_address[1]}",
        ]
        # Taken before the report, which fails if the parent has already exited. A different parent once the
        # death signal is set means the parent exited in between, and the kernel does not send it for that.
        parent_pid = os.getppid()
        report_schema_version(schema_version)
        # Python ignores these at startup and exec keeps ignored signals; subprocess.Popen resets
        # them the same way for the task runtime.
        for name in ("SIGPIPE", "SIGXFZ", "SIGXFSZ"):
            if (sig := getattr(signal, name, None)) is not None:
                signal.signal(sig, signal.SIG_DFL)
        _set_parent_death_signal()
        if os.getppid() != parent_pid:
            raise RuntimeError("The process that started the runtime has exited")
        _set_close_on_exec_above_stderr()
        os.execvpe(argv[0], argv, _build_runtime_env())

    @contextlib.contextmanager
    def _set_scan_roots(self, roots: Sequence[pathlib.Path]):
        """Expose *roots* to the command builder for the duration of the task or parse."""
        if self._active_scan_roots is not None:
            raise RuntimeError(
                "SubprocessCoordinator.execute_task and parse_task_handler are not re-entrant."
            )
        self._active_scan_roots = tuple(roots)
        try:
            yield
        finally:
            self._active_scan_roots = None

    def list_task_handler_candidates(self, bundle_path: pathlib.Path) -> list[TaskHandlerCandidate]:
        if type(self)._read_task_handler_candidate is SubprocessCoordinator._read_task_handler_candidate:
            raise NotImplementedError(f"{type(self).__name__} does not list task handler artifacts")
        candidates = []
        listed: set[tuple[int, int]] = set()
        for path in walk_files([bundle_path], match=lambda _: True):
            try:
                file_info = path.stat()
            except OSError:
                continue
            # A file reached through symlinks is listed once, under the first path walked.
            if (identity := (file_info.st_dev, file_info.st_ino)) in listed:
                continue
            rel_path = path.relative_to(bundle_path).as_posix()
            if (candidate := self._read_task_handler_candidate(path, rel_path=rel_path)) is not None:
                listed.add(identity)
                candidates.append(candidate)
        return candidates

    def _read_task_handler_candidate(
        self, path: pathlib.Path, *, rel_path: str
    ) -> TaskHandlerCandidate | None:
        """
        Return *path* as a candidate if it is one of this coordinator's artifacts, else ``None``.

        :meth:`list_task_handler_candidates` calls this once per regular file in the Dag bundle, in walk
        order, with *rel_path* the file's POSIX path in the bundle, and skips a file already listed under
        another path. Read only what identifies the artifact and its stored cache digest, never hash it,
        and take the size from the file that was read.
        """
        raise NotImplementedError(f"{type(self).__name__} does not list task handler artifacts")

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
        bundle = self._resolve_artifact_bundle(bundle_info, logger or log)
        # Hold the version lock across start()/wait() so bundle cleanup cannot
        # rmtree a version this task is still reading from, mirroring
        # task_runner.main() for the Python task path.
        with (
            BundleVersionLock(bundle_name=bundle.name, bundle_version=bundle.version),
            self._set_scan_roots([bundle.path]),
        ):
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
