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
import json
import logging
import os
import selectors
import signal
import socket
import stat
import subprocess
import time
from datetime import UTC, datetime
from pathlib import PurePosixPath
from typing import TYPE_CHECKING, NoReturn, TypeVar, cast

import attrs
import psutil
import structlog

from airflow.dag_processing.bundles.base import BundleVersionLock, unpack_bundle_version  # noqa: SDK002
from airflow.sdk._shared.module_loading import import_string
from airflow.sdk.api.datamodels._generated import BundleInfo, TaskInstanceState
from airflow.sdk.configuration import conf
from airflow.sdk.coordinators._dag_importer import find_claiming_importer
from airflow.sdk.execution_time.bundles import initialize_ti_bundle
from airflow.sdk.execution_time.comms import RetryTask, TaskState
from airflow.sdk.execution_time.coordinator import BaseCoordinator, TaskLaunchError
from airflow.sdk.execution_time.schema import get_schema_version_migrator
from airflow.sdk.execution_time.supervisor import (
    ActivitySubprocess,
    NeverRaised,
    ProcessTracker,
    forward_to_log,
    signal_process_tree,
)

if TYPE_CHECKING:
    import pathlib
    from collections.abc import Callable, Generator, Sequence
    from typing import Self

    from structlog.typing import FilteringBoundLogger

    from airflow.dag_processing.bundles.base import BaseDagBundle  # noqa: SDK002
    from airflow.sdk.api.client import Client
    from airflow.sdk.api.datamodels._generated import TaskInstance, TIRunContext
    from airflow.sdk.execution_time.comms import SetTaskStateStore
    from airflow.sdk.execution_time.supervisor import RequestResult

    Tracked = TypeVar("Tracked", socket.socket, subprocess.Popen)

log: FilteringBoundLogger = structlog.get_logger(logger_name="coordinators.subprocess")


# The tail of each stream is kept: a runtime that fails to start reports why at the end.
_STARTUP_OUTPUT_LIMIT = 16384

# A runtime that keeps printing while it fails to connect is read no further than this, so it cannot
# hold up the failure.
_STARTUP_DRAIN_BUDGET = 1024 * 1024

_STARTUP_OUTPUT_TRUNCATED = f"[earlier output dropped, last {_STARTUP_OUTPUT_LIMIT} bytes]".encode()


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
    # "The runtime exited with 1 before connecting".
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
        "AIRFLOW__API__BASE_URL": conf.get("api", "base_url", fallback="/"),
        "AIRFLOW__OPERATORS__DEFAULT_DEFERRABLE": str(conf.getboolean("operators", "default_deferrable")),
        "AIRFLOW__TRIGGERER__QUEUES_ENABLED": str(conf.getboolean("triggerer", "queues_enabled")),
        "AIRFLOW__STATE_STORE__DEFAULT_RETENTION_DAYS": conf.get("state_store", "default_retention_days"),
    }


def _kill_leftover_group(pid: int) -> None:
    """
    Kill what an exited runtime left in its process group, which ``getpgid`` can no longer resolve.

    A group outlives its leader, so it is reached by the leader's pid. Not when another process has
    reused that pid, since the group could then be that process's.
    """
    if psutil.pid_exists(pid):
        return
    with contextlib.suppress(ProcessLookupError, PermissionError):
        os.killpg(pid, signal.SIGKILL)


def _read_available(soc: socket.socket, drain_size: int) -> bytes:
    """Return the data *soc* holds now, without waiting for more from a process that may still run."""
    soc.setblocking(False)
    chunks = []
    budget = _STARTUP_DRAIN_BUDGET
    with contextlib.suppress(BlockingIOError, OSError):
        while budget > 0 and (chunk := soc.recv(min(drain_size, budget))):
            budget -= len(chunk)
            chunks.append(chunk)
    return b"".join(chunks)


def _report_startup_output(
    target_loggers: tuple[FilteringBoundLogger, ...],
    drains: dict[str, socket.socket],
    drained: dict[socket.socket, bytes],
    drain_size: int,
) -> None:
    """
    Write what a runtime printed before it failed to connect to the task log, one event per line.

    The output goes through the forwarder a connected runtime's output goes through, rather than
    into the error message, so every line keeps its own level and timestamp. The task instance is
    not started yet when this runs. If the supervisor then finds the task was cleared while it was
    queued, it stops without reporting a state, and these lines stay in this try's local log file
    without being uploaded. They never reach another try: the log file is per try.
    """
    for key, soc in drains.items():
        output = drained[soc] + _read_available(soc, drain_size)
        level = logging.ERROR if key == "stderr" else logging.INFO
        forward = forward_to_log(target_loggers, f"task.{key}", level)
        next(forward)
        if len(output) > _STARTUP_OUTPUT_LIMIT:
            output = output[-_STARTUP_OUTPUT_LIMIT:]
            forward.send(_STARTUP_OUTPUT_TRUNCATED)
        for line in output.splitlines(keepends=True):
            forward.send(line)


@contextlib.contextmanager
def _raise_as_launch_error(prefix: str) -> Generator[None, None, None]:
    """
    Raise a failure in the block as a TaskLaunchError, so the supervisor fails the task with its reason.

    Use it only before the task instance is started: the supervisor starts the task instance itself
    when it fails one with a TaskLaunchError. A TaskLaunchError raised in the block is kept as it is.
    An OSError or ValueError, such as a missing runtime or artifact, or an artifact that cannot run,
    is retryable. Any other failure, such as a coordinator bug, is not: retrying cannot fix it.
    """
    try:
        yield
    except TaskLaunchError:
        raise
    except (OSError, ValueError) as e:
        raise TaskLaunchError(f"{prefix}: {e}") from e
    except Exception as e:
        raise TaskLaunchError(f"{prefix}: {e}", retryable=False) from e


def _accept_connections(
    servers: dict[str, socket.socket],
    drains: dict[str, socket.socket],
    proc: subprocess.Popen,
    *,
    target_loggers: tuple[FilteringBoundLogger, ...],
    max_wait: float = 10.0,
    drain_size: int = 4096,
) -> tuple[dict[socket.socket, socket.socket], dict[socket.socket, bytes]]:
    """
    Block until the subprocess connects to servers, draining stdout/stderr along the way.

    :raises TaskLaunchError: when the subprocess exits or *max_wait* elapses before it connects. What
        the subprocess wrote to the *drains* until then goes to *target_loggers* first.
    """
    accepted: dict[socket.socket, socket.socket] = {}
    drained: dict[socket.socket, bytes] = {s: b"" for s in drains.values()}

    def _fail(reason: str) -> NoReturn:
        for s in accepted.values():
            s.close()
        _report_startup_output(target_loggers, drains, drained, drain_size)
        raise TaskLaunchError(f"The runtime {reason}")

    with selectors.DefaultSelector() as sel:
        for key, soc in itertools.chain(servers.items(), drains.items()):
            sel.register(soc, selectors.EVENT_READ, data=key)
        deadline = time.monotonic() + max_wait
        while len(accepted) < len(servers):
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                _fail(f"did not connect within {max_wait} seconds")
            if proc.poll() is not None:
                _fail(f"exited with {proc.returncode} before connecting")
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
                    signal_process_tree(o.pid, signal.SIGTERM, fallback=o.send_signal)
                    try:
                        o.wait(self.timeout)
                    except subprocess.TimeoutExpired:
                        signal_process_tree(o.pid, signal.SIGKILL, fallback=o.send_signal)
                    # A runtime that exited before connecting was already reaped, so its group
                    # is out of reach above while what it spawned is still running.
                    _kill_leftover_group(o.pid)

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
        subprocess_logs_to_stdout: bool = False,
        startup_timeout: float = 10.0,
        **kwargs,
    ) -> Self:
        process_log = logger or structlog.get_logger(logger_name="task").bind()
        # 'self' only exists once the runtime has connected, so the loggers its output is forwarded
        # to are resolved here, for a launch that fails before that.
        target_loggers = cls._default_target_loggers(process_log, subprocess_logs_to_stdout)
        with _ResourceTracker(timeout=startup_timeout) as tracker:
            with _raise_as_launch_error("The runtime cannot start"):
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
                    # Own process group, so the supervisor can signal everything the runtime
                    # spawned with os.killpg, as the Python task runner has done since #65505.
                    # Popen returns after exec, so the group exists before kill().
                    # Must agree with new_process_group=True below.
                    process_group=0,
                )
                tracker.track(proc)
                for soc in tracker.untrack(stdout_w, stderr_w):
                    soc.close()
                log.info("Starting subprocess", pid=proc.pid)

                socks, drained = _accept_connections(
                    {"comm": comm_server, "logs": logs_server},
                    {"stdout": stdout_r, "stderr": stderr_r},
                    proc,
                    target_loggers=target_loggers,
                    max_wait=startup_timeout,
                )
                tracker.track(*socks.values())

                self = cls(
                    id=what.id,
                    pid=proc.pid,
                    process=PopenTracker(proc),
                    process_log=process_log,
                    start_time=time.monotonic(),
                    stdin=socks[comm_server],
                    subprocess_schema_version=subprocess_schema_version,
                    subprocess_logs_to_stdout=subprocess_logs_to_stdout,
                    # The supervisor signals the group; must agree with process_group=0 above.
                    new_process_group=True,
                    comm_server=comm_server,
                    logs_server=logs_server,
                    **kwargs,
                )
                self._register_pipe_readers(
                    stdout_r, stderr_r, socks[comm_server], socks[logs_server], data=drained
                )
            self._on_child_started(
                ti=what,
                dag_rel_path=dag_rel_path,
                bundle_info=bundle_info,
                sentry_integration=sentry_integration,
            )

            # 'self' owns everything from here: it closes the pipes and the servers when the
            # subprocess exits in 'wait'. Until this point the tracker still owns them, so a
            # failure registering the pipes or starting the task instance releases them.
            tracker.untrack(
                stdout_r,
                stderr_r,
                socks[comm_server],
                socks[logs_server],
                comm_server,
                logs_server,
                proc,
            )

        return self

    def _on_child_started(
        self,
        *,
        ti: TaskInstance,
        dag_rel_path: str | os.PathLike[str],
        bundle_info,
        sentry_integration: str,
    ) -> None:
        super()._on_child_started(
            ti=ti,
            dag_rel_path=dag_rel_path,
            bundle_info=bundle_info,
            sentry_integration=sentry_integration,
        )
        # Language runtimes do not send SetExecutionTimeout, so the clock starts once they have the task.
        self._arm_execution_timeout()

    def _prepare_ti_context(self, ti: TaskInstance, ti_context: TIRunContext) -> TIRunContext | None:
        """
        Delete the XComs in ``ti_context.xcom_keys_to_clear`` so the launched runtime does not have to.

        Unlike ``XCom.delete``, this method does not purge the XCom backend, for two reasons. XComs that a
        runtime pushes go straight to the API server and never pass through the backend. Also, a purge may
        need a connection that a hook running in the supervisor process cannot look up, such as a
        connection that exists only in Airflow's metadata database. So if Python code stored an XCom for
        this task instance through the backend, deleting that XCom here leaves the stored object in the
        backend's storage.

        If an XCom cannot be deleted, this try fails and the task does not run.
        """
        for key in ti_context.xcom_keys_to_clear or ():
            self.process_log.debug("Clearing XCom with key", key=key)
            try:
                self.client.xcoms.delete(ti.dag_id, ti.run_id, ti.task_id, key, ti.map_index)
            except Exception as e:
                self.process_log.exception("Failed to clear XCom; not running the task", key=key)
                end_date = datetime.now(tz=UTC)
                reason = f"Failed to clear XCom {key!r}: {e}"
                # wait() sends this outcome to the server after the runtime process exits, the same
                # way it sends an outcome that the runtime itself reported.
                self._pending_terminal_state_msg = (
                    RetryTask(end_date=end_date, retry_reason=reason)
                    if ti_context.should_retry
                    else TaskState(state=TaskInstanceState.FAILED, end_date=end_date, retry_reason=reason)
                )
                self.kill(signal.SIGKILL)
                return None
        # The supervisor has already deleted the listed XComs, so it sends the runtime an empty list.
        # If a runtime also deletes these XComs itself, the empty list keeps the runtime from deleting
        # them a second time.
        return ti_context.model_copy(update={"xcom_keys_to_clear": []})

    def wait(self) -> int:
        code = super().wait()
        self._close_unused_sockets(self._comm_server, self._logs_server)
        return code

    def _handle_set_task_state_store(
        self, msg: SetTaskStateStore, log: FilteringBoundLogger, req_id: int
    ) -> RequestResult:
        # A Python task warns in its own setter; a language SDK runtime cannot compute the size
        # json.dumps gives, so the supervisor warns for it in the task log.
        if (limit := conf.getint("state_store", "max_value_storage_bytes")) > 0:
            if (size := len(json.dumps(msg.value))) > limit:
                self.process_log.warning(
                    "Task store value exceeds configured max_value_storage_bytes",
                    key=msg.key,
                    size=size,
                    max_value_storage_bytes=limit,
                )
        return super()._handle_set_task_state_store(msg, log, req_id)


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


def _resolve_bundle(bundle_info: BundleInfo, logger: FilteringBoundLogger) -> BaseDagBundle:
    """
    Materialize the Dag bundle of *bundle_info*, pinned to a concrete version.

    *logger* is the task logger, so materialization failures surface in the task log.

    :raises TaskLaunchError: when the bundle cannot be read.
    """
    cannot_read = f"Dag bundle {bundle_info.name!r} cannot be read"
    try:
        bundle = _initialize_pinned_bundle(bundle_info, logger)
    except Exception as e:
        raise TaskLaunchError(f"{cannot_read}: {e}") from e
    try:
        bundle.path.stat()
    except (FileNotFoundError, NotADirectoryError):
        raise TaskLaunchError(f"{cannot_read}: it resolved to {bundle.path}, which does not exist.") from None
    except OSError as e:
        raise TaskLaunchError(f"{cannot_read}: {e}") from e
    return bundle


def _is_file_in_bundle(bundle: BaseDagBundle, rel_path: str) -> bool:
    """
    Return whether *rel_path* names a file inside *bundle*.

    *rel_path* must be a relative path inside the bundle, with no ``..`` part. A symlink in the
    bundle is followed, as the Dag processor follows it when it lists the bundle's files.
    """
    path = PurePosixPath(rel_path)
    if path.is_absolute() or ".." in path.parts:
        return False
    try:
        return stat.S_ISREG((bundle.path / path).stat().st_mode)
    except (FileNotFoundError, NotADirectoryError):
        return False


def _require_supported_schema_version(version: str) -> None:
    """
    Check that the supervisor of this worker's Task SDK knows *version*.

    :raises TaskLaunchError: when it does not. Retrying cannot fix it, so this does not retry.
    """
    try:
        get_schema_version_migrator().resolve_version(version)
    except ValueError as e:
        raise TaskLaunchError(
            f"The runtime uses supervisor schema version {version!r}, which this worker's Task SDK "
            "does not support. Upgrade the worker's Airflow, or rebuild the artifact against it.",
            retryable=False,
        ) from e


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
    :param task_handler_bundle_name: Name of the Dag bundle that holds the compiled
        task handlers. It must be registered in ``[dag_processor] dag_bundle_config_list``.
        If unset, the task's own Dag bundle is used. A named bundle resolves to the
        version current when the task starts; the task's own bundle uses the run's
        version. Either way the resolved version is pinned for the whole task.
        A task of a native Dag does not read this bundle: it runs its own Dag file.
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

        :raises TaskLaunchError: when the bundle cannot be read.
        """
        if self.task_handler_bundle_name is None:
            target = bundle_info
        else:
            target = BundleInfo(name=self.task_handler_bundle_name)
        return _resolve_bundle(target, logger)

    def _get_scan_roots(self) -> tuple[pathlib.Path, ...]:
        """Return the artifact roots resolved for the active task or Dag parse."""
        if self._active_scan_roots is None:
            raise RuntimeError(
                "_get_scan_roots requires an active task or Dag parse; call it during execute_task or parse_dag."
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

        Raise ``TaskLaunchError(..., retryable=...)`` when the implementation knows whether
        retrying can help. Any other exception is reported as a launch failure with a
        retryability guessed from its type.
        """
        raise NotImplementedError

    def _build_dag_file_command(
        self, *, what: TaskInstance, path: pathlib.Path
    ) -> tuple[list[str], str | None]:
        """
        Build the command that runs *what* from the native Dag file at *path*.

        Subclasses can retrieve the root of the Dag bundle holding *path* with
        :meth:`_get_scan_roots`. The contract is that of :meth:`_build_execute_task_command`,
        including how to report whether a failure is worth retrying. Raise an exception when the
        file cannot run *what*.
        """
        raise NotImplementedError(f"{type(self).__name__} cannot run the tasks of a native Dag")

    def _build_parse_dag_command(self, *, path: pathlib.Path) -> tuple[list[str], str | None]:
        """
        Build the command that parses the Dag file at *path* and resolve its wire-schema version.

        Subclasses can retrieve the directories to scan for artifacts with
        :meth:`_get_scan_roots`; for a parse they are the Dag bundle's root. The contract is
        that of :meth:`_build_execute_task_command`: *command* MUST NOT include the
        ``--comm`` / ``--logs`` flags.
        """
        raise NotImplementedError

    def parse_dag(
        self,
        *,
        path: pathlib.Path,
        bundle_path: pathlib.Path,
        comm_address: tuple[str, int],
        logs_address: tuple[str, int],
        report_schema_version: Callable[[str | None], None],
    ) -> NoReturn:
        """
        Replace the current process with the runtime that parses the Dag file at *path*.

        Call this in a child process whose standard streams are already set up; the runtime
        inherits them and no other file descriptor. The command and its supervisor wire-schema
        version are resolved against *bundle_path*, and the version is passed to
        *report_schema_version* just before the exec. The runtime connects back to
        *comm_address* and *logs_address*.

        :raises Exception: when the command cannot be resolved or started; the process is then
            unchanged.
        """
        with self._set_scan_roots([bundle_path]):
            command, schema_version = self._build_parse_dag_command(path=path)
        if schema_version is not None:
            get_schema_version_migrator().resolve_version(schema_version)
        argv = [
            *command,
            f"--comm={comm_address[0]}:{comm_address[1]}",
            f"--logs={logs_address[0]}:{logs_address[1]}",
        ]
        report_schema_version(schema_version)
        # Python ignores these at startup and exec keeps ignored signals; subprocess.Popen resets
        # them the same way for the task runtime.
        for name in ("SIGPIPE", "SIGXFSZ"):
            if (sig := getattr(signal, name, None)) is not None:
                signal.signal(sig, signal.SIG_DFL)
        _set_close_on_exec_above_stderr()
        os.execvpe(argv[0], argv, _build_runtime_env())

    @contextlib.contextmanager
    def _set_scan_roots(self, roots: Sequence[pathlib.Path]):
        """Expose *roots* to the command builder for the duration of the task or Dag parse."""
        if self._active_scan_roots is not None:
            raise RuntimeError("SubprocessCoordinator.execute_task and parse_dag are not re-entrant.")
        self._active_scan_roots = tuple(roots)
        try:
            yield
        finally:
            self._active_scan_roots = None

    def _is_native_dag_file(self, rel_path: str, bundle_name: str) -> bool:
        """
        Return whether *rel_path* is a native Dag file, which this coordinator must run.

        :raises TaskLaunchError: when the bundle's Dag importers cannot be built, the coordinator class
            of the file's importer cannot be loaded, or the file is a native Dag file of a runtime that
            this coordinator does not run. None of these retries.
        """
        try:
            importer = find_claiming_importer(rel_path, bundle_name)
        except Exception as e:
            raise TaskLaunchError(
                f"Cannot tell whether {rel_path!r} is a native Dag file, because the Dag importers of "
                f"Dag bundle {bundle_name!r} cannot be built: {e.__cause__ or e}",
                retryable=False,
            ) from e
        if importer is None:
            return False
        try:
            runtime = import_string(importer.coordinator_classpath)
            is_runtime = isinstance(self, runtime)
        except Exception as e:
            raise TaskLaunchError(
                f"{rel_path!r} is a native Dag file, but its coordinator class "
                f"{importer.coordinator_classpath!r} cannot be loaded: {e}",
                retryable=False,
            ) from e
        if not is_runtime:
            raise TaskLaunchError(
                f"{rel_path!r} is a native Dag file that a {runtime.__name__} runs, but the task's queue "
                f"routes it to a {type(self).__name__}. Route the queue to a {runtime.__name__} in "
                "[sdk] queue_to_coordinator.",
                retryable=False,
            )
        return True

    def _build_native_task_command(
        self, *, what: TaskInstance, bundle: BaseDagBundle, rel_path: str
    ) -> tuple[list[str], str | None]:
        """
        Check that *rel_path* is a file of *bundle* that this coordinator can run, and return its command.

        :raises TaskLaunchError: when the file is missing or cannot be read, or cannot run *what*.
        """
        version = f" at version {bundle.version!r}" if bundle.version is not None else ""
        try:
            is_file = _is_file_in_bundle(bundle, rel_path)
        except OSError as e:
            raise TaskLaunchError(
                f"Dag file {rel_path!r} in Dag bundle {bundle.name!r}{version} cannot be read: {e}"
            ) from e
        if not is_file:
            raise TaskLaunchError(
                f"Dag file {rel_path!r} is not a file in Dag bundle {bundle.name!r}{version}"
            )
        with _raise_as_launch_error(
            f"Dag file {rel_path!r} in Dag bundle {bundle.name!r}{version} cannot run"
        ):
            command, schema_version = self._build_dag_file_command(what=what, path=bundle.path / rel_path)
        return command, schema_version

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
        """
        Run *what*.

        A task of a native Dag runs its own Dag file, *dag_rel_path* in the Dag bundle of
        *bundle_info* at the version of the run, and never another Dag file. Its runtime may still
        read other files of the bundle, such as the JARs on a Java classpath. Any other task runs the
        artifact found by scanning the bundle that holds the artifacts.

        :raises TaskLaunchError: when the runtime cannot start: a native Dag file cannot run, the
            artifact of any other task cannot be found or run, the artifact needs a supervisor schema
            version that this worker's Task SDK does not support, or the runtime exits or times out
            before it connects.
        """
        task_logger = logger or log
        rel_path = os.fspath(dag_rel_path)
        with contextlib.ExitStack() as stack:
            with _raise_as_launch_error(
                f"Cannot prepare the runtime of task {what.task_id!r} in Dag {what.dag_id!r}"
            ):
                is_native = self._is_native_dag_file(rel_path, bundle_info.name)
                if is_native:
                    bundle = _resolve_bundle(bundle_info, task_logger)
                else:
                    bundle = self._resolve_artifact_bundle(bundle_info, task_logger)
                # Hold the version lock across start()/wait() so bundle cleanup cannot
                # rmtree a version this task is still reading from, mirroring
                # task_runner.main() for the Python task path.
                stack.enter_context(BundleVersionLock(bundle_name=bundle.name, bundle_version=bundle.version))
                stack.enter_context(self._set_scan_roots([bundle.path]))
                if is_native:
                    command, subprocess_schema_version = self._build_native_task_command(
                        what=what, bundle=bundle, rel_path=rel_path
                    )
                else:
                    command, subprocess_schema_version = self._build_execute_task_command(what=what)
                if subprocess_schema_version is not None:
                    _require_supported_schema_version(subprocess_schema_version)
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
