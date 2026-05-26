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
"""Java runtime coordinator that launches a JVM subprocess for Dag file processing and task execution."""

from __future__ import annotations

import email
import itertools
import os
import pathlib
import selectors
import signal
import socket
import subprocess
import time
import zipfile
from typing import TYPE_CHECKING, TypeVar, cast

import attrs
import structlog

from airflow.sdk.execution_time.coordinator import BaseCoordinator
from airflow.sdk.execution_time.schema import get_schema_version_migrator
from airflow.sdk.execution_time.supervisor import ActivitySubprocess, NeverRaised, ProcessTracker

if TYPE_CHECKING:
    from collections.abc import Sequence

    from structlog.typing import FilteringBoundLogger
    from typing_extensions import Self

    from airflow.sdk.api.client import Client
    from airflow.sdk.api.datamodels._generated import BundleInfo
    from airflow.sdk.execution_time.workloads.task import TaskInstanceDTO

    Tracked = TypeVar("Tracked", socket.socket, subprocess.Popen)

log: FilteringBoundLogger = structlog.get_logger(logger_name="coordinators.java")


def _start_server() -> socket.socket:
    server = socket.socket()
    server.bind(("127.0.0.1", 0))
    server.setblocking(True)
    server.listen(1)  # Just need to listen to the child process.
    return server


def _calculate_classpath(jars_root: Sequence[pathlib.Path]) -> str:
    jars = (p.as_posix() for root in jars_root for p in root.iterdir() if p.suffix == ".jar")
    return os.pathsep.join(jars)


@attrs.define
class _JarMetadata:
    main_class: str
    schema_version: str

    @classmethod
    def from_jar(cls, path: pathlib.Path) -> Self | None:
        try:
            with zipfile.ZipFile(path) as zf:
                try:
                    manifest_info = zf.getinfo("META-INF/MANIFEST.MF")
                except KeyError:
                    log.debug("JAR does not contain META-INF/MANIFEST.MF; ignored", path=path)
                    return None
                with zf.open(manifest_info) as f:
                    manifest = email.message_from_binary_file(f)
            return cls(manifest["Main-Class"], manifest["Airflow-Supervisor-Schema-Version"])
        except zipfile.BadZipFile:
            log.exception("Cannot read JAR; ignored", path=path)
            return None


def _validate_schema_version(instance, _, value) -> str:
    return get_schema_version_migrator().resolve_version(str(value))


@attrs.define
class _JarInfo:
    main_class: str
    schema_version: str = attrs.field(validator=_validate_schema_version)

    @attrs.define
    class _Progress:
        main_class: str | None = attrs.field(init=False, default=None)
        schema_version: str | None = attrs.field(init=False, default=None)

        def collect(self) -> _JarInfo | None:
            if self.main_class is None or self.schema_version is None:
                return None
            return _JarInfo(self.main_class, self.schema_version)

    @classmethod
    def find(cls, roots: Sequence[pathlib.Path], main_class: str) -> _JarInfo:
        progress = cls._Progress()
        for root in roots:
            log.debug("Finding required JAR metadata in directory", dir=root)
            for p in root.iterdir():
                if p.suffix != ".jar":
                    continue
                if (metadata := _JarMetadata.from_jar(p)) is None:
                    continue
                if metadata.main_class and ((main_class == metadata.main_class) or not main_class):
                    log.debug("JAR located with Main-Class metadata", path=p, main_class=metadata.main_class)
                    progress.main_class = metadata.main_class
                if metadata.schema_version:
                    log.debug(
                        "JAR located with Airflow-Supervisor-Schema-Version metadata",
                        path=p,
                        schema_version=metadata.schema_version,
                    )
                    progress.schema_version = metadata.schema_version
                if (result := progress.collect()) is not None:
                    return result
        if progress.main_class is not None:
            tp = "cannot find a JAR with Airflow-Supervisor-Schema-Version metadata in {1}"
        elif main_class:
            tp = "cannot find a JAR with Main-Class matching {0!r} in {1}"
        else:
            tp = "cannot find a JAR with Main-Class metadata in {1}"
        raise FileNotFoundError(tp.format(main_class, os.pathsep.join(os.fspath(p.resolve()) for p in roots)))


def _accept_connections(
    servers: dict[str, socket.socket],
    drains: dict[str, socket.socket],
    proc: subprocess.Popen,
    *,
    max_wait: float = 10.0,
    drain_size: int = 4096,
) -> tuple[dict[socket.socket, socket.socket], dict[socket.socket, bytes]]:
    """Block until the Java process connects to servers."""
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
class _JavaActivitySubprocess(ActivitySubprocess):
    """Java task runner process."""

    _comm_server: socket.socket
    _logs_server: socket.socket

    @classmethod
    def start(  # type: ignore[override]
        cls,
        *,
        what: TaskInstanceDTO,
        dag_rel_path: str | os.PathLike[str],
        bundle_info,
        logger: FilteringBoundLogger | None = None,
        sentry_integration: str = "",
        java_executable: str,
        jvm_args: list[str],
        jars_root: Sequence[pathlib.Path],
        main_class: str,
        **kwargs,
    ) -> Self:
        jar = _JarInfo.find(jars_root, main_class)
        with _ResourceTracker(timeout=10.0) as tracker:
            comm_server, logs_server = tracker.track(_start_server(), _start_server())
            stdout_r, stdout_w = tracker.track(*socket.socketpair())
            stderr_r, stderr_w = tracker.track(*socket.socketpair())

            proc = subprocess.Popen(
                [
                    java_executable,
                    "-classpath",
                    _calculate_classpath(jars_root),
                    *jvm_args,
                    jar.main_class,
                    # Arguments to MainClass...
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
            )
            tracker.track(*socks.values())

            self = cls(
                id=what.id,
                pid=proc.pid,
                process=PopenTracker(proc),
                process_log=logger or structlog.get_logger(logger_name="task").bind(),
                start_time=time.monotonic(),
                stdin=socks[comm_server],
                subprocess_schema_version=jar.schema_version,
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

            # Untrack everything left. 'self' keeps track of these and close the
            # servers when the subprocess exits in 'wait'.
            tracker.untrack(comm_server, logs_server, proc)

        return self

    def wait(self) -> int:
        code = super().wait()
        self._close_unused_sockets(self._comm_server, self._logs_server)
        return code


def _convert_jars_root(
    value: None | os.PathLike[str] | pathlib.Path | list[os.PathLike[str] | pathlib.Path],
) -> list[pathlib.Path]:
    if value is None:
        return []
    if isinstance(value, (str, os.PathLike, pathlib.Path)):
        return [pathlib.Path(value).expanduser()]
    return [pathlib.Path(v).expanduser() for v in value]


@attrs.define(kw_only=True)
class JavaCoordinator(BaseCoordinator):
    """
    Coordinator that launches a JVM subprocess for DAG parsing and task execution.

    Configuration is taken from the ``[sdk] coordinators`` entry that constructs
    this instance::

        {
            "name": "jdk-17",
            "classpath": "airflow.sdk.coordinators.java.JavaCoordinator",
            "kwargs": {
                "java_executable": "/usr/lib/jvm/java-17-openjdk/bin/java",
                "jvm_args": ["-Xmx1024m"],
                "jars_root": ["~/airflow/jars"],
            },
        }

    :param java_executable: Path to the ``java`` command (defaults to
        ``"java"``, which relies on ``$PATH``).
    :param jvm_args: Extra arguments passed to the JVM (e.g. ``["-Xmx512m"]``).
    :param jars_root: A list of directories scanned for JAR bundles.
    :param main_class: Explicit entry point to execute with *java_executable*.

    If *main_class* is not explicitly set, JavaCoordinator scans *jars_root* to
    find an executable JAR (one with Main-Class set in its metadata). If more
    than one executable JAR is found, it may be nondeterministic which one ends
    up being executed.

    A JAR containing metadata *Airflow-Supervisor-Schema-Version* should also be
    available to specify the wire schema version. The JAR containing the Java
    SDK automatically sets this, so you don't generally need to do anything if
    dependency JARs are deployed as-is. If you repackage the dependencies,
    however, you must also reproduce the metadata entry in one of the JARs.
    """

    java_executable: str = "java"
    jvm_args: list[str] = attrs.field(factory=list)
    jars_root: list[pathlib.Path] = attrs.field(converter=_convert_jars_root, factory=list)
    main_class: str = ""

    def execute_task(
        self,
        *,
        what: TaskInstanceDTO,
        dag_rel_path: str | os.PathLike[str],
        bundle_info: BundleInfo,
        client: Client,
        logger: FilteringBoundLogger | None = None,
        sentry_integration: str = "",
        subprocess_logs_to_stdout: bool,
        **kwargs,
    ) -> BaseCoordinator.ExecutionResult:
        process = _JavaActivitySubprocess.start(
            what=what,
            dag_rel_path=dag_rel_path,
            bundle_info=bundle_info,
            client=client,
            logger=logger,
            subprocess_logs_to_stdout=subprocess_logs_to_stdout,
            sentry_integration=sentry_integration,
            java_executable=self.java_executable,
            jvm_args=self.jvm_args,
            jars_root=self.jars_root,
            main_class=self.main_class,
        )
        exit_code = process.wait()
        return self.ExecutionResult(exit_code, process.final_state)
