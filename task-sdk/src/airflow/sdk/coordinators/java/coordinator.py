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
import os
import pathlib
import selectors
import signal
import socket
import subprocess
import time
import zipfile
from typing import TYPE_CHECKING, cast

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
class _MainJar:
    path: pathlib.Path
    main_class: str
    schema_version: str = attrs.field(validator=_validate_schema_version)

    @classmethod
    def find(cls, roots: Sequence[pathlib.Path], main_class: str) -> Self:
        for root in roots:
            log.debug("Finding Main-Class in directory", dir=root)
            for p in root.iterdir():
                if p.suffix != ".jar":
                    continue
                if (metadata := _JarMetadata.from_jar(p)) is None:
                    continue
                if not metadata.main_class:
                    continue
                if main_class and metadata.main_class != main_class:
                    continue
                log.debug("JAR located with Main-Class metadata", path=p, main_class=metadata.main_class)
                if not metadata.schema_version:
                    raise ValueError("JAR with Main-Class found without Airflow-Supervisor-Schema-Version")
                return cls(p, metadata.main_class, metadata.schema_version)
        if main_class:
            tp = "cannot find a JAR with Main-Class matching {0} in {1}"
        else:
            tp = "cannot find a JAR with Main-Class metadata in {1}"
        raise FileNotFoundError(tp.format(main_class, os.pathsep.join(os.fspath(p.resolve()) for p in roots)))


def _accept_connections(
    servers: dict[str, socket.socket],
    proc: subprocess.Popen,
    *,
    max_wait: float = 10.0,
) -> dict[str, socket.socket]:
    """Block until the Java process connects to servers."""
    accepted: dict[str, socket.socket] = {}
    with selectors.DefaultSelector() as sel:
        for key, soc in servers.items():
            sel.register(soc, selectors.EVENT_READ, data=key)
        deadline = time.monotonic() + max_wait
        while len(accepted) < len(servers):
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError("process did not connect within timeout")
            if proc.poll() is not None:
                raise RuntimeError(f"process exited with {proc.returncode} before connecting")
            for event, _ in sel.select(timeout=min(remaining, 1.0)):
                log.debug("Accepting child process connection", key=(key := event.data))
                conn, _ = cast("socket.socket", event.fileobj).accept()
                sel.unregister(servers[key])
                accepted[key] = conn
    return accepted


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
class _JavaActivitySubprocess(ActivitySubprocess):
    """Java task runner process."""

    _comm_server: socket.socket
    _logs_server: socket.socket

    # Keep track of channels used to pipe subprocess stdout and stderr so we can
    # close them on exit. The "read" side is handled by _register_pipe_readers
    # callbacks so we don't need to worry about them.
    _stdout_w: socket.socket
    _stderr_w: socket.socket

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
        jar = _MainJar.find(jars_root, main_class)

        comm_server = _start_server()
        logs_server = _start_server()

        stdout_r, stdout_w = socket.socketpair()
        stderr_r, stderr_w = socket.socketpair()

        comm_host, comm_port = comm_server.getsockname()
        logs_host, logs_port = logs_server.getsockname()

        proc = subprocess.Popen(
            [
                java_executable,
                "-classpath",
                _calculate_classpath(jars_root),
                *jvm_args,
                jar.main_class,
                # Arguments to MainClass...
                f"--comm={comm_host}:{comm_port}",
                f"--logs={logs_host}:{logs_port}",
            ],
            stdout=stdout_w.makefile("wb", buffering=0).fileno(),
            stderr=stderr_w.makefile("wb", buffering=0).fileno(),
        )
        log.info("Starting subprocess", pid=proc.pid)
        socks = _accept_connections({"comm": comm_server, "logs": logs_server}, proc)

        self = cls(
            id=what.id,
            pid=proc.pid,
            process=PopenTracker(proc),
            process_log=logger or structlog.get_logger(logger_name="task").bind(),
            start_time=time.monotonic(),
            stdin=socks["comm"],
            subprocess_schema_version=jar.schema_version,
            comm_server=comm_server,
            logs_server=logs_server,
            stdout_w=stdout_w,
            stderr_w=stderr_w,
            **kwargs,
        )
        self._register_pipe_readers(stdout_r, stderr_r, socks["comm"], socks["logs"])
        self._on_child_started(
            ti=what,
            dag_rel_path=dag_rel_path,
            bundle_info=bundle_info,
            sentry_integration=sentry_integration,
        )
        return self

    def wait(self) -> int:
        code = super().wait()
        self._close_unused_sockets(self._comm_server, self._logs_server, self._stdout_w, self._stderr_w)
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
