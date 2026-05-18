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
import socket
import subprocess
import time
import zipfile
from typing import TYPE_CHECKING, cast

import attrs
import psutil
import structlog

from airflow.sdk.execution_time.coordinator import BaseCoordinator
from airflow.sdk.execution_time.supervisor import ActivitySubprocess

if TYPE_CHECKING:
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


def _calculate_classpath(app_home: pathlib.Path) -> str:
    jars = (p.as_posix() for p in app_home.iterdir() if p.suffix == ".jar")
    return os.pathsep.join(jars)


def _find_main_class(app_home: pathlib.Path) -> str:
    for p in app_home.iterdir():
        if p.suffix != ".jar":
            continue
        with zipfile.ZipFile(p) as zf:
            with zf.open("META-INF/MANIFEST.MF") as f:
                if main_class := email.message_from_binary_file(f)["Main-Class"]:
                    return main_class
    raise FileNotFoundError(f"cannot fine main class in {app_home.resolve()}")


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


@attrs.define(kw_only=True)
class _JavaActivitySubprocess(ActivitySubprocess):
    """Java task runner process."""

    _comm_server: socket.socket
    _logs_server: socket.socket
    _child_process: subprocess.Popen

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
        bundles_folder: pathlib.Path,
        **kwargs,
    ) -> Self:
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
                _calculate_classpath(bundles_folder),
                *jvm_args,
                _find_main_class(bundles_folder),
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
            process=psutil.Process(proc.pid),
            process_log=logger or structlog.get_logger(logger_name="task").bind(),
            start_time=time.monotonic(),
            stdin=socks["comm"],
            child_process=proc,
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
                "bundles_folder": "~/airflow/java-bundles",
            },
        }

    :param java_executable: Path to the ``java`` binary (defaults to ``"java"``,
        which relies on ``$PATH``).
    :param jvm_args: Extra arguments passed to the JVM (e.g. ``["-Xmx512m"]``).
    :param bundles_folder: Directory scanned for JAR bundles when a Python
        stub DAG delegates task execution to Java.  Required for the stub-DAG
        flow; unused for pure-Java DAGs.
    """

    java_executable: str = "java"
    jvm_args: list[str] = attrs.field(factory=list)
    bundles_folder: pathlib.Path = attrs.field(converter=pathlib.Path)

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
            bundles_folder=self.bundles_folder,
        )
        exit_code = process.wait()
        return self.ExecutionResult(exit_code, process.final_state)
