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
"""Native executable coordinator that launches a binary subprocess for task execution."""

from __future__ import annotations

import os
import pathlib
import selectors
import socket
import struct
import subprocess
import time
from typing import TYPE_CHECKING, Any, NamedTuple, cast

import attrs
import psutil
import structlog
import yaml

from airflow.sdk.execution_time.coordinator import BaseCoordinator
from airflow.sdk.execution_time.supervisor import ActivitySubprocess

if TYPE_CHECKING:
    from collections.abc import Sequence

    from structlog.typing import FilteringBoundLogger
    from typing_extensions import Self

    from airflow.sdk.api.client import Client
    from airflow.sdk.api.datamodels._generated import BundleInfo
    from airflow.sdk.execution_time.workloads.task import TaskInstanceDTO

log: FilteringBoundLogger = structlog.get_logger(logger_name="coordinators.executable")


FOOTER_MAGIC = b"AFBNDL01"
FOOTER_SIZE = 32
FOOTER_VERSION = 1


class _Footer(NamedTuple):
    source_len: int
    metadata_len: int
    footer_ver: int


def _read_footer(path: pathlib.Path) -> _Footer | None:
    try:
        size = path.stat().st_size
    except OSError:
        return None
    if size < FOOTER_SIZE:
        return None
    try:
        with open(path, "rb") as f:
            f.seek(size - FOOTER_SIZE)
            trailer = f.read(FOOTER_SIZE)
    except OSError:
        return None
    if len(trailer) != FOOTER_SIZE or trailer[24:32] != FOOTER_MAGIC:
        return None
    source_len, metadata_len, footer_ver = struct.unpack_from("<III", trailer, 0)
    if footer_ver != FOOTER_VERSION:
        raise ValueError(
            f"Unsupported bundle footer_ver={footer_ver} in {path}; "
            f"this runtime supports footer_ver={FOOTER_VERSION}."
        )
    metadata_start = size - FOOTER_SIZE - metadata_len
    source_start = metadata_start - source_len
    if source_start < 0:
        raise ValueError(f"Bundle trailer in {path} declares regions that extend past the start of file.")
    # Per the spec, the binary region [0, source_start) MUST be non-empty.
    if source_start == 0:
        raise ValueError(f"Bundle trailer in {path} leaves no room for the executable region.")
    return _Footer(source_len=source_len, metadata_len=metadata_len, footer_ver=footer_ver)


def _read_bundle_metadata(path: pathlib.Path) -> dict[str, Any] | None:
    try:
        footer = _read_footer(path)
    except ValueError:
        return None
    if footer is None:
        return None
    metadata_start = path.stat().st_size - FOOTER_SIZE - footer.metadata_len
    with open(path, "rb") as f:
        f.seek(metadata_start)
        metadata_bytes = f.read(footer.metadata_len)
    try:
        data = yaml.safe_load(metadata_bytes.decode("utf-8"))
    except (UnicodeDecodeError, yaml.YAMLError):
        return None
    if not isinstance(data, dict):
        return None
    return data


def _dag_ids(metadata: dict[str, Any]) -> set[str]:
    dags = metadata.get("dags")
    if not isinstance(dags, dict):
        return set()
    return set(dags.keys())


@attrs.define
class _Bundle:
    path: pathlib.Path

    @classmethod
    def find(cls, executables_root: Sequence[pathlib.Path], dag_id: str) -> Self:
        for root in executables_root:
            for p in root.iterdir():
                if not p.is_file() or not os.access(p, os.X_OK):
                    continue
                if (metadata := _read_bundle_metadata(p)) is None:
                    continue
                if dag_id in _dag_ids(metadata):
                    return cls(p.resolve())
        resolved_paths = os.pathsep.join(str(r.resolve()) for r in executables_root)
        raise FileNotFoundError(
            f"cannot find executable bundle containing dag_id={dag_id!r} in {resolved_paths}"
        )


def _start_server() -> socket.socket:
    server = socket.socket()
    server.bind(("127.0.0.1", 0))
    server.setblocking(True)
    server.listen(1)  # Just need to listen to the child process.
    return server


def _accept_connections(
    servers: dict[str, socket.socket],
    proc: subprocess.Popen,
    *,
    max_wait: float = 10.0,
) -> dict[str, socket.socket]:
    """Block until the executable process connects to servers."""
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
class _ExecutableActivitySubprocess(ActivitySubprocess):
    """Native executable task runner process."""

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
        executable_path: str,
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
                executable_path,
                f"--comm={comm_host}:{comm_port}",
                f"--logs={logs_host}:{logs_port}",
            ],
            stdout=stdout_w.makefile("wb", buffering=0).fileno(),
            stderr=stderr_w.makefile("wb", buffering=0).fileno(),
        )
        log.info("Starting subprocess", pid=proc.pid, executable=executable_path)
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
        self._register_pipe_readers(stdout_r, stderr_r, socks["comm"], socks["logs"], data={})
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


def _convert_executables_root(
    value: None | os.PathLike[str] | pathlib.Path | list[os.PathLike[str] | pathlib.Path],
) -> list[pathlib.Path]:
    if value is None:
        return []
    if isinstance(value, (str, os.PathLike, pathlib.Path)):
        return [pathlib.Path(value)]
    return [pathlib.Path(v) for v in value]


@attrs.define(kw_only=True)
class ExecutableCoordinator(BaseCoordinator):
    """
    Coordinator that launches a native executable subprocess for task execution.

    Configuration is taken from the ``[sdk] coordinators`` entry that constructs
    this instance::

        {
            "name": "go",
            "classpath": "airflow.sdk.coordinators.executable.ExecutableCoordinator",
            "kwargs": {
                "executables_root": ["~/airflow/executable-bundles"],
            },
        }

    :param executables_root: A list of directories scanned for executable
        bundles when a Python stub DAG delegates task execution to a native
        runtime.
    """

    sdk: str = "executable"
    file_extension: str = ""
    executables_root: list[pathlib.Path] = attrs.field(converter=_convert_executables_root, factory=list)

    def _resolve_executable(self, *, what: TaskInstanceDTO) -> str:
        """
        Resolve the executable path for *what*.

        Looks up the bundle whose embedded manifest declares ``what.dag_id``
        in the configured ``executables_root`` directories.
        """
        if not self.executables_root:
            raise ValueError(
                "The executables_root kwarg must be set on the ExecutableCoordinator "
                "to resolve the executable for task execution."
            )
        return str(_Bundle.find(self.executables_root, what.dag_id).path)

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
        executable_path = self._resolve_executable(what=what)
        process = _ExecutableActivitySubprocess.start(
            what=what,
            dag_rel_path=dag_rel_path,
            bundle_info=bundle_info,
            client=client,
            logger=logger,
            subprocess_logs_to_stdout=subprocess_logs_to_stdout,
            sentry_integration=sentry_integration,
            executable_path=executable_path,
        )
        exit_code = process.wait()
        return self.ExecutionResult(exit_code, process.final_state)
