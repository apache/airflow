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
import struct
from typing import TYPE_CHECKING, Any, NamedTuple

import attrs
import structlog
import yaml

from airflow.sdk.coordinators.socket.coordinator import SocketCoordinator

if TYPE_CHECKING:
    from collections.abc import Sequence

    from structlog.typing import FilteringBoundLogger
    from typing_extensions import Self

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


def _convert_executables_root(
    value: None | os.PathLike[str] | pathlib.Path | list[os.PathLike[str] | pathlib.Path],
) -> list[pathlib.Path]:
    if value is None:
        return []
    if isinstance(value, (str, os.PathLike, pathlib.Path)):
        return [pathlib.Path(value).expanduser()]
    return [pathlib.Path(v).expanduser() for v in value]


@attrs.define(kw_only=True)
class ExecutableCoordinator(SocketCoordinator):
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
    :param task_startup_timeout: Maximum time the coordinator waits for a task
        process to start, in seconds. The default is 10 seconds.
    """

    sdk: str = "executable"
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

    def _build_execute_task_command(self, *, what: TaskInstanceDTO) -> tuple[list[str], str | None]:
        return [self._resolve_executable(what=what)], None
