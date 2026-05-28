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

import functools
import hashlib
import os
import pathlib
import struct
from typing import TYPE_CHECKING, Any

import attrs
import structlog
import yaml

from airflow.sdk.coordinators.socket.coordinator import SocketCoordinator
from airflow.sdk.execution_time.schema import get_schema_version_migrator

if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator, Sequence

    from structlog.typing import FilteringBoundLogger
    from typing_extensions import Self

    from airflow.sdk.execution_time.workloads.task import TaskInstanceDTO

log: FilteringBoundLogger = structlog.get_logger(logger_name="coordinators.executable")


FOOTER_MAGIC = b"AFBNDL01"
FOOTER_SIZE = 64
FOOTER_VERSION = 1
_HASH_READ_CHUNK = 1 << 20
# Upper bound on the verification cache.
_VERIFY_CACHE_MAXSIZE = 256


@attrs.define
class _Footer:
    """
    Parsed bundle trailer plus the byte offsets it implies.

    All region offsets (``source_start``, ``metadata_start``) and the file
    size at parse time are computed once in :meth:`read` so downstream
    consumers do not re-derive them.
    """

    path: pathlib.Path
    file_size: int
    source_len: int
    metadata_len: int
    footer_ver: int
    binary_sha256: bytes
    source_start: int
    metadata_start: int

    @classmethod
    def read(cls, path: pathlib.Path) -> Self | None:
        """
        Parse the trailer of *path* and return the resulting footer.

        Returns ``None`` only when *path* is provably not a bundle (file is
        smaller than the trailer, or the magic does not match).
        """
        size = path.stat().st_size
        if size < FOOTER_SIZE:
            return None

        with open(path, "rb") as f:
            f.seek(size - FOOTER_SIZE)
            trailer = f.read(FOOTER_SIZE)

        if len(trailer) != FOOTER_SIZE or trailer[56:64] != FOOTER_MAGIC:
            return None

        source_len, metadata_len, footer_ver = struct.unpack_from("<III", trailer, 0)
        if footer_ver != FOOTER_VERSION:
            raise ValueError(
                f"Unsupported bundle footer_ver={footer_ver} in {path}; "
                f"this runtime supports footer_ver={FOOTER_VERSION}."
            )

        binary_sha256 = bytes(trailer[12:44])
        reserved = trailer[44:56]
        if reserved != b"\x00" * 12:
            raise ValueError(f"Bundle trailer in {path} has non-zero reserved bytes.")

        metadata_start = size - FOOTER_SIZE - metadata_len
        source_start = metadata_start - source_len
        if source_start < 0:
            raise ValueError(f"Bundle trailer in {path} declares regions that extend past the start of file.")
        # Per the spec, the binary region [0, source_start) MUST be non-empty.
        if source_start == 0:
            raise ValueError(f"Bundle trailer in {path} leaves no room for the executable region.")

        return cls(
            path=path,
            file_size=size,
            source_len=source_len,
            metadata_len=metadata_len,
            footer_ver=footer_ver,
            binary_sha256=binary_sha256,
            source_start=source_start,
            metadata_start=metadata_start,
        )


def _hash_binary_region(path: pathlib.Path, source_start: int) -> bytes:
    """Compute SHA-256 over bytes ``[0, source_start)`` of *path*."""
    digest = hashlib.sha256()
    remaining = source_start
    with open(path, "rb") as f:
        while remaining > 0:
            chunk = f.read(min(_HASH_READ_CHUNK, remaining))
            if not chunk:
                raise ValueError(
                    f"Bundle {path} truncated while hashing binary region "
                    f"(expected {source_start} bytes, got {source_start - remaining})."
                )
            digest.update(chunk)
            remaining -= len(chunk)
    return digest.digest()


# LRU-bounded cache of computed binary-region digests keyed by
# (path, inode, mtime_ns, size). A cache hit means the file at *path* still
# has the same identity as when we last hashed it, so re-hashing on every
# exec is unnecessary. A miss (file replaced, mtime bumped, inode swapped
# under us) yields a different key and forces re-verification;
@functools.lru_cache(maxsize=_VERIFY_CACHE_MAXSIZE)
def _cached_binary_region_digest(
    path_str: str,
    source_start: int,
    st_ino: int,
    st_mtime_ns: int,
    st_size: int,
) -> bytes:
    # st_ino / st_mtime_ns / st_size participate in the cache key only; if any
    # of them change, the LRU treats it as a different entry and re-hashes.
    del st_ino, st_mtime_ns, st_size
    return _hash_binary_region(pathlib.Path(path_str), source_start)


def _verify_binary_sha256(footer: _Footer) -> bool:
    """Verify *footer.binary_sha256* against the binary region of ``footer.path``."""
    try:
        st = footer.path.stat()
    except OSError:
        return False

    try:
        actual = _cached_binary_region_digest(
            str(footer.path), footer.source_start, st.st_ino, st.st_mtime_ns, st.st_size
        )
    except (OSError, ValueError) as exc:
        log.debug("Failed to hash bundle binary region", path=str(footer.path), error=str(exc))
        return False

    if actual != footer.binary_sha256:
        log.debug(
            "Bundle binary_sha256 mismatch; skipping",
            path=str(footer.path),
            expected=footer.binary_sha256.hex(),
            actual=actual.hex(),
        )
        return False
    return True


def _read_bundle_metadata(path: pathlib.Path) -> dict[str, Any] | None:
    try:
        if (footer := _Footer.read(path)) is None:
            return None
    except (OSError, ValueError) as exc:
        log.debug("Invalid bundle trailer; skipping", path=str(path), error=str(exc))
        return None

    if not _verify_binary_sha256(footer):
        return None

    with open(path, "rb") as f:
        f.seek(footer.metadata_start)
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


def _supervisor_schema_version(metadata: dict[str, Any]) -> str | None:
    sdk = metadata.get("sdk")
    if not isinstance(sdk, dict):
        return None

    value = sdk.get("supervisor_schema_version")
    if not isinstance(value, str) or not value:
        return None

    return value


def _find_executables(items: Iterable[pathlib.Path]) -> Iterator[pathlib.Path]:
    """Yield executable regular files under *items*, descending into directories."""
    for item in items:
        if item.is_dir():
            try:
                children = item.iterdir()
            except (FileNotFoundError, NotADirectoryError, PermissionError):
                continue
            yield from _find_executables(children)
        elif item.is_file() and os.access(item, os.X_OK):
            yield item


def _validate_schema_version(instance, _, value) -> str:
    return get_schema_version_migrator().resolve_version(str(value))


@attrs.define
class _Bundle:
    path: pathlib.Path
    schema_version: str | None = attrs.field(validator=_validate_schema_version)

    @classmethod
    def find(cls, executables_root: Sequence[pathlib.Path], dag_id: str) -> Self:
        log.debug("Finding executable bundles recursively", roots=executables_root)
        for p in _find_executables(executables_root):
            if (metadata := _read_bundle_metadata(p)) is None:
                continue
            if dag_id not in _dag_ids(metadata):
                continue
            try:
                return cls(path=p.resolve(), schema_version=_supervisor_schema_version(metadata))
            except (TypeError, ValueError) as exc:
                log.debug("Bundle metadata rejected by validator; skipping", path=str(p), error=str(exc))
                continue
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

    executables_root: list[pathlib.Path] = attrs.field(
        converter=_convert_executables_root,
        validator=attrs.validators.min_len(1),
    )

    def _build_execute_task_command(self, *, what: TaskInstanceDTO) -> tuple[list[str], str | None]:
        bundle = _Bundle.find(self.executables_root, what.dag_id)
        return [str(bundle.path)], bundle.schema_version
