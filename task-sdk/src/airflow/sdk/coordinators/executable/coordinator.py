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

import hashlib
import os
import pathlib
import stat
import struct
from collections import OrderedDict
from typing import TYPE_CHECKING, Any, BinaryIO

import attrs
import structlog

from airflow.sdk.coordinators._bundle_metadata import (
    ResolvedBundle,
    convert_roots,
    extract_supervisor_schema_version,
    parse_metadata_mapping,
)
from airflow.sdk.coordinators._subprocess import SubprocessCoordinator

if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator, Sequence

    from structlog.typing import FilteringBoundLogger
    from typing_extensions import Self

    from airflow.sdk.api.datamodels._generated import TaskInstance

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
    def read(cls, f: BinaryIO, path: pathlib.Path, file_size: int) -> Self | None:
        """
        Parse the trailer from an already-open binary handle on *path*.

        *file_size* MUST come from a stat of the same fd so the trailer
        offsets refer to the file currently held open (not whatever the
        path resolves to at some later moment).

        Returns ``None`` only when the file is provably not a bundle
        (smaller than the trailer, or the magic does not match).
        """
        if file_size < FOOTER_SIZE:
            return None

        f.seek(file_size - FOOTER_SIZE)
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

        metadata_start = file_size - FOOTER_SIZE - metadata_len
        source_start = metadata_start - source_len
        if source_start < 0:
            raise ValueError(f"Bundle trailer in {path} declares regions that extend past the start of file.")
        # Per the spec, the binary region [0, source_start) MUST be non-empty.
        if source_start == 0:
            raise ValueError(f"Bundle trailer in {path} leaves no room for the executable region.")

        return cls(
            path=path,
            file_size=file_size,
            source_len=source_len,
            metadata_len=metadata_len,
            footer_ver=footer_ver,
            binary_sha256=binary_sha256,
            source_start=source_start,
            metadata_start=metadata_start,
        )


def _hash_open_file(f: BinaryIO, length: int, path: pathlib.Path) -> bytes:
    """Compute SHA-256 over the first *length* bytes of *f* (seeks to 0 first)."""
    f.seek(0)
    digest = hashlib.sha256()
    remaining = length
    while remaining > 0:
        chunk = f.read(min(_HASH_READ_CHUNK, remaining))
        if not chunk:
            raise ValueError(
                f"Bundle {path} truncated while hashing binary region "
                f"(expected {length} bytes, got {length - remaining})."
            )
        digest.update(chunk)
        remaining -= len(chunk)
    return digest.digest()


_DigestKey = tuple[str, int, int, int, int]


class _BinaryDigestCache:
    """
    Bounded LRU cache of bundle binary-region digests.

    Entries are keyed by ``(path, source_start, st_ino, st_mtime_ns,
    st_size)``; a hit means the file at *path* still has the same
    identity as when we last hashed it. The bound prevents a
    long-running supervisor that sees many bundle redeploys from
    retaining every historical ``(ino, mtime_ns)`` tuple forever.
    """

    def __init__(self, maxsize: int) -> None:
        self._maxsize = maxsize
        self._entries: OrderedDict[_DigestKey, bytes] = OrderedDict()

    def get(self, key: _DigestKey) -> bytes | None:
        digest = self._entries.get(key)
        if digest is not None:
            self._entries.move_to_end(key)
        return digest

    def put(self, key: _DigestKey, digest: bytes) -> None:
        self._entries[key] = digest
        self._entries.move_to_end(key)
        while len(self._entries) > self._maxsize:
            self._entries.popitem(last=False)

    def clear(self) -> None:
        self._entries.clear()


# Single process-wide instance. A cache miss (file replaced, mtime
# bumped, inode swapped under us) yields a different key and forces
# re-verification.
_digest_cache = _BinaryDigestCache(maxsize=_VERIFY_CACHE_MAXSIZE)


def _read_bundle_metadata(path: pathlib.Path) -> dict[str, Any] | None:
    # One open per bundle: trailer-parse, hash (on cache miss), and
    # metadata-read all share the same fd, and the stat that keys the
    # digest cache comes from that fd too. This both halves the syscall
    # cost of the hot path and removes the trailer-vs-hash TOCTOU window
    # where a path could be swapped between separate opens.
    try:
        f = open(path, "rb")
    except OSError as exc:
        log.debug("Cannot open bundle file; skipping", path=str(path), error=str(exc))
        return None

    with f:
        try:
            st = os.fstat(f.fileno())
        except OSError as exc:
            log.debug("Cannot stat bundle file; skipping", path=str(path), error=str(exc))
            return None

        try:
            footer = _Footer.read(f, path, st.st_size)
        except (OSError, ValueError) as exc:
            log.debug("Invalid bundle trailer; skipping", path=str(path), error=str(exc))
            return None
        if footer is None:
            return None

        cache_key: _DigestKey = (str(path), footer.source_start, st.st_ino, st.st_mtime_ns, st.st_size)
        actual_digest = _digest_cache.get(cache_key)
        if actual_digest is None:
            try:
                actual_digest = _hash_open_file(f, footer.source_start, path)
            except (OSError, ValueError) as exc:
                log.debug("Failed to hash bundle binary region", path=str(path), error=str(exc))
                return None
            _digest_cache.put(cache_key, actual_digest)

        if actual_digest != footer.binary_sha256:
            log.debug(
                "Bundle binary_sha256 mismatch; skipping",
                path=str(path),
                expected=footer.binary_sha256.hex(),
                actual=actual_digest.hex(),
            )
            return None

        try:
            f.seek(footer.metadata_start)
            metadata_bytes = f.read(footer.metadata_len)
        except OSError as exc:
            log.debug("Cannot read bundle metadata; skipping", path=str(path), error=str(exc))
            return None

    try:
        return parse_metadata_mapping(metadata_bytes, source="bundle metadata")
    except ValueError as exc:
        log.debug("Cannot decode bundle metadata; skipping", path=str(path), error=str(exc))
        return None


def _dag_ids(metadata: dict[str, Any]) -> set[str]:
    dags = metadata.get("dags")
    if not isinstance(dags, dict):
        return set()

    return set(dags.keys())


def _find_executables(items: Iterable[pathlib.Path]) -> Iterator[pathlib.Path]:
    """
    Yield executable regular files under *items*, descending into directories.

    A symlink loop or a directory that hardlinks into one of its ancestors
    would otherwise recurse until the interpreter stack is exhausted, so
    directories are deduplicated by ``(st_dev, st_ino)`` for the duration
    of a single scan.
    """
    seen_dirs: set[tuple[int, int]] = set()
    yield from _walk_executables(items, seen_dirs)


def _walk_executables(
    items: Iterable[pathlib.Path], seen_dirs: set[tuple[int, int]]
) -> Iterator[pathlib.Path]:
    for item in items:
        try:
            st = item.stat()
        except OSError:
            continue
        if stat.S_ISDIR(st.st_mode):
            key = (st.st_dev, st.st_ino)
            if key in seen_dirs:
                log.debug("Skipping already-visited directory", path=str(item))
                continue
            seen_dirs.add(key)
            try:
                children = list(item.iterdir())
            except OSError:
                continue
            yield from _walk_executables(children, seen_dirs)
        elif stat.S_ISREG(st.st_mode) and os.access(item, os.X_OK):
            yield item


@attrs.define
class _Bundle(ResolvedBundle):
    @classmethod
    def find(cls, executables_root: Sequence[pathlib.Path], dag_id: str) -> Self:
        log.debug("Finding executable bundles recursively", roots=executables_root)
        rejected: list[tuple[pathlib.Path, str]] = []
        for p in _find_executables(executables_root):
            if (metadata := _read_bundle_metadata(p)) is None:
                continue
            if dag_id not in _dag_ids(metadata):
                continue

            try:
                return cls(path=p.resolve(), schema_version=extract_supervisor_schema_version(metadata))
            except (TypeError, ValueError) as exc:
                log.debug("Bundle metadata rejected; skipping", path=str(p), error=str(exc))
                rejected.append((p.resolve(), str(exc)))
                continue

        resolved_paths = os.pathsep.join(str(r.resolve()) for r in executables_root)
        if rejected:
            details = "; ".join(f"{path}: {reason}" for path, reason in rejected)
            tp = (
                "cannot find executable bundle with usable supervisor_schema_version "
                "for dag_id={0!r} in {1}: matching bundles were rejected ({2})"
            )
        else:
            tp = "cannot find executable bundle containing dag_id={0!r} in {1}"
            details = ""
        raise FileNotFoundError(tp.format(dag_id, resolved_paths, details))


@attrs.define(kw_only=True)
class ExecutableCoordinator(SubprocessCoordinator):
    """
    Coordinator that launches a native executable subprocess for task execution.

    Configuration is taken from the ``[sdk] coordinators`` entry that constructs
    this instance::

        "go": {
            "classpath": "airflow.sdk.coordinators.executable.ExecutableCoordinator",
            "kwargs": {
                "executables_root": ["~/airflow/executable-bundles"]
            }
        }

    :param executables_root: A list of directories scanned for executable
        bundles when a Python stub DAG delegates task execution to a native
        runtime.
    :param task_startup_timeout: Maximum time the coordinator waits for a task
        process to start, in seconds. The default is 10 seconds.
    """

    executables_root: list[pathlib.Path] = attrs.field(
        converter=convert_roots,
        validator=attrs.validators.min_len(1),
    )

    def _build_execute_task_command(self, *, what: TaskInstance) -> tuple[list[str], str | None]:
        bundle = _Bundle.find(self.executables_root, what.dag_id)
        return [str(bundle.path)], bundle.schema_version
