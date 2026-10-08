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
"""Read the per-Dag source files an executable bundle embeds."""

from __future__ import annotations

import functools
import hashlib
import pathlib
import re
from typing import Any

import attrs

from airflow.sdk.coordinators._bundle_metadata import resolve_source_path
from airflow.sdk.coordinators.executable.coordinator import (
    _VERIFY_CACHE_MAXSIZE,
    _open_checked_bundle,
    _VerifiedBundle,
)

_SHA256_HEX = re.compile(r"[0-9a-f]{64}")


@attrs.define(frozen=True)
class _SourceRegion:
    """One embedded file's path and its byte range inside the source region."""

    path: str
    offset: int
    length: int
    sha256: str


@attrs.define(frozen=True)
class _BundleIndex:
    """
    A verified bundle's manifest and source index, shared by every read of the same file.

    The mappings are shared between callers, so they MUST NOT be mutated.
    """

    metadata: dict[str, Any]
    source_start: int
    regions: dict[str, _SourceRegion] | None


def read_bundle_source(bundle_path: pathlib.Path, dag_id: str | None = None) -> str | None:
    """
    Return the source file the bundle embeds for *dag_id*, or ``None``.

    Pass *dag_id* only for a Dag the bundle defines natively. Omit it for a Dag owned by another
    language (Python), which shows its own source; the result is then ``None`` rather than an
    unrelated file. A Dag mapped in ``dag_source_paths`` returns its own file, and one that is not
    (such as a Dag built dynamically) falls back to the entrypoint source.

    :raises ValueError: when the file is not a valid bundle or its embedded sources are invalid.
    """
    if dag_id is None:
        return None
    return _read_source(bundle_path, dag_id)


def read_bundle_entrypoint_source(bundle_path: pathlib.Path) -> str | None:
    """
    Return the entrypoint source the bundle embeds, or ``None`` when it embeds none.

    :raises ValueError: when the file is not a valid bundle or its embedded sources are invalid.
    """
    return _read_source(bundle_path, None)


def read_bundle_language(bundle_path: pathlib.Path) -> str | None:
    """Return the ``sdk.language`` of the bundle's metadata, or ``None`` when it has none."""
    try:
        sdk = _read_index(bundle_path).metadata.get("sdk")
    except ValueError:
        return None
    language = sdk.get("language") if isinstance(sdk, dict) else None
    return language if isinstance(language, str) and language else None


def _read_index(bundle_path: pathlib.Path) -> _BundleIndex:
    try:
        st = bundle_path.stat()
    except OSError as exc:
        raise ValueError(f"Cannot stat bundle file {bundle_path}: {exc}") from exc
    return _load_index(str(bundle_path), st.st_ino, st.st_mtime_ns, st.st_size)


@functools.lru_cache(maxsize=_VERIFY_CACHE_MAXSIZE)
def _load_index(path: str, ino: int, mtime_ns: int, size: int) -> _BundleIndex:
    # The file identity is part of the key, so a replaced bundle misses the cache and is read again.
    with _open_checked_bundle(pathlib.Path(path)) as bundle:
        return _BundleIndex(bundle.metadata, bundle.footer.source_start, _parse_source_regions(bundle))


def _read_source(bundle_path: pathlib.Path, dag_id: str | None) -> str | None:
    index = _read_index(bundle_path)
    if index.regions is None:
        return None
    source_path = resolve_source_path(index.metadata, dag_id)
    if source_path is None:
        return None
    return _decode_source(
        _read_region(bundle_path, index.source_start, index.regions[source_path]), source_path
    )


def _parse_source_regions(bundle: _VerifiedBundle) -> dict[str, _SourceRegion] | None:
    """Validate the ``sources`` index and the paths that refer to it, or return ``None`` without one."""
    metadata = bundle.metadata
    sources = metadata.get("sources")
    if sources is None:
        return None
    if not isinstance(sources, list):
        raise ValueError("bundle metadata sources must be a list")

    regions: dict[str, _SourceRegion] = {}
    for entry in sources:
        region = _parse_source_region(entry, bundle.footer.source_len)
        if region.path in regions:
            raise ValueError(f"bundle metadata declares duplicate source path {region.path!r}")
        regions[region.path] = region

    entrypoint_path = metadata.get("entrypoint_path")
    if entrypoint_path is not None and entrypoint_path not in regions:
        raise ValueError(f"bundle entrypoint_path {entrypoint_path!r} is not one of its sources")

    dag_source_paths = metadata.get("dag_source_paths")
    if dag_source_paths is not None:
        if not isinstance(dag_source_paths, dict):
            raise ValueError("bundle metadata dag_source_paths must be a mapping")
        for dag_id, source_path in dag_source_paths.items():
            if source_path not in regions:
                raise ValueError(f"bundle dag_source_paths maps {dag_id!r} to {source_path!r}, not a source")
    return regions


def _parse_source_region(entry: Any, source_len: int) -> _SourceRegion:
    if not isinstance(entry, dict):
        raise ValueError("bundle metadata sources entries must be mappings")
    path = entry.get("path")
    if not isinstance(path, str) or not path:
        raise ValueError("bundle metadata source path must be a non-empty string")
    offset = _non_negative_int(entry.get("offset"), path, "offset")
    length = _non_negative_int(entry.get("length"), path, "length")
    sha256 = entry.get("sha256")
    if not isinstance(sha256, str) or _SHA256_HEX.fullmatch(sha256) is None:
        raise ValueError(f"bundle source {path!r} sha256 must be 64 lowercase hexadecimal digits")
    if offset + length > source_len:
        raise ValueError(f"bundle source {path!r} extends past the source region")
    return _SourceRegion(path=path, offset=offset, length=length, sha256=sha256)


def _non_negative_int(value: Any, source_path: str, name: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise ValueError(f"bundle source {source_path!r} {name} must be a non-negative integer")
    return value


def _read_region(bundle_path: pathlib.Path, source_start: int, region: _SourceRegion) -> bytes:
    try:
        with open(bundle_path, "rb") as f:
            f.seek(source_start + region.offset)
            payload = f.read(region.length)
    except OSError as exc:
        raise ValueError(f"Cannot read source {region.path!r} of {bundle_path}: {exc}") from exc
    if len(payload) != region.length:
        raise ValueError(f"{bundle_path.name} was truncated while reading source {region.path!r}")
    if hashlib.sha256(payload).hexdigest() != region.sha256:
        raise ValueError(f"{bundle_path.name} source {region.path!r} SHA-256 mismatch")
    return payload


def _decode_source(payload: bytes, source_path: str) -> str:
    try:
        return payload.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ValueError(f"embedded airflow source {source_path!r} is not valid UTF-8: {exc}") from exc
