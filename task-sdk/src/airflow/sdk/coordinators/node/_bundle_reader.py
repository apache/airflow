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
Read and verify TypeScript Dag bundles.

File order: layout comment, metadata comment, then executable JavaScript.
Read the headers, verify the section digests, and return coordinator metadata.
"""

from __future__ import annotations

import hashlib
import json
import os
import pathlib
import re
from collections import OrderedDict
from typing import TYPE_CHECKING, Any

import attrs

from airflow.sdk.coordinators._bundle_metadata import extract_supervisor_schema_version

if TYPE_CHECKING:
    from typing import BinaryIO

# Format prefixes and whole-line limits must agree with the TypeScript encoder.
_LAYOUT_COMMENT_PREFIX = b"//# airflowBundle="
_MAX_LAYOUT_LINE_BYTES = 4096
_METADATA_COMMENT_PREFIX = b"//# airflowMetadata="
_MAX_METADATA_LINE_BYTES = 1024 * 1024
_SUPPORTED_BUNDLE_MAJOR_VERSION = 1

# Bound hashing memory and process-local cache growth independently of the format.
_HASH_CHUNK_BYTES = 1024 * 1024
_MAX_DIGEST_CACHE_ENTRIES = 256
_LOWER_HEX_DIGITS = frozenset("0123456789abcdef")


@attrs.define(frozen=True)
class _DeclaredSection:
    """A declared byte range (end exclusive) and its expected SHA-256 digest."""

    start: int
    end: int
    sha256: bytes


@attrs.define(frozen=True)
class _BundleLayout:
    """The metadata and executable sections described by the layout comment."""

    metadata: _DeclaredSection
    code: _DeclaredSection


@attrs.define(frozen=True)
class _DigestCacheKey:
    """File identity, timestamps, size, and declared layout for one cached calculation."""

    path: str
    metadata: _DeclaredSection
    code: _DeclaredSection
    device: int
    inode: int
    mtime_ns: int
    ctime_ns: int
    size: int


@attrs.define(frozen=True)
class _ComputedDigests:
    """SHA-256 digests calculated from the actual section bytes."""

    metadata: bytes
    code: bytes


@attrs.define(frozen=True)
class BundleMetadata:
    """
    Metadata fields needed by the coordinator, extracted after integrity verification.

    This is not the full metadata document. Supervisor schema compatibility is
    checked when the coordinator selects the bundle.
    """

    dag_ids: frozenset[str]
    supervisor_schema_version: str


def read_bundle(bundle_path: pathlib.Path) -> BundleMetadata:
    """Read and verify one exact TypeScript bundle file."""
    try:
        bundle_file = bundle_path.open("rb")
    except OSError as exc:
        raise OSError(f"cannot read {bundle_path.name}: {exc}") from exc

    with bundle_file:
        try:
            # Save file identity, size, and timestamps to detect changes during reading.
            initial_file_info = os.fstat(bundle_file.fileno())
        except OSError as exc:
            raise OSError(f"cannot read {bundle_path.name}: {exc}") from exc

        layout, metadata_payload = _read_bundle_headers(
            bundle_file, path=bundle_path, file_size=initial_file_info.st_size
        )
        _verify_integrity(bundle_file, path=bundle_path, layout=layout, initial_file_info=initial_file_info)

    # Interpret metadata only after checking its serialized bytes against the declared digest.
    return _parse_bundle_metadata(metadata_payload)


class _BundleDigestCache:
    """Process-local LRU of computed digests, not file contents or verification verdicts."""

    def __init__(self, maxsize: int) -> None:
        self._maxsize = maxsize
        self._entries: OrderedDict[_DigestCacheKey, _ComputedDigests] = OrderedDict()

    def get(self, key: _DigestCacheKey) -> _ComputedDigests | None:
        digests = self._entries.get(key)
        if digests is not None:
            self._entries.move_to_end(key)
        return digests

    def put(self, key: _DigestCacheKey, digests: _ComputedDigests) -> None:
        self._entries[key] = digests
        self._entries.move_to_end(key)
        while len(self._entries) > self._maxsize:
            self._entries.popitem(last=False)

    def clear(self) -> None:
        self._entries.clear()


_digest_cache = _BundleDigestCache(maxsize=_MAX_DIGEST_CACHE_ENTRIES)


def _parse_offset(section: dict[str, Any], field: str) -> int:
    value = section.get(field)
    if (
        not isinstance(value, str)
        or len(value) != 16
        or any(character not in _LOWER_HEX_DIGITS for character in value)
    ):
        raise ValueError(f"bundle layout {field} offset must be a 16-digit lowercase hexadecimal string")
    return int(value, 16)


def _parse_section(layout: dict[str, Any], name: str) -> _DeclaredSection:
    section = layout.get(name)
    if not isinstance(section, dict):
        raise ValueError(f"bundle layout is missing the {name} section")
    start = _parse_offset(section, "start")
    end = _parse_offset(section, "end")
    if start >= end:
        raise ValueError(f"bundle layout {name} section must contain at least one byte")
    sha256 = section.get("sha256")
    if (
        not isinstance(sha256, str)
        or len(sha256) != 64
        or any(character not in _LOWER_HEX_DIGITS for character in sha256)
    ):
        raise ValueError(f"bundle layout {name}.sha256 must be 64 lowercase hexadecimal digits")
    return _DeclaredSection(start=start, end=end, sha256=bytes.fromhex(sha256))


def _parse_layout(payload: bytes) -> _BundleLayout:
    try:
        layout = json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError, RecursionError) as exc:
        raise ValueError(f"cannot parse embedded airflow bundle layout: {exc}") from exc
    if not isinstance(layout, dict):
        raise ValueError("embedded airflow bundle layout must contain a mapping")
    return _BundleLayout(
        metadata=_parse_section(layout, "metadata"),
        code=_parse_section(layout, "code"),
    )


def _is_supported_bundle_version(value: Any) -> bool:
    if not isinstance(value, str) or re.fullmatch(r"[0-9]+\.[0-9]+(?:\.[0-9]+)?", value) is None:
        return False
    # Compare decimal text without converting an arbitrarily long number to int.
    major = value.partition(".")[0].lstrip("0") or "0"
    return major == str(_SUPPORTED_BUNDLE_MAJOR_VERSION)


def _hash_region(bundle_file: BinaryIO, *, start: int, end: int, path: pathlib.Path, section: str) -> bytes:
    # Hash incrementally so large executable sections do not need to fit in memory.
    bundle_file.seek(start)
    hasher = hashlib.sha256()
    remaining_bytes = end - start
    while remaining_bytes:
        chunk = bundle_file.read(min(_HASH_CHUNK_BYTES, remaining_bytes))
        if not chunk:
            raise ValueError(f"{path.name} was truncated while hashing its {section} region")
        hasher.update(chunk)
        remaining_bytes -= len(chunk)
    return hasher.digest()


def _get_file_identity_fields(file_info: os.stat_result) -> tuple[int, int, int, int, int]:
    """Select file identity, timestamps, and size for before/after comparisons."""
    return (
        file_info.st_dev,
        file_info.st_ino,
        file_info.st_mtime_ns,
        file_info.st_ctime_ns,
        file_info.st_size,
    )


def _read_prefixed_line(
    bundle_file: BinaryIO,
    *,
    path: pathlib.Path,
    marker: bytes,
    max_bytes: int,
    section: str,
    missing_error: str,
) -> bytes:
    """Read one bounded, newline-terminated bundle line and return its payload."""
    try:
        line = bundle_file.readline(max_bytes + 1)
    except OSError as exc:
        raise OSError(f"cannot read {path.name}: {exc}") from exc
    if not line.startswith(marker):
        raise ValueError(missing_error)
    if len(line) > max_bytes:
        raise ValueError(f"embedded airflow {section} exceeds {max_bytes} bytes")
    if not line.endswith(b"\n"):
        raise ValueError(f"embedded airflow {section} is not newline-terminated")
    payload = line[len(marker) : -1]
    # These characters would end the JavaScript comment even inside JSON strings.
    if b"\r" in payload or b"\xe2\x80\xa8" in payload or b"\xe2\x80\xa9" in payload:
        raise ValueError(f"embedded airflow {section} contains a JavaScript line terminator")
    return payload


def _compute_stable_digests(
    bundle_file: BinaryIO,
    *,
    path: pathlib.Path,
    layout: _BundleLayout,
    initial_file_info: os.stat_result,
) -> _ComputedDigests:
    digests = _ComputedDigests(
        metadata=_hash_region(
            bundle_file, start=layout.metadata.start, end=layout.metadata.end, path=path, section="metadata"
        ),
        code=_hash_region(
            bundle_file, start=layout.code.start, end=layout.code.end, path=path, section="code"
        ),
    )
    try:
        post_hash_file_info = os.fstat(bundle_file.fileno())
    except OSError as exc:
        raise OSError(f"cannot stat {path.name} after verification: {exc}") from exc
    # Reject a detected file change before caching the calculated digests.
    if _get_file_identity_fields(post_hash_file_info) != _get_file_identity_fields(initial_file_info):
        raise ValueError(f"{path.name} changed while its integrity was being verified")
    return digests


def _verify_integrity(
    bundle_file: BinaryIO,
    *,
    path: pathlib.Path,
    layout: _BundleLayout,
    initial_file_info: os.stat_result,
) -> None:
    cache_key = _DigestCacheKey(
        path=os.fspath(path),
        metadata=layout.metadata,
        code=layout.code,
        device=initial_file_info.st_dev,
        inode=initial_file_info.st_ino,
        mtime_ns=initial_file_info.st_mtime_ns,
        ctime_ns=initial_file_info.st_ctime_ns,
        size=initial_file_info.st_size,
    )
    # Reuse calculated hashes only when the file information and declared layout match.
    computed_digests = _digest_cache.get(cache_key)
    if computed_digests is None:
        computed_digests = _compute_stable_digests(
            bundle_file, path=path, layout=layout, initial_file_info=initial_file_info
        )
        _digest_cache.put(cache_key, computed_digests)

    for section, computed_digest, declared_digest in (
        ("metadata", computed_digests.metadata, layout.metadata.sha256),
        ("code", computed_digests.code, layout.code.sha256),
    ):
        if computed_digest != declared_digest:
            raise ValueError(f"{path.name} {section} SHA-256 mismatch")

    # Check again on both cache-hit and cache-miss paths.
    try:
        final_file_info = os.fstat(bundle_file.fileno())
    except OSError as exc:
        raise OSError(f"cannot stat {path.name} after reading it: {exc}") from exc
    if _get_file_identity_fields(final_file_info) != _get_file_identity_fields(initial_file_info):
        raise ValueError(f"{path.name} changed while it was being read")


def _parse_bundle_metadata(payload: bytes) -> BundleMetadata:
    """Validate the metadata document and extract the fields needed by the coordinator."""
    try:
        metadata = json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError, RecursionError) as exc:
        raise ValueError(f"cannot parse embedded airflow metadata: {exc}") from exc
    if not isinstance(metadata, dict):
        raise ValueError("embedded airflow metadata must contain a mapping")
    value = metadata.get("airflow_bundle_metadata_version")
    if not _is_supported_bundle_version(value):
        raise ValueError(
            f"unsupported airflow bundle metadata version {value!r}; "
            f"this runtime supports major version {_SUPPORTED_BUNDLE_MAJOR_VERSION}"
        )
    dags = metadata.get("dags")
    if not isinstance(dags, dict):
        raise ValueError("embedded airflow metadata must contain a dags mapping")
    return BundleMetadata(
        dag_ids=frozenset(dags),
        supervisor_schema_version=extract_supervisor_schema_version(metadata),
    )


def _read_bundle_headers(
    bundle_file: BinaryIO, *, path: pathlib.Path, file_size: int
) -> tuple[_BundleLayout, bytes]:
    """Read both comment payloads and check their declared ranges against the file."""
    layout_payload = _read_prefixed_line(
        bundle_file,
        path=path,
        marker=_LAYOUT_COMMENT_PREFIX,
        max_bytes=_MAX_LAYOUT_LINE_BYTES,
        section="bundle layout",
        missing_error=f"{path.name} has no airflow bundle layout; rebuild with airflow-ts-pack",
    )
    layout = _parse_layout(layout_payload)
    metadata_payload = _read_prefixed_line(
        bundle_file,
        path=path,
        marker=_METADATA_COMMENT_PREFIX,
        max_bytes=_MAX_METADATA_LINE_BYTES,
        section="metadata",
        missing_error=f"{path.name} has no embedded airflow metadata after its layout",
    )
    layout_line_size = len(_LAYOUT_COMMENT_PREFIX) + len(layout_payload) + 1
    metadata_start = layout_line_size + len(_METADATA_COMMENT_PREFIX)
    metadata_end = metadata_start + len(metadata_payload)
    code_start = metadata_end + 1
    if (layout.metadata.start, layout.metadata.end) != (metadata_start, metadata_end):
        raise ValueError("bundle layout metadata offsets do not match the metadata section")
    if (layout.code.start, layout.code.end) != (code_start, file_size):
        raise ValueError("bundle layout code offsets do not match the executable section")
    return layout, metadata_payload
