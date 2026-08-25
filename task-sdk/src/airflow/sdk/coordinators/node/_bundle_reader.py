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
"""Read and verify TypeScript Dag bundles."""

from __future__ import annotations

import base64
import binascii
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

EMBEDDED_LAYOUT_MARKER = b"//# airflowBundle="
EMBEDDED_LAYOUT_MAX_BYTES = 4096
EMBEDDED_METADATA_MARKER = b"//# airflowMetadata="
EMBEDDED_METADATA_MAX_BYTES = 1024 * 1024
BUNDLE_METADATA_VERSION_MAJOR = 1
_HASH_READ_CHUNK = 1 << 20
_VERIFY_CACHE_MAXSIZE = 256
_LOWER_HEX_DIGITS = frozenset("0123456789abcdef")


@attrs.define(frozen=True)
class _Section:
    start: int
    end: int
    sha256: bytes


@attrs.define(frozen=True)
class _BundleLayout:
    metadata: _Section
    code: _Section


@attrs.define(frozen=True)
class _BundleDigestKey:
    path: str
    metadata: _Section
    code: _Section
    device: int
    inode: int
    mtime_ns: int
    ctime_ns: int
    size: int


@attrs.define(frozen=True)
class _BundleDigests:
    metadata: bytes
    code: bytes


@attrs.define(frozen=True)
class BundleMetadata:
    """Verified metadata needed to select and launch a TypeScript bundle."""

    dag_ids: frozenset[str]
    supervisor_schema_version: str


class _BundleDigestCache:
    """Bounded LRU cache keyed by the open file's identity and declared layout."""

    def __init__(self, maxsize: int) -> None:
        self._maxsize = maxsize
        self._entries: OrderedDict[_BundleDigestKey, _BundleDigests] = OrderedDict()

    def get(self, key: _BundleDigestKey) -> _BundleDigests | None:
        digests = self._entries.get(key)
        if digests is not None:
            self._entries.move_to_end(key)
        return digests

    def put(self, key: _BundleDigestKey, digests: _BundleDigests) -> None:
        self._entries[key] = digests
        self._entries.move_to_end(key)
        while len(self._entries) > self._maxsize:
            self._entries.popitem(last=False)

    def clear(self) -> None:
        self._entries.clear()


_digest_cache = _BundleDigestCache(maxsize=_VERIFY_CACHE_MAXSIZE)


def _decode_base64(payload: bytes, *, section: str) -> bytes:
    try:
        return base64.b64decode(payload, validate=True)
    except (binascii.Error, ValueError) as exc:
        raise ValueError(f"cannot parse embedded airflow {section}: {exc}") from exc


def _parse_offset(section: dict[str, Any], field: str) -> int:
    value = section.get(field)
    if (
        not isinstance(value, str)
        or len(value) != 16
        or any(character not in _LOWER_HEX_DIGITS for character in value)
    ):
        raise ValueError(f"bundle layout {field} offset must be a 16-digit lowercase hexadecimal string")
    return int(value, 16)


def _parse_section(layout: dict[str, Any], name: str) -> _Section:
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
    return _Section(start=start, end=end, sha256=bytes.fromhex(sha256))


def _parse_layout(payload: bytes) -> _BundleLayout:
    layout_bytes = _decode_base64(payload, section="bundle layout")
    try:
        layout = json.loads(layout_bytes.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError, RecursionError) as exc:
        raise ValueError(f"cannot parse embedded airflow bundle layout: {exc}") from exc
    if not isinstance(layout, dict):
        raise ValueError("embedded airflow bundle layout must contain a mapping")
    return _BundleLayout(
        metadata=_parse_section(layout, "metadata"),
        code=_parse_section(layout, "code"),
    )


def _is_supported_metadata_version(value: Any) -> bool:
    if not isinstance(value, str) or re.fullmatch(r"[0-9]+\.[0-9]+(?:\.[0-9]+)?", value) is None:
        return False
    major = value.partition(".")[0].lstrip("0") or "0"
    return major == str(BUNDLE_METADATA_VERSION_MAJOR)


def _validate_metadata_version(metadata: dict[str, Any]) -> None:
    value = metadata.get("airflow_bundle_metadata_version")
    if not _is_supported_metadata_version(value):
        raise ValueError(
            f"unsupported airflow bundle metadata version {value!r}; "
            f"this runtime supports major version {BUNDLE_METADATA_VERSION_MAJOR}"
        )


def _hash_region(f: BinaryIO, *, start: int, end: int, path: pathlib.Path, section: str) -> bytes:
    f.seek(start)
    digest = hashlib.sha256()
    remaining = end - start
    while remaining:
        chunk = f.read(min(_HASH_READ_CHUNK, remaining))
        if not chunk:
            raise ValueError(f"{path.name} was truncated while hashing its {section} region")
        digest.update(chunk)
        remaining -= len(chunk)
    return digest.digest()


def _stat_identity(stat_result: os.stat_result) -> tuple[int, int, int, int, int]:
    return (
        stat_result.st_dev,
        stat_result.st_ino,
        stat_result.st_mtime_ns,
        stat_result.st_ctime_ns,
        stat_result.st_size,
    )


def _read_prefixed_line(
    f: BinaryIO,
    *,
    path: pathlib.Path,
    marker: bytes,
    max_bytes: int,
    section: str,
    missing_error: str,
) -> bytes:
    """Read one bounded, newline-terminated bundle line and return its payload."""
    try:
        line = f.readline(max_bytes + 1)
    except OSError as exc:
        raise OSError(f"cannot read {path.name}: {exc}") from exc
    if not line.startswith(marker):
        raise ValueError(missing_error)
    if len(line) > max_bytes:
        raise ValueError(f"embedded airflow {section} exceeds {max_bytes} bytes")
    if not line.endswith(b"\n"):
        raise ValueError(f"embedded airflow {section} is not newline-terminated")
    payload = line[len(marker) : -1]
    if b"\r" in payload or b"\xe2\x80\xa8" in payload or b"\xe2\x80\xa9" in payload:
        raise ValueError(f"embedded airflow {section} contains a JavaScript line terminator")
    return payload


def _validate_layout(
    layout: _BundleLayout,
    *,
    metadata_start: int,
    metadata_end: int,
    code_start: int,
    code_end: int,
) -> None:
    if (layout.metadata.start, layout.metadata.end) != (metadata_start, metadata_end):
        raise ValueError("bundle layout metadata offsets do not match the metadata section")
    if (layout.code.start, layout.code.end) != (code_start, code_end):
        raise ValueError("bundle layout code offsets do not match the executable section")


def _verify_integrity(
    f: BinaryIO,
    *,
    path: pathlib.Path,
    layout: _BundleLayout,
    initial_stat: os.stat_result,
) -> None:
    cache_key = _BundleDigestKey(
        path=os.fspath(path),
        metadata=layout.metadata,
        code=layout.code,
        device=initial_stat.st_dev,
        inode=initial_stat.st_ino,
        mtime_ns=initial_stat.st_mtime_ns,
        ctime_ns=initial_stat.st_ctime_ns,
        size=initial_stat.st_size,
    )
    actual_digests = _digest_cache.get(cache_key)
    if actual_digests is None:
        actual_digests = _BundleDigests(
            metadata=_hash_region(
                f,
                start=layout.metadata.start,
                end=layout.metadata.end,
                path=path,
                section="metadata",
            ),
            code=_hash_region(
                f,
                start=layout.code.start,
                end=layout.code.end,
                path=path,
                section="code",
            ),
        )
        try:
            post_hash_stat = os.fstat(f.fileno())
        except OSError as exc:
            raise OSError(f"cannot stat {path.name} after verification: {exc}") from exc
        if _stat_identity(post_hash_stat) != _stat_identity(initial_stat):
            raise ValueError(f"{path.name} changed while its integrity was being verified")
        _digest_cache.put(cache_key, actual_digests)

    for section, actual_digest, expected_digest in (
        ("metadata", actual_digests.metadata, layout.metadata.sha256),
        ("code", actual_digests.code, layout.code.sha256),
    ):
        if actual_digest != expected_digest:
            raise ValueError(f"{path.name} {section} SHA-256 mismatch")

    try:
        final_stat = os.fstat(f.fileno())
    except OSError as exc:
        raise OSError(f"cannot stat {path.name} after reading it: {exc}") from exc
    if _stat_identity(final_stat) != _stat_identity(initial_stat):
        raise ValueError(f"{path.name} changed while it was being read")


def _decode_metadata(payload: bytes) -> BundleMetadata:
    try:
        metadata = json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError, RecursionError) as exc:
        raise ValueError(f"cannot parse embedded airflow metadata: {exc}") from exc
    if not isinstance(metadata, dict):
        raise ValueError("embedded airflow metadata must contain a mapping")
    _validate_metadata_version(metadata)
    dags = metadata.get("dags")
    if not isinstance(dags, dict):
        raise ValueError("embedded airflow metadata must contain a dags mapping")
    return BundleMetadata(
        dag_ids=frozenset(dags),
        supervisor_schema_version=extract_supervisor_schema_version(metadata),
    )


def read_bundle(bundle_path: pathlib.Path) -> BundleMetadata:
    """Read and verify one exact TypeScript bundle file."""
    try:
        bundle_file = bundle_path.open("rb")
    except OSError as exc:
        raise OSError(f"cannot read {bundle_path.name}: {exc}") from exc

    with bundle_file:
        try:
            stat_result = os.fstat(bundle_file.fileno())
        except OSError as exc:
            raise OSError(f"cannot read {bundle_path.name}: {exc}") from exc

        layout_payload = _read_prefixed_line(
            bundle_file,
            path=bundle_path,
            marker=EMBEDDED_LAYOUT_MARKER,
            max_bytes=EMBEDDED_LAYOUT_MAX_BYTES,
            section="bundle layout",
            missing_error=f"{bundle_path.name} has no airflow bundle layout; rebuild with airflow-ts-pack",
        )
        layout = _parse_layout(layout_payload)
        metadata_payload = _read_prefixed_line(
            bundle_file,
            path=bundle_path,
            marker=EMBEDDED_METADATA_MARKER,
            max_bytes=EMBEDDED_METADATA_MAX_BYTES,
            section="metadata",
            missing_error=f"{bundle_path.name} has no embedded airflow metadata after its layout",
        )
        layout_line_size = len(EMBEDDED_LAYOUT_MARKER) + len(layout_payload) + 1
        metadata_start = layout_line_size + len(EMBEDDED_METADATA_MARKER)
        metadata_end = metadata_start + len(metadata_payload)
        code_start = metadata_end + 1
        _validate_layout(
            layout,
            metadata_start=metadata_start,
            metadata_end=metadata_end,
            code_start=code_start,
            code_end=stat_result.st_size,
        )
        _verify_integrity(bundle_file, path=bundle_path, layout=layout, initial_stat=stat_result)

    return _decode_metadata(metadata_payload)
