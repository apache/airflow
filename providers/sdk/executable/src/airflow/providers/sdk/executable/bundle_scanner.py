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
Scan directories for native executable Airflow SDK bundles.

A bundle is a single self-contained executable with a fixed-format trailer
appended after the binary.  The last 32 bytes of the file form the trailer
and locate two preceding regions: the embedded DAG source and the
``airflow-metadata.yaml`` manifest.  See :doc:`bundle-spec` for the wire
format.

Detection is by the trailer magic ``AFBNDL01``; files without it are
silently ignored, so non-bundle entries (READMEs, dotfiles, ...) MAY share
the directory.
"""

from __future__ import annotations

import os
import struct
from pathlib import Path
from typing import Any, NamedTuple

import yaml

FOOTER_MAGIC = b"AFBNDL01"
FOOTER_SIZE = 32
FOOTER_VERSION = 1


class _Footer(NamedTuple):
    source_len: int
    metadata_len: int
    footer_ver: int


def _read_footer(path: Path) -> _Footer | None:
    """
    Parse the trailer at the end of *path*.

    :returns: a :class:`_Footer` when the trailer's magic matches and the
        declared regions are within bounds; ``None`` when the file is too
        small or the magic does not match (i.e. it is not a bundle).
    :raises ValueError: when the magic matches but the trailer is otherwise
        malformed (unknown ``footer_ver`` or out-of-bounds region offsets).
    """
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


def read_bundle_metadata(path: Path) -> dict[str, Any] | None:
    """
    Return the parsed ``airflow-metadata.yaml`` manifest embedded in *path*.

    Returns ``None`` when *path* is not a bundle, when the metadata bytes
    are not valid UTF-8 YAML, or when the manifest does not deserialise to
    a mapping.
    """
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


def read_source_code(path: Path) -> str | None:
    """
    Return the embedded DAG source from a bundle, decoded as UTF-8.

    Returns ``None`` when *path* is not a bundle or carries an empty source
    region (``source_len == 0``).
    """
    try:
        footer = _read_footer(path)
    except ValueError:
        return None
    if footer is None or footer.source_len == 0:
        return None
    source_start = path.stat().st_size - FOOTER_SIZE - footer.metadata_len - footer.source_len
    with open(path, "rb") as f:
        f.seek(source_start)
        source_bytes = f.read(footer.source_len)
    try:
        return source_bytes.decode("utf-8")
    except UnicodeDecodeError:
        return None


def _dag_ids(metadata: dict[str, Any]) -> set[str]:
    dags = metadata.get("dags")
    if not isinstance(dags, dict):
        return set()
    return set(dags.keys())


class BundleScanner:
    """
    Locate Airflow native executable bundles inside a directory.

    The scanner enumerates every regular, executable file in *bundles_dir*,
    reads the last 32 bytes of each, and treats files whose magic matches
    ``AFBNDL01`` as bundles.  Non-bundle files are silently ignored.
    """

    def __init__(self, bundles_dir: Path) -> None:
        self._bundles_dir = bundles_dir

    def resolve(self, dag_id: str) -> str:
        """
        Return the executable path of the bundle whose manifest declares *dag_id*.

        :raises FileNotFoundError: if no matching bundle is found.
        """
        for candidate in self._candidate_files():
            metadata = read_bundle_metadata(candidate)
            if metadata is None:
                continue
            if dag_id in _dag_ids(metadata):
                return str(candidate.resolve())

        raise FileNotFoundError(
            f"No executable bundle containing dag_id={dag_id!r} found in {self._bundles_dir}"
        )

    @staticmethod
    def resolve_executable(path: Path) -> str | None:
        """
        Validate that *path* is an Airflow executable bundle.

        Returns the resolved executable path when *path* is a regular,
        executable file whose trailer matches ``AFBNDL01`` and whose
        embedded manifest declares at least one DAG; ``None`` otherwise.
        """
        resolved = path.resolve()
        if not resolved.is_file() or not os.access(resolved, os.X_OK):
            return None
        metadata = read_bundle_metadata(resolved)
        if metadata is None:
            return None
        if not _dag_ids(metadata):
            return None
        return str(resolved)

    def _candidate_files(self) -> list[Path]:
        if not self._bundles_dir.is_dir():
            return []
        return sorted(p for p in self._bundles_dir.iterdir() if p.is_file() and os.access(p, os.X_OK))
