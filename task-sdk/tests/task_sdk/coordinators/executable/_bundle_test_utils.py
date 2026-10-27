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
from __future__ import annotations

import hashlib
import stat
import struct
from typing import TYPE_CHECKING, Any

import yaml

if TYPE_CHECKING:
    from pathlib import Path

SCHEMA_VERSION = "2026-06-16"
FOOTER_MAGIC = b"AFBNDL01"
BINARY_PAYLOAD = b"\x7fELF" + b"binary-stub-payload"
ENTRYPOINT_PATH = "example/bundle/main.go"
DEFAULT_SOURCES = {ENTRYPOINT_PATH: b"package main\n\nfunc main() {}\n"}


def write_bundle(
    path: Path,
    *dag_ids: str,
    sources: dict[str, bytes] | None = None,
    dag_source_paths: dict[str, str] | None = None,
    entrypoint_path: str | None = ENTRYPOINT_PATH,
    language: str | None = "go",
    schema_version: str | None = SCHEMA_VERSION,
    index: list[dict[str, Any]] | None = None,
    omit_sources: bool = False,
    source_region: bytes | None = None,
    binary: bytes = BINARY_PAYLOAD,
) -> Path:
    """
    Write a synthetic executable bundle to *path* and return it.

    *sources* are embedded back to back, indexed from their real offsets and digests. Pass *index* to
    replace the ``sources`` entries and *source_region* to replace the embedded bytes, to build an
    invalid bundle. Without *sources*, the default entrypoint is embedded. *omit_sources* leaves the
    ``sources`` key out.
    """
    embedded = DEFAULT_SOURCES if sources is None else sources
    region = b""
    entries = []
    for source_path, content in embedded.items():
        entries.append(
            {
                "path": source_path,
                "offset": len(region),
                "length": len(content),
                "sha256": hashlib.sha256(content).hexdigest(),
            }
        )
        region += content
    if source_region is not None:
        region = source_region

    sdk: dict[str, str] = {"version": "0.1.0"}
    if language is not None:
        sdk["language"] = language
    if schema_version is not None:
        sdk["supervisor_schema_version"] = schema_version
    metadata: dict[str, Any] = {"airflow_bundle_metadata_version": "1.0", "sdk": sdk}
    if entrypoint_path is not None:
        metadata["entrypoint_path"] = entrypoint_path
    metadata["dag_source_paths"] = (
        {dag_id: ENTRYPOINT_PATH for dag_id in dag_ids} if dag_source_paths is None else dag_source_paths
    )
    if not omit_sources:
        metadata["sources"] = entries if index is None else index
    metadata["dags"] = {dag_id: {"tasks": ["task1"]} for dag_id in dag_ids}
    metadata_bytes = yaml.safe_dump(metadata, sort_keys=True).encode("utf-8")

    trailer = (
        struct.pack("<III", len(region), len(metadata_bytes), 1)
        + hashlib.sha256(binary).digest()
        + bytes(12)
        + FOOTER_MAGIC
    )
    path.write_bytes(binary + region + metadata_bytes + trailer)
    path.chmod(path.stat().st_mode | stat.S_IEXEC | stat.S_IXGRP | stat.S_IXOTH)
    return path
