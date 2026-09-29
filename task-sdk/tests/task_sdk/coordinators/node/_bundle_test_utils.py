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
import json
import pathlib
import re

SCHEMA_VERSION = "2026-06-16"
BUNDLE_NAME = "bundle.min.mjs"
LAYOUT_PREFIX = b"//# airflowBundle="
METADATA_PREFIX = b"//# airflowMetadata="
SOURCE_OPEN_PREFIX = b"/*# airflowSource:"
SOURCE_CLOSE = b"\n#*/\n"
OFFSET_WIDTH = 16
DEFAULT_SOURCE_PATH = "main.ts"
# The full opening marker for the default single-region bundle these helpers build.
SOURCE_OPEN = SOURCE_OPEN_PREFIX + DEFAULT_SOURCE_PATH.encode() + b"\n"


def metadata_json(
    *dag_ids: str,
    schema_version: str = SCHEMA_VERSION,
    metadata_version: str | None = "1.0",
    dag_source_paths: dict[str, str] | None = None,
) -> bytes:
    if dag_source_paths is None:
        dag_source_paths = {dag_id: DEFAULT_SOURCE_PATH for dag_id in dag_ids}
    metadata = {
        "sdk": {
            "language": "typescript",
            "version": "0.1.0",
            "supervisor_schema_version": schema_version,
        },
        "dag_source_paths": dag_source_paths,
        "task_handlers": {dag_id: {"tasks": ["test_task"]} for dag_id in dag_ids},
    }
    if metadata_version is not None:
        metadata = {"airflow_bundle_metadata_version": metadata_version, **metadata}
    return json.dumps(metadata, separators=(",", ":"), ensure_ascii=False).encode()


def _section(start: int, end: int, payload: bytes) -> dict[str, str]:
    return {
        "start": f"{start:0{OFFSET_WIDTH}x}",
        "end": f"{end:0{OFFSET_WIDTH}x}",
        "sha256": hashlib.sha256(payload).hexdigest(),
    }


def _source_section(path: str, start: int, end: int, payload: bytes) -> dict[str, str]:
    return {"path": path, **_section(start, end, payload)}


def _layout_line(layout: dict[str, object]) -> bytes:
    payload = json.dumps(layout, separators=(",", ":")).encode("ascii")
    return LAYOUT_PREFIX + payload + b"\n"


def escape_source(source: bytes) -> bytes:
    """Escape a block-comment terminator the way the TypeScript encoder does."""
    return re.sub(rb"\*([\\/])", rb"*\\\1", source)


def write_bundle(
    root: pathlib.Path,
    *dag_ids: str,
    code: bytes = b"export {};\n",
    schema_version: str = SCHEMA_VERSION,
    metadata_version: str | None = "1.0",
    metadata_payload: bytes | None = None,
    source: bytes = b"export {};\n",
    source_payload: bytes | None = None,
    sources: list[tuple[str, bytes]] | None = None,
    dag_source_paths: dict[str, str] | None = None,
    name: str = BUNDLE_NAME,
) -> pathlib.Path:
    # ``sources`` gives raw content per path for multi-file or mixed-language bundles; the default is
    # a single region at ``DEFAULT_SOURCE_PATH`` driven by ``source`` / ``source_payload``.
    if sources is None:
        payload = source_payload if source_payload is not None else escape_source(source)
        regions = [(DEFAULT_SOURCE_PATH, payload)]
    else:
        regions = [(path, escape_source(content)) for path, content in sources]
    if metadata_payload is None:
        metadata_payload = metadata_json(
            *dag_ids,
            schema_version=schema_version,
            metadata_version=metadata_version,
            dag_source_paths=dag_source_paths,
        )
    metadata_line = METADATA_PREFIX + metadata_payload + b"\n"
    placeholder = _layout_line(
        {
            "code": _section(0, 0, code),
            "metadata": _section(0, 0, metadata_payload),
            "sources": [_source_section(path, 0, 0, payload) for path, payload in regions],
        }
    )
    metadata_start = len(placeholder) + len(METADATA_PREFIX)
    metadata_end = metadata_start + len(metadata_payload)

    source_entries: list[dict[str, str]] = []
    source_region = b""
    cursor = len(placeholder) + len(metadata_line)
    for path, payload in regions:
        open_marker = SOURCE_OPEN_PREFIX + path.encode("utf-8") + b"\n"
        start = cursor + len(open_marker)
        end = start + len(payload)
        source_entries.append(_source_section(path, start, end, payload))
        source_region += open_marker + payload + SOURCE_CLOSE
        cursor = end + len(SOURCE_CLOSE)
    code_start = cursor
    layout_line = _layout_line(
        {
            "code": _section(code_start, code_start + len(code), code),
            "metadata": _section(metadata_start, metadata_end, metadata_payload),
            "sources": source_entries,
        }
    )
    assert len(layout_line) == len(placeholder)

    bundle = root / name
    bundle.parent.mkdir(parents=True, exist_ok=True)
    bundle.write_bytes(layout_line + metadata_line + source_region + code)
    return bundle


def read_layout(bundle: pathlib.Path) -> dict[str, object]:
    line = bundle.read_bytes().splitlines(keepends=True)[0]
    return json.loads(line[len(LAYOUT_PREFIX) :].strip())


def rewrite_layout(bundle: pathlib.Path, layout: dict[str, object]) -> None:
    contents = bundle.read_bytes()
    _, separator, remainder = contents.partition(b"\n")
    assert separator
    replacement = _layout_line(layout)
    assert len(replacement) == len(contents) - len(remainder)
    bundle.write_bytes(replacement + remainder)


def replace_layout_payload(bundle: pathlib.Path, payload: bytes) -> None:
    contents = bundle.read_bytes()
    _, separator, remainder = contents.partition(b"\n")
    assert separator
    bundle.write_bytes(LAYOUT_PREFIX + payload + b"\n" + remainder)


def mutate_byte(bundle: pathlib.Path, offset: int) -> None:
    contents = bytearray(bundle.read_bytes())
    contents[offset] = ord("A") if contents[offset] != ord("A") else ord("B")
    bundle.write_bytes(contents)
