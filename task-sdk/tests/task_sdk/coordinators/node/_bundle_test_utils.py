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

import base64
import hashlib
import json
import pathlib

SCHEMA_VERSION = "2026-06-16"
LAYOUT_PREFIX = b"//# airflowBundle="
METADATA_PREFIX = b"//# airflowMetadata="
OFFSET_WIDTH = 16


def metadata_json(
    *dag_ids: str,
    schema_version: str = SCHEMA_VERSION,
    metadata_version: str | None = "1.0",
) -> bytes:
    metadata = {
        "sdk": {
            "language": "typescript",
            "version": "0.1.0",
            "supervisor_schema_version": schema_version,
        },
        "source": "main.ts",
        "dags": {dag_id: {"tasks": ["test_task"]} for dag_id in dag_ids},
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


def _layout_line(layout: dict[str, object]) -> bytes:
    payload = json.dumps(layout, separators=(",", ":")).encode("ascii")
    return LAYOUT_PREFIX + base64.b64encode(payload) + b"\n"


def write_bundle(
    root: pathlib.Path,
    *dag_ids: str,
    code: bytes = b"export {};\n",
    schema_version: str = SCHEMA_VERSION,
    metadata_version: str | None = "1.0",
    metadata_payload: bytes | None = None,
) -> pathlib.Path:
    if metadata_payload is None:
        metadata_payload = metadata_json(
            *dag_ids,
            schema_version=schema_version,
            metadata_version=metadata_version,
        )
    metadata_line = METADATA_PREFIX + metadata_payload + b"\n"
    placeholder = _layout_line(
        {
            "code": _section(0, 0, code),
            "metadata": _section(0, 0, metadata_payload),
        }
    )
    metadata_start = len(placeholder) + len(METADATA_PREFIX)
    metadata_end = metadata_start + len(metadata_payload)
    code_start = len(placeholder) + len(metadata_line)
    layout_line = _layout_line(
        {
            "code": _section(code_start, code_start + len(code), code),
            "metadata": _section(metadata_start, metadata_end, metadata_payload),
        }
    )
    assert len(layout_line) == len(placeholder)

    bundle = root / "bundle.mjs"
    bundle.write_bytes(layout_line + metadata_line + code)
    return bundle


def read_layout(bundle: pathlib.Path) -> dict[str, object]:
    line = bundle.read_bytes().splitlines(keepends=True)[0]
    return json.loads(base64.b64decode(line[len(LAYOUT_PREFIX) :].strip(), validate=True))


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
