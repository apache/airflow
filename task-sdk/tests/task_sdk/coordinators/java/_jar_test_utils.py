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

import pathlib
import zipfile

SCHEMA_VERSION = "2026-06-16"


def write_manifest(attributes: dict[str, str], *, newline: bytes = b"\r\n") -> bytes:
    """Write *attributes* as a manifest main section, folded at 72 bytes as ``java.util.jar.Manifest`` does."""
    out = bytearray()
    for name, value in attributes.items():
        line = f"{name}: {value}".encode()
        out += line[:72]
        for start in range(72, len(line), 71):
            out += newline + b" " + line[start : start + 71]
        out += newline
    return bytes(out + newline)


def make_jar(
    path: pathlib.Path,
    *,
    attributes: dict[str, str] | None = None,
    entries: dict[str, bytes | str] | None = None,
    manifest: bytes | None = None,
) -> pathlib.Path:
    """Write a JAR whose manifest holds *attributes* (or the raw *manifest*), plus *entries*."""
    if manifest is None and attributes is not None:
        manifest = write_manifest(attributes)
    with zipfile.ZipFile(path, "w") as zf:
        if manifest is not None:
            zf.writestr("META-INF/MANIFEST.MF", manifest)
        for name, data in (entries or {}).items():
            zf.writestr(name, data)
    return path
