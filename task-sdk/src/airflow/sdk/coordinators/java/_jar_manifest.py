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
"""Read JAR manifests as the JAR File Specification defines them."""

from __future__ import annotations

import re
from typing import TYPE_CHECKING, Final

if TYPE_CHECKING:
    import zipfile

MANIFEST_NAME: Final = "META-INF/MANIFEST.MF"

_LINE_END = re.compile(rb"\r\n|\r|\n")


def parse_main_attributes(data: bytes) -> dict[str, str]:
    """
    Return the main-section attributes of a JAR manifest, keyed by lower-cased name.

    Attribute names are case-insensitive. Lines end with CRLF, LF or CR, and a line that starts with
    one space continues the previous value. The main section ends at the first blank line.
    """
    headers: list[bytearray] = []
    for line in _LINE_END.split(data):
        if not line:
            break
        if line.startswith(b" "):
            # Lines are folded by bytes, which can split a multi-byte character, so unfold
            # before decoding.
            if headers:
                headers[-1] += line[1:]
            continue
        headers.append(bytearray(line))

    attributes: dict[str, str] = {}
    for header in headers:
        name, sep, value = header.decode("utf-8", errors="replace").partition(":")
        if sep:
            attributes[name.strip().lower()] = value.removeprefix(" ")
    return attributes


def read_main_attributes(zf: zipfile.ZipFile) -> dict[str, str] | None:
    """Return the main attributes of the JAR open in *zf*, or ``None`` when it has no manifest."""
    try:
        info = zf.getinfo(MANIFEST_NAME)
    except KeyError:
        return None
    return parse_main_attributes(zf.read(info))
