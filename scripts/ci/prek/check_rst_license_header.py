#!/usr/bin/env python
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
Make sure every RST file starts with the license header in its canonical shape.

The ``insert-license`` hook strips whitespace before comparing lines and looks for the header in
the first few lines, so it accepts a leading blank line or a different indentation. This hook
enforces the exact shape the ``insert-license`` hook writes for new files: the header on line 1,
every non-empty line indented by one space.

A header that is only blank lines or indentation away from the canonical shape is rewritten in
place. Any other deviation is reported and left for the author to fix.
"""

from __future__ import annotations

import sys
from pathlib import Path

LICENSE_TEMPLATE = Path(__file__).resolve().parents[1] / "license-templates" / "LICENSE.rst"

# A leading ``:tocdepth:`` field list only counts as document metadata while it is the first
# element, and an indented header ahead of it turns the field list into plain text.
KEEP_FIELD_LIST_FIRST = frozenset(
    {
        "airflow-core/docs/cli-and-env-variables-ref.rst",
        "airflow-ctl/docs/cli-and-env-variables-ref.rst",
    }
)


def get_canonical_header() -> list[str]:
    template = LICENSE_TEMPLATE.read_text().splitlines()
    return [f" {line}" if line.strip() else "" for line in template]


def find_header_start(lines: list[str], canonical: list[str]) -> int | None:
    """Return the index of the first header line when only blank lines precede it."""
    expected = [line.strip() for line in canonical]
    for start in range(len(lines)):
        if [line.strip() for line in lines[start : start + len(canonical)]] == expected:
            return start
        if lines[start].strip():
            return None
    return None


def fix_file(path: Path, canonical: list[str]) -> bool:
    """Rewrite the header into its canonical shape; return whether the file was changed."""
    text = path.read_bytes().decode()
    newline = "\r\n" if "\r\n" in text else "\n"
    lines = text.split(newline)
    if lines[: len(canonical)] == canonical:
        return False
    start = find_header_start(lines, canonical)
    if start is None:
        raise ValueError("the license header must be at the top of the file, before any other content")
    path.write_bytes(newline.join([*canonical, *lines[start + len(canonical) :]]).encode())
    return True


def main(argv: list[str]) -> int:
    canonical = get_canonical_header()
    failed = False
    for name in argv:
        if Path(name).as_posix() in KEEP_FIELD_LIST_FIRST:
            continue
        try:
            if fix_file(Path(name), canonical):
                print(f"Fixed the license header shape of {name}")
                failed = True
        except ValueError as e:
            print(f"{name}: {e}")
            failed = True
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
