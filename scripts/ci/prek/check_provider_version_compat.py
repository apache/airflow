#!/usr/bin/env python
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

import re
import sys
from pathlib import Path

IMPORT_RE = re.compile(r"^from airflow\.providers\.([a-zA-Z0-9_]+)\.version_compat import (.+)$")


def get_provider_from_path(path: Path):
    """Extract provider name from file path."""
    try:
        parts = path.parts
        idx = parts.index("providers")
        return parts[idx + 1]
    except (ValueError, IndexError):
        return None


def check_and_fix_file(path: Path):
    provider = get_provider_from_path(path)

    changed = False
    lines = path.read_text().splitlines()
    new_lines = []
    for line in lines:
        m = IMPORT_RE.match(line.strip())
        if m:
            imported_provider, rest = m.groups()
            if imported_provider != provider:
                print(
                    f"prek-hook: {path}: Import from wrong provider: {imported_provider} (should be {provider})"
                )
                # auto fix: rewrite the import correctly
                new_lines.append(f"from airflow.providers.{provider}.version_compat import {rest}")
                changed = True
            else:
                new_lines.append(line)
        else:
            new_lines.append(line)
    if changed:
        path.write_text("\n".join(new_lines) + "\n")
    return not changed


def main():
    failed = False
    for filename in sys.argv[1:]:
        path = Path(filename)
        if path.suffix == ".py":
            if not check_and_fix_file(path):
                failed = True
    if failed:
        print("prek-hook: Fixed some imports. Please re-add and commit.")
        sys.exit(1)


if __name__ == "__main__":
    main()
