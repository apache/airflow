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
from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.resolve()))
from common_precommit_utils import console


def check_file_for_sdk_imports(file_path: Path) -> list[tuple[int, str]]:
    """Check file for airflow.sdk imports. Returns list of (line_num, line_content)."""

    try:
        with open(file_path, encoding="utf-8") as f:
            content = f.read()
    except (OSError, UnicodeDecodeError):
        return []

    if "airflow.sdk" not in content:
        return []

    match = []
    for line_num, line in enumerate(content.splitlines(), 1):
        line = line.strip()
        if not line or line.startswith("#"):
            continue

        if re.search(r"(?:from\s+|import\s+)airflow\.sdk", line):
            match.append((line_num, line))

    return match


def main():
    parser = argparse.ArgumentParser(description="Check for task SDK imports in airflow-core files")
    parser.add_argument("files", nargs="*", help="Files to check")
    args = parser.parse_args()

    if not args.files:
        return

    files_to_check = [Path(f) for f in args.files if f.endswith(".py")]
    mismatches = 0

    for file_path in files_to_check:
        violations = check_file_for_sdk_imports(file_path)
        if violations:
            console.print(f"[red]{file_path}[/red]:")
            for line_num, line in violations:
                console.print(f"  [yellow]Line {line_num}[/yellow]: {line}")
            mismatches += len(violations)

    if mismatches:
        console.print()
        console.print(f"[red] Found {mismatches} SDK import(s) in core files[/red]")
        sys.exit(1)


if __name__ == "__main__":
    main()
    sys.exit(0)
