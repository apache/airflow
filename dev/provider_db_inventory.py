#!/usr/bin/env python3
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
# /// script
# requires-python = ">=3.10"
# dependencies = [
#   "rich>=13.6.0",
#   "pyyaml>=6.0.3",
# ]
# ///
from __future__ import annotations

import os
import re
from collections import defaultdict
from pathlib import Path

import rich
import yaml

AIRFLOW_SOURCES_PATH = Path(__file__).parents[1]

# Directories to scan
PYPROJECT_TOML_FILES = AIRFLOW_SOURCES_PATH.rglob("providers/**/pyproject.toml")

# Patterns to identify Airflow metadata DB access
DB_PATTERNS: list[tuple[re.Pattern, re.Pattern | None]] = [
    (re.compile(r"from airflow\.utils\.session"), None),
    (re.compile(r"from airflow\.settings import Session"), None),
    (re.compile(r"@provide_session"), None),
    (re.compile(r"from sqlalchemy\.orm\.session"), None),
    (re.compile(r"session\.query"), None),
]

AFFECTED_PROVIDERS: dict[str, list[Path]] = defaultdict(list)
MATCHES: dict[Path, list[str]] = defaultdict(list)


def line_matches_pattern(line: str, patterns: list[tuple[re.Pattern, re.Pattern | None]]) -> bool:
    """Check if a line matches any metadata DB access pattern."""
    return any(
        pattern.search(line) and not (exclude_pattern and exclude_pattern.search(line))
        for pattern, exclude_pattern in patterns
    )


def any_line_matches_pattern(filepath: Path) -> bool:
    """Scan a single file for metadata DB access patterns."""
    lines = filepath.read_text().splitlines()
    matches = False
    for i, line in enumerate(lines, start=1):
        if line_matches_pattern(line, DB_PATTERNS):
            rich.print(f"[bright_blue]Match found[/] in {filepath} -> #{i}:{line}")
            MATCHES[filepath].append(
                f"[Line:{i}](https://github.com/apache/airflow/blob/main/{filepath}#L{i}): {line} "
            )
            matches = True
    return matches


def scan_directory(directory):
    provider_name = yaml.safe_load((directory / "provider.yaml").read_text())["package-name"]
    for path in (directory / "src").rglob("*.py"):
        rel_path = path.relative_to(AIRFLOW_SOURCES_PATH)
        if any_line_matches_pattern(rel_path):
            rich.print(f"[green]Found metadata DB access in {path}[/]")
            AFFECTED_PROVIDERS[provider_name].append(rel_path)


def main():
    for pyproject_toml in PYPROJECT_TOML_FILES:
        directory = pyproject_toml.parent
        if os.path.exists(directory):
            rich.print(f"Scanning src folder of {directory}...")
            scan_directory(directory)
    print()
    print(f"Found {len(AFFECTED_PROVIDERS)} providers with metadata DB access patterns:")
    print()
    for provider in sorted(AFFECTED_PROVIDERS):
        print(f"## Provider: {provider}\n")
        for file in AFFECTED_PROVIDERS[provider]:
            print(f" - [ ] [{file.name}](https://github.com/apache/airflow/blob/main/{file})")
            for match in MATCHES[file]:
                print(f"    - {match}")
        print()


if __name__ == "__main__":
    main()
