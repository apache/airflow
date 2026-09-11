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
# /// script
# requires-python = ">=3.10,<3.11"
# dependencies = []
# ///
"""Keep the documented "tested with" versions in sync with global_constants.py.

The list of Python, database and Kubernetes versions Airflow is tested with lives
in a single source of truth (``dev/breeze/src/airflow_breeze/global_constants.py``)
but is also rendered for humans in two hand-maintained places:

* ``airflow-core/docs/installation/prerequisites.rst`` — the "Prerequisites" bullet list.
* ``README.md`` — the "Main version (dev)" column of the Requirements table.

These used to drift out of sync silently (e.g. PostgreSQL 18 was added to the test
matrix and the README but the prerequisites doc was forgotten). This hook regenerates
both from the constants so the drift becomes a (auto-fixable) prek failure instead of
a documentation bug nobody notices.

Only versions that have a clean single source in global_constants.py are generated:
Python, PostgreSQL, MySQL (numeric releases) and Kubernetes. SQLite has no constant
and the MySQL "Innovation" annotation is editorial, so both are defined here.
"""

from __future__ import annotations

import sys
from pathlib import Path

from common_prek_utils import (
    AIRFLOW_ROOT_PATH,
    read_allowed_kubernetes_versions,
    read_allowed_python_major_minor_versions,
    read_current_mysql_versions,
    read_current_postgres_versions,
)

PREREQUISITES_RST = AIRFLOW_ROOT_PATH / "airflow-core" / "docs" / "installation" / "prerequisites.rst"
README_MD = AIRFLOW_ROOT_PATH / "README.md"

PREREQUISITES_START = " .. Beginning of the auto-generated tested versions\n"
PREREQUISITES_END = " .. End of the auto-generated tested versions\n"

README_DEV_HEADER = "Main version (dev)"

# Editorial values without a single source of truth in global_constants.py.
SQLITE_VERSION = "3.15.0+"
MYSQL_INNOVATION_ANNOTATION = "Innovation"
MYSQL_INNOVATION_RST_LINK = (
    "`Innovation <https://dev.mysql.com/blog-archive/"
    "introducing-mysql-innovation-and-long-term-support-lts-versions>`_"
)


def kubernetes_major_minor_versions() -> list[str]:
    """Kubernetes versions reduced to ``major.minor`` (e.g. ``1.30``) for display."""
    return [".".join(version.split(".")[:2]) for version in read_allowed_kubernetes_versions()]


def comma(values: list[str]) -> str:
    return ", ".join(values)


def render_prerequisites_block() -> str:
    """The bullet list rendered between the prerequisites.rst markers."""
    python_versions = comma(read_allowed_python_major_minor_versions())
    postgres_versions = comma(read_current_postgres_versions())
    mysql_versions = comma([*read_current_mysql_versions(), MYSQL_INNOVATION_RST_LINK])
    kubernetes_versions = comma(kubernetes_major_minor_versions())
    return (
        "\n"
        f"* Python: {python_versions}\n"
        "\n"
        "* Databases:\n"
        "\n"
        f"  * PostgreSQL: {postgres_versions}\n"
        f"  * MySQL: {mysql_versions}\n"
        f"  * SQLite: {SQLITE_VERSION}\n"
        "\n"
        f"* Kubernetes: {kubernetes_versions}\n"
        "\n"
    )


def dev_version_values() -> dict[str, str]:
    """Generated value for each README row label whose dev column we keep in sync."""
    return {
        "Python": comma(read_allowed_python_major_minor_versions()),
        "Kubernetes": comma(kubernetes_major_minor_versions()),
        "PostgreSQL": comma(read_current_postgres_versions()),
        "MySQL": comma([*read_current_mysql_versions(), MYSQL_INNOVATION_ANNOTATION]),
    }


def replace_text_between(text: str, start: str, end: str, replacement: str) -> str:
    if start not in text or end not in text:
        raise RuntimeError(f"Could not find markers {start!r} / {end!r} in the file")
    leading = text.split(start)[0]
    trailing = text.split(end)[1]
    return leading + start + replacement + end + trailing


def update_prerequisites() -> bool:
    """Regenerate the tested-versions block in prerequisites.rst. Return True if changed."""
    original = PREREQUISITES_RST.read_text()
    updated = replace_text_between(
        original, PREREQUISITES_START, PREREQUISITES_END, render_prerequisites_block()
    )
    if updated != original:
        PREREQUISITES_RST.write_text(updated)
        return True
    return False


def update_readme() -> bool:
    """Sync the "Main version (dev)" column of the README Requirements table.

    Only the dev cell of the matched rows is rewritten; the label and the historical
    "Stable version" columns are preserved byte-for-byte. The dev column is widened (and
    only the dev column) if a generated value no longer fits, keeping the table aligned
    without touching the stable columns.
    """
    lines = README_MD.read_text().splitlines(keepends=True)
    header_index = next(
        (i for i, line in enumerate(lines) if README_DEV_HEADER in line and line.lstrip().startswith("|")),
        None,
    )
    if header_index is None:
        raise RuntimeError(
            f"Could not find the Requirements table header ({README_DEV_HEADER!r}) in README.md"
        )

    # The table spans contiguous lines starting with '|', header first then separator.
    table_end = header_index
    while table_end < len(lines) and lines[table_end].lstrip().startswith("|"):
        table_end += 1
    table_rows = list(range(header_index, table_end))
    separator_index = header_index + 1

    generated = dev_version_values()

    def cells(line: str) -> list[str]:
        # Drop the empty fragments before the first and after the last pipe.
        return line.rstrip("\n").split("|")[1:-1]

    # Determine the dev column width: max of the current width and every value we render.
    current_width = len(cells(lines[header_index])[1]) - 2
    needed = [current_width]
    for idx in table_rows:
        if idx == separator_index:
            continue
        row = cells(lines[idx])
        label = row[0].strip()
        value = generated.get(label, row[1].strip())
        needed.append(len(value))
    dev_width = max(needed)

    changed = False
    for idx in table_rows:
        row = cells(lines[idx])
        if idx == separator_index:
            new_dev = "-" * (dev_width + 2)
        else:
            label = row[0].strip()
            value = generated.get(label, row[1].strip())
            new_dev = f" {value.ljust(dev_width)} "
        if new_dev == row[1]:
            continue
        row[1] = new_dev
        lines[idx] = "|" + "|".join(row) + "|\n"
        changed = True

    if changed:
        README_MD.write_text("".join(lines))
    return changed


def main() -> int:
    changed_files: list[Path] = []
    if update_prerequisites():
        changed_files.append(PREREQUISITES_RST)
    if update_readme():
        changed_files.append(README_MD)
    if changed_files:
        for path in changed_files:
            print(f"Updated tested versions in {path.relative_to(AIRFLOW_ROOT_PATH)}")
        print("Files were re-generated from global_constants.py - please re-stage them.")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
