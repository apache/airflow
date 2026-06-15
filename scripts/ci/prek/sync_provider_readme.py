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
# requires-python = ">=3.10"
# dependencies = [
#   "packaging>=25",
#   "tabulate>=0.9.0",
#   "tomli>=2.0.1; python_version < '3.11'",
# ]
# ///
"""
Sync the ``Requirements`` table in each provider ``README.rst`` with the
``[project].dependencies`` block of its sibling ``pyproject.toml``.

Provider README files are generated at distribution-build time by
``dev/breeze/src/airflow_breeze/utils/packages.py``'s ``_prepare_readme_file``.
Between releases they are committed in-repo as the rendered output. A
dependency bump in ``pyproject.toml`` does not propagate to the committed
README until the next release build, leading to drift like the one fixed
manually in https://github.com/apache/airflow/pull/67554.

This hook re-renders the ``Requirements`` table directly from the current
``pyproject.toml`` whenever the latter changes, using the same column shape
``convert_pip_requirements_to_table`` produces. Only the table block is
touched — surrounding prose, the cross-provider table, and the rest of the
README are left alone.

Run as a prek hook with the changed ``providers/*/pyproject.toml`` paths as
positional arguments.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

try:
    import tomllib
except ModuleNotFoundError:  # py < 3.11 fallback (CI runs ≥3.10)
    import tomli as tomllib  # type: ignore[no-redef]

from packaging.requirements import Requirement
from tabulate import tabulate

HEADERS = ["PIP package", "Version required"]

# Matches the "Requirements" section + its following RST grid table:
#
#     Requirements
#     ------------
#
#     ============  ==============
#     PIP package   Version required
#     ============  ==============
#     ``foo``       ``>=1.0``
#     ============  ==============
#
# The pattern captures the heading + leading blank line as group(1) so the
# substitution can re-emit it verbatim, then matches the full table block —
# three ``===`` separator lines bracketing the header row and the data rows.
# The MULTILINE+DOTALL flags let the body span multiple lines while keeping
# the boundary anchors tight.
TABLE_PATTERN = re.compile(
    r"(?ms)"
    r"(^Requirements\s*\n-+\s*\n\s*\n)"  # group(1): heading + blank
    r"=+ +=+[^\S\n]*\n"  # opening ===
    r".+?\n"  # header row
    r"=+ +=+[^\S\n]*\n"  # === after header
    r".+?\n"  # data rows (non-greedy)
    r"=+ +=+[^\S\n]*\n"  # closing ===
)


def render_requirements_table(dependencies: list[str]) -> str:
    """Render the same RST table shape that breeze's release-builder produces."""
    rows: list[tuple[str, str]] = []
    for dep in dependencies:
        req = Requirement(dep)
        package = req.name
        if req.extras:
            package += f"[{','.join(sorted(req.extras))}]"
        version_required = ""
        if req.specifier:
            version_required = ",".join(
                str(spec) for spec in sorted(req.specifier, key=lambda spec: spec.version)
            )
        if req.marker:
            version_required += f"; {req.marker}"
        version_required = version_required.strip()
        formatted_version = f"``{version_required}``" if version_required else ""
        rows.append((f"``{package}``", formatted_version))
    return tabulate(rows, headers=HEADERS, tablefmt="rst")


def sync_one(pyproject_path: Path) -> bool:
    """Sync the README.rst next to ``pyproject_path``. Returns True on change."""
    readme_path = pyproject_path.parent / "README.rst"
    if not readme_path.exists():
        return False
    try:
        data = tomllib.loads(pyproject_path.read_text())
    except tomllib.TOMLDecodeError as exc:
        print(f"[sync-provider-readme] could not parse {pyproject_path}: {exc}", file=sys.stderr)
        return False
    dependencies = data.get("project", {}).get("dependencies") or []
    if not dependencies:
        return False
    new_table = render_requirements_table(dependencies)
    original = readme_path.read_text()
    if not TABLE_PATTERN.search(original):
        # README has no Requirements section in the expected shape — leave it
        # alone rather than guess where to inject one. Real-world providers
        # that have dependencies all carry this section; flag for visibility.
        print(
            f"[sync-provider-readme] {readme_path} has no Requirements table — skipping",
            file=sys.stderr,
        )
        return False
    updated = TABLE_PATTERN.sub(rf"\1{new_table}\n", original)
    if updated == original:
        return False
    readme_path.write_text(updated)
    print(f"[sync-provider-readme] updated {readme_path}")
    return True


def main(argv: list[str]) -> int:
    changed = False
    for arg in argv:
        path = Path(arg)
        if path.name != "pyproject.toml" or "providers/" not in path.as_posix():
            continue
        if sync_one(path):
            changed = True
    # prek detects modifications via git diff after the hook runs, so a zero
    # exit code combined with file changes is the standard "files were
    # modified by this hook" failure signal.
    return 0 if not changed else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
