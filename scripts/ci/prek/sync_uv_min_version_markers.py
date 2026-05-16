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
# dependencies = [
#   "rich>=13.6.0",
# ]
# ///
"""Keep ``# sync-uv-min-version`` markers in lockstep with ``[tool.uv] required-version``.

``[tool.uv] required-version`` in the root ``pyproject.toml`` is a deliberate, manual
bump — we intentionally do not wire it into ``upgrade_important_versions.py`` (see the
comment next to ``UV_PATTERNS`` there). But when a contributor does bump it, every
hard-coded uv version in tests that is meant to track that floor needs to move too,
or test fixtures silently drift out of sync.

This hook finds lines of the form::

    foo = "X.Y.Z"  # sync-uv-min-version

and rewrites the ``X.Y.Z`` to match the current ``required-version``. Mark any line
whose string value should equal the current minimum uv version — typically mocked
``_read_required_uv_version`` return values and matching "actual uv" mocks in tests
that exercise the equality/success path.

Exit code ``0`` means nothing changed; exit code ``1`` means the hook updated a file
and the contributor should re-stage it (prek convention).
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

from common_prek_utils import (
    console,
    read_uv_required_min_version,
)

MARKER_RE = re.compile(
    r"""(?P<prefix>["'])(?P<version>\d+(?:\.\d+){0,2})(?P<suffix>["']\s*)#\s*sync-uv-min-version\b""",
)


def sync_file(path: Path, required_version: str) -> bool:
    """Rewrite any ``# sync-uv-min-version``-marked version in ``path`` to ``required_version``.

    Returns ``True`` if the file was modified.
    """
    original = path.read_text()

    def _replace(match: re.Match[str]) -> str:
        if match.group("version") == required_version:
            return match.group(0)
        return f"{match.group('prefix')}{required_version}{match.group('suffix')}# sync-uv-min-version"

    updated = MARKER_RE.sub(_replace, original)
    if updated == original:
        return False
    path.write_text(updated)
    return True


def main(argv: list[str]) -> int:
    files = [Path(arg) for arg in argv[1:]]
    if not files:
        return 0
    try:
        required_version, _ = read_uv_required_min_version()
    except Exception as exc:
        console.print(
            f"[red]ERROR: could not read `[tool.uv] required-version` from pyproject.toml: {exc}[/]"
        )
        return 1
    modified: list[Path] = []
    for file in files:
        if sync_file(file, required_version):
            modified.append(file)
    if modified:
        console.print(
            f"[yellow]Synced `# sync-uv-min-version` markers to uv {required_version} "
            "in the following files:[/]"
        )
        for file in modified:
            console.print(f"  - {file}")
        console.print(
            "[yellow]Please review the changes and re-stage the files.[/]\n"
            "[info]Tip: every line carrying `# sync-uv-min-version` is kept equal to "
            r"`\[tool.uv] required-version` in the root pyproject.toml. Bump that "
            "first, then let this hook propagate the value into the marked test "
            "fixtures.[/]"
        )
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
