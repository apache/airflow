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
# requires-python = ">=3.10, <3.11"
# dependencies = []
# ///
"""
Sync NOTICE files from the canonical source.

``scripts/ci/license-templates/NOTICE`` is the canonical ASF header shared by
all standard distributions.  This script:

- Overwrites every standard NOTICE that diverges from the canonical source.
- For NOTICE files that contain a ``=======================================================================``
  separator (indicating vendored-in third-party content), the custom sections
  are preserved and only the copyright year is updated.

No hardcoded list of special distributions is needed — the presence of the
separator line is sufficient to detect custom content.  To add custom sections
to a NOTICE, insert the separator line after the standard ASF block.

Run once whenever the canonical source changes (manual stage):

    prek run sync-notice-files --all-files
"""

from __future__ import annotations

import re
import sys
from datetime import datetime
from pathlib import Path

EXCLUDE_DIRS = {"node_modules", ".git", ".venv", "__pycache__", ".build"}
CUSTOM_SEPARATOR = "=======================================================================\n"
CURRENT_YEAR = str(datetime.now().year)
COPYRIGHT_RE = re.compile(r"(Copyright 2016-)(\d{4})( The Apache Software Foundation)")


def sync_file(path: Path, canonical: str, repo_root: Path) -> bool:
    """Return True if the file was modified."""
    rel = path.relative_to(repo_root).as_posix()
    content = path.read_text()

    if CUSTOM_SEPARATOR in content:
        # Has custom sections — only update the copyright year, preserve everything else.
        match = COPYRIGHT_RE.search(content)
        if not match or match.group(2) == CURRENT_YEAR:
            return False
        new_content = COPYRIGHT_RE.sub(rf"\g<1>{CURRENT_YEAR}\3", content)
        path.write_text(new_content)
        print(f"✅  {rel}: updated year to {CURRENT_YEAR}")
        return True

    # Standard NOTICE — must match the canonical source exactly.
    if content == canonical:
        return False
    path.write_text(canonical)
    print(f"✅  {rel}: synced to canonical")
    return True


def main() -> int:
    repo_root = Path(__file__).parents[3]
    canonical_path = repo_root / "scripts" / "ci" / "license-templates" / "NOTICE"
    canonical = canonical_path.read_text()

    notice_files = sorted(
        f
        for f in repo_root.rglob("NOTICE")
        if not any(part in EXCLUDE_DIRS for part in f.parts) and f != canonical_path
    )

    updated = sum(sync_file(f, canonical, repo_root) for f in notice_files)
    print(f"\n{updated} file(s) updated, {len(notice_files) - updated} already in sync.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
