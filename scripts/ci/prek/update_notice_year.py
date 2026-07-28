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
Update the end year in NOTICE files to the current year.

Replaces ``Copyright 2016-YYYY The Apache Software Foundation`` with the
current year.  Files whose copyright line already carries the current year
are left untouched.  Files that don't contain the standard ASF copyright
line at all are skipped with a warning so that non-standard NOTICE files
(e.g. the fab provider) are never silently corrupted.

Run once a year (manual stage):

    prek run update-notice-year --all-files
"""

from __future__ import annotations

import re
import sys
from datetime import datetime
from pathlib import Path

CURRENT_YEAR = str(datetime.now().year)
COPYRIGHT_RE = re.compile(r"(Copyright 2016-)(\d{4})( The Apache Software Foundation)")


def update_file(path: Path) -> bool:
    """Return True if the file was modified."""
    content = path.read_text()
    match = COPYRIGHT_RE.search(content)
    if not match:
        print(f"{path}: no standard ASF copyright line found, skipping")
        return False
    if match.group(2) == CURRENT_YEAR:
        return False
    new_content = COPYRIGHT_RE.sub(rf"\g<1>{CURRENT_YEAR}\3", content)
    path.write_text(new_content)
    print(f"{path}: updated {match.group(2)} → {CURRENT_YEAR}")
    return True


EXCLUDE_DIRS = {"node_modules", ".git", ".venv", "__pycache__"}


def main() -> int:
    repo_root = Path(__file__).parents[3]
    notice_files = [f for f in repo_root.rglob("NOTICE") if not any(part in EXCLUDE_DIRS for part in f.parts)]
    updated = sum(update_file(f) for f in notice_files)
    print(f"\n{updated} file(s) updated, {len(notice_files) - updated} already current.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
