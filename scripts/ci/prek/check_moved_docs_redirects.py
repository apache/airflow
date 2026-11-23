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
from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path


def get_docs_root(path: Path) -> Path | None:
    parts = path.parts
    if path.suffix != ".rst" or len(parts) < 2:
        return None
    if parts[0] in {"airflow-core", "airflow-ctl", "chart"} and parts[1] == "docs":
        return Path(*parts[:2])
    if parts[0] in {"docker-stack-docs", "providers-summary-docs"}:
        return Path(parts[0])
    if parts[0] == "providers" and "docs" in parts[2:-1]:
        return Path(*parts[: parts.index("docs", 2) + 1])
    return None


def main() -> int:
    from_ref = os.environ.get("PRE_COMMIT_FROM_REF")
    to_ref = os.environ.get("PRE_COMMIT_TO_REF", "HEAD")
    diff_range = [f"{from_ref}...{to_ref}"] if from_ref else ["--cached"]
    try:
        root = subprocess.run(
            ["git", "rev-parse", "--show-toplevel"], check=True, capture_output=True, text=True
        ).stdout.strip()
        changes = subprocess.run(
            ["git", "diff", "--name-status", "-z", "-M", "--diff-filter=R", *diff_range],
            cwd=root,
            check=True,
            capture_output=True,
            text=True,
        ).stdout.split("\0")
    except (OSError, subprocess.CalledProcessError) as error:
        print(f"Could not inspect git renames: {error}", file=sys.stderr)
        return 1

    missing = False
    for old_name, new_name in zip(changes[1::3], changes[2::3]):
        old_path = Path(old_name)
        docs_root = get_docs_root(old_path)
        if docs_root is None:
            continue
        redirects = docs_root / "redirects.txt"
        result = subprocess.run(
            ["git", "show", f"{to_ref if from_ref else ''}:{redirects.as_posix()}"],
            cwd=root,
            capture_output=True,
            text=True,
            check=False,
        )
        old_url = old_path.relative_to(docs_root).as_posix()
        entries = (line.partition(" ") for line in result.stdout.splitlines() if not line.startswith("#"))
        if not any(
            source == old_url and target.strip() for source, separator, target in entries if separator
        ):
            print(f"Missing redirect for {old_name} -> {new_name} in {redirects} (source: {old_url})")
            missing = True
    return int(missing)


if __name__ == "__main__":
    sys.exit(main())
