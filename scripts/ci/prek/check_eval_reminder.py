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
"""Remind contributors to run skill-eval when guidance files change.

This hook prints a warning when AGENTS.md or SKILL.md is staged for
commit. It always exits 0 — it never blocks the commit.
"""

from __future__ import annotations

import subprocess
import sys


def get_staged_files() -> list[str]:
    result = subprocess.run(
        ["git", "diff", "--cached", "--name-only"],
        capture_output=True,
        text=True,
        check=False,
    )
    return result.stdout.strip().splitlines()


def find_guidance_files(staged: list[str]) -> list[str]:
    """Return staged paths whose basename is exactly AGENTS.md or SKILL.md."""
    return [f for f in staged if f.rsplit("/", 1)[-1] in ("AGENTS.md", "SKILL.md")]


def main() -> int:
    guidance_files = find_guidance_files(get_staged_files())
    if guidance_files:
        changed = ", ".join(guidance_files)
        print(
            f"\n\033[33m⚠  {changed} modified — "
            f"consider running 'uv run dev/skill-evals/eval.py' before pushing\033[0m\n",
            file=sys.stderr,
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
