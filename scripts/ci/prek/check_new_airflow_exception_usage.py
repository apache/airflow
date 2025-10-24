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
#   "rich>=13.0.0",
# ]
# ///

from __future__ import annotations

import subprocess
import sys
from collections.abc import Sequence
from pathlib import Path

from rich.console import Console
from rich.panel import Panel
from rich.text import Text

console = Console()


def main(argv: Sequence[str]) -> int:
    """Check only the staged diff hunks for 'raise AirflowException'."""
    if not argv:
        console.print(
            Panel.fit(
                "[bold yellow]No files passed to hook[/bold yellow]\n"
                "This hook should be run by pre-commit/prek with staged files.",
                title="⚠️ Warning",
                border_style="yellow",
            )
        )
        return 0

    bad_files: list[Path] = []
    for filename in argv:
        path: Path = Path(filename)
        if not path.exists() or path.suffix != ".py":
            continue

        try:
            diff_output: str = subprocess.check_output(
                ["git", "diff", "--cached", "-U0", "--", str(path)],
                text=True,
            )
        except subprocess.CalledProcessError as e:
            console.print(f"[bold red]⚠️ Failed to get diff for[/bold red] [cyan]{path}[/cyan]: {e}")
            continue

        for line in diff_output.splitlines():
            # Only check added lines, skip diff headers
            if line.startswith("+") and not line.startswith("+++"):
                if "raise AirflowException" in line:
                    bad_files.append(path)

    if bad_files:
        console.print(
            Panel.fit(
                Text(
                    "Found 'raise AirflowException' in staged changes:",
                    style="bold red",
                ),
                title="❌ Commit blocked",
                border_style="red",
            )
        )
        for bad_file in bad_files:
            console.print(f" • [cyan]{bad_file}[/cyan]")
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
