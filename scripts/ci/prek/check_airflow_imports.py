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
# /// script
# requires-python = ">=3.10,<3.11"
# dependencies = [
#   "rich>=13.6.0",
# ]
# ///
from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.resolve()))  # make sure common_prek_utils is imported
from common_prek_utils import console, get_imports_from_file


def parse_args():
    parser = argparse.ArgumentParser(description="Check forbidden import patterns in Python files.")
    parser.add_argument("files", nargs="+", type=Path, help="Python source files to check.")
    parser.add_argument(
        "--pattern",
        required=True,
        help="Regular expression pattern to match forbidden imports (e.g., '^airflow\\.kubernetes').",
    )
    parser.add_argument(
        "--message", required=True, help="Error message to show when a forbidden import is found."
    )
    parser.add_argument(
        "--only_top_level", action="store_true", help="If set, only top-level imports will be analyzed."
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    errors: list[str] = []

    try:
        pattern = re.compile(args.pattern)
    except re.error as e:
        console.print(f"[red]Invalid regex pattern:[/] {e}")
        return 2

    for path in args.files:
        import_count = 0
        local_error_count = 0

        try:
            imports = get_imports_from_file(path, only_top_level=args.only_top_level)
        except Exception as e:
            console.print(f"[red]Failed to parse {path}: {e}[/]")
            return 2

        for imp in imports:
            import_count += 1
            if pattern.search(imp):
                local_error_count += 1
                errors.append(f"{path}: ({imp})")

        console.print(f"[blue]{path}:[/] Import count: {import_count}, error_count {local_error_count}")

    if errors:
        console.print(f"[red]Some files contain forbidden imports matching pattern '{args.pattern}'.[/]")
        console.print(args.message)
        console.print("Error summary:")
        for error in errors:
            console.print(error)
        return 1

    console.print("[green]All good!")
    return 0


if __name__ == "__main__":
    sys.exit(main())
