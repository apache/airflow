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
# requires-python = ">=3.10,<3.11"
# dependencies = [
#   "rich>=13.6.0",
# ]
# ///
"""
Prek hook to check that all .json files in airflow-core/src/airflow/ui/public/i18n/locales/
are valid JSON and do not contain any 'TODO:' entries.
Also formats JSON files automatically.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

COMMON_PREK_PATH = Path(__file__).parent.resolve()

sys.path.insert(0, COMMON_PREK_PATH.as_posix())  # make sure common_prek_utils is imported
from common_prek_utils import AIRFLOW_ROOT_PATH, console

LOCALES_DIR = AIRFLOW_ROOT_PATH / "airflow-core" / "src" / "airflow" / "ui" / "public" / "i18n" / "locales"


def main():
    failed = False
    for json_file in LOCALES_DIR.rglob("*.json"):
        console.print(f"[bright_blue]Checking {json_file.relative_to(LOCALES_DIR)}[/]")
        rel_path = json_file.relative_to(Path.cwd()) if Path.cwd() in json_file.parents else json_file
        try:
            content = json_file.read_text(encoding="utf-8")
            if "TODO:" in content:
                console.print(f"[bold red][FAIL][/bold red] 'TODO:' found in [yellow]{rel_path}[/yellow]")
                failed = True
            try:
                parsed_json = json.loads(content)
                formatted_content = (
                    json.dumps(parsed_json, indent=2, ensure_ascii=False, sort_keys=False) + "\n"
                )
                if content != formatted_content:
                    json_file.write_text(formatted_content, encoding="utf-8")
                    console.print(f"[bold green][FORMATTED][/bold green] [yellow]{rel_path}[/yellow]")
            except Exception as e:
                console.print(
                    f"[bold red][FAIL][/bold red] Invalid JSON in [yellow]{rel_path}[/yellow]: [red]{e}[/red]"
                )
                failed = True
        except Exception as e:
            console.print(
                f"[bold red][FAIL][/bold red] Could not read [yellow]{rel_path}[/yellow]: [red]{e}[/red]"
            )
            failed = True
    if failed:
        console.print(
            "\n[bold red][ERROR][/bold red] Some JSON files are invalid or contain 'TODO:'. Commit aborted."
        )
        sys.exit(1)
    console.print(
        "[bold green][OK][/bold green] All JSON files are valid, do not contain 'TODO:', and are properly formatted."
    )
    sys.exit(0)


if __name__ == "__main__":
    main()
