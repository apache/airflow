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
# requires-python = ">=3.11,<3.12"
# dependencies = [
#   "rich>=13.6.0",
# ]
# ///
"""
Prek hook to check that all .json files in airflow-core/src/airflow/ui/public/i18n/locales/
are valid JSON and do not contain any 'TODO:' entries.
Also formats JSON files automatically and sorts their keys.
"""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path
from typing import Any

from common_prek_utils import AIRFLOW_ROOT_PATH, console

LOCALES_DIR = AIRFLOW_ROOT_PATH / "airflow-core" / "src" / "airflow" / "ui" / "public" / "i18n" / "locales"


# natural-compare's sort order of ASCII: NUL-"," keep their ASCII order, then "." "/",
# the remaining punctuation, "-" (just before digits), digits, uppercase, lowercase.
# A character's position in this string is its natural-compare sort code.
_NATURAL_CHAR_ORDER = (
    "".join(chr(code) for code in range(32))  # control characters
    + " !\"#$%&'()*+,"
    + "./"
    + ":;<=>?@"
    + "[\\]^_`"
    + "{|}~\x7f"
    + "-"
    + "0123456789"
    + "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    + "abcdefghijklmnopqrstuvwxyz"
)
_CHAR_CODES = {char: code for code, char in enumerate(_NATURAL_CHAR_ORDER)}


def _get_char_code(char: str) -> int:
    """Return natural-compare's sort code for a character; non-ASCII by code point."""
    return _CHAR_CODES.get(char, ord(char))


def natural_sort_key(text: str) -> tuple:
    """Create a sort key matching the ``natural-compare`` npm package.

    This is the order the UI's former ``jsonc/sort-keys`` ESLint rule (``natural: true``) enforced,
    and the same key ``breeze ui`` sorts locale files with (keep the two in sync). natural-compare compares
    digit runs numerically and other characters by ``_get_char_code``. Each token also
    carries a bucket so numbers sort between characters below "0" and above "9".
    """
    tokens: list[tuple] = []
    for part in re.split(r"([0-9]+)", text):
        if part.isascii() and part.isdigit():
            digits = part.lstrip("0")
            # natural-compare treats a leading "0" as a plain character and only the
            # rest of the run as a number.
            tokens.extend([(0, _get_char_code("0"))] * (len(part) - len(digits)))
            if digits:
                tokens.append((1, int(digits)))
        else:
            for char in part:
                code = _get_char_code(char)
                tokens.append((0, code) if code < _CHAR_CODES["0"] else (2, code))
    return tuple(tokens)


def sort_keys_naturally(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: sort_keys_naturally(value[key]) for key in sorted(value, key=natural_sort_key)}
    if isinstance(value, list):
        return [sort_keys_naturally(item) for item in value]
    return value


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
                parsed_json = sort_keys_naturally(json.loads(content))
                formatted_content = json.dumps(parsed_json, indent=2, ensure_ascii=False) + "\n"
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
