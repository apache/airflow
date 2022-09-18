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
from __future__ import annotations

import re
import sys
from pathlib import Path

import yaml
from rich.console import Console

if __name__ not in ("__main__", "__mp_main__"):
    raise SystemExit(
        "This file is intended to be executed as an executable program. You cannot use it as a module."
        f"To run this script, run the ./{__file__} command [FILE] ..."
    )


console = Console(color_system="standard", width=200)

AIRFLOW_SOURCES_ROOT = Path(__file__).parents[3].resolve()


EXAMPLE_DAGS_URL_MATCHER = re.compile(
    r"^(.*)(https://github.com/apache/airflow/tree/(.*)/airflow/providers/(.*)/example_dags)(/?>.*)$"
)


def get_provider_and_version(url_path: str) -> tuple[str, str]:
    candidate_folders = url_path.split("/")
    while candidate_folders:
        try:
            with open(
                (AIRFLOW_SOURCES_ROOT / "airflow" / "providers").joinpath(*candidate_folders)
                / "provider.yaml"
            ) as f:
                provider_info = yaml.safe_load(f)
            version = provider_info["versions"][0]
            provider = "-".join(candidate_folders)
            while provider.endswith("-"):
                provider = provider[:-1]
            return provider, version
        except FileNotFoundError:
            candidate_folders = candidate_folders[:-1]
    console.print(
        f"[red]Bad example path: {url_path}. Missing "
        f"provider.yaml in any of the 'airflow/providers/{url_path}' folders. [/]"
    )
    sys.exit(1)


def replace_match(file: Path, line: str) -> str | None:
    match = EXAMPLE_DAGS_URL_MATCHER.match(line)
    if match:
        url_path_to_dir = match.group(4)
        folders = url_path_to_dir.split("/")
        example_dags_folder = (AIRFLOW_SOURCES_ROOT / "airflow" / "providers").joinpath(
            *folders
        ) / "example_dags"
        provider, version = get_provider_and_version(url_path_to_dir)
        proper_system_tests_url = (
            f"https://github.com/apache/airflow/tree/providers-{provider}/{version}"
            f"/tests/system/providers/{url_path_to_dir}"
        )
        if not example_dags_folder.exists():
            if proper_system_tests_url in file.read_text():
                console.print(f'[yellow] Removing from {file}[/]\n{line.strip()}')
                return None
            else:
                new_line = re.sub(EXAMPLE_DAGS_URL_MATCHER, r"\1" + proper_system_tests_url + r"\5", line)
                if new_line != line:
                    console.print(f'[yellow] Replacing in {file}[/]\n{line.strip()}\n{new_line.strip()}')
                return new_line
    return line


def find_matches(_file: Path):
    new_lines = []
    lines = _file.read_text().splitlines(keepends=True)
    for index, line in enumerate(lines):
        new_line = replace_match(_file, line)
        if new_line is not None:
            new_lines.append(new_line)
    _file.write_text("".join(new_lines))


if __name__ == '__main__':
    for file in sys.argv[1:]:
        find_matches(Path(file))
