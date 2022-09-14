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

import os
import re
from pathlib import Path

import requests
from rich.console import Console
from rich.progress import Progress

if __name__ not in ("__main__", "__mp_main__"):
    raise SystemExit(
        "This file is intended to be executed as an executable program. You cannot use it as a module."
        f"To run this script, run the ./{__file__} command [FILE] ..."
    )


console = Console(color_system="standard", width=200)

AIRFLOW_SOURCES_ROOT = Path(__file__).parents[3].resolve()


EXAMPLE_DAGS_URL_MATCHER = re.compile(
    r"^(.*)(https://github.com/apache/airflow/tree/(.*)/airflow/providers/(.*)/example_dags)(/?\".*)$"
)

SYSTEM_TESTS_URL_MATCHER = re.compile(
    r"^(.*)(https://github.com/apache/airflow/tree/(.*)/tests/system/providers/(.*))(/?\".*)$"
)


def check_if_url_exists(url: str) -> bool:  # type: ignore[return]
    response = requests.head(url)
    if response.status_code == 200:
        return True
    if response.status_code == 404:
        return False
    console.print(f"[red]Unexpected error received: {response.status_code}[/]")
    response.raise_for_status()


def replace_match(file: str, line: str, provider: str, version: str) -> str | None:
    for matcher in [EXAMPLE_DAGS_URL_MATCHER, SYSTEM_TESTS_URL_MATCHER]:
        match = matcher.match(line)
        if match:
            url_path_to_dir = match.group(4)
            branch = match.group(3)
            if branch.startswith("providers-"):
                console.print(f"[green]Already corrected[/]: {provider}:{version}")
                continue
            system_tests_url = (
                f"https://github.com/apache/airflow/tree/providers-{provider}/{version}"
                f"/tests/system/providers/{url_path_to_dir}"
            )
            example_dags_url = (
                f"https://github.com/apache/airflow/tree/providers-{provider}/{version}"
                f"/airflow/providers/{url_path_to_dir}/example_dags"
            )
            if check_if_url_exists(system_tests_url):
                new_line = re.sub(matcher, r"\1" + system_tests_url + r"\5", line)
            elif check_if_url_exists(example_dags_url):
                new_line = re.sub(matcher, r"\1" + example_dags_url + r"\5", line)
            else:
                console.print(
                    f"[yellow] Neither example dags nor system tests folder"
                    f" exists for {provider}:{version} -> removing:[/]"
                )
                console.print(line)
                return None
            if line != new_line:
                console.print(f'[yellow] Replacing in {file}[/]\n{line.strip()}\n{new_line.strip()}')
                return new_line
    return line


def find_matches(_file: Path, provider: str, version: str):
    lines = _file.read_text().splitlines(keepends=True)
    new_lines = []
    for index, line in enumerate(lines):
        new_line = replace_match(str(_file), line, provider, version)
        if new_line:
            new_lines.append(new_line)
    _file.write_text("".join(new_lines))


if __name__ == '__main__':
    curdir = Path(os.curdir).resolve()
    dirs = list(filter(os.path.isdir, curdir.iterdir()))
    with Progress(console=console) as progress:
        task = progress.add_task(f"Updating {len(dirs)}", total=len(dirs))
        for directory in dirs:
            if directory.name.startswith('apache-airflow-providers-'):
                provider = directory.name[len('apache-airflow-providers-') :]
                console.print(f"[bright_blue] Processing {directory}")
                version_dirs = list(filter(os.path.isdir, directory.iterdir()))
                for version_dir in version_dirs:
                    version = version_dir.name
                    console.print(version)
                    for file_name in ["index.html", 'example-dags.html']:
                        candidate_file = version_dir / file_name
                        if candidate_file.exists():
                            find_matches(candidate_file, provider, version)
            progress.advance(task)
