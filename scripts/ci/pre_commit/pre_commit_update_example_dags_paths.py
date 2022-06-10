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
import re
import sys
from pathlib import Path
from typing import Tuple

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

SYSTEM_TESTS_URL_MATCHER = re.compile(
    r"^(.*)(https://github.com/apache/airflow/tree/(.*)/tests/system/providers/(.*))(/?>.*)$"
)


def get_provider_and_version(url_path: str) -> Tuple[str, str]:
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


def replace_match(file: str, line: str) -> str:
    for matcher in [EXAMPLE_DAGS_URL_MATCHER, SYSTEM_TESTS_URL_MATCHER]:
        match = matcher.match(line)
        if match:
            new_line = line
            url_path_to_dir = match.group(4)
            folders = url_path_to_dir.split("/")
            example_dags_folder = (AIRFLOW_SOURCES_ROOT / "airflow" / "providers").joinpath(
                *folders
            ) / "example_dags"
            system_tests_folder = (AIRFLOW_SOURCES_ROOT / "tests" / "system" / "providers").joinpath(*folders)
            provider, version = get_provider_and_version(url_path_to_dir)
            if system_tests_folder.exists():
                proper_system_tests_url = (
                    f"https://github.com/apache/airflow/tree/providers-{provider}/{version}"
                    f"/tests/system/providers/{url_path_to_dir}"
                )
                new_line = re.sub(matcher, r"\1" + proper_system_tests_url + r"\5", line)
            elif example_dags_folder.exists():
                proper_example_dags_url = (
                    f"https://github.com/apache/airflow/tree/providers-{provider}/{version}"
                    f"/airflow/providers/{url_path_to_dir}/example_dags"
                )
                new_line = re.sub(matcher, r"\1" + proper_example_dags_url + r"\5", line)
            else:
                console.print(
                    f"[red] Error - neither example dags nor system tests folder exists for {provider}[/]"
                )
            if line != new_line:
                console.print(f'[yellow] Replacing in {file}[/]\n{line.strip()}\n{new_line.strip()}')
                return new_line
    return line


def find_matches(_file: Path):
    lines = _file.read_text().splitlines(keepends=True)
    for index, line in enumerate(lines):
        lines[index] = replace_match(str(_file), line)
    _file.write_text("".join(lines))


if __name__ == '__main__':
    for file in sys.argv[1:]:
        find_matches(Path(file))
