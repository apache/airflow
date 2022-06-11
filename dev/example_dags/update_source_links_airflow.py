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
import os
import re
from pathlib import Path
from time import sleep
from typing import Optional, Set

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


EXAMPLE_DAGS_SOURCES_LINK_MATCHER = re.compile(
    r'^(.*class="example-title">)(.*)(</span>'
    r'<a class="example-header-button viewcode-button reference internal" href=")(.*)("><span.*)$'
)

valid_urls: Set[str] = set()


def check_if_url_exists(url: str) -> bool:  # type: ignore[return]
    while True:
        if url in valid_urls:
            return True
        response = requests.head(url, allow_redirects=True)
        if response.status_code == 200:
            valid_urls.add(url)
            return True
        if response.status_code == 404:
            return False
        if response.status_code == 429:
            console.print("\n\n\n[yellow]Throttled. Waiting for 30 seconds[/]\n\n\n")
            sleep(30)
            continue
        console.print(f"[red]Unexpected error received: {response.status_code}[/]")
        response.raise_for_status()


def replace_match(directory: str, file: str, line: str, version: str) -> Optional[str]:
    for matcher in [EXAMPLE_DAGS_SOURCES_LINK_MATCHER]:
        match = matcher.match(line)
        if match:
            source_file_path = match.group(2)
            href = match.group(4)
            if href.startswith("https://"):
                console.print(f"[green]Already replaced with URL:[/] {source_file_path}")
                continue
            github_url = f"https://github.com/apache/airflow/tree/{version}/{source_file_path}"
            if href.startswith("..") and os.path.exists(os.path.join(directory, href)):
                console.print(f"[green]The example source exists:[/] {href}")
                continue
            if check_if_url_exists(github_url):
                new_line = re.sub(matcher, r"\1\2\3" + github_url + '" target="_blank' + r"\5", line)
            else:
                console.print(f"[yellow] Missing airflow:{version} -> skipping [/] {github_url}")
                continue
            if line != new_line:
                console.print(f'[yellow] Replacing in {file}[/]\n{line.strip()}\n{new_line.strip()}')
                return new_line
    return line


def find_matches(directory: str, _file: Path, version: str):
    lines = _file.read_text().splitlines(keepends=True)
    new_lines = []
    for index, line in enumerate(lines):
        new_line = replace_match(directory, str(_file), line, version)
        if new_line:
            new_lines.append(new_line)
    _file.write_text("".join(new_lines))


if __name__ == '__main__':
    curdir = Path(os.curdir).resolve()
    directory = curdir / "apache-airflow"
    version_dirs = list(filter(os.path.isdir, directory.iterdir()))
    with Progress(console=console) as progress:
        task = progress.add_task(f"Updating {len(version_dirs)}", total=len(version_dirs))
        for version_dir in version_dirs:
            version = version_dir.name
            console.print(version)
            for root, dirnames, filenames in os.walk(str(directory / version)):
                for filename in filenames:
                    if filename.endswith('.html'):
                        candidate_file = os.path.join(root, filename)
                        console.print(f'[bright_blue]File to process: {candidate_file}[/]')
                        find_matches(root, Path(candidate_file), version)
            progress.advance(task)
