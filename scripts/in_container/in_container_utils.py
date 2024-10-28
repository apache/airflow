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

import shlex
import subprocess
import textwrap
from contextlib import contextmanager

import rich_click as click
from rich.console import Console

click.rich_click.COLOR_SYSTEM = "standard"
console = Console(width=400, color_system="standard")


@contextmanager
def ci_group(group_name: str, github_actions: bool):
    if github_actions:
        console.print(f"::group::{textwrap.shorten(group_name, width=200)}", markup=False)
    console.print(group_name, markup=False)
    try:
        yield
    finally:
        if github_actions:
            console.print("::endgroup::")


def run_command(
    cmd: list[str], github_actions: bool, **kwargs
) -> subprocess.CompletedProcess:
    with ci_group(
        f"Running command: {' '.join([shlex.quote(arg) for arg in cmd])}",
        github_actions=github_actions,
    ):
        result = subprocess.run(cmd, **kwargs)
    if result.returncode != 0 and github_actions and kwargs.get("check", False):
        console.print(
            f"[red]Command failed: {' '.join([shlex.quote(entry) for entry in cmd])}[/]"
        )
        console.print(
            "[red]Please unfold the above group and to investigate the issue[/]"
        )
    return result
