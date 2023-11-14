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
from __future__ import annotations

import os
import shutil
import subprocess
import sys
from pathlib import Path
from subprocess import call

from rich.console import Console

AIRFLOW_SOURCES_DIR = Path(__file__).parents[3].resolve()
BREEZE_IMAGES_DIR = AIRFLOW_SOURCES_DIR / "images" / "breeze"
BREEZE_INSTALL_DIR = AIRFLOW_SOURCES_DIR / "dev" / "breeze"
BREEZE_SOURCES_DIR = BREEZE_INSTALL_DIR / "src"
FORCE = os.environ.get("FORCE", "false")[0].lower() == "t"

console = Console(width=400, color_system="standard")


def verify_all_commands_described_in_docs():
    errors = []
    doc_content = (AIRFLOW_SOURCES_DIR / "BREEZE.rst").read_text()
    for file_path in BREEZE_IMAGES_DIR.glob("output_*.svg"):
        command = file_path.stem[len("output_") :]
        if command != "breeze-commands":
            if file_path.name in doc_content:
                console.print(f"[green]OK. The {command} screenshot is embedded in BREEZE.rst.")
            else:
                errors.append(command)
    if errors:
        console.print("[red]Some of Breeze commands are not described in BREEZE.rst:[/]")
        for command in errors:
            console.print(f"  * [red]{command}[/]")
        console.print()
        console.print(
            "[bright_yellow]Make sure you describe it and embed "
            "./images/breeze/output_<COMMAND>[_<SUBCOMMAND>].svg "
            "screenshot as image in the BREEZE.rst file.[/]"
        )
        sys.exit(1)


def is_regeneration_needed() -> bool:
    env = os.environ.copy()
    env["AIRFLOW_SOURCES_ROOT"] = str(AIRFLOW_SOURCES_DIR)
    # needed to keep consistent output
    env["PYTHONPATH"] = str(BREEZE_SOURCES_DIR)
    return_code = call(
        [
            sys.executable,
            str(BREEZE_SOURCES_DIR / "airflow_breeze" / "breeze.py"),
            "setup",
            "regenerate-command-images",
            "--check-only",
        ],
        env=env,
    )
    return return_code != 0


if __name__ == "__main__":
    return_code = 0
    verify_all_commands_described_in_docs()
    if is_regeneration_needed():
        return_code = 1
        if shutil.which("breeze") is None:
            console.print("\n[red]Breeze command configuration has changed.\n")
            console.print(
                "[yellow]The `breeze` command is not on path. "
                "Skipping regeneration of images and consistency check."
            )
            console.print(
                "\n[bright_blue]Some of the commands changed since last time images were generated.\n"
            )
            console.print("\n[yellow]You should install it and run those commands manually:\n")
            console.print("\n[magenta]breeze setup regenerate-command images\n")
            console.print("\n[magenta]breeze setup check-all-params-in-groups\n")
            sys.exit(return_code)
        res = subprocess.run(
            ["breeze", "setup", "regenerate-command-images"],
            check=False,
        )
        if res.returncode != 0:
            console.print("\n[red]Breeze command configuration has changed.\n")
            console.print("\n[bright_blue]Images have been regenerated.\n")
            console.print("\n[bright_blue]You might want to run it manually:\n")
            console.print("\n[magenta]breeze setup regenerate-command images\n")
    res = subprocess.run(
        ["breeze", "setup", "check-all-params-in-groups"],
        check=False,
    )
    if res.returncode != 0:
        return_code = 1
        console.print("\n[red]Breeze command configuration has changed.\n")
        console.print("\n[yellow]Please fix it in the appropriate command_*_config.py file\n")
        console.print("\n[bright_blue]You can run consistency check manually by running:\n")
        console.print("\nbreeze setup check-all-params-in-groups\n")
    sys.exit(return_code)
