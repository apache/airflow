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
import sys
from pathlib import Path
from subprocess import call

from rich.console import Console

AIRFLOW_SOURCES_DIR = Path(__file__).parents[3].resolve()
BREEZE_IMAGES_DIR = AIRFLOW_SOURCES_DIR / "images" / "breeze"
BREEZE_INSTALL_DIR = AIRFLOW_SOURCES_DIR / "dev" / "breeze"
BREEZE_SOURCES_DIR = BREEZE_INSTALL_DIR / "src"
FORCE = os.environ.get('FORCE', "false")[0].lower() == "t"
VERBOSE = os.environ.get('VERBOSE', "false")[0].lower() == "t"
DRY_RUN = os.environ.get('DRY_RUN', "false")[0].lower() == "t"

console = Console(width=400, color_system="standard")


def verify_all_commands_described_in_docs():
    errors = []
    doc_content = (AIRFLOW_SOURCES_DIR / "BREEZE.rst").read_text()
    for file_name in os.listdir(BREEZE_IMAGES_DIR):
        if file_name.startswith("output_") and file_name.endswith(".svg"):
            command = file_name[len("output_") : -len(".svg")]
            if command == "breeze-commands":
                continue
            if file_name not in doc_content:
                errors.append(command)
            else:
                console.print(f"[green]OK. The {command} screenshot is embedded in BREEZE.rst.")
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
    env['AIRFLOW_SOURCES_ROOT'] = str(AIRFLOW_SOURCES_DIR)
    # needed to keep consistent output
    env['PYTHONPATH'] = str(BREEZE_SOURCES_DIR)
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


def run_image_regeneration_in_breeze() -> int:
    sys.path.insert(0, str(AIRFLOW_SOURCES_DIR / "dev" / "breeze" / "src"))
    from airflow_breeze.utils.run_utils import run_command

    result = run_command(['breeze', 'version'], check=False, capture_output=True)
    if result.returncode != 0:
        run_command(
            [
                sys.executable,
                '-m',
                'pip',
                'install',
                '-e',
                os.fspath(AIRFLOW_SOURCES_DIR / "dev" / "breeze"),
            ],
            check=True,
            capture_output=True,
        )
    result = run_command(['breeze', 'setup', 'regenerate-command-images'], check=False)
    return result.returncode


if __name__ == '__main__':
    verify_all_commands_described_in_docs()

    run_generation = False
    if FORCE:
        run_generation = True
        console.print(
            '[bright_blue]Force regenerating all images. It will be run in Breeze image for consistency.'
        )
    elif is_regeneration_needed():
        run_generation = True
        console.print('[yellow]Image generation is needed. It will be run in Breeze image for consistency.')
    if run_generation:
        return_code = run_image_regeneration_in_breeze()
        if return_code != 0 and os.environ.get('CI'):
            console.print(
                "\n\n[yellow]Please run this command and commit resulting breeze images:[/]"
                "\n\n    `breeze setup regenerate-command-images`\n"
                "\n\n[yellow]This will regenerate all the images in your commit!\n\n"
            )
        sys.exit(return_code)
