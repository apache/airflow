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

import os
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.resolve()))
from common_prek_utils import AIRFLOW_ROOT_PATH, console, initialize_breeze_prek

BREEZE_INSTALL_DIR = AIRFLOW_ROOT_PATH / "dev" / "breeze"
BREEZE_DOC_DIR = BREEZE_INSTALL_DIR / "doc"
BREEZE_IMAGES_DIR = BREEZE_DOC_DIR / "images"
BREEZE_SOURCES_DIR = BREEZE_INSTALL_DIR / "src"
FORCE = os.environ.get("FORCE", "false")[0].lower() == "t"


def verify_all_commands_described_in_docs():
    errors = []
    doc_content = ""
    for file in BREEZE_DOC_DIR.glob("*.rst"):
        doc_content += file.read_text()
    for file_path in BREEZE_IMAGES_DIR.glob("output_*.svg"):
        command = file_path.stem[len("output_") :]
        if command != "breeze-commands":
            if file_path.name in doc_content:
                console.print(f"[green]OK. The {command} screenshot is embedded in BREEZE documentation.")
            else:
                errors.append(command)
    if errors:
        console.print("[red]Some of Breeze commands are not described in BREEZE documentation :[/]")
        for command in errors:
            console.print(f"  * [red]{command}[/]")
        console.print()
        console.print(
            "[bright_yellow]Make sure you describe it and embed "
            "./images/breeze/output_<COMMAND>[_<SUBCOMMAND>].svg "
            f"screenshot as image in one of the .rst docs in {BREEZE_DOC_DIR}.[/]"
        )
        sys.exit(1)


def is_regeneration_needed() -> bool:
    result = subprocess.run(
        ["breeze", "setup", "regenerate-command-images", "--check-only"],
        check=False,
    )
    return result.returncode != 0


initialize_breeze_prek(__name__, __file__)

return_code = 0
verify_all_commands_described_in_docs()
if is_regeneration_needed():
    console.print(
        "\n[bright_blue]Some of the commands changed since last time images were generated. Regenerating.\n"
    )
    return_code = 1
    res = subprocess.run(
        ["breeze", "setup", "regenerate-command-images"],
        check=False,
    )
    if res.returncode != 0:
        console.print("\n[red]Breeze command configuration has changed.\n")
        console.print("\n[bright_blue]Images have been regenerated.\n")
        console.print("\n[bright_blue]You might want to run it manually:\n")
        console.print("\n[magenta]breeze setup regenerate-command-images\n")
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
