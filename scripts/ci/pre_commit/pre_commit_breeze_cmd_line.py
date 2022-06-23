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
import os
import sys
from pathlib import Path
from subprocess import check_call, check_output, run

from rich.console import Console

AIRFLOW_SOURCES_DIR = Path(__file__).parents[3].resolve()
BREEZE_IMAGES_DIR = AIRFLOW_SOURCES_DIR / "images" / "breeze"
BREEZE_INSTALL_DIR = AIRFLOW_SOURCES_DIR / "dev" / "breeze"
BREEZE_SOURCES_DIR = BREEZE_INSTALL_DIR / "src"

SCREENSHOT_WIDTH = "120"


def get_command_list():
    comp_env = os.environ.copy()
    comp_env['COMP_WORDS'] = ""
    comp_env['COMP_CWORD'] = "0"
    comp_env['_BREEZE_COMPLETE'] = 'bash_complete'
    result = check_output('breeze', env=comp_env, text=True)
    return [x.split(",")[1] for x in result.splitlines(keepends=False)]


def print_help_for_all_commands():
    console = Console(width=int(SCREENSHOT_WIDTH), color_system="standard")
    env = os.environ.copy()
    env['AIRFLOW_SOURCES_ROOT'] = str(AIRFLOW_SOURCES_DIR)
    env['RECORD_BREEZE_WIDTH'] = SCREENSHOT_WIDTH
    env['RECORD_BREEZE_TITLE'] = "Breeze commands"
    env['RECORD_BREEZE_OUTPUT_FILE'] = str(BREEZE_IMAGES_DIR / "output-commands.svg")
    env['TERM'] = "xterm-256color"
    env['PYTHONPATH'] = str(BREEZE_SOURCES_DIR)
    new_hash = check_output(
        [
            sys.executable,
            str(BREEZE_SOURCES_DIR / "airflow_breeze" / "breeze.py"),
            "command-hash-export",
            "-",
        ],
        env=env,
        text=True,
    )
    hash_file_path = BREEZE_IMAGES_DIR / "output-commands-hash.txt"
    try:
        old_hash = hash_file_path.read_text()
    except FileNotFoundError:
        old_hash = ""
    if old_hash == new_hash:
        console.print(f"[bright_blue]Skip generation of SVG images as command hash is unchanged {old_hash}")
        return
    run([sys.executable, "-m", "pip", "install", "--upgrade", "-e", BREEZE_INSTALL_DIR])
    env = os.environ.copy()
    env['AIRFLOW_SOURCES_ROOT'] = str(AIRFLOW_SOURCES_DIR)
    env['RECORD_BREEZE_WIDTH'] = SCREENSHOT_WIDTH
    env['RECORD_BREEZE_TITLE'] = "Breeze commands"
    env['RECORD_BREEZE_OUTPUT_FILE'] = str(BREEZE_IMAGES_DIR / "output-commands.svg")
    env['TERM'] = "xterm-256color"
    check_call(["breeze", "--help"], env=env)
    for command in get_command_list():
        env = os.environ.copy()
        env['AIRFLOW_SOURCES_ROOT'] = str(AIRFLOW_SOURCES_DIR)
        env['RECORD_BREEZE_WIDTH'] = SCREENSHOT_WIDTH
        env['RECORD_BREEZE_TITLE'] = f"Command: {command}"
        env['RECORD_BREEZE_OUTPUT_FILE'] = str(BREEZE_IMAGES_DIR / f"output-{command}.svg")
        env['TERM'] = "xterm-256color"
        check_call(["breeze", command, "--help"], env=env)
    hash_file_path.write_text(new_hash)


def verify_all_commands_described_in_docs():
    console = Console(width=int(SCREENSHOT_WIDTH), color_system="standard")
    errors = []
    doc_content = (AIRFLOW_SOURCES_DIR / "BREEZE.rst").read_text()
    for file in os.listdir(BREEZE_IMAGES_DIR):
        if file.startswith("output-") and file.endswith(".svg"):
            command = file[len("output-") : -len(".svg")]
            if command == "breeze-commands":
                continue
            if file not in doc_content:
                errors.append(command)
            else:
                console.print(f"[green]OK. The {command} screenshot is embedded in BREEZE.rst.")
    if errors:
        console.print("[red]Some of Breeze commands are not described in BREEZE.rst:[/]")
        for command in errors:
            console.print(f"  * [red]{command}[/]")
        console.print()
        console.print(
            "[bright_yellow]Make sure you describe it and embed ./images/breeze/output-<COMMAND>.svg "
            "screenshot as image in the BREEZE.rst file.[/]"
        )
        sys.exit(1)


if __name__ == '__main__':
    print_help_for_all_commands()
    verify_all_commands_described_in_docs()
