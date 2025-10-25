#!/usr/bin/env python3
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

import hashlib
import os
import subprocess
import sys
from pathlib import Path

from airflowctl import __file__ as AIRFLOW_CTL_SRC_PATH
from rich.console import Console

sys.path.insert(0, str(Path(__file__).parent.resolve()))
AIRFLOW_ROOT_PATH = Path(AIRFLOW_CTL_SRC_PATH).parents[2]
AIRFLOW_CTL_SOURCES_PATH = AIRFLOW_ROOT_PATH / "src"

sys.path.insert(0, str(Path(__file__).parent.resolve()))  # make sure common_prek_utils is imported
AIRFLOWCTL_IMAGES_PATH = AIRFLOW_ROOT_PATH / "docs/images/"
HASH_FILE = AIRFLOW_ROOT_PATH / "docs/images/" / "command_hashes.txt"
COMMANDS = [
    "",  # for `airflowctl -h`, main help
    "assets",
    "auth",
    "backfill",
    "config",
    "connections",
    "dags",
    "dagrun",
    "jobs",
    "pools",
    "providers",
    "variables",
    "version",
]

SUBCOMMANDS = [
    "auth login",
]


console = Console(color_system="standard")


# Get new hashes
def get_airflowctl_command_hash_dict(commands):
    hash_dict = {}
    env = os.environ.copy()
    env["CI"] = "true"  # Set CI environment variable to ensure consistent behavior
    env["COLUMNS"] = "80"
    for command in commands:
        console.print(f"[bright_blue]Getting hash for command: {command}[/]")
        run_command = command if command != "main" else ""
        output = subprocess.check_output(
            [f"python {AIRFLOW_CTL_SOURCES_PATH}/airflowctl/__main__.py {run_command} -h"],
            shell=True,
            text=True,
            env=env,
        )
        help_text = output.strip()
        hash_dict[command if command != "" else "main"] = hashlib.md5(help_text.encode("utf-8")).hexdigest()
    return hash_dict


def regenerate_help_images_for_all_airflowctl_commands(commands: list[str], skip_hash_check: bool) -> int:
    hash_file = AIRFLOWCTL_IMAGES_PATH / "command_hashes.txt"
    os.makedirs(AIRFLOWCTL_IMAGES_PATH, exist_ok=True)
    env = os.environ.copy()
    env["TERM"] = "xterm-256color"
    env["COLUMNS"] = "65"
    old_hash_dict = {}
    new_hash_dict = {}

    if not skip_hash_check:
        # Load old hashes if present
        if hash_file.exists():
            for line in hash_file.read_text().splitlines():
                if line.strip():
                    cmd, hash_val = line.split(":", 1)
                    old_hash_dict[cmd] = hash_val

        new_hash_dict = get_airflowctl_command_hash_dict(commands)

    # Check for changes
    changed_commands = []
    for command in commands:
        command = command or "main"
        console.print(f"[bright_blue]Checking command: {command}[/]", end="")

        if skip_hash_check:
            console.print("[yellow] forced generation")
            changed_commands.append(command)
        elif old_hash_dict.get(command) != new_hash_dict[command]:
            console.print("[yellow] has changed")
            changed_commands.append(command)
        else:
            console.print("[green] has not changed")

    if not changed_commands:
        console.print("[bright_blue]The hash dumps old/new are the same. Returning with return code 0.")
        return 0

    # Generate SVGs for changed commands
    for command in changed_commands:
        path = (AIRFLOWCTL_IMAGES_PATH / f"output_{command.replace(' ', '_')}.svg").as_posix()
        run_command = command if command != "main" else ""
        subprocess.run(f"airflowctl {run_command} --preview {path}", shell=True, env=env, check=True)
        console.print(f"[bright_blue]Generated SVG for command: {command}")

    # Write new hashes
    with open(hash_file, "w") as f:
        for cmd, hash_val in new_hash_dict.items():
            f.write(f"{cmd}:{hash_val}\n")
    console.print("[info]New hash of airflowctl commands written")

    return 0


_skip_hash_check = False
if "--skip-hash-check" in sys.argv:
    _skip_hash_check = True
    console.print("[bright_blue]Skipping hash check")
    sys.argv.remove("--skip-hash-check")

if len(sys.argv) > 1:
    selected_commands = sys.argv[1:]
    console.print(f"[bright_blue]Filtering commands to: {selected_commands}")
else:
    selected_commands = COMMANDS + SUBCOMMANDS

try:
    regenerate_help_images_for_all_airflowctl_commands(selected_commands, _skip_hash_check)
except Exception as e:
    print(f"Error: {e}")
    sys.exit(1)
