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
import sys
from pathlib import Path

from airflowctl import __file__ as AIRFLOW_CTL_SRC_PATH
from rich.console import Console
from rich.terminal_theme import SVG_EXPORT_THEME
from rich.text import Text

sys.path.insert(0, str(Path(__file__).parent.resolve()))
AIRFLOW_ROOT_PATH = Path(AIRFLOW_CTL_SRC_PATH).parents[2]
AIRFLOW_CTL_SOURCES_PATH = AIRFLOW_ROOT_PATH / "src"

sys.path.insert(0, str(Path(__file__).parent.resolve()))  # make sure common_precommit_utils is imported
AIRFLOWCTL_IMAGES_PATH = AIRFLOW_ROOT_PATH / "docs/images/"
HASH_FILE = AIRFLOW_ROOT_PATH / "docs/images/" / "command_hashes.txt"
COMMANDS = [
    "",  # for `airflowctl -h`, main help
    "assets",
    "auth",
    "backfills",
    "config",
    "connections",
    "dag",
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


# Get new hashes
def get_airflowctl_command_hash_dict(commands):
    hash_dict = {}
    for command in commands:
        help_text = os.popen(f"python {AIRFLOW_CTL_SOURCES_PATH}/airflowctl/__main__.py {command} -h").read()
        hash_dict[command if command != "" else "main"] = hashlib.md5(help_text.encode("utf-8")).hexdigest()
    return hash_dict


def regenerate_help_images_for_all_airflowctl_commands(commands: list[str]) -> int:
    hash_file = AIRFLOWCTL_IMAGES_PATH / "command_hashes.txt"
    os.makedirs(AIRFLOWCTL_IMAGES_PATH, exist_ok=True)
    console = Console(color_system="standard", record=True)
    env = os.environ.copy()
    env["TERM"] = "xterm-256color"

    # Load old hashes if present
    old_hash_dict = {}
    if hash_file.exists():
        for line in hash_file.read_text().splitlines():
            if line.strip():
                cmd, hash_val = line.split(":", 1)
                old_hash_dict[cmd] = hash_val

    new_hash_dict = get_airflowctl_command_hash_dict(commands)

    # Check for changes
    changed_commands = []
    for command in commands:
        command_key = command
        if command == "":
            command_key = "main"
        if old_hash_dict.get(command_key) != new_hash_dict[command_key]:
            changed_commands.append(command)

    if not changed_commands:
        console.print("[bright_blue]The hash dumps old/new are the same. Returning with return code 0.")
        return 0

    # Generate SVGs for changed commands
    for command in changed_commands:
        help_text = os.popen(f"airflowctl {command} -h").read()
        text_obj = Text.from_ansi(help_text)
        # Clear previous record, print the text, then export SVG
        console.clear()
        console.print(text_obj)
        svg_content = console.export_svg(title=f"Command: {command or 'main'}", theme=SVG_EXPORT_THEME)
        output_file = AIRFLOWCTL_IMAGES_PATH / f"output_{command.replace(' ', '_') or 'main'}.svg"
        with open(output_file, "w") as svg_file:
            svg_file.write(svg_content)

    # Write new hashes
    with open(hash_file, "w") as f:
        for cmd, hash_val in new_hash_dict.items():
            f.write(f"{cmd}:{hash_val}\n")
    console.print("[info]New hash of airflowctl commands written")

    return 0


try:
    regenerate_help_images_for_all_airflowctl_commands(COMMANDS + SUBCOMMANDS)
except Exception as e:
    print(f"Error: {e}")
    sys.exit(1)
