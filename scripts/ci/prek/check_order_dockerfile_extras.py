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
# requires-python = ">=3.11,<3.12"
# dependencies = [
#   "rich>=13.6.0",
# ]
# ///
"""
Check if extras and the dependency cache epoch in Dockerfile are reflected in docker build-arg-ref.rst
and global constants.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

from common_prek_utils import AIRFLOW_ROOT_PATH, check_list_sorted
from rich import print

errors: list[str] = []

MY_DIR_PATH = Path(__file__).parent.resolve()

BUILD_ARGS_REF_PATH = AIRFLOW_ROOT_PATH / "docker-stack-docs" / "build-arg-ref.rst"
GLOBAL_CONSTANTS_PATH = (
    AIRFLOW_ROOT_PATH / "dev" / "breeze" / "src" / "airflow_breeze" / "global_constants.py"
)

START_RST_LINE = ".. BEGINNING OF EXTRAS LIST UPDATED BY PRE COMMIT"
END_RST_LINE = ".. END OF EXTRAS LIST UPDATED BY PRE COMMIT"

DOCKERFILE_CACHE_EPOCH_PATTERN = re.compile(r'^ARG DEPENDENCY_CACHE_EPOCH="([^"]*)"$', re.MULTILINE)
BUILD_ARG_REF_CACHE_EPOCH_PATTERN = re.compile(
    r"^(\| ``DEPENDENCY_CACHE_EPOCH``\s*\|)([^|]*)\|", re.MULTILINE
)

START_PYTHON_LINE = "    # BEGINNING OF EXTRAS LIST UPDATED BY PRE COMMIT"
END_PYTHON_LINE = "    # END OF EXTRAS LIST UPDATED BY PRE COMMIT"


def get_replaced_content(
    content: list[str],
    extras_list: list[str],
    start_line: str,
    end_line: str,
    prefix: str,
    suffix: str,
    add_empty_lines: bool,
) -> list[str]:
    result = []
    is_copying = True
    for line in content:
        if line.startswith(start_line):
            result.append(f"{line}")
            if add_empty_lines:
                result.append("\n")
            is_copying = False
            for extra in extras_list:
                result.append(f"{prefix}{extra}{suffix}\n")
        elif line.startswith(end_line):
            if add_empty_lines:
                result.append("\n")
            result.append(f"{line}")
            is_copying = True
        elif is_copying:
            result.append(line)
    return result


def check_dockerfile():
    lines = (AIRFLOW_ROOT_PATH / "Dockerfile").read_text().splitlines()
    extras_list = None
    for line in lines:
        if line.startswith("ARG AIRFLOW_EXTRAS="):
            extras_list = line.split("=")[1].replace('"', "").split(",")
            if check_list_sorted(extras_list, "Dockerfile's AIRFLOW_EXTRAS", errors):
                builds_args_content = BUILD_ARGS_REF_PATH.read_text().splitlines(keepends=True)
                result = get_replaced_content(
                    builds_args_content,
                    extras_list,
                    START_RST_LINE,
                    END_RST_LINE,
                    "* ",
                    "",
                    add_empty_lines=True,
                )
                BUILD_ARGS_REF_PATH.write_text("".join(result))
                global_constants_path = GLOBAL_CONSTANTS_PATH.read_text().splitlines(keepends=True)
                result = get_replaced_content(
                    global_constants_path,
                    extras_list,
                    START_PYTHON_LINE,
                    END_PYTHON_LINE,
                    '    "',
                    '",',
                    add_empty_lines=False,
                )
                GLOBAL_CONSTANTS_PATH.write_text("".join(result))
                return
    if not extras_list:
        errors.append("Something is wrong. Dockerfile does not contain AIRFLOW_EXTRAS")


def get_synced_cache_epoch_content(dockerfile_content: str, build_args_ref_content: str) -> str:
    dockerfile_match = DOCKERFILE_CACHE_EPOCH_PATTERN.search(dockerfile_content)
    if not dockerfile_match:
        raise ValueError("Something is wrong. Dockerfile does not contain DEPENDENCY_CACHE_EPOCH")
    row_match = BUILD_ARG_REF_CACHE_EPOCH_PATTERN.search(build_args_ref_content)
    if not row_match:
        raise ValueError(f"Something is wrong. {BUILD_ARGS_REF_PATH} has no DEPENDENCY_CACHE_EPOCH row")
    old_cell = row_match.group(2)
    new_cell = f' ``"{dockerfile_match.group(1)}"``'.ljust(len(old_cell))
    if len(new_cell) > len(old_cell):
        raise ValueError(f"DEPENDENCY_CACHE_EPOCH value does not fit the {BUILD_ARGS_REF_PATH} table cell")
    return (
        build_args_ref_content[: row_match.start(2)] + new_cell + build_args_ref_content[row_match.end(2) :]
    )


def sync_dependency_cache_epoch():
    try:
        content = get_synced_cache_epoch_content(
            (AIRFLOW_ROOT_PATH / "Dockerfile").read_text(), BUILD_ARGS_REF_PATH.read_text()
        )
    except ValueError as e:
        errors.append(str(e))
        return
    BUILD_ARGS_REF_PATH.write_text(content)


if __name__ == "__main__":
    check_dockerfile()
    sync_dependency_cache_epoch()
    print()
    print()
    for error in errors:
        print(error)

    print()

    if errors:
        sys.exit(1)
