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
"""
Check if extras in Dockerfile are reflected in docker build-arg-ref.rst and global constants.
"""

from __future__ import annotations

import sys
from pathlib import Path

from rich import print

sys.path.insert(
    0, str(Path(__file__).parent.resolve())
)  # make sure common_precommit_utils is imported
from common_precommit_utils import AIRFLOW_SOURCES_ROOT_PATH, check_list_sorted

errors: list[str] = []

MY_DIR_PATH = Path(__file__).parent.resolve()

BUILD_ARGS_REF_PATH = (
    AIRFLOW_SOURCES_ROOT_PATH / "docs" / "docker-stack" / "build-arg-ref.rst"
)
GLOBAL_CONSTANTS_PATH = (
    AIRFLOW_SOURCES_ROOT_PATH
    / "dev"
    / "breeze"
    / "src"
    / "airflow_breeze"
    / "global_constants.py"
)

START_RST_LINE = ".. BEGINNING OF EXTRAS LIST UPDATED BY PRE COMMIT"
END_RST_LINE = ".. END OF EXTRAS LIST UPDATED BY PRE COMMIT"

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
    lines = (AIRFLOW_SOURCES_ROOT_PATH / "Dockerfile").read_text().splitlines()
    extras_list = None
    for line in lines:
        if line.startswith("ARG AIRFLOW_EXTRAS="):
            extras_list = line.split("=")[1].replace('"', "").split(",")
            if check_list_sorted(extras_list, "Dockerfile's AIRFLOW_EXTRAS", errors):
                builds_args_content = BUILD_ARGS_REF_PATH.read_text().splitlines(
                    keepends=True
                )
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
                global_constants_path = GLOBAL_CONSTANTS_PATH.read_text().splitlines(
                    keepends=True
                )
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


if __name__ == "__main__":
    check_dockerfile()
    print()
    print()
    for error in errors:
        print(error)

    print()

    if errors:
        sys.exit(1)
