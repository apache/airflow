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
Test for an order of dependencies in setup.py
"""
from __future__ import annotations

import re
import sys
from pathlib import Path

from rich import print

errors: list[str] = []

AIRFLOW_ROOT_PATH = Path(__file__).parents[3].resolve()
PYPROJECT_TOML_PATH = AIRFLOW_ROOT_PATH / "pyproject.toml"

sys.path.insert(0, str(Path(__file__).parent.resolve()))  # make sure common_precommit_utils is imported
from common_precommit_utils import check_list_sorted


def check_extras(type: str, extra: str, extras: list[str]) -> None:
    r"""
    Test for an order of dependencies in extra defined
    `^dependent_group_name = [.*?]\n` in setup.py
    """
    print(f"[info]Checking {type}:{extra}[/]")
    extras = [extra.replace("[", "\\[") for extra in extras]
    check_list_sorted(extras, f"Order of extra: {type}:{extra}", errors)


def extract_deps(content: str, extra: str) -> list[str]:
    deps: list[str] = []
    extracting = False
    for line in content.splitlines():
        line = line.strip()
        if line.startswith("#"):
            continue
        if not extracting and line == f"{extra} = [":
            extracting = True
        elif extracting and line == "]":
            break
        elif extracting:
            deps.append(line.strip().strip(",").strip('"'))
    return deps


def check_type(pyproject_toml_contents: str, type: str) -> None:
    """
    Test for an order of dependencies groups between mark
    '# Start dependencies group' and '# End dependencies group' in setup.py
    """
    print(f"[info]Checking {type}[/]")
    pattern_type = re.compile(f"# START OF {type}\n(.*)# END OF {type}", re.DOTALL)
    parsed_type_content = pattern_type.findall(pyproject_toml_contents)[0]
    # strip comments
    parsed_type_content = (
        "\n".join([line for line in parsed_type_content.splitlines() if not line.startswith("#")]) + "\n"
    )
    pattern_extra_name = re.compile(r" = \[.*?]\n", re.DOTALL)
    type_content = pattern_extra_name.sub(",", parsed_type_content)

    list_extra_names = type_content.strip(",").split(",")
    check_list_sorted(list_extra_names, "Order of dependencies", errors)
    for extra in list_extra_names:
        deps_list = extract_deps(parsed_type_content, extra)
        check_extras(type, extra, deps_list)


if __name__ == "__main__":
    file_contents = PYPROJECT_TOML_PATH.read_text()
    check_type(file_contents, "core extras")
    check_type(file_contents, "Apache no provider extras")
    check_type(file_contents, "devel extras")
    check_type(file_contents, "doc extras")
    check_type(file_contents, "bundle extras")
    check_type(file_contents, "deprecated extras")

    print()
    for error in errors:
        print(error)

    print()

    if errors:
        sys.exit(1)
