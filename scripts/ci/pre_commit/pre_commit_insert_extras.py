#!/usr/bin/env python
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

import sys
import textwrap
from enum import Enum
from pathlib import Path

# tomllib is available in Python 3.11+ and before that tomli offers same interface for parsing TOML files
try:
    import tomllib
except ImportError:
    import tomli as tomllib

AIRFLOW_ROOT_PATH = Path(__file__).parents[3].resolve()
PYPROJECT_TOML_FILE_PATH = AIRFLOW_ROOT_PATH / "pyproject.toml"

sys.path.insert(0, str(Path(__file__).parent.resolve()))  # make sure common_precommit_utils is imported
from common_precommit_utils import insert_documentation


class ExtraType(Enum):
    DEVEL = "DEVEL"
    DOC = "DOC"
    REGULAR = "REGULAR"


def get_header_and_footer(extra_type: ExtraType, file_format: str) -> tuple[str, str]:
    if file_format == "rst":
        return f"  .. START {extra_type.value} EXTRAS HERE", f"  .. END {extra_type.value} EXTRAS HERE"
    elif file_format == "txt":
        return f"# START {extra_type.value} EXTRAS HERE", f"# END {extra_type.value} EXTRAS HERE"
    else:
        raise Exception(f"Bad format {format} passed. Only rst and txt are supported")


def get_wrapped_list(extras_set: set[str]) -> list[str]:
    array = [line + "\n" for line in textwrap.wrap(", ".join(sorted(extras_set)), 100)]
    array.insert(0, "\n")
    array.append("\n")
    return array


def get_extra_types_dict(extras: dict[str, list[str]]) -> dict[ExtraType, tuple[set[str], list[str]]]:
    """
    Split extras into four types.

    :return: dictionary of extra types with tuple of two set,list - set of extras and text-wrapped list
    """
    extra_type_dict: dict[ExtraType, tuple[set[str], list[str]]] = {}

    for extra_type in ExtraType:
        extra_type_dict[extra_type] = (set(), [])

    for key, value in extras.items():
        if key.startswith("devel"):
            extra_type_dict[ExtraType.DEVEL][0].add(key)
        elif key in ["doc", "doc-gen"]:
            extra_type_dict[ExtraType.DOC][0].add(key)
        else:
            extra_type_dict[ExtraType.REGULAR][0].add(key)

    for extra_type in ExtraType:
        extra_type_dict[extra_type][1].extend(get_wrapped_list(extra_type_dict[extra_type][0]))

    return extra_type_dict


def get_extras_from_pyproject_toml() -> dict[str, list[str]]:
    pyproject_toml_content = tomllib.loads(PYPROJECT_TOML_FILE_PATH.read_text())
    return pyproject_toml_content["project"]["optional-dependencies"]


FILES_TO_UPDATE = [
    (AIRFLOW_ROOT_PATH / "INSTALL", "txt"),
    (AIRFLOW_ROOT_PATH / "contributing-docs" / "12_airflow_dependencies_and_extras.rst", "rst"),
]


def process_documentation_files():
    extra_type_dict = get_extra_types_dict(get_extras_from_pyproject_toml())
    for file, file_format in FILES_TO_UPDATE:
        if not file.exists():
            raise Exception(f"File {file} does not exist")
        for extra_type in ExtraType:
            header, footer = get_header_and_footer(extra_type, file_format)
            insert_documentation(file, extra_type_dict[extra_type][1], header, footer)


if __name__ == "__main__":
    process_documentation_files()
