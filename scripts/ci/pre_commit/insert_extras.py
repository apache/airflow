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
from pathlib import Path

AIRFLOW_ROOT_PATH = Path(__file__).parents[3].resolve()
PYPROJECT_TOML_FILE_PATH = AIRFLOW_ROOT_PATH / "pyproject.toml"

sys.path.insert(
    0, str(Path(__file__).parent.resolve())
)  # make sure common_precommit_utils is imported
from common_precommit_utils import insert_documentation

sys.path.insert(0, AIRFLOW_ROOT_PATH.as_posix())  # make sure airflow root is imported
from hatch_build import (
    ALL_DYNAMIC_EXTRA_DICTS,
    ALL_GENERATED_BUNDLE_EXTRAS,
    BUNDLE_EXTRAS,
    PROVIDER_DEPENDENCIES,
)


def get_header_and_footer(extra_type: str, file_format: str) -> tuple[str, str]:
    if file_format == "rst":
        return (
            f"  .. START {extra_type.upper()} HERE",
            f"  .. END {extra_type.upper()} HERE",
        )
    elif file_format == "txt":
        return f"# START {extra_type.upper()} HERE", f"# END {extra_type.upper()} HERE"
    else:
        raise ValueError(f"Bad format {format} passed. Only rst and txt are supported")


def get_wrapped_list(extras_set: list[str]) -> list[str]:
    array = [line + "\n" for line in textwrap.wrap(", ".join(sorted(extras_set)), 100)]
    array.insert(0, "\n")
    array.append("\n")
    return array


def get_extra_types_dict() -> dict[str, list[str]]:
    """
    Split extras into four types.

    :return: dictionary of extra types with tuple of two set,list - set of extras and text-wrapped list
    """
    extra_type_dict: dict[str, list[str]] = {}

    for extra_dict, extra_description in ALL_DYNAMIC_EXTRA_DICTS:
        extra_list = sorted(extra_dict)
        if extra_dict == BUNDLE_EXTRAS:
            extra_list = sorted(extra_list + ALL_GENERATED_BUNDLE_EXTRAS)
        extra_type_dict[extra_description] = get_wrapped_list(extra_list)
    extra_type_dict["Provider extras"] = get_wrapped_list(PROVIDER_DEPENDENCIES)
    return extra_type_dict


FILES_TO_UPDATE: list[tuple[Path, str, bool]] = [
    (AIRFLOW_ROOT_PATH / "INSTALL", "txt", False),
    (
        AIRFLOW_ROOT_PATH
        / "contributing-docs"
        / "12_airflow_dependencies_and_extras.rst",
        "rst",
        False,
    ),
    (AIRFLOW_ROOT_PATH / "pyproject.toml", "txt", True),
]


def process_documentation_files() -> bool:
    changed = False
    extra_type_dict = get_extra_types_dict()
    for file, file_format, add_comment in FILES_TO_UPDATE:
        if not file.exists():
            raise FileNotFoundError(f"File {file} does not exist")
        for extra_type_description in extra_type_dict:
            header, footer = get_header_and_footer(extra_type_description, file_format)
            if insert_documentation(
                file, extra_type_dict[extra_type_description], header, footer, add_comment
            ):
                changed = True
    return changed


if __name__ == "__main__":
    if process_documentation_files():
        print("Some files were updated. Please commit them.")
        sys.exit(1)
