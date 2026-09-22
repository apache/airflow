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
# /// script
# requires-python = ">=3.10"
# dependencies = [
#   "rich>=13.6.0",
# ]
# ///
from __future__ import annotations

import pathlib
import re
import sys

from common_prek_utils import (
    AIRFLOW_ROOT_PATH,
    DEFAULT_PYTHON_MAJOR_MINOR_VERSION,
    get_provider_base_dir_from_path,
    initialize_breeze_prek,
    run_command_via_breeze_run,
    validate_cmd_result,
)

MIN_PYTHON_VERSION_PATTERN = re.compile(r"^\d+\.\d+\.\d+$")


def _resolve_provider_yaml_files(raw_files: list[str]) -> list[str]:
    """
    Accept a mix of provider.yaml paths and Python source files.

    When a Python source file is passed (e.g. a hook whose
    ``get_connection_form_widgets()`` was edited), map it to the
    ``provider.yaml`` at the root of the same provider package so the
    conn-fields check runs even when only the hook changes.

    All paths are relative to the ``providers/`` directory, as supplied by
    prek. Rather than guessing how many path segments make up the provider
    package name, this walks up the real directory tree (via
    ``get_provider_base_dir_from_path``) until it finds the actual
    ``provider.yaml`` file, so nested/namespace provider packages
    (e.g. ``apache/beam``, ``ibm/mq``) resolve correctly without maintaining
    a list of known namespace prefixes.
    """
    result: set[str] = set()
    for f in raw_files:
        p = pathlib.PurePosixPath(f)
        if p.name == "provider.yaml":
            result.add(f)
        else:
            provider_dir = get_provider_base_dir_from_path(pathlib.Path(f))
            if provider_dir is not None:
                result.add((provider_dir / "provider.yaml").as_posix())
    return sorted(result)


def get_min_python_version_error(provider_yaml_path: pathlib.Path) -> str | None:
    """Return an actionable error when a provider's Python floor cannot fit the uv workspace."""
    for line in provider_yaml_path.read_text().splitlines():
        if line.startswith("min-python-version:"):
            value = line.partition(":")[2].split("#", maxsplit=1)[0].strip().strip("'\"")
            break
    else:
        return None

    try:
        display_path = provider_yaml_path.resolve().relative_to(AIRFLOW_ROOT_PATH)
    except ValueError:
        display_path = provider_yaml_path
    if not MIN_PYTHON_VERSION_PATTERN.fullmatch(value):
        return (
            f"{display_path}: min-python-version {value!r} must be a full X.Y.Z version. "
            f"Set it to a patch release in Python {DEFAULT_PYTHON_MAJOR_MINOR_VERSION}."
        )
    major_minor = value.rsplit(".", maxsplit=1)[0]
    if major_minor != DEFAULT_PYTHON_MAJOR_MINOR_VERSION:
        return (
            f"{display_path}: min-python-version {value!r} is outside Airflow's lowest supported "
            f"Python minor {DEFAULT_PYTHON_MAJOR_MINOR_VERSION}. uv workspaces use the intersection "
            "of members' requires-python values, so this floor would break every job on the lower "
            f"minor. Use a {DEFAULT_PYTHON_MAJOR_MINOR_VERSION}.x floor or wait for core's floor to "
            f"move."
        )
    return None


if __name__ == "__main__":
    files_to_test = _resolve_provider_yaml_files(sys.argv[1:])
    min_python_version_errors = [
        error
        for file in files_to_test
        if (error := get_min_python_version_error(pathlib.Path(file))) is not None
    ]
    if min_python_version_errors:
        print("\n".join(min_python_version_errors), file=sys.stderr)
        sys.exit(1)

    initialize_breeze_prek(__name__, __file__)
    cmd_result = run_command_via_breeze_run(
        ["python3", "/opt/airflow/scripts/in_container/run_provider_yaml_files_check.py", *files_to_test],
        backend="sqlite",
        warn_image_upgrade_needed=True,
        extra_env={"PYTHONWARNINGS": "default"},
    )
    validate_cmd_result(cmd_result, include_ci_env_check=True)
