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
# requires-python = ">=3.10,<3.11"
# dependencies = [
#   "rich>=13.6.0",
# ]
# ///
from __future__ import annotations

import pathlib
import sys

from common_prek_utils import (
    initialize_breeze_prek,
    run_command_via_breeze_run,
    validate_cmd_result,
)

initialize_breeze_prek(__name__, __file__)


def _resolve_provider_yaml_files(raw_files: list[str]) -> list[str]:
    """
    Accept a mix of provider.yaml paths and Python source files.

    When a Python source file is passed (e.g. a hook whose
    ``get_connection_form_widgets()`` was edited), map it to the
    ``provider.yaml`` at the root of the same provider package so the
    conn-fields check runs even when only the hook changes.

    All paths are relative to the ``providers/`` directory, as supplied by
    prek.  The first path segment is the provider package name
    (e.g. ``samba/src/airflow/...`` → ``samba/provider.yaml``).
    """
    result: set[str] = set()
    for f in raw_files:
        p = pathlib.PurePosixPath(f)
        if p.name == "provider.yaml":
            result.add(f)
        else:
            # Map any Python file to the provider.yaml of its package root.
            # Path structure: <provider-pkg>/<rest...>
            # e.g. samba/src/airflow/providers/samba/hooks/samba.py
            parts = p.parts
            if parts:
                result.add(f"{parts[0]}/provider.yaml")
    return sorted(result)


files_to_test = _resolve_provider_yaml_files(sys.argv[1:])
cmd_result = run_command_via_breeze_run(
    ["python3", "/opt/airflow/scripts/in_container/run_provider_yaml_files_check.py", *files_to_test],
    backend="sqlite",
    warn_image_upgrade_needed=True,
    extra_env={"PYTHONWARNINGS": "default"},
)
validate_cmd_result(cmd_result, include_ci_env_check=True)
