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

import json
import os
from pathlib import Path
from typing import Any

from airflow_breeze.utils.path_utils import (
    AIRFLOW_PROVIDERS_NS_PACKAGE,
    AIRFLOW_SOURCES_ROOT,
    SYSTEM_TESTS_PROVIDERS_ROOT,
)

CONSOLE_WIDTH = 180


PROVIDER_DATA_SCHEMA_PATH = AIRFLOW_SOURCES_ROOT / "airflow" / "provider.yaml.schema.json"


def _load_schema() -> dict[str, Any]:
    with open(PROVIDER_DATA_SCHEMA_PATH) as schema_file:
        content = json.load(schema_file)
    return content


def _filepath_to_module(filepath: str):
    # TODO: handle relative to providers project
    return str(Path(filepath).relative_to(AIRFLOW_SOURCES_ROOT)).replace("/", ".")


def _filepath_to_system_tests(filepath: str):
    return str(SYSTEM_TESTS_PROVIDERS_ROOT / Path(filepath).relative_to(AIRFLOW_PROVIDERS_NS_PACKAGE))


def get_provider_yaml_paths():
    """Returns list of provider.yaml files"""
    return sorted(AIRFLOW_PROVIDERS_NS_PACKAGE.glob("**/provider.yaml"))


def pretty_format_path(path: str, start: str) -> str:
    """Formats path nicely."""
    relpath = os.path.relpath(path, start)
    if relpath == path:
        return path
    return f"{start}/{relpath}"


def prepare_code_snippet(file_path: str, line_no: int, context_lines_count: int = 5) -> str:
    """
    Prepare code snippet with line numbers and  a specific line marked.

    :param file_path: File name
    :param line_no: Line number
    :param context_lines_count: The number of lines that will be cut before and after.
    :return: str
    """
    with open(file_path) as text_file:
        # Highlight code
        code = text_file.read()
        code_lines = code.splitlines()
        # Prepend line number
        code_lines = [
            f">{lno:3} | {line}" if line_no == lno else f"{lno:4} | {line}"
            for lno, line in enumerate(code_lines, 1)
        ]
        # # Cut out the snippet
        start_line_no = max(0, line_no - context_lines_count - 1)
        end_line_no = line_no + context_lines_count
        code_lines = code_lines[start_line_no:end_line_no]
        # Join lines
        code = "\n".join(code_lines)
    return code
