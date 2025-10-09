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

from pathlib import Path

from rich.syntax import Syntax

from airflow_breeze.utils.ci_group import ci_group
from airflow_breeze.utils.console import MessageType, get_console
from airflow_breeze.utils.shared_options import get_dry_run, get_verbose


def debug_pyproject_tomls(pyproject_toml_paths: list[Path]) -> None:
    if get_verbose() or get_dry_run():
        for pyproject_toml_path in pyproject_toml_paths:
            with ci_group(f"Updated {pyproject_toml_path} content", MessageType.INFO):
                # Format the content to make it more readable with rich
                syntax = Syntax(pyproject_toml_path.read_text(), "toml", theme="ansi_dark", line_numbers=True)
                get_console().print(syntax)
