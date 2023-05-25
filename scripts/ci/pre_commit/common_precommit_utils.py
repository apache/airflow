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

import ast
import hashlib
import os
import re
from pathlib import Path

AIRFLOW_SOURCES_ROOT_PATH = Path(__file__).parents[3].resolve()
AIRFLOW_BREEZE_SOURCES_PATH = AIRFLOW_SOURCES_ROOT_PATH / "dev" / "breeze"


def read_airflow_version() -> str:
    ast_obj = ast.parse((AIRFLOW_SOURCES_ROOT_PATH / "airflow" / "__init__.py").read_text())
    for node in ast_obj.body:
        if not isinstance(node, ast.Assign):
            continue

        if node.targets[0].id == "__version__":  # type: ignore[attr-defined]
            return ast.literal_eval(node.value)

    raise RuntimeError("Couldn't find __version__ in AST")


def filter_out_providers_on_non_main_branch(files: list[str]) -> list[str]:
    """When running build on non-main branch do not take providers into account"""
    default_branch = os.environ.get("DEFAULT_BRANCH")
    if not default_branch or default_branch == "main":
        return files
    return [file for file in files if not file.startswith(f"airflow{os.sep}providers")]


def insert_documentation(file_path: Path, content: list[str], header: str, footer: str):
    text = file_path.read_text().splitlines(keepends=True)
    replacing = False
    result: list[str] = []
    for line in text:
        if line.strip().startswith(header.strip()):
            replacing = True
            result.append(line)
            result.extend(content)
        if line.strip().startswith(footer.strip()):
            replacing = False
        if not replacing:
            result.append(line)
    src = "".join(result)
    file_path.write_text(src)


def get_directory_hash(directory: Path, skip_path_regexp: str | None = None) -> str:
    files = [file for file in directory.rglob("*")]
    files.sort()
    if skip_path_regexp:
        matcher = re.compile(skip_path_regexp)
        files = [file for file in files if not matcher.match(os.fspath(file.resolve()))]
    sha = hashlib.sha256()
    for file in files:
        if file.is_file() and not file.name.startswith("."):
            sha.update(file.read_bytes())
    return sha.hexdigest()
