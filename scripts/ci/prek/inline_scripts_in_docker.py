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

from pathlib import Path

from common_prek_utils import AIRFLOW_ROOT_PATH


def insert_content(file_path: Path, content: list[str], header: str, footer: str, file_name: str):
    text = file_path.read_text().splitlines(keepends=True)
    replacing = False
    result: list[str] = []
    for line in text:
        if line.startswith(f"{header}{file_name}"):
            replacing = True
            result.append(line)
            result += content
        if line.startswith(footer):
            replacing = False
        if not replacing:
            result.append(line)
        file_path.write_text("".join(result))


if __name__ == "__main__":
    DOCKERFILE_FILE = AIRFLOW_ROOT_PATH / "Dockerfile"
    DOCKERFILE_CI_FILE = AIRFLOW_ROOT_PATH / "Dockerfile.ci"
    SCRIPTS_DOCKER_DIR = AIRFLOW_ROOT_PATH / "scripts" / "docker"

    for file in [DOCKERFILE_FILE, DOCKERFILE_CI_FILE]:
        for script in SCRIPTS_DOCKER_DIR.iterdir():
            script_content = script.read_text().splitlines(keepends=True)
            no_comments_script_content = [
                line for line in script_content if not line.startswith("#") or line.startswith("#!")
            ]
            no_comments_script_content.insert(0, f'COPY <<"EOF" /{script.name}\n')
            insert_content(
                file_path=file,
                content=no_comments_script_content,
                header="# The content below is automatically copied from scripts/docker/",
                footer="EOF",
                file_name=script.name,
            )
