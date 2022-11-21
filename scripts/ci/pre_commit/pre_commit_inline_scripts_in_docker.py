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

from os import listdir
from pathlib import Path

AIRFLOW_SOURCES_DIR = Path(__file__).parents[3].resolve()


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
    DOCKERFILE_FILE = AIRFLOW_SOURCES_DIR / "Dockerfile"
    DOCKERFILE_CI_FILE = AIRFLOW_SOURCES_DIR / "Dockerfile.ci"
    SCRIPTS_DOCKER_DIR = AIRFLOW_SOURCES_DIR / "scripts" / "docker"

    for file in [DOCKERFILE_FILE, DOCKERFILE_CI_FILE]:
        for script in listdir(SCRIPTS_DOCKER_DIR):
            script_content = (SCRIPTS_DOCKER_DIR / script).read_text().splitlines(keepends=True)
            no_comments_script_content = [
                line for line in script_content if not line.startswith("#") or line.startswith("#!")
            ]
            no_comments_script_content.insert(0, f'COPY <<"EOF" /{script}\n')
            insert_content(
                file_path=file,
                content=no_comments_script_content,
                header="# The content below is automatically copied from scripts/docker/",
                footer="EOF",
                file_name=script,
            )
