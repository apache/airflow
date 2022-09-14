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

import glob
import os
import re
from functools import lru_cache
from pathlib import Path

import pytest
import requests

from docker_tests.command_utils import run_command
from docker_tests.constants import SOURCE_ROOT

DOCKER_EXAMPLES_DIR = SOURCE_ROOT / "docs" / "docker-stack" / "docker-examples"


@lru_cache(maxsize=None)
def get_latest_airflow_version_released():
    response = requests.get('https://pypi.org/pypi/apache-airflow/json')
    response.raise_for_status()
    return response.json()['info']['version']


@pytest.mark.skipif(
    os.environ.get('CI') == "true",
    reason="Skipping the script builds on CI! They take very long time to build.",
)
@pytest.mark.parametrize("script_file", glob.glob(f"{DOCKER_EXAMPLES_DIR}/**/*.sh", recursive=True))
def test_shell_script_example(script_file):
    run_command(["bash", script_file])


@pytest.mark.parametrize("dockerfile", glob.glob(f"{DOCKER_EXAMPLES_DIR}/**/Dockerfile", recursive=True))
def test_dockerfile_example(dockerfile):
    rel_dockerfile_path = Path(dockerfile).relative_to(DOCKER_EXAMPLES_DIR)
    image_name = str(rel_dockerfile_path).lower().replace("/", "-")
    content = Path(dockerfile).read_text()
    new_content = re.sub(
        r'FROM apache/airflow:.*', fr'FROM apache/airflow:{get_latest_airflow_version_released()}', content
    )
    try:
        run_command(
            ["docker", "build", ".", "--tag", image_name, '-f', '-'],
            cwd=str(Path(dockerfile).parent),
            input=new_content.encode(),
        )
    finally:
        run_command(["docker", "rmi", "--force", image_name])
