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
from python_on_whales import docker

# isort:off (needed to workaround isort bug)
from docker_tests.command_utils import run_command
from docker_tests.constants import SOURCE_ROOT

# isort:on (needed to workaround isort bug)

DOCKER_EXAMPLES_DIR = SOURCE_ROOT / "docs" / "docker-stack" / "docker-examples"


@lru_cache(maxsize=None)
def get_latest_airflow_image():
    response = requests.get("https://pypi.org/pypi/apache-airflow/json")
    response.raise_for_status()
    latest_released_version = response.json()["info"]["version"]
    return f"apache/airflow:{latest_released_version}"


@pytest.mark.skipif(
    os.environ.get("CI") == "true",
    reason="Skipping the script builds on CI! They take very long time to build.",
)
@pytest.mark.parametrize("script_file", glob.glob(f"{DOCKER_EXAMPLES_DIR}/**/*.sh", recursive=True))
def test_shell_script_example(script_file):
    run_command(["bash", script_file])


@pytest.mark.parametrize("dockerfile", glob.glob(f"{DOCKER_EXAMPLES_DIR}/**/Dockerfile", recursive=True))
def test_dockerfile_example(dockerfile, tmp_path):
    rel_dockerfile_path = Path(dockerfile).relative_to(DOCKER_EXAMPLES_DIR)
    image_name = str(rel_dockerfile_path).lower().replace("/", "-")
    content = Path(dockerfile).read_text()
    test_image = os.environ.get("TEST_IMAGE", get_latest_airflow_image())

    test_image_file = tmp_path / image_name
    test_image_file.write_text(re.sub(r"FROM apache/airflow:.*", rf"FROM {test_image}", content))
    try:
        image = docker.build(
            context_path=Path(dockerfile).parent,
            tags=image_name,
            file=test_image_file,
            load=True,  # Load image to docker daemon
        )
        assert image
    finally:
        docker.image.remove(image_name, force=True)
