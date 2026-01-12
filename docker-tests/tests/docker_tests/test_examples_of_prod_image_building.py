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
from functools import cache
from pathlib import Path

import pytest
import requests
from python_on_whales import docker

from docker_tests.command_utils import run_command
from docker_tests.constants import AIRFLOW_ROOT_PATH

DOCKER_EXAMPLES_DIR = AIRFLOW_ROOT_PATH / "docker-stack-docs" / "docker-examples"
QUARANTINED_DOCKER_EXAMPLES: dict[str, str] = {
    # You could temporarily disable check for specific Dockerfile
    # In this case you need to provide a relative path with the reason, e.g:
    # "extending/add-build-essential-extend/Dockerfile": "https://github.com/apache/airflow/issues/XX",
}


@cache
def get_latest_airflow_image():
    response = requests.get(
        "https://pypi.org/pypi/apache-airflow/json", headers={"User-Agent": "Python requests"}
    )
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


def docker_examples(directory: Path, xfails: dict[str, str] | None = None):
    xfails = xfails or {}
    result = []
    for filepath in sorted(directory.rglob("**/Dockerfile")):
        markers = []
        rel_path = filepath.relative_to(directory).as_posix()
        if xfail_reason := xfails.get(rel_path):
            markers.append(pytest.mark.xfail(reason=xfail_reason))
        result.append(pytest.param(filepath, rel_path, marks=markers, id=rel_path))
    return result


@pytest.mark.parametrize(
    ("dockerfile", "relative_path"),
    docker_examples(DOCKER_EXAMPLES_DIR, xfails=QUARANTINED_DOCKER_EXAMPLES),
)
def test_dockerfile_example(dockerfile, relative_path, tmp_path):
    image_name = relative_path.lower().replace("/", "-")
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
