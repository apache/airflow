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

from typing import List

from airflow_breeze.utils.run_utils import run_command

NECESSARY_HOST_VOLUMES = [
    "/.bash_aliases:/root/.bash_aliases:cached",
    "/.bash_history:/root/.bash_history:cached",
    "/.coveragerc:/opt/airflow/.coveragerc:cached",
    "/.dockerignore:/opt/airflow/.dockerignore:cached",
    "/.flake8:/opt/airflow/.flake8:cached",
    "/.github:/opt/airflow/.github:cached",
    "/.inputrc:/root/.inputrc:cached",
    "/.rat-excludes:/opt/airflow/.rat-excludes:cached",
    "/CHANGELOG.txt:/opt/airflow/CHANGELOG.txt:cached",
    "/LICENSE:/opt/airflow/LICENSE:cached",
    "/MANIFEST.in:/opt/airflow/MANIFEST.in:cached",
    "/NOTICE:/opt/airflow/NOTICE:cached",
    "/airflow:/opt/airflow/airflow:cached",
    "/provider_packages:/opt/airflow/provider_packages:cached",
    "/dags:/opt/airflow/dags:cached",
    "/dev:/opt/airflow/dev:cached",
    "/docs:/opt/airflow/docs:cached",
    "/hooks:/opt/airflow/hooks:cached",
    "/logs:/root/airflow/logs:cached",
    "/pyproject.toml:/opt/airflow/pyproject.toml:cached",
    "/pytest.ini:/opt/airflow/pytest.ini:cached",
    "/scripts:/opt/airflow/scripts:cached",
    "/scripts/in_container/entrypoint_ci.sh:/entrypoint:cached",
    "/setup.cfg:/opt/airflow/setup.cfg:cached",
    "/setup.py:/opt/airflow/setup.py:cached",
    "/tests:/opt/airflow/tests:cached",
    "/kubernetes_tests:/opt/airflow/kubernetes_tests:cached",
    "/docker_tests:/opt/airflow/docker_tests:cached",
    "/chart:/opt/airflow/chart:cached",
    "/metastore_browser:/opt/airflow/metastore_browser:cached",
]


def get_extra_docker_flags(all: bool, airflow_sources: str) -> List:
    # get_extra_docker_flags(False, str(airflow_source))
    EXTRA_DOCKER_FLAGS = []
    if all:
        EXTRA_DOCKER_FLAGS.extend(["-v", f"{airflow_sources}:/opt/airflow/:cached"])
    else:
        for flag in NECESSARY_HOST_VOLUMES:
            EXTRA_DOCKER_FLAGS.extend(["-v", airflow_sources + flag])
    EXTRA_DOCKER_FLAGS.extend(["-v", f"{airflow_sources}/files:/files"])
    EXTRA_DOCKER_FLAGS.extend(["-v", f"{airflow_sources}/dist:/dist"])
    EXTRA_DOCKER_FLAGS.extend(["--rm"])
    EXTRA_DOCKER_FLAGS.extend(["--env-file", f"{airflow_sources}/scripts/ci/docker-compose/_docker.env"])
    return EXTRA_DOCKER_FLAGS


def check_docker_resources(
    verbose: bool, mount_all_flag: bool, airflow_sources: str, airflow_ci_image_name: str
):
    extra_docker_flags = get_extra_docker_flags(mount_all_flag, airflow_sources)
    cmd = []
    cmd.extend(["docker", "run", "-t"])
    cmd.extend(extra_docker_flags)
    cmd.extend(["--entrypoint", "/bin/bash", airflow_ci_image_name])
    cmd.extend(["-c", "python /opt/airflow/scripts/in_container/run_resource_check.py"])
    run_command(cmd, verbose=verbose, text=True)
