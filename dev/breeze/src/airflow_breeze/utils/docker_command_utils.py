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
import re
import subprocess
from typing import List

from packaging import version

from airflow_breeze.console import console
from airflow_breeze.global_constants import (
    MIN_DOCKER_COMPOSE_VERSION,
    MIN_DOCKER_VERSION,
    MOUNT_ALL_LOCAL_SOURCES,
    MOUNT_SELECTED_LOCAL_SOURCES,
)
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


def get_extra_docker_flags(all: bool, selected: bool, airflow_sources: str) -> List:
    # get_extra_docker_flags(False, str(airflow_source))
    # add verbosity
    EXTRA_DOCKER_FLAGS = []
    if all:
        EXTRA_DOCKER_FLAGS.extend(["-v", f"{airflow_sources}:/opt/airflow/:cached"])
    elif selected:
        for flag in NECESSARY_HOST_VOLUMES:
            EXTRA_DOCKER_FLAGS.extend(["-v", airflow_sources + flag])
    else:
        console.print('Skip mounting host volumes to Docker')
    EXTRA_DOCKER_FLAGS.extend(["-v", f"{airflow_sources}/files:/files"])
    EXTRA_DOCKER_FLAGS.extend(["-v", f"{airflow_sources}/dist:/dist"])
    EXTRA_DOCKER_FLAGS.extend(["--rm"])
    EXTRA_DOCKER_FLAGS.extend(["--env-file", f"{airflow_sources}/scripts/ci/docker-compose/_docker.env"])
    return EXTRA_DOCKER_FLAGS


def check_docker_resources(verbose: bool, airflow_sources: str, airflow_ci_image_name: str):
    extra_docker_flags = get_extra_docker_flags(
        MOUNT_ALL_LOCAL_SOURCES, MOUNT_SELECTED_LOCAL_SOURCES, airflow_sources
    )
    cmd = []
    cmd.extend(["docker", "run", "-t"])
    cmd.extend(extra_docker_flags)
    cmd.extend(["--entrypoint", "/bin/bash", airflow_ci_image_name])
    cmd.extend(["-c", "python /opt/airflow/scripts/in_container/run_resource_check.py"])
    run_command(cmd, verbose=verbose, text=True)


def check_docker_permission(verbose) -> bool:
    permission_denied = False
    docker_permission_command = ["docker", "info"]
    try:
        _ = run_command(
            docker_permission_command,
            verbose=verbose,
            suppress_console_print=True,
            capture_output=True,
            text=True,
        )
    except subprocess.CalledProcessError as ex:
        permission_denied = True
        if ex.stdout and 'Got permission denied while trying to connect' in ex.stdout:
            console.print('ERROR: You have `permission denied` error when trying to communicate with docker.')
            console.print(
                'Most likely you need to add your user to `docker` group: \
                https://docs.docker.com/ engine/install/linux-postinstall/ .'
            )
    return permission_denied


def compare_version(current_version: str, min_version: str) -> bool:
    return version.parse(current_version) >= version.parse(min_version)


def check_docker_version(verbose: bool):
    permission_denied = check_docker_permission(verbose)
    if not permission_denied:
        docker_version_command = ['docker', 'version', '--format', '{{.Client.Version}}']
        docker_version = ''
        docker_version_output = run_command(
            docker_version_command,
            verbose=verbose,
            suppress_console_print=True,
            capture_output=True,
            text=True,
        )
        if docker_version_output.returncode == 0:
            docker_version = docker_version_output.stdout.strip()
        if docker_version == '':
            console.print(
                f'Your version of docker is unknown. If the scripts fail, please make sure to \
                    install docker at least: {MIN_DOCKER_VERSION} version.'
            )
        else:
            good_version = compare_version(docker_version, MIN_DOCKER_VERSION)
            if good_version:
                console.print(f'Good version of Docker: {docker_version}.')
            else:
                console.print(
                    f'Your version of docker is too old:{docker_version}. Please upgrade to \
                    at least {MIN_DOCKER_VERSION}'
                )


def check_docker_compose_version(verbose: bool):
    version_pattern = re.compile(r'(\d+)\.(\d+)\.(\d+)')
    docker_compose_version_command = ["docker-compose", "--version"]
    docker_compose_version_output = run_command(
        docker_compose_version_command,
        verbose=verbose,
        suppress_console_print=True,
        capture_output=True,
        text=True,
    )
    if docker_compose_version_output.returncode == 0:
        docker_compose_version = docker_compose_version_output.stdout
        version_extracted = version_pattern.search(docker_compose_version)
        if version_extracted is not None:
            version = '.'.join(version_extracted.groups())
            good_version = compare_version(version, MIN_DOCKER_COMPOSE_VERSION)
            if good_version:
                console.print(f'Good version of docker-compose: {version}')
            else:
                console.print(
                    f'You have too old version of docker-compose: {version}! \
                At least 1.29 is needed! Please upgrade!'
                )
                console.print(
                    'See https://docs.docker.com/compose/install/ for instructions. \
                Make sure docker-compose you install is first on the PATH variable of yours.'
                )
    else:
        console.print(
            'Unknown docker-compose version. At least 1.29 is needed! \
        If Breeze fails upgrade to latest available docker-compose version'
        )
