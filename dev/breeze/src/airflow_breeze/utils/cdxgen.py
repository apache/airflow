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

import atexit
import multiprocessing
import os
import signal
import sys
import time
from dataclasses import dataclass
from multiprocessing.pool import Pool
from pathlib import Path

import yaml

from airflow_breeze.global_constants import DEFAULT_PYTHON_MAJOR_MINOR_VERSION
from airflow_breeze.utils.console import Output, get_console
from airflow_breeze.utils.github import download_constraints_file, download_file_from_github
from airflow_breeze.utils.path_utils import AIRFLOW_SOURCES_ROOT, FILES_DIR
from airflow_breeze.utils.run_utils import run_command
from airflow_breeze.utils.shared_options import get_dry_run


def start_cdxgen_server(application_root_path: Path, run_in_parallel: bool, parallelism: int) -> None:
    """
    Start cdxgen server that is used to perform cdxgen scans of applications in child process
    :param run_in_parallel: run parallel servers
    :param parallelism: parallelism to use
    :param application_root_path: path where the application to scan is located
    """
    run_command(
        [
            "docker",
            "pull",
            "ghcr.io/cyclonedx/cdxgen",
        ],
        check=True,
    )
    if not run_in_parallel:
        fork_cdxgen_server(application_root_path)
    else:
        for i in range(parallelism):
            fork_cdxgen_server(application_root_path, port=9091 + i)
    time.sleep(1)
    get_console().print("[info]Waiting for cdxgen server to start")
    time.sleep(3)


def fork_cdxgen_server(application_root_path, port=9090):
    pid = os.fork()
    if pid:
        # Parent process - send signal to process group of the child process
        atexit.register(os.killpg, pid, signal.SIGTERM)
        # Give the server child process some time to start
    else:
        # Check if we are not a group leader already (We should not be)
        if os.getpid() != os.getsid(0):
            # and create a new process group where we are the leader
            os.setpgid(0, 0)
        run_command(
            [
                "docker",
                "run",
                "--init",
                "--rm",
                "-p",
                f"{port}:{port}",
                "-v",
                "/tmp:/tmp",
                "-v",
                f"{application_root_path}:/app",
                "-t",
                "ghcr.io/cyclonedx/cdxgen",
                "--server",
                "--server-host",
                "0.0.0.0",
                "--server-port",
                str(port),
            ],
            check=True,
        )
        # we should get here when the server gets terminated
        sys.exit(0)


def get_port_mapping(x):
    # if we do not sleep here, then we could skip mapping for some process if it is handle
    time.sleep(1)
    return multiprocessing.current_process().name, 9091 + x


def get_cdxgen_port_mapping(parallelism: int, pool: Pool) -> dict[str, int]:
    """
    Map processes from pool to port numbers so that there is always the same port
    used by the same process in the pool - effectively having one multiprocessing
    process talking to the same cdxgen server

    :param parallelism: parallelism to use
    :param pool: pool to map ports for
    :return: mapping of process name to port
    """
    port_map: dict[str, int] = dict(pool.map(get_port_mapping, range(parallelism)))
    return port_map


def get_provider_requirement_image_name(airflow_version: str, python_version: str) -> str:
    return f"apache/airflow-dev/base_requirements/{airflow_version}/python{python_version}"


def build_providers_base_image(airflow_version: str, python_version: str):
    image_name = get_provider_requirement_image_name(
        airflow_version=airflow_version, python_version=python_version
    )
    dockerfile = f"""
FROM ghcr.io/apache/airflow/main/ci/python{python_version}
RUN pip install --upgrade pip
# Remove all packages
RUN python -m venv /opt/airflow/providers
RUN /opt/airflow/providers/bin/pip install --upgrade pip
RUN /opt/airflow/providers/bin/pip install apache-airflow=={airflow_version} \
    --constraint https://raw.githubusercontent.com/apache/airflow/\
constraints-{airflow_version}/constraints-{python_version}.txt
"""
    run_command(["docker", "build", "--tag", image_name, "-"], input=dockerfile, text=True, check=True)


TARGET_DIR_NAME = "provider_requirements"
DOCKER_FILE_PREFIX = f"/files/{TARGET_DIR_NAME}/"


def get_requirements_for_provider(
    provider_id: str,
    airflow_version: str,
    provider_version: str | None = None,
    python_version: str = DEFAULT_PYTHON_MAJOR_MINOR_VERSION,
):
    provider_path_array = provider_id.split(".")
    if not provider_version:
        provider_file = (AIRFLOW_SOURCES_ROOT / "airflow" / "providers").joinpath(
            *provider_path_array
        ) / "provider.yaml"
        provider_version = yaml.safe_load(provider_file.read_text())["versions"][0]
    airflow_file_name = f"provider-{provider_id}-{provider_version}-base-requirements.txt"
    provider_with_airflow_file_name = f"provider-{provider_id}-{provider_version}-airflow-requirements.txt"
    provider_file_name = f"provider-{provider_id}-{provider_version}-requirements.txt"
    command = f"""
mkdir -pv {DOCKER_FILE_PREFIX}
/opt/airflow/providers/bin/pip freeze | sort > {DOCKER_FILE_PREFIX}{airflow_file_name}
/opt/airflow/providers/bin/pip install apache-airflow=={airflow_version} \
    apache-airflow-providers-{provider_id}=={provider_version}
/opt/airflow/providers/bin/pip freeze | sort > {DOCKER_FILE_PREFIX}{provider_with_airflow_file_name}
chown --recursive {os.getuid()}:{os.getgid()} {DOCKER_FILE_PREFIX}
"""
    run_command(
        [
            "docker",
            "run",
            "--rm",
            "-e",
            f"HOST_USER_ID={os.getuid()}",
            "-e",
            f"HOST_GROUP_ID={os.getgid()}",
            "-v",
            f"{AIRFLOW_SOURCES_ROOT}/files:/files",
            get_provider_requirement_image_name(
                airflow_version=airflow_version, python_version=python_version
            ),
            "-c",
            ";".join(command.split("\n")[1:-1]),
        ]
    )
    target_dir = FILES_DIR / TARGET_DIR_NAME
    airflow_file = target_dir / airflow_file_name
    provider_with_airflow_file = target_dir / provider_with_airflow_file_name
    get_console().print(f"[info]Airflow requirements in {airflow_file}")
    get_console().print(f"[info]Provider requirements in {provider_with_airflow_file}")
    base_packages = set([package.split("==")[0] for package in airflow_file.read_text().split("\n")])
    base_packages.add("apache-airflow-providers-" + provider_id.replace(".", "-"))
    provider_packages = sorted(
        [
            line
            for line in provider_with_airflow_file.read_text().split("\n")
            if line.split("==")[0] not in base_packages
        ]
    )
    get_console().print(
        f"[info]Provider {provider_id} has {len(provider_packages)} transitively "
        f"dependent packages (excluding airflow and it's dependencies)"
    )
    get_console().print(provider_packages)
    provider_file = target_dir / provider_file_name
    provider_file.write_text("\n".join(provider_packages) + "\n")
    get_console().print(
        f"[success]Generated {provider_id}:{provider_version} requirements in {provider_file}"
    )


@dataclass
class SbomApplicationJob:
    airflow_version: str
    python_version: str
    application_root_path: Path
    include_provider_dependencies: bool
    target_path: Path


def produce_sbom_for_application_via_cdxgen_server(
    job: SbomApplicationJob, output: Output | None, port_map: dict[str, int] | None = None
) -> tuple[int, str]:
    """
    Produces SBOM for application using cdxgen server.
    :param job: Job to run
    :param output: Output to use
    :param port_map map of process name to port - making sure that one process talks to one server
         in case parallel processing is used
    :return: tuple with exit code and output
    """
    import requests

    if port_map is None:
        port = 9090
    else:
        port = port_map[multiprocessing.current_process().name]
        get_console(output=output).print(f"[info]Using port {port}")
    get_console(output=output).print(
        f"[info]Updating sbom for Airflow {job.airflow_version} and python {job.python_version}"
    )
    source_dir = job.application_root_path / job.airflow_version / job.python_version
    source_dir.mkdir(parents=True, exist_ok=True)
    lock_file_relative_path = "airflow/www/yarn.lock"
    download_file_from_github(
        tag=job.airflow_version, path=lock_file_relative_path, output_file=source_dir / "yarn.lock"
    )
    if not download_constraints_file(
        airflow_version=job.airflow_version,
        python_version=job.python_version,
        include_provider_dependencies=job.include_provider_dependencies,
        output_file=source_dir / "requirements.txt",
    ):
        get_console(output=output).print(
            f"[warning]Failed to download constraints file for "
            f"{job.airflow_version} and {job.python_version}. Skipping"
        )
        return 0, f"SBOM Generate {job.airflow_version}:{job.python_version}"
    get_console(output=output).print(
        f"[info]Generating sbom for Airflow {job.airflow_version} and python {job.python_version} with cdxgen"
    )
    url = (
        f"http://127.0.0.1:{port}/sbom?path=/app/{job.airflow_version}/{job.python_version}&"
        f"project-name=apache-airflow&project-version={job.airflow_version}&multiProject=true"
    )
    get_console(output=output).print(f"[info]Triggering sbom generation in {job.airflow_version} via {url}")
    if not get_dry_run():
        response = requests.get(url)
        if response.status_code != 200:
            get_console(output=output).print(
                f"[error]Generation for Airflow {job.airflow_version}:{job.python_version} failed. "
                f"Status code {response.status_code}"
            )
            return response.status_code, f"SBOM Generate {job.airflow_version}:{job.python_version}"
        job.target_path.write_bytes(response.content)
        get_console(output=output).print(
            f"[success]Generated SBOM for {job.airflow_version}:{job.python_version}"
        )

    return 0, f"SBOM Generate {job.airflow_version}:{job.python_version}"
