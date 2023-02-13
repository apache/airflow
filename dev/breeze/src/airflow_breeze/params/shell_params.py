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

import os
from copy import deepcopy
from dataclasses import dataclass
from pathlib import Path

from airflow_breeze.branch_defaults import AIRFLOW_BRANCH, DEFAULT_AIRFLOW_CONSTRAINTS_BRANCH
from airflow_breeze.global_constants import (
    ALL_INTEGRATIONS,
    ALLOWED_BACKENDS,
    ALLOWED_CONSTRAINTS_MODES_CI,
    ALLOWED_INSTALLATION_PACKAGE_FORMATS,
    ALLOWED_MSSQL_VERSIONS,
    ALLOWED_MYSQL_VERSIONS,
    ALLOWED_POSTGRES_VERSIONS,
    ALLOWED_PYTHON_MAJOR_MINOR_VERSIONS,
    APACHE_AIRFLOW_GITHUB_REPOSITORY,
    DOCKER_DEFAULT_PLATFORM,
    MOUNT_ALL,
    MOUNT_REMOVE,
    MOUNT_SELECTED,
    MOUNT_SKIP,
    get_airflow_version,
)
from airflow_breeze.utils.console import get_console
from airflow_breeze.utils.path_utils import (
    AIRFLOW_SOURCES_ROOT,
    BUILD_CACHE_DIR,
    MSSQL_TMP_DIR_NAME,
    SCRIPTS_CI_DIR,
)
from airflow_breeze.utils.run_utils import get_filesystem_type, run_command
from airflow_breeze.utils.shared_options import get_verbose

DOCKER_COMPOSE_DIR = SCRIPTS_CI_DIR / "docker-compose"


def add_mssql_compose_file(compose_file_list: list[Path]):
    docker_filesystem = get_filesystem_type("/var/lib/docker")
    if docker_filesystem == "tmpfs":
        compose_file_list.append(DOCKER_COMPOSE_DIR / "backend-mssql-tmpfs-volume.yml")
    else:
        compose_file_list.append(DOCKER_COMPOSE_DIR / "backend-mssql-docker-volume.yml")


@dataclass
class ShellParams:
    """
    Shell parameters. Those parameters are used to determine command issued to run shell command.
    """

    airflow_branch: str = os.environ.get("DEFAULT_BRANCH", AIRFLOW_BRANCH)
    default_constraints_branch: str = os.environ.get(
        "DEFAULT_CONSTRAINTS_BRANCH", DEFAULT_AIRFLOW_CONSTRAINTS_BRANCH
    )
    airflow_constraints_reference: str = DEFAULT_AIRFLOW_CONSTRAINTS_BRANCH
    airflow_extras: str = ""
    backend: str = ALLOWED_BACKENDS[0]
    base_branch: str = "main"
    ci: bool = False
    db_reset: bool = False
    dev_mode: bool = False
    extra_args: tuple = ()
    force_build: bool = False
    forward_ports: bool = True
    forward_credentials: str = "false"
    airflow_constraints_mode: str = ALLOWED_CONSTRAINTS_MODES_CI[0]
    github_actions: str = os.environ.get("GITHUB_ACTIONS", "false")
    github_repository: str = APACHE_AIRFLOW_GITHUB_REPOSITORY
    github_token: str = os.environ.get("GITHUB_TOKEN", "")
    image_tag: str | None = None
    include_mypy_volume: bool = False
    install_airflow_version: str = ""
    install_providers_from_sources: bool = True
    integration: tuple[str, ...] = ()
    issue_id: str = ""
    load_default_connections: bool = False
    load_example_dags: bool = False
    mount_sources: str = MOUNT_SELECTED
    mssql_version: str = ALLOWED_MSSQL_VERSIONS[0]
    mysql_version: str = ALLOWED_MYSQL_VERSIONS[0]
    num_runs: str = ""
    package_format: str = ALLOWED_INSTALLATION_PACKAGE_FORMATS[0]
    platform: str = DOCKER_DEFAULT_PLATFORM
    postgres_version: str = ALLOWED_POSTGRES_VERSIONS[0]
    python: str = ALLOWED_PYTHON_MAJOR_MINOR_VERSIONS[0]
    skip_environment_initialization: bool = False
    skip_constraints: bool = False
    start_airflow: str = "false"
    test_type: str | None = None
    skip_provider_tests: bool = False
    use_airflow_version: str | None = None
    use_packages_from_dist: bool = False
    version_suffix_for_pypi: str = ""
    dry_run: bool = False
    verbose: bool = False

    def clone_with_test(self, test_type: str) -> ShellParams:
        new_params = deepcopy(self)
        new_params.test_type = test_type
        return new_params

    @property
    def airflow_version(self):
        return get_airflow_version()

    @property
    def airflow_version_for_production_image(self):
        cmd = ["docker", "run", "--entrypoint", "/bin/bash", f"{self.airflow_image_name}"]
        cmd.extend(["-c", 'echo "${AIRFLOW_VERSION}"'])
        output = run_command(cmd, capture_output=True, text=True)
        return output.stdout.strip() if output.stdout else "UNKNOWN_VERSION"

    @property
    def airflow_base_image_name(self) -> str:
        image = f"ghcr.io/{self.github_repository.lower()}"
        return image

    @property
    def airflow_image_name(self) -> str:
        """Construct CI image link"""
        image = f"{self.airflow_base_image_name}/{self.airflow_branch}/ci/python{self.python}"
        return image

    @property
    def airflow_image_name_with_tag(self) -> str:
        image = self.airflow_image_name
        return image if not self.image_tag else image + f":{self.image_tag}"

    @property
    def airflow_image_kubernetes(self) -> str:
        image = f"{self.airflow_base_image_name}/{self.airflow_branch}/kubernetes/python{self.python}"
        return image

    @property
    def airflow_sources(self):
        return AIRFLOW_SOURCES_ROOT

    @property
    def image_type(self) -> str:
        return "CI"

    @property
    def md5sum_cache_dir(self) -> Path:
        cache_dir = Path(BUILD_CACHE_DIR, self.airflow_branch, self.python, self.image_type)
        return cache_dir

    @property
    def backend_version(self) -> str:
        version = ""
        if self.backend == "postgres":
            version = self.postgres_version
        if self.backend == "mysql":
            version = self.mysql_version
        if self.backend == "mssql":
            version = self.mssql_version
        return version

    @property
    def sqlite_url(self) -> str:
        sqlite_url = "sqlite:////root/airflow/airflow.db"
        return sqlite_url

    def print_badge_info(self):
        if get_verbose():
            get_console().print(f"[info]Use {self.image_type} image[/]")
            get_console().print(f"[info]Branch Name: {self.airflow_branch}[/]")
            get_console().print(f"[info]Docker Image: {self.airflow_image_name_with_tag}[/]")
            get_console().print(f"[info]Airflow source version:{self.airflow_version}[/]")
            get_console().print(f"[info]Python Version: {self.python}[/]")
            get_console().print(f"[info]Backend: {self.backend} {self.backend_version}[/]")
            get_console().print(f"[info]Airflow used at runtime: {self.use_airflow_version}[/]")

    def get_backend_compose_files(self, backend: str) -> list[Path]:
        backend_docker_compose_file = DOCKER_COMPOSE_DIR / f"backend-{backend}.yml"
        if backend == "sqlite" or not self.forward_ports:
            return [backend_docker_compose_file]
        return [backend_docker_compose_file, DOCKER_COMPOSE_DIR / f"backend-{backend}-port.yml"]

    @property
    def compose_file(self) -> str:
        compose_file_list: list[Path] = []
        backend_files: list[Path] = []
        if self.backend != "all":
            backend_files = self.get_backend_compose_files(self.backend)
            if self.backend == "mssql":
                add_mssql_compose_file(compose_file_list)
        else:
            for backend in ALLOWED_BACKENDS:
                backend_files.extend(self.get_backend_compose_files(backend))
            add_mssql_compose_file(compose_file_list)

        compose_file_list.append(DOCKER_COMPOSE_DIR / "base.yml")
        compose_file_list.extend(backend_files)
        compose_file_list.append(DOCKER_COMPOSE_DIR / "files.yml")

        if self.image_tag is not None and self.image_tag != "latest":
            get_console().print(
                f"[warning]Running tagged image tag = {self.image_tag}. "
                f"Forcing mounted sources to be 'skip'[/]"
            )
            self.mount_sources = MOUNT_SKIP
        if self.use_airflow_version is not None:
            get_console().print(
                "[info]Forcing --mount-sources to `remove` since we are not installing airflow "
                f"from sources but from {self.use_airflow_version}[/]"
            )
            self.mount_sources = MOUNT_REMOVE
        if self.forward_ports:
            compose_file_list.append(DOCKER_COMPOSE_DIR / "base-ports.yml")
        if self.mount_sources == MOUNT_SELECTED:
            compose_file_list.append(DOCKER_COMPOSE_DIR / "local.yml")
        elif self.mount_sources == MOUNT_ALL:
            compose_file_list.append(DOCKER_COMPOSE_DIR / "local-all-sources.yml")
        elif self.mount_sources == MOUNT_REMOVE:
            compose_file_list.append(DOCKER_COMPOSE_DIR / "remove-sources.yml")
        if self.forward_credentials:
            compose_file_list.append(DOCKER_COMPOSE_DIR / "forward-credentials.yml")
        if self.use_airflow_version is not None:
            compose_file_list.append(DOCKER_COMPOSE_DIR / "remove-sources.yml")
        if self.include_mypy_volume:
            compose_file_list.append(DOCKER_COMPOSE_DIR / "mypy.yml")
        if "all" in self.integration:
            integrations = ALL_INTEGRATIONS
        else:
            integrations = self.integration
        if len(integrations) > 0:
            for integration in integrations:
                compose_file_list.append(DOCKER_COMPOSE_DIR / f"integration-{integration}.yml")
        if "trino" in integrations and "kerberos" not in integrations:
            get_console().print(
                "[warning]Adding `kerberos` integration as it is implicitly needed by trino",
            )
            compose_file_list.append(DOCKER_COMPOSE_DIR / "integration-kerberos.yml")
        return os.pathsep.join([os.fspath(f) for f in compose_file_list])

    @property
    def command_passed(self):
        cmd = None
        if len(self.extra_args) > 0:
            cmd = str(self.extra_args[0])
        return cmd

    @property
    def mssql_data_volume(self) -> str:
        docker_filesystem = get_filesystem_type("/var/lib/docker")
        # in case of Providers[....], only leave Providers
        base_test_type = self.test_type.split("[")[0] if self.test_type else None
        volume_name = f"tmp-mssql-volume-{base_test_type}" if base_test_type else "tmp-mssql-volume"
        if docker_filesystem == "tmpfs":
            return os.fspath(Path.home() / MSSQL_TMP_DIR_NAME / f"{volume_name}-{self.mssql_version}")
        else:
            # mssql_data_volume variable is only used in case of tmpfs
            return ""
