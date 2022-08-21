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

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Tuple

from airflow_breeze.branch_defaults import AIRFLOW_BRANCH, DEFAULT_AIRFLOW_CONSTRAINTS_BRANCH
from airflow_breeze.global_constants import (
    ALLOWED_BACKENDS,
    ALLOWED_CONSTRAINTS_MODES_CI,
    ALLOWED_INSTALLATION_PACKAGE_FORMATS,
    ALLOWED_MSSQL_VERSIONS,
    ALLOWED_MYSQL_VERSIONS,
    ALLOWED_POSTGRES_VERSIONS,
    ALLOWED_PYTHON_MAJOR_MINOR_VERSIONS,
    AVAILABLE_INTEGRATIONS,
    DOCKER_DEFAULT_PLATFORM,
    MOUNT_ALL,
    MOUNT_REMOVE,
    MOUNT_SELECTED,
    MOUNT_SKIP,
    get_airflow_version,
)
from airflow_breeze.utils.console import get_console
from airflow_breeze.utils.path_utils import AIRFLOW_SOURCES_ROOT, BUILD_CACHE_DIR, SCRIPTS_CI_DIR
from airflow_breeze.utils.run_utils import get_filesystem_type, run_command


@dataclass
class ShellParams:
    """
    Shell parameters. Those parameters are used to determine command issued to run shell command.
    """

    airflow_branch: str = os.environ.get('DEFAULT_BRANCH', AIRFLOW_BRANCH)
    default_constraints_branch: str = os.environ.get(
        'DEFAULT_CONSTRAINTS_BRANCH', DEFAULT_AIRFLOW_CONSTRAINTS_BRANCH
    )
    airflow_constraints_reference: str = DEFAULT_AIRFLOW_CONSTRAINTS_BRANCH
    airflow_extras: str = ""
    answer: Optional[str] = None
    backend: str = ALLOWED_BACKENDS[0]
    ci: bool = False
    db_reset: bool = False
    dry_run: bool = False
    extra_args: Tuple = ()
    force_build: bool = False
    forward_credentials: str = "false"
    airflow_constraints_mode: str = ALLOWED_CONSTRAINTS_MODES_CI[0]
    github_actions: str = os.environ.get('GITHUB_ACTIONS', "false")
    github_repository: str = "apache/airflow"
    github_token: str = os.environ.get('GITHUB_TOKEN', "")
    image_tag: Optional[str] = None
    include_mypy_volume: bool = False
    install_airflow_version: str = ""
    install_providers_from_sources: bool = True
    integration: Tuple[str, ...] = ()
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
    use_airflow_version: Optional[str] = None
    use_packages_from_dist: bool = False
    verbose: bool = False
    version_suffix_for_pypi: str = ""

    @property
    def airflow_version(self):
        return get_airflow_version()

    @property
    def airflow_version_for_production_image(self):
        cmd = ['docker', 'run', '--entrypoint', '/bin/bash', f'{self.airflow_image_name}']
        cmd.extend(['-c', 'echo "${AIRFLOW_VERSION}"'])
        output = run_command(cmd, capture_output=True, text=True)
        return output.stdout.strip() if output.stdout else "UNKNOWN_VERSION"

    @property
    def airflow_base_image_name(self) -> str:
        image = f'ghcr.io/{self.github_repository.lower()}'
        return image

    @property
    def airflow_image_name(self) -> str:
        """Construct CI image link"""
        image = f'{self.airflow_base_image_name}/{self.airflow_branch}/ci/python{self.python}'
        return image

    @property
    def airflow_image_name_with_tag(self) -> str:
        image = self.airflow_image_name
        return image if not self.image_tag else image + f":{self.image_tag}"

    @property
    def airflow_image_kubernetes(self) -> str:
        image = f'{self.airflow_base_image_name}/{self.airflow_branch}/kubernetes/python{self.python}'
        return image

    @property
    def airflow_sources(self):
        return AIRFLOW_SOURCES_ROOT

    @property
    def enabled_integrations(self) -> str:
        if "all" in self.integration:
            enabled_integration = " ".join(AVAILABLE_INTEGRATIONS)
        elif len(self.integration) > 0:
            enabled_integration = " ".join(self.integration)
        else:
            enabled_integration = ""
        return enabled_integration

    @property
    def image_type(self) -> str:
        return 'CI'

    @property
    def md5sum_cache_dir(self) -> Path:
        cache_dir = Path(BUILD_CACHE_DIR, self.airflow_branch, self.python, self.image_type)
        return cache_dir

    @property
    def backend_version(self) -> str:
        version = ''
        if self.backend == 'postgres':
            version = self.postgres_version
        if self.backend == 'mysql':
            version = self.mysql_version
        if self.backend == 'mssql':
            version = self.mssql_version
        return version

    @property
    def sqlite_url(self) -> str:
        sqlite_url = "sqlite:////root/airflow/airflow.db"
        return sqlite_url

    def print_badge_info(self):
        if self.verbose:
            get_console().print(f'[info]Use {self.image_type} image[/]')
            get_console().print(f'[info]Branch Name: {self.airflow_branch}[/]')
            get_console().print(f'[info]Docker Image: {self.airflow_image_name_with_tag}[/]')
            get_console().print(f'[info]Airflow source version:{self.airflow_version}[/]')
            get_console().print(f'[info]Python Version: {self.python}[/]')
            get_console().print(f'[info]Backend: {self.backend} {self.backend_version}[/]')
            get_console().print(f'[info]Airflow used at runtime: {self.use_airflow_version}[/]')

    def get_backend_compose_files(self, backend: str):
        backend_docker_compose_file = f"{str(SCRIPTS_CI_DIR)}/docker-compose/backend-{backend}.yml"
        backend_port_docker_compose_file = f"{str(SCRIPTS_CI_DIR)}/docker-compose/backend-{backend}-port.yml"
        return backend_docker_compose_file, backend_port_docker_compose_file

    @property
    def compose_files(self):
        compose_ci_file = []
        main_ci_docker_compose_file = f"{str(SCRIPTS_CI_DIR)}/docker-compose/base.yml"
        if self.backend != "all":
            backend_files = self.get_backend_compose_files(self.backend)
        else:
            backend_files = []
            for backend in ALLOWED_BACKENDS:
                backend_files.extend(self.get_backend_compose_files(backend))
            compose_ci_file.append(f"{str(SCRIPTS_CI_DIR)}/docker-compose/backend-mssql-bind-volume.yml")
            compose_ci_file.append(f"{str(SCRIPTS_CI_DIR)}/docker-compose/backend-mssql-docker-volume.yml")
        local_docker_compose_file = f"{str(SCRIPTS_CI_DIR)}/docker-compose/local.yml"
        local_all_sources_docker_compose_file = f"{str(SCRIPTS_CI_DIR)}/docker-compose/local-all-sources.yml"
        files_docker_compose_file = f"{str(SCRIPTS_CI_DIR)}/docker-compose/files.yml"
        remove_sources_docker_compose_file = f"{str(SCRIPTS_CI_DIR)}/docker-compose/remove-sources.yml"
        mypy_docker_compose_file = f"{str(SCRIPTS_CI_DIR)}/docker-compose/mypy.yml"
        forward_credentials_docker_compose_file = (
            f"{str(SCRIPTS_CI_DIR)}/docker-compose/forward-credentials.yml"
        )
        # mssql based check have to be added
        if self.backend == 'mssql':
            docker_filesystem = get_filesystem_type('.')
            if docker_filesystem == 'tmpfs':
                compose_ci_file.append(f"{str(SCRIPTS_CI_DIR)}/docker-compose/backend-mssql-bind-volume.yml")
            else:
                compose_ci_file.append(
                    f"{str(SCRIPTS_CI_DIR)}/docker-compose/backend-mssql-docker-volume.yml"
                )
        compose_ci_file.extend([main_ci_docker_compose_file, *backend_files, files_docker_compose_file])

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
        if self.mount_sources == MOUNT_SELECTED:
            compose_ci_file.extend([local_docker_compose_file])
        elif self.mount_sources == MOUNT_ALL:
            compose_ci_file.extend([local_all_sources_docker_compose_file])
        elif self.mount_sources == MOUNT_REMOVE:
            compose_ci_file.extend([remove_sources_docker_compose_file])
        if self.forward_credentials:
            compose_ci_file.append(forward_credentials_docker_compose_file)
        if self.use_airflow_version is not None:
            compose_ci_file.append(remove_sources_docker_compose_file)
        if self.include_mypy_volume:
            compose_ci_file.append(mypy_docker_compose_file)
        if "all" in self.integration:
            integrations = AVAILABLE_INTEGRATIONS
        else:
            integrations = self.integration
        if len(integrations) > 0:
            for integration in integrations:
                compose_ci_file.append(f"{str(SCRIPTS_CI_DIR)}/docker-compose/integration-{integration}.yml")
        return os.pathsep.join(compose_ci_file)

    @property
    def command_passed(self):
        cmd = None
        if len(self.extra_args) > 0:
            cmd = str(self.extra_args[0])
        return cmd
