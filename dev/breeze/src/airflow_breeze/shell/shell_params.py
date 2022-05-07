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
"""Breeze shell paameters."""
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Tuple

from airflow_breeze.branch_defaults import AIRFLOW_BRANCH, DEFAULT_AIRFLOW_CONSTRAINTS_BRANCH
from airflow_breeze.global_constants import (
    ALLOWED_BACKENDS,
    ALLOWED_DEBIAN_VERSIONS,
    ALLOWED_GENERATE_CONSTRAINTS_MODES,
    ALLOWED_INSTALLATION_PACKAGE_FORMATS,
    ALLOWED_MSSQL_VERSIONS,
    ALLOWED_MYSQL_VERSIONS,
    ALLOWED_POSTGRES_VERSIONS,
    ALLOWED_PYTHON_MAJOR_MINOR_VERSIONS,
    AVAILABLE_INTEGRATIONS,
    MOUNT_ALL,
    MOUNT_SELECTED,
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

    verbose: bool = False
    extra_args: Tuple = ()
    force_build: bool = False
    integration: Tuple[str, ...] = ()
    postgres_version: str = ALLOWED_POSTGRES_VERSIONS[0]
    mssql_version: str = ALLOWED_MSSQL_VERSIONS[0]
    mysql_version: str = ALLOWED_MYSQL_VERSIONS[0]
    backend: str = ALLOWED_BACKENDS[0]
    python: str = ALLOWED_PYTHON_MAJOR_MINOR_VERSIONS[0]
    debian_version: str = ALLOWED_DEBIAN_VERSIONS[0]
    dry_run: bool = False
    load_example_dags: bool = False
    load_default_connections: bool = False
    use_airflow_version: Optional[str] = None
    airflow_extras: str = ""
    airflow_constraints_reference: str = DEFAULT_AIRFLOW_CONSTRAINTS_BRANCH
    use_packages_from_dist: bool = False
    package_format: str = ALLOWED_INSTALLATION_PACKAGE_FORMATS[0]
    generate_constraints_mode: str = ALLOWED_GENERATE_CONSTRAINTS_MODES[0]
    install_providers_from_sources: bool = True
    skip_environment_initialization: bool = False
    version_suffix_for_pypi: str = ""
    install_airflow_version: str = ""
    image_tag: str = "latest"
    github_repository: str = "apache/airflow"
    mount_sources: str = MOUNT_SELECTED
    forward_credentials: str = "false"
    airflow_branch: str = AIRFLOW_BRANCH
    start_airflow: str = "false"
    github_token: str = os.environ.get('GITHUB_TOKEN', "")
    github_actions: str = os.environ.get('GITHUB_ACTIONS', "false")
    issue_id: str = ""
    num_runs: str = ""
    answer: Optional[str] = None
    db_reset: bool = False
    ci: bool = False

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
    def the_image_type(self) -> str:
        the_image_type = 'CI'
        return the_image_type

    @property
    def md5sum_cache_dir(self) -> Path:
        cache_dir = Path(BUILD_CACHE_DIR, self.airflow_branch, self.python, self.the_image_type)
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
            get_console().print(f'[info]Use {self.the_image_type} image[/]')
            get_console().print(f'[info]Branch Name: {self.airflow_branch}[/]')
            get_console().print(f'[info]Docker Image: {self.airflow_image_name_with_tag}[/]')
            get_console().print(f'[info]Airflow source version:{self.airflow_version}[/]')
            get_console().print(f'[info]Python Version: {self.python}[/]')
            get_console().print(f'[info]Backend: {self.backend} {self.backend_version}[/]')
            get_console().print(f'[info]Airflow used at runtime: {self.use_airflow_version}[/]')

    @property
    def compose_files(self):
        compose_ci_file = []
        main_ci_docker_compose_file = f"{str(SCRIPTS_CI_DIR)}/docker-compose/base.yml"
        if self.backend == "mssql":
            backend_docker_compose_file = (
                f"{str(SCRIPTS_CI_DIR)}/docker-compose/backend-{self.backend}-{self.debian_version}.yml"
            )
        else:
            backend_docker_compose_file = f"{str(SCRIPTS_CI_DIR)}/docker-compose/backend-{self.backend}.yml"
        backend_port_docker_compose_file = (
            f"{str(SCRIPTS_CI_DIR)}/docker-compose/backend-{self.backend}-port.yml"
        )
        local_docker_compose_file = f"{str(SCRIPTS_CI_DIR)}/docker-compose/local.yml"
        local_all_sources_docker_compose_file = f"{str(SCRIPTS_CI_DIR)}/docker-compose/local-all-sources.yml"
        files_docker_compose_file = f"{str(SCRIPTS_CI_DIR)}/docker-compose/files.yml"
        remove_sources_docker_compose_file = f"{str(SCRIPTS_CI_DIR)}/docker-compose/remove-sources.yml"
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
        compose_ci_file.extend(
            [main_ci_docker_compose_file, backend_docker_compose_file, files_docker_compose_file]
        )

        if self.mount_sources == MOUNT_SELECTED:
            compose_ci_file.extend([local_docker_compose_file])
        elif self.mount_sources == MOUNT_ALL:
            compose_ci_file.extend([local_all_sources_docker_compose_file])
        else:  # none
            compose_ci_file.extend([remove_sources_docker_compose_file])
        compose_ci_file.extend([backend_port_docker_compose_file])
        if self.forward_credentials:
            compose_ci_file.append(forward_credentials_docker_compose_file)
        if self.use_airflow_version is not None:
            compose_ci_file.append(remove_sources_docker_compose_file)
        if "all" in self.integration:
            integrations = AVAILABLE_INTEGRATIONS
        else:
            integrations = self.integration
        if len(integrations) > 0:
            for integration in integrations:
                compose_ci_file.append(f"{str(SCRIPTS_CI_DIR)}/docker-compose/integration-{integration}.yml")
        return ':'.join(compose_ci_file)

    @property
    def command_passed(self):
        cmd = None
        if len(self.extra_args) > 0:
            cmd = str(self.extra_args[0])
        return cmd
