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

from dataclasses import dataclass
from pathlib import Path
from typing import Tuple

from airflow_breeze.branch_defaults import AIRFLOW_BRANCH
from airflow_breeze.console import console
from airflow_breeze.global_constants import AVAILABLE_INTEGRATIONS, get_airflow_version
from airflow_breeze.utils.host_info_utils import get_host_group_id, get_host_user_id, get_stat_bin
from airflow_breeze.utils.path_utils import AIRFLOW_SOURCE, BUILD_CACHE_DIR, SCRIPTS_CI_DIR
from airflow_breeze.utils.run_utils import get_filesystem_type, run_command


@dataclass
class ShellBuilder:
    python_version: str  # check in cache
    build_cache_local: bool
    build_cache_pulled: bool
    build_cache_disabled: bool
    backend: str  # check in cache
    integration: Tuple[str]  # check in cache
    postgres_version: str  # check in cache
    mssql_version: str  # check in cache
    mysql_version: str  # check in cache
    force_build: bool
    extra_args: Tuple
    use_airflow_version: str = ""
    install_airflow_version: str = ""
    tag: str = "latest"
    github_repository: str = "apache/airflow"
    skip_mounting_local_sources: bool = False
    mount_all_local_sources: bool = False
    forward_credentials: str = "false"
    airflow_branch: str = AIRFLOW_BRANCH
    executor: str = "KubernetesExecutor"  # check in cache
    start_airflow: str = "false"
    skip_twine_check: str = ""
    use_packages_from_dist: str = "false"
    github_actions: str = ""
    issue_id: str = ""
    num_runs: str = ""
    version_suffix_for_pypi: str = ""
    version_suffix_for_svn: str = ""

    @property
    def airflow_version(self):
        return get_airflow_version()

    @property
    def airflow_version_for_production_image(self):
        cmd = ['docker', 'run', '--entrypoint', '/bin/bash', f'{self.airflow_prod_image_name}']
        cmd.extend(['-c', 'echo "${AIRFLOW_VERSION}"'])
        output = run_command(cmd, capture_output=True, text=True)
        return output.stdout.strip()

    @property
    def host_user_id(self):
        return get_host_user_id()

    @property
    def host_group_id(self):
        return get_host_group_id()

    @property
    def airflow_image_name(self) -> str:
        image = f'ghcr.io/{self.github_repository.lower()}'
        return image

    @property
    def airflow_ci_image_name(self) -> str:
        """Construct CI image link"""
        image = f'{self.airflow_image_name}/{self.airflow_branch}/ci/python{self.python_version}'
        return image

    @property
    def airflow_ci_image_name_with_tag(self) -> str:
        image = self.airflow_ci_image_name
        return image if not self.tag else image + f":{self.tag}"

    @property
    def airflow_prod_image_name(self) -> str:
        image = f'{self.airflow_image_name}/{self.airflow_branch}/prod/python{self.python_version}'
        return image

    @property
    def airflow_image_kubernetes(self) -> str:
        image = f'{self.airflow_image_name}/{self.airflow_branch}/kubernetes/python{self.python_version}'
        return image

    @property
    def airflow_sources(self):
        return AIRFLOW_SOURCE

    @property
    def docker_cache(self) -> str:
        if self.build_cache_local:
            docker_cache = "local"
        elif self.build_cache_disabled:
            docker_cache = "disabled"
        else:
            docker_cache = "pulled"
        return docker_cache

    @property
    def mount_selected_local_sources(self) -> bool:
        mount_selected_local_sources = True
        if self.mount_all_local_sources or self.skip_mounting_local_sources:
            mount_selected_local_sources = False
        return mount_selected_local_sources

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
    def image_description(self) -> str:
        image_description = 'Airflow CI'
        return image_description

    @property
    def md5sum_cache_dir(self) -> Path:
        cache_dir = Path(BUILD_CACHE_DIR, self.airflow_branch, self.python_version, self.the_image_type)
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
        console.print(f'Use {self.the_image_type} image')
        console.print(f'Branch Name: {self.airflow_branch}')
        console.print(f'Docker Image: {self.airflow_ci_image_name_with_tag}')
        console.print(f'Airflow source version:{self.airflow_version}')
        console.print(f'Python Version: {self.python_version}')
        console.print(f'Backend: {self.backend} {self.backend_version}')
        console.print(f'Airflow used at runtime: {self.use_airflow_version}')

    @property
    def compose_files(self):
        compose_ci_file = []
        main_ci_docker_compose_file = f"{str(SCRIPTS_CI_DIR)}/docker-compose/base.yml"
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

        if self.mount_selected_local_sources:
            compose_ci_file.extend([local_docker_compose_file, backend_port_docker_compose_file])
        if self.mount_all_local_sources:
            compose_ci_file.extend([local_all_sources_docker_compose_file, backend_port_docker_compose_file])
        if self.forward_credentials:
            compose_ci_file.append(forward_credentials_docker_compose_file)
        if len(self.use_airflow_version) > 0:
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

    @property
    def get_stat_bin(self):
        return get_stat_bin()
