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
import sys
from base64 import b64encode
from copy import deepcopy
from dataclasses import dataclass, field
from functools import cached_property
from pathlib import Path

from airflow_breeze.branch_defaults import AIRFLOW_BRANCH, DEFAULT_AIRFLOW_CONSTRAINTS_BRANCH
from airflow_breeze.global_constants import (
    ALL_CORE_INTEGRATIONS,
    ALL_PROVIDERS_INTEGRATIONS,
    ALLOWED_AUTH_MANAGERS,
    ALLOWED_BACKENDS,
    ALLOWED_CONSTRAINTS_MODES_CI,
    ALLOWED_DOCKER_COMPOSE_PROJECTS,
    ALLOWED_INSTALLATION_DISTRIBUTION_FORMATS,
    ALLOWED_MYSQL_VERSIONS,
    ALLOWED_PYTHON_MAJOR_MINOR_VERSIONS,
    APACHE_AIRFLOW_GITHUB_REPOSITORY,
    BREEZE_DEBUG_APISERVER_PORT,
    BREEZE_DEBUG_CELERY_WORKER_PORT,
    BREEZE_DEBUG_DAG_PROCESSOR_PORT,
    BREEZE_DEBUG_EDGE_PORT,
    BREEZE_DEBUG_SCHEDULER_PORT,
    BREEZE_DEBUG_TRIGGERER_PORT,
    BREEZE_DEBUG_WEBSERVER_PORT,
    CELERY_BROKER_URLS_MAP,
    CELERY_EXECUTOR,
    DEFAULT_CELERY_BROKER,
    DEFAULT_POSTGRES_VERSION,
    DEFAULT_UV_HTTP_TIMEOUT,
    DOCKER_DEFAULT_PLATFORM,
    DRILL_HOST_PORT,
    EDGE_EXECUTOR,
    FAB_AUTH_MANAGER,
    FLOWER_HOST_PORT,
    GREMLIN_HOST_PORT,
    KEYCLOAK_INTEGRATION,
    MOUNT_ALL,
    MOUNT_PROVIDERS_AND_TESTS,
    MOUNT_REMOVE,
    MOUNT_SELECTED,
    MOUNT_TESTS,
    MSSQL_HOST_PORT,
    MYSQL_HOST_PORT,
    POSTGRES_BACKEND,
    POSTGRES_HOST_PORT,
    RABBITMQ_HOST_PORT,
    REDIS_HOST_PORT,
    SIMPLE_AUTH_MANAGER,
    SSH_PORT,
    START_AIRFLOW_DEFAULT_ALLOWED_EXECUTOR,
    TESTABLE_CORE_INTEGRATIONS,
    TESTABLE_PROVIDERS_INTEGRATIONS,
    USE_AIRFLOW_MOUNT_SOURCES,
    WEB_HOST_PORT,
    GithubEvents,
    GroupOfTests,
    get_airflow_version,
)
from airflow_breeze.utils.console import get_console
from airflow_breeze.utils.docker_command_utils import is_docker_rootless
from airflow_breeze.utils.host_info_utils import get_host_group_id, get_host_os, get_host_user_id
from airflow_breeze.utils.path_utils import (
    AIRFLOW_ROOT_PATH,
    BUILD_CACHE_PATH,
    GENERATED_DOCKER_COMPOSE_ENV_PATH,
    GENERATED_DOCKER_ENV_PATH,
    GENERATED_DOCKER_LOCK_PATH,
    SCRIPTS_CI_DOCKER_COMPOSE_BASE_PATH,
    SCRIPTS_CI_DOCKER_COMPOSE_BASE_PORTS_PATH,
    SCRIPTS_CI_DOCKER_COMPOSE_CI_TESTS_PATH,
    SCRIPTS_CI_DOCKER_COMPOSE_DEBUG_PORTS_PATH,
    SCRIPTS_CI_DOCKER_COMPOSE_DOCKER_SOCKET_PATH,
    SCRIPTS_CI_DOCKER_COMPOSE_ENABLE_TTY_PATH,
    SCRIPTS_CI_DOCKER_COMPOSE_FILES_PATH,
    SCRIPTS_CI_DOCKER_COMPOSE_FORWARD_CREDENTIALS_PATH,
    SCRIPTS_CI_DOCKER_COMPOSE_INTEGRATION_CELERY_PATH,
    SCRIPTS_CI_DOCKER_COMPOSE_INTEGRATION_KERBEROS_PATH,
    SCRIPTS_CI_DOCKER_COMPOSE_LOCAL_ALL_SOURCES_PATH,
    SCRIPTS_CI_DOCKER_COMPOSE_LOCAL_YAML_PATH,
    SCRIPTS_CI_DOCKER_COMPOSE_MOUNT_UI_DIST_PATH,
    SCRIPTS_CI_DOCKER_COMPOSE_MYPY_PATH,
    SCRIPTS_CI_DOCKER_COMPOSE_PATH,
    SCRIPTS_CI_DOCKER_COMPOSE_PROVIDERS_AND_TESTS_SOURCES_PATH,
    SCRIPTS_CI_DOCKER_COMPOSE_REMOVE_SOURCES_PATH,
    SCRIPTS_CI_DOCKER_COMPOSE_TESTS_SOURCES_PATH,
)
from airflow_breeze.utils.run_utils import commit_sha, run_command
from airflow_breeze.utils.shared_options import get_forced_answer, get_verbose


def generated_socket_compose_file(local_socket_path: str) -> str:
    return f"""
---
services:
  airflow:
    volumes:
      - {local_socket_path}:/var/run/docker.sock
"""


def generated_docker_host_environment() -> str:
    return """
---
services:
  airflow:
    environment:
      - DOCKER_HOST=${DOCKER_HOST}
"""


def _set_var(env: dict[str, str], variable: str, attribute: str | bool | None, default: str | None = None):
    """Set variable in env dict.

    Priorities:
    1. attribute comes first if not None
    2. then environment variable if set
    3. then not None default value if environment variable is None
    4. if default is None, then the key is not set at all in dictionary

    """
    if attribute is not None:
        if isinstance(attribute, bool):
            env[variable] = str(attribute).lower()
        else:
            env[variable] = str(attribute)
    else:
        os_variable_value = os.environ.get(variable)
        if os_variable_value is not None:
            env[variable] = os_variable_value
        elif default is not None:
            env[variable] = default


@dataclass
class ShellParams:
    """
    Shell parameters. Those parameters are used to determine command issued to run shell command.
    """

    airflow_branch: str = AIRFLOW_BRANCH
    airflow_constraints_location: str = ""
    airflow_constraints_mode: str = ALLOWED_CONSTRAINTS_MODES_CI[0]
    airflow_constraints_reference: str = ""
    airflow_extras: str = ""
    allow_pre_releases: bool = False
    auth_manager: str = ALLOWED_AUTH_MANAGERS[0]
    backend: str = ALLOWED_BACKENDS[0]
    base_branch: str = "main"
    builder: str = "autodetect"
    celery_broker: str = DEFAULT_CELERY_BROKER
    celery_flower: bool = False
    clean_airflow_installation: bool = False
    collect_only: bool = False
    create_all_roles: bool = False
    debug_components: tuple[str, ...] = ()
    debugger: str = "debugpy"
    db_reset: bool = False
    default_constraints_branch: str = DEFAULT_AIRFLOW_CONSTRAINTS_BRANCH
    dev_mode: bool = False
    docker_host: str | None = os.environ.get("DOCKER_HOST")
    downgrade_sqlalchemy: bool = False
    downgrade_pendulum: bool = False
    dry_run: bool = False
    enable_coverage: bool = False
    excluded_providers: str = ""
    executor: str = START_AIRFLOW_DEFAULT_ALLOWED_EXECUTOR
    extra_args: tuple = ()
    force_build: bool = False
    force_sa_warnings: bool = True
    force_lowest_dependencies: bool = False
    forward_credentials: bool = False
    forward_ports: bool = True
    github_actions: str = os.environ.get("GITHUB_ACTIONS", "false")
    github_repository: str = APACHE_AIRFLOW_GITHUB_REPOSITORY
    github_token: str = os.environ.get("GITHUB_TOKEN", "")
    include_mypy_volume: bool = False
    install_airflow_version: str = ""
    install_airflow_python_client: bool = False
    install_airflow_with_constraints: bool = False
    install_selected_providers: str | None = None
    integration: tuple[str, ...] = ()
    issue_id: str = ""
    keep_env_variables: bool = False
    load_default_connections: bool = False
    load_example_dags: bool = False
    mount_sources: str = MOUNT_SELECTED
    mount_ui_dist: bool = False
    mysql_version: str = ALLOWED_MYSQL_VERSIONS[0]
    no_db_cleanup: bool = False
    num_runs: str = ""
    only_min_version_update: bool = False
    distribution_format: str = ALLOWED_INSTALLATION_DISTRIBUTION_FORMATS[0]
    parallel_test_types_list: list[str] = field(default_factory=list)
    parallelism: int = 0
    platform: str = DOCKER_DEFAULT_PLATFORM
    postgres_version: str = DEFAULT_POSTGRES_VERSION
    project_name: str = ALLOWED_DOCKER_COMPOSE_PROJECTS[0]
    providers_constraints_location: str = ""
    providers_constraints_mode: str = ALLOWED_CONSTRAINTS_MODES_CI[0]
    providers_constraints_reference: str = ""
    providers_skip_constraints: bool = False
    python: str = ALLOWED_PYTHON_MAJOR_MINOR_VERSIONS[0]
    quiet: bool = False
    regenerate_missing_docs: bool = False
    restart: bool = False
    run_db_tests_only: bool = False
    run_tests: bool = False
    skip_db_tests: bool = False
    skip_environment_initialization: bool = False
    skip_image_upgrade_check: bool = False
    skip_provider_dependencies_check: bool = False
    skip_ssh_setup: bool = os.environ.get("SKIP_SSH_SETUP", "false") == "true"
    standalone_dag_processor: bool = False
    start_airflow: bool = False
    test_type: str | None = None
    start_api_server_with_examples: bool = False
    test_group: GroupOfTests | None = None
    tty: str = "auto"
    upgrade_boto: bool = False
    upgrade_sqlalchemy: bool = False
    use_airflow_version: str | None = None
    use_distributions_from_dist: bool = False
    use_mprocs: bool = False
    use_uv: bool = False
    use_xdist: bool = False
    uv_http_timeout: int = DEFAULT_UV_HTTP_TIMEOUT
    verbose: bool = False
    verbose_commands: bool = False
    version_suffix: str = ""
    warn_image_upgrade_needed: bool = False

    def clone_with_test(self, test_type: str) -> ShellParams:
        new_params = deepcopy(self)
        new_params.test_type = test_type
        return new_params

    @cached_property
    def host_user_id(self) -> str:
        return get_host_group_id()

    @cached_property
    def host_group_id(self) -> str:
        return get_host_group_id()

    @cached_property
    def host_os(self) -> str:
        return get_host_os()

    @cached_property
    def airflow_version(self):
        return get_airflow_version()

    @cached_property
    def airflow_version_for_production_image(self):
        cmd = ["docker", "run", "--entrypoint", "/bin/bash", f"{self.airflow_image_name}"]
        cmd.extend(["-c", 'echo "${AIRFLOW_VERSION}"'])
        output = run_command(cmd, capture_output=True, text=True)
        return output.stdout.strip() if output.stdout else "UNKNOWN_VERSION"

    @cached_property
    def airflow_base_image_name(self) -> str:
        image = f"ghcr.io/{self.github_repository.lower()}"
        return image

    @cached_property
    def airflow_image_name(self) -> str:
        """Construct CI image link"""
        image = f"{self.airflow_base_image_name}/{self.airflow_branch}/ci/python{self.python}"
        return image

    @cached_property
    def airflow_image_kubernetes(self) -> str:
        image = f"{self.airflow_base_image_name}/{self.airflow_branch}/kubernetes/python{self.python}"
        return image

    @cached_property
    def airflow_sources(self):
        return AIRFLOW_ROOT_PATH

    @cached_property
    def auth_manager_path(self):
        auth_manager_paths = {
            SIMPLE_AUTH_MANAGER: "airflow.api_fastapi.auth.managers.simple.simple_auth_manager.SimpleAuthManager",
            FAB_AUTH_MANAGER: "airflow.providers.fab.auth_manager.fab_auth_manager.FabAuthManager",
        }
        return auth_manager_paths[self.auth_manager]

    @cached_property
    def image_type(self) -> str:
        return "CI"

    @cached_property
    def md5sum_cache_dir(self) -> Path:
        cache_dir = Path(BUILD_CACHE_PATH, self.airflow_branch, self.python, self.image_type)
        return cache_dir

    @cached_property
    def backend_version(self) -> str:
        version = ""
        if self.backend == "postgres":
            version = self.postgres_version
        if self.backend == "mysql":
            version = self.mysql_version
        return version

    @cached_property
    def sqlite_url(self) -> str:
        return "sqlite:////root/airflow/sqlite/airflow.db"

    def print_badge_info(self):
        if get_verbose():
            get_console().print(f"[info]Use {self.image_type} image[/]")
            get_console().print(f"[info]Branch Name: {self.airflow_branch}[/]")
            get_console().print(f"[info]Docker Image: {self.airflow_image_name}[/]")
            get_console().print(f"[info]Airflow source version:{self.airflow_version}[/]")
            get_console().print(f"[info]Python Version: {self.python}[/]")
            get_console().print(f"[info]Backend: {self.backend} {self.backend_version}[/]")
            get_console().print(f"[info]Airflow used at runtime: {self.use_airflow_version}[/]")

    def get_backend_compose_files(self, backend: str) -> list[Path]:
        if backend == "sqlite" and self.project_name != "breeze":
            # When running scripts, we do not want to mount the volume to make sure that the
            # sqlite database is not persisted between runs of the script and that the
            # breeze database is not cleaned accidentally
            backend_docker_compose_file = SCRIPTS_CI_DOCKER_COMPOSE_PATH / f"backend-{backend}-no-volume.yml"
        else:
            backend_docker_compose_file = SCRIPTS_CI_DOCKER_COMPOSE_PATH / f"backend-{backend}.yml"
        if backend in ("sqlite", "none") or not self.forward_ports:
            return [backend_docker_compose_file]
        if self.project_name == "prek":
            # do not forward ports for prek - to not clash with running containers from breeze
            return [backend_docker_compose_file]
        return [backend_docker_compose_file, SCRIPTS_CI_DOCKER_COMPOSE_PATH / f"backend-{backend}-port.yml"]

    @cached_property
    def compose_file(self) -> str:
        compose_file_list: list[Path] = []
        backend_files: list[Path] = []
        if self.backend != "all":
            backend_files = self.get_backend_compose_files(self.backend)
        else:
            for backend in ALLOWED_BACKENDS:
                backend_files.extend(self.get_backend_compose_files(backend))

        if self.executor == CELERY_EXECUTOR:
            compose_file_list.append(SCRIPTS_CI_DOCKER_COMPOSE_INTEGRATION_CELERY_PATH)
            if self.use_airflow_version:
                current_extras = self.airflow_extras
                if "celery" not in current_extras.split(","):
                    get_console().print(
                        "[warning]Adding `celery` extras as it is implicitly needed by celery executor"
                    )
                    self.airflow_extras = (
                        ",".join(current_extras.split(",") + ["celery"]) if current_extras else "celery"
                    )
        if self.auth_manager == FAB_AUTH_MANAGER:
            if self.use_airflow_version:
                current_extras = self.airflow_extras
                if "fab" not in current_extras.split(","):
                    get_console().print(
                        "[warning]Adding `fab` extras as it is implicitly needed by FAB auth manager"
                    )
                    self.airflow_extras = (
                        ",".join(current_extras.split(",") + ["fab"]) if current_extras else "fab"
                    )

        compose_file_list.append(SCRIPTS_CI_DOCKER_COMPOSE_BASE_PATH)
        self.add_docker_in_docker(compose_file_list)
        compose_file_list.extend(backend_files)
        compose_file_list.append(SCRIPTS_CI_DOCKER_COMPOSE_FILES_PATH)
        if os.environ.get("CI", "false") == "true":
            compose_file_list.append(SCRIPTS_CI_DOCKER_COMPOSE_CI_TESTS_PATH)

        if self.use_airflow_version is not None and self.mount_sources not in USE_AIRFLOW_MOUNT_SOURCES:
            get_console().print(
                "\n[warning]Forcing --mount-sources to `remove` since we are not installing airflow "
                f"from sources but from {self.use_airflow_version} since you attempt"
                f" to use {self.mount_sources} (but you can use any of "
                f"{USE_AIRFLOW_MOUNT_SOURCES} in such case[/]\n"
            )
            self.mount_sources = MOUNT_REMOVE
        if self.mount_sources in USE_AIRFLOW_MOUNT_SOURCES and self.use_airflow_version is None:
            get_console().print(
                "[error]You need to specify `--use-airflow-version wheel | sdist` when using one of the"
                f"{USE_AIRFLOW_MOUNT_SOURCES} mount sources[/]"
            )
            sys.exit(1)
        if self.forward_ports and not self.project_name == "prek":
            compose_file_list.append(SCRIPTS_CI_DOCKER_COMPOSE_BASE_PORTS_PATH)
        if self.debug_components and not self.project_name == "prek":
            compose_file_list.append(SCRIPTS_CI_DOCKER_COMPOSE_DEBUG_PORTS_PATH)
        if self.mount_sources == MOUNT_SELECTED:
            compose_file_list.append(SCRIPTS_CI_DOCKER_COMPOSE_LOCAL_YAML_PATH)
        elif self.mount_sources == MOUNT_ALL:
            compose_file_list.append(SCRIPTS_CI_DOCKER_COMPOSE_LOCAL_ALL_SOURCES_PATH)
        elif self.mount_sources == MOUNT_TESTS:
            compose_file_list.append(SCRIPTS_CI_DOCKER_COMPOSE_TESTS_SOURCES_PATH)
        elif self.mount_sources == MOUNT_PROVIDERS_AND_TESTS:
            compose_file_list.append(SCRIPTS_CI_DOCKER_COMPOSE_PROVIDERS_AND_TESTS_SOURCES_PATH)
        elif self.mount_sources == MOUNT_REMOVE:
            compose_file_list.append(SCRIPTS_CI_DOCKER_COMPOSE_REMOVE_SOURCES_PATH)
        if self.mount_ui_dist:
            compose_file_list.append(SCRIPTS_CI_DOCKER_COMPOSE_MOUNT_UI_DIST_PATH)
        if self.forward_credentials:
            compose_file_list.append(SCRIPTS_CI_DOCKER_COMPOSE_FORWARD_CREDENTIALS_PATH)
        if self.include_mypy_volume:
            compose_file_list.append(SCRIPTS_CI_DOCKER_COMPOSE_MYPY_PATH)
        if self.tty == "enabled":
            compose_file_list.append(SCRIPTS_CI_DOCKER_COMPOSE_ENABLE_TTY_PATH)
        if "all-testable" in self.integration:
            if self.test_group == GroupOfTests.INTEGRATION_CORE:
                integrations = TESTABLE_CORE_INTEGRATIONS
            elif self.test_group == GroupOfTests.INTEGRATION_PROVIDERS:
                integrations = TESTABLE_PROVIDERS_INTEGRATIONS
            else:
                get_console().print(
                    "[error]You can only use `integration-core` or `integration-providers` test "
                    "group with `all-testable` integration."
                )
                sys.exit(1)
        elif "all" in self.integration:
            if self.test_group == GroupOfTests.CORE:
                integrations = ALL_CORE_INTEGRATIONS
            elif self.test_group == GroupOfTests.PROVIDERS:
                integrations = ALL_PROVIDERS_INTEGRATIONS
            else:
                get_console().print(
                    "[error]You can only use `core` or `providers` test group with `all` integration."
                )
                sys.exit(1)
        else:
            integrations = self.integration
        for integration in integrations:
            get_console().print(f"[info]Adding integration compose file for {integration}[/]")
            compose_file_list.append(SCRIPTS_CI_DOCKER_COMPOSE_PATH / f"integration-{integration}.yml")
        if "trino" in integrations and "kerberos" not in integrations:
            get_console().print(
                "[warning]Adding `kerberos` integration as it is implicitly needed by trino",
            )
            compose_file_list.append(SCRIPTS_CI_DOCKER_COMPOSE_INTEGRATION_KERBEROS_PATH)
        return os.pathsep.join([os.fspath(f) for f in compose_file_list])

    @cached_property
    def command_passed(self):
        return str(self.extra_args[0]) if self.extra_args else None

    @cached_property
    def airflow_celery_broker_url(self) -> str:
        if not self.celery_broker:
            return ""
        broker_url = CELERY_BROKER_URLS_MAP.get(self.celery_broker)
        if not broker_url:
            get_console().print(
                f"[warning]The broker {self.celery_broker} should be one of {CELERY_BROKER_URLS_MAP.keys()}"
            )
            return ""
        # Map from short form (rabbitmq/redis) to actual urls
        return broker_url

    @cached_property
    def suspended_providers_folders(self):
        from airflow_breeze.utils.packages import get_suspended_provider_folders

        return " ".join(get_suspended_provider_folders()).strip()

    def add_docker_in_docker(self, compose_file_list: list[Path]):
        generated_compose_file = SCRIPTS_CI_DOCKER_COMPOSE_PATH / "_generated_docker_in_docker.yml"
        unix_prefix = "unix://"
        if self.docker_host:
            if self.docker_host.startswith(unix_prefix):
                # Socket is locally available
                socket_path = Path(self.docker_host[len(unix_prefix) :])
                if (
                    get_host_os() == "darwin"
                    and socket_path.resolve() == (Path.home() / ".docker" / "run" / "docker.sock").resolve()
                ):
                    # We are running on MacOS and the socket is the default "user" bound one
                    # We need to pretend that we are running on Linux and use the default socket
                    # in the VM instead - see https://github.com/docker/for-mac/issues/6545
                    compose_file_list.append(SCRIPTS_CI_DOCKER_COMPOSE_DOCKER_SOCKET_PATH)
                    return
                if socket_path.is_socket():
                    generated_compose_file.write_text(generated_socket_compose_file(socket_path.as_posix()))
                    compose_file_list.append(generated_compose_file)
                else:
                    get_console().print(
                        f"[warning]The socket {socket_path} pointed at by DOCKER_HOST does not exist or is "
                        "not a socket. Cannot use it for docker-compose for docker-in-docker forwarding[/]\n"
                        "[info]If you know where your socket is, you can set DOCKER_HOST "
                        "environment variable to unix://path_to_docker.sock[/]\n"
                    )
            else:
                # Socket is something different (TCP?) just pass it through as DOCKER_HOST variable
                generated_compose_file.write_text(generated_docker_host_environment())
                compose_file_list.append(generated_compose_file)
        elif self.rootless_docker:
            xdg_runtime_dir = Path(os.environ.get("XDG_RUNTIME_DIR", f"/run/user/{get_host_user_id()}"))
            socket_path = xdg_runtime_dir / "docker.sock"
            if socket_path.is_socket():
                generated_compose_file.write_text(generated_socket_compose_file(socket_path.as_posix()))
                compose_file_list.append(generated_compose_file)
            else:
                get_console().print(
                    f"[warning]The socket {socket_path} does not exist or is not a socket. "
                    "Cannot use it for docker-compose for docker-in-docker forwarding[/]\n"
                    "[info]If you know where your socket is, you can set DOCKER_HOST environment variable "
                    "to unix://path_to_docker.sock[/]\n"
                )
        else:
            # We fall back to default docker socket when no host is defined including MacOS
            # NOTE! Even if we are using "desktop-linux" context where "/var/run/docker.sock" is not used,
            # Docker engine works fine because "/var/run/docker.sock" is mounted at the VM and there
            # the /var/run/docker.sock is available. See https://github.com/docker/for-mac/issues/6545
            compose_file_list.append(SCRIPTS_CI_DOCKER_COMPOSE_DOCKER_SOCKET_PATH)

    @cached_property
    def rootless_docker(self) -> bool:
        return is_docker_rootless()

    @property
    def env_variables_for_docker_commands(self) -> dict[str, str]:
        """
        Constructs environment variables needed by the docker-compose command, based on Shell parameters
        passed to it. We cannot cache this property because it can be run few times after modifying shell
        params - for example when we first run "pull" on images before tests anda then run tests - each
        separately with different test types.

        This is the only place where you need to add environment variables if you want to pass them to
        docker or docker-compose.

        :return: dictionary of env variables to use for docker-compose and docker command
        """

        _env: dict[str, str] = {}
        _set_var(_env, "AIRFLOW_CI_IMAGE", self.airflow_image_name)
        _set_var(_env, "AIRFLOW_CONSTRAINTS_LOCATION", self.airflow_constraints_location)
        _set_var(_env, "AIRFLOW_CONSTRAINTS_MODE", self.airflow_constraints_mode)
        _set_var(_env, "AIRFLOW_CONSTRAINTS_REFERENCE", self.airflow_constraints_reference)
        _set_var(_env, "AIRFLOW_ENV", "development")
        _set_var(_env, "AIRFLOW_EXTRAS", self.airflow_extras)
        _set_var(_env, "AIRFLOW_IMAGE_KUBERNETES", self.airflow_image_kubernetes)
        _set_var(_env, "AIRFLOW_VERSION", self.airflow_version)
        _set_var(_env, "AIRFLOW__API_AUTH__JWT_SECRET", b64encode(os.urandom(16)).decode("utf-8"))
        _set_var(_env, "AIRFLOW__CELERY__BROKER_URL", self.airflow_celery_broker_url)
        _set_var(_env, "AIRFLOW__CORE__AUTH_MANAGER", self.auth_manager_path)
        _set_var(_env, "AIRFLOW__CORE__EXECUTOR", self.executor)
        if self.auth_manager == SIMPLE_AUTH_MANAGER:
            _set_var(
                _env, "AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_USERS", "admin:admin,viewer:viewer,user:user,op:op"
            )
        _set_var(
            _env,
            "AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_PASSWORDS_FILE",
            "/opt/airflow/dev/breeze/src/airflow_breeze/files/simple_auth_manager_passwords.json",
        )
        _set_var(_env, "AIRFLOW__API__SECRET_KEY", b64encode(os.urandom(16)).decode("utf-8"))
        if self.executor == EDGE_EXECUTOR:
            _set_var(
                _env,
                "AIRFLOW__CORE__EXECUTOR",
                "airflow.providers.edge3.executors.edge_executor.EdgeExecutor",
            )
            _set_var(_env, "AIRFLOW__EDGE__API_ENABLED", "true")
            _set_var(
                _env, "AIRFLOW__CORE__INTERNAL_API_SECRET_KEY", b64encode(os.urandom(16)).decode("utf-8")
            )

            # For testing Edge Worker on Windows... Default Run ID is having a colon (":") from the time which is
            # made into the log path template, which then fails to be used in Windows. So we replace it with a dash
            _set_var(
                _env,
                "AIRFLOW__LOGGING__LOG_FILENAME_TEMPLATE",
                "dag_id={{ ti.dag_id }}/run_id={{ ti.run_id|replace(':', '-') }}/task_id={{ ti.task_id }}/"
                "{% if ti.map_index >= 0 %}map_index={{ ti.map_index }}/{% endif %}"
                "attempt={{ try_number|default(ti.try_number) }}.log",
            )

            port = 8080
            _set_var(_env, "AIRFLOW__EDGE__API_URL", f"http://localhost:{port}/edge_worker/v1/rpcapi")
        _set_var(_env, "ANSWER", get_forced_answer() or "")
        _set_var(_env, "ALLOW_PRE_RELEASES", self.allow_pre_releases)
        _set_var(_env, "BACKEND", self.backend)
        _set_var(_env, "BASE_BRANCH", self.base_branch, "main")
        _set_var(_env, "BREEZE", "true")
        _set_var(_env, "BREEZE_INIT_COMMAND", None, "")
        _set_var(_env, "CELERY_BROKER_URLS_MAP", CELERY_BROKER_URLS_MAP)
        _set_var(_env, "CELERY_FLOWER", self.celery_flower)
        _set_var(_env, "CLEAN_AIRFLOW_INSTALLATION", self.clean_airflow_installation)
        _set_var(_env, "CI", None, "false")
        _set_var(_env, "CI_BUILD_ID", None, "0")
        _set_var(_env, "CI_EVENT_TYPE", None, GithubEvents.PULL_REQUEST.value)
        _set_var(_env, "CI_JOB_ID", None, "0")
        _set_var(_env, "CI_TARGET_BRANCH", self.airflow_branch)
        _set_var(_env, "CI_TARGET_REPO", self.github_repository)
        _set_var(_env, "COLLECT_ONLY", self.collect_only)
        _set_var(_env, "CREATE_ALL_ROLES", self.create_all_roles)
        _set_var(_env, "COMMIT_SHA", None, commit_sha())
        _set_var(_env, "COMPOSE_FILE", self.compose_file)
        _set_var(_env, "DB_RESET", self.db_reset)
        _set_var(_env, "DEFAULT_BRANCH", self.airflow_branch)
        _set_var(_env, "DEFAULT_CONSTRAINTS_BRANCH", self.default_constraints_branch)
        _set_var(_env, "DEV_MODE", self.dev_mode)
        _set_var(_env, "DOCKER_IS_ROOTLESS", self.rootless_docker)
        _set_var(_env, "DOWNGRADE_SQLALCHEMY", self.downgrade_sqlalchemy)
        _set_var(_env, "DOWNGRADE_PENDULUM", self.downgrade_pendulum)
        _set_var(_env, "DRILL_HOST_PORT", None, DRILL_HOST_PORT)
        _set_var(_env, "ENABLE_COVERAGE", self.enable_coverage)
        _set_var(_env, "FLOWER_HOST_PORT", None, FLOWER_HOST_PORT)
        _set_var(_env, "GREMLIN_HOST_PORT", None, GREMLIN_HOST_PORT)
        _set_var(_env, "EXCLUDED_PROVIDERS", self.excluded_providers)
        _set_var(_env, "FORCE_LOWEST_DEPENDENCIES", self.force_lowest_dependencies)
        _set_var(_env, "SQLALCHEMY_WARN_20", self.force_sa_warnings)
        _set_var(_env, "GITHUB_ACTIONS", self.github_actions)
        _set_var(_env, "GITHUB_TOKEN", self.github_token)
        _set_var(_env, "HOST_GROUP_ID", self.host_group_id)
        _set_var(_env, "HOST_OS", self.host_os)
        _set_var(_env, "HOST_USER_ID", self.host_user_id)
        _set_var(_env, "INIT_SCRIPT_FILE", None, "init.sh")
        _set_var(_env, "INSTALL_AIRFLOW_WITH_CONSTRAINTS", self.install_airflow_with_constraints)
        _set_var(_env, "INSTALL_AIRFLOW_PYTHON_CLIENT", self.install_airflow_python_client)
        _set_var(_env, "INSTALL_AIRFLOW_VERSION", self.install_airflow_version)
        _set_var(_env, "INSTALL_SELECTED_PROVIDERS", self.install_selected_providers)
        _set_var(_env, "ISSUE_ID", self.issue_id)
        _set_var(_env, "LOAD_DEFAULT_CONNECTIONS", self.load_default_connections)
        _set_var(_env, "LOAD_EXAMPLES", self.load_example_dags)
        _set_var(_env, "MSSQL_HOST_PORT", None, MSSQL_HOST_PORT)
        _set_var(_env, "MYSQL_HOST_PORT", None, MYSQL_HOST_PORT)
        _set_var(_env, "MYSQL_VERSION", self.mysql_version)
        _set_var(_env, "MOUNT_SOURCES", self.mount_sources)
        _set_var(_env, "MOUNT_UI_DIST", self.mount_ui_dist)
        _set_var(_env, "NUM_RUNS", self.num_runs)
        _set_var(_env, "ONLY_MIN_VERSION_UPDATE", self.only_min_version_update)
        _set_var(_env, "DISTRIBUTION_FORMAT", self.distribution_format)
        _set_var(_env, "POSTGRES_HOST_PORT", None, POSTGRES_HOST_PORT)
        _set_var(_env, "POSTGRES_VERSION", self.postgres_version)
        _set_var(_env, "PROVIDERS_CONSTRAINTS_LOCATION", self.providers_constraints_location)
        _set_var(_env, "PROVIDERS_CONSTRAINTS_MODE", self.providers_constraints_mode)
        _set_var(_env, "PROVIDERS_CONSTRAINTS_REFERENCE", self.providers_constraints_reference)
        _set_var(_env, "PROVIDERS_SKIP_CONSTRAINTS", self.providers_skip_constraints)
        _set_var(_env, "PYTHONDONTWRITEBYTECODE", "true")
        _set_var(_env, "PYTHONWARNINGS", None, None)
        _set_var(_env, "PYTHON_MAJOR_MINOR_VERSION", self.python)
        _set_var(_env, "QUIET", self.quiet)
        _set_var(_env, "REDIS_HOST_PORT", None, REDIS_HOST_PORT)
        _set_var(_env, "RABBITMQ_HOST_PORT", None, RABBITMQ_HOST_PORT)
        _set_var(_env, "REGENERATE_MISSING_DOCS", self.regenerate_missing_docs)
        _set_var(_env, "RUN_TESTS", self.run_tests)
        _set_var(_env, "SKIP_ENVIRONMENT_INITIALIZATION", self.skip_environment_initialization)
        _set_var(_env, "SKIP_SSH_SETUP", self.skip_ssh_setup)
        _set_var(_env, "SQLITE_URL", self.sqlite_url)
        _set_var(_env, "SSH_PORT", None, SSH_PORT)
        _set_var(_env, "STANDALONE_DAG_PROCESSOR", self.standalone_dag_processor)
        _set_var(_env, "START_AIRFLOW", self.start_airflow)
        _set_var(_env, "USE_MPROCS", self.use_mprocs)
        _set_var(_env, "SUSPENDED_PROVIDERS_FOLDERS", self.suspended_providers_folders)
        _set_var(
            _env,
            "START_API_SERVER_WITH_EXAMPLES",
            self.start_api_server_with_examples,
        )
        _set_var(_env, "SYSTEM_TESTS_ENV_ID", None, "")
        _set_var(_env, "TEST_TYPE", self.test_type, "")
        _set_var(_env, "TEST_GROUP", str(self.test_group.value) if self.test_group else "")
        _set_var(_env, "UPGRADE_BOTO", self.upgrade_boto)
        _set_var(_env, "UPGRADE_SQLALCHEMY", self.upgrade_sqlalchemy)
        _set_var(_env, "USE_AIRFLOW_VERSION", self.use_airflow_version, "")
        _set_var(_env, "USE_DISTRIBUTIONS_FROM_DIST", self.use_distributions_from_dist)
        _set_var(_env, "USE_UV", self.use_uv)
        _set_var(_env, "USE_XDIST", self.use_xdist)
        _set_var(_env, "VERBOSE", get_verbose())
        _set_var(_env, "VERBOSE_COMMANDS", self.verbose_commands)
        _set_var(_env, "VERSION_SUFFIX", self.version_suffix)
        _set_var(_env, "WEB_HOST_PORT", None, WEB_HOST_PORT)
        _set_var(_env, "_AIRFLOW_RUN_DB_TESTS_ONLY", self.run_db_tests_only)
        _set_var(_env, "_AIRFLOW_SKIP_DB_TESTS", self.skip_db_tests)

        self._set_debug_variables(_env)
        self._generate_env_for_docker_compose_file_if_needed(_env)

        _target_env: dict[str, str] = os.environ.copy()
        _target_env.update(_env)
        return _target_env

    def _set_debug_variables(self, env) -> None:
        """Set debug environment variables based on selected debug components."""
        if self.debugger == "pydevd-pycharm":
            print("Pycharm-pydevd debugger is under development and not yet supported in Breeze.")
            return
        if not self.debug_components:
            return

        _set_var(env, "BREEZE_DEBUG_SCHEDULER_PORT", None, BREEZE_DEBUG_SCHEDULER_PORT)
        _set_var(env, "BREEZE_DEBUG_DAG_PROCESSOR_PORT", None, BREEZE_DEBUG_DAG_PROCESSOR_PORT)
        _set_var(env, "BREEZE_DEBUG_TRIGGERER_PORT", None, BREEZE_DEBUG_TRIGGERER_PORT)
        _set_var(env, "BREEZE_DEBUG_APISERVER_PORT", None, BREEZE_DEBUG_APISERVER_PORT)
        _set_var(env, "BREEZE_DEBUG_CELERY_WORKER_PORT", None, BREEZE_DEBUG_CELERY_WORKER_PORT)
        _set_var(env, "BREEZE_DEBUG_EDGE_PORT", None, BREEZE_DEBUG_EDGE_PORT)
        _set_var(env, "BREEZE_DEBUG_WEBSERVER_PORT", None, BREEZE_DEBUG_WEBSERVER_PORT)

        _set_var(env, "BREEZE_DEBUGGER", None, self.debugger)

        component_mappings = {
            "scheduler": "BREEZE_DEBUG_SCHEDULER",
            "triggerer": "BREEZE_DEBUG_TRIGGERER",
            "api-server": "BREEZE_DEBUG_APISERVER",
            "dag-processor": "BREEZE_DEBUG_DAG_PROCESSOR",
            "edge-worker": "BREEZE_DEBUG_EDGE",
            "celery-worker": "BREEZE_DEBUG_CELERY_WORKER",
        }

        for component in self.debug_components:
            env_var = component_mappings[component]
            _set_var(env, env_var, None, "true")

    @staticmethod
    def _generate_env_for_docker_compose_file_if_needed(env: dict[str, str]):
        """
        Generates docker-compose env file if needed.

        :param env: dictionary of env variables to use for docker-compose and docker env files.

        Writes env files for docker and docker compose to make sure the envs will be passed
        to docker-compose/docker when running commands.

        The list of variables might change over time, and we want to keep the list updated only in
        one place (above env_variables_for_docker_commands method). So we need to regenerate the env
        files automatically when new variable is added to the list or removed.

        Docker-Compose based tests can start in parallel, so we want to make sure we generate it once
        per invocation of breeze command otherwise there could be nasty race condition that
        the file would be empty while another compose tries to use it when starting.

        Also, it means that we need to pass the values through environment rather than writing
        them to the file directly, because they might differ between different parallel runs
        of compose or docker.

        Unfortunately docker and docker-compose do not share the same env files any more as of Compose V2
        format for passing variables from the environment, so we need to generate both files.
        Documentation is a bit vague about this.

        The docker file contain simply list of all variables that should be passed to docker.

        See https://docs.docker.com/engine/reference/commandline/run/#env

        > When running the command, the Docker CLI client checks the value the variable has in
        > your local environment and passes it to the container. If no = is provided and that
        > variable is not exported in your local environment, the variable isn't set in the container.

        The docker-compose file should instead contain VARIABLE=${VARIABLE} for each variable
        that should be passed to docker compose.

        From https://docs.docker.com/compose/compose-file/05-services/#env_file

        > VAL may be omitted, in such cases the variable value is an empty string. =VAL may be omitted,
        > in such cases the variable is unset.

        """
        from filelock import FileLock

        with FileLock(GENERATED_DOCKER_LOCK_PATH):
            if GENERATED_DOCKER_ENV_PATH.exists():
                generated_keys = GENERATED_DOCKER_ENV_PATH.read_text().splitlines()
                if set(env.keys()) == set(generated_keys):
                    # we check if the set of env variables had not changed since last run
                    # if so - cool, we do not need to do anything else
                    return
                if get_verbose():
                    get_console().print(
                        f"[info]The keys has changed vs last run. Regenerating[/]: "
                        f"{GENERATED_DOCKER_ENV_PATH} and {GENERATED_DOCKER_COMPOSE_ENV_PATH}"
                    )
            if get_verbose():
                get_console().print(f"[info]Generating new docker env file [/]: {GENERATED_DOCKER_ENV_PATH}")
            GENERATED_DOCKER_ENV_PATH.write_text("\n".join(sorted(env.keys())))
            if get_verbose():
                get_console().print(
                    f"[info]Generating new docker compose env file [/]: {GENERATED_DOCKER_COMPOSE_ENV_PATH}"
                )
            GENERATED_DOCKER_COMPOSE_ENV_PATH.write_text(
                "\n".join([f"{k}=${{{k}}}" for k in sorted(env.keys())])
            )

    def __post_init__(self):
        if self.airflow_constraints_reference == "default":
            self.airflow_constraints_reference = self.default_constraints_branch
        if self.providers_constraints_reference == "default":
            self.providers_constraints_reference = self.default_constraints_branch

        if (
            self.backend
            and self.integration
            and KEYCLOAK_INTEGRATION in self.integration
            and not self.backend == POSTGRES_BACKEND
        ):
            get_console().print(
                "[error]When using the Keycloak integration the backend must be Postgres![/]\n"
            )
            sys.exit(2)

    def __eq__(self, other) -> bool:
        if not isinstance(other, ShellParams):
            return False
        return self.__dict__ == other.__dict__

    def __hash__(self) -> int:
        return hash(str(self.__dict__))
