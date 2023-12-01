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
from dataclasses import dataclass, field
from functools import cached_property
from os import _Environ
from pathlib import Path

from airflow_breeze.branch_defaults import AIRFLOW_BRANCH, DEFAULT_AIRFLOW_CONSTRAINTS_BRANCH
from airflow_breeze.global_constants import (
    ALL_INTEGRATIONS,
    ALLOWED_BACKENDS,
    ALLOWED_CONSTRAINTS_MODES_CI,
    ALLOWED_DOCKER_COMPOSE_PROJECTS,
    ALLOWED_INSTALLATION_PACKAGE_FORMATS,
    ALLOWED_MSSQL_VERSIONS,
    ALLOWED_MYSQL_VERSIONS,
    ALLOWED_POSTGRES_VERSIONS,
    ALLOWED_PYTHON_MAJOR_MINOR_VERSIONS,
    APACHE_AIRFLOW_GITHUB_REPOSITORY,
    CELERY_BROKER_URLS_MAP,
    DEFAULT_CELERY_BROKER,
    DOCKER_DEFAULT_PLATFORM,
    FLOWER_HOST_PORT,
    MOUNT_ALL,
    MOUNT_REMOVE,
    MOUNT_SELECTED,
    MOUNT_SKIP,
    MSSQL_HOST_PORT,
    MYSQL_HOST_PORT,
    POSTGRES_HOST_PORT,
    REDIS_HOST_PORT,
    SSH_PORT,
    START_AIRFLOW_DEFAULT_ALLOWED_EXECUTOR,
    TESTABLE_INTEGRATIONS,
    WEBSERVER_HOST_PORT,
    get_airflow_version,
)
from airflow_breeze.utils.console import get_console
from airflow_breeze.utils.host_info_utils import get_host_group_id, get_host_os
from airflow_breeze.utils.packages import get_suspended_provider_folders
from airflow_breeze.utils.path_utils import (
    AIRFLOW_SOURCES_ROOT,
    BUILD_CACHE_DIR,
    GENERATED_DOCKER_COMPOSE_ENV_FILE,
    GENERATED_DOCKER_ENV_FILE,
    GENERATED_DOCKER_LOCK_FILE,
    MSSQL_TMP_DIR_NAME,
    SCRIPTS_CI_DIR,
)
from airflow_breeze.utils.run_tests import file_name_from_test_type
from airflow_breeze.utils.run_utils import commit_sha, get_filesystem_type, run_command
from airflow_breeze.utils.shared_options import get_forced_answer, get_verbose

DOCKER_COMPOSE_DIR = SCRIPTS_CI_DIR / "docker-compose"


def add_mssql_compose_file(compose_file_list: list[Path]):
    docker_filesystem = get_filesystem_type("/var/lib/docker")
    if docker_filesystem == "tmpfs":
        compose_file_list.append(DOCKER_COMPOSE_DIR / "backend-mssql-tmpfs-volume.yml")
    else:
        compose_file_list.append(DOCKER_COMPOSE_DIR / "backend-mssql-docker-volume.yml")


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

    airflow_branch: str = os.environ.get("DEFAULT_BRANCH", AIRFLOW_BRANCH)
    airflow_constraints_mode: str = ALLOWED_CONSTRAINTS_MODES_CI[0]
    airflow_constraints_reference: str = DEFAULT_AIRFLOW_CONSTRAINTS_BRANCH
    airflow_extras: str = ""
    backend: str = ALLOWED_BACKENDS[0]
    base_branch: str = "main"
    builder: str = "autodetect"
    celery_broker: str = DEFAULT_CELERY_BROKER
    celery_flower: bool = False
    chicken_egg_providers: str = ""
    collect_only: bool = False
    database_isolation: bool = False
    db_reset: bool = False
    default_constraints_branch: str = os.environ.get(
        "DEFAULT_CONSTRAINTS_BRANCH", DEFAULT_AIRFLOW_CONSTRAINTS_BRANCH
    )
    dev_mode: bool = False
    downgrade_sqlalchemy: bool = False
    dry_run: bool = False
    enable_coverage: bool = False
    executor: str = START_AIRFLOW_DEFAULT_ALLOWED_EXECUTOR
    extra_args: tuple = ()
    force_build: bool = False
    forward_credentials: str = "false"
    forward_ports: bool = True
    github_actions: str = os.environ.get("GITHUB_ACTIONS", "false")
    github_repository: str = APACHE_AIRFLOW_GITHUB_REPOSITORY
    github_token: str = os.environ.get("GITHUB_TOKEN", "")
    helm_test_package: str | None = None
    image_tag: str | None = None
    include_mypy_volume: bool = False
    install_airflow_version: str = ""
    install_providers_from_sources: bool = True
    install_selected_providers: str | None = None
    integration: tuple[str, ...] = ()
    issue_id: str = ""
    load_default_connections: bool = False
    load_example_dags: bool = False
    mount_sources: str = MOUNT_SELECTED
    mssql_version: str = ALLOWED_MSSQL_VERSIONS[0]
    mysql_version: str = ALLOWED_MYSQL_VERSIONS[0]
    num_runs: str = ""
    only_min_version_update: bool = False
    package_format: str = ALLOWED_INSTALLATION_PACKAGE_FORMATS[0]
    parallel_test_types_list: list[str] = field(default_factory=list)
    parallelism: int = 0
    platform: str = DOCKER_DEFAULT_PLATFORM
    postgres_version: str = ALLOWED_POSTGRES_VERSIONS[0]
    project_name: str = ALLOWED_DOCKER_COMPOSE_PROJECTS[0]
    python: str = ALLOWED_PYTHON_MAJOR_MINOR_VERSIONS[0]
    quiet: bool = False
    regenerate_missing_docs: bool = False
    remove_arm_packages: bool = False
    restart: bool = False
    run_db_tests_only: bool = False
    run_system_tests: bool = os.environ.get("RUN_SYSTEM_TESTS", "false") == "true"
    run_tests: bool = False
    skip_constraints: bool = False
    skip_db_tests: bool = False
    skip_environment_initialization: bool = False
    skip_image_upgrade_check: bool = False
    skip_provider_dependencies_check: bool = False
    skip_provider_tests: bool = False
    skip_ssh_setup: bool = os.environ.get("SKIP_SSH_SETUP", "false") == "true"
    standalone_dag_processor: bool = False
    start_airflow: str = "false"
    test_type: str | None = None
    tty: str = "auto"
    upgrade_boto: bool = False
    use_airflow_version: str | None = None
    use_packages_from_dist: bool = False
    use_xdist: bool = False
    verbose: bool = False
    verbose_commands: bool = False
    version_suffix_for_pypi: str = ""
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
    def airflow_image_name_with_tag(self) -> str:
        image = self.airflow_image_name
        return image if not self.image_tag else image + f":{self.image_tag}"

    @cached_property
    def airflow_image_kubernetes(self) -> str:
        image = f"{self.airflow_base_image_name}/{self.airflow_branch}/kubernetes/python{self.python}"
        return image

    @cached_property
    def airflow_sources(self):
        return AIRFLOW_SOURCES_ROOT

    @cached_property
    def image_type(self) -> str:
        return "CI"

    @cached_property
    def md5sum_cache_dir(self) -> Path:
        cache_dir = Path(BUILD_CACHE_DIR, self.airflow_branch, self.python, self.image_type)
        return cache_dir

    @cached_property
    def backend_version(self) -> str:
        version = ""
        if self.backend == "postgres":
            version = self.postgres_version
        if self.backend == "mysql":
            version = self.mysql_version
        if self.backend == "mssql":
            version = self.mssql_version
        return version

    @cached_property
    def sqlite_url(self) -> str:
        return "sqlite:////root/airflow/sqlite/airflow.db"

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
        if backend in ("sqlite", "none") or not self.forward_ports:
            return [backend_docker_compose_file]
        if self.project_name == "pre-commit":
            # do not forward ports for pre-commit runs - to not clash with running containers from
            # breeze
            return [backend_docker_compose_file]
        return [backend_docker_compose_file, DOCKER_COMPOSE_DIR / f"backend-{backend}-port.yml"]

    @cached_property
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

        if self.executor == "CeleryExecutor":
            compose_file_list.append(DOCKER_COMPOSE_DIR / "integration-celery.yml")

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
        if self.forward_ports and not self.project_name == "pre-commit":
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
        if "all-testable" in self.integration:
            integrations = TESTABLE_INTEGRATIONS
        elif "all" in self.integration:
            integrations = ALL_INTEGRATIONS
        else:
            integrations = self.integration
        for integration in integrations:
            compose_file_list.append(DOCKER_COMPOSE_DIR / f"integration-{integration}.yml")
        if "trino" in integrations and "kerberos" not in integrations:
            get_console().print(
                "[warning]Adding `kerberos` integration as it is implicitly needed by trino",
            )
            compose_file_list.append(DOCKER_COMPOSE_DIR / "integration-kerberos.yml")
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
                f"[warning]The broker {self.celery_broker} should "
                f"be one of {CELERY_BROKER_URLS_MAP.keys()}"
            )
            return ""
        # Map from short form (rabbitmq/redis) to actual urls
        return broker_url

    @cached_property
    def suspended_providers_folders(self):
        return " ".join(get_suspended_provider_folders()).strip()

    @cached_property
    def mssql_data_volume(self) -> str:
        docker_filesystem = get_filesystem_type("/var/lib/docker")
        # Make sure the test type is not too long to be used as a volume name in docker-compose
        # The tmp directory in our self-hosted runners can be quite long, so we should limit the volume name
        volume_name = (
            "tmp-mssql-volume-" + file_name_from_test_type(self.test_type)[:20]
            if self.test_type
            else "tmp-mssql-volume"
        )
        if docker_filesystem == "tmpfs":
            return os.fspath(Path.home() / MSSQL_TMP_DIR_NAME / f"{volume_name}-{self.mssql_version}")
        else:
            # mssql_data_volume variable is only used in case of tmpfs
            return ""

    @cached_property
    def rootless_docker(self) -> bool:
        try:
            response = run_command(
                ["docker", "info", "-f", "{{println .SecurityOptions}}"],
                capture_output=True,
                check=False,
                text=True,
            )
            if response.returncode == 0 and "rootless" in response.stdout.strip():
                get_console().print("[info]Docker is running in rootless mode.[/]\n")
                return True
        except FileNotFoundError:
            # we ignore if docker is missing
            pass
        return False

    @cached_property
    def env_variables_for_docker_commands(self) -> _Environ:
        """
        Constructs environment variables needed by the docker-compose command, based on Shell parameters
        passed to it.

        This is the only place where you need to add environment variables if you want to pass them to
        docker or docker-compose.

        :return: dictionary of env variables to use for docker-compose and docker command
        """

        _env: dict[str, str] = {}
        _set_var(_env, "AIRFLOW_CI_IMAGE", self.airflow_image_name)
        _set_var(_env, "AIRFLOW_CI_IMAGE_WITH_TAG", self.airflow_image_name_with_tag)
        _set_var(
            _env, "AIRFLOW_CONSTRAINTS_MODE", self.airflow_constraints_mode, "constraints-source-providers"
        )
        _set_var(
            _env,
            "AIRFLOW_CONSTRAINTS_REFERENCE",
            self.airflow_constraints_reference,
            "constraints-source-providers",
        )
        _set_var(_env, "AIRFLOW_ENABLE_AIP_44", None, "true")
        _set_var(_env, "AIRFLOW_ENV", "development")
        _set_var(_env, "AIRFLOW_EXTRAS", self.airflow_extras)
        _set_var(_env, "AIRFLOW_IMAGE_KUBERNETES", self.airflow_image_kubernetes)
        _set_var(_env, "AIRFLOW_VERSION", self.airflow_version)
        _set_var(_env, "AIRFLOW__CELERY__BROKER_URL", self.airflow_celery_broker_url)
        _set_var(_env, "AIRFLOW__CORE__EXECUTOR", self.executor)
        _set_var(_env, "ANSWER", get_forced_answer() or "")
        _set_var(_env, "BACKEND", self.backend)
        _set_var(_env, "BASE_BRANCH", self.base_branch, "main")
        _set_var(_env, "BREEZE", "true")
        _set_var(_env, "BREEZE_INIT_COMMAND", None, "")
        _set_var(_env, "CELERY_FLOWER", self.celery_flower)
        _set_var(_env, "CHICKEN_EGG_PROVIDERS", self.chicken_egg_providers)
        _set_var(_env, "CI", None, "false")
        _set_var(_env, "CI_BUILD_ID", None, "0")
        _set_var(_env, "CI_EVENT_TYPE", None, "pull_request")
        _set_var(_env, "CI_JOB_ID", None, "0")
        _set_var(_env, "CI_TARGET_BRANCH", self.airflow_branch)
        _set_var(_env, "CI_TARGET_REPO", self.github_repository)
        _set_var(_env, "COLLECT_ONLY", self.collect_only)
        _set_var(_env, "COMMIT_SHA", None, commit_sha())
        _set_var(_env, "COMPOSE_FILE", self.compose_file)
        _set_var(_env, "DATABASE_ISOLATION", self.database_isolation)
        _set_var(_env, "DB_RESET", self.db_reset)
        _set_var(_env, "DEFAULT_BRANCH", self.airflow_branch)
        _set_var(_env, "DEFAULT_CONSTRAINTS_BRANCH", self.default_constraints_branch)
        _set_var(_env, "DEV_MODE", self.dev_mode)
        _set_var(_env, "DOCKER_IS_ROOTLESS", self.rootless_docker)
        _set_var(_env, "DOWNGRADE_SQLALCHEMY", self.downgrade_sqlalchemy)
        _set_var(_env, "ENABLED_SYSTEMS", None, "")
        _set_var(_env, "FLOWER_HOST_PORT", None, FLOWER_HOST_PORT)
        _set_var(_env, "GITHUB_ACTIONS", self.github_actions)
        _set_var(_env, "HELM_TEST_PACKAGE", self.helm_test_package, "")
        _set_var(_env, "HOST_GROUP_ID", self.host_group_id)
        _set_var(_env, "HOST_OS", self.host_os)
        _set_var(_env, "HOST_USER_ID", self.host_user_id)
        _set_var(_env, "INIT_SCRIPT_FILE", None, "init.sh")
        _set_var(_env, "INSTALL_AIRFLOW_VERSION", self.install_airflow_version)
        _set_var(_env, "INSTALL_PROVIDERS_FROM_SOURCES", self.install_providers_from_sources)
        _set_var(_env, "INSTALL_SELECTED_PROVIDERS", self.install_selected_providers)
        _set_var(_env, "ISSUE_ID", self.issue_id)
        _set_var(_env, "LOAD_DEFAULT_CONNECTIONS", self.load_default_connections)
        _set_var(_env, "LOAD_EXAMPLES", self.load_example_dags)
        _set_var(_env, "MSSQL_DATA_VOLUME", self.mssql_data_volume)
        _set_var(_env, "MSSQL_HOST_PORT", None, MSSQL_HOST_PORT)
        _set_var(_env, "MSSQL_VERSION", self.mssql_version)
        _set_var(_env, "MYSQL_HOST_PORT", None, MYSQL_HOST_PORT)
        _set_var(_env, "MYSQL_VERSION", self.mysql_version)
        _set_var(_env, "NUM_RUNS", self.num_runs)
        _set_var(_env, "ONLY_MIN_VERSION_UPDATE", self.only_min_version_update)
        _set_var(_env, "PACKAGE_FORMAT", self.package_format)
        _set_var(_env, "POSTGRES_HOST_PORT", None, POSTGRES_HOST_PORT)
        _set_var(_env, "POSTGRES_VERSION", self.postgres_version)
        _set_var(_env, "PYTHONDONTWRITEBYTECODE", "true")
        _set_var(_env, "PYTHONWARNINGS", None, None)
        _set_var(_env, "PYTHON_MAJOR_MINOR_VERSION", self.python)
        _set_var(_env, "QUIET", self.quiet)
        _set_var(_env, "REDIS_HOST_PORT", None, REDIS_HOST_PORT)
        _set_var(_env, "REGENERATE_MISSING_DOCS", self.regenerate_missing_docs)
        _set_var(_env, "REMOVE_ARM_PACKAGES", self.remove_arm_packages)
        _set_var(_env, "RUN_SYSTEM_TESTS", self.run_system_tests)
        _set_var(_env, "RUN_TESTS", self.run_tests)
        _set_var(_env, "SKIP_CONSTRAINTS", self.skip_constraints)
        _set_var(_env, "SKIP_ENVIRONMENT_INITIALIZATION", self.skip_environment_initialization)
        _set_var(_env, "SKIP_SSH_SETUP", self.skip_ssh_setup)
        _set_var(_env, "SQLITE_URL", self.sqlite_url)
        _set_var(_env, "SSH_PORT", None, SSH_PORT)
        _set_var(_env, "STANDALONE_DAG_PROCESSOR", self.standalone_dag_processor)
        _set_var(_env, "START_AIRFLOW", self.start_airflow)
        _set_var(_env, "SUSPENDED_PROVIDERS_FOLDERS", self.suspended_providers_folders)
        _set_var(_env, "TEST_TYPE", self.test_type, "")
        _set_var(_env, "UPGRADE_BOTO", self.upgrade_boto)
        _set_var(_env, "USE_AIRFLOW_VERSION", self.use_airflow_version, "")
        _set_var(_env, "USE_PACKAGES_FROM_DIST", self.use_packages_from_dist)
        _set_var(_env, "USE_XDIST", self.use_xdist)
        _set_var(_env, "VERBOSE", get_verbose())
        _set_var(_env, "VERBOSE_COMMANDS", self.verbose_commands)
        _set_var(_env, "VERSION_SUFFIX_FOR_PYPI", self.version_suffix_for_pypi)
        _set_var(_env, "WEBSERVER_HOST_PORT", None, WEBSERVER_HOST_PORT)
        _set_var(_env, "_AIRFLOW_RUN_DB_TESTS_ONLY", self.run_db_tests_only)
        _set_var(_env, "_AIRFLOW_SKIP_DB_TESTS", self.skip_db_tests)

        self._generate_env_for_docker_compose_file_if_needed(_env)

        target_environment = deepcopy(os.environ)
        target_environment.update(_env)
        return target_environment

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

        with FileLock(GENERATED_DOCKER_LOCK_FILE):
            if GENERATED_DOCKER_ENV_FILE.exists():
                generated_keys = GENERATED_DOCKER_ENV_FILE.read_text().splitlines()
                if set(env.keys()) == set(generated_keys):
                    # we check if the set of env variables had not changed since last run
                    # if so - cool, we do not need to do anything else
                    return
                else:
                    if get_verbose():
                        get_console().print(
                            f"[info]The keys has changed vs last run. Regenerating[/]: "
                            f"{GENERATED_DOCKER_ENV_FILE} and {GENERATED_DOCKER_COMPOSE_ENV_FILE}"
                        )
            if get_verbose():
                get_console().print(f"[info]Generating new docker env file [/]: {GENERATED_DOCKER_ENV_FILE}")
            GENERATED_DOCKER_ENV_FILE.write_text("\n".join(sorted(env.keys())))
            if get_verbose():
                get_console().print(
                    f"[info]Generating new docker compose env file [/]: {GENERATED_DOCKER_COMPOSE_ENV_FILE}"
                )
            GENERATED_DOCKER_COMPOSE_ENV_FILE.write_text(
                "\n".join([f"{k}=${{{k}}}" for k in sorted(env.keys())])
            )
