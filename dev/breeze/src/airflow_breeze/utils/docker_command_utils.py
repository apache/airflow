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
"""Various utils to prepare docker and docker compose commands."""
import os
import re
import subprocess
import sys
from random import randint
from typing import Dict, List, Tuple, Union

from airflow_breeze.build_image.ci.build_ci_params import BuildCiParams
from airflow_breeze.build_image.prod.build_prod_params import BuildProdParams
from airflow_breeze.shell.shell_params import ShellParams
from airflow_breeze.utils.host_info_utils import get_host_group_id, get_host_os, get_host_user_id
from airflow_breeze.utils.path_utils import AIRFLOW_SOURCES_ROOT
from airflow_breeze.utils.registry import login_to_docker_registry

try:
    from packaging import version
except ImportError:
    # We handle the ImportError so that autocomplete works with just click installed
    version = None  # type: ignore[assignment]

from airflow_breeze.branch_defaults import AIRFLOW_BRANCH, DEFAULT_AIRFLOW_CONSTRAINTS_BRANCH
from airflow_breeze.global_constants import (
    ALLOWED_PACKAGE_FORMATS,
    FLOWER_HOST_PORT,
    MIN_DOCKER_COMPOSE_VERSION,
    MIN_DOCKER_VERSION,
    MOUNT_ALL,
    MOUNT_SELECTED,
    MSSQL_HOST_PORT,
    MSSQL_VERSION,
    MYSQL_HOST_PORT,
    MYSQL_VERSION,
    POSTGRES_HOST_PORT,
    POSTGRES_VERSION,
    REDIS_HOST_PORT,
    SSH_PORT,
    WEBSERVER_HOST_PORT,
)
from airflow_breeze.utils.console import get_console
from airflow_breeze.utils.run_utils import commit_sha, prepare_build_command, run_command

NECESSARY_HOST_VOLUMES = [
    "/.bash_aliases:/root/.bash_aliases:cached",
    "/.bash_history:/root/.bash_history:cached",
    "/.coveragerc:/opt/airflow/.coveragerc:cached",
    "/.dockerignore:/opt/airflow/.dockerignore:cached",
    "/.flake8:/opt/airflow/.flake8:cached",
    "/.github:/opt/airflow/.github:cached",
    "/.inputrc:/root/.inputrc:cached",
    "/.rat-excludes:/opt/airflow/.rat-excludes:cached",
    "/RELEASE_NOTES.rst:/opt/airflow/RELEASE_NOTES.rst:cached",
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
    "/scripts/docker/entrypoint_ci.sh:/entrypoint:cached",
    "/setup.cfg:/opt/airflow/setup.cfg:cached",
    "/setup.py:/opt/airflow/setup.py:cached",
    "/tests:/opt/airflow/tests:cached",
    "/kubernetes_tests:/opt/airflow/kubernetes_tests:cached",
    "/docker_tests:/opt/airflow/docker_tests:cached",
    "/chart:/opt/airflow/chart:cached",
    "/metastore_browser:/opt/airflow/metastore_browser:cached",
]


def get_extra_docker_flags(mount_sources: str) -> List[str]:
    """
    Returns extra docker flags based on the type of mounting we want to do for sources.
    :param mount_sources: type of mounting we want to have
    :return: extra flag as list of strings
    """
    extra_docker_flags = []
    if mount_sources == MOUNT_ALL:
        extra_docker_flags.extend(["-v", f"{AIRFLOW_SOURCES_ROOT}:/opt/airflow/:cached"])
    elif mount_sources == MOUNT_SELECTED:
        for flag in NECESSARY_HOST_VOLUMES:
            extra_docker_flags.extend(["-v", str(AIRFLOW_SOURCES_ROOT) + flag])
    else:  # none
        get_console().print('[info]Skip mounting host volumes to Docker[/]')
    extra_docker_flags.extend(["-v", f"{AIRFLOW_SOURCES_ROOT}/files:/files"])
    extra_docker_flags.extend(["-v", f"{AIRFLOW_SOURCES_ROOT}/dist:/dist"])
    extra_docker_flags.extend(["--rm"])
    extra_docker_flags.extend(["--env-file", f"{AIRFLOW_SOURCES_ROOT}/scripts/ci/docker-compose/_docker.env"])
    return extra_docker_flags


def check_docker_resources(
    airflow_image_name: str, verbose: bool, dry_run: bool
) -> Union[subprocess.CompletedProcess, subprocess.CalledProcessError]:
    """
    Check if we have enough resources to run docker. This is done via running script embedded in our image.
    :param verbose: print commands when running
    :param dry_run: whether to run it in dry run mode
    :param airflow_image_name: name of the airflow image to use.
    """
    return run_command(
        cmd=[
            "docker",
            "run",
            "-t",
            "--entrypoint",
            "/bin/bash",
            "-e",
            "PYTHONDONTWRITEBYTECODE=true",
            airflow_image_name,
            "-c",
            "python /opt/airflow/scripts/in_container/run_resource_check.py",
        ],
        verbose=verbose,
        dry_run=dry_run,
        text=True,
    )


def check_docker_permission_denied(verbose) -> bool:
    """
    Checks if we have permission to write to docker socket. By default, on Linux you need to add your user
    to docker group and some new users do not realize that. We help those users if we have
    permission to run docker commands.

    :param verbose: print commands when running
    :return: True if permission is denied.
    """
    permission_denied = False
    docker_permission_command = ["docker", "info"]
    command_result = run_command(
        docker_permission_command,
        verbose=verbose,
        no_output_dump_on_exception=True,
        capture_output=True,
        text=True,
        check=False,
    )
    if command_result.returncode != 0:
        permission_denied = True
        if command_result.stdout and 'Got permission denied while trying to connect' in command_result.stdout:
            get_console().print(
                'ERROR: You have `permission denied` error when trying to communicate with docker.'
            )
            get_console().print(
                'Most likely you need to add your user to `docker` group: \
                https://docs.docker.com/ engine/install/linux-postinstall/ .'
            )
    return permission_denied


def compare_version(current_version: str, min_version: str) -> bool:
    return version.parse(current_version) >= version.parse(min_version)


def check_docker_is_running(verbose: bool):
    """
    Checks if docker is running. Suppressed Dockers stdout and stderr output.
    :param verbose: print commands when running
    """
    response = run_command(
        ["docker", "info"],
        verbose=verbose,
        no_output_dump_on_exception=True,
        text=False,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.STDOUT,
        check=False,
    )
    if response.returncode != 0:
        get_console().print(
            '[error]Docker is not running.[/]\n'
            '[warning]Please make sure Docker is installed and running.[/]'
        )
        sys.exit(1)


def check_docker_version(verbose: bool):
    """
    Checks if the docker compose version is as expected (including some specific modifications done by
    some vendors such as Microsoft (they might have modified version of docker-compose/docker in their
    cloud. In case docker compose version is wrong we continue but print warning for the user.


    :param verbose: print commands when running
    """
    permission_denied = check_docker_permission_denied(verbose)
    if not permission_denied:
        docker_version_command = ['docker', 'version', '--format', '{{.Client.Version}}']
        docker_version = ''
        docker_version_result = run_command(
            docker_version_command,
            verbose=verbose,
            no_output_dump_on_exception=True,
            capture_output=True,
            text=True,
            check=False,
        )
        if docker_version_result.returncode == 0:
            docker_version = docker_version_result.stdout.strip()
        if docker_version == '':
            get_console().print(
                f"""
[warning]Your version of docker is unknown. If the scripts fail, please make sure to[/]
[warning]install docker at least: {MIN_DOCKER_VERSION} version.[/]
"""
            )
        else:
            good_version = compare_version(docker_version, MIN_DOCKER_VERSION)
            if good_version:
                get_console().print(f'[success]Good version of Docker: {docker_version}.[/]')
            else:
                get_console().print(
                    f"""
[warning]Your version of docker is too old:{docker_version}.
Please upgrade to at least {MIN_DOCKER_VERSION}[/]
"""
                )


def check_docker_compose_version(verbose: bool):
    """
    Checks if the docker compose version is as expected (including some specific modifications done by
    some vendors such as Microsoft (they might have modified version of docker-compose/docker in their
    cloud. In case docker compose version is wrong we continue but print warning for the user.

    :param verbose: print commands when running
    """
    version_pattern = re.compile(r'(\d+)\.(\d+)\.(\d+)')
    docker_compose_version_command = ["docker-compose", "--version"]
    docker_compose_version_result = run_command(
        docker_compose_version_command,
        verbose=verbose,
        no_output_dump_on_exception=True,
        capture_output=True,
        text=True,
    )
    if docker_compose_version_result.returncode == 0:
        docker_compose_version = docker_compose_version_result.stdout
        version_extracted = version_pattern.search(docker_compose_version)
        if version_extracted is not None:
            version = '.'.join(version_extracted.groups())
            good_version = compare_version(version, MIN_DOCKER_COMPOSE_VERSION)
            if good_version:
                get_console().print(f'[success]Good version of docker-compose: {version}[/]')
            else:
                get_console().print(
                    f"""
[warning]You have too old version of docker-compose: {version}! At least 1.29 is needed! Please upgrade!
"""
                )
                get_console().print(
                    """
See https://docs.docker.com/compose/install/ for instructions.
Make sure docker-compose you install is first on the PATH variable of yours.
"""
                )
    else:
        get_console().print(
            """
[warning]Unknown docker-compose version. At least 1.29 is needed![/]
[warning]If Breeze fails upgrade to latest available docker-compose version.[/]
"""
        )


def construct_arguments_for_docker_build_command(
    image_params: Union[BuildCiParams, BuildProdParams], required_args: List[str], optional_args: List[str]
) -> List[str]:
    """
    Constructs docker compose command arguments list based on parameters passed. Maps arguments to
    argument values.

    It maps:
    * all the truthy/falsy values are converted to "true" / "false" respectively
    * if upgrade_to_newer_dependencies is set to True, it is replaced by a random string to account
      for the need of always triggering upgrade for docker build.

    :param image_params: parameters of the image
    :param required_args: build argument that are required
    :param optional_args: build arguments that are optional (should not be used if missing or empty)
    :return: list of `--build-arg` commands to use for the parameters passed
    """

    def get_env_variable_value(arg_name: str):
        value = str(getattr(image_params, arg_name))
        value = "true" if value.lower() in ["true", "t", "yes", "y"] else value
        value = "false" if value.lower() in ["false", "f", "no", "n"] else value
        if arg_name == "upgrade_to_newer_dependencies" and value == "true":
            value = f"{randint(0, 2**32):x}"
        return value

    args_command = []
    for required_arg in required_args:
        args_command.append("--build-arg")
        args_command.append(required_arg.upper() + "=" + get_env_variable_value(arg_name=required_arg))
    for optional_arg in optional_args:
        param_value = get_env_variable_value(optional_arg)
        if len(param_value) > 0:
            args_command.append("--build-arg")
            args_command.append(optional_arg.upper() + "=" + param_value)
    args_command.extend(image_params.docker_cache_directive)
    return args_command


def construct_docker_build_command(
    image_params: Union[BuildProdParams, BuildCiParams],
    verbose: bool,
    required_args: List[str],
    optional_args: List[str],
    production_image: bool,
) -> List[str]:
    """
    Constructs docker build command based on the parameters passed.
    :param image_params: parameters of the image
    :param verbose: print commands when running
    :param required_args: build argument that are required
    :param optional_args: build arguments that are optional (should not be used if missing or empty)
    :param production_image: whether this is production image or ci image
    :return: Command to run as list of string
    """
    arguments = construct_arguments_for_docker_build_command(
        image_params, required_args=required_args, optional_args=optional_args
    )
    build_command = prepare_build_command(
        prepare_buildx_cache=image_params.prepare_buildx_cache, verbose=verbose
    )
    build_flags = image_params.extra_docker_build_flags
    final_command = []
    final_command.extend(["docker"])
    final_command.extend(build_command)
    final_command.extend(build_flags)
    final_command.extend(["--pull"])
    final_command.extend(arguments)
    final_command.extend(["-t", image_params.airflow_image_name, "--target", "main", "."])
    final_command.extend(["-f", 'Dockerfile' if production_image else 'Dockerfile.ci'])
    final_command.extend(["--platform", image_params.platform])
    return final_command


def construct_docker_tag_command(
    image_params: Union[BuildProdParams, BuildCiParams],
) -> List[str]:
    """
    Constructs docker tag command based on the parameters passed.
    :param image_params: parameters of the image
    :return: Command to run as list of string
    """
    return ["docker", "tag", image_params.airflow_image_name, image_params.airflow_image_name_with_tag]


def construct_docker_push_command(
    image_params: Union[BuildProdParams, BuildCiParams],
) -> List[str]:
    """
    Constructs docker push command based on the parameters passed.
    :param image_params: parameters of the image
    :return: Command to run as list of string
    """
    return ["docker", "push", image_params.airflow_image_name_with_tag]


def tag_and_push_image(
    image_params: Union[BuildProdParams, BuildCiParams], dry_run: bool, verbose: bool
) -> Tuple[int, str]:
    """
    Tag and push the image according to parameters.
    :param image_params: parameters of the image
    :param dry_run: whether we are in dry-run mode
    :param verbose: whethere we produce verbose output
    :return:
    """
    get_console().print(
        f"[info]Tagging and pushing the {image_params.airflow_image_name} as "
        f"{image_params.airflow_image_name_with_tag}.[/]"
    )
    cmd = construct_docker_tag_command(image_params)
    command_result = run_command(
        cmd, verbose=verbose, dry_run=dry_run, cwd=AIRFLOW_SOURCES_ROOT, text=True, check=False
    )
    if command_result.returncode == 0:
        return_code, info = login_to_docker_registry(image_params, dry_run=dry_run)
        if return_code != 0:
            return return_code, f"Tag and pushing the image {image_params.python}: {info}"
        cmd = construct_docker_push_command(image_params)
        command_result = run_command(
            cmd, verbose=verbose, dry_run=dry_run, cwd=AIRFLOW_SOURCES_ROOT, text=True, check=False
        )
        if command_result.returncode == 0:
            return 0, f"Tag and pushing the image {image_params.python}"
    return command_result.returncode, f"Tag and pushing the image {image_params.python}"


def construct_empty_docker_build_command(
    image_params: Union[BuildProdParams, BuildCiParams],
) -> List[str]:
    """
    Constructs docker build empty image command based on the parameters passed.
    :param image_params: parameters of the image
    :return: Command to run as list of string
    """
    return ["docker", "build", "-t", image_params.airflow_image_name_with_tag, "-"]


def set_value_to_default_if_not_set(env: Dict[str, str], name: str, default: str):
    """
    Set value of name parameter to default (indexed by name) if not set.
    :param env: dictionary where to set the parameter
    :param name: name of parameter
    :param default: default value
    :return:
    """
    if env.get(name) is None:
        env[name] = os.environ.get(name, default)


def update_expected_environment_variables(env: Dict[str, str]) -> None:
    """
    Updates default values for unset environment variables.

    :param env: environment variables to update with missing values if not set.
    """
    set_value_to_default_if_not_set(env, 'ANSWER', "")
    set_value_to_default_if_not_set(env, 'AIRFLOW_EXTRAS', "")
    set_value_to_default_if_not_set(env, 'BREEZE', "true")
    set_value_to_default_if_not_set(env, 'CI', "false")
    set_value_to_default_if_not_set(env, 'CI_BUILD_ID', "0")
    set_value_to_default_if_not_set(env, 'CI_EVENT_TYPE', "pull_request")
    set_value_to_default_if_not_set(env, 'CI_JOB_ID', "0")
    set_value_to_default_if_not_set(env, 'CI_TARGET_BRANCH', AIRFLOW_BRANCH)
    set_value_to_default_if_not_set(env, 'CI_TARGET_REPO', "apache/airflow")
    set_value_to_default_if_not_set(env, 'COMMIT_SHA', commit_sha())
    set_value_to_default_if_not_set(env, 'DB_RESET', "false")
    set_value_to_default_if_not_set(env, 'DEBIAN_VERSION', "bullseye")
    set_value_to_default_if_not_set(env, 'DEFAULT_BRANCH', AIRFLOW_BRANCH)
    set_value_to_default_if_not_set(env, 'DEFAULT_CONSTRAINTS_BRANCH', DEFAULT_AIRFLOW_CONSTRAINTS_BRANCH)
    set_value_to_default_if_not_set(env, 'ENABLED_SYSTEMS', "")
    set_value_to_default_if_not_set(env, 'ENABLE_TEST_COVERAGE', "false")
    set_value_to_default_if_not_set(env, 'GENERATE_CONSTRAINTS_MODE', "source-providers")
    set_value_to_default_if_not_set(env, 'GITHUB_REGISTRY_PULL_IMAGE_TAG', "latest")
    set_value_to_default_if_not_set(env, 'HOST_OS', get_host_os())
    set_value_to_default_if_not_set(env, 'HOST_USER_ID', get_host_user_id())
    set_value_to_default_if_not_set(env, 'HOST_GROUP_ID', get_host_group_id())
    set_value_to_default_if_not_set(env, 'INIT_SCRIPT_FILE', "init.sh")
    set_value_to_default_if_not_set(env, 'INSTALL_PROVIDERS_FROM_SOURCES', "true")
    set_value_to_default_if_not_set(env, 'INSTALL_PACKAGES_FROM_CONTEXT', "false")
    set_value_to_default_if_not_set(env, 'LIST_OF_INTEGRATION_TESTS_TO_RUN', "")
    set_value_to_default_if_not_set(env, 'LOAD_DEFAULT_CONNECTIONS', "false")
    set_value_to_default_if_not_set(env, 'LOAD_EXAMPLES', "false")
    set_value_to_default_if_not_set(env, 'PACKAGE_FORMAT', ALLOWED_PACKAGE_FORMATS[0])
    set_value_to_default_if_not_set(env, 'PRINT_INFO_FROM_SCRIPTS', "true")
    set_value_to_default_if_not_set(env, 'PYTHONDONTWRITEBYTECODE', "true")
    set_value_to_default_if_not_set(env, 'RUN_SYSTEM_TESTS', "false")
    set_value_to_default_if_not_set(env, 'RUN_TESTS', "false")
    set_value_to_default_if_not_set(env, 'SKIP_ENVIRONMENT_INITIALIZATION', "false")
    set_value_to_default_if_not_set(env, 'SKIP_SSH_SETUP', "false")
    set_value_to_default_if_not_set(env, 'TEST_TYPE', "")
    set_value_to_default_if_not_set(env, 'UPGRADE_TO_NEWER_DEPENDENCIES', "false")
    set_value_to_default_if_not_set(env, 'USE_PACKAGES_FROM_DIST', "false")
    set_value_to_default_if_not_set(env, 'VERBOSE', "false")
    set_value_to_default_if_not_set(env, 'VERBOSE_COMMANDS', "false")
    set_value_to_default_if_not_set(env, 'VERSION_SUFFIX_FOR_PYPI', "")
    set_value_to_default_if_not_set(env, 'WHEEL_VERSION', "0.36.2")


MAP_ENV_VARIABLES_TO_PARAMS_FIELDS = {
    "AIRFLOW_CI_IMAGE": "airflow_image_name",
    "AIRFLOW_CI_IMAGE_WITH_TAG": "airflow_image_name_with_tag",
    "AIRFLOW_IMAGE_KUBERNETES": "airflow_image_kubernetes",
    "AIRFLOW_PROD_IMAGE": "airflow_image_name",
    "AIRFLOW_SOURCES": "airflow_sources",
    "AIRFLOW_VERSION": "airflow_version",
    "ANSWER": "answer",
    "BACKEND": "backend",
    "COMPOSE_FILE": "compose_files",
    "DB_RESET": 'db_reset',
    "ENABLED_INTEGRATIONS": "enabled_integrations",
    "GITHUB_ACTIONS": "github_actions",
    "HOST_GROUP_ID": "host_group_id",
    "HOST_USER_ID": "host_user_id",
    "INSTALL_AIRFLOW_VERSION": "install_airflow_version",
    "ISSUE_ID": "issue_id",
    "LOAD_EXAMPLES": "load_example_dags",
    "LOAD_DEFAULT_CONNECTIONS": "load_default_connections",
    "NUM_RUNS": "num_runs",
    "PYTHON_MAJOR_MINOR_VERSION": "python",
    "SKIP_PACKAGE_VERIFICATION": "skip_package_verification",
    "SQLITE_URL": "sqlite_url",
    "START_AIRFLOW": "start_airflow",
    "USE_AIRFLOW_VERSION": "use_airflow_version",
}

VARIABLES_FOR_DOCKER_COMPOSE_CONSTANTS = {
    "FLOWER_HOST_PORT": FLOWER_HOST_PORT,
    "MSSQL_HOST_PORT": MSSQL_HOST_PORT,
    "MSSQL_VERSION": MSSQL_VERSION,
    "MYSQL_HOST_PORT": MYSQL_HOST_PORT,
    "MYSQL_VERSION": MYSQL_VERSION,
    "POSTGRES_HOST_PORT": POSTGRES_HOST_PORT,
    "POSTGRES_VERSION": POSTGRES_VERSION,
    "REDIS_HOST_PORT": REDIS_HOST_PORT,
    "SSH_PORT": SSH_PORT,
    "WEBSERVER_HOST_PORT": WEBSERVER_HOST_PORT,
}

VARIABLES_IN_CACHE = {
    'backend': 'BACKEND',
    'mssql_version': 'MSSQL_VERSION',
    'mysql_version': 'MYSQL_VERSION',
    'postgres_version': 'POSTGRES_VERSION',
    'python': 'PYTHON_MAJOR_MINOR_VERSION',
}

SOURCE_OF_DEFAULT_VALUES_FOR_VARIABLES = {
    'backend': 'DEFAULT_BACKEND',
    'mssql_version': 'MSSQL_VERSION',
    'mysql_version': 'MYSQL_VERSION',
    'postgres_version': 'POSTGRES_VERSION',
    'python': 'DEFAULT_PYTHON_MAJOR_MINOR_VERSION',
}


def construct_env_variables_docker_compose_command(
    params: Union[ShellParams, BuildCiParams]
) -> Dict[str, str]:
    """
    Constructs environment variables needed by the docker-compose command, based on Shell parameters
    passed to it.

    * It checks if appropriate params are defined for all the needed docker compose environment variables
    * It sets the environment values from the parameters passed
    * For the constant parameters that we do not have parameters for, we only override the constant values
      if the env variable that we run with does not have it.
    * Updates all other environment variables that docker-compose expects with default values if missing

    :param params: shell parameters passed
    :return: dictionary of env variables to set
    """
    env_variables: Dict[str, str] = os.environ.copy()
    for variable in MAP_ENV_VARIABLES_TO_PARAMS_FIELDS:
        param_name = MAP_ENV_VARIABLES_TO_PARAMS_FIELDS[variable]
        param_value = getattr(params, param_name, None)
        env_variables[variable] = str(param_value) if param_value is not None else ""
    # Set constant defaults if not defined
    for variable in VARIABLES_FOR_DOCKER_COMPOSE_CONSTANTS:
        constant_param_value = VARIABLES_FOR_DOCKER_COMPOSE_CONSTANTS[variable]
        if not env_variables.get(constant_param_value):
            env_variables[variable] = str(constant_param_value)
    update_expected_environment_variables(env_variables)
    return env_variables
