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
import contextlib
import sys
from typing import Dict, List

from airflow_breeze.cache import check_cache_and_write_if_not_cached, write_to_cache_file
from airflow_breeze.console import console
from airflow_breeze.prod.prod_params import ProdParams
from airflow_breeze.utils.path_utils import AIRFLOW_SOURCE, DOCKER_CONTEXT_DIR
from airflow_breeze.utils.run_utils import filter_out_none, run_command

PARAMS_PROD_IMAGE = [
    "python_base_image",
    "install_mysql_client",
    "install_mssql_client",
    "install_postgres_client",
    "airflow_version",
    "airflow_branch",
    "airflow_extras",
    "airflow_pre_cached_pip_packages",
    "docker_context_files",
    "additional_airflow_extras",
    "additional_python_deps",
    "additional_dev_apt_command",
    "additional_dev_apt_deps",
    "additional_dev_apt_env",
    "additional_runtime_apt_command",
    "additional_runtime_apt_deps",
    "additional_runtime_apt_env",
    "upgrade_to_newer_dependencies",
    "constraints_github_repository",
    "airflow_constraints",
    "airflow_image_repository",
    "airflow_image_date_created",
    "build_id",
    "commit_sha",
    "airflow_image_readme_url",
    "install_providers_from_sources",
    "install_from_pypi",
    "install_from_docker_context_files",
]

PARAMS_TO_VERIFY_PROD_IMAGE = [
    "dev_apt_command",
    "dev_apt_deps",
    "runtime_apt_command",
    "runtime_apt_deps",
]


def construct_arguments_docker_command(prod_image: ProdParams) -> List[str]:
    args_command = []
    for param in PARAMS_PROD_IMAGE:
        args_command.append("--build-arg")
        args_command.append(param.upper() + "=" + str(getattr(prod_image, param)))
    for verify_param in PARAMS_TO_VERIFY_PROD_IMAGE:
        param_value = str(getattr(prod_image, verify_param))
        if len(param_value) > 0:
            args_command.append("--build-arg")
            args_command.append(verify_param.upper() + "=" + param_value)
    docker_cache = prod_image.docker_cache_prod_directive
    if len(docker_cache) > 0:
        args_command.extend(prod_image.docker_cache_prod_directive)
    return args_command


def construct_docker_command(prod_image: ProdParams) -> List[str]:
    arguments = construct_arguments_docker_command(prod_image)
    build_command = prod_image.check_buildx_plugin_build_command()
    build_flags = prod_image.extra_docker_build_flags
    final_command = []
    final_command.extend(["docker"])
    final_command.extend(build_command)
    final_command.extend(build_flags)
    final_command.extend(["--pull"])
    final_command.extend(arguments)
    final_command.extend(["-t", prod_image.airflow_prod_image_name, "--target", "main", "."])
    final_command.extend(["-f", 'Dockerfile'])
    final_command.extend(["--platform", prod_image.platform])
    return final_command


def login_to_docker_registry(build_params: ProdParams):
    if build_params.ci == "true":
        if len(build_params.github_token) == 0:
            console.print("\n[blue]Skip logging in to GitHub Registry. No Token available!")
        elif build_params.airflow_login_to_github_registry != "true":
            console.print(
                "\n[blue]Skip logging in to GitHub Registry.\
                    AIRFLOW_LOGIN_TO_GITHUB_REGISTRY is set as false"
            )
        elif len(build_params.github_token) > 0:
            run_command(['docker', 'logout', 'ghcr.io'], verbose=True, text=True)
            run_command(
                [
                    'docker',
                    'login',
                    '--username',
                    build_params.github_username,
                    '--password-stdin',
                    'ghcr.io',
                ],
                verbose=True,
                text=True,
                input=build_params.github_token,
            )
        else:
            console.print('\n[blue]Skip Login to GitHub Container Registry as token is missing')


def clean_docker_context_files():
    with contextlib.suppress(FileNotFoundError):
        context_files_to_delete = DOCKER_CONTEXT_DIR.glob('**/*')
        for file_to_delete in context_files_to_delete:
            if file_to_delete.name != 'README.md':
                file_to_delete.unlink()


def check_docker_context_files(install_from_docker_context_files: bool):
    context_file = DOCKER_CONTEXT_DIR.glob('**/*')
    number_of_context_files = len(
        [context for context in context_file if context.is_file() and context.name != 'README.md']
    )
    if number_of_context_files == 0:
        if install_from_docker_context_files:
            console.print('[bright_yellow]\nERROR! You want to install packages from docker-context-files')
            console.print('[bright_yellow]\n but there are no packages to install in this folder.')
            sys.exit()
    else:
        if not install_from_docker_context_files:
            console.print(
                '[bright_yellow]\n ERROR! There are some extra files in docker-context-files except README.md'
            )
            console.print('[bright_yellow]\nAnd you did not choose --install-from-docker-context-files flag')
            console.print(
                '[bright_yellow]\nThis might result in unnecessary cache invalidation and long build times'
            )
            console.print(
                '[bright_yellow]\nExiting now \
                    - please restart the command with --cleanup-docker-context-files switch'
            )
            sys.exit()


def build_production_image(verbose, **kwargs):
    parameters_passed = filter_out_none(**kwargs)
    prod_params = get_image_build_params(parameters_passed)
    prod_params.print_info()
    if prod_params.cleanup_docker_context_files:
        clean_docker_context_files()
    check_docker_context_files(prod_params.install_docker_context_files)
    if prod_params.skip_building_prod_image:
        console.print('[bright_yellow]\nSkip building production image. Assume the one we have is good!')
        console.print('bright_yellow]\nYou must run Breeze2 build-prod-image before for all python versions!')
    if prod_params.prepare_buildx_cache:
        login_to_docker_registry(prod_params)

    cmd = construct_docker_command(prod_params)
    print(cmd)
    output = run_command(cmd, verbose=verbose, cwd=AIRFLOW_SOURCE, text=True)
    console.print(f"[blue]{output}")
    if prod_params.prepare_buildx_cache:
        run_command(['docker', 'push', prod_params.airflow_prod_image_name], verbose=True, text=True)


def get_image_build_params(parameters_passed: Dict[str, str]):
    cacheable_parameters = {"python_version": 'PYTHON_MAJOR_MINOR_VERSION'}
    prod_image_params = ProdParams(**parameters_passed)
    for parameter, cache_key in cacheable_parameters.items():
        value_from_parameter = parameters_passed.get(parameter)
        if value_from_parameter:
            write_to_cache_file(cache_key, value_from_parameter, check_allowed_values=True)
            setattr(prod_image_params, parameter, value_from_parameter)
        else:
            is_cached, value = check_cache_and_write_if_not_cached(
                cache_key, getattr(prod_image_params, parameter)
            )
            if is_cached:
                setattr(prod_image_params, parameter, value)
    return prod_image_params
