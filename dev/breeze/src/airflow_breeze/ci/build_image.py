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
from pathlib import Path
from typing import Dict, List

from airflow_breeze.cache import check_cache_and_write_if_not_cached, touch_cache_file, write_to_cache_file
from airflow_breeze.ci.build_params import BuildParams
from airflow_breeze.console import console
from airflow_breeze.utils.path_utils import AIRFLOW_SOURCE, BUILD_CACHE_DIR
from airflow_breeze.utils.run_utils import filter_out_none, run_command

PARAMS_CI_IMAGE = [
    "python_base_image",
    "airflow_version",
    "airflow_branch",
    "airflow_extras",
    "airflow_pre_cached_pip_packages",
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
    "airflow_constraints_reference",
    "airflow_constraints",
    "airflow_image_repository",
    "airflow_image_date_created",
    "build_id",
    "commit_sha",
]

PARAMS_TO_VERIFY_CI_IMAGE = [
    "dev_apt_command",
    "dev_apt_deps",
    "runtime_apt_command",
    "runtime_apt_deps",
]


def construct_arguments_docker_command(ci_image: BuildParams) -> List[str]:
    args_command = []
    for param in PARAMS_CI_IMAGE:
        args_command.append("--build-arg")
        args_command.append(param.upper() + "=" + str(getattr(ci_image, param)))
    for verify_param in PARAMS_TO_VERIFY_CI_IMAGE:
        param_value = str(getattr(ci_image, verify_param))
        if len(param_value) > 0:
            args_command.append("--build-arg")
            args_command.append(verify_param.upper() + "=" + param_value)
    docker_cache = ci_image.docker_cache_ci_directive
    if len(docker_cache) > 0:
        args_command.extend(ci_image.docker_cache_ci_directive)
    return args_command


def construct_docker_command(ci_image: BuildParams) -> List[str]:
    arguments = construct_arguments_docker_command(ci_image)
    final_command = []
    final_command.extend(["docker", "buildx", "build", "--builder", "default", "--progress=tty", "--pull"])
    final_command.extend(arguments)
    final_command.extend(["-t", ci_image.airflow_ci_image_name, "--target", "main", "."])
    final_command.extend(["-f", 'Dockerfile.ci'])
    final_command.extend(["--platform", ci_image.platform])
    return final_command


def build_image(verbose, **kwargs):
    parameters_passed = filter_out_none(**kwargs)
    ci_image_params = get_image_build_params(parameters_passed)
    ci_image_cache_dir = Path(BUILD_CACHE_DIR, ci_image_params.airflow_branch)
    ci_image_cache_dir.mkdir(parents=True, exist_ok=True)
    touch_cache_file(
        f"built_{ci_image_params.python_version}",
        root_dir=ci_image_cache_dir,
    )
    cmd = construct_docker_command(ci_image_params)
    output = run_command(cmd, verbose=verbose, cwd=AIRFLOW_SOURCE, text=True)
    console.print(f"[blue]{output}")


def get_image_build_params(parameters_passed: Dict[str, str]):
    cacheable_parameters = {"python_version": 'PYTHON_MAJOR_MINOR_VERSION'}
    ci_image_params = BuildParams(**parameters_passed)
    for parameter, cache_key in cacheable_parameters.items():
        value_from_parameter = parameters_passed.get(parameter)
        if value_from_parameter:
            write_to_cache_file(cache_key, value_from_parameter, check_allowed_values=True)
            setattr(ci_image_params, parameter, value_from_parameter)
        else:
            is_cached, value = check_cache_and_write_if_not_cached(
                cache_key, getattr(ci_image_params, parameter)
            )
            if is_cached:
                setattr(ci_image_params, parameter, value)
    return ci_image_params
