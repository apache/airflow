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

import subprocess
import time
from typing import Callable

from airflow_breeze.global_constants import (
    ALLOWED_PYTHON_MAJOR_MINOR_VERSIONS,
    DEFAULT_PYTHON_MAJOR_MINOR_VERSION,
    MOUNT_ALL,
)
from airflow_breeze.params.build_ci_params import BuildCiParams
from airflow_breeze.params.build_prod_params import BuildProdParams
from airflow_breeze.params.common_build_params import CommonBuildParams
from airflow_breeze.params.shell_params import ShellParams
from airflow_breeze.utils.ci_group import ci_group
from airflow_breeze.utils.console import Output, get_console
from airflow_breeze.utils.mark_image_as_refreshed import mark_image_as_refreshed
from airflow_breeze.utils.parallel import (
    DOCKER_PULL_PROGRESS_REGEXP,
    GenericRegexpProgressMatcher,
    check_async_run_results,
    run_with_pool,
)
from airflow_breeze.utils.registry import login_to_github_docker_registry
from airflow_breeze.utils.run_tests import verify_an_image
from airflow_breeze.utils.run_utils import RunCommandResult, run_command
from airflow_breeze.utils.shared_options import get_dry_run, get_verbose


def run_pull_in_parallel(
    parallelism: int,
    skip_cleanup: bool,
    debug_resources: bool,
    image_params_list: list[BuildCiParams] | list[BuildProdParams],
    python_version_list: list[str],
    verify: bool,
    include_success_outputs: bool,
    tag_as_latest: bool,
    wait_for_image: bool,
    extra_pytest_args: tuple,
):
    """Run image pull in parallel"""
    all_params = [f"Image {image_params.python}" for image_params in image_params_list]
    with ci_group(f"Pull{'/verify' if verify else ''} for {python_version_list}"):
        with run_with_pool(
            parallelism=parallelism,
            all_params=all_params,
            debug_resources=debug_resources,
            progress_matcher=GenericRegexpProgressMatcher(DOCKER_PULL_PROGRESS_REGEXP, lines_to_search=15),
        ) as (pool, outputs):

            def get_right_method() -> Callable[..., tuple[int, str]]:
                if verify:
                    return run_pull_and_verify_image
                else:
                    return run_pull_image

            def get_kwds(index: int, image_param: BuildCiParams | BuildProdParams):
                d = {
                    "image_params": image_param,
                    "wait_for_image": wait_for_image,
                    "tag_as_latest": tag_as_latest,
                    "poll_time": 10.0,
                    "output": outputs[index],
                }
                if verify:
                    d["extra_pytest_args"] = extra_pytest_args
                return d

            results = [
                pool.apply_async(get_right_method(), kwds=get_kwds(index, image_param))
                for index, image_param in enumerate(image_params_list)
            ]
    check_async_run_results(
        results=results,
        success="All images pulled",
        outputs=outputs,
        include_success_outputs=include_success_outputs,
        skip_cleanup=skip_cleanup,
    )


def run_pull_image(
    image_params: CommonBuildParams,
    wait_for_image: bool,
    tag_as_latest: bool,
    output: Output | None,
    poll_time: float = 10.0,
) -> tuple[int, str]:
    """
    Pull image specified.
    :param image_params: Image parameters.


    :param output: output to write to
    :param wait_for_image: whether we should wait for the image to be available
    :param tag_as_latest: tag the image as latest
    :param poll_time: what's the polling time between checks if images are there (default 10 s)
    :return: Tuple of return code and description of the image pulled
    """
    get_console(output=output).print(
        f"\n[info]Pulling {image_params.image_type} image of airflow python version: "
        f"{image_params.python} image: {image_params.airflow_image_name_with_tag} "
        f"with wait for image: {wait_for_image}[/]\n"
    )
    current_loop = 1
    while True:
        login_to_github_docker_registry(
            image_params=image_params,
            output=output,
        )
        command_to_run = ["docker", "pull", image_params.airflow_image_name_with_tag]
        command_result = run_command(command_to_run, check=False, output=output)
        if command_result.returncode == 0:
            command_result = run_command(
                ["docker", "inspect", image_params.airflow_image_name_with_tag, "-f", "{{.Size}}"],
                capture_output=True,
                output=output,
                text=True,
                check=False,
            )
            if not get_dry_run():
                if command_result.returncode == 0:
                    image_size = int(command_result.stdout.strip())
                    if image_size == 0:
                        get_console(output=output).print(
                            "\n[error]The image size was 0 - image creation failed.[/]\n"
                        )
                        return 1, f"Image Python {image_params.python}"
                else:
                    get_console(output=output).print(
                        "\n[error]There was an error pulling the size of the image. Failing.[/]\n"
                    )
                    return (
                        command_result.returncode,
                        f"Image Python {image_params.python}",
                    )
            if tag_as_latest:
                command_result = tag_image_as_latest(image_params=image_params, output=output)
                if command_result.returncode == 0 and isinstance(image_params, BuildCiParams):
                    mark_image_as_refreshed(image_params)
            return command_result.returncode, f"Image Python {image_params.python}"
        if wait_for_image:
            if get_verbose() or get_dry_run():
                get_console(output=output).print(
                    f"\n[info]Waiting: #{current_loop} {image_params.airflow_image_name_with_tag}.[/]\n"
                )
            time.sleep(poll_time)
            current_loop += 1
            continue
        else:
            get_console(output=output).print(
                f"\n[error]There was an error pulling the image {image_params.python}. Failing.[/]\n"
            )
            return command_result.returncode, f"Image Python {image_params.python}"


def tag_image_as_latest(image_params: CommonBuildParams, output: Output | None) -> RunCommandResult:
    if image_params.airflow_image_name_with_tag == image_params.airflow_image_name:
        get_console(output=output).print(
            f"[info]Skip tagging {image_params.airflow_image_name} as latest as it is already 'latest'[/]"
        )
        return subprocess.CompletedProcess(returncode=0, args=[])
    return run_command(
        [
            "docker",
            "tag",
            image_params.airflow_image_name_with_tag,
            image_params.airflow_image_name,
        ],
        output=output,
        capture_output=True,
        check=False,
    )


def run_pull_and_verify_image(
    image_params: CommonBuildParams,
    wait_for_image: bool,
    tag_as_latest: bool,
    poll_time: float,
    extra_pytest_args: tuple,
    output: Output | None,
) -> tuple[int, str]:
    return_code, info = run_pull_image(
        image_params=image_params,
        wait_for_image=wait_for_image,
        tag_as_latest=tag_as_latest,
        output=output,
        poll_time=poll_time,
    )
    if return_code != 0:
        get_console(output=output).print(
            f"\n[error]Not running verification for {image_params.python} as pulling failed.[/]\n"
        )
    return verify_an_image(
        image_name=image_params.airflow_image_name_with_tag,
        image_type=image_params.image_type,
        output=output,
        slim_image=False,
        extra_pytest_args=extra_pytest_args,
    )


def just_pull_ci_image(github_repository, python_version: str) -> tuple[ShellParams, RunCommandResult]:
    shell_params = ShellParams(
        mount_sources=MOUNT_ALL,
        python=python_version,
        github_repository=github_repository,
        skip_environment_initialization=True,
    )
    get_console().print(f"[info]Pulling {shell_params.airflow_image_name_with_tag}.[/]")
    pull_command_result = run_command(
        ["docker", "pull", shell_params.airflow_image_name_with_tag],
        check=True,
    )
    return shell_params, pull_command_result


def check_if_ci_image_available(
    github_repository: str, python_version: str
) -> tuple[ShellParams, RunCommandResult]:
    shell_params = ShellParams(
        mount_sources=MOUNT_ALL,
        python=python_version,
        github_repository=github_repository,
        skip_environment_initialization=True,
    )
    inspect_command_result = run_command(
        ["docker", "inspect", shell_params.airflow_image_name_with_tag],
        stdout=subprocess.DEVNULL,
        check=False,
    )
    return (
        shell_params,
        inspect_command_result,
    )


def find_available_ci_image(github_repository: str) -> ShellParams:
    for python_version in ALLOWED_PYTHON_MAJOR_MINOR_VERSIONS:
        shell_params, inspect_command_result = check_if_ci_image_available(github_repository, python_version)
        if inspect_command_result.returncode == 0:
            get_console().print(
                f"[info]Running fix_ownership with {shell_params.airflow_image_name_with_tag}.[/]"
            )
            return shell_params
    shell_params, _ = just_pull_ci_image(github_repository, DEFAULT_PYTHON_MAJOR_MINOR_VERSION)
    return shell_params
