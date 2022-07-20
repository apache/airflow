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
import multiprocessing as mp
import os
import sys
from pathlib import Path
from typing import List, Optional, Tuple, Union

import click

from airflow_breeze.commands.main_command import main
from airflow_breeze.params.build_ci_params import BuildCiParams
from airflow_breeze.params.shell_params import ShellParams
from airflow_breeze.utils.common_options import (
    option_additional_dev_apt_command,
    option_additional_dev_apt_deps,
    option_additional_dev_apt_env,
    option_additional_extras,
    option_additional_python_deps,
    option_additional_runtime_apt_command,
    option_additional_runtime_apt_deps,
    option_additional_runtime_apt_env,
    option_airflow_constraints_mode_ci,
    option_airflow_constraints_reference_build,
    option_answer,
    option_builder,
    option_debian_version,
    option_dev_apt_command,
    option_dev_apt_deps,
    option_docker_cache,
    option_dry_run,
    option_empty_image,
    option_force_build,
    option_github_repository,
    option_github_token,
    option_github_username,
    option_image_name,
    option_image_tag_for_building,
    option_image_tag_for_pulling,
    option_image_tag_for_verifying,
    option_install_providers_from_sources,
    option_parallelism,
    option_platform_multiple,
    option_prepare_buildx_cache,
    option_pull_image,
    option_push_image,
    option_python,
    option_python_image,
    option_python_versions,
    option_run_in_parallel,
    option_runtime_apt_command,
    option_runtime_apt_deps,
    option_tag_as_latest,
    option_upgrade_to_newer_dependencies,
    option_verbose,
    option_verify_image,
    option_wait_for_image,
)
from airflow_breeze.utils.confirm import STANDARD_TIMEOUT, Answer, user_confirm
from airflow_breeze.utils.console import get_console
from airflow_breeze.utils.docker_command_utils import (
    build_cache,
    perform_environment_checks,
    prepare_docker_build_command,
    prepare_docker_build_from_input,
)
from airflow_breeze.utils.image import run_pull_image, run_pull_in_parallel, tag_image_as_latest
from airflow_breeze.utils.mark_image_as_refreshed import mark_image_as_refreshed
from airflow_breeze.utils.md5_build_check import md5sum_check_if_build_is_needed
from airflow_breeze.utils.parallel import check_async_run_results
from airflow_breeze.utils.path_utils import AIRFLOW_SOURCES_ROOT, BUILD_CACHE_DIR
from airflow_breeze.utils.python_versions import get_python_version_list
from airflow_breeze.utils.registry import login_to_github_docker_registry
from airflow_breeze.utils.run_tests import verify_an_image
from airflow_breeze.utils.run_utils import (
    filter_out_none,
    fix_group_permissions,
    instruct_build_image,
    is_repo_rebased,
    run_command,
)

CI_IMAGE_TOOLS_COMMANDS = {
    "name": "CI Image tools",
    "commands": [
        "build-image",
        "pull-image",
        "verify-image",
    ],
}

CI_IMAGE_TOOLS_PARAMETERS = {
    "breeze build-image": [
        {
            "name": "Basic usage",
            "options": [
                "--python",
                "--upgrade-to-newer-dependencies",
                "--debian-version",
                "--image-tag",
                "--tag-as-latest",
                "--docker-cache",
                "--force-build",
            ],
        },
        {
            "name": "Building images in parallel",
            "options": [
                "--run-in-parallel",
                "--parallelism",
                "--python-versions",
            ],
        },
        {
            "name": "Advanced options (for power users)",
            "options": [
                "--install-providers-from-sources",
                "--airflow-constraints-mode",
                "--airflow-constraints-reference",
                "--python-image",
                "--additional-python-deps",
                "--runtime-apt-deps",
                "--runtime-apt-command",
                "--additional-extras",
                "--additional-runtime-apt-deps",
                "--additional-runtime-apt-env",
                "--additional-runtime-apt-command",
                "--additional-dev-apt-deps",
                "--additional-dev-apt-env",
                "--additional-dev-apt-command",
                "--dev-apt-deps",
                "--dev-apt-command",
            ],
        },
        {
            "name": "Preparing cache and push (for maintainers and CI)",
            "options": [
                "--github-token",
                "--github-username",
                "--platform",
                "--login-to-github-registry",
                "--push-image",
                "--empty-image",
                "--prepare-buildx-cache",
            ],
        },
    ],
    "breeze pull-image": [
        {
            "name": "Pull image flags",
            "options": [
                "--image-tag",
                "--python",
                "--github-token",
                "--verify-image",
                "--wait-for-image",
                "--tag-as-latest",
            ],
        },
        {
            "name": "Parallel running",
            "options": [
                "--run-in-parallel",
                "--parallelism",
                "--python-versions",
            ],
        },
    ],
    "breeze verify-image": [
        {
            "name": "Verify image flags",
            "options": [
                "--image-name",
                "--python",
                "--image-tag",
                "--pull-image",
            ],
        }
    ],
}


def check_if_image_building_is_needed(ci_image_params: BuildCiParams, dry_run: bool, verbose: bool) -> bool:
    """Starts building attempt. Returns false if we should not continue"""
    if not ci_image_params.force_build and not ci_image_params.upgrade_to_newer_dependencies:
        if not should_we_run_the_build(build_ci_params=ci_image_params):
            return False
    if ci_image_params.prepare_buildx_cache or ci_image_params.push_image:
        login_to_github_docker_registry(image_params=ci_image_params, dry_run=dry_run, verbose=verbose)
    return True


def run_build_in_parallel(
    image_params_list: List[BuildCiParams],
    python_version_list: List[str],
    parallelism: int,
    dry_run: bool,
    verbose: bool,
) -> None:
    get_console().print(
        f"\n[info]Building with parallelism = {parallelism} for the images: {python_version_list}:"
    )
    pool = mp.Pool(parallelism)
    results = [
        pool.apply_async(
            run_build_ci_image,
            args=(verbose, dry_run, image_param, True),
        )
        for image_param in image_params_list
    ]
    check_async_run_results(results)
    pool.close()


@main.command(name='build-image')
@option_github_repository
@option_verbose
@option_dry_run
@option_answer
@option_python
@option_run_in_parallel
@option_parallelism
@option_python_versions
@option_upgrade_to_newer_dependencies
@option_platform_multiple
@option_debian_version
@option_github_token
@option_github_username
@option_docker_cache
@option_image_tag_for_building
@option_prepare_buildx_cache
@option_push_image
@option_empty_image
@option_install_providers_from_sources
@option_additional_extras
@option_additional_dev_apt_deps
@option_additional_runtime_apt_deps
@option_additional_python_deps
@option_additional_dev_apt_command
@option_runtime_apt_command
@option_additional_dev_apt_env
@option_additional_runtime_apt_env
@option_additional_runtime_apt_command
@option_builder
@option_dev_apt_command
@option_dev_apt_deps
@option_force_build
@option_python_image
@option_runtime_apt_command
@option_runtime_apt_deps
@option_airflow_constraints_mode_ci
@option_airflow_constraints_reference_build
@option_tag_as_latest
def build_image(
    verbose: bool,
    dry_run: bool,
    run_in_parallel: bool,
    parallelism: int,
    python_versions: str,
    answer: str,
    **kwargs,
):
    """Build CI image. Include building multiple images for all python versions (sequentially)."""

    def run_build(ci_image_params: BuildCiParams) -> None:
        return_code, info = run_build_ci_image(
            verbose=verbose, dry_run=dry_run, ci_image_params=ci_image_params, parallel=False
        )
        if return_code != 0:
            get_console().print(f"[error]Error when building image! {info}")
            sys.exit(return_code)

    perform_environment_checks(verbose=verbose)
    parameters_passed = filter_out_none(**kwargs)
    parameters_passed['force_build'] = True
    fix_group_permissions(verbose=verbose)
    if run_in_parallel:
        python_version_list = get_python_version_list(python_versions)
        params_list: List[BuildCiParams] = []
        for python in python_version_list:
            params = BuildCiParams(**parameters_passed)
            params.python = python
            params.answer = answer
            params_list.append(params)
        check_if_image_building_is_needed(params_list[0], dry_run=dry_run, verbose=verbose)
        run_build_in_parallel(
            image_params_list=params_list,
            python_version_list=python_version_list,
            parallelism=parallelism,
            dry_run=dry_run,
            verbose=verbose,
        )
    else:
        params = BuildCiParams(**parameters_passed)
        check_if_image_building_is_needed(params, dry_run=dry_run, verbose=verbose)
        run_build(ci_image_params=params)


@main.command(name='pull-image')
@option_verbose
@option_dry_run
@option_python
@option_github_repository
@option_run_in_parallel
@option_parallelism
@option_python_versions
@option_github_token
@option_verify_image
@option_wait_for_image
@option_image_tag_for_pulling
@option_tag_as_latest
@click.argument('extra_pytest_args', nargs=-1, type=click.UNPROCESSED)
def pull_ci_image(
    verbose: bool,
    dry_run: bool,
    python: str,
    github_repository: str,
    run_in_parallel: bool,
    python_versions: str,
    github_token: str,
    parallelism: int,
    image_tag: str,
    wait_for_image: bool,
    tag_as_latest: bool,
    verify_image: bool,
    extra_pytest_args: Tuple,
):
    """Pull and optionally verify CI images - possibly in parallel for all Python versions."""
    if image_tag == "latest":
        get_console().print("[red]You cannot pull latest images because they are not published any more!\n")
        get_console().print(
            "[yellow]You need to specify commit tag to pull and image. If you wish to get"
            " the latest image, you need to run `breeze build-image` command\n"
        )
        sys.exit(1)
    perform_environment_checks(verbose=verbose)
    if run_in_parallel:
        python_version_list = get_python_version_list(python_versions)
        ci_image_params_list = [
            BuildCiParams(
                image_tag=image_tag,
                python=python,
                github_repository=github_repository,
                github_token=github_token,
            )
            for python in python_version_list
        ]
        run_pull_in_parallel(
            dry_run=dry_run,
            parallelism=parallelism,
            image_params_list=ci_image_params_list,
            python_version_list=python_version_list,
            verbose=verbose,
            verify_image=verify_image,
            wait_for_image=wait_for_image,
            tag_as_latest=tag_as_latest,
            extra_pytest_args=extra_pytest_args if extra_pytest_args is not None else (),
        )
    else:
        image_params = BuildCiParams(
            image_tag=image_tag, python=python, github_repository=github_repository, github_token=github_token
        )
        return_code, info = run_pull_image(
            image_params=image_params,
            dry_run=dry_run,
            verbose=verbose,
            wait_for_image=wait_for_image,
            tag_as_latest=tag_as_latest,
        )
        if return_code != 0:
            get_console().print(f"[error]There was an error when pulling CI image: {info}[/]")
            sys.exit(return_code)


@main.command(
    name='verify-image',
    context_settings=dict(
        ignore_unknown_options=True,
        allow_extra_args=True,
    ),
)
@option_verbose
@option_dry_run
@option_python
@option_github_repository
@option_image_tag_for_verifying
@option_image_name
@option_pull_image
@click.argument('extra_pytest_args', nargs=-1, type=click.UNPROCESSED)
def verify_ci_image(
    verbose: bool,
    dry_run: bool,
    python: str,
    github_repository: str,
    image_name: str,
    image_tag: Optional[str],
    pull_image: bool,
    extra_pytest_args: Tuple,
):
    """Verify CI image."""
    perform_environment_checks(verbose=verbose)
    if image_name is None:
        build_params = BuildCiParams(python=python, image_tag=image_tag, github_repository=github_repository)
        image_name = build_params.airflow_image_name_with_tag
    if pull_image:
        command_to_run = ["docker", "pull", image_name]
        run_command(command_to_run, verbose=verbose, dry_run=dry_run, check=True)
    get_console().print(f"[info]Verifying CI image: {image_name}[/]")
    return_code, info = verify_an_image(
        image_name=image_name,
        verbose=verbose,
        dry_run=dry_run,
        image_type='CI',
        slim_image=False,
        extra_pytest_args=extra_pytest_args,
    )
    sys.exit(return_code)


def should_we_run_the_build(build_ci_params: BuildCiParams) -> bool:
    """
    Check if we should run the build based on what files have been modified since last build and answer from
    the user.

    * If build is needed, the user is asked for confirmation
    * If the branch is not rebased it warns the user to rebase (to make sure latest remote cache is useful)
    * Builds Image/Skips/Quits depending on the answer

    :param build_ci_params: parameters for the build
    :param verbose: should we get verbose information
    """
    # We import those locally so that click autocomplete works
    from inputimeout import TimeoutOccurred

    if not md5sum_check_if_build_is_needed(md5sum_cache_dir=build_ci_params.md5sum_cache_dir):
        return False
    try:
        answer = user_confirm(
            message="Do you want to build the image (this works best when you have good connection and "
            "can take usually from 20 seconds to few minutes depending how old your image is)?",
            timeout=STANDARD_TIMEOUT,
            default_answer=Answer.NO,
        )
        if answer == answer.YES:
            if is_repo_rebased(build_ci_params.github_repository, build_ci_params.airflow_branch):
                return True
            else:
                get_console().print(
                    "\n[warning]This might take a lot of time (more than 10 minutes) even if you have"
                    "a good network connection. We think you should attempt to rebase first.[/]\n"
                )
                answer = user_confirm(
                    "But if you really, really want - you can attempt it. Are you really sure?",
                    timeout=STANDARD_TIMEOUT,
                    default_answer=Answer.NO,
                )
                if answer == Answer.YES:
                    return True
                else:
                    get_console().print(
                        f"[info]Please rebase your code to latest {build_ci_params.airflow_branch} "
                        "before continuing.[/]\nCheck this link to find out how "
                        "https://github.com/apache/airflow/blob/main/CONTRIBUTING.rst#id15\n"
                    )
                    get_console().print('[error]Exiting the process[/]\n')
                    sys.exit(1)
        elif answer == Answer.NO:
            instruct_build_image(build_ci_params.python)
            return False
        else:  # users_status == Answer.QUIT:
            get_console().print('\n[warning]Quitting the process[/]\n')
            sys.exit()
    except TimeoutOccurred:
        get_console().print('\nTimeout. Considering your response as No\n')
        instruct_build_image(build_ci_params.python)
        return False
    except Exception as e:
        get_console().print(f'\nTerminating the process on {e}')
        sys.exit(1)


def run_build_ci_image(
    verbose: bool, dry_run: bool, ci_image_params: BuildCiParams, parallel: bool
) -> Tuple[int, str]:
    """
    Builds CI image:

      * fixes group permissions for files (to improve caching when umask is 002)
      * converts all the parameters received via kwargs into BuildCIParams (including cache)
      * prints info about the image to build
      * logs int to docker registry on CI if build cache is being executed
      * removes "tag" for previously build image so that inline cache uses only remote image
      * constructs docker-compose command to run based on parameters passed
      * run the build command
      * update cached information that the build completed and saves checksums of all files
        for quick future check if the build is needed

    :param verbose: print commands when running
    :param dry_run: do not execute "write" commands - just print what would happen
    :param ci_image_params: CI image parameters
    :param parallel: whether the pull is run as part of parallel execution
    """
    if (
        ci_image_params.is_multi_platform()
        and not ci_image_params.push_image
        and not ci_image_params.prepare_buildx_cache
    ):
        get_console().print(
            "\n[red]You cannot use multi-platform build without using --push-image flag or "
            "preparing buildx cache![/]\n"
        )
        return 1, "Error: building multi-platform image without --push-image."
    if verbose or dry_run:
        get_console().print(
            f"\n[info]Building CI image of airflow from {AIRFLOW_SOURCES_ROOT} "
            f"python version: {ci_image_params.python}[/]\n"
        )
    if ci_image_params.prepare_buildx_cache:
        build_command_result = build_cache(
            image_params=ci_image_params, dry_run=dry_run, verbose=verbose, parallel=parallel
        )
    else:
        if ci_image_params.empty_image:
            env = os.environ.copy()
            env['DOCKER_BUILDKIT'] = "1"
            get_console().print(f"\n[info]Building empty CI Image for Python {ci_image_params.python}\n")
            build_command_result = run_command(
                prepare_docker_build_from_input(image_params=ci_image_params),
                input="FROM scratch\n",
                verbose=verbose,
                dry_run=dry_run,
                cwd=AIRFLOW_SOURCES_ROOT,
                text=True,
                env=env,
                enabled_output_group=not parallel,
            )
        else:
            get_console().print(f"\n[info]Building CI Image for Python {ci_image_params.python}\n")
            build_command_result = run_command(
                prepare_docker_build_command(
                    image_params=ci_image_params,
                    verbose=verbose,
                ),
                verbose=verbose,
                dry_run=dry_run,
                cwd=AIRFLOW_SOURCES_ROOT,
                text=True,
                check=False,
                enabled_output_group=not parallel,
            )
            if build_command_result.returncode == 0:
                if ci_image_params.tag_as_latest:
                    build_command_result = tag_image_as_latest(ci_image_params, dry_run, verbose)
                if ci_image_params.preparing_latest_image():
                    if dry_run:
                        get_console().print(
                            "[info]Not updating build hash because we are in `dry_run` mode.[/]"
                        )
                    else:
                        mark_image_as_refreshed(ci_image_params)
    return build_command_result.returncode, f"Image build: {ci_image_params.python}"


def rebuild_or_pull_ci_image_if_needed(
    command_params: Union[ShellParams, BuildCiParams], dry_run: bool, verbose: bool
) -> None:
    """
    Rebuilds CI image if needed and user confirms it.

    :param command_params: parameters of the command to execute
    :param dry_run: whether it's a dry_run
    :param verbose: should we print verbose messages
    """
    build_ci_image_check_cache = Path(
        BUILD_CACHE_DIR, command_params.airflow_branch, f".built_{command_params.python}"
    )
    ci_image_params = BuildCiParams(
        python=command_params.python,
        upgrade_to_newer_dependencies=False,
        image_tag=command_params.image_tag,
        platform=command_params.platform,
        force_build=command_params.force_build,
    )
    if command_params.image_tag is not None and command_params.image_tag != "latest":
        return_code, message = run_pull_image(
            image_params=ci_image_params,
            dry_run=dry_run,
            verbose=verbose,
            parallel=False,
            wait_for_image=True,
            tag_as_latest=False,
        )
        if return_code != 0:
            get_console().print(f"[error]Pulling image with {command_params.image_tag} failed! {message}[/]")
            sys.exit(return_code)
        return
    if build_ci_image_check_cache.exists():
        if verbose:
            get_console().print(f'[info]{command_params.image_type} image already built locally.[/]')
    else:
        get_console().print(
            f'[warning]{command_params.image_type} image was never built locally or deleted. '
            'Forcing build.[/]'
        )
        ci_image_params.force_build = True
    if check_if_image_building_is_needed(ci_image_params=ci_image_params, dry_run=dry_run, verbose=verbose):
        run_build_ci_image(verbose, dry_run=dry_run, ci_image_params=ci_image_params, parallel=False)
