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
import multiprocessing as mp
import os
import sys
from typing import List, Optional, Tuple

import click

from airflow_breeze.commands.main_command import main
from airflow_breeze.global_constants import ALLOWED_INSTALLATION_METHODS, DEFAULT_EXTRAS
from airflow_breeze.params.build_prod_params import BuildProdParams
from airflow_breeze.utils.common_options import (
    option_additional_dev_apt_command,
    option_additional_dev_apt_deps,
    option_additional_dev_apt_env,
    option_additional_extras,
    option_additional_python_deps,
    option_additional_runtime_apt_command,
    option_additional_runtime_apt_deps,
    option_additional_runtime_apt_env,
    option_airflow_constraints_mode_prod,
    option_airflow_constraints_reference_build,
    option_answer,
    option_debian_version,
    option_dev_apt_command,
    option_dev_apt_deps,
    option_docker_cache,
    option_dry_run,
    option_empty_image,
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
from airflow_breeze.utils.console import get_console
from airflow_breeze.utils.custom_param_types import BetterChoice
from airflow_breeze.utils.docker_command_utils import (
    build_cache,
    perform_environment_checks,
    prepare_docker_build_command,
    prepare_docker_build_from_input,
)
from airflow_breeze.utils.image import run_pull_image, run_pull_in_parallel, tag_image_as_latest
from airflow_breeze.utils.parallel import check_async_run_results
from airflow_breeze.utils.path_utils import AIRFLOW_SOURCES_ROOT, DOCKER_CONTEXT_DIR
from airflow_breeze.utils.python_versions import get_python_version_list
from airflow_breeze.utils.registry import login_to_github_docker_registry
from airflow_breeze.utils.run_tests import verify_an_image
from airflow_breeze.utils.run_utils import filter_out_none, fix_group_permissions, run_command

PRODUCTION_IMAGE_TOOLS_COMMANDS = {
    "name": "Production Image tools",
    "commands": [
        "build-prod-image",
        "pull-prod-image",
        "verify-prod-image",
    ],
}
PRODUCTION_IMAGE_TOOLS_PARAMETERS = {
    "breeze build-prod-image": [
        {
            "name": "Basic usage",
            "options": [
                "--python",
                "--install-airflow-version",
                "--upgrade-to-newer-dependencies",
                "--debian-version",
                "--image-tag",
                "--tag-as-latest",
                "--docker-cache",
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
            "name": "Options for customizing images",
            "options": [
                "--install-providers-from-sources",
                "--airflow-extras",
                "--airflow-constraints-mode",
                "--airflow-constraints-reference",
                "--python-image",
                "--additional-python-deps",
                "--additional-extras",
                "--additional-runtime-apt-deps",
                "--additional-runtime-apt-env",
                "--additional-runtime-apt-command",
                "--additional-dev-apt-deps",
                "--additional-dev-apt-env",
                "--additional-dev-apt-command",
                "--runtime-apt-deps",
                "--runtime-apt-command",
                "--dev-apt-deps",
                "--dev-apt-command",
            ],
        },
        {
            "name": "Customization options (for specific customization needs)",
            "options": [
                "--install-packages-from-context",
                "--airflow-is-in-context",
                "--cleanup-context",
                "--disable-mysql-client-installation",
                "--disable-mssql-client-installation",
                "--disable-postgres-client-installation",
                "--disable-airflow-repo-cache",
                "--install-airflow-reference",
                "--installation-method",
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
    "breeze pull-prod-image": [
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
    "breeze verify-prod-image": [
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


def start_building(prod_image_params: BuildProdParams, dry_run: bool, verbose: bool):
    if prod_image_params.cleanup_context:
        clean_docker_context_files(verbose=verbose, dry_run=dry_run)
    check_docker_context_files(prod_image_params.install_packages_from_context)
    if prod_image_params.prepare_buildx_cache or prod_image_params.push_image:
        login_to_github_docker_registry(image_params=prod_image_params, dry_run=dry_run, verbose=verbose)


def run_build_in_parallel(
    image_params_list: List[BuildProdParams],
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
            run_build_production_image,
            args=(
                verbose,
                dry_run,
                image_param,
                True,
            ),
        )
        for image_param in image_params_list
    ]
    check_async_run_results(results)
    pool.close()


@option_verbose
@option_dry_run
@option_answer
@main.command(name='build-prod-image')
@option_python
@option_run_in_parallel
@option_parallelism
@option_python_versions
@option_upgrade_to_newer_dependencies
@option_platform_multiple
@option_debian_version
@option_github_repository
@option_github_token
@option_github_username
@option_docker_cache
@option_image_tag_for_building
@option_prepare_buildx_cache
@option_push_image
@option_empty_image
@option_airflow_constraints_mode_prod
@click.option(
    '--installation-method',
    help="Install Airflow from: sources or PyPI.",
    type=BetterChoice(ALLOWED_INSTALLATION_METHODS),
)
@option_install_providers_from_sources
@click.option(
    '--airflow-is-in-context',
    help="If set Airflow is installed from docker-context-files only rather than from PyPI or sources.",
    is_flag=True,
)
@click.option(
    '--install-packages-from-context',
    help='Install wheels from local docker-context-files when building image.',
    is_flag=True,
)
@click.option(
    '--cleanup-context',
    help='Clean up docker context files before running build (cannot be used together'
    ' with --install-packages-from-context).',
    is_flag=True,
)
@click.option(
    '--airflow-extras',
    default=",".join(DEFAULT_EXTRAS),
    show_default=True,
    help="Extras to install by default.",
)
@click.option('--disable-mysql-client-installation', help="Do not install MySQL client.", is_flag=True)
@click.option('--disable-mssql-client-installation', help="Do not install MsSQl client.", is_flag=True)
@click.option('--disable-postgres-client-installation', help="Do not install Postgres client.", is_flag=True)
@click.option(
    '--disable-airflow-repo-cache',
    help="Disable cache from Airflow repository during building.",
    is_flag=True,
)
@click.option(
    '--install-airflow-reference',
    help="Install Airflow using GitHub tag or branch.",
)
@option_airflow_constraints_reference_build
@click.option('-V', '--install-airflow-version', help="Install version of Airflow from PyPI.")
@option_additional_extras
@option_additional_dev_apt_deps
@option_additional_runtime_apt_deps
@option_additional_python_deps
@option_additional_dev_apt_command
@option_additional_dev_apt_env
@option_additional_runtime_apt_env
@option_additional_runtime_apt_command
@option_dev_apt_command
@option_dev_apt_deps
@option_python_image
@option_runtime_apt_command
@option_runtime_apt_deps
@option_tag_as_latest
def build_prod_image(
    verbose: bool,
    dry_run: bool,
    run_in_parallel: bool,
    parallelism: int,
    python_versions: str,
    answer: Optional[str],
    **kwargs,
):
    """
    Build Production image. Include building multiple images for all or selected Python versions sequentially.
    """

    def run_build(prod_image_params: BuildProdParams) -> None:
        return_code, info = run_build_production_image(
            verbose=verbose, dry_run=dry_run, prod_image_params=prod_image_params, parallel=False
        )
        if return_code != 0:
            get_console().print(f"[error]Error when building image! {info}")
            sys.exit(return_code)

    perform_environment_checks(verbose=verbose)
    parameters_passed = filter_out_none(**kwargs)

    fix_group_permissions(verbose=verbose)
    if run_in_parallel:
        python_version_list = get_python_version_list(python_versions)
        params_list: List[BuildProdParams] = []
        for python in python_version_list:
            params = BuildProdParams(**parameters_passed)
            params.python = python
            params.answer = answer
            params_list.append(params)
        start_building(prod_image_params=params_list[0], dry_run=dry_run, verbose=verbose)
        run_build_in_parallel(
            image_params_list=params_list,
            python_version_list=python_version_list,
            parallelism=parallelism,
            dry_run=dry_run,
            verbose=verbose,
        )
    else:
        params = BuildProdParams(**parameters_passed)
        start_building(prod_image_params=params, dry_run=dry_run, verbose=verbose)
        run_build(prod_image_params=params)


@main.command(name='pull-prod-image')
@option_verbose
@option_dry_run
@option_python
@option_github_repository
@option_run_in_parallel
@option_parallelism
@option_python_versions
@option_github_token
@option_image_tag_for_pulling
@option_wait_for_image
@option_tag_as_latest
@option_verify_image
@click.argument('extra_pytest_args', nargs=-1, type=click.UNPROCESSED)
def pull_prod_image(
    verbose: bool,
    dry_run: bool,
    python: str,
    github_repository: str,
    run_in_parallel: bool,
    parallelism: int,
    python_versions: str,
    github_token: str,
    image_tag: str,
    wait_for_image: bool,
    tag_as_latest: bool,
    verify_image: bool,
    extra_pytest_args: Tuple,
):
    """Pull and optionally verify Production images - possibly in parallel for all Python versions."""
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
        prod_image_params_list = [
            BuildProdParams(
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
            image_params_list=prod_image_params_list,
            python_version_list=python_version_list,
            verbose=verbose,
            verify_image=verify_image,
            wait_for_image=wait_for_image,
            tag_as_latest=tag_as_latest,
            extra_pytest_args=extra_pytest_args if extra_pytest_args is not None else (),
        )
    else:
        image_params = BuildProdParams(
            image_tag=image_tag, python=python, github_repository=github_repository, github_token=github_token
        )
        return_code, info = run_pull_image(
            image_params=image_params,
            dry_run=dry_run,
            verbose=verbose,
            wait_for_image=wait_for_image,
            tag_as_latest=tag_as_latest,
            poll_time=10.0,
            parallel=False,
        )
        if return_code != 0:
            get_console().print(f"[error]There was an error when pulling PROD image: {info}[/]")
            sys.exit(return_code)


@main.command(
    name='verify-prod-image',
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
@click.option(
    '--slim-image',
    help='The image to verify is slim and non-slim tests should be skipped.',
    is_flag=True,
)
@click.argument('extra_pytest_args', nargs=-1, type=click.UNPROCESSED)
def verify_prod_image(
    verbose: bool,
    dry_run: bool,
    python: str,
    github_repository: str,
    image_name: str,
    image_tag: Optional[str],
    pull_image: bool,
    slim_image: bool,
    extra_pytest_args: Tuple,
):
    """Verify Production image."""
    perform_environment_checks(verbose=verbose)
    if image_name is None:
        build_params = BuildProdParams(
            python=python, image_tag=image_tag, github_repository=github_repository
        )
        image_name = build_params.airflow_image_name_with_tag
    if pull_image:
        command_to_run = ["docker", "pull", image_name]
        run_command(command_to_run, verbose=verbose, dry_run=dry_run, check=True)
    get_console().print(f"[info]Verifying PROD image: {image_name}[/]")
    return_code, info = verify_an_image(
        image_name=image_name,
        verbose=verbose,
        dry_run=dry_run,
        image_type='PROD',
        extra_pytest_args=extra_pytest_args,
        slim_image=slim_image,
    )
    sys.exit(return_code)


def clean_docker_context_files(verbose: bool, dry_run: bool):
    """
    Cleans up docker context files folder - leaving only .README.md there.
    """
    if verbose or dry_run:
        get_console().print("[info]Cleaning docker-context-files[/]")
    if dry_run:
        return
    with contextlib.suppress(FileNotFoundError):
        context_files_to_delete = DOCKER_CONTEXT_DIR.glob('**/*')
        for file_to_delete in context_files_to_delete:
            if file_to_delete.name != '.README.md':
                file_to_delete.unlink()


def check_docker_context_files(install_packages_from_context: bool):
    """
    Quick check - if we want to install from docker-context-files we expect some packages there but if
    we don't - we don't expect them, and they might invalidate Docker cache.

    This method exits with an error if what we see is unexpected for given operation.

    :param install_packages_from_context: whether we want to install from docker-context-files
    """
    context_file = DOCKER_CONTEXT_DIR.glob('**/*')
    number_of_context_files = len(
        [context for context in context_file if context.is_file() and context.name != '.README.md']
    )
    if number_of_context_files == 0:
        if install_packages_from_context:
            get_console().print('[warning]\nERROR! You want to install packages from docker-context-files')
            get_console().print('[warning]\n but there are no packages to install in this folder.')
            sys.exit(1)
    else:
        if not install_packages_from_context:
            get_console().print(
                '[warning]\n ERROR! There are some extra files in docker-context-files except README.md'
            )
            get_console().print('[warning]\nAnd you did not choose --install-packages-from-context flag')
            get_console().print(
                '[warning]\nThis might result in unnecessary cache invalidation and long build times'
            )
            get_console().print(
                '[warning]\nExiting now \
                    - please restart the command with --cleanup-context switch'
            )
            sys.exit(1)


def run_build_production_image(
    verbose: bool, dry_run: bool, prod_image_params: BuildProdParams, parallel: bool
) -> Tuple[int, str]:
    """
    Builds PROD image:

      * fixes group permissions for files (to improve caching when umask is 002)
      * converts all the parameters received via kwargs into BuildProdParams (including cache)
      * prints info about the image to build
      * removes docker-context-files if requested
      * performs quick check if the files are present in docker-context-files if expected
      * logs int to docker registry on CI if build cache is being executed
      * removes "tag" for previously build image so that inline cache uses only remote image
      * constructs docker-compose command to run based on parameters passed
      * run the build command
      * update cached information that the build completed and saves checksums of all files
        for quick future check if the build is needed

    :param verbose: print commands when running
    :param dry_run: do not execute "write" commands - just print what would happen
    :param prod_image_params: PROD image parameters
    """
    if (
        prod_image_params.is_multi_platform()
        and not prod_image_params.push_image
        and not prod_image_params.prepare_buildx_cache
    ):
        get_console().print(
            "\n[red]You cannot use multi-platform build without using --push-image flag"
            " or preparing buildx cache![/]\n"
        )
        return 1, "Error: building multi-platform image without --push-image."
    get_console().print(f"\n[info]Building PROD Image for Python {prod_image_params.python}\n")
    if prod_image_params.prepare_buildx_cache:
        build_command_result = build_cache(
            image_params=prod_image_params, dry_run=dry_run, verbose=verbose, parallel=parallel
        )
    else:
        if prod_image_params.empty_image:
            env = os.environ.copy()
            env['DOCKER_BUILDKIT'] = "1"
            get_console().print(f"\n[info]Building empty PROD Image for Python {prod_image_params.python}\n")
            build_command_result = run_command(
                prepare_docker_build_from_input(image_params=prod_image_params),
                input="FROM scratch\n",
                verbose=verbose,
                dry_run=dry_run,
                cwd=AIRFLOW_SOURCES_ROOT,
                check=False,
                text=True,
                env=env,
            )
        else:
            build_command_result = run_command(
                prepare_docker_build_command(
                    image_params=prod_image_params,
                    verbose=verbose,
                ),
                verbose=verbose,
                dry_run=dry_run,
                cwd=AIRFLOW_SOURCES_ROOT,
                check=False,
                text=True,
                enabled_output_group=not parallel,
            )
            if build_command_result.returncode == 0:
                if prod_image_params.tag_as_latest:
                    build_command_result = tag_image_as_latest(prod_image_params, dry_run, verbose)
    return build_command_result.returncode, f"Image build: {prod_image_params.python}"
