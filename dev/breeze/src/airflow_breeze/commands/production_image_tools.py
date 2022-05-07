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

import sys
from typing import Optional, Tuple

import click

from airflow_breeze.build_image.prod.build_prod_image import build_production_image
from airflow_breeze.build_image.prod.build_prod_params import BuildProdParams
from airflow_breeze.commands.common_options import (
    option_additional_dev_apt_command,
    option_additional_dev_apt_deps,
    option_additional_dev_apt_env,
    option_additional_extras,
    option_additional_python_deps,
    option_additional_runtime_apt_command,
    option_additional_runtime_apt_deps,
    option_additional_runtime_apt_env,
    option_answer,
    option_build_multiple_images,
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
    option_image_tag,
    option_install_providers_from_sources,
    option_login_to_github_registry,
    option_parallelism,
    option_platform,
    option_prepare_buildx_cache,
    option_push_image,
    option_python,
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
from airflow_breeze.commands.custom_param_types import BetterChoice
from airflow_breeze.commands.main import main
from airflow_breeze.global_constants import ALLOWED_INSTALLATION_METHODS, DEFAULT_EXTRAS
from airflow_breeze.utils.console import get_console
from airflow_breeze.utils.pulll_image import run_pull_image, run_pull_in_parallel
from airflow_breeze.utils.python_versions import get_python_version_list
from airflow_breeze.utils.run_tests import verify_an_image
from airflow_breeze.utils.run_utils import filter_out_none

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
                "--docker-cache",
            ],
        },
        {
            "name": "Building multiple images",
            "options": [
                "--build-multiple-images",
                "--python-versions",
            ],
        },
        {
            "name": "Options for customizing images",
            "options": [
                "--install-providers-from-sources",
                "--additional-python-deps",
                "--additional-extras",
                "--additional-runtime-apt-deps",
                "--additional-runtime-apt-env",
                "--additional-runtime-apt-command",
                "--additional-dev-apt-deps",
                "--additional-dev-apt-env",
                "--additional-dev-apt-command",
                "--extras",
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
                "--login-to-github-registry",
                "--push-image",
                "--prepare-buildx-cache",
                "--platform",
                "--empty-image",
            ],
        },
    ],
    "breeze pull-prod-image": [
        {
            "name": "Pull image flags",
            "options": [
                "--image-tag",
                "--python",
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
            ],
        }
    ],
}


@option_verbose
@option_dry_run
@option_answer
@main.command(name='build-prod-image')
@option_python
@option_build_multiple_images
@option_python_versions
@option_upgrade_to_newer_dependencies
@option_platform
@option_debian_version
@option_github_repository
@option_github_token
@option_github_username
@option_login_to_github_registry
@option_docker_cache
@option_image_tag
@option_prepare_buildx_cache
@option_push_image
@option_empty_image
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
    '--extras', default=",".join(DEFAULT_EXTRAS), show_default=True, help="Extras to install by default."
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
@option_runtime_apt_command
@option_runtime_apt_deps
def build_prod_image(
    verbose: bool,
    dry_run: bool,
    build_multiple_images: bool,
    python_versions: str,
    answer: Optional[str],
    **kwargs,
):
    """
    Build Production image. Include building multiple images for all or selected Python versions sequentially.
    """

    def run_build(prod_image_params: BuildProdParams) -> None:
        return_code, info = build_production_image(
            verbose=verbose, dry_run=dry_run, prod_image_params=prod_image_params
        )
        if return_code != 0:
            get_console().print(f"[error]Error when building image! {info}")
            sys.exit(return_code)

    parameters_passed = filter_out_none(**kwargs)
    if build_multiple_images:
        python_version_list = get_python_version_list(python_versions)
        for python in python_version_list:
            params = BuildProdParams(**parameters_passed)
            params.python = python
            params.answer = answer
            run_build(prod_image_params=params)
    else:
        params = BuildProdParams(**parameters_passed)
        run_build(prod_image_params=params)


@main.command(name='pull-prod-image')
@option_verbose
@option_dry_run
@option_python
@option_github_repository
@option_run_in_parallel
@option_parallelism
@option_python_versions
@option_image_tag
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
    image_tag: Optional[str],
    wait_for_image: bool,
    tag_as_latest: bool,
    verify_image: bool,
    extra_pytest_args: Tuple,
):
    """Pull and optionally verify Production images - possibly in parallel for all Python versions."""
    if run_in_parallel:
        python_version_list = get_python_version_list(python_versions)
        prod_image_params_list = [
            BuildProdParams(image_tag=image_tag, python=python, github_repository=github_repository)
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
            image_tag=image_tag, python=python, github_repository=github_repository
        )
        return_code, info = run_pull_image(
            image_params=image_params,
            dry_run=dry_run,
            verbose=verbose,
            wait_for_image=wait_for_image,
            tag_as_latest=tag_as_latest,
            poll_time=10.0,
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
@option_image_tag
@option_image_name
@click.argument('extra_pytest_args', nargs=-1, type=click.UNPROCESSED)
def verify_prod_image(
    verbose: bool,
    dry_run: bool,
    python: str,
    github_repository: str,
    image_name: str,
    image_tag: str,
    extra_pytest_args: Tuple,
):
    """Verify Production image."""
    if image_name is None:
        build_params = BuildProdParams(
            python=python, image_tag=image_tag, github_repository=github_repository
        )
        image_name = build_params.airflow_image_name_with_tag
    get_console().print(f"[info]Verifying PROD image: {image_name}[/]")
    return_code, info = verify_an_image(
        image_name=image_name,
        verbose=verbose,
        dry_run=dry_run,
        image_type='PROD',
        extra_pytest_args=extra_pytest_args,
    )
    sys.exit(return_code)
