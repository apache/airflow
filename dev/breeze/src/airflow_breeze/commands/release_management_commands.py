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
import re
import shlex
import shutil
import sys
import textwrap
import time
from copy import deepcopy
from datetime import datetime
from pathlib import Path
from typing import IO, Generator, NamedTuple

import click
from rich.progress import Progress
from rich.syntax import Syntax

from airflow_breeze.commands.ci_image_commands import rebuild_or_pull_ci_image_if_needed
from airflow_breeze.commands.release_management_group import release_management
from airflow_breeze.global_constants import (
    ALLOWED_PLATFORMS,
    APACHE_AIRFLOW_GITHUB_REPOSITORY,
    CURRENT_PYTHON_MAJOR_MINOR_VERSIONS,
    DEFAULT_PYTHON_MAJOR_MINOR_VERSION,
    MOUNT_ALL,
    MOUNT_SELECTED,
    MULTI_PLATFORM,
)
from airflow_breeze.params.shell_params import ShellParams
from airflow_breeze.utils.add_back_references import (
    start_generating_back_references,
)
from airflow_breeze.utils.ci_group import ci_group
from airflow_breeze.utils.common_options import (
    argument_doc_packages,
    argument_provider_packages,
    option_airflow_constraints_mode_ci,
    option_airflow_constraints_mode_update,
    option_airflow_constraints_reference,
    option_airflow_extras,
    option_airflow_site_directory,
    option_answer,
    option_commit_sha,
    option_debug_resources,
    option_dry_run,
    option_github_repository,
    option_historical_python_version,
    option_image_tag_for_running,
    option_include_success_outputs,
    option_install_selected_providers,
    option_installation_package_format,
    option_package_format,
    option_parallelism,
    option_python,
    option_python_versions,
    option_run_in_parallel,
    option_skip_cleanup,
    option_skip_constraints,
    option_use_airflow_version,
    option_use_packages_from_dist,
    option_verbose,
    option_version_suffix_for_pypi,
)
from airflow_breeze.utils.confirm import Answer, user_confirm
from airflow_breeze.utils.console import Output, get_console
from airflow_breeze.utils.custom_param_types import BetterChoice
from airflow_breeze.utils.docker_command_utils import (
    check_remote_ghcr_io_commands,
    get_env_variables_for_docker_commands,
    get_extra_docker_flags,
    perform_environment_checks,
)
from airflow_breeze.utils.github import download_constraints_file, get_active_airflow_versions
from airflow_breeze.utils.packages import convert_to_long_package_names, expand_all_provider_packages
from airflow_breeze.utils.parallel import (
    GenericRegexpProgressMatcher,
    SummarizeAfter,
    check_async_run_results,
    run_with_pool,
)
from airflow_breeze.utils.path_utils import (
    AIRFLOW_SOURCES_ROOT,
    CONSTRAINTS_CACHE_DIR,
    DIST_DIR,
    PROVIDER_METADATA_JSON_FILE_PATH,
    cleanup_python_generated_files,
)
from airflow_breeze.utils.provider_dependencies import (
    DEPENDENCIES,
    generate_providers_metadata_for_package,
    get_related_providers,
)
from airflow_breeze.utils.publish_docs_builder import PublishDocsBuilder
from airflow_breeze.utils.python_versions import get_python_version_list
from airflow_breeze.utils.run_utils import (
    RunCommandResult,
    assert_pre_commit_installed,
    run_command,
    run_compile_www_assets,
)
from airflow_breeze.utils.shared_options import get_dry_run, get_forced_answer, get_verbose
from airflow_breeze.utils.suspended_providers import get_removed_provider_ids

option_debug_release_management = click.option(
    "--debug",
    is_flag=True,
    help="Drop user in shell instead of running the command. Useful for debugging.",
    envvar="DEBUG",
)


def run_docker_command_with_debug(
    params: ShellParams,
    command: list[str],
    debug: bool,
    enable_input: bool = False,
    output_outside_the_group: bool = False,
    **kwargs,
) -> RunCommandResult:
    env_variables = get_env_variables_for_docker_commands(params)
    extra_docker_flags = get_extra_docker_flags(mount_sources=params.mount_sources)
    if enable_input or debug:
        term_flag = "-it"
    else:
        term_flag = "-t"
    base_command = [
        "docker",
        "run",
        term_flag,
        *extra_docker_flags,
        "--pull",
        "never",
        params.airflow_image_name_with_tag,
    ]
    if debug:
        cmd_string = " ".join([shlex.quote(s) for s in command if s != "-c"])
        base_command.extend(
            [
                "-c",
                f"""
echo -e '\\e[34mRun this command to debug:

    {cmd_string}

\\e[0m\n'; exec bash
""",
            ]
        )
        return run_command(
            base_command,
            env=env_variables,
            output_outside_the_group=output_outside_the_group,
            **kwargs,
        )
    else:
        base_command.extend(command)
        return run_command(
            base_command,
            env=env_variables,
            check=False,
            output_outside_the_group=output_outside_the_group,
            **kwargs,
        )


@release_management.command(
    name="prepare-airflow-package",
    help="Prepare sdist/whl package of Airflow.",
)
@option_package_format
@option_version_suffix_for_pypi
@option_debug_release_management
@option_github_repository
@option_verbose
@option_dry_run
def prepare_airflow_packages(
    package_format: str,
    version_suffix_for_pypi: str,
    debug: bool,
    github_repository: str,
):
    perform_environment_checks()
    cleanup_python_generated_files()
    assert_pre_commit_installed()
    run_compile_www_assets(dev=False, run_in_background=False)
    shell_params = ShellParams(
        github_repository=github_repository,
        python=DEFAULT_PYTHON_MAJOR_MINOR_VERSION,
        package_format=package_format,
        version_suffix_for_pypi=version_suffix_for_pypi,
        skip_environment_initialization=True,
        install_providers_from_sources=False,
        mount_sources=MOUNT_ALL,
    )
    rebuild_or_pull_ci_image_if_needed(command_params=shell_params)
    result_command = run_docker_command_with_debug(
        params=shell_params,
        command=["/opt/airflow/scripts/in_container/run_prepare_airflow_packages.sh"],
        debug=debug,
        output_outside_the_group=True,
    )
    sys.exit(result_command.returncode)


@release_management.command(
    name="prepare-provider-documentation",
    help="Prepare CHANGELOG, README and COMMITS information for providers.",
)
@option_debug_release_management
@click.option(
    "--base-branch",
    type=str,
    default="main",
    help="Base branch to use as diff for documentation generation (used for releasing from old branch)",
)
@option_github_repository
@click.option(
    "--only-min-version-update",
    is_flag=True,
    help="Only update minimum version in __init__.py files and regenerate corresponding documentation",
)
@click.option(
    "--regenerate-missing-docs",
    is_flag=True,
    help="Only regenerate missing documentation, do not bump version. Useful if templates were added"
    " and you need to regenerate documentation.",
)
@argument_provider_packages
@option_verbose
@option_dry_run
@option_answer
def prepare_provider_documentation(
    github_repository: str,
    base_branch: str,
    debug: bool,
    provider_packages: list[str],
    only_min_version_update: bool,
    regenerate_missing_docs: bool,
):
    perform_environment_checks()
    check_remote_ghcr_io_commands()
    cleanup_python_generated_files()
    shell_params = ShellParams(
        mount_sources=MOUNT_ALL,
        github_repository=github_repository,
        python=DEFAULT_PYTHON_MAJOR_MINOR_VERSION,
        base_branch=base_branch,
        only_min_version_update=only_min_version_update,
        regenerate_missing_docs=regenerate_missing_docs,
        skip_environment_initialization=True,
    )
    rebuild_or_pull_ci_image_if_needed(command_params=shell_params)
    cmd_to_run = [
        "/opt/airflow/scripts/in_container/run_prepare_provider_documentation.sh",
        *provider_packages,
    ]
    answer = get_forced_answer()
    result_command = run_docker_command_with_debug(
        params=shell_params,
        command=cmd_to_run,
        enable_input=answer is None or answer[0].lower() != "y",
        debug=debug,
    )
    sys.exit(result_command.returncode)


@release_management.command(
    name="prepare-provider-packages",
    help="Prepare sdist/whl packages of Airflow Providers.",
)
@option_package_format
@option_version_suffix_for_pypi
@click.option(
    "--package-list-file",
    type=click.File("rt"),
    help="Read list of packages from text file (one package per line).",
)
@option_debug_release_management
@argument_provider_packages
@option_github_repository
@option_verbose
@option_dry_run
def prepare_provider_packages(
    package_format: str,
    version_suffix_for_pypi: str,
    package_list_file: IO,
    debug: bool,
    provider_packages: tuple[str, ...],
    github_repository: str,
):
    perform_environment_checks()
    cleanup_python_generated_files()
    packages_list = list(provider_packages)

    removed_provider_ids = get_removed_provider_ids()
    if package_list_file:
        packages_list.extend(
            [
                package.strip()
                for package in package_list_file.readlines()
                if package.strip() not in removed_provider_ids
            ]
        )
    shell_params = ShellParams(
        mount_sources=MOUNT_ALL,
        github_repository=github_repository,
        python=DEFAULT_PYTHON_MAJOR_MINOR_VERSION,
        package_format=package_format,
        skip_environment_initialization=True,
        version_suffix_for_pypi=version_suffix_for_pypi,
    )
    rebuild_or_pull_ci_image_if_needed(command_params=shell_params)
    cmd_to_run = ["/opt/airflow/scripts/in_container/run_prepare_provider_packages.sh", *packages_list]
    result_command = run_docker_command_with_debug(
        params=shell_params,
        command=cmd_to_run,
        debug=debug,
    )
    sys.exit(result_command.returncode)


def run_generate_constraints(
    shell_params: ShellParams,
    debug: bool,
    output: Output | None,
) -> tuple[int, str]:
    cmd_to_run = [
        "/opt/airflow/scripts/in_container/run_generate_constraints.sh",
    ]
    generate_constraints_result = run_docker_command_with_debug(
        params=shell_params,
        command=cmd_to_run,
        debug=debug,
        output=output,
        output_outside_the_group=True,
    )
    return (
        generate_constraints_result.returncode,
        f"Constraints {shell_params.airflow_constraints_mode}:{shell_params.python}",
    )


CONSTRAINT_PROGRESS_MATCHER = (
    r"Found|Uninstalling|uninstalled|Collecting|Downloading|eta|Running|Installing|built|Attempting"
)


def run_generate_constraints_in_parallel(
    shell_params_list: list[ShellParams],
    python_version_list: list[str],
    include_success_outputs: bool,
    parallelism: int,
    skip_cleanup: bool,
    debug_resources: bool,
):
    """Run generate constraints in parallel"""
    with ci_group(f"Constraints for {python_version_list}"):
        all_params = [
            f"Constraints {shell_params.airflow_constraints_mode}:{shell_params.python}"
            for shell_params in shell_params_list
        ]
        with run_with_pool(
            parallelism=parallelism,
            all_params=all_params,
            debug_resources=debug_resources,
            progress_matcher=GenericRegexpProgressMatcher(
                regexp=CONSTRAINT_PROGRESS_MATCHER, lines_to_search=6
            ),
        ) as (pool, outputs):
            results = [
                pool.apply_async(
                    run_generate_constraints,
                    kwds={
                        "shell_params": shell_params,
                        "debug": False,
                        "output": outputs[index],
                    },
                )
                for index, shell_params in enumerate(shell_params_list)
            ]
    check_async_run_results(
        results=results,
        success="All constraints are generated.",
        outputs=outputs,
        include_success_outputs=include_success_outputs,
        skip_cleanup=skip_cleanup,
        summarize_on_ci=SummarizeAfter.SUCCESS,
        summary_start_regexp=".*Constraints generated in.*",
    )


@release_management.command(
    name="generate-constraints",
    help="Generates pinned constraint files with all extras from setup.py in parallel.",
)
@option_python
@option_run_in_parallel
@option_parallelism
@option_skip_cleanup
@option_debug_resources
@option_python_versions
@option_image_tag_for_running
@option_debug_release_management
@option_airflow_constraints_mode_ci
@option_github_repository
@option_verbose
@option_dry_run
@option_answer
def generate_constraints(
    python: str,
    run_in_parallel: bool,
    parallelism: int,
    skip_cleanup: bool,
    debug_resources: bool,
    python_versions: str,
    image_tag: str | None,
    debug: bool,
    airflow_constraints_mode: str,
    github_repository: str,
):
    perform_environment_checks()
    check_remote_ghcr_io_commands()
    cleanup_python_generated_files()
    if debug and run_in_parallel:
        get_console().print("\n[error]Cannot run --debug and --run-in-parallel at the same time[/]\n")
        sys.exit(1)
    if run_in_parallel:
        given_answer = user_confirm(
            f"Did you build all CI images {python_versions} with --upgrade-to-newer-dependencies flag set?",
        )
    else:
        given_answer = user_confirm(
            f"Did you build CI image {python} with --upgrade-to-newer-dependencies flag set?",
        )
    if given_answer != Answer.YES:
        if run_in_parallel:
            get_console().print("\n[info]Use this command to build the images:[/]\n")
            get_console().print(
                f"     breeze ci-image build --run-in-parallel --python-versions '{python_versions}' "
                f"--upgrade-to-newer-dependencies\n"
            )
        else:
            shell_params = ShellParams(
                image_tag=image_tag,
                python=python,
                github_repository=github_repository,
            )
            get_console().print("\n[info]Use this command to build the image:[/]\n")
            get_console().print(
                f"     breeze ci-image build --python '{shell_params.python}' "
                f"--upgrade-to-newer-dependencies\n"
            )
        sys.exit(1)
    if run_in_parallel:
        python_version_list = get_python_version_list(python_versions)
        shell_params_list = [
            ShellParams(
                image_tag=image_tag,
                python=python,
                github_repository=github_repository,
                airflow_constraints_mode=airflow_constraints_mode,
            )
            for python in python_version_list
        ]
        run_generate_constraints_in_parallel(
            shell_params_list=shell_params_list,
            parallelism=parallelism,
            skip_cleanup=skip_cleanup,
            debug_resources=debug_resources,
            include_success_outputs=True,
            python_version_list=python_version_list,
        )
    else:
        shell_params = ShellParams(
            image_tag=image_tag,
            python=python,
            github_repository=github_repository,
            skip_environment_initialization=True,
            airflow_constraints_mode=airflow_constraints_mode,
        )
        return_code, info = run_generate_constraints(
            shell_params=shell_params,
            output=None,
            debug=debug,
        )
        if return_code != 0:
            get_console().print(f"[error]There was an error when generating constraints: {info}[/]")
            sys.exit(return_code)


SDIST_FILENAME_PREFIX = "apache-airflow-providers-"
WHEEL_FILENAME_PREFIX = "apache_airflow_providers-"

SDIST_FILENAME_PATTERN = re.compile(rf"{SDIST_FILENAME_PREFIX}(.*)-[0-9].*\.tar\.gz")
WHEEL_FILENAME_PATTERN = re.compile(rf"{WHEEL_FILENAME_PREFIX}(.*)-[0-9].*\.whl")


def _get_all_providers_in_dist(
    filename_prefix: str, filename_pattern: re.Pattern[str]
) -> Generator[str, None, None]:
    for file in DIST_DIR.glob(f"{filename_prefix}*.tar.gz"):
        matched = filename_pattern.match(file.name)
        if not matched:
            raise Exception(f"Cannot parse provider package name from {file.name}")
        provider_package_id = matched.group(1).replace("-", ".")
        yield provider_package_id


def get_all_providers_in_dist(package_format: str, install_selected_providers: str) -> list[str]:
    """
    Returns all providers in dist, optionally filtered by install_selected_providers.

    :param package_format: package format to look for
    :param install_selected_providers: list of providers to filter by
    """
    if package_format == "sdist":
        all_found_providers = list(
            _get_all_providers_in_dist(
                filename_prefix=SDIST_FILENAME_PREFIX, filename_pattern=SDIST_FILENAME_PATTERN
            )
        )
    elif package_format == "wheel":
        all_found_providers = list(
            _get_all_providers_in_dist(
                filename_prefix=WHEEL_FILENAME_PREFIX, filename_pattern=WHEEL_FILENAME_PATTERN
            )
        )
    else:
        raise Exception(f"Unknown package format {package_format}")
    if install_selected_providers:
        filter_list = install_selected_providers.split(",")
        return [provider for provider in all_found_providers if provider in filter_list]
    return all_found_providers


def _run_command_for_providers(
    shell_params: ShellParams,
    cmd_to_run: list[str],
    list_of_providers: list[str],
    output: Output | None,
) -> tuple[int, str]:
    shell_params.install_selected_providers = " ".join(list_of_providers)
    result_command = run_docker_command_with_debug(
        params=shell_params,
        command=cmd_to_run,
        debug=False,
        output=output,
    )
    return result_command.returncode, f"{list_of_providers}"


SDIST_INSTALL_PROGRESS_REGEXP = r"Processing .*|Requirement already satisfied:.*|  Created wheel.*"


@release_management.command(
    name="install-provider-packages",
    help="Installs provider packages that can be found in dist.",
)
@option_use_airflow_version
@option_airflow_extras
@option_airflow_constraints_reference
@option_skip_constraints
@option_install_selected_providers
@option_installation_package_format
@option_debug_release_management
@option_github_repository
@option_verbose
@option_dry_run
@option_run_in_parallel
@option_skip_cleanup
@option_parallelism
@option_debug_resources
@option_include_success_outputs
def install_provider_packages(
    use_airflow_version: str | None,
    airflow_constraints_reference: str,
    skip_constraints: bool,
    install_selected_providers: str,
    airflow_extras: str,
    debug: bool,
    package_format: str,
    github_repository: str,
    run_in_parallel: bool,
    skip_cleanup: bool,
    parallelism: int,
    debug_resources: bool,
    include_success_outputs: bool,
):
    perform_environment_checks()
    cleanup_python_generated_files()
    shell_params = ShellParams(
        mount_sources=MOUNT_SELECTED,
        github_repository=github_repository,
        python=DEFAULT_PYTHON_MAJOR_MINOR_VERSION,
        use_airflow_version=use_airflow_version,
        airflow_extras=airflow_extras,
        airflow_constraints_reference=airflow_constraints_reference,
        install_selected_providers=install_selected_providers,
        use_packages_from_dist=True,
        skip_constraints=skip_constraints,
        package_format=package_format,
    )
    rebuild_or_pull_ci_image_if_needed(command_params=shell_params)
    # We just want to install the providers by entrypoint, we do not need to run any command in the container
    cmd_to_run = [
        "-c",
        "exit 0",
    ]
    if run_in_parallel:
        list_of_all_providers = get_all_providers_in_dist(
            package_format=package_format, install_selected_providers=install_selected_providers
        )
        get_console().print(
            f"[info]Splitting {len(list_of_all_providers)} providers into max {parallelism} chunks"
        )
        provider_chunks = [sorted(list_of_all_providers[i::parallelism]) for i in range(parallelism)]
        # filter out empty ones
        provider_chunks = [chunk for chunk in provider_chunks if chunk]
        if not provider_chunks:
            get_console().print("[info]No providers to install")
            return
        total_num_providers = 0
        for index, chunk in enumerate(provider_chunks):
            get_console().print(f"Chunk {index}: {chunk} ({len(chunk)} providers)")
            total_num_providers += len(chunk)
        # For every chunk make sure that all direct dependencies are installed as well
        # because there might be new version of the downstream dependency that is not
        # yet released in PyPI, so we need to make sure it is installed from dist
        for chunk in provider_chunks:
            for provider in chunk.copy():
                downstream_dependencies = get_related_providers(
                    provider, upstream_dependencies=False, downstream_dependencies=True
                )
                for dependency in downstream_dependencies:
                    if dependency not in chunk:
                        chunk.append(dependency)
        if len(list_of_all_providers) != total_num_providers:
            raise Exception(
                f"Total providers {total_num_providers} is different "
                f"than {len(list_of_all_providers)} (just to be sure"
                f" no rounding errors crippled in)"
            )
        parallelism = min(parallelism, len(provider_chunks))
        with ci_group(f"Installing providers in {parallelism} chunks"):
            all_params = [f"Chunk {n}" for n in range(parallelism)]
            with run_with_pool(
                parallelism=parallelism,
                all_params=all_params,
                debug_resources=debug_resources,
                progress_matcher=GenericRegexpProgressMatcher(
                    regexp=SDIST_INSTALL_PROGRESS_REGEXP, lines_to_search=10
                ),
            ) as (pool, outputs):
                results = [
                    pool.apply_async(
                        _run_command_for_providers,
                        kwds={
                            "shell_params": shell_params,
                            "cmd_to_run": cmd_to_run,
                            "list_of_providers": list_of_providers,
                            "output": outputs[index],
                        },
                    )
                    for index, list_of_providers in enumerate(provider_chunks)
                ]
        check_async_run_results(
            results=results,
            success="All packages installed successfully",
            outputs=outputs,
            include_success_outputs=include_success_outputs,
            skip_cleanup=skip_cleanup,
        )
    else:
        result_command = run_docker_command_with_debug(
            params=shell_params,
            command=cmd_to_run,
            debug=debug,
            output_outside_the_group=True,
        )
        sys.exit(result_command.returncode)


@release_management.command(
    name="verify-provider-packages",
    help="Verifies if all provider code is following expectations for providers.",
)
@option_use_airflow_version
@option_airflow_extras
@option_airflow_constraints_reference
@option_skip_constraints
@option_use_packages_from_dist
@option_install_selected_providers
@option_installation_package_format
@option_debug_release_management
@option_github_repository
@option_verbose
@option_dry_run
def verify_provider_packages(
    use_airflow_version: str | None,
    airflow_constraints_reference: str,
    skip_constraints: bool,
    install_selected_providers: str,
    airflow_extras: str,
    use_packages_from_dist: bool,
    debug: bool,
    package_format: str,
    github_repository: str,
):
    if install_selected_providers and not use_packages_from_dist:
        get_console().print("Forcing use_packages_from_dist as installing selected_providers is set")
        use_packages_from_dist = True
    perform_environment_checks()
    cleanup_python_generated_files()
    shell_params = ShellParams(
        mount_sources=MOUNT_SELECTED,
        github_repository=github_repository,
        python=DEFAULT_PYTHON_MAJOR_MINOR_VERSION,
        use_airflow_version=use_airflow_version,
        airflow_extras=airflow_extras,
        airflow_constraints_reference=airflow_constraints_reference,
        use_packages_from_dist=use_packages_from_dist,
        skip_constraints=skip_constraints,
        package_format=package_format,
    )
    rebuild_or_pull_ci_image_if_needed(command_params=shell_params)
    cmd_to_run = [
        "-c",
        "python /opt/airflow/scripts/in_container/verify_providers.py",
    ]
    result_command = run_docker_command_with_debug(
        params=shell_params,
        command=cmd_to_run,
        debug=debug,
        output_outside_the_group=True,
    )
    sys.exit(result_command.returncode)


def convert_build_args_dict_to_array_of_args(build_args: dict[str, str]) -> list[str]:
    array_of_args = []
    for key, value in build_args.items():
        array_of_args.append("--build-arg")
        array_of_args.append(f"{key}={value}")
    return array_of_args


def alias_image(image_from: str, image_to: str):
    get_console().print(f"[info]Creating {image_to} alias for {image_from}[/]")
    run_command(
        ["regctl", "image", "copy", "--force-recursive", "--digest-tags", image_from, image_to],
    )


def run_docs_publishing(
    package_name: str,
    airflow_site_directory: str,
    override_versioned: bool,
    verbose: bool,
    output: Output | None,
) -> tuple[int, str]:
    builder = PublishDocsBuilder(package_name=package_name, output=output, verbose=verbose)
    builder.publish(override_versioned=override_versioned, airflow_site_dir=airflow_site_directory)
    return (
        0,
        f"Docs published: {package_name}",
    )


PUBLISHING_DOCS_PROGRESS_MATCHER = r"Publishing docs|Copy directory"


def run_publish_docs_in_parallel(
    package_list: tuple[str, ...],
    airflow_site_directory: str,
    override_versioned: bool,
    include_success_outputs: bool,
    parallelism: int,
    skip_cleanup: bool,
    debug_resources: bool,
):
    """Run docs publishing in parallel"""
    with ci_group("Publishing docs for packages"):
        all_params = [f"Publishing docs {package_name}" for package_name in package_list]
        with run_with_pool(
            parallelism=parallelism,
            all_params=all_params,
            debug_resources=debug_resources,
            progress_matcher=GenericRegexpProgressMatcher(
                regexp=PUBLISHING_DOCS_PROGRESS_MATCHER, lines_to_search=6
            ),
        ) as (pool, outputs):
            results = [
                pool.apply_async(
                    run_docs_publishing,
                    kwds={
                        "package_name": package_name,
                        "airflow_site_directory": airflow_site_directory,
                        "override_versioned": override_versioned,
                        "output": outputs[index],
                        "verbose": get_verbose(),
                    },
                )
                for index, package_name in enumerate(package_list)
            ]
    check_async_run_results(
        results=results,
        success="All package documentation published.",
        outputs=outputs,
        include_success_outputs=include_success_outputs,
        skip_cleanup=skip_cleanup,
        summarize_on_ci=SummarizeAfter.NO_SUMMARY,
    )


@release_management.command(
    name="publish-docs",
    help="Command to publish generated documentation to airflow-site",
)
@click.option("-s", "--override-versioned", help="Overrides versioned directories.", is_flag=True)
@option_airflow_site_directory
@click.option(
    "--package-filter",
    help="List of packages to consider. You can use the full names like apache-airflow-providers-<provider>, "
    "the short hand names or the glob pattern matching the full package name. "
    "The list of short hand names can be found in --help output",
    type=str,
    multiple=True,
)
@option_run_in_parallel
@option_parallelism
@option_debug_resources
@option_include_success_outputs
@option_skip_cleanup
@argument_doc_packages
@option_verbose
@option_dry_run
def publish_docs(
    override_versioned: bool,
    airflow_site_directory: str,
    doc_packages: tuple[str, ...],
    package_filter: tuple[str, ...],
    run_in_parallel: bool,
    parallelism: int,
    debug_resources: bool,
    include_success_outputs: bool,
    skip_cleanup: bool,
):
    """Publishes documentation to airflow-site."""
    if not os.path.isdir(airflow_site_directory):
        get_console().print(
            "\n[error]location pointed by airflow_site_dir is not valid. "
            "Provide the path of cloned airflow-site repo\n"
        )

    current_packages = convert_to_long_package_names(
        package_filters=package_filter, packages_short_form=expand_all_provider_packages(doc_packages)
    )
    print(f"Publishing docs for {len(current_packages)} package(s)")
    for pkg in current_packages:
        print(f" - {pkg}")
    print()
    if run_in_parallel:
        run_publish_docs_in_parallel(
            package_list=current_packages,
            parallelism=parallelism,
            skip_cleanup=skip_cleanup,
            debug_resources=debug_resources,
            include_success_outputs=include_success_outputs,
            airflow_site_directory=airflow_site_directory,
            override_versioned=override_versioned,
        )
    else:
        for package_name in current_packages:
            run_docs_publishing(
                package_name, airflow_site_directory, override_versioned, verbose=get_verbose(), output=None
            )


@release_management.command(
    name="add-back-references",
    help="Command to add back references for documentation to make it backward compatible.",
)
@option_airflow_site_directory
@argument_doc_packages
@option_verbose
@option_dry_run
def add_back_references(
    airflow_site_directory: str,
    doc_packages: tuple[str, ...],
):
    """Adds back references for documentation generated by build-docs and publish-docs"""
    site_path = Path(airflow_site_directory)
    if not site_path.is_dir():
        get_console().print(
            "\n[error]location pointed by airflow_site_dir is not valid. "
            "Provide the path of cloned airflow-site repo\n"
        )
        sys.exit(1)
    if not doc_packages:
        get_console().print(
            "\n[error]You need to specify at least one package to generate back references for\n"
        )
        sys.exit(1)
    start_generating_back_references(site_path, list(expand_all_provider_packages(doc_packages)))


@release_management.command(
    name="release-prod-images", help="Release production images to DockerHub (needs DockerHub permissions)."
)
@click.option("--airflow-version", required=True, help="Airflow version to release (2.3.0, 2.3.0rc1 etc.)")
@click.option(
    "--dockerhub-repo",
    default=APACHE_AIRFLOW_GITHUB_REPOSITORY,
    show_default=True,
    help="DockerHub repository for the images",
)
@click.option(
    "--slim-images",
    is_flag=True,
    help="Whether to prepare slim images instead of the regular ones.",
)
@click.option(
    "--limit-python",
    type=BetterChoice(CURRENT_PYTHON_MAJOR_MINOR_VERSIONS),
    help="Specific python to build slim images for (if not specified - the images are built for all"
    " available python versions)",
)
@click.option(
    "--limit-platform",
    type=BetterChoice(ALLOWED_PLATFORMS),
    default=MULTI_PLATFORM,
    show_default=True,
    help="Specific platform to build images for (if not specified, multiplatform images will be built.",
)
@click.option(
    "--skip-latest",
    is_flag=True,
    help="Whether to skip publishing the latest images (so that 'latest' images are not updated). "
    "This should only be used if you release image for previous branches. Automatically set when "
    "rc/alpha/beta images are built.",
)
@option_commit_sha
@option_verbose
@option_dry_run
def release_prod_images(
    airflow_version: str,
    dockerhub_repo: str,
    slim_images: bool,
    limit_platform: str,
    limit_python: str | None,
    commit_sha: str | None,
    skip_latest: bool,
):
    perform_environment_checks()
    check_remote_ghcr_io_commands()
    rebuild_or_pull_ci_image_if_needed(command_params=ShellParams(python=DEFAULT_PYTHON_MAJOR_MINOR_VERSION))
    if not re.match(r"^\d*\.\d*\.\d*$", airflow_version):
        get_console().print(
            f"[warning]Skipping latest image tagging as this is a pre-release version: {airflow_version}"
        )
        skip_latest = True
    else:
        if skip_latest:
            get_console().print("[info]Skipping latest image tagging as user requested it.[/]")
        else:
            get_console().print(
                "[info]Also tagging the images with latest tags as this is release version.[/]"
            )
    result_docker_buildx = run_command(
        ["docker", "buildx", "version"],
        check=False,
    )
    if result_docker_buildx.returncode != 0:
        get_console().print("[error]Docker buildx plugin must be installed to release the images[/]")
        get_console().print()
        get_console().print("See https://docs.docker.com/buildx/working-with-buildx/ for installation info.")
        sys.exit(1)
    result_inspect_builder = run_command(["docker", "buildx", "inspect", "airflow_cache"], check=False)
    if result_inspect_builder.returncode != 0:
        get_console().print("[error]Airflow Cache builder must be configured to release the images[/]")
        get_console().print()
        get_console().print(
            "See https://github.com/apache/airflow/blob/main/dev/MANUALLY_BUILDING_IMAGES.md"
            " for instructions on setting it up."
        )
        sys.exit(1)
    result_regctl = run_command(["regctl", "version"], check=False)
    if result_regctl.returncode != 0:
        get_console().print("[error]Regctl must be installed and on PATH to release the images[/]")
        get_console().print()
        get_console().print(
            "See https://github.com/regclient/regclient/blob/main/docs/regctl.md for installation info."
        )
        sys.exit(1)
    python_versions = CURRENT_PYTHON_MAJOR_MINOR_VERSIONS if limit_python is None else [limit_python]
    for python in python_versions:
        if slim_images:
            slim_build_args = {
                "AIRFLOW_EXTRAS": "",
                "AIRFLOW_CONSTRAINTS": "constraints-no-providers",
                "PYTHON_BASE_IMAGE": f"python:{python}-slim-bookworm",
                "AIRFLOW_VERSION": airflow_version,
            }
            if commit_sha:
                slim_build_args["COMMIT_SHA"] = commit_sha
            get_console().print(f"[info]Building slim {airflow_version} image for Python {python}[/]")
            python_build_args = deepcopy(slim_build_args)
            slim_image_name = f"{dockerhub_repo}:slim-{airflow_version}-python{python}"
            docker_buildx_command = [
                "docker",
                "buildx",
                "build",
                "--builder",
                "airflow_cache",
                *convert_build_args_dict_to_array_of_args(build_args=python_build_args),
                "--platform",
                limit_platform,
                ".",
                "-t",
                slim_image_name,
                "--push",
            ]
            run_command(docker_buildx_command)
            if python == DEFAULT_PYTHON_MAJOR_MINOR_VERSION:
                alias_image(
                    slim_image_name,
                    f"{dockerhub_repo}:slim-{airflow_version}",
                )
        else:
            get_console().print(f"[info]Building regular {airflow_version} image for Python {python}[/]")
            image_name = f"{dockerhub_repo}:{airflow_version}-python{python}"
            regular_build_args = {
                "PYTHON_BASE_IMAGE": f"python:{python}-slim-bookworm",
                "AIRFLOW_VERSION": airflow_version,
            }
            if commit_sha:
                regular_build_args["COMMIT_SHA"] = commit_sha
            docker_buildx_command = [
                "docker",
                "buildx",
                "build",
                "--builder",
                "airflow_cache",
                *convert_build_args_dict_to_array_of_args(build_args=regular_build_args),
                "--platform",
                limit_platform,
                ".",
                "-t",
                image_name,
                "--push",
            ]
            run_command(docker_buildx_command)
            if python == DEFAULT_PYTHON_MAJOR_MINOR_VERSION:
                alias_image(image_name, f"{dockerhub_repo}:{airflow_version}")
    # in case of re-tagging the images might need few seconds to refresh multi-platform images in DockerHub
    time.sleep(10)
    if not skip_latest:
        get_console().print("[info]Replacing latest images with links to the newly created images.[/]")
        for python in python_versions:
            if slim_images:
                alias_image(
                    f"{dockerhub_repo}:slim-{airflow_version}-python{python}",
                    f"{dockerhub_repo}:slim-latest-python{python}",
                )
            else:
                alias_image(
                    f"{dockerhub_repo}:{airflow_version}-python{python}",
                    f"{dockerhub_repo}:latest-python{python}",
                )
        if python == DEFAULT_PYTHON_MAJOR_MINOR_VERSION:
            # only tag latest  "default" image when we build default python version
            # otherwise if the non-default images complete before the default one, their jobs will fail
            if slim_images:
                alias_image(
                    f"{dockerhub_repo}:slim-{airflow_version}",
                    f"{dockerhub_repo}:slim-latest",
                )
            else:
                alias_image(
                    f"{dockerhub_repo}:{airflow_version}",
                    f"{dockerhub_repo}:latest",
                )


def is_package_in_dist(dist_files: list[str], package: str) -> bool:
    """Check if package has been prepared in dist folder."""
    return any(
        file.startswith(
            (
                f'apache_airflow_providers_{package.replace(".", "_")}',
                f'apache-airflow-providers-{package.replace(".", "-")}',
            )
        )
        for file in dist_files
    )


def get_prs_for_package(package_id: str) -> list[int]:
    import yaml

    pr_matcher = re.compile(r".*\(#([0-9]*)\)``$")
    changelog_path = (
        AIRFLOW_SOURCES_ROOT / "airflow" / "providers" / package_id.replace(".", os.sep) / "CHANGELOG.rst"
    )
    # load yaml from file
    provider_yaml_dict = yaml.safe_load(
        (
            AIRFLOW_SOURCES_ROOT
            / "airflow"
            / "providers"
            / package_id.replace(r".", os.sep)
            / "provider.yaml"
        ).read_text()
    )
    current_release_version = provider_yaml_dict["versions"][0]
    prs = []
    with open(changelog_path) as changelog_file:
        changelog_lines = changelog_file.readlines()
        extract_prs = False
        skip_line = False
        for line in changelog_lines:
            if skip_line:
                # Skip first "....." header
                skip_line = False
            elif line.strip() == current_release_version:
                extract_prs = True
                skip_line = True
            elif extract_prs:
                if len(line) > 1 and all(c == "." for c in line.strip()):
                    # Header for next version reached
                    break
                if line.startswith(".. Below changes are excluded from the changelog"):
                    # The reminder of PRs is not important skipping it
                    break
                match_result = pr_matcher.match(line.strip())
                if match_result:
                    prs.append(int(match_result.group(1)))
    return prs


@release_management.command(
    name="generate-issue-content-providers", help="Generates content for issue to test the release."
)
@click.option(
    "--github-token",
    envvar="GITHUB_TOKEN",
    help=textwrap.dedent(
        """
      GitHub token used to authenticate.
      You can set omit it if you have GITHUB_TOKEN env variable set.
      Can be generated with:
      https://github.com/settings/tokens/new?description=Read%20sssues&scopes=repo:status"""
    ),
)
@click.option("--suffix", default="rc1", help="Suffix to add to the version prepared")
@click.option(
    "--only-available-in-dist",
    is_flag=True,
    help="Only consider package ids with packages prepared in the dist folder",
)
@click.option("--excluded-pr-list", type=str, help="Coma-separated list of PRs to exclude from the issue.")
@click.option("--disable-progress", is_flag=True, help="Disable progress bar")
@argument_provider_packages
def generate_issue_content_providers(
    provider_packages: list[str],
    github_token: str,
    suffix: str,
    only_available_in_dist: bool,
    excluded_pr_list: str,
    disable_progress: bool,
):
    import jinja2
    import yaml
    from github import Github, Issue, PullRequest, UnknownObjectException

    class ProviderPRInfo(NamedTuple):
        provider_package_id: str
        pypi_package_name: str
        version: str
        pr_list: list[PullRequest.PullRequest | Issue.Issue]

    if not provider_packages:
        provider_packages = list(DEPENDENCIES.keys())
    with ci_group("Generates GitHub issue content with people who can test it"):
        if excluded_pr_list:
            excluded_prs = [int(pr) for pr in excluded_pr_list.split(",")]
        else:
            excluded_prs = []
        all_prs: set[int] = set()
        provider_prs: dict[str, list[int]] = {}
        if only_available_in_dist:
            files_in_dist = os.listdir(str(AIRFLOW_SOURCES_ROOT / "dist"))
        prepared_package_ids = []
        for package_id in provider_packages:
            if not only_available_in_dist or is_package_in_dist(files_in_dist, package_id):
                get_console().print(f"Extracting PRs for provider {package_id}")
                prepared_package_ids.append(package_id)
            else:
                get_console().print(
                    f"Skipping extracting PRs for provider {package_id} as it is missing in dist"
                )
                continue
            prs = get_prs_for_package(package_id)
            provider_prs[package_id] = [pr for pr in prs if pr not in excluded_prs]
            all_prs.update(provider_prs[package_id])
        g = Github(github_token)
        repo = g.get_repo("apache/airflow")
        pull_requests: dict[int, PullRequest.PullRequest | Issue.Issue] = {}
        with Progress(console=get_console(), disable=disable_progress) as progress:
            task = progress.add_task(f"Retrieving {len(all_prs)} PRs ", total=len(all_prs))
            for pr_number in all_prs:
                progress.console.print(
                    f"Retrieving PR#{pr_number}: https://github.com/apache/airflow/pull/{pr_number}"
                )
                try:
                    pull_requests[pr_number] = repo.get_pull(pr_number)
                except UnknownObjectException:
                    # Fallback to issue if PR not found
                    try:
                        pull_requests[pr_number] = repo.get_issue(pr_number)  # (same fields as PR)
                    except UnknownObjectException:
                        get_console().print(f"[red]The PR #{pr_number} could not be found[/]")
                progress.advance(task)
        providers: dict[str, ProviderPRInfo] = {}
        for package_id in prepared_package_ids:
            pull_request_list = [pull_requests[pr] for pr in provider_prs[package_id] if pr in pull_requests]
            provider_yaml_dict = yaml.safe_load(
                (
                    AIRFLOW_SOURCES_ROOT
                    / "airflow"
                    / "providers"
                    / package_id.replace(".", os.sep)
                    / "provider.yaml"
                ).read_text()
            )
            if pull_request_list:
                providers[package_id] = ProviderPRInfo(
                    version=provider_yaml_dict["versions"][0],
                    provider_package_id=package_id,
                    pypi_package_name=provider_yaml_dict["package-name"],
                    pr_list=pull_request_list,
                )
        template = jinja2.Template(
            (Path(__file__).parents[1] / "provider_issue_TEMPLATE.md.jinja2").read_text()
        )
        issue_content = template.render(providers=providers, date=datetime.now(), suffix=suffix)
        get_console().print()
        get_console().print(
            "[green]Below you can find the issue content that you can use "
            "to ask contributor to test providers![/]"
        )
        get_console().print()
        get_console().print()
        get_console().print(
            "Issue title: [yellow]Status of testing Providers that were "
            f"prepared on {datetime.now():%B %d, %Y}[/]"
        )
        get_console().print()
        syntax = Syntax(issue_content, "markdown", theme="ansi_dark")
        get_console().print(syntax)
        get_console().print()
        users: set[str] = set()
        for provider_info in providers.values():
            for pr in provider_info.pr_list:
                users.add("@" + pr.user.login)
        get_console().print("All users involved in the PRs:")
        get_console().print(" ".join(users))


def get_all_constraint_files(refresh_constraints: bool, python_version: str) -> None:
    if refresh_constraints:
        shutil.rmtree(CONSTRAINTS_CACHE_DIR, ignore_errors=True)
    if not CONSTRAINTS_CACHE_DIR.exists():
        with ci_group(f"Downloading constraints for all Airflow versions for Python {python_version}"):
            CONSTRAINTS_CACHE_DIR.mkdir(parents=True, exist_ok=True)
            all_airflow_versions = get_active_airflow_versions(confirm=False)
            for airflow_version in all_airflow_versions:
                if not download_constraints_file(
                    airflow_version=airflow_version,
                    python_version=python_version,
                    include_provider_dependencies=True,
                    output_file=CONSTRAINTS_CACHE_DIR
                    / f"constraints-{airflow_version}-python-{python_version}.txt",
                ):
                    get_console().print(
                        "[warning]Could not download constraints for "
                        f"Airflow {airflow_version} and Python {python_version}[/]"
                    )


MATCH_CONSTRAINTS_FILE_REGEX = re.compile(r"constraints-(.*)-python-(.*).txt")


def load_constraints(python_version: str) -> dict[str, dict[str, str]]:
    constraints: dict[str, dict[str, str]] = {}
    for filename in CONSTRAINTS_CACHE_DIR.glob(f"constraints-*-python-{python_version}.txt"):
        filename_match = MATCH_CONSTRAINTS_FILE_REGEX.match(filename.name)
        if filename_match:
            airflow_version = filename_match.group(1)
            constraints[airflow_version] = {}
            for line in filename.read_text().splitlines():
                if line and not line.startswith("#"):
                    package, version = line.split("==")
                    constraints[airflow_version][package] = version
    return constraints


@release_management.command(name="generate-providers-metadata", help="Generates metadata for providers.")
@click.option(
    "--refresh-constraints",
    is_flag=True,
    help="Refresh constraints before generating metadata",
)
@option_historical_python_version
def generate_providers_metadata(refresh_constraints: bool, python: str | None):
    metadata_dict: dict[str, dict[str, dict[str, str]]] = {}
    if python is None:
        python = DEFAULT_PYTHON_MAJOR_MINOR_VERSION
    get_all_constraint_files(refresh_constraints=refresh_constraints, python_version=python)
    constraints = load_constraints(python_version=python)
    for package_id in DEPENDENCIES.keys():
        with ci_group(f"Generating metadata for {package_id}"):
            metadata = generate_providers_metadata_for_package(package_id, constraints)
            if metadata:
                metadata_dict[package_id] = metadata
    import json

    PROVIDER_METADATA_JSON_FILE_PATH.write_text(json.dumps(metadata_dict, indent=4, sort_keys=True))


def fetch_remote(constraints_repo: Path, remote_name: str) -> None:
    run_command(["git", "fetch", remote_name], cwd=constraints_repo)


def checkout_constraint_tag_and_reset_branch(constraints_repo: Path, airflow_version: str) -> None:
    run_command(
        ["git", "reset", "--hard"],
        cwd=constraints_repo,
    )
    # Switch to tag
    run_command(
        ["git", "checkout", f"constraints-{airflow_version}"],
        cwd=constraints_repo,
    )
    # Create or reset branch to point
    run_command(
        ["git", "checkout", "-B", f"constraints-{airflow_version}-fix"],
        cwd=constraints_repo,
    )
    get_console().print(
        f"[info]Checked out constraints tag: constraints-{airflow_version} and "
        f"reset branch constraints-{airflow_version}-fix to it.[/]"
    )
    result = run_command(
        ["git", "show", "-s", "--format=%H"],
        cwd=constraints_repo,
        text=True,
        capture_output=True,
    )
    get_console().print(f"[info]The hash commit of the tag:[/] {result.stdout}")


def update_comment(content: str, comment_file: Path) -> str:
    comment_text = comment_file.read_text()
    if comment_text in content:
        return content
    comment_lines = comment_text.splitlines()
    content_lines = content.splitlines()
    updated_lines: list[str] = []
    updated = False
    for line in content_lines:
        if not line.strip().startswith("#") and not updated:
            updated_lines.extend(comment_lines)
            updated = True
        updated_lines.append(line)
    return "".join(f"{line}\n" for line in updated_lines)


def modify_single_file_constraints(
    constraints_file: Path, updated_constraints: tuple[str, ...] | None, comment_file: Path | None
) -> bool:
    constraint_content = constraints_file.read_text()
    original_content = constraint_content
    if comment_file:
        constraint_content = update_comment(constraint_content, comment_file)
    if updated_constraints:
        for constraint in updated_constraints:
            package, version = constraint.split("==")
            constraint_content = re.sub(
                rf"^{package}==.*$", f"{package}=={version}", constraint_content, flags=re.MULTILINE
            )
    if constraint_content != original_content:
        if not get_dry_run():
            constraints_file.write_text(constraint_content)
        get_console().print("[success]Updated.[/]")
        return True
    else:
        get_console().print("[warning]The file has not been modified.[/]")
        return False


def modify_all_constraint_files(
    constraints_repo: Path,
    updated_constraint: tuple[str, ...] | None,
    comit_file: Path | None,
    airflow_constrains_mode: str | None,
) -> bool:
    get_console().print("[info]Updating constraints files:[/]")
    modified = False
    select_glob = "constraints-*.txt"
    if airflow_constrains_mode == "constraints":
        select_glob = "constraints-[0-9.]*.txt"
    elif airflow_constrains_mode == "constraints-source-providers":
        select_glob = "constraints-source-providers-[0-9.]*.txt"
    elif airflow_constrains_mode == "constraints-no-providers":
        select_glob = "constraints-no-providers-[0-9.]*.txt"
    else:
        raise RuntimeError(f"Invalid airflow-constraints-mode: {airflow_constrains_mode}")
    for constraints_file in constraints_repo.glob(select_glob):
        get_console().print(f"[info]Updating {constraints_file.name}")
        if modify_single_file_constraints(constraints_file, updated_constraint, comit_file):
            modified = True
    return modified


def confirm_modifications(constraints_repo: Path) -> bool:
    run_command(["git", "diff"], cwd=constraints_repo, env={"PAGER": ""})
    confirm = user_confirm("Do you want to continue?")
    if confirm == Answer.YES:
        return True
    elif confirm == Answer.NO:
        return False
    else:
        sys.exit(1)


def commit_constraints_and_tag(constraints_repo: Path, airflow_version: str, commit_message: str) -> None:
    run_command(
        ["git", "commit", "-a", "--no-verify", "-m", commit_message],
        cwd=constraints_repo,
    )
    run_command(
        ["git", "tag", f"constraints-{airflow_version}", "--force", "-s", "-m", commit_message, "HEAD"],
        cwd=constraints_repo,
    )


def push_constraints_and_tag(constraints_repo: Path, remote_name: str, airflow_version: str) -> None:
    run_command(
        ["git", "push", remote_name, f"constraints-{airflow_version}-fix"],
        cwd=constraints_repo,
    )
    run_command(
        ["git", "push", remote_name, f"constraints-{airflow_version}", "--force"],
        cwd=constraints_repo,
    )


@release_management.command(
    name="update-constraints", help="Update released constraints with manual changes."
)
@click.option(
    "--constraints-repo",
    type=click.Path(file_okay=False, dir_okay=True, path_type=Path, exists=True),
    required=True,
    envvar="CONSTRAINTS_REPO",
    help="Path where airflow repository is checked out, with ``constraints-main`` branch checked out.",
)
@click.option(
    "--remote-name",
    type=str,
    default="apache",
    envvar="REMOTE_NAME",
    help="Name of the remote to push the changes to.",
)
@click.option(
    "--airflow-versions",
    type=str,
    required=True,
    envvar="AIRFLOW_VERSIONS",
    help="Comma separated list of Airflow versions to update constraints for.",
)
@click.option(
    "--commit-message",
    type=str,
    required=True,
    envvar="COMMIT_MESSAGE",
    help="Commit message to use for the constraints update.",
)
@click.option(
    "--updated-constraint",
    required=False,
    envvar="UPDATED_CONSTRAINT",
    multiple=True,
    help="Constraints to be set - in the form of `package==version`. Can be repeated",
)
@click.option(
    "--comment-file",
    required=False,
    type=click.Path(file_okay=True, dir_okay=False, path_type=Path, exists=True),
    envvar="COMMENT_FILE",
    help="File containing comment to be added to the constraint "
    "file before the first package (if not added yet).",
)
@option_airflow_constraints_mode_update
@option_verbose
@option_dry_run
@option_answer
def update_constraints(
    constraints_repo: Path,
    remote_name: str,
    airflow_versions: str,
    commit_message: str,
    airflow_constraints_mode: str | None,
    updated_constraint: tuple[str, ...] | None,
    comment_file: Path | None,
) -> None:
    if not updated_constraint and not comment_file:
        get_console().print("[error]You have to provide one of --updated-constraint or --comment-file[/]")
        sys.exit(1)
    airflow_versions_array = airflow_versions.split(",")
    if not airflow_versions_array:
        get_console().print("[error]No airflow versions specified - you provided empty string[/]")
        sys.exit(1)
    get_console().print(f"Updating constraints for {airflow_versions_array} with {updated_constraint}")
    if (
        user_confirm(f"The {constraints_repo.name} repo will be reset. Continue?", quit_allowed=False)
        != Answer.YES
    ):
        sys.exit(1)
    fetch_remote(constraints_repo, remote_name)
    for airflow_version in airflow_versions_array:
        checkout_constraint_tag_and_reset_branch(constraints_repo, airflow_version)
        if modify_all_constraint_files(
            constraints_repo, updated_constraint, comment_file, airflow_constraints_mode
        ):
            if confirm_modifications(constraints_repo):
                commit_constraints_and_tag(constraints_repo, airflow_version, commit_message)
                push_constraints_and_tag(constraints_repo, remote_name, airflow_version)
