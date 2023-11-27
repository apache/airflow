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
from subprocess import DEVNULL
from typing import IO, Any, Generator, NamedTuple

import click
from rich.progress import Progress
from rich.syntax import Syntax

from airflow_breeze.commands.ci_image_commands import rebuild_or_pull_ci_image_if_needed
from airflow_breeze.commands.release_management_group import release_management
from airflow_breeze.global_constants import (
    ALLOWED_DEBIAN_VERSIONS,
    ALLOWED_PLATFORMS,
    APACHE_AIRFLOW_GITHUB_REPOSITORY,
    CURRENT_PYTHON_MAJOR_MINOR_VERSIONS,
    DEFAULT_PYTHON_MAJOR_MINOR_VERSION,
    MOUNT_SELECTED,
    MULTI_PLATFORM,
)
from airflow_breeze.params.shell_params import ShellParams
from airflow_breeze.prepare_providers.provider_packages import (
    PrepareReleasePackageErrorBuildingPackageException,
    PrepareReleasePackageTagExistException,
    PrepareReleasePackageWrongSetupException,
    build_provider_package,
    cleanup_build_remnants,
    copy_provider_sources_to_target,
    generate_build_files,
    get_packages_list_to_act_on,
    move_built_packages_and_cleanup,
    should_skip_the_package,
)
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
from airflow_breeze.utils.console import MessageType, Output, get_console
from airflow_breeze.utils.custom_param_types import BetterChoice
from airflow_breeze.utils.docker_command_utils import (
    check_remote_ghcr_io_commands,
    fix_ownership_using_docker,
    get_extra_docker_flags,
    perform_environment_checks,
)
from airflow_breeze.utils.github import download_constraints_file, get_active_airflow_versions
from airflow_breeze.utils.packages import (
    PackageSuspendedException,
    expand_all_provider_packages,
    find_matching_long_package_names,
    get_available_packages,
    get_provider_details,
    get_provider_packages_metadata,
    make_sure_remote_apache_exists_and_fetch,
)
from airflow_breeze.utils.parallel import (
    GenericRegexpProgressMatcher,
    SummarizeAfter,
    check_async_run_results,
    run_with_pool,
)
from airflow_breeze.utils.path_utils import (
    AIRFLOW_SOURCES_ROOT,
    AIRFLOW_WWW_DIR,
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
    clean_www_assets,
    run_command,
    run_compile_www_assets,
)
from airflow_breeze.utils.shared_options import get_dry_run, get_verbose

option_debug_release_management = click.option(
    "--debug",
    is_flag=True,
    help="Drop user in shell instead of running the command. Useful for debugging.",
    envvar="DEBUG",
)


def run_docker_command_with_debug(
    shell_params: ShellParams,
    command: list[str],
    debug: bool,
    enable_input: bool = False,
    output_outside_the_group: bool = False,
    **kwargs,
) -> RunCommandResult:
    env = shell_params.env_variables_for_docker_commands
    extra_docker_flags = get_extra_docker_flags(mount_sources=shell_params.mount_sources)
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
        shell_params.airflow_image_name_with_tag,
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
            output_outside_the_group=output_outside_the_group,
            env=env,
            **kwargs,
        )
    else:
        base_command.extend(command)
        return run_command(
            base_command,
            check=False,
            env=env,
            output_outside_the_group=output_outside_the_group,
            **kwargs,
        )


AIRFLOW_PIP_VERSION = "23.3.1"
WHEEL_VERSION = "0.36.2"
GITPYTHON_VERSION = "3.1.40"
RICH_VERSION = "13.7.0"


AIRFLOW_BUILD_DOCKERFILE = f"""
FROM python:{DEFAULT_PYTHON_MAJOR_MINOR_VERSION}-slim-{ALLOWED_DEBIAN_VERSIONS[0]}
RUN apt-get update && apt-get install -y --no-install-recommends git
RUN pip install pip=={AIRFLOW_PIP_VERSION} wheel=={WHEEL_VERSION} \\
   gitpython=={GITPYTHON_VERSION} rich=={RICH_VERSION}
"""

AIRFLOW_BUILD_IMAGE_TAG = "apache/airflow:local-build-image"
NODE_BUILD_IMAGE_TAG = "node:21.2.0-bookworm-slim"


def _compile_assets_in_docker():
    clean_www_assets()
    get_console().print("[info]Compiling assets in docker container\n")
    result = run_command(
        [
            "docker",
            "run",
            "-t",
            "-v",
            f"{AIRFLOW_WWW_DIR}:/opt/airflow/airflow/www/",
            "-e",
            "FORCE_COLOR=true",
            NODE_BUILD_IMAGE_TAG,
            "bash",
            "-c",
            "cd /opt/airflow/airflow/www && yarn install --frozen-lockfile && yarn run build",
        ],
        text=True,
        capture_output=not get_verbose(),
        check=False,
    )
    if result.returncode != 0:
        get_console().print("[error]Error compiling assets[/]")
        get_console().print(result.stdout)
        get_console().print(result.stderr)
        fix_ownership_using_docker()
        sys.exit(result.returncode)

    get_console().print("[success]compiled assets in docker container\n")
    get_console().print("[info]Fixing ownership of compiled assets\n")
    fix_ownership_using_docker()
    get_console().print("[success]Fixing ownership of compiled assets\n")


@release_management.command(
    name="prepare-airflow-package",
    help="Prepare sdist/whl package of Airflow.",
)
@option_package_format
@click.option(
    "--use-container-for-assets-compilation",
    is_flag=True,
    help="If set, the assets are compiled in docker container. On MacOS, asset compilation in containers "
    "is slower, due to slow mounted filesystem and number of node_module files so by default asset "
    "compilation is done locally. This option is useful for officially building packages by release "
    "manager on MacOS to make sure it is a reproducible build.",
)
@option_version_suffix_for_pypi
@option_verbose
@option_dry_run
def prepare_airflow_packages(
    package_format: str,
    version_suffix_for_pypi: str,
    use_container_for_assets_compilation: bool,
):
    perform_environment_checks()
    fix_ownership_using_docker()
    cleanup_python_generated_files()
    get_console().print("[info]Compiling assets\n")
    from sys import platform

    if platform == "darwin" and not use_container_for_assets_compilation:
        run_compile_www_assets(dev=False, run_in_background=False, force_clean=True)
    else:
        _compile_assets_in_docker()
    get_console().print("[success]Assets compiled successfully[/]")
    run_command(
        ["docker", "build", "--tag", AIRFLOW_BUILD_IMAGE_TAG, "-"],
        input=AIRFLOW_BUILD_DOCKERFILE,
        text=True,
        check=True,
        env={"DOCKER_CLI_HINTS": "false"},
    )
    run_command(
        cmd=[
            "docker",
            "run",
            "-t",
            "-v",
            f"{AIRFLOW_SOURCES_ROOT}:/opt/airflow:cached",
            "-e",
            f"VERSION_SUFFIX_FOR_PYPI={version_suffix_for_pypi}",
            "-e",
            "GITHUB_ACTIONS",
            "-e",
            f"PACKAGE_FORMAT={package_format}",
            AIRFLOW_BUILD_IMAGE_TAG,
            "python",
            "/opt/airflow/scripts/in_container/run_prepare_airflow_packages.py",
        ],
        check=True,
    )
    get_console().print("[success]Successfully prepared Airflow package!\n\n")
    get_console().print("\n[info]Cleaning ownership of generated files\n")
    fix_ownership_using_docker()
    get_console().print("\n[success]Cleaned ownership of generated files\n")


def provider_action_summary(description: str, message_type: MessageType, packages: list[str]):
    if packages:
        get_console().print(f"{description}: {len(packages)}\n")
        get_console().print(f"[{message_type.value}]{' '.join(packages)}")
        get_console().print()


@release_management.command(
    name="prepare-provider-documentation",
    help="Prepare CHANGELOG, README and COMMITS information for providers.",
)
@click.option(
    "--skip-git-fetch",
    is_flag=True,
    help="Skips removal and recreation of `apache-https-for-providers` remote in git. By default, the "
    "remote is recreated and fetched to make sure that it's up to date and that recent commits "
    "are not missing",
)
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
    "--reapply-templates-only",
    is_flag=True,
    help="Only reapply templates, do not bump version. Useful if templates were added"
    " and you need to regenerate documentation.",
)
@click.option(
    "--non-interactive",
    is_flag=True,
    help="Run in non-interactive mode. Provides random answers to the type of changes and confirms release"
    "for providers prepared for release - useful to test the script in non-interactive mode in CI.",
)
@argument_provider_packages
@option_verbose
@option_dry_run
@option_answer
def prepare_provider_documentation(
    github_repository: str,
    skip_git_fetch: bool,
    base_branch: str,
    provider_packages: tuple[str],
    only_min_version_update: bool,
    reapply_templates_only: bool,
    non_interactive: bool,
):
    from airflow_breeze.prepare_providers.provider_documentation import (
        PrepareReleaseDocsChangesOnlyException,
        PrepareReleaseDocsErrorOccurredException,
        PrepareReleaseDocsNoChangesException,
        PrepareReleaseDocsUserQuitException,
        PrepareReleaseDocsUserSkippedException,
        update_changelog,
        update_min_airflow_version,
        update_release_notes,
    )

    perform_environment_checks()
    fix_ownership_using_docker()
    cleanup_python_generated_files()
    if not provider_packages:
        provider_packages = get_available_packages()

    if not skip_git_fetch:
        run_command(["git", "remote", "rm", "apache-https-for-providers"], check=False, stderr=DEVNULL)
        make_sure_remote_apache_exists_and_fetch(github_repository=github_repository)
    no_changes_packages = []
    doc_only_packages = []
    error_packages = []
    user_skipped_packages = []
    success_packages = []
    suspended_packages = []
    removed_packages = []
    for provider_id in provider_packages:
        provider_metadata = basic_provider_checks(provider_id)
        if os.environ.get("GITHUB_ACTIONS", "false") != "true":
            get_console().print("-" * get_console().width)
        try:
            with_breaking_changes = False
            maybe_with_new_features = False
            with ci_group(f"Update release notes for package '{provider_id}' "):
                get_console().print("Updating documentation for the latest release version.")
                if not only_min_version_update:
                    with_breaking_changes, maybe_with_new_features = update_release_notes(
                        provider_id,
                        reapply_templates_only=reapply_templates_only,
                        base_branch=base_branch,
                        regenerate_missing_docs=reapply_templates_only,
                        non_interactive=non_interactive,
                    )
                update_min_airflow_version(
                    provider_package_id=provider_id,
                    with_breaking_changes=with_breaking_changes,
                    maybe_with_new_features=maybe_with_new_features,
                )
            with ci_group(f"Updates changelog for last release of package '{provider_id}'"):
                update_changelog(
                    package_id=provider_id,
                    base_branch=base_branch,
                    reapply_templates_only=reapply_templates_only,
                    with_breaking_changes=with_breaking_changes,
                    maybe_with_new_features=maybe_with_new_features,
                )
        except PrepareReleaseDocsNoChangesException:
            no_changes_packages.append(provider_id)
        except PrepareReleaseDocsChangesOnlyException:
            doc_only_packages.append(provider_id)
        except PrepareReleaseDocsErrorOccurredException:
            error_packages.append(provider_id)
        except PrepareReleaseDocsUserSkippedException:
            user_skipped_packages.append(provider_id)
        except PackageSuspendedException:
            suspended_packages.append(provider_id)
        except PrepareReleaseDocsUserQuitException:
            break
        else:
            if provider_metadata.get("removed"):
                removed_packages.append(provider_id)
            else:
                success_packages.append(provider_id)
    get_console().print()
    get_console().print("\n[info]Summary of prepared documentation:\n")
    provider_action_summary("Success", MessageType.SUCCESS, success_packages)
    provider_action_summary("Scheduled for removal", MessageType.SUCCESS, removed_packages)
    provider_action_summary("Docs only", MessageType.SUCCESS, doc_only_packages)
    provider_action_summary("Skipped on no changes", MessageType.WARNING, no_changes_packages)
    provider_action_summary("Suspended", MessageType.WARNING, suspended_packages)
    provider_action_summary("Skipped by user", MessageType.SPECIAL, user_skipped_packages)
    provider_action_summary("Errors", MessageType.ERROR, error_packages)
    if error_packages:
        get_console().print("\n[errors]There were errors when generating packages. Exiting!\n")
        sys.exit(1)
    if not success_packages and not doc_only_packages and not removed_packages:
        get_console().print("\n[warning]No packages prepared!\n")
        sys.exit(0)
    get_console().print("\n[success]Successfully prepared documentation for packages!\n\n")
    get_console().print(
        "\n[info]Please review the updated files, classify the changelog entries and commit the changes.\n"
    )


def basic_provider_checks(provider_package_id: str) -> dict[str, Any]:
    provider_packages_metadata = get_provider_packages_metadata()
    provider_metadata = provider_packages_metadata.get(provider_package_id)
    if not provider_metadata:
        get_console().print(f"[error]The package {provider_package_id} is not a provider package. Exiting[/]")
        sys.exit(1)
    if provider_metadata.get("removed", False):
        get_console().print(
            f"[warning]The package: {provider_package_id} is scheduled for removal, but "
            f"since you asked for it, it will be built [/]\n"
        )
    elif provider_metadata.get("suspended"):
        get_console().print(f"[warning]The package: {provider_package_id} is suspended " f"skipping it [/]\n")
        raise PackageSuspendedException()
    return provider_metadata


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
@click.option(
    "--skip-tag-check",
    default=False,
    is_flag=True,
    help="Skip checking if the tag already exists in the remote repository",
)
@click.option(
    "--skip-deleting-generated-files",
    default=False,
    is_flag=True,
    help="Skip deleting files that were used to generate provider package. Useful for debugging and "
    "developing changes to the build process.",
)
@click.option(
    "--clean-dist",
    default=False,
    is_flag=True,
    help="Clean dist directory before building packages. Useful when you want to build multiple packages "
    " in a clean environment",
)
@option_github_repository
@argument_provider_packages
@option_verbose
@option_dry_run
def prepare_provider_packages(
    package_format: str,
    version_suffix_for_pypi: str,
    package_list_file: IO | None,
    skip_tag_check: bool,
    skip_deleting_generated_files: bool,
    clean_dist: bool,
    github_repository: str,
    provider_packages: tuple[str, ...],
):
    perform_environment_checks()
    fix_ownership_using_docker()
    cleanup_python_generated_files()
    packages_list = get_packages_list_to_act_on(package_list_file, provider_packages)
    if not skip_tag_check:
        run_command(["git", "remote", "rm", "apache-https-for-providers"], check=False, stderr=DEVNULL)
        make_sure_remote_apache_exists_and_fetch(github_repository=github_repository)
    success_packages = []
    skipped_as_already_released_packages = []
    suspended_packages = []
    wrong_setup_packages = []
    error_packages = []
    if clean_dist:
        get_console().print("\n[warning]Cleaning dist directory before building packages[/]\n")
        shutil.rmtree(DIST_DIR, ignore_errors=True)
        DIST_DIR.mkdir(parents=True, exist_ok=True)
    for provider_id in packages_list:
        try:
            basic_provider_checks(provider_id)
            if not skip_tag_check and should_skip_the_package(provider_id, version_suffix_for_pypi):
                continue
            get_console().print()
            with ci_group(f"Preparing provider package [special]{provider_id}"):
                get_console().print()
                target_provider_root_sources_path = copy_provider_sources_to_target(provider_id)
                generate_build_files(
                    provider_id=provider_id,
                    version_suffix=version_suffix_for_pypi,
                    target_provider_root_sources_path=target_provider_root_sources_path,
                )
                cleanup_build_remnants(target_provider_root_sources_path)
                build_provider_package(
                    provider_id=provider_id,
                    package_format=package_format,
                    target_provider_root_sources_path=target_provider_root_sources_path,
                )
                move_built_packages_and_cleanup(
                    target_provider_root_sources_path, DIST_DIR, skip_cleanup=skip_deleting_generated_files
                )
        except PrepareReleasePackageTagExistException:
            skipped_as_already_released_packages.append(provider_id)
        except PrepareReleasePackageWrongSetupException:
            wrong_setup_packages.append(provider_id)
        except PrepareReleasePackageErrorBuildingPackageException:
            error_packages.append(provider_id)
        except PackageSuspendedException:
            suspended_packages.append(provider_id)
        else:
            get_console().print(f"\n[success]Generated package [special]{provider_id}")
            success_packages.append(provider_id)
    get_console().print()
    get_console().print("\n[info]Summary of prepared packages:\n")
    provider_action_summary("Success", MessageType.SUCCESS, success_packages)
    provider_action_summary(
        "Skipped as already released", MessageType.SUCCESS, skipped_as_already_released_packages
    )
    provider_action_summary("Suspended", MessageType.WARNING, suspended_packages)
    provider_action_summary("Wrong setup generated", MessageType.ERROR, wrong_setup_packages)
    provider_action_summary("Errors", MessageType.ERROR, error_packages)
    if error_packages or wrong_setup_packages:
        get_console().print("\n[errors]There were errors when generating packages. Exiting!\n")
        sys.exit(1)
    if not success_packages and not skipped_as_already_released_packages:
        get_console().print("\n[warning]No packages prepared!\n")
        sys.exit(0)
    get_console().print("\n[success]Successfully built packages!\n\n")
    get_console().print("\n[info]Packages available in dist:\n")
    for file in sorted(DIST_DIR.glob("apache*")):
        get_console().print(file.name)
    get_console().print()


def run_generate_constraints(
    shell_params: ShellParams,
    debug: bool,
    output: Output | None,
) -> tuple[int, str]:
    cmd_to_run = [
        "/opt/airflow/scripts/in_container/run_generate_constraints.sh",
    ]
    generate_constraints_result = run_docker_command_with_debug(
        shell_params=shell_params,
        command=cmd_to_run,
        debug=debug,
        output=output,
        output_outside_the_group=True,
    )
    fix_ownership_using_docker()
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
@click.option(
    "--chicken-egg-providers",
    default="",
    help="List of chicken-egg provider packages - "
    "those that have airflow_version >= current_version and should "
    "be installed in CI from locally built packages with >= current_version.dev0 ",
    envvar="CHICKEN_EGG_PROVIDERS",
)
@option_github_repository
@option_verbose
@option_dry_run
@option_answer
def generate_constraints(
    airflow_constraints_mode: str,
    debug: bool,
    debug_resources: bool,
    github_repository: str,
    image_tag: str | None,
    parallelism: int,
    python: str,
    python_versions: str,
    run_in_parallel: bool,
    skip_cleanup: bool,
    chicken_egg_providers: str,
):
    perform_environment_checks()
    check_remote_ghcr_io_commands()
    fix_ownership_using_docker()
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
                chicken_egg_providers=chicken_egg_providers,
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
            skip_image_upgrade_check=True,
            quiet=True,
            airflow_constraints_mode=airflow_constraints_mode,
            chicken_egg_providers=chicken_egg_providers,
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
        shell_params=shell_params,
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
    fix_ownership_using_docker()
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
            shell_params=shell_params,
            command=cmd_to_run,
            debug=debug,
            output_outside_the_group=True,
        )
        fix_ownership_using_docker()
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
    fix_ownership_using_docker()
    cleanup_python_generated_files()
    shell_params = ShellParams(
        backend="sqlite",
        executor="SequentialExecutor",
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
        shell_params=shell_params,
        command=cmd_to_run,
        debug=debug,
        output_outside_the_group=True,
    )
    fix_ownership_using_docker()
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

    current_packages = find_matching_long_package_names(
        short_packages=expand_all_provider_packages(doc_packages), filters=package_filter
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


def get_prs_for_package(provider_id: str) -> list[int]:
    pr_matcher = re.compile(r".*\(#([0-9]*)\)``$")
    prs = []
    provider_yaml_dict = get_provider_packages_metadata().get(provider_id)
    if not provider_yaml_dict:
        raise RuntimeError(f"The provider id {provider_id} does not have provider.yaml file")
    current_release_version = provider_yaml_dict["versions"][0]
    provider_details = get_provider_details(provider_id)
    changelog_lines = provider_details.changelog_path.read_text().splitlines()
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
        for provider_id in provider_packages:
            if not only_available_in_dist or is_package_in_dist(files_in_dist, provider_id):
                get_console().print(f"Extracting PRs for provider {provider_id}")
                prepared_package_ids.append(provider_id)
            else:
                get_console().print(
                    f"Skipping extracting PRs for provider {provider_id} as it is missing in dist"
                )
                continue
            prs = get_prs_for_package(provider_id)
            provider_prs[provider_id] = [pr for pr in prs if pr not in excluded_prs]
            all_prs.update(provider_prs[provider_id])
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
        for provider_id in prepared_package_ids:
            pull_request_list = [pull_requests[pr] for pr in provider_prs[provider_id] if pr in pull_requests]
            provider_yaml_dict = yaml.safe_load(
                (
                    AIRFLOW_SOURCES_ROOT
                    / "airflow"
                    / "providers"
                    / provider_id.replace(".", os.sep)
                    / "provider.yaml"
                ).read_text()
            )
            if pull_request_list:
                providers[provider_id] = ProviderPRInfo(
                    version=provider_yaml_dict["versions"][0],
                    provider_package_id=provider_id,
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
    for filename in sorted(CONSTRAINTS_CACHE_DIR.glob(f"constraints-*-python-{python_version}.txt")):
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
    for package_id in sorted(DEPENDENCIES.keys()):
        with ci_group(f"Generating metadata for {package_id}"):
            metadata = generate_providers_metadata_for_package(package_id, constraints)
            if metadata:
                metadata_dict[package_id] = metadata
    import json

    PROVIDER_METADATA_JSON_FILE_PATH.write_text(json.dumps(metadata_dict, indent=4))


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
