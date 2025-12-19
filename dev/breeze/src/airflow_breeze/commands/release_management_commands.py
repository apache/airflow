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

import ast
import glob
import operator
import os
import random
import re
import shutil
import subprocess
import sys
import tempfile
import textwrap
import time
from collections import defaultdict
from collections.abc import Generator, Iterable
from copy import deepcopy
from datetime import datetime
from enum import Enum
from functools import partial
from multiprocessing import Pool
from pathlib import Path
from subprocess import DEVNULL
from typing import IO, TYPE_CHECKING, Any, Literal, NamedTuple

import click
from rich.progress import Progress
from rich.syntax import Syntax

from airflow_breeze.branch_defaults import AIRFLOW_BRANCH
from airflow_breeze.commands.ci_image_commands import rebuild_or_pull_ci_image_if_needed
from airflow_breeze.commands.common_options import (
    argument_doc_packages,
    option_airflow_extras,
    option_allow_pre_releases,
    option_answer,
    option_builder,
    option_clean_airflow_installation,
    option_commit_sha,
    option_debug_resources,
    option_dry_run,
    option_github_repository,
    option_github_token,
    option_include_not_ready_providers,
    option_include_removed_providers,
    option_include_success_outputs,
    option_install_airflow_with_constraints,
    option_install_airflow_with_constraints_default_true,
    option_installation_distribution_format,
    option_mount_sources,
    option_parallelism,
    option_python,
    option_python_no_default,
    option_python_versions,
    option_run_in_parallel,
    option_skip_cleanup,
    option_use_airflow_version,
    option_use_uv,
    option_verbose,
    option_version_suffix,
)
from airflow_breeze.commands.common_package_installation_options import (
    option_airflow_constraints_location,
    option_airflow_constraints_mode_ci,
    option_airflow_constraints_mode_update,
    option_airflow_constraints_reference,
    option_install_selected_providers,
    option_providers_constraints_location,
    option_providers_constraints_mode_ci,
    option_providers_constraints_reference,
    option_providers_skip_constraints,
    option_use_distributions_from_dist,
)
from airflow_breeze.commands.release_management_group import release_management_group
from airflow_breeze.global_constants import (
    ALL_PYTHON_VERSION_TO_PATCHLEVEL_VERSION,
    ALLOWED_DEBIAN_VERSIONS,
    ALLOWED_DISTRIBUTION_FORMATS,
    ALLOWED_PLATFORMS,
    APACHE_AIRFLOW_GITHUB_REPOSITORY,
    CONSTRAINTS,
    CURRENT_PYTHON_MAJOR_MINOR_VERSIONS,
    DEFAULT_PYTHON_MAJOR_MINOR_VERSION,
    DEFAULT_PYTHON_MAJOR_MINOR_VERSION_FOR_IMAGES,
    DESTINATION_LOCATIONS,
    MULTI_PLATFORM,
    UV_VERSION,
)
from airflow_breeze.params.build_ci_params import BuildCiParams
from airflow_breeze.params.shell_params import ShellParams
from airflow_breeze.prepare_providers.provider_distributions import (
    PrepareReleasePackageErrorBuildingPackageException,
    PrepareReleasePackageTagExistException,
    PrepareReleasePackageWrongSetupException,
    build_provider_distribution,
    cleanup_build_remnants,
    get_packages_list_to_act_on,
    move_built_distributions_and_cleanup,
    should_skip_the_package,
)
from airflow_breeze.utils.add_back_references import (
    start_generating_back_references,
)
from airflow_breeze.utils.ci_group import ci_group
from airflow_breeze.utils.click_validators import validate_release_date
from airflow_breeze.utils.confirm import Answer, user_confirm
from airflow_breeze.utils.console import MessageType, Output, get_console
from airflow_breeze.utils.constraints_version_check import constraints_version_check
from airflow_breeze.utils.custom_param_types import BetterChoice, NotVerifiedBetterChoice
from airflow_breeze.utils.debug_pyproject_toml import debug_pyproject_tomls
from airflow_breeze.utils.docker_command_utils import (
    check_airflow_cache_builder_configured,
    check_docker_buildx_plugin,
    check_regctl_installed,
    check_remote_ghcr_io_commands,
    execute_command_in_shell,
    fix_ownership_using_docker,
    perform_environment_checks,
)
from airflow_breeze.utils.packages import (
    PackageSuspendedException,
    apply_version_suffix_to_non_provider_pyproject_tomls,
    apply_version_suffix_to_provider_pyproject_toml,
    expand_all_provider_distributions,
    find_matching_long_package_names,
    get_available_distributions,
    get_provider_details,
    get_provider_distributions_metadata,
    make_sure_remote_apache_exists_and_fetch,
)
from airflow_breeze.utils.parallel import (
    GenericRegexpProgressMatcher,
    SummarizeAfter,
    check_async_run_results,
    run_with_pool,
)
from airflow_breeze.utils.path_utils import (
    AIRFLOW_CORE_ROOT_PATH,
    AIRFLOW_CORE_SOURCES_PATH,
    AIRFLOW_CTL_DIST_PATH,
    AIRFLOW_CTL_ROOT_PATH,
    AIRFLOW_CTL_SOURCES_PATH,
    AIRFLOW_DIST_PATH,
    AIRFLOW_PROVIDERS_LAST_RELEASE_DATE_PATH,
    AIRFLOW_ROOT_PATH,
    OUT_PATH,
    PROVIDER_METADATA_JSON_PATH,
    TASK_SDK_DIST_PATH,
    TASK_SDK_ROOT_PATH,
    TASK_SDK_SOURCES_PATH,
    cleanup_python_generated_files,
)
from airflow_breeze.utils.provider_dependencies import (
    generate_providers_metadata_for_provider,
    get_all_constraint_files_and_airflow_releases,
    get_provider_dependencies,
    get_related_providers,
    load_constraints,
    regenerate_provider_dependencies_once,
)
from airflow_breeze.utils.python_versions import get_python_version_list
from airflow_breeze.utils.reproducible import get_source_date_epoch, repack_deterministically
from airflow_breeze.utils.run_utils import (
    run_command,
)
from airflow_breeze.utils.shared_options import get_dry_run, get_verbose, set_forced_answer
from airflow_breeze.utils.version_utils import (
    is_local_package_version,
)
from airflow_breeze.utils.versions import is_pre_release
from airflow_breeze.utils.virtualenv_utils import create_venv

if TYPE_CHECKING:
    from packaging.version import Version

argument_provider_distributions = click.argument(
    "provider_distributions",
    nargs=-1,
    required=False,
    type=NotVerifiedBetterChoice(get_available_distributions(include_removed=False, include_not_ready=False)),
)
option_airflow_site_directory = click.option(
    "-a",
    "--airflow-site-directory",
    envvar="AIRFLOW_SITE_DIRECTORY",
    type=click.Path(exists=True, file_okay=False, dir_okay=True, resolve_path=True),
    help="Local directory path of cloned airflow-site repo.",
    required=True,
)
option_debug_release_management = click.option(
    "--debug",
    is_flag=True,
    help="Drop user in shell instead of running the command. Useful for debugging.",
    envvar="DEBUG",
)
option_directory = click.option(
    "--directory",
    type=click.Path(exists=True, file_okay=False, dir_okay=True, resolve_path=True),
    required=True,
    help="Directory to clean the provider artifacts from.",
)
option_distribution_format = click.option(
    "--distribution-format",
    type=BetterChoice(ALLOWED_DISTRIBUTION_FORMATS),
    help="Format of packages.",
    default=ALLOWED_DISTRIBUTION_FORMATS[0],
    show_default=True,
    envvar="DISTRIBUTION_FORMAT",
)
option_use_local_hatch = click.option(
    "--use-local-hatch",
    is_flag=True,
    envvar="USE_LOCAL_HATCH",
    help="Use local hatch instead of docker to build the package. You need to have hatch installed.",
)

MY_DIR_PATH = os.path.dirname(__file__)
SOURCE_DIR_PATH = os.path.abspath(
    os.path.join(MY_DIR_PATH, os.pardir, os.pardir, os.pardir, os.pardir, os.pardir)
)
PR_PATTERN = re.compile(r".*\(#([0-9]+)\)")
ISSUE_MATCH_IN_BODY = re.compile(r" #([0-9]+)[^0-9]")


def remove_code_blocks(text: str) -> str:
    """Remove content within code blocks (```...``` and `...`) from text."""
    text = re.sub(r"```.*?```", "", text, flags=re.DOTALL)
    text = re.sub(r"`[^`]+`", "", text)
    return text


class VersionedFile(NamedTuple):
    base: str
    version: str
    suffix: str
    type: str
    comparable_version: Version
    file_name: str


AIRFLOW_PIP_VERSION = "25.3"
AIRFLOW_UV_VERSION = "0.9.18"
AIRFLOW_USE_UV = False
GITPYTHON_VERSION = "3.1.45"
RICH_VERSION = "14.2.0"
PREK_VERSION = "0.2.22"
HATCH_VERSION = "1.16.2"
PYYAML_VERSION = "6.0.3"

# prek environment and this is done with node, no python installation is needed.
AIRFLOW_BUILD_DOCKERFILE = f"""
# syntax=docker/dockerfile:1.4
FROM python:{DEFAULT_PYTHON_MAJOR_MINOR_VERSION}-slim-{ALLOWED_DEBIAN_VERSIONS[0]}
RUN apt-get update && apt-get install -y --no-install-recommends libatomic1 git curl
RUN pip install uv=={UV_VERSION}
RUN --mount=type=cache,id=cache-airflow-build-dockerfile-installation,target=/root/.cache/ \
  uv pip install --system ignore pip=={AIRFLOW_PIP_VERSION} hatch=={HATCH_VERSION} \
  pyyaml=={PYYAML_VERSION} gitpython=={GITPYTHON_VERSION} rich=={RICH_VERSION} \
  prek=={PREK_VERSION}
COPY . /opt/airflow
"""

AIRFLOW_BUILD_IMAGE_TAG = "apache/airflow:local-build-image"

AIRFLOW_BUILD_DOCKERFILE_PATH = AIRFLOW_ROOT_PATH / "airflow-build-dockerfile"
AIRFLOW_BUILD_DOCKERFILE_DOCKERIGNORE_PATH = AIRFLOW_ROOT_PATH / "airflow-build-dockerfile.dockerignore"
AIRFLOW_DOCKERIGNORE_PATH = AIRFLOW_ROOT_PATH / ".dockerignore"


class DistributionBuildType(Enum):
    """Type of the Python distribution to build."""

    AIRFLOW = "airflow"
    PROVIDERS = "providers"
    TASK_SDK = "task-sdk"
    AIRFLOW_CTL = "airflow-ctl"


class DistributionPackageInfo(NamedTuple):
    filepath: Path
    package: str
    version: Version
    dist_type: Literal["sdist", "wheel"]

    @classmethod
    def from_sdist(cls, filepath: Path) -> DistributionPackageInfo:
        from packaging.utils import parse_sdist_filename

        package, version = parse_sdist_filename(filepath.name)
        return cls(
            filepath=filepath.resolve().absolute(), package=package, version=version, dist_type="sdist"
        )

    @classmethod
    def from_wheel(cls, filepath: Path) -> DistributionPackageInfo:
        from packaging.utils import parse_wheel_filename

        package, version, *_ = parse_wheel_filename(filepath.name)
        return cls(
            filepath=filepath.resolve().absolute(), package=package, version=version, dist_type="wheel"
        )

    @classmethod
    def dist_packages(
        cls,
        *,
        distribution_format: str,
        dist_directory: Path,
        build_type: DistributionBuildType,
    ) -> tuple[DistributionPackageInfo, ...]:
        if build_type == DistributionBuildType.AIRFLOW:
            default_glob_patterns = ["apache_airflow-", "apache_airflow_core-"]
        elif build_type == DistributionBuildType.TASK_SDK:
            default_glob_patterns = ["apache_airflow_task_sdk"]
        elif build_type == DistributionBuildType.AIRFLOW_CTL:
            default_glob_patterns = ["apache_airflow_ctl"]
        else:
            default_glob_patterns = ["apache_airflow_providers"]
        dists_info = []
        if distribution_format in ["sdist", "both"]:
            for default_glob_pattern in default_glob_patterns:
                for file in dist_directory.glob(f"{default_glob_pattern}*.tar.gz"):
                    if not file.is_file() or "-source.tar.gz" in file.name:
                        continue
                    dists_info.append(cls.from_sdist(filepath=file))
        if distribution_format in ["wheel", "both"]:
            for default_glob_pattern in default_glob_patterns:
                for file in dist_directory.glob(f"{default_glob_pattern}*.whl"):
                    if not file.is_file():
                        continue
                    dists_info.append(cls.from_wheel(filepath=file))
        return tuple(sorted(dists_info, key=lambda di: (di.package, di.dist_type)))

    def __str__(self):
        return f"{self.package} ({self.version}): {self.dist_type} - {self.filepath.name}"


def _build_local_build_image():
    # This is security feature.
    #
    # Building the image needed to build airflow package including .git directory
    # In isolated environment, to not allow the in-docker code to override local code
    # The image used to build airflow package is built from scratch and contains
    # Full Airflow code including Airflow repository is added to the image, but locally build node_modules
    # are not added to the context of that image
    AIRFLOW_BUILD_DOCKERFILE_PATH.unlink(missing_ok=True)
    AIRFLOW_BUILD_DOCKERFILE_PATH.write_text(AIRFLOW_BUILD_DOCKERFILE)
    AIRFLOW_BUILD_DOCKERFILE_DOCKERIGNORE_PATH.unlink(missing_ok=True)
    dockerignore_content = AIRFLOW_DOCKERIGNORE_PATH.read_text()
    dockerignore_content = dockerignore_content + textwrap.dedent("""
        # Include git in the build context - we need to get git version and prek configuration
        # And clients python code to be included in the context
        !.git
        !.pre-commit-config.yaml
        !clients/python
    """)
    AIRFLOW_BUILD_DOCKERFILE_DOCKERIGNORE_PATH.write_text(dockerignore_content)
    run_command(
        [
            "docker",
            "build",
            ".",
            "-f",
            "airflow-build-dockerfile",
            "--load",
            "--tag",
            AIRFLOW_BUILD_IMAGE_TAG,
        ],
        text=True,
        check=True,
        cwd=AIRFLOW_ROOT_PATH,
        env={"DOCKER_CLI_HINTS": "false"},
    )


def _build_airflow_packages_with_docker(
    distribution_format: str, source_date_epoch: int, version_suffix: str
):
    with apply_version_suffix_to_non_provider_pyproject_tomls(
        version_suffix=version_suffix,
        init_file_path=AIRFLOW_CORE_SOURCES_PATH / "airflow" / "__init__.py",
        pyproject_toml_paths=[
            AIRFLOW_ROOT_PATH / "pyproject.toml",
            AIRFLOW_CORE_ROOT_PATH / "pyproject.toml",
        ],
    ) as pyproject_toml_paths:
        debug_pyproject_tomls(pyproject_toml_paths)
        _build_local_build_image()
        container_id = f"airflow-build-{random.getrandbits(64):08x}"
        result = run_command(
            cmd=[
                "docker",
                "run",
                "--name",
                container_id,
                "-t",
                "-e",
                f"SOURCE_DATE_EPOCH={source_date_epoch}",
                "-e",
                "HOME=/opt/airflow/files/home",
                "-e",
                "GITHUB_ACTIONS",
                "-e",
                f"DISTRIBUTION_FORMAT={distribution_format}",
                "-w",
                "/opt/airflow",
                AIRFLOW_BUILD_IMAGE_TAG,
                "python",
                "/opt/airflow/scripts/in_container/run_prepare_airflow_distributions.py",
            ],
            check=False,
        )
    if result.returncode != 0:
        get_console().print("[error]Error preparing Airflow package[/]")
        fix_ownership_using_docker()
        sys.exit(result.returncode)
    AIRFLOW_DIST_PATH.mkdir(parents=True, exist_ok=True)
    # Copy all files in the dist directory in container to the host dist directories (note '/.' in SRC)
    run_command(["docker", "cp", f"{container_id}:/opt/airflow/dist/.", "./dist"], check=True)
    run_command(["docker", "cp", f"{container_id}:/opt/airflow/airflow-core/dist/.", "./dist"], check=True)
    run_command(["docker", "rm", "--force", container_id], check=False, stderr=DEVNULL, stdout=DEVNULL)


def apply_distribution_format_to_hatch_command(build_command: list[str], distribution_format: str):
    if distribution_format in ["sdist", "both"]:
        build_command.extend(["-t", "sdist"])
    if distribution_format in ["wheel", "both"]:
        build_command.extend(["-t", "wheel"])


def _build_airflow_packages_with_hatch(distribution_format: str, source_date_epoch: int, version_suffix: str):
    env_copy = os.environ.copy()
    env_copy["SOURCE_DATE_EPOCH"] = str(source_date_epoch)
    build_airflow_core_command = ["hatch", "build", "-c", "-t", "custom"]
    apply_distribution_format_to_hatch_command(build_airflow_core_command, distribution_format)
    get_console().print(f"[bright_blue]Building apache-airflow-core distributions: {distribution_format}\n")
    with apply_version_suffix_to_non_provider_pyproject_tomls(
        version_suffix=version_suffix,
        init_file_path=AIRFLOW_CORE_SOURCES_PATH / "airflow" / "__init__.py",
        pyproject_toml_paths=[AIRFLOW_CORE_ROOT_PATH / "pyproject.toml"],
    ) as pyproject_toml_paths:
        debug_pyproject_tomls(pyproject_toml_paths)
        run_command(
            build_airflow_core_command,
            check=True,
            env=env_copy,
            cwd=AIRFLOW_CORE_ROOT_PATH,
        )
    get_console().print(f"[bright_blue]Building apache-airflow distributions: {distribution_format}\n")
    build_airflow_command = ["hatch", "build", "-c"]
    apply_distribution_format_to_hatch_command(build_airflow_command, distribution_format)
    with apply_version_suffix_to_non_provider_pyproject_tomls(
        version_suffix=version_suffix,
        init_file_path=AIRFLOW_CORE_SOURCES_PATH / "airflow" / "__init__.py",
        pyproject_toml_paths=[AIRFLOW_ROOT_PATH / "pyproject.toml"],
    ) as pyproject_toml_paths:
        debug_pyproject_tomls(pyproject_toml_paths)
        run_command(
            build_airflow_command,
            check=True,
            env=env_copy,
            cwd=AIRFLOW_ROOT_PATH,
        )
    for distribution_path in (AIRFLOW_CORE_ROOT_PATH / "dist").glob("apache_airflow_core*"):
        shutil.move(distribution_path, AIRFLOW_DIST_PATH)


def _check_sdist_to_wheel_dists(dists_info: tuple[DistributionPackageInfo, ...]):
    venv_created = False
    success_build = True
    with tempfile.TemporaryDirectory() as tmp_dir_name:
        for di in dists_info:
            if di.dist_type != "sdist":
                continue

            if not venv_created:
                python_path = create_venv(
                    Path(tmp_dir_name) / ".venv",
                    pip_version=AIRFLOW_PIP_VERSION,
                    uv_version=AIRFLOW_UV_VERSION,
                )
                venv_created = True

            returncode = _check_sdist_to_wheel(python_path, di, str(tmp_dir_name))
            if returncode != 0:
                success_build = False

    if not success_build:
        get_console().print(
            "\n[errors]Errors detected during build wheel distribution(s) from sdist. Exiting!\n"
        )
        sys.exit(1)


def _check_sdist_to_wheel(python_path: Path, dist_info: DistributionPackageInfo, cwd: str) -> int:
    get_console().print(
        f"[info]Validate build wheel from sdist distribution for package {dist_info.package!r}.[/]"
    )
    result_build_wheel = run_command(
        [
            "uv",
            "build",
            "--wheel",
            "--out-dir",
            cwd,
            dist_info.filepath.as_posix(),
        ],
        check=False,
        # We should run `pip wheel` outside of Project directory for avoid the case
        # when some files presented into the project directory, but not included in sdist.
        cwd=cwd,
        capture_output=True,
        text=True,
    )
    if (returncode := result_build_wheel.returncode) == 0:
        get_console().print(
            f"[success]Successfully build wheel from sdist distribution for package {dist_info.package!r}.[/]"
        )
    else:
        get_console().print(
            f"[error]Unable to build wheel from sdist distribution for package {dist_info.package!r}.[/]\n"
            f"{result_build_wheel.stdout}\n{result_build_wheel.stderr}"
        )
    return returncode


@release_management_group.command(
    name="prepare-airflow-distributions",
    help="Prepare sdist/whl package of Airflow.",
)
@option_distribution_format
@option_version_suffix
@option_use_local_hatch
@option_verbose
@option_dry_run
def prepare_airflow_distributions(
    distribution_format: str,
    version_suffix: str,
    use_local_hatch: bool,
):
    perform_environment_checks()
    fix_ownership_using_docker()
    cleanup_python_generated_files()
    source_date_epoch = get_source_date_epoch(AIRFLOW_ROOT_PATH)
    if use_local_hatch:
        _build_airflow_packages_with_hatch(
            distribution_format=distribution_format,
            source_date_epoch=source_date_epoch,
            version_suffix=version_suffix,
        )
        get_console().print("[info]Checking if sdist packages can be built into wheels[/]")
        packages = DistributionPackageInfo.dist_packages(
            distribution_format=distribution_format,
            dist_directory=AIRFLOW_DIST_PATH,
            build_type=DistributionBuildType.AIRFLOW,
        )
        get_console().print()
        _check_sdist_to_wheel_dists(packages)
        get_console().print("\n[info]Packages available in dist:[/]\n")
        for dist_info in packages:
            get_console().print(str(dist_info))
        get_console().print()
    else:
        _build_airflow_packages_with_docker(
            distribution_format=distribution_format,
            source_date_epoch=source_date_epoch,
            version_suffix=version_suffix,
        )
    get_console().print("[success]Successfully prepared Airflow packages")


def _prepare_non_core_distributions(
    distribution_format: str,
    version_suffix: str,
    use_local_hatch: bool,
    root_path: Path,
    init_file_path: Path,
    distribution_path: Path,
    distribution_name: str,
    distribution_pretty_name: str,
    full_distribution_pretty_name: str | None = None,
):
    if full_distribution_pretty_name is not None:
        distribution_pretty_name = full_distribution_pretty_name
    perform_environment_checks()
    fix_ownership_using_docker()
    cleanup_python_generated_files()
    source_date_epoch = get_source_date_epoch(AIRFLOW_ROOT_PATH)

    def _build_package_with_hatch(build_distribution_format: str):
        command = [
            "hatch",
            "build",
            "-c",
        ]
        if build_distribution_format == "sdist" or build_distribution_format == "both":
            command += ["-t", "sdist"]
        if build_distribution_format == "wheel" or build_distribution_format == "both":
            command += ["-t", "wheel"]
        env_copy = os.environ.copy()
        env_copy["SOURCE_DATE_EPOCH"] = str(source_date_epoch)
        run_command(
            cmd=command,
            cwd=root_path,
            env=env_copy,
            check=True,
        )
        shutil.copytree(distribution_path, AIRFLOW_DIST_PATH, dirs_exist_ok=True)

    def _build_package_with_docker(build_distribution_format: str):
        _build_local_build_image()
        command = "hatch build -c "
        if build_distribution_format == "sdist" or build_distribution_format == "both":
            command += "-t sdist "
        if build_distribution_format == "wheel" or build_distribution_format == "both":
            command += "-t wheel "
        container_id = f"airflow-{distribution_name}-build-{random.getrandbits(64):08x}"
        result = run_command(
            cmd=[
                "docker",
                "run",
                "--name",
                container_id,
                "-t",
                "-e",
                f"SOURCE_DATE_EPOCH={source_date_epoch}",
                "-e",
                "HOME=/opt/airflow/files/home",
                "-e",
                "GITHUB_ACTIONS",
                "-w",
                f"/opt/airflow/{distribution_name}",
                AIRFLOW_BUILD_IMAGE_TAG,
                "bash",
                "-c",
                command,
            ],
            check=False,
        )
        if result.returncode != 0:
            get_console().print(f"[error]Error preparing Airflow {distribution_pretty_name}[/]")
            fix_ownership_using_docker()
            sys.exit(result.returncode)
        AIRFLOW_DIST_PATH.mkdir(parents=True, exist_ok=True)
        get_console().print()
        # Copy all files in the dist directory in container to the host dist directory (note '/.' in SRC)
        run_command(
            ["docker", "cp", f"{container_id}:/opt/airflow/{distribution_name}/dist/.", "./dist"], check=True
        )
        run_command(["docker", "rm", "--force", container_id], check=False, stdout=DEVNULL, stderr=DEVNULL)

    if use_local_hatch:
        with apply_version_suffix_to_non_provider_pyproject_tomls(
            version_suffix=version_suffix,
            init_file_path=init_file_path,
            pyproject_toml_paths=[TASK_SDK_ROOT_PATH / "pyproject.toml"],
        ) as pyproject_toml_paths:
            debug_pyproject_tomls(pyproject_toml_paths)
            _build_package_with_hatch(
                build_distribution_format=distribution_format,
            )
        get_console().print("[info]Checking if sdist packages can be built into wheels[/]")
        packages = DistributionPackageInfo.dist_packages(
            distribution_format=distribution_format,
            dist_directory=distribution_path,
            build_type=DistributionBuildType(distribution_name),
        )
        get_console().print()
        _check_sdist_to_wheel_dists(packages)
        get_console().print("\n[info]Packages available in dist:[/]\n")
        for dist_info in packages:
            get_console().print(str(dist_info))
        get_console().print()
    else:
        with apply_version_suffix_to_non_provider_pyproject_tomls(
            version_suffix=version_suffix,
            init_file_path=init_file_path,
            pyproject_toml_paths=[root_path / "pyproject.toml"],
        ) as pyproject_toml_paths:
            debug_pyproject_tomls(pyproject_toml_paths)
            _build_package_with_docker(
                build_distribution_format=distribution_format,
            )
    get_console().print(
        f"[success]Successfully prepared {f'Airflow {distribution_pretty_name}' if not full_distribution_pretty_name else full_distribution_pretty_name} packages"
    )


@release_management_group.command(
    name="prepare-task-sdk-distributions",
    help="Prepare sdist/whl distributions of Airflow Task SDK.",
)
@option_distribution_format
@option_version_suffix
@option_use_local_hatch
@option_verbose
@option_dry_run
def prepare_task_sdk_distributions(
    distribution_format: str,
    version_suffix: str,
    use_local_hatch: bool,
):
    _prepare_non_core_distributions(
        # Argument parameters
        distribution_format=distribution_format,
        version_suffix=version_suffix,
        use_local_hatch=use_local_hatch,
        # Distribution specific parameters
        root_path=TASK_SDK_ROOT_PATH,
        init_file_path=TASK_SDK_SOURCES_PATH / "airflow" / "sdk" / "__init__.py",
        distribution_path=TASK_SDK_DIST_PATH,
        distribution_name="task-sdk",
        distribution_pretty_name="Task SDK",
    )


@release_management_group.command(
    name="prepare-airflow-ctl-distributions",
    help="Prepare sdist/whl distributions of airflowctl.",
)
@option_distribution_format
@option_version_suffix
@option_use_local_hatch
@option_verbose
@option_dry_run
def prepare_airflow_ctl_distributions(
    distribution_format: str,
    version_suffix: str,
    use_local_hatch: bool,
):
    _prepare_non_core_distributions(
        # Argument parameters
        distribution_format=distribution_format,
        version_suffix=version_suffix,
        use_local_hatch=use_local_hatch,
        # Distribution specific parameters
        root_path=AIRFLOW_CTL_ROOT_PATH,
        init_file_path=AIRFLOW_CTL_SOURCES_PATH / "airflowctl" / "__init__.py",
        distribution_path=AIRFLOW_CTL_DIST_PATH,
        distribution_name="airflow-ctl",
        distribution_pretty_name="",
        full_distribution_pretty_name="airflowctl",
    )


def provider_action_summary(description: str, message_type: MessageType, packages: list[str]):
    if packages:
        get_console().print(f"{description}: {len(packages)}\n")
        get_console().print(f"[{message_type.value}]{' '.join(packages)}")
        get_console().print()


@release_management_group.command(
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
@option_include_not_ready_providers
@option_include_removed_providers
@click.option(
    "--non-interactive",
    is_flag=True,
    help="Run in non-interactive mode. Provides random answers to the type of changes and confirms release"
    "for providers prepared for release - useful to test the script in non-interactive mode in CI.",
)
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
    "--skip-changelog",
    is_flag=True,
    help="Skip changelog generation. This is used in prek that updates build-files only.",
)
@click.option(
    "--incremental-update",
    is_flag=True,
    help="Runs incremental update only after rebase of earlier branch to check if there are no changes.",
)
@click.option(
    "--skip-readme",
    is_flag=True,
    help="Skip readme generation. This is used in prek that updates build-files only.",
)
@click.option(
    "--release-date",
    type=str,
    callback=validate_release_date,
    envvar="RELEASE_DATE",
    help="Planned release date for the providers release in format "
    "YYYY-MM-DD[_NN] (e.g., 2025-11-16 or 2025-11-16_01).",
)
@argument_provider_distributions
@option_verbose
@option_answer
@option_dry_run
def prepare_provider_documentation(
    base_branch: str,
    github_repository: str,
    include_not_ready_providers: bool,
    include_removed_providers: bool,
    non_interactive: bool,
    only_min_version_update: bool,
    provider_distributions: tuple[str],
    reapply_templates_only: bool,
    skip_git_fetch: bool,
    skip_changelog: bool,
    skip_readme: bool,
    incremental_update: bool,
    release_date: str | None,
):
    from airflow_breeze.prepare_providers.provider_documentation import (
        PrepareReleaseDocsChangesOnlyException,
        PrepareReleaseDocsErrorOccurredException,
        PrepareReleaseDocsNoChangesException,
        PrepareReleaseDocsUserQuitException,
        PrepareReleaseDocsUserSkippedException,
        update_changelog,
        update_index_rst,
        update_min_airflow_version_and_build_files,
        update_release_notes,
    )

    if not release_date and not only_min_version_update:
        get_console().print("[error]Release date is required unless --only-min-version-update is used![/]")
        sys.exit(1)

    perform_environment_checks()
    fix_ownership_using_docker()
    cleanup_python_generated_files()
    if incremental_update:
        set_forced_answer("yes")
    if not provider_distributions:
        provider_distributions = get_available_distributions(
            include_removed=include_removed_providers, include_not_ready=include_not_ready_providers
        )

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
    for provider_id in provider_distributions:
        provider_metadata = basic_provider_checks(provider_id)
        if os.environ.get("GITHUB_ACTIONS", "false") != "true":
            if not only_min_version_update:
                get_console().print("-" * get_console().width)
        try:
            with_breaking_changes = False
            maybe_with_new_features = False
            with ci_group(
                f"Update release notes for package '{provider_id}' ",
                skip_printing_title=only_min_version_update,
            ):
                if not only_min_version_update and not reapply_templates_only:
                    get_console().print("Updating documentation for the latest release version.")
                    with_breaking_changes, maybe_with_new_features, with_min_airflow_version_bump = (
                        update_release_notes(
                            provider_id,
                            reapply_templates_only=reapply_templates_only,
                            base_branch=base_branch,
                            regenerate_missing_docs=reapply_templates_only,
                            non_interactive=non_interactive,
                            only_min_version_update=only_min_version_update,
                        )
                    )
                update_min_airflow_version_and_build_files(
                    provider_id=provider_id,
                    with_breaking_changes=with_breaking_changes,
                    maybe_with_new_features=maybe_with_new_features,
                    skip_readme=skip_readme,
                )
            if not only_min_version_update and not reapply_templates_only and not skip_changelog:
                with ci_group(
                    f"Updates changelog for last release of package '{provider_id}'",
                    skip_printing_title=only_min_version_update,
                ):
                    update_changelog(
                        package_id=provider_id,
                        base_branch=base_branch,
                        reapply_templates_only=reapply_templates_only,
                        with_breaking_changes=with_breaking_changes,
                        maybe_with_new_features=maybe_with_new_features,
                        only_min_version_update=only_min_version_update,
                        with_min_airflow_version_bump=with_min_airflow_version_bump,
                    )
            update_index_rst(
                provider_id=provider_id,
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
            if provider_metadata["state"] == "removed":
                removed_packages.append(provider_id)
            else:
                success_packages.append(provider_id)
    get_console().print()
    get_console().print("\n[info]Summary of prepared documentation:\n")
    provider_action_summary(
        "Success" if not only_min_version_update else "Min Version Bumped",
        MessageType.SUCCESS,
        success_packages,
    )
    provider_action_summary("Scheduled for removal", MessageType.SUCCESS, removed_packages)
    provider_action_summary("Docs only", MessageType.SUCCESS, doc_only_packages)
    provider_action_summary(
        "Skipped on no changes" if not only_min_version_update else "Min Version Not Bumped",
        MessageType.WARNING,
        no_changes_packages,
    )
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
    if release_date:
        AIRFLOW_PROVIDERS_LAST_RELEASE_DATE_PATH.write_text(release_date + "\n")
    if incremental_update:
        get_console().print(r"\[warning] Generated changes:")
        run_command(["git", "diff"])
        get_console().print("\n")
        get_console().print("[warning]Important")
        get_console().print(
            " * Please review manually the changes in changelogs above and move the new changelog "
            "entries to the right sections."
        )
        get_console().print(
            "* Remove the `Please review ...` comments from the changelogs after moving changeslogs"
        )
        get_console().print(
            "* Update both changelog.rst AND provider.yaml in case the new changes require "
            "different classification of the upgrade (patchlevel/minor/major)"
        )


def basic_provider_checks(provider_id: str) -> dict[str, Any]:
    provider_distributions_metadata = get_provider_distributions_metadata()
    get_console().print(f"\n[info]Reading provider package metadata: {provider_id}[/]")
    provider_metadata = provider_distributions_metadata.get(provider_id)
    if not provider_metadata:
        get_console().print(
            f"[error]The package {provider_id} could not be found in the list "
            f"of provider distributions. Exiting[/]"
        )
        get_console().print("Available provider distributions:")
        get_console().print(provider_distributions_metadata)
        sys.exit(1)
    if provider_metadata["state"] == "removed":
        get_console().print(
            f"[warning]The package: {provider_id} is scheduled for removal, but "
            f"since you asked for it, it will be built [/]\n"
        )
    elif provider_metadata.get("state") == "suspended":
        get_console().print(f"[warning]The package: {provider_id} is suspended skipping it [/]\n")
        raise PackageSuspendedException()
    return provider_metadata


def _build_provider_distributions(
    provider_id: str,
    package_version_suffix: str,
    distribution_format: str,
    skip_tag_check: bool,
    skip_deleting_generated_files: bool,
) -> bool:
    """
    Builds provider distribution.

    :param provider_id: id of the provider package
    :param package_version_suffix: suffix to append to the package version
    :param distribution_format: format of the distribution to build (wheel or sdist)
    :param skip_tag_check: whether to skip tag check
    :param skip_deleting_generated_files: whether to skip deleting generated files
    :return: True if package was built, False if it was skipped.
    """
    if not skip_tag_check:
        should_skip, package_version_suffix = should_skip_the_package(provider_id, package_version_suffix)
        if should_skip:
            return False
    get_console().print()
    with ci_group(f"Preparing provider package [special]{provider_id}"):
        get_console().print()
        get_console().print(
            f"[info]Provider {provider_id} building in-place with suffix: '{package_version_suffix}'."
        )
        with apply_version_suffix_to_provider_pyproject_toml(
            provider_id, package_version_suffix
        ) as pyproject_toml_file:
            provider_root_dir = pyproject_toml_file.parent
            cleanup_build_remnants(provider_root_dir)
            build_provider_distribution(
                provider_id=provider_id,
                distribution_format=distribution_format,
                target_provider_root_sources_path=provider_root_dir,
            )
        move_built_distributions_and_cleanup(
            provider_root_dir,
            AIRFLOW_DIST_PATH,
            skip_cleanup=skip_deleting_generated_files,
            delete_only_build_and_dist_folders=True,
        )
    return True


@release_management_group.command(
    name="prepare-provider-distributions",
    help="Prepare sdist/whl distributions of Airflow Providers.",
)
@option_distribution_format
@option_version_suffix
@click.option(
    "--distributions-list-file",
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
@click.option(
    "--distributions-list",
    envvar="DISTRIBUTIONS_LIST",
    type=str,
    help="Optional, contains space separated list of package ids that are processed for documentation "
    "building, and document publishing. It is an easier alternative to adding individual packages as"
    " arguments to every command. This overrides the packages passed as arguments.",
)
@option_dry_run
@option_github_repository
@option_include_not_ready_providers
@option_include_removed_providers
@argument_provider_distributions
@option_verbose
def prepare_provider_distributions(
    clean_dist: bool,
    distributions_list: str,
    github_repository: str,
    include_not_ready_providers: bool,
    include_removed_providers: bool,
    distribution_format: str,
    distributions_list_file: IO | None,
    provider_distributions: tuple[str, ...],
    skip_deleting_generated_files: bool,
    skip_tag_check: bool,
    version_suffix: str,
):
    perform_environment_checks()
    fix_ownership_using_docker()
    cleanup_python_generated_files()
    distributions_list_as_tuple: tuple[str, ...] = ()
    if distributions_list and len(distributions_list):
        get_console().print(
            f"\n[info]Populating provider list from DISTRIBUTIONS_LIST env as {distributions_list}"
        )
        # Override provider_distributions with values from DISTRIBUTIONS_LIST
        distributions_list_as_tuple = tuple(distributions_list.split(" "))
    if provider_distributions and distributions_list_as_tuple:
        get_console().print(
            f"[warning]Both package arguments and --distributions-list / DISTRIBUTIONS_LIST passed. "
            f"Overriding to {distributions_list_as_tuple}"
        )
    provider_distributions = distributions_list_as_tuple or provider_distributions

    packages_list = get_packages_list_to_act_on(
        distributions_list_file=distributions_list_file,
        provider_distributions=provider_distributions,
        include_removed=include_removed_providers,
        include_not_ready=include_not_ready_providers,
    )
    if not skip_tag_check and not is_local_package_version(version_suffix):
        run_command(["git", "remote", "rm", "apache-https-for-providers"], check=False, stderr=DEVNULL)
        make_sure_remote_apache_exists_and_fetch(github_repository=github_repository)
    success_packages = []
    skipped_as_already_released_packages = []
    suspended_packages = []
    wrong_setup_packages = []
    error_packages = []
    if clean_dist:
        get_console().print("\n[warning]Cleaning dist directory before building packages[/]\n")
        shutil.rmtree(AIRFLOW_DIST_PATH, ignore_errors=True)
        AIRFLOW_DIST_PATH.mkdir(parents=True, exist_ok=True)
    for provider_id in packages_list:
        try:
            basic_provider_checks(provider_id)
            created = _build_provider_distributions(
                provider_id,
                version_suffix,
                distribution_format,
                skip_tag_check,
                skip_deleting_generated_files,
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
            if created:
                success_packages.append(provider_id)
            else:
                skipped_as_already_released_packages.append(provider_id)
    get_console().print()
    get_console().print("\n[info]Summary of prepared packages:\n")
    provider_action_summary("Success", MessageType.SUCCESS, success_packages)
    provider_action_summary(
        "Skipped as already released", MessageType.INFO, skipped_as_already_released_packages
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
    packages = DistributionPackageInfo.dist_packages(
        distribution_format=distribution_format,
        dist_directory=AIRFLOW_DIST_PATH,
        build_type=DistributionBuildType.PROVIDERS,
    )
    get_console().print()
    _check_sdist_to_wheel_dists(packages)
    get_console().print("\n[info]Packages available in dist:\n")
    for dist_info in packages:
        get_console().print(str(dist_info))
    get_console().print()


def run_generate_constraints(
    shell_params: ShellParams,
    output: Output | None,
) -> tuple[int, str]:
    result = execute_command_in_shell(
        shell_params,
        project_name=f"constraints-{shell_params.python.replace('.', '-')}",
        command="/opt/airflow/scripts/in_container/run_generate_constraints.py",
        output=output,
    )

    return (
        result.returncode,
        f"Constraints {shell_params.airflow_constraints_mode}:{shell_params.python}",
    )


CONSTRAINT_PROGRESS_MATCHER = (
    r"Found|Uninstalling|uninstalled|Collecting|Downloading|eta|Running|Installing|built|Attempting"
)


def list_generated_constraints(output: Output | None):
    get_console(output=output).print("\n[info]List of generated files in './files' folder:[/]\n")
    found_files = Path("./files").rglob("*")
    for file in sorted(found_files):
        if file.is_file():
            get_console(output=output).print(file.as_posix())
    get_console(output=output).print()


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
                        "output": outputs[index],
                    },
                )
                for index, shell_params in enumerate(shell_params_list)
            ]
    check_async_run_results(
        results=results,
        success_message="All constraints are generated.",
        outputs=outputs,
        include_success_outputs=include_success_outputs,
        skip_cleanup=skip_cleanup,
        summarize_on_ci=SummarizeAfter.SUCCESS,
        summary_start_regexp=".*Constraints generated in.*",
    )


@release_management_group.command(
    name="tag-providers",
    help="Generates tags for airflow provider releases.",
)
@click.option(
    "--clean-tags/--no-clean-tags",
    default=True,
    is_flag=True,
    envvar="CLEAN_TAGS",
    help="Delete tags (both local and remote) that are created due to github connectivity "
    "issues to avoid errors. The default behaviour would be to clean both local and remote tags.",
    show_default=True,
)
@click.option(
    "--release-date",
    type=str,
    help="Date of the release in YYYY-MM-DD format.",
    required=True,
    envvar="RELEASE_DATE",
)
@option_dry_run
@option_verbose
def tag_providers(
    clean_tags: bool,
    release_date: str,
):
    found_remote = None
    remotes = ["origin", "apache"]
    for remote in remotes:
        try:
            result = run_command(
                ["git", "remote", "get-url", "--push", remote],
                stdout=subprocess.PIPE,
                stderr=subprocess.DEVNULL,
                text=True,
            )
            if "apache/airflow.git" in result.stdout:
                found_remote = remote
                break
        except subprocess.CalledProcessError:
            pass

    if found_remote is None:
        raise ValueError("Could not find the remote configured to push to apache/airflow")

    extra_flags = []
    tags = []
    if clean_tags:
        extra_flags.append("--force")
    for file in os.listdir(os.path.join(SOURCE_DIR_PATH, "dist")):
        if file.endswith(".whl"):
            match = re.match(r".*airflow_providers_(.*)-(.*)-py3.*", file)
            if match:
                provider = f"providers-{match.group(1).replace('_', '-')}"
                tag = f"{provider}/{match.group(2)}"
                get_console().print(f"[info]Creating tag: {tag}")
                run_command(
                    ["git", "tag", tag, *extra_flags, "-m", f"Release {release_date} of providers"],
                    check=True,
                )
                tags.append(tag)
    if tags:
        push_result = run_command(
            ["git", "push", found_remote, *extra_flags, *tags],
            check=True,
        )
        if push_result.returncode == 0:
            get_console().print("\n[success]Tags pushed successfully.[/]")


@release_management_group.command(
    name="generate-constraints",
    help="Generates pinned constraint files with all extras from pyproject.toml in parallel.",
)
@option_python
@option_run_in_parallel
@option_parallelism
@option_skip_cleanup
@option_debug_resources
@option_python_versions
@option_airflow_constraints_mode_ci
@option_github_repository
@option_use_uv
@option_verbose
@option_dry_run
@option_answer
def generate_constraints(
    airflow_constraints_mode: str,
    debug_resources: bool,
    github_repository: str,
    parallelism: int,
    python: str,
    python_versions: str,
    run_in_parallel: bool,
    skip_cleanup: bool,
    use_uv: bool,
):
    perform_environment_checks()
    check_remote_ghcr_io_commands()
    fix_ownership_using_docker()
    cleanup_python_generated_files()
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
                airflow_constraints_mode=airflow_constraints_mode,
                github_repository=github_repository,
                python=python,
                use_uv=use_uv,
            )
            for python in python_version_list
        ]
        run_generate_constraints_in_parallel(
            debug_resources=debug_resources,
            include_success_outputs=True,
            parallelism=parallelism,
            python_version_list=python_version_list,
            shell_params_list=shell_params_list,
            skip_cleanup=skip_cleanup,
        )
        fix_ownership_using_docker()
    else:
        shell_params = ShellParams(
            airflow_constraints_mode=airflow_constraints_mode,
            github_repository=github_repository,
            python=python,
            use_uv=use_uv,
        )
        return_code, info = run_generate_constraints(
            shell_params=shell_params,
            output=None,
        )
        fix_ownership_using_docker()
        if return_code != 0:
            get_console().print(f"[error]There was an error when generating constraints: {info}[/]")
            sys.exit(return_code)
    list_generated_constraints(output=None)


SDIST_FILENAME_PREFIX = "apache_airflow_providers_"
WHEEL_FILENAME_PREFIX = "apache_airflow_providers-"

SDIST_FILENAME_PATTERN = re.compile(rf"{SDIST_FILENAME_PREFIX}(.*)-[0-9].*\.tar\.gz")
WHEEL_FILENAME_PATTERN = re.compile(rf"{WHEEL_FILENAME_PREFIX}(.*)-[0-9].*\.whl")


def _get_all_providers_in_dist(
    filename_prefix: str, filename_pattern: re.Pattern[str]
) -> Generator[str, None, None]:
    for file in AIRFLOW_DIST_PATH.glob(f"{filename_prefix}*.tar.gz"):
        matched = filename_pattern.match(file.name)
        if not matched:
            raise SystemExit(f"Cannot parse provider package name from {file.name}")
        provider_id = matched.group(1).replace("_", ".")
        yield provider_id


def get_all_providers_in_dist(distribution_format: str, install_selected_providers: str) -> list[str]:
    """
    Returns all providers in dist, optionally filtered by install_selected_providers.

    :param distribution_format: package format to look for
    :param install_selected_providers: list of providers to filter by
    """
    if distribution_format == "sdist":
        all_found_providers = list(
            _get_all_providers_in_dist(
                filename_prefix=SDIST_FILENAME_PREFIX, filename_pattern=SDIST_FILENAME_PATTERN
            )
        )
    elif distribution_format == "wheel":
        all_found_providers = list(
            _get_all_providers_in_dist(
                filename_prefix=WHEEL_FILENAME_PREFIX, filename_pattern=WHEEL_FILENAME_PATTERN
            )
        )
    else:
        raise SystemExit(f"Unknown package format {distribution_format}")
    if install_selected_providers:
        filter_list = install_selected_providers.split(",")
        return [provider for provider in all_found_providers if provider in filter_list]
    return all_found_providers


def _run_command_for_providers(
    shell_params: ShellParams,
    list_of_providers: list[str],
    index: int,
    output: Output | None,
) -> tuple[int, str]:
    shell_params.install_selected_providers = " ".join(list_of_providers)
    result_command = execute_command_in_shell(shell_params, project_name=f"providers-{index}", output=output)
    return result_command.returncode, f"{list_of_providers}"


SDIST_INSTALL_PROGRESS_REGEXP = r"Processing .*|Requirement already satisfied:.*|  Created wheel.*"


@release_management_group.command(
    name="install-provider-distributions",
    help="Installs provider distributiobs that can be found in dist.",
)
@option_airflow_constraints_mode_ci
@option_airflow_constraints_location
@option_airflow_constraints_reference
@option_airflow_extras
@option_clean_airflow_installation
@option_debug_resources
@option_dry_run
@option_github_repository
@option_install_airflow_with_constraints_default_true
@option_include_success_outputs
@option_install_selected_providers
@option_installation_distribution_format
@option_mount_sources
@option_parallelism
@option_providers_constraints_location
@option_providers_constraints_mode_ci
@option_providers_constraints_reference
@option_providers_skip_constraints
@option_python
@option_run_in_parallel
@option_skip_cleanup
@option_use_airflow_version
@option_allow_pre_releases
@option_use_distributions_from_dist
@option_verbose
def install_provider_distributions(
    airflow_constraints_location: str,
    airflow_constraints_mode: str,
    airflow_constraints_reference: str,
    install_airflow_with_constraints: bool,
    airflow_extras: str,
    allow_pre_releases: bool,
    clean_airflow_installation: bool,
    debug_resources: bool,
    github_repository: str,
    include_success_outputs: bool,
    install_selected_providers: str,
    mount_sources: str,
    distribution_format: str,
    providers_constraints_location: str,
    providers_constraints_mode: str,
    providers_constraints_reference: str,
    providers_skip_constraints: bool,
    python: str,
    parallelism: int,
    run_in_parallel: bool,
    skip_cleanup: bool,
    use_airflow_version: str | None,
    use_distributions_from_dist: bool,
):
    perform_environment_checks()
    fix_ownership_using_docker()
    cleanup_python_generated_files()
    shell_params = ShellParams(
        airflow_constraints_location=airflow_constraints_location,
        airflow_constraints_mode=airflow_constraints_mode,
        airflow_constraints_reference=airflow_constraints_reference,
        airflow_extras=airflow_extras,
        install_airflow_with_constraints=install_airflow_with_constraints,
        allow_pre_releases=allow_pre_releases,
        # We just want to install the providers by entrypoint
        # we do not need to run any command in the container
        extra_args=("exit 0",),
        clean_airflow_installation=clean_airflow_installation,
        github_repository=github_repository,
        install_selected_providers=install_selected_providers,
        mount_sources=mount_sources,
        distribution_format=distribution_format,
        providers_constraints_location=providers_constraints_location,
        providers_constraints_mode=providers_constraints_mode,
        providers_constraints_reference=providers_constraints_reference,
        providers_skip_constraints=providers_skip_constraints,
        python=python,
        use_airflow_version=use_airflow_version,
        use_distributions_from_dist=use_distributions_from_dist,
    )
    rebuild_or_pull_ci_image_if_needed(command_params=shell_params)
    if run_in_parallel:
        list_of_all_providers = get_all_providers_in_dist(
            distribution_format=distribution_format, install_selected_providers=install_selected_providers
        )
        get_console().print(
            f"[info]Splitting {len(list_of_all_providers)} providers into max {parallelism} chunks"
        )
        provider_chunks = [sorted(list_of_all_providers[i::parallelism]) for i in range(parallelism)]
        # filter out empty ones
        provider_chunks = [chunk for chunk in provider_chunks if chunk]
        if not provider_chunks:
            get_console().print("[info]No providers to install")
            sys.exit(1)
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
            msg = (
                f"Total providers {total_num_providers} is different "
                f"than {len(list_of_all_providers)} (just to be sure"
                f" no rounding errors crippled in)"
            )
            raise RuntimeError(msg)
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
                            "list_of_providers": list_of_providers,
                            "output": outputs[index],
                            "index": index,
                        },
                    )
                    for index, list_of_providers in enumerate(provider_chunks)
                ]
        check_async_run_results(
            results=results,
            success_message="All packages installed successfully",
            outputs=outputs,
            include_success_outputs=include_success_outputs,
            skip_cleanup=skip_cleanup,
        )
    else:
        result_command = execute_command_in_shell(shell_params, project_name="providers")
        fix_ownership_using_docker()
        sys.exit(result_command.returncode)


@release_management_group.command(
    name="verify-provider-distributions",
    help="Verifies if all provider code is following expectations for providers.",
)
@option_airflow_constraints_mode_ci
@option_airflow_constraints_location
@option_airflow_constraints_reference
@option_airflow_extras
@option_clean_airflow_installation
@option_dry_run
@option_github_repository
@option_install_airflow_with_constraints
@option_install_selected_providers
@option_installation_distribution_format
@option_mount_sources
@option_python
@option_providers_constraints_location
@option_providers_constraints_mode_ci
@option_providers_constraints_reference
@option_providers_skip_constraints
@option_use_airflow_version
@option_allow_pre_releases
@option_use_distributions_from_dist
@option_verbose
def verify_provider_distributions(
    airflow_constraints_location: str,
    airflow_constraints_mode: str,
    airflow_constraints_reference: str,
    airflow_extras: str,
    clean_airflow_installation: bool,
    github_repository: str,
    install_airflow_with_constraints: bool,
    install_selected_providers: str,
    mount_sources: str,
    distribution_format: str,
    providers_constraints_location: str,
    providers_constraints_mode: str,
    providers_constraints_reference: str,
    providers_skip_constraints: bool,
    python: str,
    use_airflow_version: str | None,
    allow_pre_releases: bool,
    use_distributions_from_dist: bool,
):
    if install_selected_providers and not use_distributions_from_dist:
        get_console().print("Forcing use_distributions_from_dist as installing selected_providers is set")
        use_distributions_from_dist = True
    perform_environment_checks()
    fix_ownership_using_docker()
    cleanup_python_generated_files()
    shell_params = ShellParams(
        airflow_constraints_location=airflow_constraints_location,
        airflow_constraints_mode=airflow_constraints_mode,
        airflow_constraints_reference=airflow_constraints_reference,
        airflow_extras=airflow_extras,
        allow_pre_releases=allow_pre_releases,
        clean_airflow_installation=clean_airflow_installation,
        github_repository=github_repository,
        install_airflow_with_constraints=install_airflow_with_constraints,
        mount_sources=mount_sources,
        distribution_format=distribution_format,
        providers_constraints_location=providers_constraints_location,
        providers_constraints_mode=providers_constraints_mode,
        providers_constraints_reference=providers_constraints_reference,
        providers_skip_constraints=providers_skip_constraints,
        python=python,
        use_airflow_version=use_airflow_version,
        use_distributions_from_dist=use_distributions_from_dist,
    )
    rebuild_or_pull_ci_image_if_needed(command_params=shell_params)
    result_command = execute_command_in_shell(
        shell_params,
        project_name="providers",
        command="python /opt/airflow/scripts/in_container/verify_providers.py",
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
    from airflow_breeze.utils.docs_publisher import DocsPublisher

    builder = DocsPublisher(package_name=package_name, output=output, verbose=verbose)
    return builder.publish(override_versioned=override_versioned, airflow_site_dir=airflow_site_directory)


PUBLISHING_DOCS_PROGRESS_MATCHER = r"Publishing docs|Copy directory"


def run_publish_docs_in_parallel(
    distributions_list: tuple[str, ...],
    airflow_site_directory: str,
    override_versioned: bool,
    parallelism: int,
    debug_resources: bool,
):
    """Run docs publishing in parallel"""
    success_entries = []
    skipped_entries = []

    with ci_group("Publishing docs for packages"):
        all_params = [f"Publishing docs {package_name}" for package_name in distributions_list]
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
                for index, package_name in enumerate(distributions_list)
            ]

            # Iterate over the results and collect success and skipped entries
            for result in results:
                return_code, message = result.get()
                if return_code == 0:
                    success_entries.append(message)
                else:
                    skipped_entries.append(message)

    get_console().print("[blue]Summary:")
    need_rule = False
    if len(success_entries):
        get_console().print("[success]Packages published:")
        for entry in success_entries:
            get_console().print(f"[success]{entry}")
        need_rule = True
    if need_rule:
        get_console().rule()
    if len(skipped_entries):
        get_console().print("\n[warning]Packages skipped:")
        for entry in skipped_entries:
            get_console().print(f"[warning]{entry}")


@release_management_group.command(
    name="publish-docs",
    help="Command to publish generated documentation to airflow-site",
)
@argument_doc_packages
@option_airflow_site_directory
@option_debug_resources
@option_dry_run
@option_include_not_ready_providers
@option_include_removed_providers
@click.option("-s", "--override-versioned", help="Overrides versioned directories.", is_flag=True)
@click.option(
    "--package-filter",
    help="Filter(s) to use more than one can be specified. You can use glob pattern matching the "
    "full package name, for example `apache-airflow-providers-*`. Useful when you want to select"
    "several similarly named packages together.",
    type=str,
    multiple=True,
)
@click.option(
    "--distributions-list",
    envvar="DISTRIBUTIONS_LIST",
    type=str,
    help="Optional, contains space separated list of package ids that are processed for documentation "
    "building, and document publishing. It is an easier alternative to adding individual packages as"
    " arguments to every command. This overrides the packages passed as arguments.",
)
@option_parallelism
@option_run_in_parallel
@option_verbose
def publish_docs(
    airflow_site_directory: str,
    debug_resources: bool,
    doc_packages: tuple[str, ...],
    include_not_ready_providers: bool,
    include_removed_providers: bool,
    override_versioned: bool,
    package_filter: tuple[str, ...],
    distributions_list: str,
    parallelism: int,
    run_in_parallel: bool,
):
    """Publishes documentation to airflow-site."""
    if not os.path.isdir(airflow_site_directory):
        get_console().print(
            "\n[error]location pointed by airflow_site_dir is not valid. "
            "Provide the path of cloned airflow-site repo\n"
        )
    packages_list_as_tuple: tuple[str, ...] = ()
    if distributions_list and len(distributions_list):
        get_console().print(
            f"\n[info]Populating provider list from DISTRIBUTIONS_LIST env as {distributions_list}"
        )
        # Override doc_packages with values from DISTRIBUTIONS_LIST
        packages_list_as_tuple = tuple(distributions_list.split(" "))
    if doc_packages and packages_list_as_tuple:
        get_console().print(
            f"[warning]Both package arguments and --distributions-list / DISTRIBUTIONS_LIST passed. "
            f"Overriding to {packages_list_as_tuple}"
        )
    doc_packages = packages_list_as_tuple or doc_packages

    current_packages = find_matching_long_package_names(
        short_packages=expand_all_provider_distributions(
            short_doc_packages=doc_packages,
            include_removed=include_removed_providers,
            include_not_ready=include_not_ready_providers,
        ),
        filters=package_filter,
    )
    print(f"Publishing docs for {len(current_packages)} package(s)")
    for pkg in current_packages:
        print(f" - {pkg}")
    print()
    if run_in_parallel:
        run_publish_docs_in_parallel(
            distributions_list=current_packages,
            parallelism=parallelism,
            debug_resources=debug_resources,
            airflow_site_directory=airflow_site_directory,
            override_versioned=override_versioned,
        )
    else:
        success_entries = []
        skipped_entries = []
        for package_name in current_packages:
            return_code, message = run_docs_publishing(
                package_name, airflow_site_directory, override_versioned, verbose=get_verbose(), output=None
            )
            if return_code == 0:
                success_entries.append(message)
            else:
                skipped_entries.append(message)
        get_console().print("[blue]Summary:")
        need_rule = False
        if len(success_entries):
            get_console().print("[success]Packages published:")
            for entry in success_entries:
                get_console().print(f"[success]{entry}")
            need_rule = True
        if need_rule:
            get_console().rule()
        if len(skipped_entries):
            get_console().print("\n[warning]Packages skipped:")
            for entry in skipped_entries:
                get_console().print(f"[warning]{entry}")


@release_management_group.command(
    name="add-back-references",
    help="Command to add back references for documentation to make it backward compatible.",
)
@option_airflow_site_directory
@option_include_not_ready_providers
@option_include_removed_providers
@argument_doc_packages
@option_dry_run
@option_verbose
@click.option(
    "--head-ref",
    help="The branch of redirect files to use.",
    default=AIRFLOW_BRANCH,
    envvar="HEAD_REF",
    show_default=True,
)
@click.option(
    "--head-repo",
    help="The repository of redirect files to use.",
    default=APACHE_AIRFLOW_GITHUB_REPOSITORY,
    envvar="HEAD_REPO",
    show_default=True,
)
def add_back_references(
    airflow_site_directory: str,
    include_not_ready_providers: bool,
    include_removed_providers: bool,
    doc_packages: tuple[str, ...],
    head_ref: str,
    head_repo: str,
):
    # head_ref and head_repo could be empty in canary runs

    if head_ref == "":
        head_ref = AIRFLOW_BRANCH
    if head_repo == "":
        head_repo = APACHE_AIRFLOW_GITHUB_REPOSITORY

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
    start_generating_back_references(
        site_path,
        list(
            expand_all_provider_distributions(
                short_doc_packages=doc_packages,
                include_removed=include_removed_providers,
                include_not_ready=include_not_ready_providers,
            )
        ),
        head_ref=head_ref,
        head_repo=head_repo,
    )


@release_management_group.command(
    name="clean-old-provider-artifacts",
    help="Cleans the old provider artifacts",
)
@option_directory
@option_dry_run
@option_verbose
def clean_old_provider_artifacts(
    directory: str,
):
    """Cleans up the old airflow providers artifacts in order to maintain
    only one provider version in the release SVN folder and one -source artifact."""
    cleanup_suffixes = [
        ".tar.gz",
        ".tar.gz.sha512",
        ".tar.gz.asc",
        "-py3-none-any.whl",
        "-py3-none-any.whl.sha512",
        "-py3-none-any.whl.asc",
    ]

    for suffix in cleanup_suffixes:
        get_console().print(f"[info]Running provider cleanup for suffix: {suffix}[/]")
        package_types_dicts: dict[str, list[VersionedFile]] = defaultdict(list)
        os.chdir(directory)

        for file in glob.glob(f"*{suffix}"):
            if "-source.tar.gz" in file:
                versioned_file = split_date_version_and_suffix(file, "-source" + suffix)
            else:
                versioned_file = split_version_and_suffix(file, suffix)
            package_types_dicts[versioned_file.type].append(versioned_file)

        for package_types in package_types_dicts.values():
            package_types.sort(key=operator.attrgetter("comparable_version"))

        for package_types in package_types_dicts.values():
            if len(package_types) == 1:
                versioned_file = package_types[0]
                get_console().print(
                    f"[success]Leaving the only version: "
                    f"{versioned_file.base + versioned_file.version + versioned_file.suffix}[/]"
                )
            # Leave only last version from each type
            for versioned_file in package_types[:-1]:
                get_console().print(
                    f"[warning]Removing {versioned_file.file_name} as they are older than remaining file: "
                    f"{package_types[-1].file_name}[/]"
                )
                command = ["svn", "rm", versioned_file.file_name]
                run_command(command, check=False)


def alias_images(
    airflow_version: str,
    dockerhub_repo: str,
    python_versions: list[str],
    image_prefix: str,
    skip_latest: bool = False,
):
    get_console().print("[info]Waiting for a few seconds for the new images to refresh.[/]")
    time.sleep(10)
    get_console().print("[info]Aliasing images with links to the newly created images.[/]")
    for python in python_versions:
        # Always alias the last python version to point to the non-python version
        if python == DEFAULT_PYTHON_MAJOR_MINOR_VERSION_FOR_IMAGES:
            get_console().print(
                f"[info]Aliasing the {image_prefix}{airflow_version}-python{python} "
                f"version with {image_prefix}{airflow_version}[/]"
            )
            alias_image(
                f"{dockerhub_repo}:{image_prefix}{airflow_version}-python{python}",
                f"{dockerhub_repo}:{image_prefix}{airflow_version}",
            )
        if not skip_latest:
            # if we are taging latest images, we also need to alias the non-version images
            get_console().print(
                f"[info]Aliasing {image_prefix}{airflow_version}-python{python} "
                f"version with {image_prefix}latest-python{python}[/]"
            )
            alias_image(
                f"{dockerhub_repo}:{image_prefix}{airflow_version}-python{python}",
                f"{dockerhub_repo}:{image_prefix}latest-python{python}",
            )
            if image_prefix == "":
                alias_image(
                    f"{dockerhub_repo}:{airflow_version}-python{python}",
                    f"{dockerhub_repo}:latest-python{python}",
                )
            if python == DEFAULT_PYTHON_MAJOR_MINOR_VERSION_FOR_IMAGES:
                alias_image(
                    f"{dockerhub_repo}:{image_prefix}{airflow_version}",
                    f"{dockerhub_repo}:{image_prefix}latest",
                )


def check_skip_latest(airflow_version, skip_latest):
    if is_pre_release(airflow_version):
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
    check_docker_buildx_plugin()
    return skip_latest


@release_management_group.command(
    name="release-prod-images", help="Release production images to DockerHub (needs DockerHub permissions)."
)
@click.option("--airflow-version", required=True, help="Airflow version to release (2.3.0, 2.3.0rc1 etc.)")
@option_dry_run
@click.option(
    "--dockerhub-repo",
    default=APACHE_AIRFLOW_GITHUB_REPOSITORY,
    show_default=True,
    envvar="DOCKERHUB_REPO",
    help="DockerHub repository for the images",
)
@option_python_no_default
@click.option(
    "--platform",
    type=BetterChoice(ALLOWED_PLATFORMS),
    default=MULTI_PLATFORM,
    show_default=True,
    envvar="PLATFORM",
    help="Platform to build images for (if not specified, multiplatform images will be built.",
)
@option_commit_sha
@click.option(
    "--skip-latest",
    is_flag=True,
    envvar="SKIP_LATEST",
    help="Whether to skip publishing the latest images (so that 'latest' images are not updated). "
    "This should only be used if you release image for previous branches. Automatically set when "
    "rc/alpha/beta images are built.",
)
@click.option(
    "--include-pre-release",
    is_flag=True,
    envvar="INCLUDE_PRE_RELEASE",
    help="Whether to Include pre-release distributions from PyPI when building images. Useful when we "
    "want to build an RC image with RC provider versions.",
)
@click.option(
    "--slim-images",
    is_flag=True,
    help="Whether to prepare slim images instead of the regular ones.",
    envvar="SLIM_IMAGES",
)
@click.option(
    "--metadata-folder",
    type=click.Path(dir_okay=True, file_okay=False, writable=True, path_type=Path),
    envvar="METADATA_FOLDER",
    help="Folder to write the build metadata to. When this option is specified the image is pushed to registry"
    "only by digests not by the tag because we are going to merge several images in multi-platform one.",
)
@option_verbose
def release_prod_images(
    airflow_version: str,
    dockerhub_repo: str,
    slim_images: bool,
    platform: str,
    python: str | None,
    commit_sha: str | None,
    skip_latest: bool,
    include_pre_release: bool,
    metadata_folder: Path | None,
):
    perform_environment_checks()
    check_remote_ghcr_io_commands()
    skip_latest = check_skip_latest(airflow_version, skip_latest)
    check_airflow_cache_builder_configured()
    from packaging.version import Version

    include_pre_release = include_pre_release or Version(airflow_version).is_prerelease
    if include_pre_release:
        get_console().print("[warning]Including pre-releases when considering dependencies.[/]")

    if metadata_folder:
        if platform == MULTI_PLATFORM:
            get_console().print(
                "[error]Cannot use metadata folder with multi-platform image. Use "
                "--platform to limit it to a single platform only[/]"
            )
            sys.exit(1)
        get_console().print(
            f"[info]Will push images to registry only by digests and store metadata files in "
            f"the {metadata_folder}[/]"
        )
        if not metadata_folder.exists():
            metadata_folder.mkdir(parents=True)
    else:
        check_regctl_installed()

    python_versions = CURRENT_PYTHON_MAJOR_MINOR_VERSIONS if python is None else [python]
    if slim_images:
        image_prefix = "slim-"
        image_type = "slim"
    else:
        image_prefix = ""
        image_type = "regular"
    for python in python_versions:
        build_args = {
            "AIRFLOW_CONSTRAINTS": "constraints-no-providers",
            "BASE_IMAGE": "debian:bookworm-slim",
            "AIRFLOW_PYTHON_VERSION": ALL_PYTHON_VERSION_TO_PATCHLEVEL_VERSION.get(python, python),
            "AIRFLOW_VERSION": airflow_version,
            "INCLUDE_PRE_RELEASE": "true" if include_pre_release else "false",
            "INSTALL_DISTRIBUTIONS_FROM_CONTEXT": "false",
            "DOCKER_CONTEXT_FILES": "./docker-context-files",
        }
        if commit_sha:
            build_args["COMMIT_SHA"] = commit_sha
        if slim_images:
            build_args["AIRFLOW_EXTRAS"] = ""
        get_console().print(f"[info]Building {image_type} {airflow_version} image for Python {python}[/]")
        python_build_args = deepcopy(build_args)
        image_name = f"{dockerhub_repo}:{image_prefix}{airflow_version}-python{python}"
        docker_buildx_command = [
            "docker",
            "buildx",
            "build",
            "--builder",
            "airflow_cache",
            *convert_build_args_dict_to_array_of_args(build_args=python_build_args),
            "--platform",
            platform,
            "--push",
        ]
        metadata_file: Path | None = None
        if metadata_folder:
            metadata_file = (
                metadata_folder
                / f"metadata-{airflow_version}-{image_prefix}{platform.replace('/', '_')}-{python}.json"
            )
            docker_buildx_command += [
                "-t",
                dockerhub_repo,
                "--metadata-file",
                str(metadata_file),
                "--output",
                "type=image,push-by-digest=true,name-canonical=true",
            ]
        else:
            docker_buildx_command += [
                "-t",
                image_name,
            ]
        docker_buildx_command += ["."]
        run_command(docker_buildx_command)
        if metadata_file:
            get_console().print(f"[green]Metadata file stored in {metadata_file}")
        if python == DEFAULT_PYTHON_MAJOR_MINOR_VERSION_FOR_IMAGES and not metadata_file:
            get_console().print(
                f"[info]Aliasing the latest {python} version to {image_prefix}{airflow_version}[/]"
            )
            alias_image(
                image_name,
                f"{dockerhub_repo}:{image_prefix}{airflow_version}",
            )
    if not metadata_folder:
        alias_images(airflow_version, dockerhub_repo, python_versions, image_prefix, skip_latest)


@release_management_group.command(
    name="merge-prod-images",
    help="Merge production images in DockerHub based on digest files (needs DockerHub permissions).",
)
@click.option("--airflow-version", required=True, help="Airflow version to release (2.3.0, 2.3.0rc1 etc.)")
@option_dry_run
@click.option(
    "--dockerhub-repo",
    default=APACHE_AIRFLOW_GITHUB_REPOSITORY,
    show_default=True,
    envvar="DOCKERHUB_REPO",
    help="DockerHub repository for the images",
)
@option_python_no_default
@click.option(
    "--skip-latest",
    is_flag=True,
    envvar="SKIP_LATEST",
    help="Whether to skip publishing the latest images (so that 'latest' images are not updated). "
    "This should only be used if you release image for previous branches. Automatically set when "
    "rc/alpha/beta images are built.",
)
@click.option(
    "--slim-images",
    is_flag=True,
    envvar="SLIM_IMAGES",
    help="Whether to prepare slim images instead of the regular ones.",
)
@click.option(
    "--metadata-folder",
    type=click.Path(exists=True, dir_okay=True, file_okay=False, writable=True, path_type=Path),
    envvar="METADATA_FOLDER",
    help="Folder to write the build metadata to. When this option is specified the image is pushed to registry"
    "only by digests not by the tag because we are going to merge several images in multi-platform one.",
    required=True,
)
@option_verbose
def merge_prod_images(
    airflow_version: str,
    dockerhub_repo: str,
    slim_images: bool,
    python: str | None,
    skip_latest: bool,
    metadata_folder: Path,
):
    perform_environment_checks()
    check_remote_ghcr_io_commands()
    check_docker_buildx_plugin()
    check_regctl_installed()
    skip_latest = check_skip_latest(airflow_version, skip_latest)
    python_versions = CURRENT_PYTHON_MAJOR_MINOR_VERSIONS if python is None else [python]
    if slim_images:
        image_prefix = "slim-"
    else:
        image_prefix = ""
    for python in python_versions:
        metadata_files = list(
            metadata_folder.rglob(f"metadata-{airflow_version}-{image_prefix}linux*-{python}.json")
        )
        image_name = f"{dockerhub_repo}:{image_prefix}{airflow_version}-python{python}"
        import json

        metadata_array = [json.loads(file.read_text()) for file in metadata_files]
        digests_to_merge = [
            dockerhub_repo + "@" + metadata_content["containerimage.digest"]
            for metadata_content in metadata_array
        ]
        get_console().print(f"[info]Merging {image_name} file from digests found in {metadata_files}[/]")
        get_console().print(f"[info]Digests to merge: {digests_to_merge}[/]")
        imagetool_command = [
            "docker",
            "buildx",
            "imagetools",
            "create",
            *digests_to_merge,
            "-t",
            image_name,
        ]
        run_command(imagetool_command)
    alias_images(
        airflow_version,
        dockerhub_repo,
        python_versions,
        image_prefix=image_prefix,
        skip_latest=skip_latest,
    )


def is_package_in_dist(dist_files: list[str], package: str) -> bool:
    """Check if package has been prepared in dist folder."""
    return any(
        file.startswith(
            (
                f"apache_airflow_providers_{package.replace('.', '_')}",
                f"apache-airflow-providers-{package.replace('.', '-')}",
            )
        )
        for file in dist_files
    )


VERSION_MATCH = re.compile(r"([0-9]+)\.([0-9]+)\.([0-9]+)(.*)")


def get_suffix_from_package_in_dist(dist_files: list[str], package: str) -> str | None:
    """Get suffix from package prepared in dist folder."""
    for filename in dist_files:
        if filename.startswith(f"apache_airflow_providers_{package.replace('.', '_')}") and filename.endswith(
            ".tar.gz"
        ):
            file = filename[: -len(".tar.gz")]
            version = file.split("-")[-1]
            match = VERSION_MATCH.match(version)
            if match:
                return match.group(4)
    return None


def get_prs_for_package(provider_id: str) -> list[int]:
    pr_matcher = re.compile(r".*\(#([0-9]*)\)``$")
    prs = []
    provider_yaml_dict = get_provider_distributions_metadata().get(provider_id)
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


def create_github_issue_url(title: str, body: str, labels: Iterable[str]) -> str:
    """
    Creates URL to create the issue with title, body and labels.
    :param title: issue title
    :param body: issue body
    :param labels: labels for the issue
    :return: URL to use to create the issue
    """
    from urllib.parse import quote

    quoted_labels = quote(",".join(labels))
    quoted_title = quote(title)
    quoted_body = quote(body)
    return (
        f"https://github.com/apache/airflow/issues/new?labels={quoted_labels}&"
        f"title={quoted_title}&body={quoted_body}"
    )


def get_commented_out_prs_from_provider_changelogs() -> list[int]:
    """
    Returns list of PRs that are commented out in the changelog.
    :return: list of PR numbers that appear only in comments in changelog.rst files in "providers" dir
    """
    pr_matcher = re.compile(r".*\(#([0-9]+)\).*")
    commented_prs = set()

    # Get all provider distributions
    provider_distributions_metadata = get_provider_distributions_metadata()

    for provider_id in provider_distributions_metadata.keys():
        provider_details = get_provider_details(provider_id)
        changelog_path = provider_details.changelog_path
        print(f"[info]Checking changelog {changelog_path} for PRs to be excluded automatically.")
        if not changelog_path.exists():
            continue

        changelog_lines = changelog_path.read_text().splitlines()
        in_excluded_section = False

        for line in changelog_lines:
            # Check if we're entering an excluded/commented section
            if line.strip().startswith(
                ".. Below changes are excluded from the changelog"
            ) or line.strip().startswith(".. Review and move the new changes"):
                in_excluded_section = True
                continue
            # Check if we're exiting the excluded section (new version header or regular content)
            # Version headers are lines that contain only dots (like "4.10.1" followed by "......")
            # Or lines that start with actual content sections like "Misc", "Features", etc.
            if (
                in_excluded_section
                and line
                and not line.strip().startswith("..")
                and not line.strip().startswith("*")
            ):
                # end excluded section with empty line
                if line.strip() == "":
                    in_excluded_section = False

            # Extract PRs from excluded sections
            if in_excluded_section and line.strip().startswith("*"):
                match_result = pr_matcher.search(line)
                if match_result:
                    commented_prs.add(int(match_result.group(1)))

    return sorted(commented_prs)


@release_management_group.command(
    name="generate-issue-content-providers", help="Generates content for issue to test the release."
)
@click.option("--disable-progress", is_flag=True, help="Disable progress bar")
@click.option("--excluded-pr-list", type=str, help="Coma-separated list of PRs to exclude from the issue.")
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
@click.option(
    "--only-available-in-dist",
    is_flag=True,
    help="Only consider package ids with packages prepared in the dist folder",
)
@argument_provider_distributions
def generate_issue_content_providers(
    disable_progress: bool,
    excluded_pr_list: str,
    github_token: str,
    only_available_in_dist: bool,
    provider_distributions: list[str],
):
    import jinja2
    from github import Github, Issue, PullRequest, UnknownObjectException

    class ProviderPRInfo(NamedTuple):
        provider_id: str
        pypi_package_name: str
        version: str
        pr_list: list[PullRequest.PullRequest | Issue.Issue]
        suffix: str

    if not provider_distributions:
        provider_distributions = list(get_provider_dependencies().keys())
    with ci_group("Generates GitHub issue content with people who can test it"):
        if excluded_pr_list:
            excluded_prs = [int(pr) for pr in excluded_pr_list.split(",")]
        else:
            excluded_prs = []
        commented_prs = get_commented_out_prs_from_provider_changelogs()
        get_console().print(
            "[info]Automatically excluding {len(commented_prs)} PRs that are only commented out in changelog:"
        )
        excluded_prs.extend(commented_prs)
        all_prs: set[int] = set()
        all_retrieved_prs: set[int] = set()
        provider_prs: dict[str, list[int]] = {}
        files_in_dist = os.listdir(str(AIRFLOW_DIST_PATH)) if only_available_in_dist else []
        prepared_package_ids = []
        for provider_id in provider_distributions:
            if not only_available_in_dist or is_package_in_dist(files_in_dist, provider_id):
                get_console().print(f"Extracting PRs for provider {provider_id}")
                prepared_package_ids.append(provider_id)
            else:
                get_console().print(
                    f"Skipping extracting PRs for provider {provider_id} as it is missing in dist"
                )
                continue
            prs = get_prs_for_package(provider_id)
            if not prs:
                get_console().print(
                    f"[warning]Skipping provider {provider_id}. "
                    "The changelog file doesn't contain any PRs for the release.\n"
                )
                continue
            all_prs.update(prs)
            provider_prs[provider_id] = [pr for pr in prs if pr not in excluded_prs]
            all_retrieved_prs.update(provider_prs[provider_id])
        if not github_token:
            # Get GitHub token from gh CLI and set it in environment copy
            gh_token_result = run_command(
                ["gh", "auth", "token"],
                capture_output=True,
                text=True,
                check=False,
            )
            if gh_token_result.returncode == 0:
                github_token = gh_token_result.stdout.strip()
        g = Github(github_token)
        repo = g.get_repo("apache/airflow")
        pull_requests: dict[int, PullRequest.PullRequest | Issue.Issue] = {}
        linked_issues: dict[int, list[Issue.Issue]] = {}
        all_prs_len = len(all_prs)
        all_retrieved_prs_len = len(all_retrieved_prs)
        get_console().print(
            f"[info] Found {all_prs_len} PRs in the providers. "
            f"Retrieving {all_retrieved_prs_len} (excluded {all_prs_len - all_retrieved_prs_len})"
        )
        get_console().print(f"Retrieved PRs: {all_retrieved_prs}")
        excluded_prs = sorted(set(all_prs) - set(all_retrieved_prs))
        get_console().print(f"Excluded PRs: {excluded_prs}")
        with Progress(console=get_console(), disable=disable_progress) as progress:
            task = progress.add_task(f"Retrieving {all_retrieved_prs_len} PRs ", total=all_retrieved_prs_len)
            for pr_number in all_retrieved_prs:
                progress.console.print(
                    f"Retrieving PR#{pr_number}: https://github.com/apache/airflow/pull/{pr_number}"
                )
                pr_or_issue = None
                try:
                    pr_or_issue = repo.get_pull(pr_number)
                    if pr_or_issue.user.login == "dependabot[bot]":
                        get_console().print(
                            f"[yellow]Skipping PR #{pr_number} as it was created by dependabot[/]"
                        )
                        continue
                    pull_requests[pr_number] = pr_or_issue
                except UnknownObjectException:
                    # Fallback to issue if PR not found
                    try:
                        pr_or_issue = repo.get_issue(pr_number)  # type: ignore[assignment]
                    except UnknownObjectException:
                        get_console().print(f"[red]The PR #{pr_number} could not be found[/]")
                pull_requests[pr_number] = pr_or_issue  # type: ignore[assignment]

                # Retrieve linked issues
                if pr_number in pull_requests and pull_requests[pr_number].body:
                    body = " ".join(pull_requests[pr_number].body.splitlines())
                    linked_issue_numbers = {
                        int(issue_match.group(1)) for issue_match in ISSUE_MATCH_IN_BODY.finditer(body)
                    }
                    for linked_issue_number in linked_issue_numbers:
                        try:
                            _ = repo.get_issue(linked_issue_number)
                        except UnknownObjectException:
                            progress.console.print(
                                f"Failed to retrieve linked issue #{linked_issue_number}: is not a issue,"
                                f"likely a discussion is linked."
                            )
                        progress.console.print(
                            f"Retrieving Linked issue PR#{linked_issue_number}: "
                            f"https://github.com/apache/airflow/issues/{linked_issue_number}"
                        )
                        try:
                            if pr_number not in linked_issues:
                                linked_issues[pr_number] = []
                            linked_issues[pr_number].append(repo.get_issue(linked_issue_number))
                        except UnknownObjectException:
                            progress.console.print(
                                f"Failed to retrieve linked issue #{linked_issue_number}: Unknown Issue"
                            )
                progress.advance(task)
        get_provider_distributions_metadata.cache_clear()
        providers: dict[str, ProviderPRInfo] = {}
        for provider_id in prepared_package_ids:
            if provider_id not in provider_prs:
                continue
            pull_request_list = [pull_requests[pr] for pr in provider_prs[provider_id] if pr in pull_requests]
            provider_yaml_dict = get_provider_distributions_metadata().get(provider_id)
            if pull_request_list:
                if only_available_in_dist:
                    package_suffix = get_suffix_from_package_in_dist(files_in_dist, provider_id)
                else:
                    package_suffix = ""
                providers[provider_id] = ProviderPRInfo(
                    version=provider_yaml_dict["versions"][0],
                    provider_id=provider_id,
                    pypi_package_name=provider_yaml_dict["package-name"],
                    pr_list=pull_request_list,
                    suffix=package_suffix if package_suffix else "",
                )
        template = jinja2.Template(
            (Path(__file__).parents[1] / "provider_issue_TEMPLATE.md.jinja2").read_text()
        )
        issue_content = template.render(providers=providers, linked_issues=linked_issues, date=datetime.now())
        get_console().print()
        get_console().print(
            "[green]Below you can find the issue content that you can use "
            "to ask contributor to test providers![/]"
        )
        get_console().print()
        get_console().print()
        get_console().print(
            "Issue title: [warning]Status of testing Providers that were "
            f"prepared on {datetime.now():%B %d, %Y}[/]"
        )
        get_console().print()
        issue_content += "\n"
        users: set[str] = set()
        for provider_info in providers.values():
            for pr in provider_info.pr_list:
                if pr.user.login:
                    users.add("@" + pr.user.login)
        issue_content += f"All users involved in the PRs:\n{' '.join(users)}"
        syntax = Syntax(issue_content, "markdown", theme="ansi_dark")
        get_console().print(syntax)
        create_issue = user_confirm("Should I create the issue?")
        if create_issue == Answer.YES:
            res = run_command(
                [
                    "gh",
                    "issue",
                    "create",
                    "-t",
                    f"Status of testing Providers that were prepared on {datetime.now():%B %d, %Y}",
                    "-b",
                    issue_content,
                    "-l",
                    "testing status,kind:meta",
                    "-w",
                ],
                check=False,
            )
            if res.returncode != 0:
                get_console().print(
                    "Failed to create issue. If the error is about 'too long URL' you have "
                    "to create the issue manually by copy&pasting the above output"
                )
                sys.exit(1)


def get_git_log_command(
    verbose: bool,
    from_commit: str | None = None,
    to_commit: str | None = None,
    is_helm_chart: bool = True,
) -> list[str]:
    git_cmd = [
        "git",
        "log",
        "--pretty=format:%H %h %cd %s",
        "--date=short",
    ]
    if from_commit and to_commit:
        git_cmd.append(f"{from_commit}...{to_commit}")
    elif from_commit:
        git_cmd.append(from_commit)
    if is_helm_chart:
        git_cmd.extend(["--", "chart/"])
    else:
        git_cmd.extend(["--", "."])
    if verbose:
        get_console().print(f"Command to run: '{' '.join(git_cmd)}'")
    return git_cmd


class Change(NamedTuple):
    """Stores details about commits"""

    full_hash: str
    short_hash: str
    date: str
    message: str
    message_without_backticks: str
    pr: int | None


def get_change_from_line(line: str):
    split_line = line.split(" ", maxsplit=3)
    message = split_line[3]
    pr = None
    pr_match = PR_PATTERN.match(message)
    if pr_match:
        pr = pr_match.group(1)
    return Change(
        full_hash=split_line[0],
        short_hash=split_line[1],
        date=split_line[2],
        message=message,
        message_without_backticks=message.replace("`", "'").replace("&#39;", "'").replace("&amp;", "&"),
        pr=int(pr) if pr else None,
    )


def get_changes(
    verbose: bool,
    previous_release: str,
    current_release: str,
    is_helm_chart: bool = False,
) -> list[Change]:
    print(MY_DIR_PATH, SOURCE_DIR_PATH)
    change_strings = subprocess.check_output(
        get_git_log_command(
            verbose,
            from_commit=previous_release,
            to_commit=current_release,
            is_helm_chart=is_helm_chart,
        ),
        cwd=SOURCE_DIR_PATH,
        text=True,
    )
    return [get_change_from_line(line) for line in change_strings.splitlines()]


def render_template(
    template_name: str,
    context: dict[str, Any],
    autoescape: bool = True,
    keep_trailing_newline: bool = False,
) -> str:
    import jinja2

    template_loader = jinja2.FileSystemLoader(searchpath=MY_DIR_PATH)
    template_env = jinja2.Environment(
        loader=template_loader,
        undefined=jinja2.StrictUndefined,
        autoescape=autoescape,
        keep_trailing_newline=keep_trailing_newline,
    )
    template = template_env.get_template(f"{template_name}_TEMPLATE.md.jinja2")
    content: str = template.render(context)
    return content


def print_issue_content(
    current_release: str,
    pull_requests,
    linked_issues,
    users: dict[int, set[str]],
    is_helm_chart: bool = False,
):
    link = f"https://pypi.org/project/apache-airflow/{current_release}/"
    link_text = f"Apache Airflow RC {current_release}"
    if is_helm_chart:
        link = f"https://dist.apache.org/repos/dist/dev/airflow/{current_release}"
        link_text = f"Apache Airflow Helm Chart {current_release.split('/')[-1]}"
    # Only include PRs that have corresponding user data to avoid KeyError in template
    pr_list = sorted([pr for pr in pull_requests.keys() if pr in users])
    user_logins: dict[int, str] = {pr: " ".join(f"@{u}" for u in uu) for pr, uu in users.items()}
    all_users: set[str] = set()
    for user_list in users.values():
        all_users.update(user_list)
    all_users = {user for user in all_users if user not in {"github-actions[bot]", "dependabot[bot]"}}
    all_user_logins = " ".join(f"@{u}" for u in all_users)
    content = render_template(
        template_name="ISSUE",
        context={
            "link": link,
            "link_text": link_text,
            "pr_list": pr_list,
            "pull_requests": pull_requests,
            "linked_issues": linked_issues,
            "users": users,
            "user_logins": user_logins,
            "all_user_logins": all_user_logins,
        },
        autoescape=False,
        keep_trailing_newline=True,
    )
    print(content)


@release_management_group.command(
    name="generate-issue-content-helm-chart",
    help="Generates content for issue to test the helm chart release.",
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
@click.option(
    "--previous-release",
    type=str,
    help="commit reference (for example hash or tag) of the previous release.",
    required=True,
)
@click.option(
    "--current-release",
    type=str,
    help="commit reference (for example hash or tag) of the current release.",
    required=True,
)
@click.option("--excluded-pr-list", type=str, help="Coma-separated list of PRs to exclude from the issue.")
@click.option(
    "--limit-pr-count",
    type=int,
    default=None,
    help="Limit PR count processes (useful for testing small subset of PRs).",
)
@option_verbose
def generate_issue_content_helm_chart(
    github_token: str,
    previous_release: str,
    current_release: str,
    excluded_pr_list: str,
    limit_pr_count: int | None,
):
    generate_issue_content(
        github_token,
        previous_release,
        current_release,
        excluded_pr_list,
        limit_pr_count,
        is_helm_chart=True,
    )


@release_management_group.command(
    name="generate-issue-content-core", help="Generates content for issue to test the core release."
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
@click.option(
    "--previous-release",
    type=str,
    help="commit reference (for example hash or tag) of the previous release.",
    required=True,
)
@click.option(
    "--current-release",
    type=str,
    help="commit reference (for example hash or tag) of the current release.",
    required=True,
)
@click.option("--excluded-pr-list", type=str, help="Coma-separated list of PRs to exclude from the issue.")
@click.option(
    "--limit-pr-count",
    type=int,
    default=None,
    help="Limit PR count processes (useful for testing small subset of PRs).",
)
@option_verbose
def generate_issue_content_core(
    github_token: str,
    previous_release: str,
    current_release: str,
    excluded_pr_list: str,
    limit_pr_count: int | None,
):
    generate_issue_content(
        github_token,
        previous_release,
        current_release,
        excluded_pr_list,
        limit_pr_count,
        is_helm_chart=False,
    )


@release_management_group.command(
    name="generate-providers-metadata", help="Generates metadata for providers."
)
@click.option(
    "--refresh-constraints-and-airflow-releases",
    is_flag=True,
    envvar="REFRESH_CONSTRAINTS_AND_AIRFLOW_RELEASES",
    help="Refresh constraints and airflow_releases before generating metadata",
)
@click.option(
    "--provider-id",
    type=BetterChoice(get_available_distributions(include_removed=True, include_suspended=True)),
    envvar="PROVIDER_ID",
    help="Provider_id to generate metadata for. If not specified, metadata for all packages will be generated. "
    "This is debug-only option. When this option is specified, the metadata produced for "
    "the distribution will not be written to the file, but printed via console.",
)
@click.option(
    "--provider-version",
    type=str,
    envvar="PROVIDER_VERSION",
    help="Provider version to generate metadata for. Only used when --provider-id is specified. Limits running "
    "metadata generation to only this version of the provider.",
)
@option_github_token
@option_dry_run
@option_verbose
def generate_providers_metadata(
    refresh_constraints_and_airflow_releases: bool,
    provider_id: str | None,
    provider_version: str | None,
    github_token: str | None,
):
    import json

    if PROVIDER_METADATA_JSON_PATH.exists():
        current_metadata = json.loads(PROVIDER_METADATA_JSON_PATH.read_text())
    else:
        current_metadata = {}
    metadata_dict: dict[str, dict[str, dict[str, str]]] = {}
    all_airflow_releases, airflow_release_dates = get_all_constraint_files_and_airflow_releases(
        refresh_constraints_and_airflow_releases=refresh_constraints_and_airflow_releases,
        airflow_constraints_mode=CONSTRAINTS,
        github_token=github_token,
    )
    constraints = load_constraints()
    if provider_id:
        get_console().print(f"[info]Generating metadata for provider {provider_id} only (for debugging)[/]")
        result = generate_providers_metadata_for_provider(
            provider_id,
            provider_version=provider_version,
            constraints=constraints,
            all_airflow_releases=all_airflow_releases,
            airflow_release_dates=airflow_release_dates,
            current_metadata=current_metadata,
        )
        if result:
            metadata_dict[provider_id] = result
        get_console().print(metadata_dict)
        return

    partial_generate_providers_metadata = partial(
        generate_providers_metadata_for_provider,
        provider_version=None,
        constraints=constraints,
        all_airflow_releases=all_airflow_releases,
        airflow_release_dates=airflow_release_dates,
        current_metadata=current_metadata,
    )
    package_ids = get_provider_dependencies().keys()
    with Pool() as pool:
        results = pool.map(
            partial_generate_providers_metadata,
            package_ids,
        )
    for package_id, result in zip(package_ids, results):
        if result:
            metadata_dict[package_id] = result

    PROVIDER_METADATA_JSON_PATH.write_text(json.dumps(metadata_dict, indent=4) + "\n")


@release_management_group.command(
    name="update-providers-next-version",
    help="Update provider versions marked with '# use next version' comment.",
)
@option_verbose
def update_providers_next_version():
    """
    Scan all provider pyproject.toml files for dependencies with "# use next version" comment
    and update them to use the current version from the referenced provider's pyproject.toml.
    """
    from airflow_breeze.utils.packages import update_providers_with_next_version_comment

    # make sure dependencies are regenerated before we start
    regenerate_provider_dependencies_once()
    get_console().print("\n[info]Scanning for providers with '# use next version' comments...\n")

    updates_made = update_providers_with_next_version_comment()

    if updates_made:
        get_console().print("\n[success]Summary of updates:[/]")
        for provider_id, dependencies in updates_made.items():
            get_console().print(f"\n[info]Provider: {provider_id}[/]")
            for dep_name, dep_info in dependencies.items():
                get_console().print(f"  • {dep_name}: {dep_info['old_version']} → {dep_info['new_version']}")
        get_console().print(
            f"\n[success]Updated {len(updates_made)} provider(s) with "
            f"{sum(len(deps) for deps in updates_made.values())} dependency change(s).[/]"
        )
        # Regenerate provider dependencies after some of them changed
        regenerate_provider_dependencies_once.cache_clear()
        regenerate_provider_dependencies_once()
    else:
        get_console().print(
            "\n[info]No updates needed. All providers with '# use next version' "
            "comments are already using the latest versions.[/]"
        )


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
    get_console().print("[warning]The file has not been modified.[/]")
    return False


def modify_all_constraint_files(
    constraints_repo: Path,
    updated_constraint: tuple[str, ...] | None,
    comment_file: Path | None,
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
        if modify_single_file_constraints(constraints_file, updated_constraint, comment_file):
            modified = True
    return modified


def confirm_modifications(constraints_repo: Path) -> bool:
    run_command(["git", "diff"], cwd=constraints_repo, env={"PAGER": ""})
    confirm = user_confirm("Do you want to continue?")
    if confirm == Answer.YES:
        return True
    if confirm == Answer.NO:
        return False
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


@release_management_group.command(
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


def split_version_and_suffix(file_name: str, suffix: str) -> VersionedFile:
    from packaging.version import Version

    no_suffix_file = file_name[: -len(suffix)]
    no_version_file, version = no_suffix_file.rsplit("-", 1)
    no_version_file = no_version_file.replace("_", "-")
    return VersionedFile(
        base=no_version_file + "-",
        version=version,
        suffix=suffix,
        type=no_version_file + "-" + suffix,
        comparable_version=Version(version),
        file_name=file_name,
    )


def split_date_version_and_suffix(file_name: str, suffix: str) -> VersionedFile:
    """Split file name with date-based version (YYYY-MM-DD format) and suffix.

    Example: apache_airflow_providers-2025-11-18-source.tar.gz
    """
    from packaging.version import Version

    no_suffix_file = file_name[: -len(suffix)]
    # Date format is YYYY-MM-DD, so we need to extract last 3 parts
    parts = no_suffix_file.rsplit("-", 3)
    if len(parts) != 4:
        raise ValueError(f"Invalid date-versioned file name format: {file_name}")

    no_version_file = parts[0]
    date_version = f"{parts[1]}-{parts[2]}-{parts[3]}"

    # Validate date format
    try:
        datetime.strptime(date_version, "%Y-%m-%d")
    except ValueError as e:
        raise ValueError(f"Invalid date format in file name {file_name}: {e}")

    no_version_file = no_version_file.replace("_", "-")

    # Convert date to a comparable version format (YYYYMMDD as integer-like version)
    comparable_date_str = date_version.replace("-", ".")

    return VersionedFile(
        base=no_version_file + "-",
        version=date_version,
        suffix=suffix,
        type=no_version_file + "-" + suffix,
        comparable_version=Version(comparable_date_str),
        file_name=file_name,
    )


PYTHON_CLIENT_DIR_PATH = AIRFLOW_ROOT_PATH / "clients" / "python"
PYTHON_CLIENT_DIST_DIR_PATH = PYTHON_CLIENT_DIR_PATH / "dist"
PYTHON_CLIENT_TMP_DIR = PYTHON_CLIENT_DIR_PATH / "tmp"

REPRODUCIBLE_BUILD_YAML = AIRFLOW_ROOT_PATH / "reproducible_build.yaml"

VERSION_FILE = PYTHON_CLIENT_DIR_PATH / "version.txt"
SOURCE_API_YAML_PATH = (
    AIRFLOW_ROOT_PATH / "airflow-core/src/airflow/api_fastapi/core_api/openapi/v2-rest-api-generated.yaml"
)
TARGET_API_YAML_PATH = PYTHON_CLIENT_DIR_PATH / "v2.yaml"
OPENAPI_GENERATOR_CLI_VER = "7.13.0"

GENERATED_CLIENT_DIRECTORIES_TO_COPY: list[Path] = [
    Path("airflow_client") / "client",
    Path("docs"),
    Path("test"),
]
FILES_TO_COPY_TO_CLIENT_REPO = [
    ".openapi-generator-ignore",
    "CHANGELOG.md",
    "README.md",
    "INSTALL",
    "LICENSE",
    "NOTICE",
    "pyproject.toml",
    "test_python_client.py",
    "version.txt",
]


def _get_python_client_version(version_suffix=None):
    from packaging.version import Version

    python_client_version = VERSION_FILE.read_text().strip()
    version = Version(python_client_version)
    if version_suffix:
        if version.pre:
            current_suffix = version.pre[0] + str(version.pre[1])
            if current_suffix != version_suffix:
                get_console().print(
                    f"[error]The version suffix for PyPI ({version_suffix}) does not match the "
                    f"suffix in the version ({version})[/]"
                )
                sys.exit(1)
        return version.base_version + version_suffix
    return python_client_version


def _generate_python_client_sources(python_client_version: str) -> None:
    get_console().print(f"\n[info]Generating client code in {PYTHON_CLIENT_TMP_DIR}[/]")
    result = run_command(
        [
            "docker",
            "run",
            "--rm",
            "-u",
            f"{os.getuid()}:{os.getgid()}",
            "-v",
            f"{TARGET_API_YAML_PATH}:/spec.yaml",
            "-v",
            f"{PYTHON_CLIENT_TMP_DIR}:/output",
            f"openapitools/openapi-generator-cli:v{OPENAPI_GENERATOR_CLI_VER}",
            "generate",
            "--input-spec",
            "/spec.yaml",
            "--generator-name",
            "python",
            "--git-user-id",
            f"{os.environ.get('GIT_USER')}",
            "--output",
            "/output",
            "--package-name",
            "airflow_client.client",
            "--git-repo-id",
            "airflow-client-python",
            "--additional-properties",
            f"packageVersion={python_client_version}",
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        get_console().print("[error]Failed to generate client code[/]")
        get_console().print(result.stdout, markup=False)
        get_console().print(result.stderr, markup=False, style="error")
        sys.exit(result.returncode)
    get_console().print(f"[success]Generated client code in {PYTHON_CLIENT_TMP_DIR}:[/]")
    get_console().print(f"\n[info]Content of {PYTHON_CLIENT_TMP_DIR}:[/]")
    for file in sorted(PYTHON_CLIENT_TMP_DIR.glob("*")):
        get_console().print(f"[info]  {file.name}[/]")
    get_console().print()


def _copy_selected_sources_from_tmp_directory_to_clients_python():
    get_console().print(
        f"[info]Copying selected sources: {GENERATED_CLIENT_DIRECTORIES_TO_COPY} from "
        f"{PYTHON_CLIENT_TMP_DIR} to {PYTHON_CLIENT_DIR_PATH}[/]"
    )
    for dir in GENERATED_CLIENT_DIRECTORIES_TO_COPY:
        source_dir = PYTHON_CLIENT_TMP_DIR / dir
        target_dir = PYTHON_CLIENT_DIR_PATH / dir
        get_console().print(f"[info]  Copying generated sources from {source_dir} to {target_dir}[/]")
        shutil.rmtree(target_dir, ignore_errors=True)
        shutil.copytree(source_dir, target_dir)
        get_console().print(f"[success]  Copied generated sources from {source_dir} to {target_dir}[/]")
    get_console().print(
        f"[info]Copied selected sources {GENERATED_CLIENT_DIRECTORIES_TO_COPY} from "
        f"{PYTHON_CLIENT_TMP_DIR} to {PYTHON_CLIENT_DIR_PATH}[/]\n"
    )
    get_console().print(f"\n[info]Content of {PYTHON_CLIENT_DIR_PATH}:[/]")
    for file in sorted(PYTHON_CLIENT_DIR_PATH.glob("*")):
        get_console().print(f"[info]  {file.name}[/]")
    get_console().print()


def _build_client_packages_with_hatch(source_date_epoch: int, distribution_format: str):
    command = [
        "hatch",
        "build",
        "-c",
    ]
    if distribution_format == "sdist" or distribution_format == "both":
        command += ["-t", "sdist"]
    if distribution_format == "wheel" or distribution_format == "both":
        command += ["-t", "wheel"]
    env_copy = os.environ.copy()
    env_copy["SOURCE_DATE_EPOCH"] = str(source_date_epoch)
    run_command(
        cmd=command,
        cwd=PYTHON_CLIENT_DIR_PATH,
        env=env_copy,
        check=True,
    )
    shutil.copytree(PYTHON_CLIENT_DIST_DIR_PATH, AIRFLOW_DIST_PATH, dirs_exist_ok=True)


def _build_client_packages_with_docker(source_date_epoch: int, distribution_format: str):
    _build_local_build_image()
    command = "hatch build -c "
    if distribution_format == "sdist" or distribution_format == "both":
        command += "-t sdist "
    if distribution_format == "wheel" or distribution_format == "both":
        command += "-t wheel "
    container_id = f"airflow-build-{random.getrandbits(64):08x}"
    result = run_command(
        cmd=[
            "docker",
            "run",
            "--name",
            container_id,
            "-t",
            "-e",
            f"SOURCE_DATE_EPOCH={source_date_epoch}",
            "-e",
            "HOME=/opt/airflow/files/home",
            "-e",
            "GITHUB_ACTIONS",
            "-w",
            "/opt/airflow/clients/python",
            AIRFLOW_BUILD_IMAGE_TAG,
            "bash",
            "-c",
            command,
        ],
        check=False,
    )
    if result.returncode != 0:
        get_console().print("[error]Error preparing Python client packages[/]")
        fix_ownership_using_docker()
        sys.exit(result.returncode)
    AIRFLOW_DIST_PATH.mkdir(parents=True, exist_ok=True)
    get_console().print()
    # Copy all files in the dist directory in container to the host dist directory (note '/.' in SRC)
    run_command(["docker", "cp", f"{container_id}:/opt/airflow/clients/python/dist/.", "./dist"], check=True)
    run_command(["docker", "rm", "--force", container_id], check=False, stdout=DEVNULL, stderr=DEVNULL)


@release_management_group.command(name="prepare-python-client", help="Prepares python client packages.")
@option_distribution_format
@option_version_suffix
@option_use_local_hatch
@click.option(
    "--python-client-repo",
    envvar="PYTHON_CLIENT_REPO",
    type=click.Path(file_okay=False, dir_okay=True, path_type=Path, exists=True),
    help="Directory where the python client repo is checked out",
)
@click.option(
    "--only-publish-build-scripts",
    envvar="ONLY_PUBLISH_BUILD_SCRIPTS",
    is_flag=True,
    help="Only publish updated build scripts to python client repo, not generated client code.",
)
@click.option(
    "--security-schemes",
    default="Basic,GoogleOpenID,Kerberos",
    envvar="SECURITY_SCHEMES",
    show_default=True,
    help="Security schemes to be added to the API documentation (coma separated)",
)
@option_dry_run
@option_verbose
def prepare_python_client(
    distribution_format: str,
    version_suffix: str,
    use_local_hatch: bool,
    python_client_repo: Path | None,
    only_publish_build_scripts: bool,
    security_schemes: str,
):
    shutil.rmtree(PYTHON_CLIENT_TMP_DIR, ignore_errors=True)
    PYTHON_CLIENT_TMP_DIR.mkdir(parents=True, exist_ok=True)
    shutil.copy(src=SOURCE_API_YAML_PATH, dst=TARGET_API_YAML_PATH)
    import yaml

    openapi_yaml = yaml.safe_load(TARGET_API_YAML_PATH.read_text())

    # Client generator does not yet support OpenAPI 3.1 fully

    def fix_anyof_null_and_required(obj):
        """
        Fixes OpenAPI 3.1 `anyOf` constructs with `type: null` to be compatible with OpenAPI 3.0 generators.

        Specifically:
        - Replaces `anyOf: [<type>, {"type": "null"}]` with the base type + `nullable: true`
        - Ensures such fields are treated as optional by removing them from any `required` list

        This is a workaround for https://github.com/OpenAPITools/openapi-generator issues with `"null"` handling.
        Note: `type: "null"` is valid OpenAPI 3.1, but openapi-generator treats it incorrectly in some Python generators.
        """
        if isinstance(obj, dict):
            if "anyOf" in obj:
                types = [x.get("type") for x in obj["anyOf"] if isinstance(x, dict)]
                if "null" in types and len(types) == 2:
                    non_null_type = next(t for t in obj["anyOf"] if t.get("type") != "null")
                    return {**non_null_type, "nullable": True}

            # Fix `required` list by removing nullable fields
            if "required" in obj and "properties" in obj:
                new_required = []
                for field in obj["required"]:
                    prop = obj["properties"].get(field, {})
                    if (
                        isinstance(prop, dict)
                        and "anyOf" in prop
                        and any(t.get("type") == "null" for t in prop["anyOf"] if isinstance(t, dict))
                    ):
                        continue
                    new_required.append(field)
                obj["required"] = new_required

            return {k: fix_anyof_null_and_required(v) for k, v in obj.items()}
        if isinstance(obj, list):
            return [fix_anyof_null_and_required(i) for i in obj]
        return obj

    openapi_yaml = fix_anyof_null_and_required(openapi_yaml)

    # Add security schemes to documentation
    security: list[dict[str, Any]] = []
    for scheme in security_schemes.split(","):
        security.append({scheme: []})
    openapi_yaml["security"] = security
    TARGET_API_YAML_PATH.write_text(yaml.dump(openapi_yaml))

    def patch_trigger_dag_run_post_body():
        """
        Post-process the generated `TriggerDAGRunPostBody` model to explicitly include `"logical_date": None`
        in the `to_dict()` output if it was not set.

        Why this is needed:
        - The Airflow API server expects the `logical_date` field to always be present in the request payload,
          even if its value is `null`.
        - By default, the OpenAPI-generated Pydantic model uses `model_dump(exclude_none=True)`, which omits
          any fields set to `None`, including `logical_date`.
        - This causes a 422 error from the server when the field is missing, despite it being marked `nullable`.

        Since we cannot fix this cleanly via OpenAPI spec due to OpenAPI Generator's limitations with
        `nullable` and `anyOf`, we insert an explicit fallback into `to_dict()` after client codegen.

        This patch:
        - Locates the `_dict = self.model_dump(...)` line in `to_dict()`
        - Inserts a conditional to add `"logical_date": None` if it's missing
        """
        current_python_version = f"{sys.version_info.major}.{sys.version_info.minor}"
        if current_python_version != DEFAULT_PYTHON_MAJOR_MINOR_VERSION:
            get_console().print(
                f"[error]Python version mismatch: current version is {current_python_version}, "
                f"but default version is {DEFAULT_PYTHON_MAJOR_MINOR_VERSION} - this might cause "
                f"reproducibility problems with prepared package.[/]"
            )
            get_console().print(
                f"[info]Please reinstall breeze with uv using Python {DEFAULT_PYTHON_MAJOR_MINOR_VERSION}:[/]"
            )
            get_console().print(
                f"\nuv tool install --python {DEFAULT_PYTHON_MAJOR_MINOR_VERSION} -e ./dev/breeze --force\n"
            )
            sys.exit(1)

        TRIGGER_MODEL_PATH = PYTHON_CLIENT_TMP_DIR / Path(
            "airflow_client/client/models/trigger_dag_run_post_body.py"
        )

        class LogicalDateDictPatch(ast.NodeTransformer):
            def visit_FunctionDef(self, node: ast.FunctionDef) -> ast.FunctionDef:
                if node.name != "to_dict":
                    return node

                # Inject this:
                injected = ast.parse('if "logical_date" not in _dict:\n    _dict["logical_date"] = None').body

                for idx, stmt in enumerate(node.body):
                    if (
                        isinstance(stmt, ast.Assign)
                        and isinstance(stmt.targets[0], ast.Name)
                        and stmt.targets[0].id == "_dict"
                        and isinstance(stmt.value, ast.Call)
                        and isinstance(stmt.value.func, ast.Attribute)
                        and stmt.value.func.attr == "model_dump"
                    ):
                        node.body.insert(idx + 1, *injected)
                        break

                return node

        source = TRIGGER_MODEL_PATH.read_text(encoding="utf-8")
        tree = ast.parse(source)
        LogicalDateDictPatch().visit(tree)
        ast.fix_missing_locations(tree)
        TRIGGER_MODEL_PATH.write_text(ast.unparse(tree), encoding="utf-8")

    _generate_python_client_sources(python_client_version=_get_python_client_version())

    # Call this after codegen and before packaging
    try:
        patch_trigger_dag_run_post_body()
    except Exception:
        get_console().print("[warning]Failed to patch trigger_dag_run_post_body.py - skipping this step[/]")

    _copy_selected_sources_from_tmp_directory_to_clients_python()

    reproducible_build_yaml = yaml.safe_load(REPRODUCIBLE_BUILD_YAML.read_text())
    source_date_epoch = reproducible_build_yaml["source-date-epoch"]

    if python_client_repo:
        if not only_publish_build_scripts:
            get_console().print(
                f"[info]Copying generated client from {PYTHON_CLIENT_DIR_PATH} to {python_client_repo}[/]"
            )
            for dir in GENERATED_CLIENT_DIRECTORIES_TO_COPY:
                source_dir = PYTHON_CLIENT_DIR_PATH / dir
                target_dir = python_client_repo / dir
                get_console().print(f"[info]  Copying {source_dir} to {target_dir}[/]")
                shutil.rmtree(target_dir, ignore_errors=True)
                shutil.copytree(source_dir, target_dir)
                get_console().print(f"[success]  Copied {source_dir} to {target_dir}[/]")
            get_console().print(
                f"[info]Copied generated client from {PYTHON_CLIENT_DIR_PATH} to {python_client_repo}[/]"
            )
        get_console().print(
            f"[info]Copying build scripts from {PYTHON_CLIENT_DIR_PATH} to {python_client_repo}[/]"
        )
        for file in FILES_TO_COPY_TO_CLIENT_REPO:
            source_file = PYTHON_CLIENT_DIR_PATH / file
            target_file = python_client_repo / file
            get_console().print(f"[info]  Copying {source_file} to {target_file}[/]")
            shutil.copy(source_file, target_file)
            get_console().print(f"[success]  Copied {source_file} to {target_file}[/]")
        get_console().print(
            f"[success]Copied build scripts from {PYTHON_CLIENT_DIR_PATH} to {python_client_repo}[/]"
        )
        spec_dir = python_client_repo / "spec"
        spec_dir.mkdir(parents=True, exist_ok=True)
        source_spec_file = PYTHON_CLIENT_DIR_PATH / "v2.yaml"
        target_spec_file = spec_dir / "v2.yaml"
        get_console().print(f"[info]  Copying {source_spec_file} to {target_spec_file}[/]")
        shutil.copy(source_spec_file, target_spec_file)
        get_console().print(f"[success]  Copied {source_spec_file} to {target_spec_file}[/]")

        # Copy gitignore file
        source_gitignore_file = PYTHON_CLIENT_DIR_PATH / "python-client.gitignore"
        target_gitignore_file = python_client_repo / ".gitignore"
        get_console().print(f"[info]  Copying {source_gitignore_file} to {target_gitignore_file}[/]")
        shutil.copy(source_gitignore_file, target_gitignore_file)
        get_console().print(f"[success]  Copied {source_gitignore_file} to {target_gitignore_file}[/]")

        get_console().print(
            f"[success]Copied client code from {PYTHON_CLIENT_DIR_PATH} to {python_client_repo}[/]\n"
        )
    else:
        get_console().print(
            "\n[warning]No python client repo directory provided - skipping copying the generated client[/]\n"
        )
    get_console().print(f"\n[info]Building packages in {PYTHON_CLIENT_DIST_DIR_PATH}[/]\n")
    shutil.rmtree(PYTHON_CLIENT_DIST_DIR_PATH, ignore_errors=True)
    PYTHON_CLIENT_DIST_DIR_PATH.mkdir(parents=True, exist_ok=True)
    version = _get_python_client_version(version_suffix)
    original_version = VERSION_FILE.read_text().strip()
    if version_suffix:
        VERSION_FILE.write_text(version + "\n")
    try:
        if use_local_hatch:
            _build_client_packages_with_hatch(
                source_date_epoch=source_date_epoch, distribution_format=distribution_format
            )
        else:
            _build_client_packages_with_docker(
                source_date_epoch=source_date_epoch, distribution_format=distribution_format
            )
        get_console().print(f"\n[success]Built packages in {AIRFLOW_DIST_PATH}[/]\n")
    finally:
        if version_suffix:
            VERSION_FILE.write_text(original_version)


CHART_DIR = AIRFLOW_ROOT_PATH / "chart"
CHART_YAML_FILE = CHART_DIR / "Chart.yaml"
VALUES_YAML_FILE = CHART_DIR / "values.yaml"


@release_management_group.command(name="prepare-helm-chart-tarball", help="Prepares helm chart tarball.")
@click.option(
    "--version",
    help="Version used for helm chart. This version has to be set and has to match the version in "
    "Chart.yaml, unless the --ignore-version-check flag is used.",
    envvar="VERSION",
)
@click.option(
    "--version-suffix",
    help="Version suffix used to publish the package. Needs to be present as we always build "
    "archive using release candidate tag.",
    required=True,
    envvar="VERSION_SUFFIX",
)
@click.option(
    "--ignore-version-check",
    is_flag=True,
    help="Ignores result of version update check. Produce tarball regardless of "
    "whether version is correctly set in the Chart.yaml.",
)
@click.option(
    "--skip-tagging",
    is_flag=True,
    help="Skip tagging the chart. Useful if the tag is already created, or when you verify the chart.",
)
@click.option(
    "--skip-tag-signing",
    is_flag=True,
    help="Skip signing the tag. Useful for CI where we just tag without signing the tag.",
)
@click.option(
    "--override-tag",
    is_flag=True,
    help="Override tag if it already exists. Useful when you want to re-create the tag, usually when you"
    "test the breeze command locally.",
)
@option_dry_run
@option_verbose
def prepare_helm_chart_tarball(
    version: str | None,
    version_suffix: str,
    ignore_version_check: bool,
    override_tag: bool,
    skip_tagging: bool,
    skip_tag_signing: bool,
) -> None:
    import yaml

    chart_yaml_file_content = CHART_YAML_FILE.read_text()
    chart_yaml_dict = yaml.safe_load(chart_yaml_file_content)
    version_in_chart = chart_yaml_dict["version"]
    airflow_version_in_chart = chart_yaml_dict["appVersion"]
    values_content = yaml.safe_load(VALUES_YAML_FILE.read_text())
    airflow_version_in_values = values_content["airflowVersion"]
    default_airflow_tag_in_values = values_content["defaultAirflowTag"]

    # Check if this is an RC version and replace documentation links with staging URLs
    is_rc_version = version_suffix and "rc" in version_suffix.lower()
    if is_rc_version:
        get_console().print(
            f"[info]RC version detected ({version_suffix}). Replacing documentation links with staging URLs.[/]"
        )
        # Replace production URLs with staging URLs for RC versions
        chart_yaml_file_content = chart_yaml_file_content.replace(
            "https://airflow.apache.org/", "https://airflow.staged.apache.org/"
        )
        get_console().print("[success]Documentation links updated to staging environment for RC version.[/]")
    if ignore_version_check:
        if not version:
            version = version_in_chart
    else:
        if not version or not version_suffix:
            get_console().print(
                "[error]You need to provide --version and --version-suffix parameter unless you "
                "use --ignore-version-check[/]"
            )
            sys.exit(1)
    get_console().print(f"[info]Airflow version in values.yaml: {airflow_version_in_values}[/]")
    get_console().print(f"[info]Default Airflow Tag in values.yaml: {default_airflow_tag_in_values}[/]")
    get_console().print(f"[info]Airflow version in Chart.yaml: {airflow_version_in_chart}[/]")
    if airflow_version_in_values != default_airflow_tag_in_values:
        get_console().print(
            f"[error]Airflow version ({airflow_version_in_values}) does not match the "
            f"defaultAirflowTag ({default_airflow_tag_in_values}) in values.yaml[/]"
        )
        sys.exit(1)
    updating = False
    if version_in_chart != version:
        get_console().print(
            f"[warning]Version in chart.yaml ({version_in_chart}) does not match the version "
            f"passed as parameter ({version}). Updating[/]"
        )
        updating = True
        chart_yaml_file_content = chart_yaml_file_content.replace(
            f"version: {version_in_chart}", f"version: {version}"
        )
    else:
        get_console().print(f"[success]Version in chart.yaml is good: {version}[/]")
    if airflow_version_in_values != airflow_version_in_chart:
        get_console().print(
            f"[warning]Airflow version in Chart.yaml ({airflow_version_in_chart}) does not match the "
            f"airflow version ({airflow_version_in_values}) in values.yaml. Updating[/]"
        )
        updating = True
        chart_yaml_file_content = chart_yaml_file_content.replace(
            f"appVersion: {airflow_version_in_chart}", f"appVersion: {airflow_version_in_values}"
        )
    else:
        get_console().print(
            f"[success]Airflow version in chart.yaml matches the airflow version in values.yaml: "
            f"({airflow_version_in_values})[/]"
        )
    if updating:
        CHART_YAML_FILE.write_text(chart_yaml_file_content)
        get_console().print("\n[warning]Versions of the chart has been updated[/]\n")
        if ignore_version_check:
            get_console().print(
                "[warning]Ignoring the version check. "
                "The tarball will be created but it should not be published[/]"
            )
        else:
            get_console().print(
                "\n[info]Please create a PR with that change, get it merged, and try again.[/]\n"
            )
            sys.exit(1)
    tag_with_suffix = f"helm-chart/{version}{version_suffix}"
    if not skip_tagging:
        get_console().print(f"[info]Tagging the chart with {tag_with_suffix}[/]")
        tag_command = [
            "git",
            "tag",
            tag_with_suffix,
            "-m",
            f"Apache Airflow Helm Chart {version}{version_suffix}",
        ]
        if override_tag:
            tag_command.append("--force")
        if not skip_tag_signing:
            tag_command.append("--sign")
        result = run_command(tag_command, check=False)
        if result.returncode != 0:
            get_console().print(f"[error]Error tagging the chart with {tag_with_suffix}.\n")
            get_console().print(
                "[warning]If you are sure the tag is set correctly, you can add --skip-tagging"
                " flag to the command[/]"
            )
            sys.exit(result.returncode)
    else:
        get_console().print(f"[warning]Skipping tagging the chart with {tag_with_suffix}[/]")
    get_console().print(f"[info]Creating tarball for Helm Chart {tag_with_suffix}[/]")
    archive_name = f"airflow-chart-{version}-source.tar.gz"
    OUT_PATH.mkdir(parents=True, exist_ok=True)
    source_archive = OUT_PATH / archive_name
    source_archive.unlink(missing_ok=True)
    result = run_command(
        [
            "git",
            "-c",
            "tar.umask=0077",
            "archive",
            "--format=tar.gz",
            tag_with_suffix,
            f"--prefix=airflow-chart-{version}/",
            "-o",
            source_archive.as_posix(),
            "chart",
            ".rat-excludes",
        ],
        check=False,
    )
    if result.returncode != 0:
        get_console().print(f"[error]Error running git archive for Helm Chart {tag_with_suffix}[/]")
        sys.exit(result.returncode)
    AIRFLOW_DIST_PATH.mkdir(parents=True, exist_ok=True)
    final_archive = AIRFLOW_DIST_PATH / archive_name
    final_archive.unlink(missing_ok=True)
    result = repack_deterministically(
        source_archive=source_archive,
        dest_archive=final_archive,
        prepend_path=None,
        timestamp=get_source_date_epoch(CHART_DIR),
    )
    if result.returncode != 0:
        get_console().print(
            f"[error]Error repackaging source tarball for Helm Chart from {source_archive} tp "
            f"{tag_with_suffix}[/]"
        )
        sys.exit(result.returncode)
    get_console().print(f"[success]Tarball created in {final_archive}")


@release_management_group.command(name="prepare-helm-chart-package", help="Prepares helm chart package.")
@click.option(
    "--sign-email",
    help="Email associated with the key used to sign the package.",
    envvar="SIGN_EMAIL",
    default="",
)
@click.option(
    "--version-suffix",
    help="Version suffix used to determine if RC version. For RC versions, documentation links will be replaced with staging URLs.",
    default="",
    envvar="VERSION_SUFFIX",
)
@option_dry_run
@option_verbose
def prepare_helm_chart_package(sign_email: str, version_suffix: str):
    import yaml

    from airflow_breeze.utils.kubernetes_utils import (
        K8S_BIN_BASE_PATH,
        make_sure_helm_installed,
        sync_virtualenv,
    )

    # Check if this is an RC version and temporarily replace documentation links
    chart_yaml_backup = None
    is_rc_version = version_suffix and "rc" in version_suffix.lower()

    if is_rc_version:
        get_console().print(
            f"[info]RC version detected ({version_suffix}). Temporarily replacing documentation links with staging URLs for packaging.[/]"
        )
        # Backup original content
        chart_yaml_backup = CHART_YAML_FILE.read_text()
        # Replace production URLs with staging URLs for RC versions
        chart_yaml_content = chart_yaml_backup.replace(
            "https://airflow.apache.org/", "https://airflow.staged.apache.org/"
        )
        CHART_YAML_FILE.write_text(chart_yaml_content)
        get_console().print(
            "[success]Documentation links temporarily updated to staging environment for RC version packaging.[/]"
        )

    try:
        chart_yaml_dict = yaml.safe_load(CHART_YAML_FILE.read_text())
        version = chart_yaml_dict["version"]
        result = sync_virtualenv(force_venv_setup=False)
        if result.returncode != 0:
            sys.exit(result.returncode)
        make_sure_helm_installed()
        get_console().print(f"[info]Packaging the chart for Helm Chart {version}[/]")
        k8s_env = os.environ.copy()
        k8s_env["PATH"] = str(K8S_BIN_BASE_PATH) + os.pathsep + k8s_env["PATH"]
        # Tar on modern unix options requires --wildcards parameter to work with globs
        # See https://github.com/technosophos/helm-gpg/issues/1
        k8s_env["TAR_OPTIONS"] = "--wildcards"
        archive_name = f"airflow-{version}.tgz"
        OUT_PATH.mkdir(parents=True, exist_ok=True)
        result = run_command(
            cmd=["helm", "package", "chart", "--dependency-update", "--destination", OUT_PATH.as_posix()],
            env=k8s_env,
            check=False,
        )
        if result.returncode != 0:
            get_console().print("[error]Error packaging the chart[/]")
            sys.exit(result.returncode)
        AIRFLOW_DIST_PATH.mkdir(parents=True, exist_ok=True)
        final_archive = AIRFLOW_DIST_PATH / archive_name
        final_archive.unlink(missing_ok=True)
        source_archive = OUT_PATH / archive_name
        result = repack_deterministically(
            source_archive=source_archive,
            dest_archive=final_archive,
            prepend_path=None,
            timestamp=get_source_date_epoch(CHART_DIR),
        )
        if result.returncode != 0:
            get_console().print(
                f"[error]Error repackaging package for Helm Chart from {source_archive} to {final_archive}[/]"
            )
            sys.exit(result.returncode)
        else:
            get_console().print(f"[success]Package created in {final_archive}[/]")
        if sign_email:
            get_console().print(f"[info]Signing the package with {sign_email}[/]")
            prov_file = final_archive.with_suffix(".tgz.prov")
            if prov_file.exists():
                get_console().print(f"[warning]Removing existing {prov_file}[/]")
                prov_file.unlink()
            result = run_command(
                cmd=["helm", "gpg", "sign", "-u", sign_email, archive_name],
                cwd=AIRFLOW_DIST_PATH.as_posix(),
                env=k8s_env,
                check=False,
            )
            if result.returncode != 0:
                get_console().print("[error]Error signing the chart[/]")
                sys.exit(result.returncode)
            result = run_command(
                cmd=["helm", "gpg", "verify", archive_name],
                cwd=AIRFLOW_DIST_PATH.as_posix(),
                env=k8s_env,
                check=False,
            )
            if result.returncode != 0:
                get_console().print("[error]Error signing the chart[/]")
                sys.exit(result.returncode)
            else:
                get_console().print(f"[success]Chart signed - the {prov_file} file created.[/]")
    finally:
        # Restore original Chart.yaml content if it was modified for RC version
        if is_rc_version and chart_yaml_backup:
            CHART_YAML_FILE.write_text(chart_yaml_backup)
            get_console().print("[info]Restored original Chart.yaml content after packaging.[/]")


def generate_issue_content(
    github_token: str,
    previous_release: str,
    current_release: str,
    excluded_pr_list: str,
    limit_pr_count: int | None,
    is_helm_chart: bool,
):
    from github import Github, Issue, PullRequest, UnknownObjectException

    PullRequestOrIssue = PullRequest.PullRequest | Issue.Issue
    verbose = get_verbose()

    previous = previous_release
    current = current_release

    changes = get_changes(verbose, previous, current, is_helm_chart)
    change_prs = [change.pr for change in changes]
    if excluded_pr_list:
        excluded_prs = [int(pr) for pr in excluded_pr_list.split(",")]
    else:
        excluded_prs = []
    prs = [pr for pr in change_prs if pr is not None and pr not in excluded_prs]

    g = Github(github_token)
    repo = g.get_repo("apache/airflow")
    pull_requests: dict[int, PullRequestOrIssue] = {}
    linked_issues: dict[int, list[Issue.Issue]] = defaultdict(lambda: [])
    users: dict[int, set[str]] = defaultdict(lambda: set())
    count_prs = limit_pr_count or len(prs)

    with Progress(console=get_console()) as progress:
        task = progress.add_task(f"Retrieving {count_prs} PRs ", total=count_prs)
        for pr_number in prs[:count_prs]:
            progress.console.print(
                f"Retrieving PR#{pr_number}: https://github.com/apache/airflow/pull/{pr_number}"
            )

            pr: PullRequestOrIssue
            try:
                pr = repo.get_pull(pr_number)
            except UnknownObjectException:
                # Fallback to issue if PR not found
                try:
                    pr = repo.get_issue(pr_number)  # (same fields as PR)
                except UnknownObjectException:
                    get_console().print(f"[red]The PR #{pr_number} could not be found[/]")
                    continue

            if pr.user.login == "dependabot[bot]":
                get_console().print(f"[yellow]Skipping PR #{pr_number} as it was created by dependabot[/]")
                continue
            # Ignore doc-only and skipped PRs
            label_names = [label.name for label in pr.labels]
            if not is_helm_chart and ("type:doc-only" in label_names or "changelog:skip" in label_names):
                continue

            pull_requests[pr_number] = pr

            # retrieve and append commit authors (to handle cherry picks)
            if hasattr(pr, "get_commits"):
                try:
                    commits = pr.get_commits()
                    for commit in commits:
                        author = commit.author
                        if author:
                            users[pr_number].add(author.login)
                            progress.console.print(f"Added commit author {author.login} for PR#{pr_number}")

                except Exception as e:
                    progress.console.print(
                        f"[warn]Could not retrieve commits for PR#{pr_number}: {e}, skipping[/]"
                    )

            # GitHub does not have linked issues in PR - but we quite rigorously add Fixes/Closes
            # Relate so we can find those from the body
            if pr.body:
                body = " ".join(pr.body.splitlines())
                body_without_code_blocks = remove_code_blocks(body)
                linked_issue_numbers = {
                    int(issue_match.group(1))
                    for issue_match in ISSUE_MATCH_IN_BODY.finditer(body_without_code_blocks)
                }
                for linked_issue_number in linked_issue_numbers:
                    progress.console.print(
                        f"Retrieving Linked issue PR#{linked_issue_number}: "
                        f"https://github.com/apache/airflow/issue/{linked_issue_number}"
                    )
                    try:
                        linked_issues[pr_number].append(repo.get_issue(linked_issue_number))
                    except UnknownObjectException:
                        progress.console.print(
                            f"Failed to retrieve linked issue #{linked_issue_number}: Unknown Issue"
                        )
            # do not add bot users to the list of users
            if not pr.user.login.endswith("[bot]"):
                users[pr_number].add(pr.user.login)
            for linked_issue in linked_issues[pr_number]:
                users[pr_number].add(linked_issue.user.login)
            progress.advance(task)

    print_issue_content(current, pull_requests, linked_issues, users, is_helm_chart)


@release_management_group.command(name="publish-docs-to-s3", help="Publishes docs to S3.")
@click.option(
    "--source-dir-path",
    help="Path to the directory with the generated documentation.",
    required=True,
)
@click.option(
    "--exclude-docs",
    help="Comma separated list of directories to exclude from the documentation.",
    default="",
)
@click.option(
    "--dry-run",
    is_flag=True,
    help="Dry run - only print what would be done.",
)
@click.option(
    "--overwrite",
    is_flag=True,
    help="Overwrite existing files in the S3 bucket.",
)
@click.option(
    "--destination-location",
    help="Name of the S3 bucket to publish the documentation to.",
    type=NotVerifiedBetterChoice(DESTINATION_LOCATIONS),
    required=True,
)
@click.option(
    "--publish-all-docs", is_flag=True, help="Publish all the available docs in the source directory."
)
@click.option(
    "--stable-versions",
    is_flag=True,
    help="Publish all the stable versions of the docs in the source directory.",
)
@click.option(
    "--skip-write-to-stable-folder",
    is_flag=True,
    help="Skip writing stable versions folder.",
)
@option_parallelism
def publish_docs_to_s3(
    source_dir_path: str,
    destination_location: str,
    exclude_docs: str,
    dry_run: bool,
    overwrite: bool,
    parallelism: int,
    publish_all_docs: bool,
    stable_versions: bool,
    skip_write_to_stable_folder: bool,
):
    from airflow_breeze.utils.publish_docs_to_s3 import S3DocsPublish

    if publish_all_docs and stable_versions:
        get_console().print("[error]You cannot use --publish-all and --stable-versions together[/]")
        sys.exit(1)

    destination_location = destination_location.rstrip("/")
    source_dir_path = source_dir_path.rstrip("/")

    get_console().print("[info]Your publishing docs to S3[/]")
    get_console().print(f"[info]Your source directory path is {source_dir_path}[/]")
    get_console().print(f"[info]Your destination path to docs is {destination_location}[/]")
    get_console().print(f"[info]Your excluded docs are {exclude_docs}[/]")

    docs_to_s3 = S3DocsPublish(
        source_dir_path=source_dir_path,
        exclude_docs=exclude_docs,
        dry_run=dry_run,
        overwrite=overwrite,
        destination_location=destination_location,
        parallelism=parallelism,
        skip_write_to_stable_folder=skip_write_to_stable_folder,
    )
    if publish_all_docs:
        docs_to_s3.publish_all_docs()
    if stable_versions:
        docs_to_s3.publish_stable_version_docs()
    from airflow_breeze.utils.publish_docs_to_s3 import VersionError

    if VersionError.has_any_error():
        get_console().print(
            "[error]There was an error with the version of the docs. "
            "Please check the version in the docs and try again.[/]"
        )
        sys.exit(1)


@release_management_group.command(
    name="constraints-version-check", help="Check constraints against released versions of packages."
)
@option_builder
@option_python
@option_airflow_constraints_mode_ci
@click.option(
    "--diff-mode",
    type=click.Choice(["full", "diff-all", "diff-constraints"], case_sensitive=False),
    default="full",
    show_default=True,
    help="Report mode: full, diff-all, diff-constraints.",
)
@click.option(
    "--package",
    multiple=True,
    help="Only check specific package(s). Can be used multiple times.",
)
@click.option(
    "--explain-why/--no-explain-why",
    default=False,
    help="Show explanations for outdated packages.",
)
@option_github_token
@option_github_repository
@option_verbose
@option_dry_run
def version_check(
    python: str,
    airflow_constraints_mode: str,
    diff_mode,
    package: tuple[str],
    explain_why: bool,
    github_token: str,
    github_repository: str,
    builder: str,
):
    perform_environment_checks()
    fix_ownership_using_docker()
    cleanup_python_generated_files()
    build_params = BuildCiParams(
        github_repository=github_repository,
        python=python,
        builder=builder,
    )
    rebuild_or_pull_ci_image_if_needed(command_params=build_params)
    if os.environ.get("CI", "false") == "true":
        # Show output outside the group in CI
        print("::endgroup::")
    selected_packages = set(package) if package else None
    constraints_version_check(
        python=python,
        airflow_constraints_mode=airflow_constraints_mode,
        diff_mode=diff_mode,
        selected_packages=selected_packages,
        explain_why=explain_why,
        github_token=github_token,
        github_repository=github_repository,
    )


@release_management_group.command(
    name="check-release-files",
    help="Verify that all expected packages are present in Apache Airflow svn.",
)
@click.option(
    "--path-to-airflow-svn",
    "-p",
    required=True,
    type=click.Path(exists=True, file_okay=False, dir_okay=True, resolve_path=True, path_type=Path),
    envvar="PATH_TO_AIRFLOW_SVN",
    help="Path to directory where release files are checked out from SVN (e.g., ~/code/asf-dist/dev/airflow)",
)
@click.option(
    "--version",
    type=str,
    help="Version of package to verify (e.g., 2.8.1rc2, 1.0.0rc1). "
    "Required for airflow, task-sdk, airflow-ctl, and python-client.",
)
@click.option(
    "--release-date",
    type=str,
    help="Date of the release in YYYY-MM-DD format. Required for providers.",
)
@click.option(
    "--packages-file",
    type=click.Path(exists=True, file_okay=True, dir_okay=False, resolve_path=True, path_type=Path),
    show_default=True,
    help="File containing list of packages to check (for providers only). use path to local packages.txt for providers releases.",
)
@click.argument(
    "release_type",
    type=BetterChoice(["airflow", "task-sdk", "airflow-ctl", "python-client", "providers"]),
    required=True,
)
@option_verbose
@option_dry_run
def check_release_files(
    path_to_airflow_svn: Path,
    version: str | None,
    release_date: str | None,
    packages_file: Path | None,
    release_type: str,
):
    """
    Verify that all expected packages are present in Apache Airflow svn.

    In case of providers, it will generate Dockerfile.pmc that you can use
    to verify that all packages are installable.

    In case of providers, you should update `packages.txt` file with list of packages
    that you expect to find (copy-paste the list from VOTE thread).

    """
    from airflow_breeze.utils.check_release_files import (
        AIRFLOW_CTL_DOCKER,
        AIRFLOW_DOCKER,
        PROVIDERS_DOCKER,
        PYTHON_CLIENT_DOCKER,
        TASK_SDK_DOCKER,
        check_airflow_ctl_release,
        check_airflow_release,
        check_providers,
        check_python_client_release,
        check_task_sdk_release,
        create_docker,
        get_packages,
        warn_of_missing_files,
    )

    console = get_console()

    # Validate required parameters based on release type
    if release_type == "providers":
        if not release_date:
            console.print("[error]--release-date is required for providers[/]")
            sys.exit(1)
        directory = path_to_airflow_svn / "providers" / release_date
    else:
        if not version:
            console.print(f"[error]--version is required for {release_type}[/]")
            sys.exit(1)

        # Determine directory based on release type
        if release_type == "airflow":
            directory = path_to_airflow_svn / version
        elif release_type == "task-sdk":
            directory = path_to_airflow_svn / "task-sdk" / version
        elif release_type == "airflow-ctl":
            directory = path_to_airflow_svn / "airflow-ctl" / version
        elif release_type == "python-client":
            directory = path_to_airflow_svn / "clients" / "python" / version
        else:
            console.print(f"[error]Unknown release type: {release_type}[/]")
            sys.exit(1)

    if not directory.exists():
        console.print(f"[error]Directory does not exist: {directory}[/]")
        sys.exit(1)

    files = os.listdir(directory)
    dockerfile_path = AIRFLOW_ROOT_PATH / "Dockerfile.pmc"

    # Check files based on release type
    missing_files = []

    if release_type == "providers":
        if not packages_file:
            console.print(f"[error]--packages-file is required for {release_type}[/]")
            sys.exit(1)
        packages = get_packages(packages_file)
        missing_files = check_providers(files, release_date, packages)
        pips = [f"{name}=={ver}" for name, ver in packages]
        create_docker(
            PROVIDERS_DOCKER.format("RUN uv pip install --pre --system " + " ".join(f"'{p}'" for p in pips)),
            dockerfile_path,
        )
    elif release_type == "airflow":
        missing_files = check_airflow_release(files, version)
        create_docker(AIRFLOW_DOCKER.format(version, version), dockerfile_path)
    elif release_type == "task-sdk":
        missing_files = check_task_sdk_release(files, version)
        if not version:
            get_console().print("[error]--version is required for task-sdk[/]")
            sys.exit(1)
        airflow_version = version.replace("1", "3", 1)
        create_docker(
            TASK_SDK_DOCKER.format(version, airflow_version, airflow_version, airflow_version),
            dockerfile_path,
        )
    elif release_type == "airflow-ctl":
        missing_files = check_airflow_ctl_release(files, version)
        create_docker(AIRFLOW_CTL_DOCKER.format(version), dockerfile_path)
    elif release_type == "python-client":
        missing_files = check_python_client_release(files, version)
        create_docker(PYTHON_CLIENT_DOCKER.format(version), dockerfile_path)

    if missing_files:
        warn_of_missing_files(missing_files, str(directory))
        sys.exit(1)
    else:
        console.print("\n[success]All expected files are present![/]")
        sys.exit(0)
