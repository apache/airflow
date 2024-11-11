#!/usr/bin/env python3
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

import json
import os
import sys
from dataclasses import dataclass
from datetime import datetime
from functools import cached_property
from pathlib import Path
from typing import TextIO

import requests
from click import Choice
from in_container_utils import click, console, run_command

AIRFLOW_SOURCE_DIR = Path(__file__).resolve().parents[2]

DEFAULT_BRANCH = os.environ.get("DEFAULT_BRANCH", "main")
PYTHON_VERSION = os.environ.get("PYTHON_MAJOR_MINOR_VERSION", "3.9")
GENERATED_PROVIDER_DEPENDENCIES_FILE = AIRFLOW_SOURCE_DIR / "generated" / "provider_dependencies.json"

ALL_PROVIDER_DEPENDENCIES = json.loads(GENERATED_PROVIDER_DEPENDENCIES_FILE.read_text())

now = datetime.now().isoformat()

NO_PROVIDERS_CONSTRAINTS_PREFIX = f"""
#
# This constraints file was automatically generated on {now}
# via "eager-upgrade" mechanism of PIP. For the "{DEFAULT_BRANCH}" branch of Airflow.
# This variant of constraints install just the 'bare' 'apache-airflow' package build from the HEAD of
# the branch, without installing any of the providers.
#
# Those constraints represent the "newest" dependencies airflow could use, if providers did not limit
# Airflow in any way.
#
"""

SOURCE_PROVIDERS_CONSTRAINTS_PREFIX = f"""
#
# This constraints file was automatically generated on {now}
# via "eager-upgrade" mechanism of PIP. For the "{DEFAULT_BRANCH}" branch of Airflow.
# This variant of constraints install uses the HEAD of the branch version of both
# 'apache-airflow' package and all available community provider packages.
#
# Those constraints represent the dependencies that are used by all pull requests when they are build in CI.
# They represent "latest" and greatest set of constraints that HEAD of the "apache-airflow" package should
# Install with "HEAD" of providers. Those are the only constraints that are used by our CI builds.
#
"""

PYPI_PROVIDERS_CONSTRAINTS_PREFIX = f"""
#
# This constraints file was automatically generated on {now}
# via "eager-upgrade" mechanism of PIP. For the "{DEFAULT_BRANCH}" branch of Airflow.
# This variant of constraints install uses the HEAD of the branch version for 'apache-airflow' but installs
# the providers from PIP-released packages at the moment of the constraint generation.
#
# Those constraints are actually those that regular users use to install released version of Airflow.
# We also use those constraints after "apache-airflow" is released and the constraints are tagged with
# "constraints-X.Y.Z" tag to build the production image for that version.
#
# This constraints file is meant to be used only in the "apache-airflow" installation command and not
# in all subsequent pip commands. By using a constraints.txt file, we ensure that solely the Airflow
# installation step is reproducible. Subsequent pip commands may install packages that would have
# been incompatible with the constraints used in Airflow reproducible installation step. Finally, pip
# commands that might change the installed version of apache-airflow should include "apache-airflow==X.Y.Z"
# in the list of install targets to prevent Airflow accidental upgrade or downgrade.
#
# Typical installation process of airflow for Python 3.9 is (with random selection of extras and custom
# dependencies added), usually consists of two steps:
#
# 1. Reproducible installation of airflow with selected providers (note constraints are used):
#
# pip install "apache-airflow[celery,cncf.kubernetes,google,amazon,snowflake]==X.Y.Z" \\
#     --constraint \\
#    "https://raw.githubusercontent.com/apache/airflow/constraints-X.Y.Z/constraints-{PYTHON_VERSION}.txt"
#
# 2. Installing own dependencies that are potentially not matching the constraints (note constraints are not
#    used, and apache-airflow==X.Y.Z is used to make sure there is no accidental airflow upgrade/downgrade.
#
# pip install "apache-airflow==X.Y.Z" "snowflake-connector-python[pandas]=N.M.O"
#
"""


@dataclass
class ConfigParams:
    airflow_constraints_mode: str
    chicken_egg_providers: str
    constraints_github_repository: str
    default_constraints_branch: str
    github_actions: bool
    eager_upgrade_additional_requirements: str
    python: str
    use_uv: bool

    @cached_property
    def constraints_dir(self) -> Path:
        constraints_dir = Path("/files") / f"constraints-{self.python}"
        constraints_dir.mkdir(parents=True, exist_ok=True)
        return constraints_dir

    @cached_property
    def latest_constraints_file(self) -> Path:
        return self.constraints_dir / f"original-{self.airflow_constraints_mode}-{self.python}.txt"

    @cached_property
    def constraints_diff_file(self) -> Path:
        return self.constraints_dir / f"diff-{self.airflow_constraints_mode}-{self.python}.md"

    @cached_property
    def current_constraints_file(self) -> Path:
        return self.constraints_dir / f"{self.airflow_constraints_mode}-{self.python}.txt"

    @cached_property
    def get_freeze_command(self) -> list[str]:
        # Some day we might use uv instead of pip
        # if self.use_uv:
        #     return ["uv", "pip", "freeze", "--python", sys.executable]
        # else:
        #     return ["pip", "freeze"]
        return ["pip", "freeze"]

    @cached_property
    def get_install_command(self) -> list[str]:
        if self.use_uv:
            return ["uv", "pip", "install", "--python", sys.executable]
        else:
            return ["pip", "install"]

    @cached_property
    def get_uninstall_command(self) -> list[str]:
        if self.use_uv:
            return ["uv", "pip", "uninstall", "--python", sys.executable]
        else:
            return ["pip", "uninstall"]

    @cached_property
    def get_install_args(self) -> list[str]:
        if self.use_uv:
            return []
        else:
            return ["--root-user-action", "ignore"]

    @cached_property
    def get_uninstall_args(self) -> list[str]:
        if self.use_uv:
            return []
        else:
            return ["--root-user-action", "ignore", "--yes"]

    @cached_property
    def get_resolution_highest_args(self) -> list[str]:
        if self.use_uv:
            return ["--resolution", "highest"]
        else:
            return ["--upgrade", "--upgrade-strategy", "eager"]

    @cached_property
    def eager_upgrade_additional_requirements_list(self) -> list[str]:
        if self.eager_upgrade_additional_requirements:
            return self.eager_upgrade_additional_requirements.split(" ")
        return []


def install_local_airflow_with_eager_upgrade(config_params: ConfigParams) -> None:
    run_command(
        [
            *config_params.get_install_command,
            "-e",
            ".[all-core]",
            *config_params.eager_upgrade_additional_requirements_list,
            *config_params.get_resolution_highest_args,
        ],
        github_actions=config_params.github_actions,
        cwd=AIRFLOW_SOURCE_DIR,
        check=True,
    )


def freeze_packages_to_file(config_params: ConfigParams, file: TextIO) -> None:
    console.print(f"[bright_blue]Freezing constraints to file: {file.name}")
    result = run_command(
        cmd=config_params.get_freeze_command,
        github_actions=config_params.github_actions,
        text=True,
        check=True,
        capture_output=True,
    )
    count_lines = 0
    for line in sorted(result.stdout.split("\n")):
        if line.startswith(("apache_airflow", "apache-airflow==", "/opt/airflow", "#", "-e")):
            continue
        if "@" in line:
            continue
        if "from file://" in line:
            continue
        if line.strip() == "":
            continue
        count_lines += 1
        file.write(line)
        file.write("\n")
    file.flush()
    console.print(f"[green]Constraints generated to file: {file.name}. Wrote {count_lines} lines")


def download_latest_constraint_file(config_params: ConfigParams):
    constraints_url = (
        "https://raw.githubusercontent.com/"
        f"{config_params.constraints_github_repository}/{config_params.default_constraints_branch}/"
        f"{config_params.airflow_constraints_mode}-{config_params.python}.txt"
    )
    # download the latest constraints file
    # download using requests
    console.print(f"[bright_blue]Downloading constraints file from {constraints_url}")
    r = requests.get(constraints_url, timeout=60)
    r.raise_for_status()
    with config_params.latest_constraints_file.open("w") as constraints_file:
        constraints_file.write(r.text)
    console.print(f"[green]Downloaded constraints file from {constraints_url} to {constraints_file.name}")


def diff_constraints(config_params: ConfigParams) -> None:
    """
    Diffs constraints files and prints the diff to the console.
    """
    console.print("[bright_blue]Diffing constraints files")
    result = run_command(
        [
            "diff",
            "--ignore-matching-lines=#",
            "--color=always",
            config_params.latest_constraints_file.as_posix(),
            config_params.current_constraints_file.as_posix(),
        ],
        # always shows output directly in CI without folded group
        github_actions=False,
        check=False,
    )
    if result.returncode == 0:
        console.print("[green]No changes in constraints files. exiting")
        config_params.constraints_diff_file.unlink(missing_ok=True)
        return
    result = run_command(
        [
            "diff",
            "--ignore-matching-lines=#",
            "--color=never",
            config_params.latest_constraints_file.as_posix(),
            config_params.current_constraints_file.as_posix(),
        ],
        github_actions=config_params.github_actions,
        check=False,
        text=True,
        capture_output=True,
    )
    with config_params.constraints_diff_file.open("w") as diff_file:
        diff_file.write(
            f"Dependencies {config_params.airflow_constraints_mode} updated "
            f"for Python {config_params.python}\n\n"
        )
        diff_file.write("```diff\n")
        diff_file.write(result.stdout)
        diff_file.write("```\n")
    console.print(f"[green]Diff generated to file: {config_params.constraints_diff_file}")


def uninstall_all_packages(config_params: ConfigParams):
    console.print("[bright_blue]Uninstall All PIP packages")
    result = run_command(
        cmd=config_params.get_freeze_command,
        github_actions=config_params.github_actions,
        cwd=AIRFLOW_SOURCE_DIR,
        text=True,
        check=True,
        capture_output=True,
    )
    # do not remove installer!
    installer = "uv==" if config_params.use_uv else "pip=="
    all_installed_packages = [
        dep.split("==")[0]
        for dep in result.stdout.strip().split("\n")
        if not dep.startswith(("apache-airflow", "apache-airflow==", "/opt/airflow", "#", "-e", installer))
    ]
    run_command(
        cmd=[*config_params.get_uninstall_command, *all_installed_packages],
        github_actions=config_params.github_actions,
        cwd=AIRFLOW_SOURCE_DIR,
        text=True,
        check=True,
    )


def get_all_active_provider_packages(python_version: str | None = None) -> list[str]:
    return [
        f"apache-airflow-providers-{provider.replace('.','-')}"
        for provider in ALL_PROVIDER_DEPENDENCIES.keys()
        if ALL_PROVIDER_DEPENDENCIES[provider]["state"] == "ready"
        and (
            python_version is None
            or python_version not in ALL_PROVIDER_DEPENDENCIES[provider]["excluded-python-versions"]
        )
    ]


def generate_constraints_source_providers(config_params: ConfigParams) -> None:
    """
    Generates constraints with provider dependencies used from current sources. This might be different
    from the constraints generated from the latest released version of the providers in PyPI. Those
    constraints are used in CI builds when we install providers built using current sources and in
    Breeze CI image builds.
    """
    with config_params.current_constraints_file.open("w") as constraints_file:
        constraints_file.write(SOURCE_PROVIDERS_CONSTRAINTS_PREFIX)
        freeze_packages_to_file(config_params, constraints_file)
    download_latest_constraint_file(config_params)
    diff_constraints(config_params)


def generate_constraints_pypi_providers(config_params: ConfigParams) -> None:
    """
    Generates constraints with provider installed from PyPI. This is the default constraints file
    used in production/release builds when we install providers from PyPI and when tagged, those
    providers are used by our users to install Airflow in reproducible way.
    :return:
    """
    dist_dir = Path("/dist")
    all_provider_packages = get_all_active_provider_packages(python_version=config_params.python)
    chicken_egg_prefixes = []
    packages_to_install = []
    console.print("[bright_blue]Installing Airflow with PyPI providers with eager upgrade")
    if config_params.chicken_egg_providers:
        for chicken_egg_provider in config_params.chicken_egg_providers.split(" "):
            chicken_egg_prefixes.append(f"apache-airflow-providers-{chicken_egg_provider.replace('.','-')}")
        console.print(
            f"[bright_blue]Checking if {chicken_egg_prefixes} are available in local dist folder "
            f"as chicken egg providers)"
        )
    for provider_package in all_provider_packages:
        if config_params.chicken_egg_providers and provider_package.startswith(tuple(chicken_egg_prefixes)):
            glob_pattern = f"{provider_package.replace('-','_')}-*.whl"
            console.print(
                f"[bright_blue]Checking if {provider_package} is available in local dist folder "
                f"with {glob_pattern} pattern"
            )
            files = dist_dir.glob(glob_pattern)
            for file in files:
                console.print(
                    f"[yellow]Installing {file.name} from local dist folder as it is "
                    f"a chicken egg provider"
                )
                packages_to_install.append(f"{provider_package} @ file://{file.as_posix()}")
                break
            else:
                console.print(
                    f"[yellow]Skipping {provider_package} as it is not found in dist folder to install."
                )
            # Skip checking if chicken egg provider is available in PyPI - it does not have to be there
            continue
        console.print(f"[bright_blue]Checking if {provider_package} is available in PyPI: ... ", end="")
        r = requests.head(f"https://pypi.org/pypi/{provider_package}/json", timeout=60)
        if r.status_code == 200:
            console.print("[green]OK")
            packages_to_install.append(provider_package)
        else:
            console.print("[yellow]NOK. Skipping.")
    run_command(
        cmd=[
            *config_params.get_install_command,
            ".[all-core]",
            *packages_to_install,
            *config_params.eager_upgrade_additional_requirements_list,
            *config_params.get_resolution_highest_args,
        ],
        github_actions=config_params.github_actions,
        check=True,
    )
    console.print("[success]Installed airflow with PyPI providers with eager upgrade.")
    with config_params.current_constraints_file.open("w") as constraints_file:
        constraints_file.write(PYPI_PROVIDERS_CONSTRAINTS_PREFIX)
        freeze_packages_to_file(config_params, constraints_file)
    download_latest_constraint_file(config_params)
    diff_constraints(config_params)


def generate_constraints_no_providers(config_params: ConfigParams) -> None:
    """
    Generates constraints without any provider dependencies. This is used mostly to generate SBOM
    files - where we generate list of dependencies for Airflow without any provider installed.
    """
    uninstall_all_packages(config_params)
    console.print(
        "[bright_blue]Installing airflow with `all-core` extras only with eager upgrade in "
        "installable mode."
    )
    install_local_airflow_with_eager_upgrade(config_params)
    console.print("[success]Installed airflow with [all-core] extras only with eager upgrade.")
    with config_params.current_constraints_file.open("w") as constraints_file:
        constraints_file.write(NO_PROVIDERS_CONSTRAINTS_PREFIX)
        freeze_packages_to_file(config_params, constraints_file)
    download_latest_constraint_file(config_params)
    diff_constraints(config_params)


ALLOWED_CONSTRAINTS_MODES = ["constraints", "constraints-source-providers", "constraints-no-providers"]


@click.command()
@click.option(
    "--airflow-constraints-mode",
    type=Choice(ALLOWED_CONSTRAINTS_MODES),
    required=True,
    envvar="AIRFLOW_CONSTRAINTS_MODE",
    help="Mode of constraints to generate",
)
@click.option(
    "--chicken-egg-providers",
    envvar="CHICKEN_EGG_PROVIDERS",
    help="Providers that should be installed from packages built from current sources.",
)
@click.option(
    "--constraints-github-repository",
    default="apache/airflow",
    show_default=True,
    envvar="CONSTRAINTS_GITHUB_REPOSITORY",
    help="GitHub repository to get constraints from",
)
@click.option(
    "--default-constraints-branch",
    required=True,
    envvar="DEFAULT_CONSTRAINTS_BRANCH",
    help="Branch to get constraints from",
)
@click.option(
    "--eager-upgrade-additional-requirements",
    envvar="EAGER_UPGRADE_ADDITIONAL_REQUIREMENTS",
    default="",
    help="Additional requirements to add to eager upgrade",
)
@click.option(
    "--github-actions",
    is_flag=True,
    default=False,
    show_default=True,
    envvar="GITHUB_ACTIONS",
    help="Running in GitHub Actions",
)
@click.option(
    "--python",
    required=True,
    envvar="PYTHON_MAJOR_MINOR_VERSION",
    help="Python major.minor version",
)
@click.option(
    "--use-uv/--no-use-uv",
    is_flag=True,
    default=True,
    help="Use uv instead of pip as packaging tool.",
    envvar="USE_UV",
)
def generate_constraints(
    airflow_constraints_mode: str,
    chicken_egg_providers: str,
    constraints_github_repository: str,
    default_constraints_branch: str,
    eager_upgrade_additional_requirements: str,
    github_actions: bool,
    python: str,
    use_uv: bool,
) -> None:
    config_params = ConfigParams(
        airflow_constraints_mode=airflow_constraints_mode,
        chicken_egg_providers=chicken_egg_providers,
        constraints_github_repository=constraints_github_repository,
        default_constraints_branch=default_constraints_branch,
        eager_upgrade_additional_requirements=eager_upgrade_additional_requirements,
        github_actions=github_actions,
        python=python,
        use_uv=use_uv,
    )
    if airflow_constraints_mode == "constraints-source-providers":
        generate_constraints_source_providers(config_params)
    elif airflow_constraints_mode == "constraints":
        generate_constraints_pypi_providers(config_params)
    elif airflow_constraints_mode == "constraints-no-providers":
        generate_constraints_no_providers(config_params)
    else:
        console.print(f"[red]Unknown constraints mode: {airflow_constraints_mode}")
        sys.exit(1)
    console.print("[green]Generated constraints:")
    files = config_params.constraints_dir.rglob("*.txt")
    for file in files:
        console.print(file.as_posix())
    console.print()


if __name__ == "__main__":
    generate_constraints()
