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

import os
import re
import sys
from pathlib import Path
from shutil import rmtree
from typing import NamedTuple

from in_container_utils import click, console, run_command

AIRFLOW_SOURCE_DIR = Path(__file__).resolve().parents[1]
DIST_FOLDER = Path("/dist")


def get_provider_name(package_name: str) -> str:
    return ".".join(package_name.split("-")[0].replace("apache_airflow_providers_", "").split("_"))


def get_airflow_version_from_package(package_name: str) -> str:
    return ".".join(Path(package_name).name.split("-")[1].split(".")[0:3])


def find_airflow_package(extension: str) -> str | None:
    packages = [f.as_posix() for f in DIST_FOLDER.glob(f"apache_airflow-[0-9]*.{extension}")]
    if len(packages) > 1:
        console.print(f"\n[red]Found multiple airflow packages: {packages}\n")
        sys.exit(1)
    elif len(packages) == 0:
        console.print("\n[red]No airflow package found\n")
        sys.exit(1)
    airflow_package = packages[0] if packages else None
    if airflow_package:
        console.print(f"\n[bright_blue]Found airflow package: {airflow_package}\n")
    else:
        console.print("\n[yellow]No airflow package found.\n")
    return airflow_package


def find_provider_packages(extension: str, selected_providers: list[str]) -> list[str]:
    candidates = list(DIST_FOLDER.glob(f"apache_airflow_providers_*.{extension}"))
    console.print("\n[bright_blue]Found the following provider packages: ")
    for candidate in sorted(candidates):
        console.print(f"  {candidate.as_posix()}")
    console.print()
    if selected_providers:
        candidates = [
            candidate for candidate in candidates if get_provider_name(candidate.name) in selected_providers
        ]
        console.print("[bright_blue]Selected provider packages:")
        for candidate in sorted(candidates):
            console.print(f"  {candidate.as_posix()}")
        console.print()
    return [candidate.as_posix() for candidate in candidates]


def calculate_constraints_location(
    constraints_mode: str,
    constraints_reference: str | None,
    github_repository: str,
    python_version: str,
    providers: bool,
):
    constraints_base = f"https://raw.githubusercontent.com/{github_repository}/{constraints_reference}"
    location = f"{constraints_base}/{constraints_mode}-{python_version}.txt"
    console.print(f"[info]Determined {'providers' if providers else 'airflow'} constraints as: {location}")
    return location


def get_airflow_constraints_location(
    airflow_constraints_mode: str,
    airflow_constraints_location: str | None,
    airflow_constraints_reference: str | None,
    default_constraints_branch: str,
    airflow_package_version: str | None,
    github_repository: str,
    python_version: str,
    airflow_skip_constraints: bool,
) -> str | None:
    """For Airflow we determine constraints in this order of preference:
    * AIRFLOW_SKIP_CONSTRAINTS=true - no constraints
    * AIRFLOW_CONSTRAINTS_LOCATION -  constraints from this location (url)
    * AIRFLOW_CONSTRAINTS_REFERENCE + constraints mode if specified
    * if we know airflow version "constraints-VERSION" + constraints mode
    * DEFAULT_CONSTRAINT_BRANCH + constraints mode - as fallback
    * constraints-main + constraints mode - as fallback
    """
    if airflow_skip_constraints:
        return None
    if airflow_constraints_location:
        console.print(f"[info]Using constraints from location: {airflow_constraints_location}")
        return airflow_constraints_location
    if airflow_constraints_reference:
        console.print(
            f"[info]Building constraints location from "
            f"constraints reference: {airflow_constraints_reference}"
        )
    elif airflow_package_version:
        if re.match(r"[0-9]+\.[0-9]+\.[0-9]+[0-9a-z.]*|main|v[0-9]_.*", airflow_package_version):
            airflow_constraints_reference = f"constraints-{airflow_package_version}"
            console.print(
                f"[info]Determined constraints reference from airflow package version "
                f"{airflow_package_version} as: {airflow_constraints_reference}"
            )
        else:
            airflow_constraints_reference = default_constraints_branch
            console.print(f"[info]Falling back tp: {default_constraints_branch}")
    return calculate_constraints_location(
        constraints_mode=airflow_constraints_mode,
        constraints_reference=airflow_constraints_reference,
        github_repository=github_repository,
        python_version=python_version,
        providers=False,
    )


def get_providers_constraints_location(
    default_constraints_branch: str,
    github_repository: str,
    providers_constraints_location: str | None,
    providers_constraints_mode: str,
    providers_constraints_reference: str | None,
    providers_skip_constraints: bool,
    python_version: str,
) -> str | None:
    """For providers we determine constraints in this order of preference:
    * PROVIDERS_SKIP_CONSTRAINTS=true - no constraints
    * PROVIDERS_CONSTRAINTS_LOCATION -  constraints from this location (url)
    * PROVIDERS_CONSTRAINTS_REFERENCE + constraints mode if specified
    * DEFAULT_CONSTRAINT_BRANCH + constraints mode
    * constraints-main + constraints mode - as fallback
    """
    if providers_skip_constraints:
        return None
    if providers_constraints_location:
        console.print(f"[info]Using constraints from location: {providers_constraints_location}")
        return providers_constraints_location
    if not providers_constraints_reference:
        providers_constraints_reference = default_constraints_branch
    return calculate_constraints_location(
        constraints_mode=providers_constraints_mode,
        constraints_reference=providers_constraints_reference,
        python_version=python_version,
        github_repository=github_repository,
        providers=True,
    )


class InstallationSpec(NamedTuple):
    airflow_package: str | None
    airflow_constraints_location: str | None
    provider_packages: list[str]
    provider_constraints_location: str | None


ALLOWED_VCS_PROTOCOLS = ("git+file://", "git+https://", "git+ssh://", "git+http://", "git+git://", "git://")


def find_installation_spec(
    airflow_constraints_mode: str,
    airflow_constraints_location: str | None,
    airflow_constraints_reference: str,
    airflow_extras: str,
    airflow_skip_constraints: bool,
    default_constraints_branch: str,
    github_repository: str,
    install_selected_providers: str,
    package_format: str,
    providers_constraints_mode: str,
    providers_constraints_location: str | None,
    providers_constraints_reference: str,
    providers_skip_constraints: bool,
    python_version: str,
    use_airflow_version: str,
    use_packages_from_dist: bool,
) -> InstallationSpec:
    if use_packages_from_dist:
        console.print("[bright_blue]Using packages from dist folder")
    else:
        console.print("[bright_blue]Not using packages from dist folder - only install from remote sources")
    if use_packages_from_dist and package_format not in ["wheel", "sdist"]:
        console.print(f"[red]PACKAGE_FORMAT must be one of 'wheel' or 'sdist' and not {package_format}")
        sys.exit(1)
    extension = "whl" if package_format == "wheel" else "tar.gz"
    if airflow_extras:
        console.print(f"[bright_blue]Using airflow extras: {airflow_extras}")
        airflow_extras = f"[{airflow_extras}]"
    else:
        console.print("[bright_blue]No airflow extras specified.")
    if use_airflow_version and (AIRFLOW_SOURCE_DIR / "airflow").exists():
        console.print(
            f"[red]The airflow source folder exists in {AIRFLOW_SOURCE_DIR}, but you are "
            f"removing it and installing airflow from {use_airflow_version}."
        )
        console.print("[red]This is not supported. Please use --mount-sources=remove flag in breeze.")
        sys.exit(1)
    if use_airflow_version in ["wheel", "sdist"] and use_packages_from_dist:
        airflow_package_spec = find_airflow_package(extension)
        if use_airflow_version != package_format:
            console.print(
                "[red]USE_AIRFLOW_VERSION must be the same as PACKAGE_FORMAT cannot have "
                "wheel and sdist values set at the same time"
            )
            sys.exit(1)
        if airflow_package_spec:
            airflow_version = get_airflow_version_from_package(airflow_package_spec)
            if airflow_version:
                airflow_constraints_location = get_airflow_constraints_location(
                    airflow_skip_constraints=airflow_skip_constraints,
                    airflow_constraints_mode=airflow_constraints_mode,
                    airflow_constraints_location=airflow_constraints_location,
                    airflow_constraints_reference=airflow_constraints_reference,
                    airflow_package_version=airflow_version,
                    default_constraints_branch=default_constraints_branch,
                    github_repository=github_repository,
                    python_version=python_version,
                )
            if airflow_extras:
                airflow_package_spec += airflow_extras

    elif use_airflow_version == "none" or use_airflow_version == "":
        console.print("\n[bright_blue]Skipping airflow package installation\n")
        airflow_package_spec = None
        airflow_constraints_location = None
    elif use_airflow_version.startswith(ALLOWED_VCS_PROTOCOLS):
        console.print(f"\nInstalling airflow from remote spec {use_airflow_version}\n")
        if airflow_extras:
            airflow_package_spec = f"apache-airflow{airflow_extras} @ {use_airflow_version}"
        else:
            airflow_package_spec = use_airflow_version
        airflow_constraints_location = get_airflow_constraints_location(
            airflow_skip_constraints=airflow_skip_constraints,
            airflow_constraints_mode=airflow_constraints_mode,
            airflow_constraints_location=airflow_constraints_location,
            airflow_constraints_reference=airflow_constraints_reference,
            airflow_package_version=use_airflow_version,
            default_constraints_branch=default_constraints_branch,
            github_repository=github_repository,
            python_version=python_version,
        )
    else:
        console.print(f"\nInstalling airflow via apache-airflow=={use_airflow_version}")
        airflow_package_spec = f"apache-airflow{airflow_extras}=={use_airflow_version}"
        airflow_constraints_location = get_airflow_constraints_location(
            airflow_skip_constraints=airflow_skip_constraints,
            airflow_constraints_mode=airflow_constraints_mode,
            airflow_constraints_location=airflow_constraints_location,
            airflow_constraints_reference=airflow_constraints_reference,
            airflow_package_version=use_airflow_version,
            default_constraints_branch=default_constraints_branch,
            github_repository=github_repository,
            python_version=python_version,
        )
    provider_package_list = []
    if use_packages_from_dist:
        selected_providers_list = install_selected_providers.split(",") if install_selected_providers else []
        if selected_providers_list:
            console.print(f"\n[bright_blue]Selected providers: {selected_providers_list}\n")
        else:
            console.print("\n[bright_blue]No preselected providers\n")
        provider_package_list = find_provider_packages(extension, selected_providers_list)
    return InstallationSpec(
        airflow_package=airflow_package_spec,
        airflow_constraints_location=airflow_constraints_location,
        provider_packages=provider_package_list,
        provider_constraints_location=get_providers_constraints_location(
            providers_constraints_mode=providers_constraints_mode,
            providers_constraints_location=providers_constraints_location,
            providers_constraints_reference=providers_constraints_reference,
            providers_skip_constraints=providers_skip_constraints,
            default_constraints_branch=default_constraints_branch,
            github_repository=github_repository,
            python_version=python_version,
        ),
    )


ALLOWED_PACKAGE_FORMAT = ["wheel", "sdist", "both"]
ALLOWED_CONSTRAINTS_MODE = ["constraints-source-providers", "constraints", "constraints-no-providers"]
ALLOWED_MOUNT_SOURCES = ["remove", "tests", "providers-and-tests"]


@click.command()
@click.option(
    "--airflow-constraints-location",
    default="",
    show_default=True,
    envvar="AIRFLOW_CONSTRAINTS_LOCATION",
    help="Airflow constraints location (full constraints URL)",
)
@click.option(
    "--airflow-constraints-mode",
    show_default=True,
    type=click.Choice(ALLOWED_CONSTRAINTS_MODE),
    default=ALLOWED_CONSTRAINTS_MODE[0],
    show_envvar=True,
    envvar="AIRFLOW_CONSTRAINTS_MODE",
    help="Airflow constraints mode.",
)
@click.option(
    "--airflow-constraints-reference",
    show_default=True,
    envvar="AIRFLOW_CONSTRAINTS_REFERENCE",
    help="Airflow constraints reference constraints reference: constraints-(BRANCH or TAG)",
)
@click.option(
    "--airflow-extras",
    default="",
    show_default=True,
    envvar="AIRFLOW_EXTRAS",
    help="Airflow extras to install",
)
@click.option(
    "--airflow-skip-constraints",
    is_flag=True,
    default=False,
    show_default=True,
    envvar="AIRFLOW_SKIP_CONSTRAINTS",
    help="Skip constraints for airflow installation if set.",
)
@click.option(
    "--default-constraints-branch",
    required=True,
    envvar="DEFAULT_CONSTRAINTS_BRANCH",
    help="Default constraints branch to use",
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
    "--github-repository",
    default="apache/airflow",
    show_default=True,
    envvar="GITHUB_REPOSITORY",
    help="Github repository to use",
)
@click.option(
    "--install-selected-providers",
    default="",
    show_default=True,
    envvar="INSTALL_SELECTED_PROVIDERS",
    help="Comma separated list of providers to install",
)
@click.option(
    "--package-format",
    default=ALLOWED_PACKAGE_FORMAT[0],
    envvar="PACKAGE_FORMAT",
    show_default=True,
    type=click.Choice(ALLOWED_PACKAGE_FORMAT),
    help="Package format to use",
)
@click.option(
    "--providers-constraints-location",
    default="",
    show_default=True,
    envvar="PROVIDERS_CONSTRAINTS_LOCATION",
    help="Providers constraints location (full URL)",
)
@click.option(
    "--providers-constraints-mode",
    show_default=True,
    type=click.Choice(ALLOWED_CONSTRAINTS_MODE),
    default=ALLOWED_CONSTRAINTS_MODE[0],
    show_envvar=True,
    envvar="AIRFLOW_CONSTRAINTS_MODE",
    help="Providers constraints mode.",
)
@click.option(
    "--providers-constraints-reference",
    envvar="PROVIDERS_CONSTRAINTS_REFERENCE",
    help="Providers constraints reference: constraints-(BRANCH or TAG)",
)
@click.option(
    "--providers-skip-constraints",
    is_flag=True,
    default=False,
    show_default=True,
    envvar="PROVIDERS_SKIP_CONSTRAINTS",
    help="Skip constraints for providers installation if set.",
)
@click.option(
    "--python-version",
    default="3.9",
    envvar="PYTHON_MAJOR_MINOR_VERSION",
    show_default=True,
    help="Python version to use",
)
@click.option(
    "--use-airflow-version",
    default="",
    envvar="USE_AIRFLOW_VERSION",
    show_default=True,
    help="Airflow version to install (version from PyPI, wheel or sdist or VCS URL)",
)
@click.option(
    "--use-packages-from-dist",
    is_flag=True,
    default=False,
    show_default=True,
    envvar="USE_PACKAGES_FROM_DIST",
    help="Should install packages from dist folder if set.",
)
@click.option(
    "--mount-sources",
    type=click.Choice(ALLOWED_MOUNT_SOURCES),
    required=True,
    envvar="MOUNT_SOURCES",
    help="What sources are mounted .",
)
@click.option(
    "--install-airflow-with-constraints",
    is_flag=True,
    default=False,
    show_default=True,
    envvar="INSTALL_AIRFLOW_WITH_CONSTRAINTS",
    help="Install airflow in a separate step, with constraints determined from package or airflow version.",
)
def install_airflow_and_providers(
    airflow_constraints_mode: str,
    airflow_constraints_location: str,
    airflow_constraints_reference: str,
    airflow_extras: str,
    airflow_skip_constraints: bool,
    default_constraints_branch: str,
    github_actions: bool,
    github_repository: str,
    install_selected_providers: str,
    mount_sources: str,
    package_format: str,
    providers_constraints_mode: str,
    providers_constraints_location: str,
    providers_constraints_reference: str,
    providers_skip_constraints: bool,
    python_version: str,
    use_airflow_version: str,
    use_packages_from_dist: bool,
    install_airflow_with_constraints: bool,
):
    console.print("[bright_blue]Installing Airflow and Providers")
    installation_spec = find_installation_spec(
        airflow_constraints_mode=airflow_constraints_mode,
        airflow_constraints_location=airflow_constraints_location,
        airflow_constraints_reference=airflow_constraints_reference,
        airflow_extras=airflow_extras,
        airflow_skip_constraints=airflow_skip_constraints,
        default_constraints_branch=default_constraints_branch,
        github_repository=github_repository,
        install_selected_providers=install_selected_providers,
        package_format=package_format,
        providers_constraints_mode=providers_constraints_mode,
        providers_constraints_location=providers_constraints_location,
        providers_constraints_reference=providers_constraints_reference,
        providers_skip_constraints=providers_skip_constraints,
        python_version=python_version,
        use_airflow_version=use_airflow_version,
        use_packages_from_dist=use_packages_from_dist,
    )
    if installation_spec.airflow_package and install_airflow_with_constraints:
        base_install_airflow_cmd = [
            "/usr/local/bin/uv",
            "pip",
            "install",
            "--no-sources",
            "--python",
            "/usr/local/bin/python",
            installation_spec.airflow_package,
        ]
        install_airflow_cmd = base_install_airflow_cmd.copy()
        console.print(f"\n[bright_blue]Installing airflow package: {installation_spec.airflow_package}")
        if installation_spec.airflow_constraints_location:
            console.print(f"[bright_blue]Use constraints: {installation_spec.airflow_constraints_location}")
            install_airflow_cmd.extend(["--constraint", installation_spec.airflow_constraints_location])
        console.print()
        result = run_command(install_airflow_cmd, github_actions=github_actions, check=False)
        if result.returncode != 0:
            console.print(
                "[warning]Installation with constraints failed - might be because pre-installed provider"
                " has conflicting dependencies in PyPI. Falling back to a non-constraint installation."
            )
            run_command(base_install_airflow_cmd, github_actions=github_actions, check=True)
    if installation_spec.provider_packages or not install_airflow_with_constraints:
        base_install_providers_cmd = [
            "/usr/local/bin/uv",
            "pip",
            "install",
            "--no-sources",
            "--python",
            "/usr/local/bin/python",
        ]
        if not install_airflow_with_constraints and installation_spec.airflow_package:
            base_install_providers_cmd.append(installation_spec.airflow_package)
        console.print("\n[bright_blue]Installing provider packages:")
        for provider_package in sorted(installation_spec.provider_packages):
            console.print(f"  {provider_package}")
        console.print()
        for provider_package in installation_spec.provider_packages:
            base_install_providers_cmd.append(provider_package)
        install_providers_command = base_install_providers_cmd.copy()
        # if airflow is also being installed we should add airflow to the base_install_providers_cmd
        # to avoid accidentally upgrading airflow to a version that is different than installed in the
        # previous step
        if installation_spec.airflow_package:
            base_install_providers_cmd.append(installation_spec.airflow_package)

        if installation_spec.provider_constraints_location:
            console.print(
                f"[bright_blue]with constraints: {installation_spec.provider_constraints_location}\n"
            )
            install_providers_command.extend(
                ["--constraint", installation_spec.provider_constraints_location]
            )
            console.print()
            result = run_command(install_providers_command, github_actions=github_actions, check=False)
            if result.returncode != 0:
                console.print(
                    "[warning]Installation with constraints failed - might be because pre-installed provider"
                    " has conflicting dependencies in PyPI. Falling back to a non-constraint installation."
                )
                run_command(base_install_providers_cmd, github_actions=github_actions, check=True)
        else:
            run_command(base_install_providers_cmd, github_actions=github_actions, check=True)
    if mount_sources == "providers-and-tests":
        console.print("[bright_blue]Removing installed providers")
        run_command(
            ["pip freeze | grep apache-airflow-providers | xargs pip uninstall -y "],
            github_actions=github_actions,
            shell=True,
            check=False,
        )
        import importlib.util

        spec = importlib.util.find_spec("airflow")
        if spec is None or spec.origin is None:
            console.print("[red]Airflow not found - cannot mount sources")
            sys.exit(1)
        airflow_path = Path(spec.origin).parent
        rmtree(airflow_path / "providers", ignore_errors=True)
        os.symlink("/opt/airflow/airflow/providers", (airflow_path / "providers").as_posix())
    console.print("\n[green]Done!")


if __name__ == "__main__":
    install_airflow_and_providers()
