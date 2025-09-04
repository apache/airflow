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
from typing import NamedTuple

sys.path.insert(0, str(Path(__file__).parent.resolve()))
from in_container_utils import AIRFLOW_CORE_SOURCES_PATH, AIRFLOW_DIST_PATH, click, console, run_command


def get_provider_name(package_name: str) -> str:
    return ".".join(package_name.split("-")[0].replace("apache_airflow_providers_", "").split("_"))


def get_airflow_version_from_package(package_name: str) -> str:
    return ".".join(Path(package_name).name.split("-")[1].split(".")[0:3])


def find_airflow_package(extension: str) -> tuple[str, str | None]:
    airflow_packages = [f.as_posix() for f in AIRFLOW_DIST_PATH.glob(f"apache_airflow-[0-9]*.{extension}")]
    if len(airflow_packages) > 1:
        console.print(f"\n[red]Found multiple airflow packages: {airflow_packages}\n")
        sys.exit(1)
    elif len(airflow_packages) == 0:
        console.print("\n[red]No airflow package found\n")
        sys.exit(1)
    airflow_package = airflow_packages[0]
    console.print(f"\n[bright_blue]Found airflow package: {airflow_package}\n")
    airflow_core_packages = [
        f.as_posix() for f in AIRFLOW_DIST_PATH.glob(f"apache_airflow_core-[0-9]*.{extension}")
    ]
    if len(airflow_core_packages) > 1:
        console.print(f"\n[red]Found multiple airflow core packages: {airflow_packages}\n")
        sys.exit(1)
    airflow_core_package = airflow_core_packages[0] if airflow_core_packages else None
    if airflow_core_package:
        console.print(f"\n[bright_blue]Found airflow core package: {airflow_package}\n")
    else:
        console.print("\n[yellow]No airflow core package found.\n")
    return airflow_package, airflow_core_package


def find_airflow_task_sdk_package(extension: str) -> str | None:
    packages = [f.as_posix() for f in AIRFLOW_DIST_PATH.glob(f"apache_airflow_task_sdk-[0-9]*.{extension}")]
    if len(packages) > 1:
        console.print(f"\n[red]Found multiple airflow task sdk packages: {packages}\n")
        sys.exit(1)
    airflow_task_sdk_package = packages[0] if packages else None
    if airflow_task_sdk_package:
        console.print(f"\n[bright_blue]Found airflow task_sdk package: {airflow_task_sdk_package}\n")
    else:
        console.print("\n[yellow]No airflow task_sdk package found.\n")
    return airflow_task_sdk_package


def find_airflow_ctl_sdk_package(extension: str) -> str | None:
    packages = [f.as_posix() for f in AIRFLOW_DIST_PATH.glob(f"apache_airflow_ctl-[0-9]*.{extension}")]
    if len(packages) > 1:
        console.print(f"\n[red]Found multiple airflow ctl packages: {packages}\n")
        sys.exit(1)
    airflow_ctl_package = packages[0] if packages else None
    if airflow_ctl_package:
        console.print(f"\n[bright_blue]Found airflow ctl package: {airflow_ctl_package}\n")
    else:
        console.print("\n[yellow]No airflow ctl package found.\n")
    return airflow_ctl_package


def find_provider_distributions(extension: str, selected_providers: list[str]) -> list[str]:
    candidates = list(AIRFLOW_DIST_PATH.glob(f"apache_airflow_providers_*.{extension}"))
    console.print("\n[bright_blue]Found the following provider distributions: ")
    for candidate in sorted(candidates):
        console.print(f"  {candidate.as_posix()}")
    console.print()
    if selected_providers:
        candidates = [
            candidate for candidate in candidates if get_provider_name(candidate.name) in selected_providers
        ]
        console.print("[bright_blue]Selected provider distributions:")
        for candidate in sorted(candidates):
            console.print(f"  {candidate.as_posix()}")
        console.print()
    result = []
    for candidate in candidates:
        # https://github.com/apache/airflow/pull/49339
        path_str = candidate.as_posix()
        if "apache_airflow_providers_common_sql" in path_str:
            console.print(f"[bright_blue]Adding [polars] extra to common.sql provider: {path_str}")
            path_str += "[polars]"
        result.append(path_str)
    return result


def calculate_constraints_location(
    constraints_mode: str,
    constraints_reference: str | None,
    github_repository: str,
    python_version: str,
    providers: bool,
):
    constraints_base = f"https://raw.githubusercontent.com/{github_repository}/{constraints_reference}"
    location = f"{constraints_base}/{constraints_mode}-{python_version}.txt"
    console.print(
        f"[bright_blue]Determined {'providers' if providers else 'airflow'} constraints as: {location}"
    )
    return location


def get_airflow_constraints_location(
    airflow_constraints_mode: str,
    airflow_constraints_location: str | None,
    airflow_constraints_reference: str | None,
    default_constraints_branch: str,
    airflow_package_version: str | None,
    github_repository: str,
    python_version: str,
    install_airflow_with_constraints: bool,
) -> str | None:
    """For Airflow we determine constraints in this order of preference:
    * INSTALL_AIRFLOW_WITH_CONSTRAINTS=false - no constraints
    * AIRFLOW_CONSTRAINTS_LOCATION -  constraints from this location (url)
    * AIRFLOW_CONSTRAINTS_REFERENCE + constraints mode if specified
    * if we know airflow version "constraints-VERSION" + constraints mode
    * DEFAULT_CONSTRAINT_BRANCH + constraints mode - as fallback
    * constraints-main + constraints mode - as fallback
    """
    console.print("[bright_blue]Determining airflow constraints location")
    if not install_airflow_with_constraints:
        console.print("[bright_blue]Skipping airflow constraints.")
        return None
    if airflow_constraints_location:
        console.print(f"[bright_blue]Using constraints from location: {airflow_constraints_location}")
        return airflow_constraints_location
    if airflow_constraints_reference:
        console.print(
            f"[bright_blue]Building constraints location from constraints reference: {airflow_constraints_reference}"
        )
    elif airflow_package_version:
        if re.match(r"[0-9]+\.[0-9]+\.[0-9]+[0-9a-z.]*|main|v[0-9]_.*", airflow_package_version):
            airflow_constraints_reference = f"constraints-{airflow_package_version}"
            console.print(
                f"[bright_blue]Determined constraints reference from airflow package version "
                f"{airflow_package_version} as: {airflow_constraints_reference}"
            )
        else:
            airflow_constraints_reference = default_constraints_branch
            console.print(f"[bright_blue]Falling back to constraints branch: {default_constraints_branch}")
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
        console.print("[bright_blue]Skipping providers constraints.")
        return None
    if providers_constraints_location:
        console.print(f"[bright_blue]Using constraints from location: {providers_constraints_location}")
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
    airflow_distribution: str | None
    airflow_core_distribution: str | None
    airflow_constraints_location: str | None
    airflow_task_sdk_distribution: str | None
    airflow_ctl_distribution: str | None
    airflow_ctl_constraints_location: str | None
    provider_distributions: list[str]
    provider_constraints_location: str | None
    pre_release: bool = os.environ.get("ALLOW_PRE_RELEASES", "false").lower() == "true"


GITHUB_REPO_BRANCH_PATTERN = r"^([^/]+)/([^/:]+):([^:]+)$"


def find_installation_spec(
    airflow_constraints_mode: str,
    airflow_constraints_location: str | None,
    airflow_constraints_reference: str,
    airflow_extras: str,
    install_airflow_with_constraints: bool,
    default_constraints_branch: str,
    github_repository: str,
    install_selected_providers: str,
    distribution_format: str,
    providers_constraints_mode: str,
    providers_constraints_location: str | None,
    providers_constraints_reference: str,
    providers_skip_constraints: bool,
    python_version: str,
    use_airflow_version: str,
    use_distributions_from_dist: bool,
) -> InstallationSpec:
    console.print("[bright_blue]Finding installation specification")
    if use_distributions_from_dist:
        console.print("[bright_blue]Using distributions from dist folder")
    else:
        console.print(
            "[bright_blue]Not using distributions from dist folder - only install from remote sources"
        )
    if use_distributions_from_dist and distribution_format not in ["wheel", "sdist"]:
        console.print(
            f"[red]DISTRIBUTION_FORMAT must be one of 'wheel' or 'sdist' and not {distribution_format}"
        )
        sys.exit(1)
    extension = "whl" if distribution_format == "wheel" else "tar.gz"
    pre_release = os.environ.get("ALLOW_PRE_RELEASES", "false").lower() == "true"
    console.print("[bright_blue]Pre-release: ", pre_release)
    if airflow_extras:
        console.print(f"[bright_blue]Using airflow extras: {airflow_extras}")
        airflow_extras = f"[{airflow_extras}]"
    else:
        console.print("[bright_blue]No airflow extras specified.")
    if use_airflow_version and (AIRFLOW_CORE_SOURCES_PATH / "airflow" / "__main__.py").exists():
        console.print(
            f"[red]The airflow source folder exists in {AIRFLOW_CORE_SOURCES_PATH}, but you are "
            f"removing it and installing airflow from {use_airflow_version}."
        )
        console.print("[red]This is not supported. Please use --mount-sources=remove flag in breeze.")
        sys.exit(1)
    if use_airflow_version in ["wheel", "sdist"] and use_distributions_from_dist:
        console.print("[bright_blue]Finding specification from local distribution files in dist folder")
        airflow_distribution_spec, airflow_core_distribution_spec = find_airflow_package(extension)
        if use_airflow_version != distribution_format:
            console.print(
                "[red]USE_AIRFLOW_VERSION must be the same as DISTRIBUTION_FORMAT cannot have "
                "wheel and sdist values set at the same time"
            )
            sys.exit(1)
        if airflow_distribution_spec:
            airflow_version = get_airflow_version_from_package(airflow_distribution_spec)
            if airflow_version:
                console.print(f"[bright_blue]Using airflow version retrieved from package: {airflow_version}")
                airflow_constraints_location = get_airflow_constraints_location(
                    install_airflow_with_constraints=install_airflow_with_constraints,
                    airflow_constraints_mode=airflow_constraints_mode,
                    airflow_constraints_location=airflow_constraints_location,
                    airflow_constraints_reference=airflow_constraints_reference,
                    airflow_package_version=airflow_version,
                    default_constraints_branch=default_constraints_branch,
                    github_repository=github_repository,
                    python_version=python_version,
                )
            if airflow_extras:
                airflow_distribution_spec += airflow_extras
        # We always install latest task-sdk - it's independent from Airflow
        airflow_task_sdk_spec = find_airflow_task_sdk_package(extension)
        airflow_task_sdk_distribution = airflow_task_sdk_spec

        # We always install latest ctl - it's independent of Airflow
        airflow_ctl_spec = find_airflow_ctl_sdk_package(extension=extension)
        if airflow_ctl_spec:
            airflow_ctl_constraints_location = get_airflow_constraints_location(
                install_airflow_with_constraints=install_airflow_with_constraints,
                airflow_constraints_mode=airflow_constraints_mode,
                airflow_constraints_location=airflow_constraints_location,
                airflow_constraints_reference=airflow_constraints_reference,
                airflow_package_version="main",
                default_constraints_branch=default_constraints_branch,
                github_repository=github_repository,
                python_version=python_version,
            )
        else:
            airflow_ctl_constraints_location = None
        airflow_ctl_distribution = airflow_ctl_spec
    elif use_airflow_version == "none" or use_airflow_version == "":
        console.print("\n[bright_blue]Skipping airflow package installation\n")
        airflow_distribution_spec = None
        airflow_core_distribution_spec = None
        airflow_constraints_location = None
        airflow_task_sdk_distribution = None
        airflow_ctl_distribution = None
        airflow_ctl_constraints_location = None
    elif repo_match := re.match(GITHUB_REPO_BRANCH_PATTERN, use_airflow_version):
        owner, repo, branch = repo_match.groups()
        console.print(f"\nInstalling airflow from GitHub: {use_airflow_version}\n")
        vcs_url = f"git+https://github.com/{owner}/{repo}.git@{branch}"
        if airflow_extras:
            airflow_distribution_spec = f"apache-airflow{airflow_extras} @ {vcs_url}"
            airflow_core_distribution_spec = f"apache-airflow-core @ {vcs_url}#subdirectory=airflow-core"
        else:
            airflow_distribution_spec = f"apache-airflow @ {vcs_url}"
            airflow_core_distribution_spec = f"apache-airflow-core @ {vcs_url}#subdirectory=airflow-core"
        airflow_constraints_location = get_airflow_constraints_location(
            install_airflow_with_constraints=install_airflow_with_constraints,
            airflow_constraints_mode=airflow_constraints_mode,
            airflow_constraints_location=airflow_constraints_location,
            airflow_constraints_reference=airflow_constraints_reference,
            airflow_package_version=use_airflow_version,
            default_constraints_branch=default_constraints_branch,
            github_repository=github_repository,
            python_version=python_version,
        )
        console.print(f"\nInstalling airflow task-sdk from GitHub {use_airflow_version}\n")
        airflow_task_sdk_distribution = f"apache-airflow-task-sdk @ {vcs_url}#subdirectory=task-sdk"
        airflow_constraints_location = get_airflow_constraints_location(
            install_airflow_with_constraints=install_airflow_with_constraints,
            airflow_constraints_mode=airflow_constraints_mode,
            airflow_constraints_location=airflow_constraints_location,
            airflow_constraints_reference=airflow_constraints_reference,
            airflow_package_version=use_airflow_version,
            default_constraints_branch=default_constraints_branch,
            github_repository=github_repository,
            python_version=python_version,
        )
        console.print(f"\nInstalling airflow ctl from remote spec {use_airflow_version}\n")
        airflow_ctl_distribution = f"apache-airflow-ctl @ {vcs_url}#subdirectory=airflow-ctl"
        airflow_ctl_constraints_location = get_airflow_constraints_location(
            install_airflow_with_constraints=install_airflow_with_constraints,
            airflow_constraints_mode=airflow_constraints_mode,
            airflow_constraints_location=airflow_constraints_location,
            airflow_constraints_reference=airflow_constraints_reference,
            airflow_package_version=use_airflow_version,
            default_constraints_branch=default_constraints_branch,
            github_repository=github_repository,
            python_version=python_version,
        )
        console.print(
            "[yellow]Note that installing airflow from branch has no assets compiled, so you will"
            "not be able to run UI (we might add asset compilation for this case later if needed)."
        )
    elif use_airflow_version in ["wheel", "sdist"] and not use_distributions_from_dist:
        console.print(
            "[red]USE_AIRFLOW_VERSION cannot be 'wheel' or 'sdist' without --use-distributions-from-dist"
        )
        sys.exit(1)
    else:
        console.print(f"\nInstalling airflow via apache-airflow=={use_airflow_version}")
        airflow_distribution_spec = f"apache-airflow{airflow_extras}=={use_airflow_version}"
        airflow_core_distribution_spec = (
            f"apache-airflow-core=={use_airflow_version}" if not use_airflow_version.startswith("2") else None
        )
        airflow_constraints_location = get_airflow_constraints_location(
            install_airflow_with_constraints=install_airflow_with_constraints,
            airflow_constraints_mode=airflow_constraints_mode,
            airflow_constraints_location=airflow_constraints_location,
            airflow_constraints_reference=airflow_constraints_reference,
            airflow_package_version=use_airflow_version,
            default_constraints_branch=default_constraints_branch,
            github_repository=github_repository,
            python_version=python_version,
        )
        console.print(
            "\nDo not install airflow task-sdk. It should be installed automatically if needed by providers."
        )
        airflow_task_sdk_distribution = None

        console.print(
            "\nDo not install airflow ctl. It should be installed automatically if needed by providers."
        )
        airflow_ctl_distribution = None
        airflow_ctl_constraints_location = None
        try:
            from packaging.version import Version

            if Version(use_airflow_version).is_prerelease:
                console.print(
                    f"[yellow]Forcing pre-release installation for pre-release "
                    f"airflow version {use_airflow_version}"
                )
                pre_release = True
        except Exception:
            # In case parsing version fails here we just skip setting pre-release
            pass
    provider_distributions_list = []
    if use_distributions_from_dist:
        selected_providers_list = install_selected_providers.split(",") if install_selected_providers else []
        if selected_providers_list:
            console.print(f"\n[bright_blue]Selected providers: {selected_providers_list}\n")
        else:
            console.print("\n[bright_blue]No preselected providers\n")
        provider_distributions_list = find_provider_distributions(extension, selected_providers_list)
    installation_spec = InstallationSpec(
        airflow_distribution=airflow_distribution_spec,
        airflow_core_distribution=airflow_core_distribution_spec,
        airflow_constraints_location=airflow_constraints_location,
        airflow_task_sdk_distribution=airflow_task_sdk_distribution,
        airflow_ctl_distribution=airflow_ctl_distribution,
        airflow_ctl_constraints_location=airflow_ctl_constraints_location,
        provider_distributions=provider_distributions_list,
        provider_constraints_location=get_providers_constraints_location(
            providers_constraints_mode=providers_constraints_mode,
            providers_constraints_location=providers_constraints_location,
            providers_constraints_reference=providers_constraints_reference,
            providers_skip_constraints=providers_skip_constraints,
            default_constraints_branch=default_constraints_branch,
            github_repository=github_repository,
            python_version=python_version,
        ),
        pre_release=pre_release,
    )
    console.print("[bright_blue]Installation specification:[/]", installation_spec)
    return installation_spec


ALLOWED_DISTRIBUTION_FORMAT = ["wheel", "sdist", "both"]
ALLOWED_CONSTRAINTS_MODE = ["constraints-source-providers", "constraints", "constraints-no-providers"]
ALLOWED_MOUNT_SOURCES = ["remove", "tests", "providers-and-tests", "selected"]

INIT_CONTENT = '__path__ = __import__("pkgutil").extend_path(__path__, __name__) # type: ignore'
FUTURE_CONTENT = "from __future__ import annotations"


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
    help="GitHub repository to use",
)
@click.option(
    "--install-selected-providers",
    default="",
    show_default=True,
    envvar="INSTALL_SELECTED_PROVIDERS",
    help="Comma separated list of providers to install",
)
@click.option(
    "--distribution-format",
    default=ALLOWED_DISTRIBUTION_FORMAT[0],
    envvar="DISTRIBUTION_FORMAT",
    show_default=True,
    type=click.Choice(ALLOWED_DISTRIBUTION_FORMAT),
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
    default="3.10",
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
    "--use-distributions-from-dist",
    is_flag=True,
    default=False,
    show_default=True,
    envvar="USE_DISTRIBUTIONS_FROM_DIST",
    help="Should install distributions from dist folder if set.",
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
    default_constraints_branch: str,
    github_actions: bool,
    github_repository: str,
    install_selected_providers: str,
    mount_sources: str,
    distribution_format: str,
    providers_constraints_mode: str,
    providers_constraints_location: str,
    providers_constraints_reference: str,
    providers_skip_constraints: bool,
    python_version: str,
    use_airflow_version: str,
    use_distributions_from_dist: bool,
    install_airflow_with_constraints: bool,
):
    if mount_sources in ["tests", "remove"]:
        console.print("[bright_blue]Uninstall editable packages installed in CI image")
        command = [
            "uv pip freeze | grep -v '@ file://' | grep '-e file' | sed s'/-e //' | xargs -r uv pip uninstall"
        ]
        run_command(
            command,
            github_actions=github_actions,
            shell=True,
            check=False,
        )
    console.print("[bright_blue]Installing Airflow and Providers")
    installation_spec = find_installation_spec(
        airflow_constraints_mode=airflow_constraints_mode,
        airflow_constraints_location=airflow_constraints_location,
        airflow_constraints_reference=airflow_constraints_reference,
        airflow_extras=airflow_extras,
        install_airflow_with_constraints=install_airflow_with_constraints,
        default_constraints_branch=default_constraints_branch,
        github_repository=github_repository,
        install_selected_providers=install_selected_providers,
        distribution_format=distribution_format,
        providers_constraints_mode=providers_constraints_mode,
        providers_constraints_location=providers_constraints_location,
        providers_constraints_reference=providers_constraints_reference,
        providers_skip_constraints=providers_skip_constraints,
        python_version=python_version,
        use_airflow_version=use_airflow_version,
        use_distributions_from_dist=use_distributions_from_dist,
    )
    if install_airflow_with_constraints:
        if installation_spec.airflow_distribution:
            _install_only_airflow_airflow_core_task_sdk_with_constraints(installation_spec, github_actions)
        if installation_spec.airflow_ctl_distribution:
            _install_airflow_ctl_with_constraints(installation_spec, github_actions)
    if installation_spec.provider_distributions or not install_airflow_with_constraints:
        _install_airflow_and_optionally_providers_together(installation_spec, github_actions)
    if mount_sources == "providers-and-tests":
        if (
            use_airflow_version
            and use_airflow_version.startswith("2")
            or use_airflow_version in ["wheel", "sdist"]
        ):
            command = [
                "uv pip freeze | grep apache-airflow-providers | grep -v '#' | "
                "grep -v apache-airflow-providers-fab | xargs uv pip uninstall"
            ]
            console.print("[bright_blue]Removing installed providers except FAB")
        else:
            command = ["uv pip freeze | grep apache-airflow-providers | grep -v '#' | xargs pip uninstall"]
            console.print("[bright_blue]Removing installed providers")
        run_command(
            command,
            github_actions=github_actions,
            shell=True,
            check=False,
        )
        import importlib.util

        spec = importlib.util.find_spec("airflow")
        if spec is None or spec.origin is None:
            console.print("[red]Airflow not found - cannot mount sources")
            sys.exit(1)
        from packaging.version import Version

        from airflow import __version__

        version = Version(__version__)
        if version.major == 2:
            console.print(
                "[yellow]Patching airflow 2 installation "
                "in order to load providers from separate distributions.\n"
            )
            airflow_path = Path(spec.origin).parent
            # Make sure old Airflow will include providers including common subfolder allow to extend loading
            # providers from the installed separate source packages
            console.print("[yellow]Uninstalling Airflow-3 only providers\n")
            providers_to_uninstall_for_airflow_2 = [
                "apache-airflow-providers-keycloak",
                "apache-airflow-providers-common-messaging",
                "apache-airflow-providers-git",
            ]
            if version.minor < 10:
                providers_to_uninstall_for_airflow_2.append("apache-airflow-providers-edge3")
            run_command(
                ["uv", "pip", "uninstall", *providers_to_uninstall_for_airflow_2],
                github_actions=github_actions,
                check=False,
            )
            console.print("[yellow]Uninstalling Airflow-3 only providers")
            console.print(
                "[yellow]Patching airflow 2 __init__.py -> replacing `from future`"
                "with legacy namespace packages.\n"
            )
            airflow_init_path = airflow_path / "__init__.py"
            airflow_init_content = airflow_init_path.read_text()
            airflow_init_path.write_text(airflow_init_content.replace(FUTURE_CONTENT, INIT_CONTENT))
            console.print("[yellow]Creating legacy `providers.common` and `providers` namespace packages.\n")
            airflow_providers_init_py = airflow_path / "providers" / "__init__.py"
            airflow_providers_common_init_py = airflow_path / "providers" / "common" / "__init__.py"
            airflow_providers_init_py.parent.mkdir(exist_ok=True)
            airflow_providers_init_py.write_text(INIT_CONTENT + "\n")
            airflow_providers_common_init_py.parent.mkdir(exist_ok=True)
            airflow_providers_common_init_py.write_text(INIT_CONTENT + "\n")

    console.print("\n[green]Done!")


def _install_airflow_and_optionally_providers_together(
    installation_spec: InstallationSpec, github_actions: bool
):
    console.print("[bright_blue]Installing airflow and optionally providers together")
    base_install_cmd = [
        "uv",
        "pip",
        "install",
    ]
    if installation_spec.pre_release:
        base_install_cmd.append("--pre")
    if installation_spec.airflow_distribution:
        console.print(
            f"\n[bright_blue]Adding airflow distribution to installation: {installation_spec.airflow_distribution} "
        )
        base_install_cmd.append(installation_spec.airflow_distribution)
    if installation_spec.airflow_core_distribution:
        console.print(
            f"\n[bright_blue]Adding airflow core distribution to installation: {installation_spec.airflow_core_distribution}"
        )
        base_install_cmd.append(installation_spec.airflow_core_distribution)
    if installation_spec.airflow_task_sdk_distribution:
        console.print(
            f"\n[bright_blue]Adding task-sdk distribution to installation: {installation_spec.airflow_task_sdk_distribution}"
        )
        base_install_cmd.append(installation_spec.airflow_task_sdk_distribution)
    if installation_spec.airflow_ctl_distribution:
        console.print(
            f"\n[bright_blue]Adding airflow ctl distribution to installation: {installation_spec.airflow_ctl_distribution}"
        )
        base_install_cmd.append(installation_spec.airflow_ctl_distribution)
    console.print("\n[bright_blue]Adding provider distributions to installation:")
    for provider_package in sorted(installation_spec.provider_distributions):
        console.print(f"  {provider_package}")
    console.print()
    for provider_package in installation_spec.provider_distributions:
        base_install_cmd.append(provider_package)
    install_providers_command = base_install_cmd.copy()
    if installation_spec.provider_constraints_location:
        console.print(
            f"[bright_blue]Installing with provider constraints: {installation_spec.provider_constraints_location}\n"
        )
        install_providers_command.extend(["--constraint", installation_spec.provider_constraints_location])
        console.print()
        result = run_command(install_providers_command, github_actions=github_actions, check=False)
        if result.returncode != 0:
            console.print(
                "[warning]Installation with constraints failed - might be because pre-installed provider"
                " has conflicting dependencies in PyPI. Falling back to a non-constraint installation."
            )
            run_command(base_install_cmd, github_actions=github_actions, check=True)
    else:
        run_command(base_install_cmd, github_actions=github_actions, check=True)


def _install_airflow_ctl_with_constraints(installation_spec: InstallationSpec, github_actions: bool):
    console.print(
        f"\n[bright_blue]Installing airflow ctl distribution: "
        f"{installation_spec.airflow_ctl_distribution} with constraints"
    )
    base_install_airflow_ctl_cmd = [
        "uv",
        "pip",
        "install",
    ]
    # if airflow is also being installed we should add airflow to the base_install_providers_cmd
    # to avoid accidentally upgrading airflow to a version that is different from installed in the
    # previous step
    if installation_spec.airflow_distribution:
        base_install_airflow_ctl_cmd.append(installation_spec.airflow_distribution)
    install_airflow_ctl_cmd = base_install_airflow_ctl_cmd.copy()
    if installation_spec.airflow_ctl_constraints_location:
        console.print(f"[bright_blue]Use constraints: {installation_spec.airflow_ctl_constraints_location}")
        install_airflow_ctl_cmd.extend(["--constraint", installation_spec.airflow_ctl_constraints_location])
    console.print()
    result = run_command(install_airflow_ctl_cmd, github_actions=github_actions, check=True)
    if result.returncode != 0:
        console.print(
            "[warning]Installation with constraints failed - might be because there are"
            " conflicting dependencies in PyPI. Falling back to a non-constraint installation."
        )
        run_command(base_install_airflow_ctl_cmd, github_actions=github_actions, check=True)


def _install_only_airflow_airflow_core_task_sdk_with_constraints(
    installation_spec: InstallationSpec, github_actions: bool
) -> None:
    console.print("[bright_blue]Installing airflow core, task-sdk with constraints")
    console.print(
        "[bright_blue]Airflow constraints location: ", installation_spec.airflow_constraints_location
    )
    console.print("[bright_blue]Airflow distribution", installation_spec.airflow_distribution)
    console.print("[bright_blue]Airflow core distribution", installation_spec.airflow_core_distribution)
    console.print(
        "[bright_blue]Airflow task-sdk distribution", installation_spec.airflow_task_sdk_distribution
    )
    console.print(
        "[bright_blue]Airflow ctl constraints location: ",
        installation_spec.airflow_ctl_constraints_location,
    )
    base_install_airflow_cmd = [
        "uv",
        "pip",
        "install",
    ]
    if installation_spec.pre_release:
        console.print("[bright_blue]Allowing pre-release versions of airflow")
        base_install_airflow_cmd.append("--pre")
    if installation_spec.airflow_distribution:
        console.print(
            f"\n[bright_blue]Installing airflow distribution: {installation_spec.airflow_distribution} with constraints"
        )
        base_install_airflow_cmd.append(installation_spec.airflow_distribution)
    if installation_spec.airflow_core_distribution:
        console.print(
            f"\n[bright_blue]Installing airflow core distribution: {installation_spec.airflow_core_distribution} with constraints"
        )
        base_install_airflow_cmd.append(installation_spec.airflow_core_distribution)
    if installation_spec.airflow_task_sdk_distribution:
        base_install_airflow_cmd.append(installation_spec.airflow_task_sdk_distribution)
        console.print(
            f"\n[bright_blue]Installing airflow task-sdk distribution: "
            f"{installation_spec.airflow_task_sdk_distribution} with constraints"
        )
        console.print()
    install_airflow_cmd = base_install_airflow_cmd.copy()
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


if __name__ == "__main__":
    install_airflow_and_providers()
