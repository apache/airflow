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
import re
import shutil
import subprocess
import sys
from collections.abc import Generator
from functools import cache, partial
from multiprocessing import Pool
from pathlib import Path
from threading import Lock
from typing import NamedTuple

from airflow_breeze.global_constants import (
    ALL_HISTORICAL_PYTHON_VERSIONS,
    ALL_PYPROJECT_TOML_FILES,
    PYTHON_TO_MIN_AIRFLOW_MAPPING,
    UPDATE_PROVIDER_DEPENDENCIES_SCRIPT,
)
from airflow_breeze.utils.ci_group import ci_group
from airflow_breeze.utils.console import get_console
from airflow_breeze.utils.github import download_constraints_file, get_active_airflow_versions, get_tag_date
from airflow_breeze.utils.packages import get_provider_distributions_metadata
from airflow_breeze.utils.path_utils import (
    AIRFLOW_PYPROJECT_TOML_FILE_PATH,
    AIRFLOW_ROOT_PATH,
    CONSTRAINTS_CACHE_PATH,
    PROVIDER_DEPENDENCIES_JSON_HASH_PATH,
    PROVIDER_DEPENDENCIES_JSON_PATH,
)
from airflow_breeze.utils.shared_options import get_verbose

_regenerate_provider_deps_lock = Lock()


def get_all_provider_pyproject_toml_provider_yaml_files() -> Generator[Path, None, None]:
    pyproject_toml_content = AIRFLOW_PYPROJECT_TOML_FILE_PATH.read_text().splitlines()
    in_workspace = False
    for line in pyproject_toml_content:
        trimmed_line = line.strip()
        if not in_workspace and trimmed_line.startswith("[tool.uv.workspace]"):
            in_workspace = True
        elif in_workspace:
            if trimmed_line.startswith("#"):
                continue
            if trimmed_line.startswith('"'):
                path = trimmed_line.split('"')[1]
                ALL_PYPROJECT_TOML_FILES.append(AIRFLOW_ROOT_PATH / path / "pyproject.toml")
                if trimmed_line.startswith('"providers/'):
                    yield AIRFLOW_ROOT_PATH / path / "pyproject.toml"
                    yield AIRFLOW_ROOT_PATH / path / "provider.yaml"
            elif trimmed_line.startswith("]"):
                break


@cache  # Note: using functools.cache to avoid multiple dumps in the same run
def regenerate_provider_dependencies_once() -> None:
    """Run provider dependencies regeneration once per interpreter execution.

    This function is safe to call multiple times from different modules; the
    underlying command will only run once. If the underlying command fails the
    CalledProcessError is propagated to the caller.
    """
    with _regenerate_provider_deps_lock:
        # Run the regeneration command from the repository root to ensure correct
        # relative paths if the script expects to be run from AIRFLOW_ROOT_PATH.
        subprocess.check_call(
            ["uv", "run", UPDATE_PROVIDER_DEPENDENCIES_SCRIPT.as_posix()], cwd=AIRFLOW_ROOT_PATH
        )


def _calculate_provider_deps_hash():
    import hashlib

    hasher = hashlib.sha256()
    for file in sorted(get_all_provider_pyproject_toml_provider_yaml_files()):
        hasher.update(file.read_bytes())
    return hasher.hexdigest()


@cache
def get_provider_dependencies() -> dict:
    if not PROVIDER_DEPENDENCIES_JSON_PATH.exists():
        calculated_hash = _calculate_provider_deps_hash()
        PROVIDER_DEPENDENCIES_JSON_HASH_PATH.write_text(calculated_hash)
        # We use regular print there as rich console might not be initialized yet here
        print("Regenerating provider dependencies file")
        regenerate_provider_dependencies_once()
    return json.loads(PROVIDER_DEPENDENCIES_JSON_PATH.read_text())


def generate_provider_dependencies_if_needed():
    if not PROVIDER_DEPENDENCIES_JSON_PATH.exists() or not PROVIDER_DEPENDENCIES_JSON_HASH_PATH.exists():
        get_provider_dependencies.cache_clear()
        get_provider_dependencies()
    else:
        calculated_hash = _calculate_provider_deps_hash()
        if calculated_hash.strip() != PROVIDER_DEPENDENCIES_JSON_HASH_PATH.read_text().strip():
            # Force re-generation
            PROVIDER_DEPENDENCIES_JSON_PATH.unlink(missing_ok=True)
            get_provider_dependencies.cache_clear()
            get_provider_dependencies()


def get_related_providers(
    provider_to_check: str,
    upstream_dependencies: bool,
    downstream_dependencies: bool,
) -> set[str]:
    """
    Gets cross dependencies of a provider.

    :param provider_to_check: id of the provider to check
    :param upstream_dependencies: whether to include providers that depend on it
    :param downstream_dependencies: whether to include providers it depends on
    :return: set of dependent provider ids
    """
    if not upstream_dependencies and not downstream_dependencies:
        raise ValueError("At least one of upstream_dependencies or downstream_dependencies must be True")
    related_providers = set()
    if upstream_dependencies:
        # Providers that use this provider
        for provider, provider_info in get_provider_dependencies().items():
            if provider_to_check in provider_info["cross-providers-deps"]:
                related_providers.add(provider)
    # and providers we use directly
    if downstream_dependencies:
        for dep_name in get_provider_dependencies()[provider_to_check]["cross-providers-deps"]:
            related_providers.add(dep_name)
    return related_providers


def is_airflow_version_supported_for_python(airflow_version: str, python_version: str) -> bool:
    from packaging.version import Version

    min_airflow_version = PYTHON_TO_MIN_AIRFLOW_MAPPING.get(python_version)
    if not min_airflow_version:
        return False
    return Version(airflow_version) >= Version(min_airflow_version)


def get_all_constraint_files_and_airflow_releases(
    refresh_constraints_and_airflow_releases: bool,
    airflow_constraints_mode: str,
    github_token: str | None,
) -> tuple[list[str], dict[str, str]]:
    all_airflow_versions_path = CONSTRAINTS_CACHE_PATH / "all_airflow_versions.json"
    airflow_release_dates_path = CONSTRAINTS_CACHE_PATH / "airflow_release_dates.json"
    if not all_airflow_versions_path.exists() or not airflow_release_dates_path.exists():
        get_console().print(
            "\n[warning]Airflow version cache does not exist. "
            "Forcing refreshing constraints and airflow versions.[/]\n"
        )
        refresh_constraints_and_airflow_releases = True
    if refresh_constraints_and_airflow_releases:
        shutil.rmtree(CONSTRAINTS_CACHE_PATH, ignore_errors=True)
    if not CONSTRAINTS_CACHE_PATH.exists():
        if not github_token:
            get_console().print(
                "[error]You need to provide GITHUB_TOKEN to generate providers metadata.[/]\n\n"
                "You can generate it with this URL: "
                "Please set it to a valid GitHub token with public_repo scope. You can create one by clicking "
                "the URL:\n\n"
                "https://github.com/settings/tokens/new?scopes=public_repo&description=airflow-refresh-constraints\n\n"
                "Once you have the token you can prepend prek command with GITHUB_TOKEN='<your token>' or"
                "set it in your environment with export GITHUB_TOKEN='<your token>'\n\n"
            )
            sys.exit(1)
        all_python_versions = ALL_HISTORICAL_PYTHON_VERSIONS
        CONSTRAINTS_CACHE_PATH.mkdir(parents=True, exist_ok=True)
        all_airflow_versions, airflow_release_dates = get_active_airflow_versions(confirm=False)
        all_airflow_versions_path.write_text(json.dumps(all_airflow_versions, indent=2))
        get_console().print(f"[info]All Airflow versions saved in: {all_airflow_versions_path}[/]")
        airflow_release_dates_path.write_text(json.dumps(airflow_release_dates, indent=2))
        get_console().print(f"[info]Airflow release dates saved in: {airflow_release_dates_path}[/]")
        with ci_group("Downloading constraints for all Airflow versions for all historical Python versions"):
            with Pool() as pool:
                # We use partial to pass the common parameters to the function
                get_constraints_for_python_version_partial = partial(
                    get_constraints_for_python_version,
                    airflow_constraints_mode=airflow_constraints_mode,
                    all_airflow_versions=all_airflow_versions,
                    github_token=github_token,
                )
                pool.map(get_constraints_for_python_version_partial, all_python_versions)
    else:
        get_console().print("[info]Retrieving airflow versions and using constraint files from cache.[/]")
        all_airflow_versions = json.loads(all_airflow_versions_path.read_text())
        airflow_release_dates = json.loads(airflow_release_dates_path.read_text())
    return all_airflow_versions, airflow_release_dates


def get_constraints_for_python_version(
    python_version: str, airflow_constraints_mode: str, all_airflow_versions: list[str], github_token: str
):
    for airflow_version in all_airflow_versions:
        if not download_constraints_file(
            constraints_reference=f"constraints-{airflow_version}",
            python_version=python_version,
            github_token=github_token,
            airflow_constraints_mode=airflow_constraints_mode,
            output_file=CONSTRAINTS_CACHE_PATH / f"constraints-{airflow_version}-python-{python_version}.txt",
        ):
            get_console().print(
                "[info]Could not download constraints for "
                f"Airflow {airflow_version} and Python {python_version}[/]"
            )


MATCH_CONSTRAINTS_FILE_REGEX = re.compile(r"constraints-(.*)-python-(.*).txt")


class PackageInfo(NamedTuple):
    package_name: str
    version: str


class ConstraintsForPython(NamedTuple):
    python_version: str
    packages: dict[str, PackageInfo]


class AirflowVersionConstraints(NamedTuple):
    airflow_version: str
    constraints_files: list[ConstraintsForPython]


def load_constraints() -> dict[str, AirflowVersionConstraints]:
    get_console().print("[info]Loading constraints for all Airflow versions[/]")
    all_constraints: dict[str, AirflowVersionConstraints] = {}
    for filename in sorted(CONSTRAINTS_CACHE_PATH.glob("constraints-*-python-*.txt")):
        filename_match = MATCH_CONSTRAINTS_FILE_REGEX.match(filename.name)
        if filename_match:
            airflow_version = filename_match.group(1)
            python_version = filename_match.group(2)
            if airflow_version not in all_constraints:
                airflow_version_constraints = AirflowVersionConstraints(
                    airflow_version=airflow_version, constraints_files=[]
                )
                all_constraints[airflow_version] = airflow_version_constraints
            else:
                airflow_version_constraints = all_constraints[airflow_version]
            package_dict: dict[str, PackageInfo] = {}
            for line in filename.read_text().splitlines():
                if line and not line.startswith("#"):
                    package_name, version = line.split("==")
                    package_dict[package_name] = PackageInfo(package_name=package_name, version=version)
            airflow_version_constraints.constraints_files.append(
                ConstraintsForPython(python_version=python_version, packages=package_dict)
            )
    get_console().print("[info]Constraints loaded[/]\n")
    if get_verbose():
        get_console().print("[info]All constraints loaded:\n")
        for airflow_version, constraints in all_constraints.items():
            get_console().print(f"[info]Airflow version: {airflow_version}[/]")
            for constraints_file in constraints.constraints_files:
                get_console().print(f"  Python version: {constraints_file.python_version}")
    return all_constraints


START_AIRFLOW_VERSION_FROM = "0.0.0"


def generate_providers_metadata_for_provider(
    provider_id: str,
    provider_version: str | None,
    constraints: dict[str, AirflowVersionConstraints],
    all_airflow_releases: list[str],
    airflow_release_dates: dict[str, str],
    current_metadata: dict[str, dict[str, dict[str, str]]],
) -> dict[str, dict[str, str]]:
    get_console().print(f"[info]Generating metadata for {provider_id}")
    provider_yaml_dict = get_provider_distributions_metadata()[provider_id]
    provider_metadata: dict[str, dict[str, str]] = {}
    package_name = "apache-airflow-providers-" + provider_id.replace(".", "-")
    provider_versions = list(reversed(provider_yaml_dict["versions"]))
    provider_metadata_found = False
    if get_verbose():
        get_console().print(f"[info]Provider {provider_id} versions:")
        get_console().print(provider_versions)
    if provider_version and provider_version not in provider_versions:
        get_console().print(
            f"[error]Provider {provider_id} version {provider_version} is not in the list of versions: "
            f"{provider_versions}. Skipping it."
        )
        sys.exit(1)
    old_provider_metadata = current_metadata.get(provider_id, {})
    for current_provider_version in provider_versions:
        if provider_version and current_provider_version != provider_version:
            continue
        exact_provider_version_found_in_constraints = False
        provider_date_released = get_tag_date(
            tag="providers-" + provider_id.replace(".", "-") + "/" + current_provider_version
        )
        if not provider_date_released:
            continue
        if get_verbose():
            get_console().print(
                f"[info]Checking provider {provider_id} version {current_provider_version} released on {provider_date_released}"
            )
        airflow_date_released = airflow_release_dates[all_airflow_releases[0]]
        last_airflow_version = START_AIRFLOW_VERSION_FROM
        for airflow_version in all_airflow_releases:
            airflow_date_released = airflow_release_dates[airflow_version]
            if get_verbose():
                get_console().print(
                    f"[info]Checking airflow_version {airflow_version} released on {airflow_date_released}"
                )
            for python_version_constraint_file in constraints[airflow_version].constraints_files:
                if get_verbose():
                    get_console().print(
                        f"[info]Checking constraints for Python {python_version_constraint_file.python_version}"
                    )
                package_info = python_version_constraint_file.packages.get(package_name)
                if not package_info:
                    if get_verbose():
                        get_console().print(
                            f"[info]Package {package_name} not found in constraints for Airflow {airflow_version} "
                            f"and Python version {python_version_constraint_file.python_version}"
                        )
                else:
                    if get_verbose():
                        get_console().print(
                            f"[info]Package {package_name} found in constraints for Airflow {airflow_version} "
                            f"and Python version {python_version_constraint_file.python_version}: {package_info}"
                        )
                if package_info and package_info.version == current_provider_version:
                    last_airflow_version = airflow_version
                    exact_provider_version_found_in_constraints = True
                    if get_verbose():
                        get_console().print(
                            f"[success]Package {package_name} in version {current_provider_version} "
                            f"found in constraints for Airflow {airflow_version} and "
                            f"Python version {python_version_constraint_file.python_version}"
                        )
                    break
                if (
                    airflow_date_released > provider_date_released
                    and last_airflow_version == START_AIRFLOW_VERSION_FROM
                ):
                    # released before first Airflow version so it should be associated with the
                    # first "real" Airflow version released after it - but in case it was actually
                    # mentioned later in constraints, we will override it later
                    last_airflow_version = airflow_version
                    if get_verbose():
                        get_console().print(
                            f"[warning]Provider {provider_id} version {current_provider_version} released on "
                            f"{provider_date_released} could be associated with {airflow_version} that "
                            f"was released on {airflow_date_released}. Setting it as candidate."
                        )
            if exact_provider_version_found_in_constraints:
                break
        if last_airflow_version == START_AIRFLOW_VERSION_FROM:
            # If we did not find any Airflow version that is associated with this provider version
            # we will not include it in the metadata
            get_console().print(
                f"[warning]Provider {provider_id} version {current_provider_version} released on {provider_date_released} "
                f"is NOT associated with any Airflow version in constraints. Skipping it."
            )
            continue
        old_provider_metadata_for_version = old_provider_metadata.get(current_provider_version, {})
        new_provider_metadata_for_version = {
            "associated_airflow_version": last_airflow_version,
            "date_released": provider_date_released,
        }
        provider_metadata[current_provider_version] = new_provider_metadata_for_version
        provider_version_metadata_changed_or_added = False
        if old_provider_metadata_for_version:
            if (
                old_provider_metadata_for_version["associated_airflow_version"] != last_airflow_version
                or old_provider_metadata_for_version["date_released"] != provider_date_released
            ):
                get_console().print(
                    f"[warning]Old provider metadata for {provider_id} version {current_provider_version} "
                    f"released on {provider_date_released} differs: "
                    f"Old metadata: {old_provider_metadata_for_version}. "
                    f"New metadata: {new_provider_metadata_for_version}."
                )
                provider_version_metadata_changed_or_added = True
        else:
            provider_version_metadata_changed_or_added = True
        if exact_provider_version_found_in_constraints:
            provider_metadata_found = True
            if get_verbose() or provider_version_metadata_changed_or_added:
                get_console().print(
                    f"[success]Provider {provider_id} version {current_provider_version} released on {provider_date_released} "
                    f"is associated with Airflow {last_airflow_version} released on {airflow_date_released}"
                )
        else:
            if get_verbose() or provider_version_metadata_changed_or_added:
                get_console().print(
                    f"[warning]Provider {provider_id} version {current_provider_version} released on {provider_date_released} "
                    f"was not mentioned in any Airflow version in constraints. Assuming {last_airflow_version} "
                    f"released on {airflow_date_released} that was released after it."
                )
            provider_metadata_found = True
    if not provider_metadata_found:
        get_console().print(
            f"[warning]No constraints mention {provider_id} in any Airflow version in any Python version. "
            f"Skipping it altogether."
        )
        return {}
    if get_verbose():
        get_console().print(f"[success]Metadata for {provider_id} found:\n")
        get_console().print(provider_metadata)
    return provider_metadata
