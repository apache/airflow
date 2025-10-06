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

import fnmatch
import json
import os
import re
import subprocess
import sys
from collections.abc import Generator, Iterable
from contextlib import contextmanager
from enum import Enum
from functools import lru_cache
from pathlib import Path
from typing import Any, NamedTuple

from rich.syntax import Syntax

from airflow_breeze.global_constants import (
    ALLOWED_PYTHON_MAJOR_MINOR_VERSIONS,
    DEFAULT_PYTHON_MAJOR_MINOR_VERSION,
    PROVIDER_RUNTIME_DATA_SCHEMA_PATH,
    REGULAR_DOC_PACKAGES,
)
from airflow_breeze.utils.console import get_console
from airflow_breeze.utils.functools_cache import clearable_cache
from airflow_breeze.utils.path_utils import (
    AIRFLOW_ORIGINAL_PROVIDERS_DIR,
    AIRFLOW_PROVIDERS_ROOT_PATH,
    BREEZE_SOURCES_PATH,
    DOCS_ROOT,
    PREVIOUS_AIRFLOW_PROVIDERS_NS_PACKAGE_PATH,
)
from airflow_breeze.utils.publish_docs_helpers import (
    PROVIDER_DATA_SCHEMA_PATH,
)
from airflow_breeze.utils.run_utils import run_command
from airflow_breeze.utils.version_utils import remove_local_version_suffix
from airflow_breeze.utils.versions import get_version_tag, strip_leading_zeros_from_version

MIN_AIRFLOW_VERSION = "2.11.0"
HTTPS_REMOTE = "apache-https-for-providers"

LONG_PROVIDERS_PREFIX = "apache-airflow-providers-"


class EntityType(Enum):
    Operators = "Operators"
    Transfers = "Transfers"
    Sensors = "Sensors"
    Hooks = "Hooks"
    Secrets = "Secrets"


class PluginInfo(NamedTuple):
    name: str
    package_name: str
    class_name: str


class ProviderPackageDetails(NamedTuple):
    provider_id: str
    provider_yaml_path: Path
    source_date_epoch: int
    full_package_name: str
    pypi_package_name: str
    root_provider_path: Path
    base_provider_package_path: Path
    documentation_provider_distribution_path: Path
    possible_old_provider_paths: list[Path]
    changelog_path: Path
    provider_description: str
    dependencies: list[str]
    versions: list[str]
    excluded_python_versions: list[str]
    plugins: list[PluginInfo]
    removed: bool
    extra_project_metadata: str | None = None
    hatch_artifacts: list[str] = []
    hatch_excludes: list[str] = []


class PackageSuspendedException(Exception):
    """Exception raised when package is suspended."""


class PipRequirements(NamedTuple):
    """Store details about python packages"""

    package: str
    version_required: str

    @classmethod
    def from_requirement(cls, requirement_string: str) -> PipRequirements:
        from packaging.requirements import Requirement

        req = Requirement(requirement_string)

        package = req.name
        if req.extras:
            # Sort extras by name
            package += f"[{','.join(sorted(req.extras))}]"

        version_required = ""
        if req.specifier:
            # String representation of `packaging.specifiers.SpecifierSet` sorted by the operator
            # which might not looking good, e.g. '>=5.3.0,<6,!=5.3.3,!=5.3.2' transform into the
            # '!=5.3.3,!=5.3.2,<6,>=5.3.0'. Instead of that we sort by version and resulting string would be
            # '>=5.3.0,!=5.3.2,!=5.3.3,<6'
            version_required = ",".join(map(str, sorted(req.specifier, key=lambda spec: spec.version)))
        if req.marker:
            version_required += f"; {req.marker}"

        return cls(package=package, version_required=version_required.strip())


@clearable_cache
def provider_yaml_schema() -> dict[str, Any]:
    with open(PROVIDER_DATA_SCHEMA_PATH) as schema_file:
        return json.load(schema_file)


PROVIDER_METADATA: dict[str, dict[str, Any]] = {}


def refresh_provider_metadata_from_yaml_file(provider_yaml_path: Path):
    import yaml

    with open(provider_yaml_path) as yaml_file:
        provider_yaml_content = yaml.safe_load(yaml_file)
    provider_id = get_short_package_name(provider_yaml_content["package-name"])
    PROVIDER_METADATA[provider_id] = provider_yaml_content
    toml_content = load_pyproject_toml(provider_yaml_path.parent / "pyproject.toml")
    dependencies = toml_content["project"].get("dependencies")
    if dependencies:
        PROVIDER_METADATA[provider_id]["dependencies"] = dependencies
    optional_dependencies = toml_content["project"].get("optional-dependencies")
    if optional_dependencies:
        PROVIDER_METADATA[provider_id]["optional-dependencies"] = optional_dependencies
    dependency_groups = toml_content.get("dependency-groups")
    if dependency_groups and dependency_groups.get("dev"):
        devel_dependencies = [
            dep for dep in dependency_groups.get("dev") if not dep.startswith("apache-airflow")
        ]
        PROVIDER_METADATA[provider_id]["devel-dependencies"] = devel_dependencies


def clear_cache_for_provider_metadata(provider_yaml_path: Path):
    get_provider_distributions_metadata.cache_clear()
    refresh_provider_metadata_from_yaml_file(provider_yaml_path)


@clearable_cache
def get_all_provider_yaml_paths() -> list[Path]:
    """Returns list of provider.yaml files"""
    return sorted(list(AIRFLOW_PROVIDERS_ROOT_PATH.glob("**/provider.yaml")))


def get_provider_id_from_path(file_path: Path) -> str | None:
    """
    Get the provider id from the path of the file it belongs to.
    """
    for parent in file_path.parents:
        # This works fine for both new and old providers structure - because we moved provider.yaml to
        # the top-level of the provider and this code finding "providers"  will find the "providers" package
        # in old structure and "providers" directory in new structure - in both cases we can determine
        # the provider id from the relative folders
        if (parent / "provider.yaml").exists():
            for providers_root_candidate in parent.parents:
                if providers_root_candidate.name == "providers":
                    return parent.relative_to(providers_root_candidate).as_posix().replace("/", ".")
            return None
    return None


@clearable_cache
def get_provider_distributions_metadata() -> dict[str, dict[str, Any]]:
    """
    Load all data from providers files

    :return: A list containing the contents of all provider.yaml files.
    """

    if PROVIDER_METADATA:
        return PROVIDER_METADATA

    for provider_yaml_path in get_all_provider_yaml_paths():
        refresh_provider_metadata_from_yaml_file(provider_yaml_path)
    return PROVIDER_METADATA


def validate_provider_info_with_runtime_schema(provider_info: dict[str, Any]) -> None:
    """Validates provider info against the runtime schema.

    This way we check if the provider info in the packages is future-compatible.
    The Runtime Schema should only change when there is a major version change.

    :param provider_info: provider info to validate
    """
    import jsonschema

    schema = json.loads(PROVIDER_RUNTIME_DATA_SCHEMA_PATH.read_text())
    try:
        jsonschema.validate(provider_info, schema=schema)
    except jsonschema.ValidationError as ex:
        get_console().print(
            "[red]Error when validating schema. The schema must be compatible with "
            "[bold]'airflow/provider_info.schema.json'[/bold].\n"
            f"Original exception [bold]{type(ex).__name__}: {ex}[/]"
        )
        raise SystemExit(1)


def filter_provider_info_data(provider_info: dict[str, Any]) -> dict[str, Any]:
    json_schema_dict = json.loads(PROVIDER_RUNTIME_DATA_SCHEMA_PATH.read_text())
    runtime_properties = json_schema_dict["properties"].keys()
    return_dict = {
        property: provider_info[property]
        for property in provider_info.keys()
        if property in runtime_properties
    }
    return return_dict


def get_provider_info_dict(provider_id: str) -> dict[str, Any]:
    """Retrieves provider info from the provider yaml file.

    :param provider_id: package id to retrieve provider.yaml from
    :return: provider_info dictionary
    """
    provider_yaml_dict = get_provider_distributions_metadata().get(provider_id)
    if provider_yaml_dict:
        provider_yaml_dict = filter_provider_info_data(provider_yaml_dict)
        validate_provider_info_with_runtime_schema(provider_yaml_dict)
    return provider_yaml_dict or {}


@lru_cache
def get_suspended_provider_ids() -> list[str]:
    return get_available_distributions(include_suspended=True, include_regular=False)


@lru_cache
def get_suspended_provider_folders() -> list[str]:
    return [provider_id.replace(".", "/") for provider_id in get_suspended_provider_ids()]


@lru_cache
def get_excluded_provider_ids(python_version: str) -> list[str]:
    metadata = get_provider_distributions_metadata()
    return [
        provider_id
        for provider_id, provider_metadata in metadata.items()
        if python_version in provider_metadata.get("excluded-python-versions", [])
    ]


@lru_cache
def get_excluded_provider_folders(python_version: str) -> list[str]:
    return [provider_id.replace(".", "/") for provider_id in get_excluded_provider_ids(python_version)]


@lru_cache
def get_removed_provider_ids() -> list[str]:
    return get_available_distributions(include_removed=True, include_regular=False)


@lru_cache
def get_not_ready_provider_ids() -> list[str]:
    return get_available_distributions(include_not_ready=True, include_regular=False)


def get_provider_requirements(provider_id: str) -> list[str]:
    package_metadata = get_provider_distributions_metadata().get(provider_id)
    return package_metadata["dependencies"] if package_metadata else []


def get_provider_optional_dependencies(provider_id: str) -> dict[str, list[str]]:
    package_metadata = get_provider_distributions_metadata().get(provider_id)
    return package_metadata.get("optional-dependencies", {}) if package_metadata else {}


@lru_cache
def get_available_distributions(
    include_non_provider_doc_packages: bool = False,
    include_all_providers: bool = False,
    include_suspended: bool = False,
    include_removed: bool = False,
    include_not_ready: bool = False,
    include_regular: bool = True,
) -> list[str]:
    """
    Return provider ids for all packages that are available currently (not suspended).

    :param include_suspended: whether the suspended packages should be included
    :param include_removed: whether the removed packages should be included
    :param include_not_ready: whether the not-ready packages should be included
    :param include_regular: whether the regular packages should be included
    :param include_non_provider_doc_packages: whether the non-provider doc packages should be included
           (packages like apache-airflow, helm-chart, docker-stack)
    :param include_all_providers: whether "all-providers" should be included ni the list.

    """
    # Need lazy import to prevent circular dependencies
    from airflow_breeze.utils.provider_dependencies import get_provider_dependencies

    provider_dependencies = get_provider_dependencies()

    valid_states = set()
    if include_not_ready:
        valid_states.add("not-ready")
    if include_regular:
        valid_states.update({"ready", "pre-release"})
    if include_suspended:
        valid_states.add("suspended")
    if include_removed:
        valid_states.add("removed")
    available_packages: list[str] = [
        provider_id
        for provider_id, provider_dependencies in provider_dependencies.items()
        if provider_dependencies["state"] in valid_states
    ]
    if include_non_provider_doc_packages:
        available_packages.extend(REGULAR_DOC_PACKAGES)
    if include_all_providers:
        available_packages.append("all-providers")
    return sorted(set(available_packages))


def expand_all_provider_distributions(
    short_doc_packages: tuple[str, ...],
    include_removed: bool = False,
    include_not_ready: bool = False,
) -> tuple[str, ...]:
    """In case there are "all-providers" in the list, expand the list with all providers."""
    if "all-providers" in short_doc_packages:
        packages = [package for package in short_doc_packages if package != "all-providers"]
        packages.extend(
            get_available_distributions(include_removed=include_removed, include_not_ready=include_not_ready)
        )
        short_doc_packages = tuple(set(packages))
    return short_doc_packages


def get_long_package_names(short_form_providers: Iterable[str]) -> tuple[str, ...]:
    providers: list[str] = []
    for short_form_provider in short_form_providers:
        long_package_name = get_long_package_name(short_form_provider)
        providers.append(long_package_name)
    return tuple(providers)


def get_long_package_name(short_form_provider: str) -> str:
    if short_form_provider in REGULAR_DOC_PACKAGES:
        long_package_name = short_form_provider
    else:
        long_package_name = LONG_PROVIDERS_PREFIX + "-".join(short_form_provider.split("."))
    return long_package_name


def get_short_package_names(long_form_providers: Iterable[str]) -> tuple[str, ...]:
    providers: list[str] = []
    for long_form_provider in long_form_providers:
        providers.append(get_short_package_name(long_form_provider))
    return tuple(providers)


def get_short_package_name(long_form_provider: str) -> str:
    if long_form_provider in REGULAR_DOC_PACKAGES:
        return long_form_provider
    if not long_form_provider.startswith(LONG_PROVIDERS_PREFIX):
        raise ValueError(
            f"Invalid provider name: {long_form_provider}. Should start with {LONG_PROVIDERS_PREFIX}"
        )
    return long_form_provider[len(LONG_PROVIDERS_PREFIX) :].replace("-", ".")


def find_matching_long_package_names(
    short_packages: tuple[str, ...],
    filters: tuple[str, ...] | None = None,
) -> tuple[str, ...]:
    """Finds matching long package names based on short package name and package filters specified.

    The sequence of specified packages / filters is kept (filters first, packages next). In case there
    are filters that do not match any of the packages error is raised.

    :param short_packages: short forms of package names
    :param filters: package filters specified
    """
    available_doc_packages = list(
        get_long_package_names(get_available_distributions(include_non_provider_doc_packages=True))
    )
    if not filters and not short_packages:
        available_doc_packages.extend(filters or ())
        return tuple(set(available_doc_packages))

    processed_package_filters = list(filters or ())
    processed_package_filters.extend(get_long_package_names(short_packages))

    removed_packages: list[str] = [
        f"apache-airflow-providers-{provider.replace('.', '-')}" for provider in get_removed_provider_ids()
    ]
    all_packages_including_removed: list[str] = available_doc_packages + removed_packages
    invalid_filters = [
        f
        for f in processed_package_filters
        if not any(fnmatch.fnmatch(p, f) for p in all_packages_including_removed)
    ]
    if invalid_filters:
        raise SystemExit(
            f"Some filters did not find any package: {invalid_filters}, Please check if they are correct."
        )

    return tuple(
        [
            p
            for p in all_packages_including_removed
            if any(fnmatch.fnmatch(p, f) for f in processed_package_filters)
        ]
    )


def get_provider_root_path(provider_id: str) -> Path:
    return AIRFLOW_PROVIDERS_ROOT_PATH / provider_id.replace(".", "/")


def get_possible_old_provider_paths(provider_id: str) -> list[Path]:
    # This is used to get historical commits for the provider
    paths: list[Path] = []
    paths.append(AIRFLOW_ORIGINAL_PROVIDERS_DIR.joinpath(*provider_id.split(".")))
    paths.append(PREVIOUS_AIRFLOW_PROVIDERS_NS_PACKAGE_PATH.joinpath(*provider_id.split(".")))
    paths.append(DOCS_ROOT / f"apache-airflow-providers-{provider_id.replace('.', '-')}")
    if provider_id == "edge3":
        paths.append(get_provider_root_path("edge"))
        paths.append(get_provider_root_path("edgeexecutor"))
    return paths


def get_documentation_package_path(provider_id: str) -> Path:
    return AIRFLOW_PROVIDERS_ROOT_PATH.joinpath(*provider_id.split(".")) / "docs"


def get_pip_package_name(provider_id: str) -> str:
    """
    Returns PIP package name for the package id.

    :param provider_id: id of the package
    :return: the name of pip package
    """
    return "apache-airflow-providers-" + provider_id.replace(".", "-")


def get_dist_package_name_prefix(provider_id: str) -> str:
    """
    Returns Wheel package name prefix for the package id.

    :param provider_id: id of the package
    :return: the name of wheel package prefix
    """
    return "apache_airflow_providers_" + provider_id.replace(".", "_")


def floor_version_suffix(version_suffix: str) -> str:
    # always use `pre-release`+ `0` as the version suffix
    from packaging.version import Version

    base_version = "1.0.0"
    version = Version(base_version + version_suffix)
    pre_version_floored = version.pre[0] + "1" if version.pre else ""
    dev_version_floored = ".dev0" if version.dev is not None else ""
    post_version_floored = ".post0" if version.post is not None else ""
    # local version cannot be used in >= comparison - so we have to remove it from floored version
    floored_version = Version(
        f"{base_version}{pre_version_floored}{dev_version_floored}{post_version_floored}"
    )
    return str(floored_version)[len(base_version) :]


def apply_version_suffix(install_clause: str, version_suffix: str) -> str:
    # Need to resolve a version suffix based on PyPi versions, but can ignore local version suffix.
    pypi_version_suffix = remove_local_version_suffix(version_suffix)
    if pypi_version_suffix and install_clause.startswith("apache-airflow") and ">=" in install_clause:
        # Applies version suffix to the apache-airflow and provider package dependencies to make
        # sure that pre-release versions have correct limits - this address the issue with how
        # pip handles pre-release versions when packages are pre-release and refer to each other - we
        # need to make sure that all our >= references for all apache-airflow packages in pre-release
        # versions of providers contain the same suffix as the provider itself.
        # For example `apache-airflow-providers-fab==2.0.0.dev0` should refer to
        # `apache-airflow>=2.9.0.dev0` and not `apache-airflow>=2.9.0` because both packages are
        # released together and >= 2.9.0 is not correct reference for 2.9.0.dev0 version of Airflow.
        # This assumes a local release, one where the suffix starts with a plus sign, uses the last
        # version of the dependency, so it is not necessary to add the suffix to the dependency.
        prefix, version = install_clause.split(">=")
        # If version has a upper limit (e.g. ">=2.10.0,<3.0"), we need to cut this off not to fail
        if "," in version:
            version = version.split(",")[0]
        from packaging.version import Version

        base_version = Version(version).base_version
        target_version = Version(str(base_version) + floor_version_suffix(pypi_version_suffix))
        return prefix + ">=" + str(target_version)
    return install_clause


def get_provider_yaml(provider_id: str) -> Path:
    return AIRFLOW_PROVIDERS_ROOT_PATH / provider_id.replace(".", "/") / "provider.yaml"


def load_pyproject_toml(pyproject_toml_file_path: Path) -> dict[str, Any]:
    try:
        import tomllib
    except ImportError:
        import tomli as tomllib
    toml_content = pyproject_toml_file_path.read_text()
    syntax = Syntax(toml_content, "toml", theme="ansi_dark", line_numbers=True)
    try:
        return tomllib.loads(toml_content)
    except tomllib.TOMLDecodeError as e:
        get_console().print(syntax)
        get_console().print(f"[red]Error when loading {pyproject_toml_file_path}: {e}:")
        sys.exit(1)


def get_provider_details(provider_id: str) -> ProviderPackageDetails:
    provider_info = get_provider_distributions_metadata().get(provider_id)
    if not provider_info:
        raise RuntimeError(f"The provider {provider_id} has no provider.yaml defined.")
    plugins: list[PluginInfo] = []
    if "plugins" in provider_info:
        for plugin in provider_info["plugins"]:
            package_name, class_name = plugin["plugin-class"].rsplit(".", maxsplit=1)
            plugins.append(
                PluginInfo(
                    name=plugin["name"],
                    package_name=package_name,
                    class_name=class_name,
                )
            )
    provider_yaml_path = get_provider_yaml(provider_id)
    pyproject_toml = load_pyproject_toml(provider_yaml_path.parent / "pyproject.toml")
    dependencies = pyproject_toml["project"]["dependencies"]
    _hatch_targets: dict = pyproject_toml.get("tool", {}).get("hatch", {}).get("build", {}).get("targets", {})
    hatch_artifacts = _hatch_targets.get("custom", {}).get("artifacts", [])
    hatch_excludes = _hatch_targets.get("sdist", {}).get("exclude", [])
    changelog_path = provider_yaml_path.parent / "docs" / "changelog.rst"
    documentation_provider_distribution_path = get_documentation_package_path(provider_id)
    root_provider_path = provider_yaml_path.parent
    base_provider_package_path = (provider_yaml_path.parent / "src" / "airflow" / "providers").joinpath(
        *provider_id.split(".")
    )
    return ProviderPackageDetails(
        provider_id=provider_id,
        provider_yaml_path=provider_yaml_path,
        source_date_epoch=provider_info["source-date-epoch"],
        full_package_name=f"airflow.providers.{provider_id}",
        pypi_package_name=f"apache-airflow-providers-{provider_id.replace('.', '-')}",
        root_provider_path=root_provider_path,
        base_provider_package_path=base_provider_package_path,
        possible_old_provider_paths=get_possible_old_provider_paths(provider_id),
        documentation_provider_distribution_path=documentation_provider_distribution_path,
        changelog_path=changelog_path,
        provider_description=provider_info["description"],
        dependencies=dependencies,
        versions=provider_info["versions"],
        excluded_python_versions=provider_info.get("excluded-python-versions", []),
        plugins=plugins,
        removed=provider_info["state"] == "removed",
        extra_project_metadata=provider_info.get("extra-project-metadata", ""),
        hatch_artifacts=hatch_artifacts,
        hatch_excludes=hatch_excludes,
    )


def get_min_airflow_version(provider_id: str) -> str:
    from packaging.version import Version as PackagingVersion

    provider_details = get_provider_details(provider_id=provider_id)
    min_airflow_version = MIN_AIRFLOW_VERSION
    for dependency in provider_details.dependencies:
        if dependency.startswith("apache-airflow>="):
            current_min_airflow_version = dependency.split(">=")[1]
            # If version has a upper limit (e.g. ">=2.10.0,<3.0"), we need to cut this off not to fail
            if "," in current_min_airflow_version:
                current_min_airflow_version = current_min_airflow_version.split(",")[0]
            if PackagingVersion(current_min_airflow_version) > PackagingVersion(MIN_AIRFLOW_VERSION):
                min_airflow_version = current_min_airflow_version
    return min_airflow_version


def get_python_requires(provider_id: str) -> str:
    python_requires = "~=3.10"
    provider_details = get_provider_details(provider_id=provider_id)
    for p in provider_details.excluded_python_versions:
        python_requires += f", !={p}"
    return python_requires


def convert_cross_package_dependencies_to_table(
    cross_package_dependencies: list[str],
    markdown: bool = True,
) -> str:
    """
    Converts cross-package dependencies to a Markdown table
    :param cross_package_dependencies: list of cross-package dependencies
    :param markdown: if True, Markdown format is used else rst
    :return: formatted table
    """
    from tabulate import tabulate

    headers = ["Dependent package", "Extra"]
    table_data = []
    prefix = "apache-airflow-providers-"
    base_url = "https://airflow.apache.org/docs/"
    for dependency in cross_package_dependencies:
        pip_package_name = f"{prefix}{dependency.replace('.', '-')}"
        url_suffix = f"{dependency.replace('.', '-')}"
        if markdown:
            url = f"[{pip_package_name}]({base_url}{url_suffix})"
        else:
            url = f"`{pip_package_name} <{base_url}{prefix}{url_suffix}>`_"
        table_data.append((url, f"`{dependency}`" if markdown else f"``{dependency}``"))
    return tabulate(table_data, headers=headers, tablefmt="pipe" if markdown else "rst")


def convert_optional_dependencies_to_table(
    optional_dependencies: dict[str, list[str]],
    markdown: bool = True,
) -> str:
    """
    Converts optional dependencies to a Markdown/RST table
    :param optional_dependencies: dict of optional dependencies
    :param markdown: if True, Markdown format is used else rst
    :return: formatted table
    """
    import html

    from tabulate import tabulate

    headers = ["Extra", "Dependencies"]
    table_data = []
    for extra_name, dependencies in optional_dependencies.items():
        decoded_deps = [html.unescape(dep) for dep in dependencies]
        formatted_deps = ", ".join(f"`{dep}`" if markdown else f"``{dep}``" for dep in decoded_deps)
        extra_col = f"`{extra_name}`" if markdown else f"``{extra_name}``"
        table_data.append((extra_col, formatted_deps))
    return tabulate(table_data, headers=headers, tablefmt="pipe" if markdown else "rst")


def get_cross_provider_dependent_packages(provider_id: str) -> list[str]:
    if provider_id in get_removed_provider_ids():
        return []

    # Need lazy import to prevent circular dependencies
    from airflow_breeze.utils.provider_dependencies import get_provider_dependencies

    return get_provider_dependencies()[provider_id]["cross-providers-deps"]


def get_license_files(provider_id: str) -> str:
    if provider_id == "fab":
        return str(["LICENSE", "NOTICE", "3rd-party-licenses/LICENSES-*"]).replace('"', "'")
    return str(["LICENSE", "NOTICE"]).replace('"', "'")


def get_provider_jinja_context(
    provider_id: str,
    current_release_version: str,
    version_suffix: str,
):
    provider_details = get_provider_details(provider_id=provider_id)
    release_version_no_leading_zeros = strip_leading_zeros_from_version(current_release_version)
    changelog = provider_details.changelog_path.read_text()
    supported_python_versions = [
        p for p in ALLOWED_PYTHON_MAJOR_MINOR_VERSIONS if p not in provider_details.excluded_python_versions
    ]
    cross_providers_dependencies = get_cross_provider_dependent_packages(provider_id=provider_id)

    requires_python_version: str = f">={DEFAULT_PYTHON_MAJOR_MINOR_VERSION}"
    # Most providers require the same python versions, but some may have exclusions
    for excluded_python_version in provider_details.excluded_python_versions:
        requires_python_version += f",!={excluded_python_version}"

    context: dict[str, Any] = {
        "PROVIDER_ID": provider_details.provider_id,
        "PACKAGE_PIP_NAME": get_pip_package_name(provider_details.provider_id),
        "PACKAGE_DIST_PREFIX": get_dist_package_name_prefix(provider_details.provider_id),
        "FULL_PACKAGE_NAME": provider_details.full_package_name,
        "RELEASE": current_release_version,
        "RELEASE_NO_LEADING_ZEROS": release_version_no_leading_zeros,
        "VERSION_SUFFIX": version_suffix,
        "PIP_REQUIREMENTS": get_provider_requirements(provider_details.provider_id),
        "PROVIDER_DESCRIPTION": provider_details.provider_description,
        "CHANGELOG_RELATIVE_PATH": os.path.relpath(
            provider_details.root_provider_path,
            provider_details.documentation_provider_distribution_path,
        ),
        "LICENSE_FILES": get_license_files(provider_details.provider_id),
        "CHANGELOG": changelog,
        "SUPPORTED_PYTHON_VERSIONS": supported_python_versions,
        "PLUGINS": provider_details.plugins,
        "MIN_AIRFLOW_VERSION": get_min_airflow_version(provider_id),
        "PROVIDER_REMOVED": provider_details.removed,
        "PROVIDER_INFO": get_provider_info_dict(provider_id),
        "CROSS_PROVIDERS_DEPENDENCIES": get_cross_provider_dependent_packages(provider_id),
        "CROSS_PROVIDERS_DEPENDENCIES_TABLE_RST": convert_cross_package_dependencies_to_table(
            cross_providers_dependencies, markdown=False
        ),
        "PIP_REQUIREMENTS_TABLE_RST": convert_pip_requirements_to_table(
            get_provider_requirements(provider_id), markdown=False
        ),
        "REQUIRES_PYTHON": requires_python_version,
        "EXTRA_PROJECT_METADATA": provider_details.extra_project_metadata,
        "OPTIONAL_DEPENDENCIES": get_provider_optional_dependencies(provider_id),
        "OPTIONAL_DEPENDENCIES_TABLE_RST": convert_optional_dependencies_to_table(
            get_provider_optional_dependencies(provider_id), markdown=False
        ),
    }
    return context


def render_template(
    template_name: str,
    context: dict[str, Any],
    extension: str,
    autoescape: bool = True,
    lstrip_blocks: bool = False,
    trim_blocks: bool = False,
    keep_trailing_newline: bool = False,
) -> str:
    """
    Renders template based on its name. Reads the template from <name>_TEMPLATE.md.jinja2 in current dir.
    :param template_name: name of the template to use
    :param context: Jinja2 context
    :param extension: Target file extension
    :param autoescape: Whether to autoescape HTML
    :param lstrip_blocks: Whether to strip leading blocks
    :param trim_blocks: Whether to trim blocks
    :param keep_trailing_newline: Whether to keep the newline in rendered output
    :return: rendered template
    """
    import jinja2

    template_loader = jinja2.FileSystemLoader(searchpath=BREEZE_SOURCES_PATH / "airflow_breeze" / "templates")
    template_env = jinja2.Environment(
        loader=template_loader,
        undefined=jinja2.StrictUndefined,
        autoescape=autoescape,
        lstrip_blocks=lstrip_blocks,
        trim_blocks=trim_blocks,
        keep_trailing_newline=keep_trailing_newline,
    )
    template = template_env.get_template(f"{template_name}_TEMPLATE{extension}.jinja2")
    content: str = template.render(context)
    return content


def make_sure_remote_apache_exists_and_fetch(github_repository: str = "apache/airflow"):
    """Make sure that apache remote exist in git.

    We need to take a log from the apache repository main branch - not locally because we might
    not have the latest version. Also, the local repo might be shallow, so we need to
    un-shallow it to see all the history.

    This will:
    * check if the remote exists and add if it does not
    * check if the local repo is shallow, mark it to un-shallow in this case
    * fetch from the remote including all tags and overriding local tags in case
      they are set differently

    """
    try:
        run_command(["git", "remote", "get-url", HTTPS_REMOTE], text=True, capture_output=True)
    except subprocess.CalledProcessError as ex:
        if ex.returncode == 128 or ex.returncode == 2:
            run_command(
                [
                    "git",
                    "remote",
                    "add",
                    HTTPS_REMOTE,
                    f"https://github.com/{github_repository}.git",
                ],
                check=True,
            )
        else:
            get_console().print(
                f"[error]Error {ex}[/]\n[error]When checking if {HTTPS_REMOTE} is set.[/]\n\n"
            )
            sys.exit(1)
    get_console().print("[info]Fetching full history and tags from remote.")
    get_console().print("[info]This might override your local tags!")
    result = run_command(
        ["git", "rev-parse", "--is-shallow-repository"],
        check=True,
        capture_output=True,
        text=True,
    )
    is_shallow_repo = result.stdout.strip() == "true"
    fetch_command = ["git", "fetch", "--tags", "--force", HTTPS_REMOTE]
    if is_shallow_repo:
        fetch_command.append("--unshallow")
    try:
        run_command(fetch_command)
    except subprocess.CalledProcessError as e:
        get_console().print(
            f"[error]Error {e}[/]\n"
            f"[error]When fetching tags from remote. Your tags might not be refreshed.[/]\n\n"
            f'[warning]Please refresh the tags manually via:[/]\n\n"'
            f"{' '.join(fetch_command)}\n\n"
        )
        sys.exit(1)


def convert_pip_requirements_to_table(requirements: Iterable[str], markdown: bool = True) -> str:
    """
    Converts PIP requirement list to a Markdown table.
    :param requirements: requirements list
    :param markdown: if True, Markdown format is used else rst
    :return: formatted table
    """
    from tabulate import tabulate

    headers = ["PIP package", "Version required"]
    table_data = []
    for dependency in requirements:
        req = PipRequirements.from_requirement(dependency)
        formatted_package = f"`{req.package}`" if markdown else f"``{req.package}``"
        formatted_version = ""
        if req.version_required:
            formatted_version = f"`{req.version_required}`" if markdown else f"``{req.version_required}``"
        table_data.append((formatted_package, formatted_version))
    return tabulate(table_data, headers=headers, tablefmt="pipe" if markdown else "rst")


def tag_exists_for_provider(provider_id: str, current_tag: str) -> bool:
    """Return true if the tag exists in the provider repository."""
    provider_details = get_provider_details(provider_id)
    result = run_command(
        ["git", "rev-parse", current_tag],
        cwd=provider_details.root_provider_path,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        check=False,
    )
    return result.returncode == 0


def get_latest_provider_tag(provider_id: str, suffix: str) -> str:
    """Returns latest tag for the provider."""
    provider_details = get_provider_details(provider_id)
    current_version = provider_details.versions[0]
    return get_version_tag(current_version, provider_id, suffix)


def regenerate_pyproject_toml(
    context: dict[str, Any], provider_details: ProviderPackageDetails, version_suffix: str | None
):
    # Need lazy import to prevent circular dependencies
    from airflow_breeze.utils.provider_dependencies import get_provider_dependencies

    get_pyproject_toml_path = provider_details.root_provider_path / "pyproject.toml"
    # we want to preserve comments in dependencies - both required and additional,
    # so we should not really parse the toml file but extract dependencies "as is" in text form and pass
    # them to context. While this is not "generic toml" perfect, for provider pyproject.toml files it is
    # good enough, because we fully control the pyproject.toml content for providers as they are generated
    # from our templates (Except the dependencies section that is manually updated)
    pyproject_toml_content = get_pyproject_toml_path.read_text()
    required_dependencies: list[str] = []
    optional_dependencies: list[str] = []
    dependency_groups: list[str] = []
    in_required_dependencies = False
    in_optional_dependencies = False
    in_additional_devel_dependency_groups = False
    for line in pyproject_toml_content.splitlines():
        if line == "dependencies = [":
            in_required_dependencies = True
            continue
        if in_required_dependencies and line == "]":
            in_required_dependencies = False
            continue
        if line == (
            "    # Additional devel dependencies (do not remove this "
            "line and add extra development dependencies)"
        ):
            in_additional_devel_dependency_groups = True
            continue
        if in_additional_devel_dependency_groups and line == "]":
            in_additional_devel_dependency_groups = False
            continue
        if line == "[project.optional-dependencies]":
            in_optional_dependencies = True
            continue
        if in_optional_dependencies and line == "":
            in_optional_dependencies = False
            continue
        if in_optional_dependencies and line.startswith("["):
            in_optional_dependencies = False
        if in_required_dependencies:
            required_dependencies.append(line)
        if in_optional_dependencies:
            optional_dependencies.append(line)
        if in_additional_devel_dependency_groups:
            dependency_groups.append(line)
    # For additional providers we want to load the dependencies and see if cross-provider-dependencies are
    # present and if not, add them to the optional dependencies
    if version_suffix:
        new_required_dependencies = []
        for dependency in required_dependencies:
            modified_dependency = modify_dependency_with_suffix(dependency, version_suffix)
            new_required_dependencies.append(modified_dependency)
        required_dependencies = new_required_dependencies
        new_optional_dependencies = []
        for dependency in optional_dependencies:
            modified_dependency = modify_dependency_with_suffix(dependency, version_suffix)
            new_optional_dependencies.append(modified_dependency)
        optional_dependencies = new_optional_dependencies
    context["INSTALL_REQUIREMENTS"] = "\n".join(required_dependencies)
    context["AIRFLOW_DOC_URL"] = (
        "https://airflow.staged.apache.org" if version_suffix else "https://airflow.apache.org"
    )
    cross_provider_ids = set(
        get_provider_dependencies()[provider_details.provider_id]["cross-providers-deps"]
    )
    cross_provider_dependencies = []
    # Add cross-provider dependencies to the optional dependencies if they are missing
    for provider_id in sorted(cross_provider_ids):
        cross_provider_dependencies.append(f'    "{get_pip_package_name(provider_id)}",')
        if f'"{provider_id}" = [' not in optional_dependencies and get_pip_package_name(
            provider_id
        ) not in "\n".join(required_dependencies):
            optional_dependencies.append(f'"{provider_id}" = [')
            optional_dependencies.append(f'    "{get_pip_package_name(provider_id)}"')
            optional_dependencies.append("]")
    context["EXTRAS_REQUIREMENTS"] = "\n".join(optional_dependencies)
    formatted_dependency_groups = "\n".join(dependency_groups)
    if formatted_dependency_groups:
        formatted_dependency_groups = "\n" + formatted_dependency_groups
    if cross_provider_dependencies:
        formatted_cross_provider_dependencies = "\n" + "\n".join(cross_provider_dependencies)
    else:  # If there are no cross-provider dependencies, we need to remove the line
        formatted_cross_provider_dependencies = ""
    context["CROSS_PROVIDER_DEPENDENCIES"] = formatted_cross_provider_dependencies
    context["DEPENDENCY_GROUPS"] = formatted_dependency_groups
    context["BUILD_SYSTEM"] = (
        "hatchling" if (provider_details.root_provider_path / "hatch_build.py").exists() else "flit_core"
    )
    if context["BUILD_SYSTEM"] == "hatchling":
        context["HATCH_ARTIFACTS"] = provider_details.hatch_artifacts
        context["HATCH_EXCLUDES"] = provider_details.hatch_excludes

    pyproject_toml_content = render_template(
        template_name="pyproject",
        context=context,
        extension=".toml",
        autoescape=False,
        lstrip_blocks=True,
        trim_blocks=True,
        keep_trailing_newline=True,
    )
    get_pyproject_toml_path.write_text(pyproject_toml_content)
    get_console().print(
        f"[info]Generated {get_pyproject_toml_path} for the {provider_details.provider_id} provider\n"
    )


AIRFLOW_PACKAGE_MATCHER = re.compile(r"(^.*\")(apache-airflow.*>=[\d.]*)((\".*)$|;.*$)")


def modify_dependency_with_suffix(dependency: str, version_suffix: str) -> str:
    match = AIRFLOW_PACKAGE_MATCHER.match(dependency)
    if match and not version_suffix.startswith(".post"):
        specifier_with_version_suffix = apply_version_suffix(match.group(2), version_suffix)
        return match.group(1) + specifier_with_version_suffix + match.group(3)
    return dependency


def get_provider_distribution_jinja_context(provider_id: str, version_suffix: str) -> dict[str, Any]:
    provider_details = get_provider_details(provider_id)
    jinja_context = get_provider_jinja_context(
        provider_id=provider_id,
        current_release_version=provider_details.versions[0],
        version_suffix=version_suffix,
    )
    return jinja_context


def _prepare_get_provider_info_py_file(context: dict[str, Any], provider_id: str, target_path: Path):
    from airflow_breeze.utils.black_utils import black_format

    get_provider_template_name = "get_provider_info"
    get_provider_content = render_template(
        template_name=get_provider_template_name,
        context=context,
        extension=".py",
        autoescape=False,
        keep_trailing_newline=True,
    )
    target_provider_specific_path = (target_path / "airflow" / "providers").joinpath(*provider_id.split("."))
    (target_provider_specific_path / "get_provider_info.py").write_text(black_format(get_provider_content))
    get_console().print(f"[info]Generated get_provider_info.py in {target_provider_specific_path}[/]")


LICENCE_RST = """
.. Licensed to the Apache Software Foundation (ASF) under one
   or more contributor license agreements.  See the NOTICE file
   distributed with this work for additional information
   regarding copyright ownership.  The ASF licenses this file
   to you under the Apache License, Version 2.0 (the
   "License"); you may not use this file except in compliance
   with the License.  You may obtain a copy of the License at

..   http://www.apache.org/licenses/LICENSE-2.0

.. Unless required by applicable law or agreed to in writing,
   software distributed under the License is distributed on an
   "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
   KIND, either express or implied.  See the License for the
   specific language governing permissions and limitations
   under the License.
"""


def _prepare_pyproject_toml_file(context: dict[str, Any], target_path: Path):
    manifest_content = render_template(
        template_name="pyproject",
        context=context,
        extension=".toml",
        autoescape=False,
        lstrip_blocks=True,
        trim_blocks=True,
        keep_trailing_newline=True,
    )
    (target_path / "pyproject.toml").write_text(manifest_content)
    get_console().print(f"[info]Generated pyproject.toml in {target_path}[/]")


def _prepare_readme_file(context: dict[str, Any], target_path: Path):
    readme_content = LICENCE_RST + render_template(
        template_name="PROVIDER_README", context=context, extension=".rst"
    )
    (target_path / "README.rst").write_text(readme_content)
    get_console().print(f"[info]Generated README.rst in {target_path}[/]")


def generate_build_files(provider_id: str, version_suffix: str, target_provider_root_sources_path: Path):
    get_console().print(f"\n[info]Generate build files for {provider_id}\n")
    jinja_context = get_provider_distribution_jinja_context(
        provider_id=provider_id, version_suffix=version_suffix
    )
    _prepare_get_provider_info_py_file(jinja_context, provider_id, target_provider_root_sources_path)
    _prepare_pyproject_toml_file(jinja_context, target_provider_root_sources_path)
    _prepare_readme_file(jinja_context, target_provider_root_sources_path)
    get_console().print(f"\n[info]Generated package build files for {provider_id}[/]\n")


@contextmanager
def apply_version_suffix_to_provider_pyproject_toml(
    provider_id: str, version_suffix: str
) -> Generator[Path, None, None]:
    """Apply version suffix to pyproject.toml file of provider.

    This context manager will read the pyproject.toml file, apply the version suffix
    to the version, and write the modified content back to the file.
    It will also restore the original content of the file when the context manager is exited.
    """

    provider_details = get_provider_details(provider_id)
    pyproject_toml_path = provider_details.root_provider_path / "pyproject.toml"
    if not version_suffix:
        yield pyproject_toml_path
        return
    original_pyproject_toml_content = pyproject_toml_path.read_text()
    get_console().print(f"\n[info]Applying version suffix {version_suffix} to {pyproject_toml_path}")
    jinja_context = get_provider_distribution_jinja_context(
        provider_id=provider_id, version_suffix=version_suffix
    )
    regenerate_pyproject_toml(jinja_context, provider_details, version_suffix)
    _prepare_pyproject_toml_file(jinja_context, provider_details.root_provider_path)
    try:
        yield pyproject_toml_path
    finally:
        get_console().print(f"\n[info]Restoring original pyproject.toml file {pyproject_toml_path}")
        pyproject_toml_path.write_text(original_pyproject_toml_content)


def update_version_suffix_in_non_provider_pyproject_toml(version_suffix: str, pyproject_toml_path: Path):
    if not version_suffix:
        return
    get_console().print(f"[info]Updating version suffix to {version_suffix} for {pyproject_toml_path}.\n")
    lines = pyproject_toml_path.read_text().splitlines()
    updated_lines = []
    for line in lines:
        base_line, comment = line.split(" #", 1) if " #" in line else (line, "")
        if comment:
            comment = " #" + comment
        if base_line.startswith("version = "):
            get_console().print(f"[info]Updating version suffix to {version_suffix} for {line}.")
            base_line = base_line.rstrip('"') + f'{version_suffix}"'
        if "https://airflow.apache.org/" in base_line and version_suffix:
            get_console().print(f"[info]Updating documentation link to staging for {line}.")
            base_line = base_line.replace("https://airflow.apache.org/", "https://airflow.staged.apache.org/")
        # do not modify references for .post prefixes
        if not version_suffix.startswith(".post"):
            if base_line.strip().startswith('"apache-airflow-') and ">=" in base_line:
                floored_version_suffix = floor_version_suffix(version_suffix)
                get_console().print(
                    f"[info]Updating version suffix to {floored_version_suffix} for {base_line}."
                )
                if ";" in base_line:
                    split_on_semicolon = base_line.split(";")
                    # If there is a semicolon, we need to remove it before adding the version suffix
                    base_line = split_on_semicolon[0] + f"{floored_version_suffix};" + split_on_semicolon[1]
                else:
                    base_line = base_line.rstrip('",') + f'{floored_version_suffix}",'
            if base_line.strip().startswith('"apache-airflow-core') and "==" in base_line:
                get_console().print(f"[info]Updating version suffix to {version_suffix} for {base_line}.")
                base_line = base_line.rstrip('",') + f'{version_suffix}",'
            if base_line.strip().startswith('"apache-airflow-task-sdk') and "==" in base_line:
                get_console().print(f"[info]Updating version suffix to {version_suffix} for {base_line}.")
                base_line = base_line.rstrip('",') + f'{version_suffix}",'
        updated_lines.append(f"{base_line}{comment}")
    new_content = "\n".join(updated_lines) + "\n"
    get_console().print(f"[info]Writing updated content to {pyproject_toml_path}.\n")
    pyproject_toml_path.write_text(new_content)


def set_package_version(version: str, init_file_path: Path, extra_text: str) -> None:
    get_console().print(f"\n[warning]Setting {extra_text} {version} version in {init_file_path}\n")
    # replace __version__ with the version passed as argument in python
    init_content = init_file_path.read_text()
    init_content = re.sub(r'__version__ = "[^"]+"', f'__version__ = "{version}"', init_content)
    init_file_path.write_text(init_content)


@contextmanager
def apply_version_suffix_to_non_provider_pyproject_tomls(
    version_suffix: str, init_file_path: Path, pyproject_toml_paths: list[Path]
) -> Generator[list[Path], None, None]:
    from packaging.version import Version

    original_version_search = re.search('__version__ = "([^"]+)"', init_file_path.read_text())
    # Search beta version
    beta_version_search = re.search('__version__ = "([^"]+)b[0-9]+"', init_file_path.read_text())
    if not original_version_search:
        raise RuntimeError(f"Could not find __version__ in {init_file_path}")
    original_distribution_version = original_version_search.group(1)
    packaging_version = Version(original_distribution_version)
    # Forgiving check for beta versions
    if not beta_version_search and packaging_version.base_version != str(packaging_version):
        raise RuntimeError(
            f"The package version in {init_file_path} should be `simple version` "
            f"(no suffixes) and it is `{original_distribution_version}`."
        )
    original_contents = []
    for pyproject_toml_path in pyproject_toml_paths:
        original_contents.append(pyproject_toml_path.read_text())
    update_version_in__init_py = False
    base_package_version = original_distribution_version
    if version_suffix:
        base_package_version = str(Version(original_distribution_version).base_version)
        update_version_in__init_py = True
    if update_version_in__init_py:
        set_package_version(
            f"{base_package_version}{version_suffix}",
            init_file_path=init_file_path,
            extra_text="temporarily",
        )
    for pyproject_toml_path in pyproject_toml_paths:
        update_version_suffix_in_non_provider_pyproject_toml(
            version_suffix=version_suffix,
            pyproject_toml_path=pyproject_toml_path,
        )
    try:
        yield pyproject_toml_paths
    finally:
        if update_version_in__init_py:
            set_package_version(
                original_distribution_version, init_file_path=init_file_path, extra_text="back"
            )
        for pyproject_toml_path, original_content in zip(pyproject_toml_paths, original_contents):
            get_console().print(f"[info]Restoring original content of {pyproject_toml_path}.\n")
            pyproject_toml_path.write_text(original_content)


def _get_provider_version_from_package_name(provider_package_name: str) -> str | None:
    """
    Get the current version of a provider from its pyproject.toml.

    Args:
        provider_package_name: The full package name (e.g., "apache-airflow-providers-common-compat")

    Returns:
        The version string if found, None otherwise
    """
    # Convert package name to provider path
    # apache-airflow-providers-common-compat -> common/compat
    provider_id = provider_package_name.replace("apache-airflow-providers-", "").replace("-", "/")
    provider_pyproject = AIRFLOW_PROVIDERS_ROOT_PATH / provider_id / "pyproject.toml"

    if not provider_pyproject.exists():
        get_console().print(f"[warning]Provider pyproject.toml not found: {provider_pyproject}")
        return None

    provider_toml = load_pyproject_toml(provider_pyproject)
    provider_version = provider_toml.get("project", {}).get("version")

    if not provider_version:
        get_console().print(
            f"[warning]Could not find version for {provider_package_name} in {provider_pyproject}"
        )
        return None

    return provider_version


def _update_dependency_line_with_new_version(
    line: str,
    provider_package_name: str,
    current_min_version: str,
    new_version: str,
    pyproject_file: Path,
    updates_made: dict[str, dict[str, Any]],
) -> tuple[str, bool]:
    """
    Update a dependency line with a new version and track the change.

    Returns:
        Tuple of (updated_line, was_modified)
    """
    if new_version == current_min_version:
        get_console().print(
            f"[dim]Skipping {provider_package_name} in {pyproject_file.relative_to(AIRFLOW_PROVIDERS_ROOT_PATH)}: "
            f"already at version {new_version}"
        )
        return line, False

    # Replace the version in the line
    old_constraint = f'"{provider_package_name}>={current_min_version}"'
    new_constraint = f'"{provider_package_name}>={new_version}"'
    updated_line = line.replace(old_constraint, new_constraint)

    # remove the comment starting with '# use next version' (and anything after it) and rstrip spaces
    updated_line = re.sub(r"#\s*use next version.*$", "", updated_line).rstrip()

    # Track the update
    provider_id_short = pyproject_file.parent.relative_to(AIRFLOW_PROVIDERS_ROOT_PATH)
    provider_key = str(provider_id_short)

    if provider_key not in updates_made:
        updates_made[provider_key] = {}

    updates_made[provider_key][provider_package_name] = {
        "old_version": current_min_version,
        "new_version": new_version,
        "file": str(pyproject_file),
    }

    get_console().print(
        f"[info]Updating {provider_package_name} in {pyproject_file.relative_to(AIRFLOW_PROVIDERS_ROOT_PATH)}: "
        f"{current_min_version} -> {new_version} (comment removed)"
    )

    return updated_line, True


def _process_line_with_next_version_comment(
    line: str, pyproject_file: Path, updates_made: dict[str, dict[str, Any]]
) -> tuple[str, bool]:
    """
    Process a line that contains the "# use next version" comment.

    Returns:
        Tuple of (processed_line, was_modified)
    """
    # Extract the provider package name and current version constraint
    # Format is typically: "apache-airflow-providers-xxx>=version", # use next version
    match = re.search(r'"(apache-airflow-providers-[^">=<]+)>=([^",]+)"', line)

    if not match:
        # Comment found but couldn't parse the line
        return line, False

    provider_package_name = match.group(1)
    current_min_version = match.group(2)

    # Get the current version from the referenced provider
    provider_version = _get_provider_version_from_package_name(provider_package_name)

    if not provider_version:
        return line, False

    # Update the line with the new version
    return _update_dependency_line_with_new_version(
        line, provider_package_name, current_min_version, provider_version, pyproject_file, updates_made
    )


def update_providers_with_next_version_comment() -> dict[str, dict[str, Any]]:
    """
    Scan all provider pyproject.toml files for "# use next version" comments and update the version
    of the referenced provider to the current version from that provider's pyproject.toml.

    Returns a dictionary with information about updated providers.
    """
    updates_made: dict[str, dict[str, Any]] = {}

    # Find all provider pyproject.toml files
    provider_pyproject_files = list(AIRFLOW_PROVIDERS_ROOT_PATH.glob("**/pyproject.toml"))

    for pyproject_file in provider_pyproject_files:
        content = pyproject_file.read_text()
        lines = content.split("\n")
        updated_lines = []
        file_modified = False

        for line in lines:
            # Check if line contains "# use next version" comment (but not the dependencies declaration line)
            if "# use next version" in line and "dependencies = [" not in line:
                processed_line, was_modified = _process_line_with_next_version_comment(
                    line, pyproject_file, updates_made
                )
                updated_lines.append(processed_line)
                file_modified = file_modified or was_modified
            else:
                updated_lines.append(line)

        # Write back if modified
        if file_modified:
            new_content = "\n".join(updated_lines)
            pyproject_file.write_text(new_content)
            get_console().print(
                f"[success]Updated {pyproject_file.relative_to(AIRFLOW_PROVIDERS_ROOT_PATH)}\n"
            )

    return updates_made
