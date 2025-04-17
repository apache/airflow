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
from collections.abc import Iterable
from enum import Enum
from functools import lru_cache
from pathlib import Path
from typing import Any, NamedTuple

from rich.syntax import Syntax

from airflow_breeze.global_constants import (
    ALLOWED_PYTHON_MAJOR_MINOR_VERSIONS,
    DEFAULT_PYTHON_MAJOR_MINOR_VERSION,
    PROVIDER_DEPENDENCIES,
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
    PROVIDER_DEPENDENCIES_JSON_PATH,
)
from airflow_breeze.utils.publish_docs_helpers import (
    PROVIDER_DATA_SCHEMA_PATH,
)
from airflow_breeze.utils.run_utils import run_command
from airflow_breeze.utils.version_utils import remove_local_version_suffix
from airflow_breeze.utils.versions import get_version_tag, strip_leading_zeros_from_version

MIN_AIRFLOW_VERSION = "2.9.0"
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
    original_source_provider_distribution_path: Path
    root_provider_path: Path
    base_provider_package_path: Path
    documentation_provider_distribution_path: Path
    previous_documentation_provider_distribution_path: Path
    previous_source_provider_distribution_path: Path
    changelog_path: Path
    provider_description: str
    dependencies: list[str]
    versions: list[str]
    excluded_python_versions: list[str]
    plugins: list[PluginInfo]
    removed: bool
    extra_project_metadata: str | None = None


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
            else:
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
    provider_dependencies = json.loads(PROVIDER_DEPENDENCIES_JSON_PATH.read_text())

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


# !!!! We should not remove those old/original package paths as they are used to get changes
# When documentation is generated using git_log
def get_original_source_distribution_path(provider_id: str) -> Path:
    return AIRFLOW_ORIGINAL_PROVIDERS_DIR.joinpath(*provider_id.split("."))


def get_previous_source_providers_distribution_path(provider_id: str) -> Path:
    return PREVIOUS_AIRFLOW_PROVIDERS_NS_PACKAGE_PATH.joinpath(*provider_id.split("."))


def get_previous_documentation_distribution_path(provider_id: str) -> Path:
    return DOCS_ROOT / f"apache-airflow-providers-{provider_id.replace('.', '-')}"


# End of do not remove those package paths.


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
    return version_suffix.rstrip("0123456789") + "0"


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
        target_version = Version(str(base_version) + "." + floor_version_suffix(pypi_version_suffix))
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
        original_source_provider_distribution_path=get_original_source_distribution_path(provider_id),
        previous_documentation_provider_distribution_path=get_previous_documentation_distribution_path(
            provider_id
        ),
        previous_source_provider_distribution_path=get_previous_source_providers_distribution_path(
            provider_id
        ),
        documentation_provider_distribution_path=documentation_provider_distribution_path,
        changelog_path=changelog_path,
        provider_description=provider_info["description"],
        dependencies=dependencies,
        versions=provider_info["versions"],
        excluded_python_versions=provider_info.get("excluded-python-versions", []),
        plugins=plugins,
        removed=provider_info["state"] == "removed",
        extra_project_metadata=provider_info.get("extra-project-metadata", ""),
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
    python_requires = "~=3.9"
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


def get_cross_provider_dependent_packages(provider_id: str) -> list[str]:
    if provider_id in get_removed_provider_ids():
        return []
    return PROVIDER_DEPENDENCIES[provider_id]["cross-providers-deps"]


def format_version_suffix(version_suffix: str) -> str:
    """
    Formats the version suffix by adding a dot prefix unless it is a local prefix. If no version suffix is
    passed in, an empty string is returned.

    Args:
        version_suffix (str): The version suffix to be formatted.

    Returns:
        str: The formatted version suffix.

    """
    if version_suffix:
        if version_suffix[0] == "." or version_suffix[0] == "+":
            return version_suffix
        return f".{version_suffix}"
    return ""


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

    # Most providers require the same python versions, but some may have exclusions
    requires_python_version: str = f"~={DEFAULT_PYTHON_MAJOR_MINOR_VERSION}"
    for excluded_python_version in provider_details.excluded_python_versions:
        requires_python_version += f",!={excluded_python_version}"

    context: dict[str, Any] = {
        "PROVIDER_ID": provider_details.provider_id,
        "PACKAGE_PIP_NAME": get_pip_package_name(provider_details.provider_id),
        "PACKAGE_DIST_PREFIX": get_dist_package_name_prefix(provider_details.provider_id),
        "FULL_PACKAGE_NAME": provider_details.full_package_name,
        "RELEASE": current_release_version,
        "RELEASE_NO_LEADING_ZEROS": release_version_no_leading_zeros,
        "VERSION_SUFFIX": format_version_suffix(version_suffix),
        "PIP_REQUIREMENTS": get_provider_requirements(provider_details.provider_id),
        "PROVIDER_DESCRIPTION": provider_details.provider_description,
        "CHANGELOG_RELATIVE_PATH": os.path.relpath(
            provider_details.root_provider_path,
            provider_details.documentation_provider_distribution_path,
        ),
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
    matcher = re.compile(r"(^.*\")(apache-airflow.*>=[\d.]*)(\".*)$")
    # For additional providers we want to load the dependencies and see if cross-provider-dependencies are
    # present and if not, add them to the optional dependencies
    if version_suffix:
        new_dependencies = []
        for dependency in required_dependencies:
            match = matcher.match(dependency)
            if match:
                specifier_with_version_suffix = apply_version_suffix(match.group(2), version_suffix)
                new_dependencies.append(match.group(1) + specifier_with_version_suffix + match.group(3))
            else:
                new_dependencies.append(dependency)
        required_dependencies = new_dependencies
    context["INSTALL_REQUIREMENTS"] = "\n".join(required_dependencies)
    cross_provider_ids = set(PROVIDER_DEPENDENCIES.get(provider_details.provider_id)["cross-providers-deps"])
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
