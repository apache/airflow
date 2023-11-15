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
from enum import Enum
from functools import lru_cache
from pathlib import Path
from typing import Any, Iterable, NamedTuple

from airflow_breeze.global_constants import (
    ALLOWED_PYTHON_MAJOR_MINOR_VERSIONS,
    PROVIDER_DEPENDENCIES,
    PROVIDER_RUNTIME_DATA_SCHEMA_PATH,
    REGULAR_DOC_PACKAGES,
)
from airflow_breeze.utils.console import get_console
from airflow_breeze.utils.path_utils import (
    AIRFLOW_PROVIDERS_ROOT,
    DOCS_ROOT,
    PROVIDER_DEPENDENCIES_JSON_FILE_PATH,
)
from airflow_breeze.utils.publish_docs_helpers import (
    _filepath_to_module,
    _filepath_to_system_tests,
    _load_schema,
    get_provider_yaml_paths,
)
from airflow_breeze.utils.versions import strip_leading_zeros_from_version

MIN_AIRFLOW_VERSION = "2.5.0"

LONG_PROVIDERS_PREFIX = "apache-airflow-providers-"

# TODO: use single source of truth for those
# for now we need to keep them in sync with the ones in setup.py
PREINSTALLED_PROVIDERS = [
    #   Until we cut off the 2.8.0 branch and bump current airflow version to 2.9.0, we should
    #   Keep common.io commented out in order ot be able to generate PyPI constraints because
    #   The version from PyPI has requirement of apache-airflow>=2.8.0
    #   "common.io",
    "common.sql",
    "ftp",
    "http",
    "imap",
    "sqlite",
]


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
    full_package_name: str
    pypi_package_name: str
    source_provider_package_path: Path
    documentation_provider_package_path: Path
    changelog_path: Path
    provider_description: str
    dependencies: list[str]
    versions: list[str]
    excluded_python_versions: list[str]
    plugins: list[PluginInfo]
    removed: bool


@lru_cache
def get_provider_packages_metadata() -> dict[str, dict[str, Any]]:
    """
    Load all data from providers files

    :return: A list containing the contents of all provider.yaml files.
    """
    import jsonschema
    import yaml

    schema = _load_schema()
    result: dict[str, dict[str, Any]] = {}
    for provider_yaml_path in get_provider_yaml_paths():
        with open(provider_yaml_path) as yaml_file:
            provider = yaml.safe_load(yaml_file)
        try:
            jsonschema.validate(provider, schema=schema)
        except jsonschema.ValidationError:
            raise Exception(f"Unable to parse: {provider_yaml_path}.")
        provider_yaml_dir = os.path.dirname(provider_yaml_path)
        provider["python-module"] = _filepath_to_module(provider_yaml_dir)
        provider["package-dir"] = provider_yaml_dir
        provider["system-tests-dir"] = _filepath_to_system_tests(provider_yaml_dir)
        result[get_short_package_name(provider["package-name"])] = provider
    return result


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
        get_console().print("[red]Provider info not validated against runtime schema[/]")
        raise Exception(
            "Error when validating schema. The schema must be compatible with "
            "airflow/provider_info.schema.json.",
            ex,
        )


def get_provider_info_dict(provider_id: str) -> dict[str, Any]:
    """Retrieves provider info from the provider yaml file.

    :param provider_id: package id to retrieve provider.yaml from
    :return: provider_info dictionary
    """
    provider_yaml_dict = get_provider_packages_metadata().get(provider_id)
    if provider_yaml_dict:
        validate_provider_info_with_runtime_schema(provider_yaml_dict)
    return provider_yaml_dict or {}


@lru_cache
def get_suspended_provider_ids() -> list[str]:
    return [
        provider_id
        for provider_id, provider_metadata in get_provider_packages_metadata().items()
        if provider_metadata.get("suspended", False)
    ]


@lru_cache
def get_suspended_provider_folders() -> list[str]:
    return [provider_id.replace(".", "/") for provider_id in get_suspended_provider_ids()]


@lru_cache
def get_removed_provider_ids() -> list[str]:
    return [
        provider_id
        for provider_id, provider_metadata in get_provider_packages_metadata().items()
        if provider_metadata.get("removed", False)
    ]


def get_provider_requirements(provider_id: str) -> list[str]:
    package_metadata = get_provider_packages_metadata().get(provider_id)
    return package_metadata["dependencies"] if package_metadata else []


@lru_cache
def get_available_packages(
    include_non_provider_doc_packages: bool = False, include_all_providers: bool = False
) -> list[str]:
    """
    Return provider ids for all packages that are available currently (not suspended).

    :param include_non_provider_doc_packages: whether the non-provider doc packages should be included
           (packages like apache-airflow, helm-chart, docker-stack)
    :param include_all_providers: whether "all-providers" should be included ni the list.

    """
    provider_ids: list[str] = list(json.loads(PROVIDER_DEPENDENCIES_JSON_FILE_PATH.read_text()).keys())
    available_packages = []
    if include_non_provider_doc_packages:
        available_packages.extend(REGULAR_DOC_PACKAGES)
    if include_all_providers:
        available_packages.append("all-providers")
    available_packages.extend(provider_ids)
    return available_packages


def expand_all_provider_packages(short_doc_packages: tuple[str, ...]) -> tuple[str, ...]:
    """In case there are "all-providers" in the list, expand the list with all providers."""
    if "all-providers" in short_doc_packages:
        packages = [package for package in short_doc_packages if package != "all-providers"]
        packages.extend(get_available_packages())
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
    else:
        if not long_form_provider.startswith(LONG_PROVIDERS_PREFIX):
            raise ValueError(
                f"Invalid provider name: {long_form_provider}. " f"Should start with {LONG_PROVIDERS_PREFIX}"
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
        get_long_package_names(get_available_packages(include_non_provider_doc_packages=True))
    )
    if not filters and not short_packages:
        available_doc_packages.extend(filters or ())
        return tuple(set(available_doc_packages))

    processed_package_filters = list(filters or ())
    processed_package_filters.extend(get_long_package_names(short_packages))

    removed_packages: list[str] = [
        f"apache-airflow-providers-{provider.replace('.','-')}" for provider in get_removed_provider_ids()
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


def get_source_package_path(provider_id: str) -> Path:
    return AIRFLOW_PROVIDERS_ROOT.joinpath(*provider_id.split("."))


def get_documentation_package_path(provider_id: str) -> Path:
    return DOCS_ROOT / f"apache-airflow-providers-{provider_id.replace('.', '-')}"


def get_pip_package_name(provider_id: str) -> str:
    """
    Returns PIP package name for the package id.

    :param provider_id: id of the package
    :return: the name of pip package
    """
    return "apache-airflow-providers-" + provider_id.replace(".", "-")


def get_wheel_package_name(provider_id: str) -> str:
    """
    Returns Wheel package name prefix for the package id.

    :param provider_id: id of the package
    :return: the name of wheel package prefix
    """
    return "apache_airflow_providers_" + provider_id.replace(".", "_")


def get_install_requirements(provider_id: str, version_suffix: str) -> str:
    """
    Returns install requirements for the package.

    :param provider_id: id of the provider package
    :param version_suffix: optional version suffix for packages

    :return: install requirements of the package
    """

    def apply_version_suffix(install_clause: str) -> str:
        if install_clause.startswith("apache-airflow") and ">=" in install_clause and version_suffix != "":
            # This is workaround for `pip` way of handling `--pre` installation switch. It apparently does
            # not modify the meaning of `install_requires` to include also pre-releases, so we need to
            # modify our internal provider and airflow package version references to include all pre-releases
            # including all development releases. When you specify dependency as >= X.Y.Z, and you
            # have packages X.Y.Zdev0 or X.Y.Zrc1 in a local file, such package is not considered
            # as fulfilling the requirement even if `--pre` switch is used.
            return install_clause + ".dev0"
        return install_clause

    if provider_id in get_removed_provider_ids():
        dependencies = get_provider_requirements(provider_id)
    else:
        dependencies = PROVIDER_DEPENDENCIES.get(provider_id)["deps"]
    install_requires = [apply_version_suffix(clause) for clause in dependencies]
    return "".join(f"\n    {ir}" for ir in install_requires)


def get_package_extras(provider_id: str) -> dict[str, list[str]]:
    """
    Finds extras for the package specified.

    :param provider_id: id of the package
    """
    if provider_id == "providers":
        return {}
    if provider_id in get_removed_provider_ids():
        return {}
    extras_dict: dict[str, list[str]] = {
        module: [get_pip_package_name(module)]
        for module in PROVIDER_DEPENDENCIES.get(provider_id)["cross-providers-deps"]
    }
    provider_yaml_dict = get_provider_packages_metadata().get(provider_id)
    additional_extras = provider_yaml_dict.get("additional-extras") if provider_yaml_dict else None
    if additional_extras:
        for entry in additional_extras:
            name = entry["name"]
            dependencies = entry["dependencies"]
            if name in extras_dict:
                # remove non-versioned dependencies if versioned ones are coming
                existing_dependencies = set(extras_dict[name])
                for new_dependency in dependencies:
                    for dependency in existing_dependencies:
                        # remove extra if exists as non-versioned one
                        if new_dependency.startswith(dependency):
                            extras_dict[name].remove(dependency)
                            break
                    extras_dict[name].append(new_dependency)
            else:
                extras_dict[name] = dependencies
    return extras_dict


def get_provider_details(provider_id: str) -> ProviderPackageDetails:
    provider_info = get_provider_packages_metadata().get(provider_id)
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
    return ProviderPackageDetails(
        provider_id=provider_id,
        full_package_name=f"airflow.providers.{provider_id}",
        pypi_package_name=f"apache-airflow-providers-{provider_id.replace('.', '-')}",
        source_provider_package_path=get_source_package_path(provider_id),
        documentation_provider_package_path=get_documentation_package_path(provider_id),
        changelog_path=get_source_package_path(provider_id) / "CHANGELOG.rst",
        provider_description=provider_info["description"],
        dependencies=provider_info["dependencies"],
        versions=provider_info["versions"],
        excluded_python_versions=provider_info.get("excluded-python-versions") or [],
        plugins=plugins,
        removed=provider_info.get("removed", False),
    )


def get_min_airflow_version(provider_id: str) -> str:
    from packaging.version import Version as PackagingVersion

    provider_details = get_provider_details(provider_id=provider_id)
    min_airflow_version = MIN_AIRFLOW_VERSION
    for dependency in provider_details.dependencies:
        if dependency.startswith("apache-airflow>="):
            current_min_airflow_version = dependency.split(">=")[1]
            if PackagingVersion(current_min_airflow_version) > PackagingVersion(MIN_AIRFLOW_VERSION):
                min_airflow_version = current_min_airflow_version
    return min_airflow_version


def get_python_requires(provider_id: str) -> str:
    python_requires = "~=3.8"
    provider_details = get_provider_details(provider_id=provider_id)
    for p in provider_details.excluded_python_versions:
        python_requires += f", !={p}"
    return python_requires


def get_provider_jinja_context(
    provider_id: str,
    current_release_version: str,
    version_suffix: str,
    with_breaking_changes: bool,
    maybe_with_new_features: bool,
):
    provider_details = get_provider_details(provider_id=provider_id)
    release_version_no_leading_zeros = strip_leading_zeros_from_version(current_release_version)
    changelog = provider_details.changelog_path.read_text()
    supported_python_versions = [
        p for p in ALLOWED_PYTHON_MAJOR_MINOR_VERSIONS if p not in provider_details.excluded_python_versions
    ]
    context: dict[str, Any] = {
        "WITH_BREAKING_CHANGES": with_breaking_changes,
        "MAYBE_WITH_NEW_FEATURES": maybe_with_new_features,
        "ENTITY_TYPES": list(EntityType),
        "README_FILE": "README.rst",
        "PROVIDER_ID": provider_details.provider_id,
        "PACKAGE_PIP_NAME": get_pip_package_name(provider_details.provider_id),
        "PACKAGE_WHEEL_NAME": get_wheel_package_name(provider_details.provider_id),
        "FULL_PACKAGE_NAME": provider_details.full_package_name,
        "PROVIDER_PATH": provider_details.full_package_name.replace(".", "/"),
        "RELEASE": current_release_version,
        "RELEASE_NO_LEADING_ZEROS": release_version_no_leading_zeros,
        "VERSION_SUFFIX": version_suffix or "",
        "PIP_REQUIREMENTS": get_provider_requirements(provider_details.provider_id),
        "PROVIDER_TYPE": "Provider",
        "PROVIDERS_FOLDER": "providers",
        "PROVIDER_DESCRIPTION": provider_details.provider_description,
        "INSTALL_REQUIREMENTS": get_install_requirements(
            provider_id=provider_details.provider_id, version_suffix=version_suffix
        ),
        "SETUP_REQUIREMENTS": """
    setuptools
    wheel
""",
        "EXTRAS_REQUIREMENTS": get_package_extras(provider_id=provider_details.provider_id),
        "CHANGELOG_RELATIVE_PATH": os.path.relpath(
            provider_details.source_provider_package_path,
            provider_details.documentation_provider_package_path,
        ),
        "CHANGELOG": changelog,
        "SUPPORTED_PYTHON_VERSIONS": supported_python_versions,
        "PYTHON_REQUIRES": get_python_requires(provider_id),
        "PLUGINS": provider_details.plugins,
        "MIN_AIRFLOW_VERSION": get_min_airflow_version(provider_id),
        "PREINSTALLED_PROVIDER": provider_details.provider_id in PREINSTALLED_PROVIDERS,
        "PROVIDER_REMOVED": provider_details.removed,
        "PROVIDER_INFO": get_provider_info_dict(provider_id),
    }
    return context
