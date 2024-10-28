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
import subprocess
import sys
from collections.abc import Iterable
from enum import Enum
from functools import lru_cache
from pathlib import Path
from typing import Any, NamedTuple

from airflow_breeze.global_constants import (
    ALLOWED_PYTHON_MAJOR_MINOR_VERSIONS,
    PROVIDER_DEPENDENCIES,
    PROVIDER_RUNTIME_DATA_SCHEMA_PATH,
    REGULAR_DOC_PACKAGES,
)
from airflow_breeze.utils.console import get_console
from airflow_breeze.utils.path_utils import (
    AIRFLOW_OLD_PROVIDERS_DIR,
    AIRFLOW_PROVIDERS_NS_PACKAGE,
    BREEZE_SOURCES_ROOT,
    DOCS_ROOT,
    GENERATED_PROVIDER_PACKAGES_DIR,
    PROVIDER_DEPENDENCIES_JSON_FILE_PATH,
)
from airflow_breeze.utils.publish_docs_helpers import (
    _load_schema,
    get_provider_yaml_paths,
)
from airflow_breeze.utils.run_utils import run_command
from airflow_breeze.utils.versions import (
    get_version_tag,
    strip_leading_zeros_from_version,
)

MIN_AIRFLOW_VERSION = "2.8.0"
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
    source_date_epoch: int
    full_package_name: str
    pypi_package_name: str
    source_provider_package_path: Path
    old_source_provider_package_path: Path
    documentation_provider_package_path: Path
    changelog_path: Path
    provider_description: str
    dependencies: list[str]
    versions: list[str]
    excluded_python_versions: list[str]
    plugins: list[PluginInfo]
    removed: bool


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
            version_required = ",".join(
                map(str, sorted(req.specifier, key=lambda spec: spec.version))
            )
        if req.marker:
            version_required += f"; {req.marker}"

        return cls(package=package, version_required=version_required.strip())


PROVIDER_METADATA: dict[str, dict[str, Any]] = {}


def refresh_provider_metadata_from_yaml_file(provider_yaml_path: Path):
    import yaml

    schema = _load_schema()
    with open(provider_yaml_path) as yaml_file:
        provider = yaml.safe_load(yaml_file)
    try:
        import jsonschema

        try:
            jsonschema.validate(provider, schema=schema)
        except jsonschema.ValidationError as ex:
            msg = f"Unable to parse: {provider_yaml_path}. Original error {type(ex).__name__}: {ex}"
            raise RuntimeError(msg)
    except ImportError:
        # we only validate the schema if jsonschema is available. This is needed for autocomplete
        # to not fail with import error if jsonschema is not installed
        pass
    PROVIDER_METADATA[get_short_package_name(provider["package-name"])] = provider


def refresh_provider_metadata_with_provider_id(provider_id: str):
    provider_yaml_path = get_source_package_path(provider_id) / "provider.yaml"
    refresh_provider_metadata_from_yaml_file(provider_yaml_path)


def clear_cache_for_provider_metadata(provider_id: str):
    get_provider_packages_metadata.cache_clear()
    refresh_provider_metadata_with_provider_id(provider_id)


@lru_cache(maxsize=1)
def get_provider_packages_metadata() -> dict[str, dict[str, Any]]:
    """
    Load all data from providers files

    :return: A list containing the contents of all provider.yaml files.
    """

    if PROVIDER_METADATA:
        return PROVIDER_METADATA

    for provider_yaml_path in get_provider_yaml_paths():
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
    return get_available_packages(include_suspended=True, include_regular=False)


@lru_cache
def get_suspended_provider_folders() -> list[str]:
    return [provider_id.replace(".", "/") for provider_id in get_suspended_provider_ids()]


@lru_cache
def get_excluded_provider_ids(python_version: str) -> list[str]:
    metadata = get_provider_packages_metadata()
    return [
        provider_id
        for provider_id, provider_metadata in metadata.items()
        if python_version in provider_metadata.get("excluded-python-versions", [])
    ]


@lru_cache
def get_excluded_provider_folders(python_version: str) -> list[str]:
    return [
        provider_id.replace(".", "/")
        for provider_id in get_excluded_provider_ids(python_version)
    ]


@lru_cache
def get_removed_provider_ids() -> list[str]:
    return get_available_packages(include_removed=True, include_regular=False)


@lru_cache
def get_not_ready_provider_ids() -> list[str]:
    return get_available_packages(include_not_ready=True, include_regular=False)


def get_provider_requirements(provider_id: str) -> list[str]:
    package_metadata = get_provider_packages_metadata().get(provider_id)
    return package_metadata["dependencies"] if package_metadata else []


@lru_cache
def get_available_packages(
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
    provider_dependencies = json.loads(PROVIDER_DEPENDENCIES_JSON_FILE_PATH.read_text())

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


def expand_all_provider_packages(
    short_doc_packages: tuple[str, ...],
    include_removed: bool = False,
    include_not_ready: bool = False,
) -> tuple[str, ...]:
    """In case there are "all-providers" in the list, expand the list with all providers."""
    if "all-providers" in short_doc_packages:
        packages = [
            package for package in short_doc_packages if package != "all-providers"
        ]
        packages.extend(
            get_available_packages(
                include_removed=include_removed, include_not_ready=include_not_ready
            )
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
        long_package_name = LONG_PROVIDERS_PREFIX + "-".join(
            short_form_provider.split(".")
        )
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
        get_long_package_names(
            get_available_packages(include_non_provider_doc_packages=True)
        )
    )
    if not filters and not short_packages:
        available_doc_packages.extend(filters or ())
        return tuple(set(available_doc_packages))

    processed_package_filters = list(filters or ())
    processed_package_filters.extend(get_long_package_names(short_packages))

    removed_packages: list[str] = [
        f"apache-airflow-providers-{provider.replace('.','-')}"
        for provider in get_removed_provider_ids()
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
    return AIRFLOW_PROVIDERS_NS_PACKAGE.joinpath(*provider_id.split("."))


def get_old_source_package_path(provider_id: str) -> Path:
    return AIRFLOW_OLD_PROVIDERS_DIR.joinpath(*provider_id.split("."))


def get_documentation_package_path(provider_id: str) -> Path:
    return DOCS_ROOT / f"apache-airflow-providers-{provider_id.replace('.', '-')}"


def get_target_root_for_copied_provider_sources(provider_id: str) -> Path:
    return GENERATED_PROVIDER_PACKAGES_DIR.joinpath(*provider_id.split("."))


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


def apply_version_suffix(install_clause: str, version_suffix: str) -> str:
    if (
        install_clause.startswith("apache-airflow")
        and ">=" in install_clause
        and version_suffix
    ):
        # Applies version suffix to the apache-airflow and provider package dependencies to make
        # sure that pre-release versions have correct limits - this address the issue with how
        # pip handles pre-release versions when packages are pre-release and refer to each other - we
        # need to make sure that all our >= references for all apache-airflow packages in pre-release
        # versions of providers contain the same suffix as the provider itself.
        # For example `apache-airflow-providers-fab==2.0.0.dev0` should refer to
        # `apache-airflow>=2.9.0.dev0` and not `apache-airflow>=2.9.0` because both packages are
        # released together and >= 2.9.0 is not correct reference for 2.9.0.dev0 version of Airflow.
        prefix, version = install_clause.split(">=")
        # If version has a upper limit (e.g. ">=2.10.0,<3.0"), we need to cut this off not to fail
        if "," in version:
            version = version.split(",")[0]
        from packaging.version import Version

        base_version = Version(version).base_version
        # always use `pre-release`+ `0` as the version suffix
        version_suffix = version_suffix.rstrip("0123456789") + "0"

        target_version = Version(str(base_version) + "." + version_suffix)
        return prefix + ">=" + str(target_version)
    return install_clause


def get_install_requirements(provider_id: str, version_suffix: str) -> str:
    """
    Returns install requirements for the package.

    :param provider_id: id of the provider package
    :param version_suffix: optional version suffix for packages

    :return: install requirements of the package
    """
    if provider_id in get_removed_provider_ids():
        dependencies = get_provider_requirements(provider_id)
    else:
        dependencies = PROVIDER_DEPENDENCIES.get(provider_id)["deps"]
    install_requires = [
        apply_version_suffix(clause, version_suffix).replace('"', '\\"')
        for clause in dependencies
    ]
    return "".join(f'\n    "{ir}",' for ir in install_requires)


def get_package_extras(provider_id: str, version_suffix: str) -> dict[str, list[str]]:
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
    additional_extras = (
        provider_yaml_dict.get("additional-extras") if provider_yaml_dict else None
    )
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
    for extra, dependencies in extras_dict.items():
        extras_dict[extra] = [
            apply_version_suffix(clause, version_suffix) for clause in dependencies
        ]
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
        source_date_epoch=provider_info["source-date-epoch"],
        full_package_name=f"airflow.providers.{provider_id}",
        pypi_package_name=f"apache-airflow-providers-{provider_id.replace('.', '-')}",
        source_provider_package_path=get_source_package_path(provider_id),
        old_source_provider_package_path=get_old_source_package_path(provider_id),
        documentation_provider_package_path=get_documentation_package_path(provider_id),
        changelog_path=get_source_package_path(provider_id) / "CHANGELOG.rst",
        provider_description=provider_info["description"],
        dependencies=provider_info["dependencies"],
        versions=provider_info["versions"],
        excluded_python_versions=provider_info.get("excluded-python-versions") or [],
        plugins=plugins,
        removed=provider_info["state"] == "removed",
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
            if PackagingVersion(current_min_airflow_version) > PackagingVersion(
                MIN_AIRFLOW_VERSION
            ):
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
        pip_package_name = f"{prefix}{dependency.replace('.','-')}"
        url_suffix = f"{dependency.replace('.','-')}"
        if markdown:
            url = f"[{pip_package_name}]({base_url}{url_suffix})"
        else:
            url = f"`{pip_package_name} <{base_url}{prefix}{url_suffix}>`_"
        table_data.append((url, f"`{dependency}`" if markdown else f"``{dependency}``"))
    return tabulate(table_data, headers=headers, tablefmt="pipe" if markdown else "rst")


def get_cross_provider_dependent_packages(provider_package_id: str) -> list[str]:
    if provider_package_id in get_removed_provider_ids():
        return []
    return PROVIDER_DEPENDENCIES[provider_package_id]["cross-providers-deps"]


def get_provider_jinja_context(
    provider_id: str,
    current_release_version: str,
    version_suffix: str,
):
    provider_details = get_provider_details(provider_id=provider_id)
    release_version_no_leading_zeros = strip_leading_zeros_from_version(
        current_release_version
    )
    changelog = provider_details.changelog_path.read_text()
    supported_python_versions = [
        p
        for p in ALLOWED_PYTHON_MAJOR_MINOR_VERSIONS
        if p not in provider_details.excluded_python_versions
    ]
    cross_providers_dependencies = get_cross_provider_dependent_packages(
        provider_package_id=provider_id
    )
    context: dict[str, Any] = {
        "PROVIDER_ID": provider_details.provider_id,
        "PACKAGE_PIP_NAME": get_pip_package_name(provider_details.provider_id),
        "PACKAGE_DIST_PREFIX": get_dist_package_name_prefix(provider_details.provider_id),
        "FULL_PACKAGE_NAME": provider_details.full_package_name,
        "RELEASE": current_release_version,
        "RELEASE_NO_LEADING_ZEROS": release_version_no_leading_zeros,
        "VERSION_SUFFIX": f".{version_suffix}" if version_suffix else "",
        "PIP_REQUIREMENTS": get_provider_requirements(provider_details.provider_id),
        "PROVIDER_DESCRIPTION": provider_details.provider_description,
        "INSTALL_REQUIREMENTS": get_install_requirements(
            provider_id=provider_details.provider_id, version_suffix=version_suffix
        ),
        "EXTRAS_REQUIREMENTS": get_package_extras(
            provider_id=provider_details.provider_id, version_suffix=version_suffix
        ),
        "CHANGELOG_RELATIVE_PATH": os.path.relpath(
            provider_details.source_provider_package_path,
            provider_details.documentation_provider_package_path,
        ),
        "CHANGELOG": changelog,
        "SUPPORTED_PYTHON_VERSIONS": supported_python_versions,
        "PLUGINS": provider_details.plugins,
        "MIN_AIRFLOW_VERSION": get_min_airflow_version(provider_id),
        "PROVIDER_REMOVED": provider_details.removed,
        "PROVIDER_INFO": get_provider_info_dict(provider_id),
        "CROSS_PROVIDERS_DEPENDENCIES": get_cross_provider_dependent_packages(
            provider_id
        ),
        "CROSS_PROVIDERS_DEPENDENCIES_TABLE_RST": convert_cross_package_dependencies_to_table(
            cross_providers_dependencies, markdown=False
        ),
        "PIP_REQUIREMENTS_TABLE_RST": convert_pip_requirements_to_table(
            get_provider_requirements(provider_id), markdown=False
        ),
    }
    return context


def render_template(
    template_name: str,
    context: dict[str, Any],
    extension: str,
    autoescape: bool = True,
    keep_trailing_newline: bool = False,
) -> str:
    """
    Renders template based on its name. Reads the template from <name>_TEMPLATE.md.jinja2 in current dir.
    :param template_name: name of the template to use
    :param context: Jinja2 context
    :param extension: Target file extension
    :param autoescape: Whether to autoescape HTML
    :param keep_trailing_newline: Whether to keep the newline in rendered output
    :return: rendered template
    """
    import jinja2

    template_loader = jinja2.FileSystemLoader(
        searchpath=BREEZE_SOURCES_ROOT / "src" / "airflow_breeze" / "templates"
    )
    template_env = jinja2.Environment(
        loader=template_loader,
        undefined=jinja2.StrictUndefined,
        autoescape=autoescape,
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
        run_command(
            ["git", "remote", "get-url", HTTPS_REMOTE], text=True, capture_output=True
        )
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
            f'{" ".join(fetch_command)}\n\n'
        )
        sys.exit(1)


def convert_pip_requirements_to_table(
    requirements: Iterable[str], markdown: bool = True
) -> str:
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
            formatted_version = (
                f"`{req.version_required}`" if markdown else f"``{req.version_required}``"
            )
        table_data.append((formatted_package, formatted_version))
    return tabulate(table_data, headers=headers, tablefmt="pipe" if markdown else "rst")


def tag_exists_for_provider(provider_id: str, current_tag: str) -> bool:
    """Return true if the tag exists in the provider repository."""
    provider_details = get_provider_details(provider_id)
    result = run_command(
        ["git", "rev-parse", current_tag],
        cwd=provider_details.source_provider_package_path,
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
