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

import itertools
import json
import logging
import os
import re
import sys
from pathlib import Path
from typing import Any

from hatchling.builders.config import BuilderConfig
from hatchling.builders.hooks.plugin.interface import BuildHookInterface
from hatchling.metadata.plugin.interface import MetadataHookInterface
from packaging.requirements import Requirement
from packaging.utils import NormalizedName, canonicalize_name
from packaging.version import Version

try:
    from tomllib import loads as loads_tomllib
except ImportError:
    from tomli import loads as loads_tomllib

AIRFLOW_CORE_TOML = loads_tomllib((Path(__file__).parent / "airflow-core/pyproject.toml").read_text())

log = logging.getLogger(__name__)
log_level = logging.getLevelName(os.getenv("CUSTOM_AIRFLOW_BUILD_LOG_LEVEL", "INFO"))
log.setLevel(log_level)

AIRFLOW_ROOT_PATH = Path(__file__).parent.resolve()
AIRFLOW_INIT_PY_PATH = AIRFLOW_ROOT_PATH / "airflow-core" / "src" / "airflow" / "__init__.py"
GENERATED_PROVIDERS_DEPENDENCIES_FILE = AIRFLOW_ROOT_PATH / "generated" / "provider_dependencies.json"
PROVIDER_DEPENDENCIES = json.loads(GENERATED_PROVIDERS_DEPENDENCIES_FILE.read_text())

DEPENDENCIES = AIRFLOW_CORE_TOML["project"]["dependencies"]
OPTIONAL_DEPENDENCIES = AIRFLOW_CORE_TOML["project"]["optional-dependencies"]

PRE_INSTALLED_PROVIDERS = [
    "common.compat",
    "common.io",
    "common.sql",
    "fab",
    "smtp",
    "sqlite",
    "standard",
]


DOC_EXTRAS: dict[str, list[str]] = {
    "doc": [
        "astroid>=3",
        "checksumdir>=1.2.0",
        "click>=8.1.8",
        "docutils>=0.21",
        "sphinx-airflow-theme>=0.1.0",
        "sphinx-argparse>=0.4.0",
        "sphinx-autoapi>=3",
        "sphinx-copybutton>=0.5.2",
        "sphinx-design>=0.5.0",
        "sphinx-jinja>=2.0.2",
        "sphinx-rtd-theme>=2.0.0",
        "sphinx>=7",
        "sphinxcontrib-applehelp>=1.0.4",
        "sphinxcontrib-devhelp>=1.0.2",
        "sphinxcontrib-htmlhelp>=2.0.1",
        "sphinxcontrib-httpdomain>=1.8.1",
        "sphinxcontrib-jquery>=4.1",
        "sphinxcontrib-jsmath>=1.0.1",
        "sphinxcontrib-qthelp>=1.0.3",
        "sphinxcontrib-redoc>=1.6.0",
        "sphinxcontrib-serializinghtml>=1.1.5",
        "sphinxcontrib-spelling>=8.0.0",
    ],
    "doc-gen": [
        "apache-airflow[doc]",
        # The graphviz package creates friction when installing on MacOS as it needs graphviz system package to
        # be installed, and it's really only used for very obscure features of Airflow, so we can skip it on MacOS
        # Instead, if someone attempts to use it on MacOS, they will get explanatory error on how to install it
        "diagrams>=0.23.4; sys_platform != 'darwin'",
        "eralchemy2>=1.3.8; sys_platform != 'darwin'",
    ],
    # END OF doc extras
}


def normalize_extra(dependency_id: str) -> str:
    return dependency_id.replace(".", "-").replace("_", "-")


ALL_DYNAMIC_EXTRA_DICTS: list[tuple[dict[str, list[str]], str]] = [
    (OPTIONAL_DEPENDENCIES, "Core extras"),
    (DOC_EXTRAS, "Doc extras"),
]

ALL_DYNAMIC_EXTRAS: list[str] = sorted(
    set(
        itertools.chain(
            *[d for d, desc in ALL_DYNAMIC_EXTRA_DICTS],
            [normalize_extra(_provider_id) for _provider_id in PROVIDER_DEPENDENCIES],
        )
    )
)


def get_dependencies_including_devel(provider_id: str) -> list[str]:
    """
    Get provider dependencies including devel dependencies.

    :param provider_id: provider id
    :return: editable deps of the provider excluding airflow and including devel-deps.
    """
    deps: list[str] = PROVIDER_DEPENDENCIES[provider_id]["deps"]
    deps = [dep for dep in deps if not dep.startswith("apache-airflow>=")]
    devel_deps: list[str] = PROVIDER_DEPENDENCIES[provider_id].get("devel-deps", [])
    # for editable packages - add regular + devel dependencies retrieved from provider.yaml
    # but convert the provider dependencies to apache-airflow[extras]
    # and adding python exclusions where needed
    editable_deps = []
    for dep in itertools.chain(deps, devel_deps):
        if dep.startswith("apache-airflow-providers-"):
            dep = convert_to_extra_dependency(dep)
        editable_deps.append(dep)
    return editable_deps


def normalize_requirement(requirement: str):
    req = Requirement(requirement)
    package: NormalizedName = canonicalize_name(req.name)
    package_str = str(package)
    if req.extras:
        # Sort extras by name
        package_str += f"[{','.join(sorted([normalize_extra(extra) for extra in req.extras]))}]"
    version_required = ""
    if req.specifier:
        version_required = ",".join(map(str, sorted(req.specifier, key=lambda spec: spec.version)))
    if req.marker:
        version_required += f"; {req.marker}"
    return str(package_str + version_required)


def get_provider_id(provider_spec: str) -> str:
    """
    Extract provider id from provider specification.

    :param provider_spec: provider specification can be in the form of the "PROVIDER_ID" or
           "apache-airflow-providers-PROVIDER", optionally followed by ">=VERSION".

    :return: short provider_id with `.` instead of `-` in case of `apache` and other providers with
             `-` in the name.
    """
    _provider_id = provider_spec.split(">=")[0]
    if _provider_id.startswith("apache-airflow-providers-"):
        _provider_id = _provider_id.replace("apache-airflow-providers-", "").replace("-", ".")
    return _provider_id


def get_provider_requirement(provider_spec: str) -> str:
    """
    Convert provider specification with provider_id to provider requirement.

    The requirement can be used when constructing dependencies. It automatically adds pre-release specifier
    in case we are building pre-release version of Airflow. This way we can handle the case when airflow
    depends on specific version of the provider that has not yet been released - then we release the
    pre-release version of provider to PyPI and airflow built in CI, or Airflow pre-release version will
    automatically depend on that pre-release version of the provider.

    :param provider_spec: provider specification can be in the form of the "PROVIDER_ID" optionally followed
       by >=VERSION.
    :return: requirement for the provider that can be used as dependency.
    """
    if ">=" in provider_spec and AIRFLOW_INIT_PY_PATH.exists():
        current_airflow_version = get_current_airflow_version()
        provider_id, min_version = provider_spec.split(">=")
        provider_version = Version(min_version)
        if provider_version.is_prerelease and not current_airflow_version.is_prerelease:
            # strip pre-release version from the pre-installed provider's version when we are preparing
            # the official package
            min_version = str(provider_version.base_version)
        return f"apache-airflow-providers-{provider_id.replace('.', '-')}>={min_version}"
    else:
        return f"apache-airflow-providers-{provider_spec.replace('.', '-')}"


def get_current_airflow_version() -> Version:
    # we cannot import `airflow` here directly as it would pull re2 and a number of airflow
    # dependencies so we need to read airflow version by matching a regexp
    airflow_init_content = AIRFLOW_INIT_PY_PATH.read_text()
    airflow_version_pattern = r'__version__ = "(\d+\.\d+\.\d+\S*)"'
    airflow_version_match = re.search(airflow_version_pattern, airflow_init_content)
    if not airflow_version_match:
        raise RuntimeError(f"Cannot find Airflow version in {AIRFLOW_INIT_PY_PATH}")
    return Version(airflow_version_match.group(1))


# if providers are ready, we build provider requirements for them
PREINSTALLED_PROVIDER_REQUIREMENTS = [
    get_provider_requirement(provider_spec)
    for provider_spec in PRE_INSTALLED_PROVIDERS
    if PROVIDER_DEPENDENCIES[get_provider_id(provider_spec)]["state"] == "ready"
]

# Here we keep all pre-installed provider dependencies, so that we can add them as requirements in
# editable build to make sure that all dependencies are installed when we install Airflow in editable mode
# We need to skip apache-airflow min-versions and flag (exit) when pre-installed provider has
# dependency to another provider
ALL_PREINSTALLED_PROVIDER_DEPS: list[str] = []

for provider_spec in PRE_INSTALLED_PROVIDERS:
    _provider_id = get_provider_id(provider_spec)
    for dependency in PROVIDER_DEPENDENCIES[_provider_id]["deps"]:
        if (
            dependency.startswith("apache-airflow-providers")
            and get_provider_id(dependency) not in PRE_INSTALLED_PROVIDERS
        ):
            msg = (
                f"The provider {_provider_id} is pre-installed and it has a dependency "
                f"to another provider {dependency} which is not preinstalled. This is not allowed. "
                f"Pre-installed providers should only have 'apache-airflow', other preinstalled providers"
                f"and regular non-airflow dependencies."
            )
            raise SystemExit(msg)
        if not dependency.startswith("apache-airflow"):
            if PROVIDER_DEPENDENCIES[_provider_id]["state"] not in ["suspended", "removed"]:
                ALL_PREINSTALLED_PROVIDER_DEPS.append(dependency)

ALL_PREINSTALLED_PROVIDER_DEPS = sorted(set(ALL_PREINSTALLED_PROVIDER_DEPS))


def convert_to_extra_dependency(provider_requirement: str) -> str:
    """
    Convert provider specification to extra dependency.

    :param provider_requirement: requirement of the provider in the form of apache-airflow-provider-*,
        optionally followed by >=VERSION.
    :return: extra dependency in the form of apache-airflow[extra]
    """
    # if there is version in dependency - remove it as we do not need it in extra specification
    # for editable installation
    if ">=" in provider_requirement:
        provider_requirement = provider_requirement.split(">=")[0]
    extra = provider_requirement.replace("apache-airflow-providers-", "").replace("-", "_").replace(".", "_")
    return f"apache-airflow[{extra}]"


def get_python_exclusion(excluded_python_versions: list[str]):
    """
    Produce the Python exclusion that should be used - converted from the list of python versions.

    :param excluded_python_versions: list of python versions to exclude the dependency for.
    :return: python version exclusion string that can be added to dependency in specification.
    """
    exclusion = ""
    if excluded_python_versions:
        separator = ";"
        for version in excluded_python_versions:
            exclusion += f'{separator}python_version != "{version}"'
            separator = " and "
    return exclusion


def skip_for_editable_build(excluded_python_versions: list[str]) -> bool:
    """
    Whether the dependency should be skipped for editable build for current python version.

    :param excluded_python_versions: list of excluded python versions.
    :return: True if the dependency should be skipped for editable build for the current python version.
    """
    current_python_version = f"{sys.version_info.major}.{sys.version_info.minor}"
    if current_python_version in excluded_python_versions:
        return True
    return False


def update_optional_dependencies_with_editable_provider_deps(optional_dependencies):
    for provider_id in PROVIDER_DEPENDENCIES:
        excluded_python_versions = PROVIDER_DEPENDENCIES[provider_id].get("excluded-python-versions")
        if skip_for_editable_build(excluded_python_versions):
            continue
        normalized_extra_name = normalize_extra(provider_id)
        optional_dependencies[normalized_extra_name] = get_dependencies_including_devel(provider_id)


def update_optional_dependencies_with_standard_provider_deps(optional_dependencies) -> None:
    """
    Process all provider extras for standard wheel build.

    Processes all provider dependencies. This generates dependencies for editable builds
    and providers for wheel builds.

    """
    for provider_id in PROVIDER_DEPENDENCIES.keys():
        normalized_extra_name = normalize_extra(provider_id)
        if PROVIDER_DEPENDENCIES[provider_id]["state"] != "ready":
            if optional_dependencies.get(normalized_extra_name):
                del optional_dependencies[normalized_extra_name]
        # add providers instead of dependencies for wheel builds
        excluded_python_versions = PROVIDER_DEPENDENCIES[provider_id].get("excluded-python-versions")
        optional_dependencies[normalized_extra_name] = [
            f"apache-airflow-providers-{normalized_extra_name}"
            f"{get_python_exclusion(excluded_python_versions)}"
        ]


def get_all_core_deps() -> list[str]:
    all_core_deps: list[str] = []
    for deps in OPTIONAL_DEPENDENCIES.values():
        all_core_deps.extend(deps)
    return all_core_deps


def update_editable_optional_dependencies(optional_dependencies: dict[str, list[str]]):
    optional_dependencies.update(OPTIONAL_DEPENDENCIES)
    optional_dependencies.update(DOC_EXTRAS)
    update_optional_dependencies_with_editable_provider_deps(optional_dependencies)
    all_deps: list[str] = []
    for extra, deps in optional_dependencies.items():
        if extra == "all":
            raise RuntimeError("The 'all' extra should not be in the original optional_dependencies")
        all_deps.extend(deps)
    optional_dependencies["all"] = all_deps
    optional_dependencies["all-core"] = get_all_core_deps()


class CustomMetadataHook(MetadataHookInterface):
    """
    Custom metadata hook that updates optional dependencies and dependencies of airflow.

    Since our extras are (still) dynamic - because we need preinstalled provider requirements and we
    have to treat provider dependencies differently for editable and standard builds (including
    installing devel dependencies of the provider including the provider dependencies), we need to
    generate the optional dependencies and dependencies in the metadata hook.

    Those are the "editable" dependencies variants, because the hook is locally resolved only when
    either preparing to build the wheel, or when we install airflow in editable mode. In both
    cases we just need the "editable" dependencies, and we replace the editable dependencies in
    metadata in the build hook to reflect the changes needed in wheel.

    This whole dynamic mechanism might not be needed in the future when:

    * `doc` building will be extracted to a separate distribution with its own doc dependencies.
       Similarly to test dependencies, doc dependencies might be extracted to a separate distribution
       where we will not need to include doc dependencies in the main distribution at all. This is
       planned as part of documentation restructuring.

    * `pip` will be released with support for dependency groups - then we will be able to install
       provider devel dependencies via `dev` dependency group: https://peps.python.org/pep-0735/.
       This PEP is already approved and implemented by uv, so `uv sync` does not need to use provider
       extras at all. PIP 25.1 is supposed to release support for dependency groups. Once we switch
       to PIP 25.1, we will be able to get rid of "dynamic" dependencies for extras as they will stop being
       used at all for local development and we will be able to replace them with static
       "apache-airflow-provider" deps.

    * The default extras for python software packages will be approved and implemented in `pip` and `uv` -
       https://peps.python.org/pep-0771/. This will allow us to convert preinstalled providers into
       default extras. This might also be fixed by separating out airflow-core from the main
       airflow pyproject.toml which is planned next.
    """

    def update(self, metadata: dict) -> None:
        optional_dependencies: dict[str, list[str]] = {}
        update_editable_optional_dependencies(optional_dependencies)
        metadata["optional-dependencies"] = optional_dependencies
        dependencies: list[str] = [
            f"apache-airflow-core=={get_current_airflow_version()}",
            "apache-airflow-task-sdk",
        ]
        dependencies.extend(PREINSTALLED_PROVIDER_REQUIREMENTS)
        metadata["dependencies"] = dependencies


class CustomBuildHook(BuildHookInterface[BuilderConfig]):
    """
    Custom build hook for Airflow.

    Generates required and optional dependencies depends on the build `version`.

    - standard: Generates all dependencies for the standard (.whl) package:
       * doc extras not included
       * core extras
       * provider optional dependencies resolve to "apache-airflow-providers-{provider}"
       * pre-installed providers added as required dependencies

    - editable: Generates all dependencies for the editable installation:
       * doc extras included
       * core extras
       * provider optional dependencies resolve to provider dependencies including devel dependencies
       * pre-installed providers not included - instead their dependencies included in required dependencies
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self.optional_dependencies: dict[str, list[str]] = {}
        self._dependencies: list[str] = []

        super().__init__(*args, **kwargs)

    def initialize(self, version: str, build_data: dict[str, Any]) -> None:
        """
        Initialize hook immediately before each build.

        Any modifications to the build data will be seen by the build target.

        :param version: "standard" or "editable" build.
        :param build_data: build data dictionary.
        """
        self._dependencies = [
            f"apache-airflow-core=={get_current_airflow_version()}",
        ]
        if version == "standard":
            # Process all provider extras and replace provider requirements with providers
            for extra in OPTIONAL_DEPENDENCIES:
                self.optional_dependencies[extra] = [f"apache-airflow-core[{extra}]"]
            update_optional_dependencies_with_standard_provider_deps(self.optional_dependencies)
        else:
            update_editable_optional_dependencies(self.optional_dependencies)

        # with hatchling, we can modify dependencies dynamically by modifying the build_data
        build_data["dependencies"] = self._dependencies

        # unfortunately hatchling currently does not have a way to override optional_dependencies
        # via build_data (or so it seem) so we need to modify internal _optional_dependencies
        # field in core.metadata until this is possible
        self.metadata.core._optional_dependencies = self.optional_dependencies
