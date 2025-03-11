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
from collections.abc import Iterable
from pathlib import Path
from subprocess import run
from typing import Any, Callable

from hatchling.builders.config import BuilderConfig
from hatchling.builders.hooks.plugin.interface import BuildHookInterface
from hatchling.builders.plugin.interface import BuilderInterface
from hatchling.metadata.plugin.interface import MetadataHookInterface
from hatchling.plugin.manager import PluginManager

log = logging.getLogger(__name__)
log_level = logging.getLevelName(os.getenv("CUSTOM_AIRFLOW_BUILD_LOG_LEVEL", "INFO"))
log.setLevel(log_level)

AIRFLOW_ROOT_PATH = Path(__file__).parent.resolve()
GENERATED_PROVIDERS_DEPENDENCIES_FILE = AIRFLOW_ROOT_PATH / "generated" / "provider_dependencies.json"
PROVIDER_DEPENDENCIES = json.loads(GENERATED_PROVIDERS_DEPENDENCIES_FILE.read_text())

PRE_INSTALLED_PROVIDERS = [
    "common.compat",
    "common.io",
    "common.sql",
    "fab>=1.0.2",
    "ftp",
    "http",
    "imap",
    "smtp",
    "sqlite",
    "standard",
]

# Those extras are dynamically added by hatch in the build hook to metadata optional dependencies
# when project is installed locally (editable build) or when wheel package is built based on it.
CORE_EXTRAS: dict[str, list[str]] = {
    # Aiobotocore required for AWS deferrable operators.
    # There is conflict between boto3 and aiobotocore dependency botocore.
    # TODO: We can remove it once boto3 and aiobotocore both have compatible botocore version or
    # boto3 have native aync support and we move away from aio aiobotocore
    "aiobotocore": [
        "aiobotocore>=2.9.0",
    ],
    "async": [
        "eventlet>=0.33.3",
        "gevent>=0.13",
        "greenlet>=0.4.9",
    ],
    "apache-atlas": [
        "atlasclient>=0.1.2",
    ],
    "apache-webhdfs": [
        "hdfs[avro,dataframe,kerberos]>=2.0.4",
    ],
    "cgroups": [
        # Cgroupspy 0.2.2 added Python 3.10 compatibility
        "cgroupspy>=0.2.2",
    ],
    "cloudpickle": [
        # Latest version of apache-beam requires cloudpickle~=2.2.1
        "cloudpickle>=2.2.1",
    ],
    "github-enterprise": [
        "apache-airflow[fab]",
        "authlib>=1.0.0",
    ],
    "google-auth": [
        "apache-airflow[fab]",
        "authlib>=1.0.0",
    ],
    "graphviz": [
        # The graphviz package creates friction when installing on MacOS as it needs graphviz system package to
        # be installed, and it's really only used for very obscure features of Airflow, so we can skip it on MacOS
        # Instead, if someone attempts to use it on MacOS, they will get explanatory error on how to install it
        "graphviz>=0.12; sys_platform != 'darwin'",
    ],
    "kerberos": [
        "pykerberos>=1.1.13",
        "requests-kerberos>=0.10.0",
        "thrift-sasl>=0.2.0",
    ],
    "ldap": [
        "python-ldap>=3.4.4",
    ],
    "leveldb": [
        # The plyvel package is a huge pain when installing on MacOS - especially when Apple releases new
        # OS version. It's usually next to impossible to install it at least for a few months after the new
        # MacOS version is released. We can skip it on MacOS as this is an optional feature anyway.
        "plyvel>=1.5.1; sys_platform != 'darwin'",
    ],
    "otel": [
        "opentelemetry-exporter-prometheus>=0.47b0",
    ],
    "pandas": [
        # In pandas 2.2 minimal version of the sqlalchemy is 2.0
        # https://pandas.pydata.org/docs/whatsnew/v2.2.0.html#increased-minimum-versions-for-dependencies
        # However Airflow not fully supports it yet: https://github.com/apache/airflow/issues/28723
        # In addition FAB also limit sqlalchemy to < 2.0
        "pandas>=1.2.5,<2.2",
    ],
    "password": [
        "bcrypt>=2.0.0",
        "flask-bcrypt>=0.7.1",
    ],
    "rabbitmq": [
        "amqp>=5.2.0",
    ],
    "s3fs": [
        # This is required for support of S3 file system which uses aiobotocore
        # which can have a conflict with boto3 as mentioned in aiobotocore extra
        "s3fs>=2023.10.0",
    ],
    "sentry": [
        "blinker>=1.1",
        # Sentry SDK 1.33 is broken when greenlets are installed and fails to import
        # See https://github.com/getsentry/sentry-python/issues/2473
        "sentry-sdk>=1.32.0,!=1.33.0",
    ],
    "statsd": [
        "statsd>=3.3.0",
    ],
    "uv": [
        "uv>=0.5.14",
    ],
}

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

DEPENDENCIES = [
    "a2wsgi>=1.10.8",
    # Alembic is important to handle our migrations in predictable and performant way. It is developed
    # together with SQLAlchemy. Our experience with Alembic is that it very stable in minor version
    # The 1.13.0 of alembic marked some migration code as SQLAlchemy 2+ only so we limit it to 1.13.1
    "alembic>=1.13.1, <2.0",
    "argcomplete>=1.10",
    "asgiref>=2.3.0",
    "attrs>=22.1.0",
    # Blinker use for signals in Flask, this is an optional dependency in Flask 2.2 and lower.
    # In Flask 2.3 it becomes a mandatory dependency, and flask signals are always available.
    "blinker>=1.6.2",
    "colorlog>=6.8.2",
    "configupdater>=3.1.1",
    "cron-descriptor>=1.2.24",
    "croniter>=2.0.2",
    "cryptography>=41.0.0",
    "deprecated>=1.2.13",
    "dill>=0.2.2",
    # Required for python 3.9 to work with new annotations styles. Check package
    # description on PyPI for more details: https://pypi.org/project/eval-type-backport/
    'eval-type-backport>=0.2.0;python_version<"3.10"',
    # 0.115.10 fastapi was a bad release that broke our API's and static checks.
    # Related fastapi issue here: https://github.com/fastapi/fastapi/discussions/13431
    "fastapi[standard]>=0.112.2,!=0.115.10",
    "flask-caching>=2.0.0",
    # Flask-Session 0.6 add new arguments into the SqlAlchemySessionInterface constructor as well as
    # all parameters now are mandatory which make AirflowDatabaseSessionInterface incompatible with this version.
    "flask-session>=0.4.0,<0.6",
    "flask-wtf>=1.1.0",
    # Flask 2.3 is scheduled to introduce a number of deprecation removals - some of them might be breaking
    # for our dependencies - notably `_app_ctx_stack` and `_request_ctx_stack` removals.
    # We should remove the limitation after 2.3 is released and our dependencies are updated to handle it
    "flask>=2.2.1,<2.3",
    "fsspec>=2023.10.0",
    "gitpython>=3.1.40",
    "gunicorn>=20.1.0",
    "httpx>=0.25.0",
    'importlib_metadata>=6.5;python_version<"3.12"',
    "itsdangerous>=2.0",
    "jinja2>=3.0.0",
    "jsonschema>=4.18.0",
    "lazy-object-proxy>=1.2.0",
    "libcst >=1.1.0",
    "linkify-it-py>=2.0.0",
    "lockfile>=0.12.2",
    "markdown-it-py>=2.1.0",
    "markupsafe>=1.1.1",
    "marshmallow-oneofschema>=2.0.1",
    "mdit-py-plugins>=0.3.0",
    "methodtools>=0.4.7",
    "opentelemetry-api>=1.24.0",
    "opentelemetry-exporter-otlp>=1.24.0",
    "packaging>=23.2",
    "pathspec>=0.9.0",
    'pendulum>=2.1.2,<4.0;python_version<"3.12"',
    'pendulum>=3.0.0,<4.0;python_version>="3.12"',
    "pluggy>=1.5.0",
    "psutil>=5.8.0",
    "pydantic>=2.10.2",
    # Pygments 2.19.0 improperly renders .ini files with dictionaries as values
    # See https://github.com/pygments/pygments/issues/2834
    "pygments>=2.0.1,!=2.19.0",
    "pyjwt>=2.0.0",
    "python-daemon>=3.0.0",
    "python-dateutil>=2.7.0",
    "python-nvd3>=0.15.0",
    "python-slugify>=5.0",
    # Requests 3 if it will be released, will be heavily breaking.
    "requests>=2.27.0,<3",
    "requests-toolbelt>=1.0.0",
    "rfc3339-validator>=0.1.4",
    "rich-argparse>=1.0.0",
    "rich>=13.1.0",
    "setproctitle>=1.3.3",
    # We use some deprecated features of sqlalchemy 2.0 and we should replace them before we can upgrade
    # See https://sqlalche.me/e/b8d9 for details of deprecated features
    # you can set environment variable SQLALCHEMY_WARN_20=1 to show all deprecation warnings.
    # The issue tracking it is https://github.com/apache/airflow/issues/28723
    "sqlalchemy>=1.4.49,<2.0",
    "sqlalchemy-jsonfield>=1.0",
    "sqlalchemy-utils>=0.41.2",
    "tabulate>=0.7.5",
    "tenacity>=8.0.0,!=8.2.0",
    "termcolor>=2.5.0",
    # Universal Pathlib 0.2.4 adds extra validation for Paths and our integration with local file paths
    # Does not work with it Tracked in https://github.com/fsspec/universal_pathlib/issues/276
    "universal-pathlib>=0.2.2,!=0.2.4",
    "uuid6>=2024.7.10",
    # Werkzug 3 breaks Flask-Login 0.6.2
    # we should remove this limitation when FAB supports Flask 2.3
    "werkzeug>=2.0,<3",
]


def normalize_extra(dependency_id: str) -> str:
    return dependency_id.replace(".", "-").replace("_", "-")


ALL_DYNAMIC_EXTRA_DICTS: list[tuple[dict[str, list[str]], str]] = [
    (CORE_EXTRAS, "Core extras"),
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
    from packaging.requirements import Requirement
    from packaging.utils import NormalizedName, canonicalize_name

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
    airflow_init_py_path = AIRFLOW_ROOT_PATH / "airflow" / "__init__.py"
    if ">=" in provider_spec and airflow_init_py_path.exists():
        # we cannot import `airflow` here directly as it would pull re2 and a number of airflow
        # dependencies so we need to read airflow version by matching a regexp
        airflow_init_content = airflow_init_py_path.read_text()
        airflow_version_pattern = r'__version__ = "(\d+\.\d+\.\d+\S*)"'
        airflow_version_match = re.search(airflow_version_pattern, airflow_init_content)
        if not airflow_version_match:
            raise RuntimeError(f"Cannot find Airflow version in {airflow_init_py_path}")
        from packaging.version import Version

        current_airflow_version = Version(airflow_version_match.group(1))
        provider_id, min_version = provider_spec.split(">=")
        provider_version = Version(min_version)
        if provider_version.is_prerelease and not current_airflow_version.is_prerelease:
            # strip pre-release version from the pre-installed provider's version when we are preparing
            # the official package
            min_version = str(provider_version.base_version)
        return f"apache-airflow-providers-{provider_id.replace('.', '-')}>={min_version}"
    else:
        return f"apache-airflow-providers-{provider_spec.replace('.', '-')}"


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
    for deps in CORE_EXTRAS.values():
        all_core_deps.extend(deps)
    return all_core_deps


def update_editable_optional_dependencies(optional_dependencies: dict[str, list[str]]):
    optional_dependencies.update(CORE_EXTRAS)
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
        dependencies: list[str] = []
        dependencies.extend(DEPENDENCIES)
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
        self._dependencies = DEPENDENCIES
        if version == "standard":
            # Process all provider extras and replace provider requirements with providers
            self.optional_dependencies.update(CORE_EXTRAS)
            update_optional_dependencies_with_standard_provider_deps(self.optional_dependencies)
            # Add preinstalled providers into the dependencies for standard packages
            self._dependencies.extend(PREINSTALLED_PROVIDER_REQUIREMENTS)
        else:
            update_editable_optional_dependencies(self.optional_dependencies)

        # with hatchling, we can modify dependencies dynamically by modifying the build_data
        build_data["dependencies"] = self._dependencies

        # unfortunately hatchling currently does not have a way to override optional_dependencies
        # via build_data (or so it seem) so we need to modify internal _optional_dependencies
        # field in core.metadata until this is possible
        self.metadata.core._optional_dependencies = self.optional_dependencies


class CustomBuild(BuilderInterface[BuilderConfig, PluginManager]):
    """Custom build class for Airflow assets and git version."""

    # Note that this name of the plugin MUST be `custom` - as long as we use it from custom
    # hatch_build.py file and not from external plugin. See note in the:
    # https://hatch.pypa.io/latest/plugins/build-hook/custom/#example
    PLUGIN_NAME = "custom"

    def clean(self, directory: str, versions: Iterable[str]) -> None:
        work_dir = Path(self.root)
        commands = [
            ["rm -rf airflow/ui/dist"],
            ["rm -rf airflow/ui/node_modules"],
            ["rm -rf airflow/api_fastapi/auth/managers/simple/ui/dist"],
            ["rm -rf airflow/api_fastapi/auth/managers/simple/ui/node_modules"],
        ]
        for cmd in commands:
            run(cmd, cwd=work_dir.as_posix(), check=True, shell=True)

    def get_version_api(self) -> dict[str, Callable[..., str]]:
        """Get custom build target for standard package preparation."""
        return {"standard": self.build_standard}

    def build_standard(self, directory: str, artifacts: Any, **build_data: Any) -> str:
        self.write_git_version()
        work_dir = Path(self.root)
        commands = [
            ["pre-commit run --hook-stage manual compile-ui-assets --all-files"],
        ]
        for cmd in commands:
            run(cmd, cwd=work_dir.as_posix(), check=True, shell=True)
        dist_path = work_dir / "airflow" / "ui" / "dist"
        return dist_path.resolve().as_posix()

    def get_git_version(self) -> str:
        """
        Return a version to identify the state of the underlying git repo.

        The version will indicate whether the head of the current git-backed working directory
        is tied to a release tag or not. It will indicate the former with a 'release:{version}'
        prefix and the latter with a '.dev0' suffix. Following the prefix will be a sha of the
        current branch head. Finally, a "dirty" suffix is appended to indicate that uncommitted
        changes are present.

        Example pre-release version: ".dev0+2f635dc265e78db6708f59f68e8009abb92c1e65".
        Example release version: ".release+2f635dc265e78db6708f59f68e8009abb92c1e65".
        Example modified release version: ".release+2f635dc265e78db6708f59f68e8009abb92c1e65".dirty

        :return: Found Airflow version in Git repo.
        """
        try:
            import git

            try:
                repo = git.Repo(str(Path(self.root) / ".git"))
            except git.NoSuchPathError:
                log.warning(".git directory not found: Cannot compute the git version")
                return ""
            except git.InvalidGitRepositoryError:
                log.warning("Invalid .git directory not found: Cannot compute the git version")
                return ""
        except ImportError:
            log.warning("gitpython not found: Cannot compute the git version.")
            return ""
        if repo:
            sha = repo.head.commit.hexsha
            if repo.is_dirty():
                return f".dev0+{sha}.dirty"
            # commit is clean
            return f".release:{sha}"
        return "no_git_version"

    def write_git_version(self) -> None:
        """Write git version to git_version file."""
        version = self.get_git_version()
        git_version_file = Path(self.root) / "airflow" / "git_version"
        self.app.display(f"Writing version {version} to {git_version_file}")
        git_version_file.write_text(version)
