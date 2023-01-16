#
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
"""Setup.py for the Airflow project."""
from __future__ import annotations

import glob
import json
import logging
import os
import subprocess
import sys
import unittest
from copy import deepcopy
from os.path import relpath
from pathlib import Path
from textwrap import wrap
from typing import Iterable

from setuptools import Command, Distribution, find_namespace_packages, setup
from setuptools.command.develop import develop as develop_orig
from setuptools.command.install import install as install_orig

# Setuptools patches this import to point to a vendored copy instead of the
# stdlib, which is deprecated in Python 3.10 and will be removed in 3.12.
from distutils import log  # isort: skip


# Controls whether providers are installed from packages or directly from sources
# It is turned on by default in case of development environments such as Breeze
# And it is particularly useful when you add a new provider and there is no
# PyPI version to install the provider package from
INSTALL_PROVIDERS_FROM_SOURCES = "INSTALL_PROVIDERS_FROM_SOURCES"
PY39 = sys.version_info >= (3, 9)

logger = logging.getLogger(__name__)

version = "2.6.0.dev0"

AIRFLOW_SOURCES_ROOT = Path(__file__).parent.resolve()
PROVIDERS_ROOT = AIRFLOW_SOURCES_ROOT / "airflow" / "providers"

CROSS_PROVIDERS_DEPS = "cross-providers-deps"
DEPS = "deps"


#
# NOTE! IN Airflow 2.4.+ dependencies for providers are maintained in `provider.yaml` files for each
# provider separately. They are loaded here and if you want to modify them, you need to modify
# corresponding provider.yaml file.
#
def fill_provider_dependencies() -> dict[str, dict[str, list[str]]]:
    try:
        return json.loads((AIRFLOW_SOURCES_ROOT / "generated" / "provider_dependencies.json").read_text())
    except Exception as e:
        print(f"Exception while loading provider dependencies {e}")
        # we can ignore loading dependencies when they are missing - they are only used to generate
        # correct extras when packages are build and when we install airflow from sources
        # (in both cases the provider_dependencies should be present).
        return {}


PROVIDER_DEPENDENCIES = fill_provider_dependencies()


def airflow_test_suite() -> unittest.TestSuite:
    """Test suite for Airflow tests"""
    test_loader = unittest.TestLoader()
    test_suite = test_loader.discover(str(AIRFLOW_SOURCES_ROOT / "tests"), pattern="test_*.py")
    return test_suite


class CleanCommand(Command):
    """
    Command to tidy up the project root.
    Registered as cmdclass in setup() so it can be called with ``python setup.py extra_clean``.
    """

    description = "Tidy up the project root"
    user_options: list[str] = []

    def initialize_options(self) -> None:
        """Set default values for options."""

    def finalize_options(self) -> None:
        """Set final values for options."""

    @staticmethod
    def rm_all_files(files: list[str]) -> None:
        """Remove all files from the list"""
        for file in files:
            try:
                os.remove(file)
            except Exception as e:
                logger.warning("Error when removing %s: %s", file, e)

    def run(self) -> None:
        """Remove temporary files and directories."""
        os.chdir(str(AIRFLOW_SOURCES_ROOT))
        self.rm_all_files(glob.glob("./build/*"))
        self.rm_all_files(glob.glob("./**/__pycache__/*", recursive=True))
        self.rm_all_files(glob.glob("./**/*.pyc", recursive=True))
        self.rm_all_files(glob.glob("./dist/*"))
        self.rm_all_files(glob.glob("./*.egg-info"))
        self.rm_all_files(glob.glob("./docker-context-files/*.whl"))
        self.rm_all_files(glob.glob("./docker-context-files/*.tgz"))


class CompileAssets(Command):
    """
    Compile and build the frontend assets using yarn and webpack.
    Registered as cmdclass in setup() so it can be called with ``python setup.py compile_assets``.
    """

    description = "Compile and build the frontend assets"
    user_options: list[str] = []

    def initialize_options(self) -> None:
        """Set default values for options."""

    def finalize_options(self) -> None:
        """Set final values for options."""

    def run(self) -> None:
        """Run a command to compile and build assets."""
        www_dir = AIRFLOW_SOURCES_ROOT / "airflow" / "www"
        subprocess.check_call(["yarn", "install", "--frozen-lockfile"], cwd=str(www_dir))
        subprocess.check_call(["yarn", "run", "build"], cwd=str(www_dir))


class ListExtras(Command):
    """
    List all available extras
    Registered as cmdclass in setup() so it can be called with ``python setup.py list_extras``.
    """

    description = "List available extras"
    user_options: list[str] = []

    def initialize_options(self) -> None:
        """Set default values for options."""

    def finalize_options(self) -> None:
        """Set final values for options."""

    def run(self) -> None:
        """List extras."""
        print("\n".join(wrap(", ".join(EXTRAS_DEPENDENCIES.keys()), 100)))


def git_version(version_: str) -> str:
    """
    Return a version to identify the state of the underlying git repo. The version will
    indicate whether the head of the current git-backed working directory is tied to a
    release tag or not : it will indicate the former with a 'release:{version}' prefix
    and the latter with a '.dev0' suffix. Following the prefix will be a sha of the current
    branch head. Finally, a "dirty" suffix is appended to indicate that uncommitted
    changes are present.

    :param str version_: Semver version
    :return: Found Airflow version in Git repo
    """
    try:
        import git

        try:
            repo = git.Repo(str(AIRFLOW_SOURCES_ROOT / ".git"))
        except (git.NoSuchPathError):
            logger.warning(".git directory not found: Cannot compute the git version")
            return ""
        except git.InvalidGitRepositoryError:
            logger.warning("Invalid .git directory not found: Cannot compute the git version")
            return ""
    except ImportError:
        logger.warning("gitpython not found: Cannot compute the git version.")
        return ""
    if repo:
        sha = repo.head.commit.hexsha
        if repo.is_dirty():
            return f".dev0+{sha}.dirty"
        # commit is clean
        return f".release:{version_}+{sha}"
    return "no_git_version"


def write_version(filename: str = str(AIRFLOW_SOURCES_ROOT / "airflow" / "git_version")) -> None:
    """
    Write the Semver version + git hash to file, e.g. ".dev0+2f635dc265e78db6708f59f68e8009abb92c1e65".

    :param str filename: Destination file to write.
    """
    text = f"{git_version(version)}"
    with open(filename, "w") as file:
        file.write(text)


#
# NOTE! IN Airflow 2.4.+ dependencies for providers are maintained in `provider.yaml` files for each
# provider separately. Before, the provider dependencies were kept here. THEY ARE NOT HERE ANYMORE.
#
# 'Start dependencies group' and 'Start dependencies group' are mark for ./scripts/ci/check_order_setup.py
# If you change this mark you should also change ./scripts/ci/check_order_setup.py
# Start dependencies group
async_packages = [
    # dnspython 2.3.0 is not compatible with eventlet.
    # This can be removed when the issue is resolved (reported in two places):
    # * https://github.com/eventlet/eventlet/issues/781
    # * https://datastax-oss.atlassian.net/browse/PYTHON-1320
    "dnspython<2.3.0",
    "eventlet>=0.9.7",
    "gevent>=0.13",
    "greenlet>=0.4.9",
]
atlas = [
    "atlasclient>=0.1.2",
]
celery = [
    # The Celery is known to introduce problems when upgraded to a MAJOR version. Airflow Core
    # Uses Celery for CeleryExecutor, and we also know that Kubernetes Python client follows SemVer
    # (https://docs.celeryq.dev/en/stable/contributing.html?highlight=semver#versions).
    # This is a crucial component of Airflow, so we should limit it to the next MAJOR version and only
    # deliberately bump the version when we tested it, and we know it can be bumped.
    # Bumping this version should also be connected with
    # limiting minimum airflow version supported in celery provider due to the
    # potential breaking changes in Airflow Core as well (celery is added as extra, so Airflow
    # core is not hard-limited via install-requires, only by extra).
    "celery>=5.2.3,<6"
]
cgroups = [
    # Cgroupspy 0.2.2 added Python 3.10 compatibility
    "cgroupspy>=0.2.2",
]
dask = [
    # Dask support is limited, we need Dask team to upgrade support for dask if we were to continue
    # Supporting it in the future
    "cloudpickle>=1.4.1",
    # Dask in version 2022.10.1 removed `bokeh` support and we should avoid installing it
    "dask>=2.9.0,!=2022.10.1",
    "distributed>=2.11.1",
]
deprecated_api = [
    "requests>=2.26.0",
]
doc = [
    # Astroid 2.12.* breaks documentation building
    # We can remove the limit here after https://github.com/PyCQA/astroid/issues/1708 is solved
    "astroid<2.12.0",
    "checksumdir",
    "click>=8.0",
    # Docutils 0.17.0 converts generated <div class="section"> into <section> and breaks our doc formatting
    # By adding a lot of whitespace separation. This limit can be lifted when we update our doc to handle
    # <section> tags for sections
    "docutils<0.17.0",
    "eralchemy2",
    # Without this, Sphinx goes in to a _very_ large backtrack on Python 3.7,
    # even though Sphinx 4.4.0 has this but with python_version<3.10.
    'importlib-metadata>=4.4; python_version < "3.8"',
    "sphinx-airflow-theme",
    "sphinx-argparse>=0.1.13",
    "sphinx-autoapi>=2.0.0",
    "sphinx-copybutton",
    "sphinx-jinja>=2.0",
    "sphinx-rtd-theme>=0.1.6",
    "sphinx>=5.2.0",
    "sphinxcontrib-httpdomain>=1.7.0",
    "sphinxcontrib-redoc>=1.6.0",
    "sphinxcontrib-spelling>=7.3",
]
doc_gen = [
    "eralchemy2",
]
flask_appbuilder_oauth = [
    "authlib>=1.0.0",
    # The version here should be upgraded at the same time as flask-appbuilder in setup.cfg
    "flask-appbuilder[oauth]==4.1.4",
]
kerberos = [
    "pykerberos>=1.1.13",
    "requests_kerberos>=0.10.0",
    "thrift_sasl>=0.2.0",
]
kubernetes = [
    # The Kubernetes API is known to introduce problems when upgraded to a MAJOR version. Airflow Core
    # Uses Kubernetes for Kubernetes executor, and we also know that Kubernetes Python client follows SemVer
    # (https://github.com/kubernetes-client/python#compatibility). This is a crucial component of Airflow
    # So we should limit it to the next MAJOR version and only deliberately bump the version when we
    # tested it, and we know it can be bumped. Bumping this version should also be connected with
    # limiting minimum airflow version supported in cncf.kubernetes provider, due to the
    # potential breaking changes in Airflow Core as well (kubernetes is added as extra, so Airflow
    # core is not hard-limited via install-requires, only by extra).
    "cryptography>=2.0.0",
    "kubernetes>=21.7.0,<24",
]
ldap = [
    "ldap3>=2.5.1",
    "python-ldap",
]
leveldb = ["plyvel"]
pandas = [
    "pandas>=0.17.1",
]
password = [
    "bcrypt>=2.0.0",
    "flask-bcrypt>=0.7.1",
]
rabbitmq = [
    "amqp",
]
sentry = [
    "blinker>=1.1",
    "sentry-sdk>=0.8.0",
]
statsd = [
    "statsd>=3.3.0",
]
virtualenv = [
    "virtualenv",
]
webhdfs = [
    "hdfs[avro,dataframe,kerberos]>=2.0.4",
]
# End dependencies group

# Mypy 0.900 and above ships only with stubs from stdlib so if we need other stubs, we need to install them
# manually as `types-*`. See https://mypy.readthedocs.io/en/stable/running_mypy.html#missing-imports
# for details. Wy want to install them explicitly because we want to eventually move to
# mypyd which does not support installing the types dynamically with --install-types
mypy_dependencies = [
    # TODO: upgrade to newer versions of MyPy continuously as they are released
    # Make sure to upgrade the mypy version in update-common-sql-api-stubs in .pre-commit-config.yaml
    # when you upgrade it here !!!!
    "mypy==0.971",
    "types-boto",
    "types-certifi",
    "types-croniter",
    "types-Deprecated",
    "types-docutils",
    "types-paramiko",
    "types-protobuf",
    "types-python-dateutil",
    "types-python-slugify",
    "types-pytz",
    "types-redis",
    "types-requests",
    "types-setuptools",
    "types-termcolor",
    "types-tabulate",
    "types-toml",
    "types-Markdown",
    "types-PyMySQL",
    "types-PyYAML",
]

# Dependencies needed for development only
devel_only = [
    "asynctest~=0.13",
    "aws_xray_sdk",
    "beautifulsoup4>=4.7.1",
    "black",
    "blinker",
    "bowler",
    "click>=8.0",
    "coverage",
    "filelock",
    "gitpython",
    "ipdb",
    "jira",
    "jsondiff",
    "mongomock",
    "moto[cloudformation, glue]>=4.0",
    "parameterized",
    "paramiko",
    "pipdeptree",
    "pre-commit",
    "pypsrp",
    "pygithub",
    # Pytest 7 has been released in February 2022 and we should attempt to upgrade and remove the limit
    # It contains a number of potential breaking changes but none of them looks breaking our use
    # https://docs.pytest.org/en/latest/changelog.html#pytest-7-0-0-2022-02-03
    # TODO: upgrade it and remove the limit
    "pytest~=6.0",
    "pytest-asyncio",
    "pytest-capture-warnings",
    "pytest-cov",
    "pytest-instafail",
    # We should attempt to remove the limit when we upgrade Pytest
    # TODO: remove the limit when we upgrade pytest
    "pytest-rerunfailures~=9.1",
    "pytest-timeouts",
    "pytest-xdist",
    "python-jose",
    "pywinrm",
    "qds-sdk>=1.9.6",
    "pytest-httpx",
    "requests_mock",
    "rich-click>=1.5",
    "ruff>=0.0.219",
    "semver",
    "time-machine",
    "towncrier",
    "twine",
    "wheel",
    "yamllint",
]


def get_provider_dependencies(provider_name: str) -> list[str]:
    return PROVIDER_DEPENDENCIES[provider_name][DEPS]


def get_unique_dependency_list(req_list_iterable: Iterable[list[str]]):
    _all_reqs: set[str] = set()
    for req_list in req_list_iterable:
        for req in req_list:
            _all_reqs.add(req)
    return list(_all_reqs)


devel = get_unique_dependency_list(
    [
        cgroups,
        devel_only,
        doc,
        kubernetes,
        mypy_dependencies,
        get_provider_dependencies("mysql"),
        pandas,
        password,
    ]
)

devel_hadoop = get_unique_dependency_list(
    [
        devel,
        get_provider_dependencies("apache.hdfs"),
        get_provider_dependencies("apache.hive"),
        get_provider_dependencies("apache.hdfs"),
        get_provider_dependencies("apache.hive"),
        get_provider_dependencies("apache.impala"),
        kerberos,
        get_provider_dependencies("presto"),
        webhdfs,
    ]
)

# Those are all additional extras which do not have their own 'providers'
# The 'apache.atlas' and 'apache.webhdfs' are extras that provide additional libraries
# but they do not have separate providers (yet?), they are merely there to add extra libraries
# That can be used in custom python/bash operators.
ADDITIONAL_EXTRAS_DEPENDENCIES: dict[str, list[str]] = {
    "apache.atlas": atlas,
    "apache.webhdfs": webhdfs,
}

# Those are extras that are extensions of the 'core' Airflow. They provide additional features
# To airflow core. They do not have separate providers because they do not have any operators/hooks etc.
CORE_EXTRAS_DEPENDENCIES: dict[str, list[str]] = {
    "async": async_packages,
    "celery": celery,
    "cgroups": cgroups,
    "cncf.kubernetes": kubernetes,
    "dask": dask,
    "deprecated_api": deprecated_api,
    "github_enterprise": flask_appbuilder_oauth,
    "google_auth": flask_appbuilder_oauth,
    "kerberos": kerberos,
    "ldap": ldap,
    "leveldb": leveldb,
    "pandas": pandas,
    "password": password,
    "rabbitmq": rabbitmq,
    "sentry": sentry,
    "statsd": statsd,
    "virtualenv": virtualenv,
}

EXTRAS_DEPENDENCIES: dict[str, list[str]] = deepcopy(CORE_EXTRAS_DEPENDENCIES)


def add_extras_for_all_providers() -> None:
    for (provider_name, provider_dict) in PROVIDER_DEPENDENCIES.items():
        EXTRAS_DEPENDENCIES[provider_name] = provider_dict[DEPS]


def add_additional_extras() -> None:
    for (extra_name, extra_dependencies) in ADDITIONAL_EXTRAS_DEPENDENCIES.items():
        EXTRAS_DEPENDENCIES[extra_name] = extra_dependencies


add_extras_for_all_providers()
add_additional_extras()

#############################################################################################################
#  The whole section can be removed in Airflow 3.0 as those old aliases are deprecated in 2.* series
#############################################################################################################

# Dictionary of aliases from 1.10 - deprecated in Airflow 2.*
EXTRAS_DEPRECATED_ALIASES: dict[str, str] = {
    "atlas": "apache.atlas",
    "aws": "amazon",
    "azure": "microsoft.azure",
    "cassandra": "apache.cassandra",
    "crypto": "",  # this is legacy extra - all dependencies are already "install-requires"
    "druid": "apache.druid",
    "gcp": "google",
    "gcp_api": "google",
    "hdfs": "apache.hdfs",
    "hive": "apache.hive",
    "kubernetes": "cncf.kubernetes",
    "mssql": "microsoft.mssql",
    "pinot": "apache.pinot",
    "qds": "qubole",
    "s3": "amazon",
    "spark": "apache.spark",
    "webhdfs": "apache.webhdfs",
    "winrm": "microsoft.winrm",
}

EXTRAS_DEPRECATED_ALIASES_NOT_PROVIDERS: list[str] = [
    "crypto",
    "webhdfs",
]

EXTRAS_DEPRECATED_ALIASES_IGNORED_FROM_REF_DOCS: list[str] = [
    "jira",
]


def add_extras_for_all_deprecated_aliases() -> None:
    """
    Add extras for all deprecated aliases. Requirements for those deprecated aliases are the same
    as the extras they are replaced with.
    The dependencies are not copies - those are the same lists as for the new extras. This is intended.
    Thanks to that if the original extras are later extended with providers, aliases are extended as well.
    """
    for alias, extra in EXTRAS_DEPRECATED_ALIASES.items():
        dependencies = EXTRAS_DEPENDENCIES.get(extra) if extra != "" else []
        if dependencies is None:
            raise Exception(f"The extra {extra} is missing for deprecated alias {alias}")
        EXTRAS_DEPENDENCIES[alias] = dependencies


def add_all_deprecated_provider_packages() -> None:
    """
    For deprecated aliases that are providers, we will swap the providers dependencies to instead
    be the provider itself.

    e.g. {"kubernetes": ["kubernetes>=3.0.0, <12.0.0", ...]} becomes
    {"kubernetes": ["apache-airflow-provider-cncf-kubernetes"]}
    """
    for alias, provider in EXTRAS_DEPRECATED_ALIASES.items():
        if alias in EXTRAS_DEPRECATED_ALIASES_NOT_PROVIDERS:
            continue
        replace_extra_dependencies_with_provider_packages(alias, [provider])


add_extras_for_all_deprecated_aliases()

#############################################################################################################
#  End of deprecated section
#############################################################################################################

# This is list of all providers. It's a shortcut for anyone who would like to easily get list of
# All providers. It is used by pre-commits.
ALL_PROVIDERS = list(PROVIDER_DEPENDENCIES.keys())

ALL_DB_PROVIDERS = [
    "apache.cassandra",
    "apache.drill",
    "apache.druid",
    "apache.hdfs",
    "apache.hive",
    "apache.impala",
    "apache.pinot",
    "arangodb",
    "cloudant",
    "databricks",
    "exasol",
    "influxdb",
    "microsoft.mssql",
    "mongo",
    "mysql",
    "neo4j",
    "postgres",
    "presto",
    "trino",
    "vertica",
]


def get_all_db_dependencies() -> list[str]:
    _all_db_reqs: set[str] = set()
    for provider in ALL_DB_PROVIDERS:
        for req in PROVIDER_DEPENDENCIES[provider][DEPS]:
            _all_db_reqs.add(req)
    return list(_all_db_reqs)


# Special dependencies for all database-related providers. They are de-duplicated.
all_dbs = get_all_db_dependencies()

# All db user extras here
EXTRAS_DEPENDENCIES["all_dbs"] = all_dbs

# Requirements for all "user" extras (no devel). They are de-duplicated. Note that we do not need
# to separately add providers dependencies - they have been already added as 'providers' extras above
_all_dependencies = get_unique_dependency_list(EXTRAS_DEPENDENCIES.values())

_all_dependencies_without_airflow_providers = list(
    filter(lambda k: "apache-airflow-" not in k, _all_dependencies)
)

# All user extras here
# all is purely development extra and it should contain only direct dependencies of Airflow
# It should contain all dependencies of airflow and dependencies of all community providers,
# but not the providers themselves
EXTRAS_DEPENDENCIES["all"] = _all_dependencies_without_airflow_providers

# This can be simplified to devel_hadoop + _all_dependencies due to inclusions
# but we keep it for explicit sake. We are de-duplicating it anyway.
devel_all = get_unique_dependency_list(
    [_all_dependencies_without_airflow_providers, doc, doc_gen, devel, devel_hadoop]
)

# Those are packages excluded for "all" dependencies
PACKAGES_EXCLUDED_FOR_ALL = []
PACKAGES_EXCLUDED_FOR_ALL.extend(["snakebite"])


def is_package_excluded(package: str, exclusion_list: list[str]) -> bool:
    """
    Checks if package should be excluded.

    :param package: package name (beginning of it)
    :param exclusion_list: list of excluded packages
    :return: true if package should be excluded
    """
    return any(package.startswith(excluded_package) for excluded_package in exclusion_list)


def remove_provider_limits(package: str) -> str:
    """
    Removes the limit for providers in devel_all to account for pre-release and development packages.

    :param package: package name (beginning of it)
    :return: true if package should be excluded
    """
    return (
        package.split(">=")[0]
        if package.startswith("apache-airflow-providers") and ">=" in package
        else package
    )


devel = [remove_provider_limits(package) for package in devel]
devel_all = [
    remove_provider_limits(package)
    for package in devel_all
    if not is_package_excluded(package=package, exclusion_list=PACKAGES_EXCLUDED_FOR_ALL)
]
devel_hadoop = [remove_provider_limits(package) for package in devel_hadoop]
devel_ci = devel_all


# Those are extras that we have to add for development purposes
# They can be use to install some predefined set of dependencies.
EXTRAS_DEPENDENCIES["doc"] = doc
EXTRAS_DEPENDENCIES["doc_gen"] = doc_gen
EXTRAS_DEPENDENCIES["devel"] = devel  # devel already includes doc
EXTRAS_DEPENDENCIES["devel_hadoop"] = devel_hadoop  # devel_hadoop already includes devel
EXTRAS_DEPENDENCIES["devel_all"] = devel_all
EXTRAS_DEPENDENCIES["devel_ci"] = devel_ci


def sort_extras_dependencies() -> dict[str, list[str]]:
    """
    The dictionary order remains when keys() are retrieved.
    Sort both: extras and list of dependencies to make it easier to analyse problems
    external packages will be first, then if providers are added they are added at the end of the lists.
    """
    sorted_dependencies: dict[str, list[str]] = {}
    sorted_extra_ids = sorted(EXTRAS_DEPENDENCIES.keys())
    for extra_id in sorted_extra_ids:
        sorted_dependencies[extra_id] = sorted(EXTRAS_DEPENDENCIES[extra_id])
    return sorted_dependencies


EXTRAS_DEPENDENCIES = sort_extras_dependencies()

# Those providers are pre-installed always when airflow is installed.
# Those providers do not have dependency on airflow2.0 because that would lead to circular dependencies.
# This is not a problem for PIP but some tools (pipdeptree) show those as a warning.
PREINSTALLED_PROVIDERS = [
    "common.sql",
    "ftp",
    "http",
    "imap",
    "sqlite",
]


def get_provider_package_name_from_package_id(package_id: str) -> str:
    """
    Builds the name of provider package out of the package id provided/

    :param package_id: id of the package (like amazon or microsoft.azure)
    :return: full name of package in PyPI
    """
    package_suffix = package_id.replace(".", "-")
    return f"apache-airflow-providers-{package_suffix}"


def get_excluded_providers() -> list[str]:
    """Returns packages excluded for the current python version."""
    return []


def get_all_provider_packages() -> str:
    """Returns all provider packages configured in setup.py"""
    excluded_providers = get_excluded_providers()
    return " ".join(
        get_provider_package_name_from_package_id(package)
        for package in ALL_PROVIDERS
        if package not in excluded_providers
    )


class AirflowDistribution(Distribution):
    """The setuptools.Distribution subclass with Airflow specific behaviour"""

    def __init__(self, attrs=None):
        super().__init__(attrs)
        self.install_requires = None

    def parse_config_files(self, *args, **kwargs) -> None:
        """
        Ensure that when we have been asked to install providers from sources
        that we don't *also* try to install those providers from PyPI.
        Also we should make sure that in this case we copy provider.yaml files so that
        Providers manager can find package information.
        """
        super().parse_config_files(*args, **kwargs)
        if os.getenv(INSTALL_PROVIDERS_FROM_SOURCES) == "true":
            self.install_requires = [
                req for req in self.install_requires if not req.startswith("apache-airflow-providers-")
            ]
            provider_yaml_files = glob.glob("airflow/providers/**/provider.yaml", recursive=True)
            for provider_yaml_file in provider_yaml_files:
                provider_relative_path = relpath(provider_yaml_file, str(AIRFLOW_SOURCES_ROOT / "airflow"))
                self.package_data["airflow"].append(provider_relative_path)
        else:
            self.install_requires.extend(
                [
                    get_provider_package_name_from_package_id(package_id)
                    for package_id in PREINSTALLED_PROVIDERS
                ]
            )


def replace_extra_dependencies_with_provider_packages(extra: str, providers: list[str]) -> None:
    """
    Replaces extra dependencies with provider package. The intention here is that when
    the provider is added as dependency of extra, there is no need to add the dependencies
    separately. This is not needed and even harmful, because in case of future versions of
    the provider, the dependencies might change, so hard-coding dependencies from the version
    that was available at the release time might cause dependency conflicts in the future.

    Say for example that you have salesforce provider with those deps:

    { 'salesforce': ['simple-salesforce>=1.0.0', 'tableauserverclient'] }

    Initially ['salesforce'] extra has those dependencies, and it works like that when you install
    it when INSTALL_PROVIDERS_FROM_SOURCES is set to `true` (during the development). However, when
    the production installation is used, The dependencies are changed:

    { 'salesforce': ['apache-airflow-providers-salesforce'] }

    And then, 'apache-airflow-providers-salesforce' package has those 'install_requires' dependencies:
            ['simple-salesforce>=1.0.0', 'tableauserverclient']

    So transitively 'salesforce' extra has all the dependencies it needs and in case the provider
    changes its dependencies, they will transitively change as well.

    In the constraint mechanism we save both - provider versions and it's dependencies
    version, which means that installation using constraints is repeatable.

    For K8s and Celery which are both "Core executors" and "Providers" we have to
    add the base dependencies to core as well, in order to mitigate problems where
    newer version of provider will have less strict limits. This should be done for both
    extras and their deprecated aliases. This is not a full protection however, the way
    extras work, this will not add "hard" limits for Airflow and the user who does not use
    constraints.

    :param extra: Name of the extra to add providers to
    :param providers: list of provider ids
    """
    if extra in ["cncf.kubernetes", "kubernetes", "celery"]:
        EXTRAS_DEPENDENCIES[extra].extend(
            [get_provider_package_name_from_package_id(package_name) for package_name in providers]
        )
    elif extra == "apache.hive":
        # We moved the hive macros to the hive provider, and they are available in hive provider only as of
        # 5.1.0 version only, so we have to make sure minimum version is used
        EXTRAS_DEPENDENCIES[extra] = ["apache-airflow-providers-hive>=5.1.0"]
    else:
        EXTRAS_DEPENDENCIES[extra] = [
            get_provider_package_name_from_package_id(package_name) for package_name in providers
        ]


def add_provider_packages_to_extra_dependencies(extra: str, providers: list[str]) -> None:
    """
    Adds provider packages as dependencies to extra. This is used to add provider packages as dependencies
    to the "bulk" kind of extras. Those bulk extras do not have the detailed 'extra' dependencies as
    initial values, so instead of replacing them (see previous function) we can extend them.

    :param extra: Name of the extra to add providers to
    :param providers: list of provider ids
    """
    EXTRAS_DEPENDENCIES[extra].extend(
        [get_provider_package_name_from_package_id(package_name) for package_name in providers]
    )


def add_all_provider_packages() -> None:
    """
    In case of regular installation (providers installed from packages), we should add extra dependencies to
    Airflow - to get the providers automatically installed when those extras are installed.

    For providers installed from sources we skip that step. That helps to test and install airflow with
    all packages in CI - for example when new providers are added, otherwise the installation would fail
    as the new provider is not yet in PyPI.

    """
    for provider_id in ALL_PROVIDERS:
        replace_extra_dependencies_with_provider_packages(provider_id, [provider_id])
    add_provider_packages_to_extra_dependencies("all", ALL_PROVIDERS)
    add_provider_packages_to_extra_dependencies("devel_ci", ALL_PROVIDERS)
    add_provider_packages_to_extra_dependencies("devel_all", ALL_PROVIDERS)
    add_provider_packages_to_extra_dependencies("all_dbs", ALL_DB_PROVIDERS)
    add_provider_packages_to_extra_dependencies(
        "devel_hadoop", ["apache.hdfs", "apache.hive", "presto", "trino"]
    )
    add_all_deprecated_provider_packages()


class Develop(develop_orig):
    """Forces removal of providers in editable mode."""

    def run(self) -> None:  # type: ignore
        self.announce("Installing in editable mode. Uninstalling provider packages!", level=log.INFO)
        # We need to run "python3 -m pip" because it might be that older PIP binary is in the path
        # And it results with an error when running pip directly (cannot import pip module)
        # also PIP does not have a stable API so we have to run subprocesses ¯\_(ツ)_/¯
        try:
            installed_packages = (
                subprocess.check_output(["python3", "-m", "pip", "freeze"]).decode().splitlines()
            )
            airflow_provider_packages = [
                package_line.split("=")[0]
                for package_line in installed_packages
                if package_line.startswith("apache-airflow-providers")
            ]
            self.announce(f"Uninstalling ${airflow_provider_packages}!", level=log.INFO)
            subprocess.check_call(["python3", "-m", "pip", "uninstall", "--yes", *airflow_provider_packages])
        except subprocess.CalledProcessError as e:
            self.announce(f"Error when uninstalling airflow provider packages: {e}!", level=log.WARN)
        super().run()


class Install(install_orig):
    """Forces installation of providers from sources in editable mode."""

    def run(self) -> None:
        self.announce("Standard installation. Providers are installed from packages", level=log.INFO)
        super().run()


def do_setup() -> None:
    """
    Perform the Airflow package setup.

    Most values come from setup.cfg, only the dynamically calculated ones are passed to setup
    function call. See https://setuptools.readthedocs.io/en/latest/userguide/declarative_config.html
    """
    setup_kwargs = {}

    def include_provider_namespace_packages_when_installing_from_sources() -> None:
        """
        When installing providers from sources we install all namespace packages found below airflow,
        including airflow and provider packages, otherwise defaults from setup.cfg control this.
        The kwargs in setup() call override those that are specified in setup.cfg.
        """
        if os.getenv(INSTALL_PROVIDERS_FROM_SOURCES) == "true":
            setup_kwargs["packages"] = find_namespace_packages(include=["airflow*"])

    include_provider_namespace_packages_when_installing_from_sources()
    if os.getenv(INSTALL_PROVIDERS_FROM_SOURCES) == "true":
        print("Installing providers from sources. Skip adding providers as dependencies")
    else:
        add_all_provider_packages()

    write_version()
    setup(
        distclass=AirflowDistribution,
        version=version,
        extras_require=EXTRAS_DEPENDENCIES,
        download_url=("https://archive.apache.org/dist/airflow/" + version),
        cmdclass={
            "extra_clean": CleanCommand,
            "compile_assets": CompileAssets,
            "list_extras": ListExtras,
            "install": Install,  # type: ignore
            "develop": Develop,
        },
        test_suite="setup.airflow_test_suite",
        **setup_kwargs,  # type: ignore
    )


if __name__ == "__main__":
    do_setup()  # comment
