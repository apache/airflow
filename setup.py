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
# To make sure the CI build is using "upgrade to newer dependencies", which is useful when you want to check
# if the dependencies are still compatible with the latest versions as they seem to break some unrelated
# tests in main, you can modify this file. The modification can be simply modifying this particular comment.
# e.g. you can modify the following number "00001" to something else to trigger it.
from __future__ import annotations

import glob
import json
import logging
import os
import subprocess
import sys
import textwrap
import unittest
from copy import deepcopy
from pathlib import Path
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

AIRFLOW_SOURCES_ROOT = Path(__file__).parent.resolve()
PROVIDERS_ROOT = AIRFLOW_SOURCES_ROOT / "airflow" / "providers"

CROSS_PROVIDERS_DEPS = "cross-providers-deps"
DEPS = "deps"
CURRENT_PYTHON_VERSION = f"{sys.version_info.major}.{sys.version_info.minor}"


def apply_pypi_suffix_to_airflow_packages(dependencies: list[str]) -> None:
    """
    Apply version suffix to dependencies that do not have one.

    Looks through the list of dependencies, finds which one are airflow or airflow providers packages
    and applies the version suffix to those of them that do not have the suffix applied yet.

    :param dependencies: list of dependencies to add suffix to
    """
    for i in range(len(dependencies)):
        dependency = dependencies[i]
        if dependency.startswith("apache-airflow"):
            # in case we want to depend on other airflow package, the chance is the package
            # has not yet been released to PyPI and we only see it as a local package that is
            # being installed with .dev0 suffix in CI. Unfortunately, there is no way in standard
            # PEP-440 compliant way to specify version that would be both - releasable, and
            # testable to install on CI with .dev0 or .rc suffixes. We could add `--pre` flag to
            # enable it, but `--pre` flag is not selective and will work for all packages so
            # we would automatically install all "pre-release" packages for all packages that
            # we install from PyPI - and this is definitely not what we want. So in order to
            # install only airflow packages that are available in sources in .dev0 or .rc version
            # we need to dynamically modify the dependencies here.
            if ">=" in dependency:
                package, version = dependency.split(">=")
                version_spec = f">={version}"
                version_suffix = os.environ.get("VERSION_SUFFIX_FOR_PYPI")
                if version_suffix and version_suffix not in version_spec:
                    version_spec += version_suffix
                dependencies[i] = f"{package}{version_spec}"


# NOTE! IN Airflow 2.4.+ dependencies for providers are maintained in `provider.yaml` files for each
# provider separately. They are loaded here and if you want to modify them, you need to modify
# corresponding provider.yaml file.
#
def fill_provider_dependencies() -> dict[str, dict[str, list[str]]]:
    # in case we are loading setup from pre-commits, we want to skip the check for python version
    # because if someone uses a version of Python where providers are excluded, the setup will fail
    # to see the extras for those providers
    skip_python_version_check = os.environ.get("_SKIP_PYTHON_VERSION_CHECK")
    try:
        with AIRFLOW_SOURCES_ROOT.joinpath("generated", "provider_dependencies.json").open() as f:
            dependencies = json.load(f)
        provider_dict = {}
        for key, value in dependencies.items():
            if value.get(DEPS):
                apply_pypi_suffix_to_airflow_packages(value[DEPS])
            if CURRENT_PYTHON_VERSION not in value["excluded-python-versions"] or skip_python_version_check:
                provider_dict[key] = value
        return provider_dict
    except Exception as e:
        print(f"Exception while loading provider dependencies {e}")
        # we can ignore loading dependencies when they are missing - they are only used to generate
        # correct extras when packages are build and when we install airflow from sources
        # (in both cases the provider_dependencies should be present).
        return {}


PROVIDER_DEPENDENCIES = fill_provider_dependencies()


def airflow_test_suite() -> unittest.TestSuite:
    """Test suite for Airflow tests."""
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
        """Remove all files from the list."""
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
    List all available extras.

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
        print("\n".join(textwrap.wrap(", ".join(EXTRAS_DEPENDENCIES.keys()), 100)))


def git_version() -> str:
    """
    Return a version to identify the state of the underlying git repo.

    The version will indicate whether the head of the current git-backed working directory
    is tied to a release tag or not : it will indicate the former with a 'release:{version}'
    prefix and the latter with a '.dev0' suffix. Following the prefix will be a sha of the
    current branch head. Finally, a "dirty" suffix is appended to indicate that uncommitted
    changes are present.

    :return: Found Airflow version in Git repo
    """
    try:
        import git

        try:
            repo = git.Repo(str(AIRFLOW_SOURCES_ROOT / ".git"))
        except git.NoSuchPathError:
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
        return f".release:{sha}"
    return "no_git_version"


def write_version(filename: str = str(AIRFLOW_SOURCES_ROOT / "airflow" / "git_version")) -> None:
    """
    Write the Semver version + git hash to file, e.g. ".dev0+2f635dc265e78db6708f59f68e8009abb92c1e65".

    :param str filename: Destination file to write.
    """
    text = git_version()
    with open(filename, "w") as file:
        file.write(text)


#
# NOTE! IN Airflow 2.4.+ dependencies for providers are maintained in `provider.yaml` files for each
# provider separately. Before, the provider dependencies were kept here. THEY ARE NOT HERE ANYMORE.
#
# 'Start dependencies group' and 'End dependencies group' are marks for ./scripts/ci/check_order_setup.py
# If you change these marks you should also change ./scripts/ci/check_order_setup.py
# Start dependencies group
async_packages = [
    "eventlet>=0.33.3",
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
    "celery>=5.3.0,<6"
]
cgroups = [
    # Cgroupspy 0.2.2 added Python 3.10 compatibility
    "cgroupspy>=0.2.2",
]
dask = [
    # Dask support is limited, we need Dask team to upgrade support for dask if we were to continue
    # Supporting it in the future
    "cloudpickle>=1.4.1",
    # Dask and distributed in version 2023.5.0 break our tests for Python > 3.7
    # See https://github.com/dask/dask/issues/10279
    "dask>=2.9.0,!=2022.10.1,!=2023.5.0",
    "distributed>=2.11.1,!=2023.5.0",
]
deprecated_api = [
    "requests>=2.26.0",
]
doc = [
    # sphinx-autoapi fails with astroid 3.0, see: https://github.com/readthedocs/sphinx-autoapi/issues/407
    # This was fixed in sphinx-autoapi 3.0, however it has requirement sphinx>=6.1, but we stuck on 5.x
    "astroid>=2.12.3, <3.0",
    "checksumdir",
    # click 8.1.4 and 8.1.5 generate mypy errors due to typing issue in the upstream package:
    # https://github.com/pallets/click/issues/2558
    "click>=8.0,!=8.1.4,!=8.1.5",
    # Docutils 0.17.0 converts generated <div class="section"> into <section> and breaks our doc formatting
    # By adding a lot of whitespace separation. This limit can be lifted when we update our doc to handle
    # <section> tags for sections
    "docutils<0.17.0",
    "eralchemy2",
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
    "flask-appbuilder[oauth]==4.3.9",
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
otel = ["opentelemetry-exporter-prometheus"]
pandas = ["pandas>=0.17.1", "pyarrow>=9.0.0"]
password = [
    "bcrypt>=2.0.0",
    "flask-bcrypt>=0.7.1",
]
rabbitmq = [
    "amqp",
]
sentry = [
    "blinker>=1.1",
    # Sentry SDK 1.33 is broken when greenlets are installed and fails to import
    # See https://github.com/getsentry/sentry-python/issues/2473
    "sentry-sdk>=1.32.0,!=1.33.0",
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
# for details. We want to install them explicitly because we want to eventually move to
# mypyd which does not support installing the types dynamically with --install-types
mypy_dependencies = [
    # TODO: upgrade to newer versions of MyPy continuously as they are released
    # Make sure to upgrade the mypy version in update-common-sql-api-stubs in .pre-commit-config.yaml
    # when you upgrade it here !!!!
    "mypy==1.2.0",
    "types-aiofiles",
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

# make sure to update providers/amazon/provider.yaml botocore min version when you update it here
_MIN_BOTO3_VERSION = "1.28.0"

_devel_only_amazon = [
    "aws_xray_sdk",
    "moto[cloudformation,glue]>=4.2.5",
    f"mypy-boto3-rds>={_MIN_BOTO3_VERSION}",
    f"mypy-boto3-redshift-data>={_MIN_BOTO3_VERSION}",
    f"mypy-boto3-s3>={_MIN_BOTO3_VERSION}",
    f"mypy-boto3-appflow>={_MIN_BOTO3_VERSION}",
]

_devel_only_azure = [
    "pywinrm",
]

_devel_only_breeze = [
    "filelock",
]

_devel_only_debuggers = [
    "ipdb",
]

_devel_only_deltalake = [
    "deltalake>=0.12.0",
]

_devel_only_devscripts = [
    "click>=8.0",
    "gitpython",
    "pipdeptree",
    "pygithub",
    "rich-click>=1.7.0",
    "restructuredtext-lint",
    "semver",
    "towncrier",
    "twine",
    "wheel",
]

_devel_only_duckdb = [
    "duckdb>=0.9.0",
]

_devel_only_mongo = [
    "mongomock",
]

_devel_only_iceberg = [
    "pyiceberg>=0.5.0",
]

_devel_only_sentry = [
    "blinker",
]

_devel_only_static_checks = [
    "pre-commit",
    "black",
    "ruff>=0.0.219",
    "yamllint",
]

_devel_only_tests = [
    "aioresponses",
    "backports.zoneinfo>=0.2.1;python_version<'3.9'",
    "beautifulsoup4>=4.7.1",
    "coverage>=7.2",
    "pytest",
    "pytest-asyncio",
    "pytest-cov",
    "pytest-httpx",
    "pytest-icdiff",
    "pytest-instafail",
    "pytest-mock",
    "pytest-rerunfailures",
    "pytest-timeouts",
    "pytest-xdist",
    "requests_mock",
    "time-machine",
]

# Dependencies needed for development only
devel_only = [
    *_devel_only_amazon,
    *_devel_only_azure,
    *_devel_only_breeze,
    *_devel_only_debuggers,
    *_devel_only_deltalake,
    *_devel_only_devscripts,
    *_devel_only_duckdb,
    *_devel_only_mongo,
    *_devel_only_iceberg,
    *_devel_only_sentry,
    *_devel_only_static_checks,
    *_devel_only_tests,
]

aiobotocore = [
    # This required for AWS deferrable operators.
    # There is conflict between boto3 and aiobotocore dependency botocore.
    # TODO: We can remove it once boto3 and aiobotocore both have compatible botocore version or
    # boto3 have native aync support and we move away from aio aiobotocore
    "aiobotocore>=2.1.1",
]

s3fs = [
    # This is required for support of S3 file system which uses aiobotocore
    # which can have a conflict with boto3 as mentioned above
    "s3fs>=2023.9.2",
]


def get_provider_dependencies(provider_name: str) -> list[str]:
    if provider_name not in PROVIDER_DEPENDENCIES:
        return []
    return PROVIDER_DEPENDENCIES[provider_name][DEPS]


def get_unique_dependency_list(req_list_iterable: Iterable[list[str]]):
    _all_reqs: set[str] = set()
    for req_list in req_list_iterable:
        for req in req_list:
            _all_reqs.add(req)
    return list(_all_reqs)


devel = get_unique_dependency_list(
    [
        aiobotocore,
        cgroups,
        devel_only,
        doc,
        kubernetes,
        mypy_dependencies,
        get_provider_dependencies("mysql"),
        pandas,
        password,
        s3fs,
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
    "aiobotocore": aiobotocore,
    "async": async_packages,
    "celery": celery,  # TODO: remove and move to a regular provider package in a separate PR
    "cgroups": cgroups,
    "cncf.kubernetes": kubernetes,  # TODO: remove and move to a regular provider package in a separate PR
    "dask": dask,  # TODO: remove and move to a provider package in a separate PR
    "deprecated_api": deprecated_api,
    "github_enterprise": flask_appbuilder_oauth,
    "google_auth": flask_appbuilder_oauth,
    "kerberos": kerberos,
    "ldap": ldap,
    "leveldb": leveldb,
    "otel": otel,
    "pandas": pandas,
    "password": password,
    "rabbitmq": rabbitmq,
    "s3fs": s3fs,
    "sentry": sentry,
    "statsd": statsd,
    "virtualenv": virtualenv,
}


def filter_out_excluded_extras() -> Iterable[tuple[str, list[str]]]:
    for key, value in CORE_EXTRAS_DEPENDENCIES.items():
        if value:
            yield key, value
        else:
            print(f"Removing extra {key} as it has been excluded")


CORE_EXTRAS_DEPENDENCIES = dict(filter_out_excluded_extras())

EXTRAS_DEPENDENCIES: dict[str, list[str]] = deepcopy(CORE_EXTRAS_DEPENDENCIES)


def add_extras_for_all_providers() -> None:
    for provider_name, provider_dict in PROVIDER_DEPENDENCIES.items():
        EXTRAS_DEPENDENCIES[provider_name] = provider_dict[DEPS]


def add_additional_extras() -> None:
    for extra_name, extra_dependencies in ADDITIONAL_EXTRAS_DEPENDENCIES.items():
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
    "dask": "daskexecutor",
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
    Add extras for all deprecated aliases.

    Requirements for those deprecated aliases are the same as the extras they are replaced with.
    The dependencies are not copies - those are the same lists as for the new extras. This is intended.
    Thanks to that if the original extras are later extended with providers, aliases are extended as well.
    """
    for alias, extra in EXTRAS_DEPRECATED_ALIASES.items():
        dependencies = EXTRAS_DEPENDENCIES.get(extra) if extra != "" else []
        if dependencies is not None:
            EXTRAS_DEPENDENCIES[alias] = dependencies


def add_all_deprecated_provider_packages() -> None:
    """
    For deprecated aliases that are providers, swap the providers dependencies to be the provider itself.

    e.g. {"kubernetes": ["kubernetes>=3.0.0, <12.0.0", ...]} becomes
    {"kubernetes": ["apache-airflow-provider-cncf-kubernetes"]}
    """
    for alias, provider in EXTRAS_DEPRECATED_ALIASES.items():
        if alias not in EXTRAS_DEPRECATED_ALIASES_NOT_PROVIDERS:
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
        if provider in PROVIDER_DEPENDENCIES:
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

_all_dependencies_without_airflow_providers = [k for k in _all_dependencies if "apache-airflow-" not in k]

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
PACKAGES_EXCLUDED_FOR_ALL: list[str] = []


def is_package_excluded(package: str, exclusion_list: list[str]) -> bool:
    """
    Check if package should be excluded.

    :param package: package name (beginning of it)
    :param exclusion_list: list of excluded packages
    :return: true if package should be excluded
    """
    return package.startswith(tuple(exclusion_list))


def remove_provider_limits(package: str) -> str:
    """
    Remove the limit for providers in devel_all to account for pre-release and development packages.

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
    Sort dependencies; the dictionary order remains when keys() are retrieved.

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
PREINSTALLED_PROVIDERS = [
    #   Until we cut-off the 2.8.0 branch and bump current airflow version to 2.9.0, we should
    #   Keep common.io commented out in order ot be able to generate PyPI constraints because
    #   The version from PyPI has requirement of apache-airflow>=2.8.0
    #   "common.io",
    "common.sql",
    "ftp",
    "http",
    "imap",
    "sqlite",
]


def get_provider_package_name_from_package_id(package_id: str) -> str:
    """
    Build the name of provider package out of the package id provided.

    :param package_id: id of the package (like amazon or microsoft.azure)
    :return: full name of package in PyPI
    """
    version_spec = ""
    if ">=" in package_id:
        package, version = package_id.split(">=")
        version_spec = f">={version}"
        version_suffix = os.environ.get("VERSION_SUFFIX_FOR_PYPI")
        if version_suffix:
            version_spec += version_suffix
    else:
        package = package_id
    package_suffix = package.replace(".", "-")
    return f"apache-airflow-providers-{package_suffix}{version_spec}"


def get_excluded_providers() -> list[str]:
    """Return packages excluded for the current python version."""
    return []


def get_all_provider_packages() -> str:
    """Return all provider packages configured in setup.py."""
    excluded_providers = get_excluded_providers()
    return " ".join(
        get_provider_package_name_from_package_id(package)
        for package in ALL_PROVIDERS
        if package not in excluded_providers
    )


class AirflowDistribution(Distribution):
    """The setuptools.Distribution subclass with Airflow specific behaviour."""

    def __init__(self, attrs=None):
        super().__init__(attrs)
        self.install_requires = None

    def parse_config_files(self, *args, **kwargs) -> None:
        """
        When asked to install providers from sources, ensure we don't *also* try to install from PyPI.

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
                provider_relative_path = os.path.relpath(
                    provider_yaml_file, str(AIRFLOW_SOURCES_ROOT / "airflow")
                )
                self.package_data["airflow"].append(provider_relative_path)
            # Add python_kubernetes_script.jinja2 to package data
            self.package_data["airflow"].append("providers/cncf/kubernetes/python_kubernetes_script.jinja2")
        else:
            self.install_requires.extend(
                [
                    get_provider_package_name_from_package_id(package_id)
                    for package_id in PREINSTALLED_PROVIDERS
                ]
            )


def replace_extra_dependencies_with_provider_packages(extra: str, providers: list[str]) -> None:
    """
    Replace extra dependencies with provider package.

    The intention here is that when the provider is added as dependency of extra, there is no
    need to add the dependencies separately. This is not needed and even harmful, because in
    case of future versions of the provider, the dependencies might change, so hard-coding
    dependencies from the version that was available at the release time might cause dependency
    conflicts in the future.

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

    In the constraint mechanism we save both - provider versions and its dependencies
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
    if extra in ["cncf.kubernetes", "kubernetes", "celery", "daskexecutor", "dask"]:
        EXTRAS_DEPENDENCIES[extra].extend(
            [get_provider_package_name_from_package_id(package_name) for package_name in providers]
        )
    elif extra == "apache.hive":
        # We moved the hive macros to the hive provider, and they are available in hive provider only as of
        # 5.1.0 version only, so we have to make sure minimum version is used
        EXTRAS_DEPENDENCIES[extra] = ["apache-airflow-providers-apache-hive>=5.1.0"]
    else:
        EXTRAS_DEPENDENCIES[extra] = [
            get_provider_package_name_from_package_id(package_name) for package_name in providers
        ]


def add_provider_packages_to_extra_dependencies(extra: str, providers: list[str]) -> None:
    """
    Add provider packages as dependencies to extra.

    This is used to add provider packages as dependencies to the "bulk" kind of extras.
    Those bulk extras do not have the detailed 'extra' dependencies as initial values,
    so instead of replacing them (see previous function) we can extend them.

    :param extra: Name of the extra to add providers to
    :param providers: list of provider ids
    """
    EXTRAS_DEPENDENCIES[extra].extend(
        [get_provider_package_name_from_package_id(package_name) for package_name in providers]
    )


def add_all_provider_packages() -> None:
    """
    Add extra dependencies when providers are installed from packages.

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
        When installing providers from sources we install all namespace packages found below airflow.

        Includes airflow and provider packages, otherwise defaults from setup.cfg control this.
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
        extras_require=EXTRAS_DEPENDENCIES,
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
    do_setup()  # comment to trigger upgrade to newer dependencies when setup.py is changed
