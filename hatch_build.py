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
import sys
from pathlib import Path
from subprocess import run
from typing import Any, Callable, Iterable

from hatchling.builders.config import BuilderConfig
from hatchling.builders.hooks.plugin.interface import BuildHookInterface
from hatchling.builders.plugin.interface import BuilderInterface
from hatchling.plugin.manager import PluginManager

log = logging.getLogger(__name__)
log_level = logging.getLevelName(os.getenv("CUSTOM_AIRFLOW_BUILD_LOG_LEVEL", "INFO"))
log.setLevel(log_level)

AIRFLOW_ROOT_PATH = Path(__file__).parent.resolve()
GENERATED_PROVIDERS_DEPENDENCIES_FILE = AIRFLOW_ROOT_PATH / "generated" / "provider_dependencies.json"
PROVIDER_DEPENDENCIES = json.loads(GENERATED_PROVIDERS_DEPENDENCIES_FILE.read_text())

PRE_INSTALLED_PROVIDERS = [
    "common.io",
    "common.sql",
    "fab>=1.0.2dev0",
    "ftp",
    "http",
    "imap",
    "smtp",
    "sqlite",
]

# Those extras are dynamically added by hatch in the build hook to metadata optional dependencies
# when project is installed locally (editable build) or when wheel package is built based on it.
CORE_EXTRAS: dict[str, list[str]] = {
    # Aiobotocore required for AWS deferrable operators.
    # There is conflict between boto3 and aiobotocore dependency botocore.
    # TODO: We can remove it once boto3 and aiobotocore both have compatible botocore version or
    # boto3 have native aync support and we move away from aio aiobotocore
    "aiobotocore": [
        "aiobotocore>=2.7.0",
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
    "deprecated-api": [
        "requests>=2.27.0,<3",
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
        "graphviz>=0.12",
    ],
    "kerberos": [
        "pykerberos>=1.1.13",
        "requests-kerberos>=0.10.0",
        "thrift-sasl>=0.2.0",
    ],
    "ldap": [
        "ldap3>=2.5.1",
        "python-ldap",
    ],
    "leveldb": [
        "plyvel",
    ],
    "otel": [
        "opentelemetry-exporter-prometheus",
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
    "pydantic": [
        "pydantic>=2.3.0",
    ],
    "rabbitmq": [
        "amqp",
    ],
    "s3fs": [
        # This is required for support of S3 file system which uses aiobotocore
        # which can have a conflict with boto3 as mentioned in aiobotocore extra
        "s3fs>=2023.10.0",
    ],
    "saml": [
        # This is required for support of SAML which might be used by some providers (e.g. Amazon)
        "python3-saml>=1.16.0",
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
        "uv>=0.1.24",
    ],
    "virtualenv": [
        "virtualenv",
    ],
}

DOC_EXTRAS: dict[str, list[str]] = {
    "doc": [
        "astroid>=2.12.3,<3.0",
        "checksumdir>=1.2.0",
        # click 8.1.4 and 8.1.5 generate mypy errors due to typing issue in the upstream package:
        # https://github.com/pallets/click/issues/2558
        "click>=8.0,!=8.1.4,!=8.1.5",
        # Docutils 0.17.0 converts generated <div class="section"> into <section> and breaks our doc formatting
        # By adding a lot of whitespace separation. This limit can be lifted when we update our doc to handle
        # <section> tags for sections
        "docutils<0.17,>=0.16",
        "sphinx-airflow-theme>=0.0.12",
        "sphinx-argparse>=0.4.0",
        # sphinx-autoapi fails with astroid 3.0, see: https://github.com/readthedocs/sphinx-autoapi/issues/407
        # This was fixed in sphinx-autoapi 3.0, however it has requirement sphinx>=6.1, but we stuck on 5.x
        "sphinx-autoapi>=2.1.1",
        "sphinx-copybutton>=0.5.2",
        "sphinx-design>=0.5.0",
        "sphinx-jinja>=2.0.2",
        "sphinx-rtd-theme>=2.0.0",
        # Currently we are using sphinx 5 but we need to migrate to Sphinx 7
        "sphinx>=5.3.0,<6.0.0",
        "sphinxcontrib-applehelp>=1.0.4",
        "sphinxcontrib-devhelp>=1.0.2",
        "sphinxcontrib-htmlhelp>=2.0.1",
        "sphinxcontrib-httpdomain>=1.8.1",
        "sphinxcontrib-jquery>=4.1",
        "sphinxcontrib-jsmath>=1.0.1",
        "sphinxcontrib-qthelp>=1.0.3",
        "sphinxcontrib-redoc>=1.6.0",
        "sphinxcontrib-serializinghtml==1.1.5",
        "sphinxcontrib-spelling>=8.0.0",
    ],
    "doc-gen": [
        "apache-airflow[doc]",
        "eralchemy2>=1.3.8",
    ],
    # END OF doc extras
}

DEVEL_EXTRAS: dict[str, list[str]] = {
    # START OF devel extras
    "devel-debuggers": [
        "ipdb>=0.13.13",
    ],
    "devel-devscripts": [
        "click>=8.0",
        "gitpython>=3.1.40",
        "hatch>=1.9.1",
        "pipdeptree>=2.13.1",
        "pygithub>=2.1.1",
        "restructuredtext-lint>=1.4.0",
        "rich-click>=1.7.0",
        "semver>=3.0.2",
        "towncrier>=23.11.0",
        "twine>=4.0.2",
    ],
    "devel-duckdb": [
        # Python 3.12 support was added in 0.10.0
        "duckdb>=0.10.0; python_version >= '3.12'",
        "duckdb>=0.9.0; python_version < '3.12'",
    ],
    # Mypy 0.900 and above ships only with stubs from stdlib so if we need other stubs, we need to install them
    # manually as `types-*`. See https://mypy.readthedocs.io/en/stable/running_mypy.html#missing-imports
    # for details. We want to install them explicitly because we want to eventually move to
    # mypyd which does not support installing the types dynamically with --install-types
    "devel-mypy": [
        # TODO: upgrade to newer versions of MyPy continuously as they are released
        # Make sure to upgrade the mypy version in update-common-sql-api-stubs in .pre-commit-config.yaml
        # when you upgrade it here !!!!
        "mypy==1.9.0",
        "types-Deprecated",
        "types-Markdown",
        "types-PyMySQL",
        "types-PyYAML",
        "types-aiofiles",
        "types-certifi",
        "types-croniter",
        "types-docutils",
        "types-paramiko",
        "types-protobuf",
        "types-python-dateutil",
        "types-python-slugify",
        "types-pytz",
        "types-redis",
        "types-requests",
        "types-setuptools",
        "types-tabulate",
        "types-termcolor",
        "types-toml",
    ],
    "devel-sentry": [
        "blinker>=1.7.0",
    ],
    "devel-static-checks": [
        "black>=23.12.0",
        "pre-commit>=3.5.0",
        "ruff==0.3.3",
        "yamllint>=1.33.0",
    ],
    "devel-tests": [
        "aiofiles>=23.2.0",
        "aioresponses>=0.7.6",
        "backports.zoneinfo>=0.2.1;python_version<'3.9'",
        "beautifulsoup4>=4.7.1",
        # Coverage 7.4.0 added experimental support for Python 3.12 PEP669 which we use in Airflow
        "coverage>=7.4.0",
        "pytest-asyncio>=0.23.3",
        "pytest-cov>=4.1.0",
        "pytest-custom-exit-code>=0.3.0",
        "pytest-icdiff>=0.9",
        "pytest-instafail>=0.5.0",
        "pytest-mock>=3.12.0",
        "pytest-rerunfailures>=13.0",
        "pytest-timeouts>=1.2.1",
        "pytest-xdist>=3.5.0",
        # Temporary upper limmit to <8, not all dependencies at that moment ready to use 8.0
        # Internal meta-task for track https://github.com/apache/airflow/issues/37156
        "pytest>=7.4.4,<8.0",
        "requests_mock>=1.11.0",
        "time-machine>=2.13.0",
        "wheel>=0.42.0",
    ],
    "devel": [
        "apache-airflow[celery]",
        "apache-airflow[cncf-kubernetes]",
        "apache-airflow[common-io]",
        "apache-airflow[common-sql]",
        "apache-airflow[devel-debuggers]",
        "apache-airflow[devel-devscripts]",
        "apache-airflow[devel-duckdb]",
        "apache-airflow[devel-mypy]",
        "apache-airflow[devel-sentry]",
        "apache-airflow[devel-static-checks]",
        "apache-airflow[devel-tests]",
        "apache-airflow[fab]",
        "apache-airflow[ftp]",
        "apache-airflow[http]",
        "apache-airflow[imap]",
        "apache-airflow[sqlite]",
    ],
    "devel-all-dbs": [
        "apache-airflow[apache-cassandra]",
        "apache-airflow[apache-drill]",
        "apache-airflow[apache-druid]",
        "apache-airflow[apache-hdfs]",
        "apache-airflow[apache-hive]",
        "apache-airflow[apache-impala]",
        "apache-airflow[apache-pinot]",
        "apache-airflow[arangodb]",
        "apache-airflow[cloudant]",
        "apache-airflow[databricks]",
        "apache-airflow[exasol]",
        "apache-airflow[influxdb]",
        "apache-airflow[microsoft-mssql]",
        "apache-airflow[mongo]",
        "apache-airflow[mysql]",
        "apache-airflow[neo4j]",
        "apache-airflow[postgres]",
        "apache-airflow[presto]",
        "apache-airflow[trino]",
        "apache-airflow[vertica]",
    ],
    "devel-ci": [
        "apache-airflow[devel-all]",
    ],
    "devel-hadoop": [
        "apache-airflow[apache-hdfs]",
        "apache-airflow[apache-hive]",
        "apache-airflow[apache-impala]",
        "apache-airflow[devel]",
        "apache-airflow[hdfs]",
        "apache-airflow[kerberos]",
        "apache-airflow[presto]",
    ],
}

BUNDLE_EXTRAS: dict[str, list[str]] = {
    "all-dbs": [
        "apache-airflow[apache-cassandra]",
        "apache-airflow[apache-drill]",
        "apache-airflow[apache-druid]",
        "apache-airflow[apache-hdfs]",
        "apache-airflow[apache-hive]",
        "apache-airflow[apache-impala]",
        "apache-airflow[apache-pinot]",
        "apache-airflow[arangodb]",
        "apache-airflow[cloudant]",
        "apache-airflow[databricks]",
        "apache-airflow[exasol]",
        "apache-airflow[influxdb]",
        "apache-airflow[microsoft-mssql]",
        "apache-airflow[mongo]",
        "apache-airflow[mysql]",
        "apache-airflow[neo4j]",
        "apache-airflow[postgres]",
        "apache-airflow[presto]",
        "apache-airflow[trino]",
        "apache-airflow[vertica]",
    ],
}

DEPRECATED_EXTRAS: dict[str, list[str]] = {
    ########################################################################################################
    #  The whole section can be removed in Airflow 3.0 as those old aliases are deprecated in 2.* series
    ########################################################################################################
    "atlas": [
        "apache-airflow[apache-atlas]",
    ],
    "aws": [
        "apache-airflow[amazon]",
    ],
    "azure": [
        "apache-airflow[microsoft-azure]",
    ],
    "cassandra": [
        "apache-airflow[apache-cassandra]",
    ],
    # Empty alias extra just for backward compatibility with Airflow 1.10
    "crypto": [],
    "druid": [
        "apache-airflow[apache-druid]",
    ],
    "gcp": [
        "apache-airflow[google]",
    ],
    "gcp-api": [
        "apache-airflow[google]",
    ],
    "hdfs": [
        "apache-airflow[apache-hdfs]",
    ],
    "hive": [
        "apache-airflow[apache-hive]",
    ],
    "kubernetes": [
        "apache-airflow[cncf-kubernetes]",
    ],
    "mssql": [
        "apache-airflow[microsoft-mssql]",
    ],
    "pinot": [
        "apache-airflow[apache-pinot]",
    ],
    "s3": [
        "apache-airflow[amazon]",
    ],
    "spark": [
        "apache-airflow[apache-spark]",
    ],
    "webhdfs": [
        "apache-airflow[apache-webhdfs]",
    ],
    "winrm": [
        "apache-airflow[microsoft-winrm]",
    ],
}

# When you remove a dependency from the list, you should also make sure to add the dependency to be removed
# in the scripts/docker/install_airflow_dependencies_from_branch_tip.sh script DEPENDENCIES_TO_REMOVE
# in order to make sure the dependency is not installed in the CI image build process from the main
# of Airflow branch. After your PR is merged, you should remove it from the list there.
DEPENDENCIES = [
    # Alembic is important to handle our migrations in predictable and performant way. It is developed
    # together with SQLAlchemy. Our experience with Alembic is that it very stable in minor version
    # The 1.13.0 of alembic marked some migration code as SQLAlchemy 2+ only so we limit it to 1.13.1
    "alembic>=1.13.1, <2.0",
    "argcomplete>=1.10",
    "asgiref",
    "attrs>=22.1.0",
    # Blinker use for signals in Flask, this is an optional dependency in Flask 2.2 and lower.
    # In Flask 2.3 it becomes a mandatory dependency, and flask signals are always available.
    "blinker>=1.6.2",
    # Colorlog 6.x merges TTYColoredFormatter into ColoredFormatter, breaking backwards compatibility with 4.x
    # Update CustomTTYColoredFormatter to remove
    "colorlog>=4.0.2, <5.0",
    "configupdater>=3.1.1",
    # `airflow/www/extensions/init_views` imports `connexion.decorators.validation.RequestBodyValidator`
    # connexion v3 has refactored the entire module to middleware, see: /spec-first/connexion/issues/1525
    # Specifically, RequestBodyValidator was removed in: /spec-first/connexion/pull/1595
    # The usage was added in #30596, seemingly only to override and improve the default error message.
    # Either revert that change or find another way, preferably without using connexion internals.
    # This limit can be removed after https://github.com/apache/airflow/issues/35234 is fixed
    "connexion[flask]>=2.10.0,<3.0",
    "cron-descriptor>=1.2.24",
    "croniter>=2.0.2",
    "cryptography>=39.0.0",
    "deprecated>=1.2.13",
    "dill>=0.2.2",
    "flask-caching>=1.5.0",
    # Flask-Session 0.6 add new arguments into the SqlAlchemySessionInterface constructor as well as
    # all parameters now are mandatory which make AirflowDatabaseSessionInterface incopatible with this version.
    "flask-session>=0.4.0,<0.6",
    "flask-wtf>=0.15",
    # Flask 2.3 is scheduled to introduce a number of deprecation removals - some of them might be breaking
    # for our dependencies - notably `_app_ctx_stack` and `_request_ctx_stack` removals.
    # We should remove the limitation after 2.3 is released and our dependencies are updated to handle it
    "flask>=2.2,<2.3",
    "fsspec>=2023.10.0",
    "google-re2>=1.0",
    "gunicorn>=20.1.0",
    "httpx",
    'importlib_metadata>=6.5;python_version<"3.12"',
    # Importib_resources 6.2.0-6.3.1 break pytest_rewrite
    # see https://github.com/python/importlib_resources/issues/299
    'importlib_resources>=5.2,!=6.2.0,!=6.3.0,!=6.3.1;python_version<"3.9"',
    "itsdangerous>=2.0",
    "jinja2>=3.0.0",
    "jsonschema>=4.18.0",
    "lazy-object-proxy",
    "linkify-it-py>=2.0.0",
    "lockfile>=0.12.2",
    "markdown-it-py>=2.1.0",
    "markupsafe>=1.1.1",
    "marshmallow-oneofschema>=2.0.1",
    "mdit-py-plugins>=0.3.0",
    "opentelemetry-api>=1.15.0",
    "opentelemetry-exporter-otlp",
    "packaging>=14.0",
    "pathspec>=0.9.0",
    "pendulum>=2.1.2,<4.0",
    "pluggy>=1.0",
    "psutil>=4.2.0",
    "pygments>=2.0.1",
    "pyjwt>=2.0.0",
    "python-daemon>=3.0.0",
    "python-dateutil>=2.3",
    "python-nvd3>=0.15.0",
    "python-slugify>=5.0",
    # Requests 3 if it will be released, will be heavily breaking.
    "requests>=2.27.0,<3",
    "rfc3339-validator>=0.1.4",
    "rich-argparse>=1.0.0",
    "rich>=12.4.4",
    "setproctitle>=1.1.8",
    # We use some deprecated features of sqlalchemy 2.0 and we should replace them before we can upgrade
    # See https://sqlalche.me/e/b8d9 for details of deprecated features
    # you can set environment variable SQLALCHEMY_WARN_20=1 to show all deprecation warnings.
    # The issue tracking it is https://github.com/apache/airflow/issues/28723
    "sqlalchemy>=1.4.36,<2.0",
    "sqlalchemy-jsonfield>=1.0",
    "tabulate>=0.7.5",
    "tenacity>=6.2.0,!=8.2.0",
    "termcolor>=1.1.0",
    # We should remove this dependency when Providers are limited to Airflow 2.7+
    # as we replaced the usage of unicodecsv with csv in Airflow 2.7
    # See https://github.com/apache/airflow/pull/31693
    # We should also remove "licenses/LICENSE-unicodecsv.txt" file when we remove this dependency
    "unicodecsv>=0.14.1",
    # The Universal Pathlib provides  Pathlib-like interface for FSSPEC
    "universal-pathlib>=0.2.2",
    # Werkzug 3 breaks Flask-Login 0.6.2, also connexion needs to be updated to >= 3.0
    # we should remove this limitation when FAB supports Flask 2.3 and we migrate connexion to 3+
    "werkzeug>=2.0,<3",
]


ALL_DYNAMIC_EXTRA_DICTS: list[tuple[dict[str, list[str]], str]] = [
    (CORE_EXTRAS, "Core extras"),
    (DOC_EXTRAS, "Doc extras"),
    (DEVEL_EXTRAS, "Devel extras"),
    (BUNDLE_EXTRAS, "Bundle extras"),
    (DEPRECATED_EXTRAS, "Deprecated extras"),
]

ALL_GENERATED_BUNDLE_EXTRAS = ["all", "all-core", "devel-all", "devel-ci"]


def normalize_extra(dependency_id: str) -> str:
    return dependency_id.replace(".", "-").replace("_", "-")


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


ALL_DYNAMIC_EXTRAS: list[str] = sorted(
    set(
        itertools.chain(
            *[d for d, desc in ALL_DYNAMIC_EXTRA_DICTS],
            [normalize_extra(provider) for provider in PROVIDER_DEPENDENCIES],
            ALL_GENERATED_BUNDLE_EXTRAS,
        )
    )
)


def get_provider_id(provider_spec: str) -> str:
    # in case provider_spec is "<provider_id>=<version>"
    return provider_spec.split(">=")[0]


def get_provider_requirement(provider_spec: str) -> str:
    if ">=" in provider_spec:
        provider_id, min_version = provider_spec.split(">=")
        return f"apache-airflow-providers-{provider_id.replace('.', '-')}>={min_version}"
    else:
        return f"apache-airflow-providers-{provider_spec.replace('.', '-')}"


# if providers are ready, we can preinstall them
PREINSTALLED_PROVIDERS = [
    get_provider_requirement(provider_spec)
    for provider_spec in PRE_INSTALLED_PROVIDERS
    if PROVIDER_DEPENDENCIES[get_provider_id(provider_spec)]["state"] == "ready"
]

# if provider is in not-ready or pre-release, we need to install its dependencies
# however we need to skip apache-airflow itself and potentially any providers that are
PREINSTALLED_NOT_READY_DEPS = []
for provider_spec in PRE_INSTALLED_PROVIDERS:
    provider_id = get_provider_id(provider_spec)
    if PROVIDER_DEPENDENCIES[provider_id]["state"] not in ["ready", "suspended", "removed"]:
        for dependency in PROVIDER_DEPENDENCIES[provider_id]["deps"]:
            if dependency.startswith("apache-airflow-providers"):
                raise Exception(
                    f"The provider {provider_id} is pre-installed and it has as dependency "
                    f"to another provider {dependency}. This is not allowed. Pre-installed"
                    f"providers should only have 'apache-airflow' and regular dependencies."
                )
            if not dependency.startswith("apache-airflow"):
                PREINSTALLED_NOT_READY_DEPS.append(dependency)


class CustomBuild(BuilderInterface[BuilderConfig, PluginManager]):
    """Custom build class for Airflow assets and git version."""

    # Note that this name of the plugin MUST be `custom` - as long as we use it from custom
    # hatch_build.py file and not from external plugin. See note in the:
    # https://hatch.pypa.io/latest/plugins/build-hook/custom/#example
    #
    PLUGIN_NAME = "custom"

    def clean(self, directory: str, versions: Iterable[str]) -> None:
        work_dir = Path(self.root)
        commands = [
            ["rm -rf airflow/www/static/dist"],
            ["rm -rf airflow/www/node_modules"],
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
            ["pre-commit run --hook-stage manual compile-www-assets --all-files"],
        ]
        for cmd in commands:
            run(cmd, cwd=work_dir.as_posix(), check=True, shell=True)
        dist_path = work_dir / "airflow" / "www" / "static" / "dist"
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


def _is_devel_extra(extra: str) -> bool:
    return extra.startswith("devel") or extra in ["doc", "doc-gen"]


GENERATED_DEPENDENCIES_START = "# START OF GENERATED DEPENDENCIES"
GENERATED_DEPENDENCIES_END = "# END OF GENERATED DEPENDENCIES"


def convert_to_extra_dependency(dependency: str) -> str:
    # if there is version in dependency - remove it as we do not need it in extra specification
    # for editable installation
    if ">=" in dependency:
        dependency = dependency.split(">=")[0]
    extra = dependency.replace("apache-airflow-providers-", "").replace("-", "_").replace(".", "_")
    return f"apache-airflow[{extra}]"


def get_python_exclusion(excluded_python_versions: list[str]):
    exclusion = ""
    if excluded_python_versions:
        separator = ";"
        for version in excluded_python_versions:
            exclusion += f'{separator}python_version != "{version}"'
            separator = " and "
    return exclusion


def skip_for_editable_build(excluded_python_versions: list[str]) -> bool:
    current_python_version = f"{sys.version_info.major}.{sys.version_info.minor}"
    if current_python_version in excluded_python_versions:
        return True
    return False


class CustomBuildHook(BuildHookInterface[BuilderConfig]):
    """Custom build hook for Airflow - remove devel extras and adds preinstalled providers."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        # Stores all dependencies that that any of the airflow extras (including devel) use
        self.all_devel_ci_dependencies: set[str] = set()
        # All extras that should be included in the wheel package
        self.all_non_devel_extras: set[str] = set()
        # All extras that should be available in the editable install
        self.all_devel_extras: set[str] = set()
        self.optional_dependencies: dict[str, list[str]] = {}
        self._dependencies: list[str] = []
        super().__init__(*args, **kwargs)

    def initialize(self, version: str, build_data: dict[str, Any]) -> None:
        """
        Initialize hook immediately before each build.

        Any modifications to the build data will be seen by the build target.
        """
        self._process_all_built_in_extras(version)
        self._process_all_provider_extras(version)

        # Adds all-core extras for the extras that are built-in and not devel
        self.optional_dependencies["all-core"] = sorted(
            set([f"apache-airflow[{extra}]" for extra in CORE_EXTRAS.keys()])
        )
        # Adds "apache-airflow[extra]" for all extras that are not devel extras for wheel and editable builds
        self.optional_dependencies["all"] = [
            f"apache-airflow[{extra}]" for extra in sorted(self.all_non_devel_extras)
        ]
        # Adds all devel extras for the extras that are built-in only for editable builds
        if version != "standard":
            self.optional_dependencies["devel-all"] = [
                f"apache-airflow[{extra}]" for extra in sorted(self.all_devel_extras)
            ]
        # This is special dependency that is used to install all possible
        # 3rd-party dependencies for airflow for the CI image. It is exposed in the wheel package
        # because we want to use for building the image cache from GitHub URL.
        self.optional_dependencies["devel-ci"] = sorted(self.all_devel_ci_dependencies)
        self._dependencies = DEPENDENCIES

        if version == "standard":
            # Inject preinstalled providers into the dependencies for standard packages
            for provider in PREINSTALLED_PROVIDERS:
                self._dependencies.append(provider)
            for not_ready_provider_dependency in PREINSTALLED_NOT_READY_DEPS:
                self._dependencies.append(not_ready_provider_dependency)

        # with hatchling, we can modify dependencies dynamically by modifying the build_data
        build_data["dependencies"] = self._dependencies

        # unfortunately hatchling currently does not have a way to override optional_dependencies
        # via build_data (or so it seem) so we need to modify internal _optional_dependencies
        # field in core.metadata until this is possible
        self.metadata.core._optional_dependencies = self.optional_dependencies

    def _add_devel_ci_dependencies(self, deps: list[str], python_exclusion: str) -> None:
        """
        Add devel_ci_dependencies.

        Adds all external dependencies which are not apache-airflow deps to the list of dependencies
        that are going to be added to `devel-ci` extra.

        :param deps: list of dependencies to add
        :param version: "standard" or "editable" build.
        :param excluded_python_versions: List of python versions to exclude
        :param python_exclusion: Python version exclusion string.
        """
        for dep in deps:
            if not dep.startswith("apache-airflow"):
                self.all_devel_ci_dependencies.add(normalize_requirement(dep) + python_exclusion)

    def _process_all_provider_extras(self, version: str) -> None:
        """
        Process all provider extras.

        Processes all provider dependencies. This generates dependencies for editable builds
        and providers for wheel builds.

        :param version: "standard" or "editable" build.
        :return:
        """
        for dependency_id in PROVIDER_DEPENDENCIES.keys():
            if PROVIDER_DEPENDENCIES[dependency_id]["state"] != "ready":
                continue
            excluded_python_versions = PROVIDER_DEPENDENCIES[dependency_id].get("excluded-python-versions")
            if version != "standard" and skip_for_editable_build(excluded_python_versions):
                continue
            normalized_extra_name = normalize_extra(dependency_id)
            deps: list[str] = PROVIDER_DEPENDENCIES[dependency_id]["deps"]

            deps = [dep for dep in deps if not dep.startswith("apache-airflow>=")]
            devel_deps: list[str] = PROVIDER_DEPENDENCIES[dependency_id].get("devel-deps", [])

            if version == "standard":
                # add providers instead of dependencies for wheel builds
                self.optional_dependencies[normalized_extra_name] = [
                    f"apache-airflow-providers-{normalized_extra_name}"
                    f"{get_python_exclusion(excluded_python_versions)}"
                ]
            else:
                # for editable packages - add regular + devel dependencies retrieved from provider.yaml
                # but convert the provider dependencies to apache-airflow[extras]
                # and adding python exclusions where needed
                editable_deps = []
                for dep in itertools.chain(deps, devel_deps):
                    if dep.startswith("apache-airflow-providers-"):
                        dep = convert_to_extra_dependency(dep)
                    editable_deps.append(dep)
                self.optional_dependencies[normalized_extra_name] = sorted(set(editable_deps))
                self._add_devel_ci_dependencies(editable_deps, python_exclusion="")
            self.all_devel_extras.add(normalized_extra_name)
            self.all_non_devel_extras.add(normalized_extra_name)

    def _process_all_built_in_extras(self, version: str) -> None:
        """
        Process all built-in extras.

        Adds all core extras (for editable builds) minus devel and doc extras (for wheel builds)
        to the list of dependencies. It also builds the list of all non-devel built-in extras that will be
        used to produce "all" extra.

        :param version: "standard" or "editable" build.
        :return:
        """
        for dict, _ in ALL_DYNAMIC_EXTRA_DICTS:
            for extra, deps in dict.items():
                self.all_devel_extras.add(extra)
                self._add_devel_ci_dependencies(deps, python_exclusion="")
                if dict not in [DEPRECATED_EXTRAS, DEVEL_EXTRAS, DOC_EXTRAS]:
                    # do not add deprecated extras to "all" extras
                    self.all_non_devel_extras.add(extra)
                if version == "standard":
                    # for wheel builds we skip devel and doc extras
                    if dict not in [DEVEL_EXTRAS, DOC_EXTRAS]:
                        self.optional_dependencies[extra] = deps
                else:
                    # for editable builds we add all extras
                    self.optional_dependencies[extra] = deps
