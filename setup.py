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

import logging
import os
import subprocess
import unittest
from os.path import dirname
from textwrap import wrap
from typing import Dict, List, Set, Tuple

from setuptools import Command, Distribution, find_namespace_packages, setup

logger = logging.getLogger(__name__)

version = '2.0.0'

my_dir = dirname(__file__)


def airflow_test_suite():
    """Test suite for Airflow tests"""
    test_loader = unittest.TestLoader()
    test_suite = test_loader.discover(os.path.join(my_dir, 'tests'), pattern='test_*.py')
    return test_suite


class CleanCommand(Command):
    """
    Command to tidy up the project root.
    Registered as cmdclass in setup() so it can be called with ``python setup.py extra_clean``.
    """

    description = "Tidy up the project root"
    user_options = []  # type: List[str]

    def initialize_options(self):
        """Set default values for options."""

    def finalize_options(self):
        """Set final values for options."""

    def run(self):  # noqa
        """Run command to remove temporary files and directories."""
        os.chdir(my_dir)
        os.system('rm -vrf ./build ./dist ./*.pyc ./*.tgz ./*.egg-info')


class CompileAssets(Command):
    """
    Compile and build the frontend assets using yarn and webpack.
    Registered as cmdclass in setup() so it can be called with ``python setup.py compile_assets``.
    """

    description = "Compile and build the frontend assets"
    user_options = []  # type: List[str]

    def initialize_options(self):
        """Set default values for options."""

    def finalize_options(self):
        """Set final values for options."""

    def run(self):  # noqa
        """Run a command to compile and build assets."""
        subprocess.check_call('./airflow/www/compile_assets.sh')


class ListExtras(Command):
    """
    List all available extras
    Registered as cmdclass in setup() so it can be called with ``python setup.py list_extras``.
    """

    description = "List available extras"
    user_options = []  # type: List[str]

    def initialize_options(self):
        """Set default values for options."""

    def finalize_options(self):
        """Set final values for options."""

    def run(self):  # noqa
        """List extras."""
        print("\n".join(wrap(", ".join(EXTRAS_REQUIREMENTS.keys()), 100)))


def git_version(version_: str) -> str:
    """
    Return a version to identify the state of the underlying git repo. The version will
    indicate whether the head of the current git-backed working directory is tied to a
    release tag or not : it will indicate the former with a 'release:{version}' prefix
    and the latter with a 'dev0' prefix. Following the prefix will be a sha of the current
    branch head. Finally, a "dirty" suffix is appended to indicate that uncommitted
    changes are present.

    :param str version_: Semver version
    :return: Found Airflow version in Git repo
    :rtype: str
    """
    try:
        import git

        try:
            repo = git.Repo(os.path.join(*[my_dir, '.git']))
        except git.NoSuchPathError:
            logger.warning('.git directory not found: Cannot compute the git version')
            return ''
        except git.InvalidGitRepositoryError:
            logger.warning('Invalid .git directory not found: Cannot compute the git version')
            return ''
    except ImportError:
        logger.warning('gitpython not found: Cannot compute the git version.')
        return ''
    if repo:
        sha = repo.head.commit.hexsha
        if repo.is_dirty():
            return f'.dev0+{sha}.dirty'
        # commit is clean
        return f'.release:{version_}+{sha}'
    else:
        return 'no_git_version'


def write_version(filename: str = os.path.join(*[my_dir, "airflow", "git_version"])):
    """
    Write the Semver version + git hash to file, e.g. ".dev0+2f635dc265e78db6708f59f68e8009abb92c1e65".

    :param str filename: Destination file to write
    """
    text = "{}".format(git_version(version))
    with open(filename, 'w') as file:
        file.write(text)


if os.environ.get('USE_THEME_FROM_GIT'):
    _SPHINX_AIRFLOW_THEME_URL = (
        "@ https://github.com/apache/airflow-site/releases/download/0.0.4/"
        "sphinx_airflow_theme-0.0.4-py3-none-any.whl"
    )
else:
    _SPHINX_AIRFLOW_THEME_URL = ''

# 'Start dependencies group' and 'Start dependencies group' are mark for ./scripts/ci/check_order_setup.py
# If you change this mark you should also change ./scripts/ci/check_order_setup.py
# Start dependencies group
amazon = [
    'boto3>=1.15.0,<1.16.0',
    'botocore>=1.18.0,<1.19.0',
    'watchtower~=0.7.3',
]
apache_beam = [
    'apache-beam[gcp]',
]
async_packages = [
    'eventlet>= 0.9.7',
    'gevent>=0.13',
    'greenlet>=0.4.9',
]
atlas = [
    'atlasclient>=0.1.2',
]
azure = [
    'azure-batch>=8.0.0',
    'azure-cosmos>=3.0.1,<4',
    'azure-datalake-store>=0.0.45',
    'azure-identity>=1.3.1',
    'azure-keyvault>=4.1.0',
    'azure-kusto-data>=0.0.43,<0.1',
    'azure-mgmt-containerinstance>=1.5.0,<2.0',
    'azure-mgmt-datalake-store>=0.5.0',
    'azure-mgmt-resource>=2.2.0',
    'azure-storage>=0.34.0, <0.37.0',
]
cassandra = [
    'cassandra-driver>=3.13.0,<3.21.0',
]
celery = [
    'celery~=4.4.2',
    'flower>=0.7.3, <1.0',
    'vine~=1.3',  # https://stackoverflow.com/questions/32757259/celery-no-module-named-five
]
cgroups = [
    'cgroupspy>=0.1.4',
]
cloudant = [
    'cloudant>=2.0',
]
dask = ['cloudpickle>=1.4.1, <1.5.0', 'distributed>=2.11.1, <2.20']
databricks = [
    'requests>=2.20.0, <3',
]
datadog = [
    'datadog>=0.14.0',
]
doc = [
    'sphinx>=2.1.2',
    f'sphinx-airflow-theme{_SPHINX_AIRFLOW_THEME_URL}',
    'sphinx-argparse>=0.1.13',
    'sphinx-autoapi==1.0.0',
    'sphinx-copybutton',
    'sphinx-jinja~=1.1',
    'sphinx-rtd-theme>=0.1.6',
    'sphinxcontrib-httpdomain>=1.7.0',
    'sphinxcontrib-redoc>=1.6.0',
    'sphinxcontrib-spelling==5.2.1',
]
docker = [
    'docker~=3.0',
]
druid = [
    'pydruid>=0.4.1',
]
elasticsearch = [
    'elasticsearch>7, <7.6.0',
    'elasticsearch-dbapi==0.1.0',
    'elasticsearch-dsl>=5.0.0',
]
exasol = [
    'pyexasol>=0.5.1,<1.0.0',
]
facebook = [
    'facebook-business>=6.0.2',
]
flask_oauth = [
    'Flask-OAuthlib>=0.9.1,<0.9.6',  # Flask OAuthLib 0.9.6 requires Flask-Login 0.5.0 - breaks FAB
    'oauthlib!=2.0.3,!=2.0.4,!=2.0.5,<3.0.0,>=1.1.2',
    'requests-oauthlib<1.2.0',
]
google = [
    'PyOpenSSL',
    'google-ads>=4.0.0,<8.0.0',
    'google-api-python-client>=1.6.0,<2.0.0',
    'google-auth>=1.0.0,<2.0.0',
    'google-auth-httplib2>=0.0.1',
    'google-cloud-automl>=0.4.0,<2.0.0',
    'google-cloud-bigquery-datatransfer>=0.4.0,<2.0.0',
    'google-cloud-bigtable>=1.0.0,<2.0.0',
    'google-cloud-container>=0.1.1,<2.0.0',
    'google-cloud-datacatalog>=0.5.0, <0.8',  # TODO: we should migrate to 1.0 likely and add <2.0.0 then
    'google-cloud-dataproc>=1.0.1,<2.0.0',
    'google-cloud-dlp>=0.11.0,<2.0.0',
    'google-cloud-kms>=1.2.1,<2.0.0',
    'google-cloud-language>=1.1.1,<2.0.0',
    'google-cloud-logging>=1.14.0,<2.0.0',
    'google-cloud-memcache>=0.2.0',
    'google-cloud-monitoring>=0.34.0,<2.0.0',
    'google-cloud-os-login>=1.0.0,<2.0.0',
    'google-cloud-pubsub>=1.0.0,<2.0.0',
    'google-cloud-redis>=0.3.0,<2.0.0',
    'google-cloud-secret-manager>=0.2.0,<2.0.0',
    'google-cloud-spanner>=1.10.0,<2.0.0',
    'google-cloud-speech>=0.36.3,<2.0.0',
    'google-cloud-storage>=1.16,<2.0.0',
    'google-cloud-tasks>=1.2.1,<2.0.0',
    'google-cloud-texttospeech>=0.4.0,<2.0.0',
    'google-cloud-translate>=1.5.0,<2.0.0',
    'google-cloud-videointelligence>=1.7.0,<2.0.0',
    'google-cloud-vision>=0.35.2,<2.0.0',
    'grpcio-gcp>=0.2.2',
    'json-merge-patch~=0.2',
    'pandas-gbq',
]
grpc = [
    'google-auth>=1.0.0, <2.0.0dev',
    'google-auth-httplib2>=0.0.1',
    'grpcio>=1.15.0',
]
hashicorp = [
    'hvac~=0.10',
]
hdfs = [
    'snakebite-py3',
]
hive = [
    'hmsclient>=0.1.0',
    'pyhive[hive]>=0.6.0',
]
jdbc = [
    'jaydebeapi>=1.1.1',
]
jenkins = [
    'python-jenkins>=1.0.0',
]
jira = [
    'JIRA>1.0.7',
]
kerberos = [
    'pykerberos>=1.1.13',
    'requests_kerberos>=0.10.0',
    'thrift_sasl>=0.2.0',
]
kubernetes = [
    'cryptography>=2.0.0',
    'kubernetes>=3.0.0, <12.0.0',
]
kylin = ['kylinpy>=2.6']
ldap = [
    'ldap3>=2.5.1',
    'python-ldap',
]
mongo = [
    'dnspython>=1.13.0,<2.0.0',
    'pymongo>=3.6.0',
]
mssql = [
    'pymssql~=2.1,>=2.1.5',
]
mysql = [
    'mysql-connector-python>=8.0.11, <=8.0.18',
    'mysqlclient>=1.3.6,<1.4',
]
odbc = [
    'pyodbc',
]
oracle = [
    'cx_Oracle>=5.1.2',
]
pagerduty = [
    'pdpyras>=4.1.2,<5',
]
papermill = [
    'nteract-scrapbook[all]>=0.3.1',
    'papermill[all]>=1.2.1',
]
password = [
    'bcrypt>=2.0.0',
    'flask-bcrypt>=0.7.1',
]
pinot = [
    # pinotdb v0.1.1 may still work with older versions of Apache Pinot, but we've confirmed that it
    # causes a problem with newer versions.
    'pinotdb>0.1.2,<1.0.0',
]
plexus = [
    'arrow>=0.16.0',
]
postgres = [
    'psycopg2-binary>=2.7.4',
]
presto = ['presto-python-client>=0.7.0,<0.8']
qubole = [
    'qds-sdk>=1.10.4',
]
rabbitmq = [
    'amqp<5.0.0',
]
redis = [
    'redis~=3.2',
]
salesforce = [
    'simple-salesforce>=1.0.0',
]
samba = [
    'pysmbclient>=0.1.3',
]
segment = [
    'analytics-python>=1.2.9',
]
sendgrid = [
    'sendgrid>=6.0.0,<7',
]
sentry = [
    'blinker>=1.1',
    'sentry-sdk>=0.8.0',
]
singularity = ['spython>=0.0.56']
slack = [
    'slackclient>=2.0.0,<3.0.0',
]
snowflake = [
    # The `azure` provider uses legacy `azure-storage` library, where `snowflake` uses the
    # newer and more stable versions of those libraries. Most of `azure` operators and hooks work
    # fine together with `snowflake` because the deprecated library does not overlap with the
    # new libraries except the `blob` classes. So while `azure` works fine for most cases
    # blob is the only exception
    # Solution to that is being worked on in https://github.com/apache/airflow/pull/12188
    # once it is merged, we can move those two back to `azure` extra.
    'azure-storage-blob',
    'azure-storage-common',
    # snowflake is not compatible with latest version.
    # This library monkey patches the requests library, so SSL is broken globally.
    # See: https://github.com/snowflakedb/snowflake-connector-python/issues/324
    'requests<2.24.0',
    # Newest version drop support for old version of azure-storage-blob
    # Until #12188 is solved at least we need to limit maximum version.
    # https://github.com/apache/airflow/pull/12188
    'snowflake-connector-python>=1.5.2,<=2.3.6',
    'snowflake-sqlalchemy>=1.1.0',
]
spark = [
    'pyspark',
]
ssh = [
    'paramiko>=2.6.0',
    'pysftp>=0.2.9',
    'sshtunnel>=0.1.4,<0.2',
]
statsd = [
    'statsd>=3.3.0, <4.0',
]
tableau = [
    'tableauserverclient~=0.12',
]
telegram = [
    'python-telegram-bot==13.0',
]
vertica = [
    'vertica-python>=0.5.1',
]
virtualenv = [
    'virtualenv',
]
webhdfs = [
    'hdfs[avro,dataframe,kerberos]>=2.0.4',
]
winrm = [
    'pywinrm~=0.4',
]
yandex = [
    'yandexcloud>=0.22.0',
]
zendesk = [
    'zdesk',
]
# End dependencies group

############################################################################################################
# IMPORTANT NOTE!!!!!!!!!!!!!!!
# IF you are removing dependencies from this list, please make sure that you also increase
# DEPENDENCIES_EPOCH_NUMBER in the Dockerfile.ci
############################################################################################################
devel = [
    'beautifulsoup4~=4.7.1',
    'black',
    'blinker',
    'bowler',
    'click~=7.1',
    'contextdecorator;python_version<"3.4"',
    'coverage',
    'docutils',
    'flake8>=3.6.0',
    'flake8-colors',
    'flaky',
    'freezegun',
    'github3.py',
    'gitpython',
    'importlib-resources~=1.4',
    'ipdb',
    'jira',
    # HACK: Moto is not compatible with newer versions
    # See: https://github.com/spulec/moto/issues/3535
    'mock<4.0.3',
    'mongomock',
    'moto',
    'mypy==0.770',
    'parameterized',
    'paramiko',
    'pipdeptree',
    'pre-commit',
    'pylint==2.6.0',
    'pysftp',
    'pytest',
    'pytest-cov',
    'pytest-instafail',
    'pytest-rerunfailures',
    'pytest-timeouts',
    'pytest-xdist',
    'pywinrm',
    'qds-sdk>=1.9.6',
    'requests_mock',
    'testfixtures',
    'wheel',
    'yamllint',
]

############################################################################################################
# IMPORTANT NOTE!!!!!!!!!!!!!!!
# If you are removing dependencies from the above list, please make sure that you also increase
# DEPENDENCIES_EPOCH_NUMBER in the Dockerfile.ci
############################################################################################################

devel_minreq = cgroups + devel + doc + kubernetes + mysql + password
devel_hadoop = devel_minreq + hdfs + hive + kerberos + presto + webhdfs

############################################################################################################
# IMPORTANT NOTE!!!!!!!!!!!!!!!
# If you have a 'pip check' problem with dependencies, it might be because some dependency has been
# installed via 'install_requires' in setup.cfg in higher version than required in one of the options below.
# For example pip check was failing with requests=2.25.1 installed even if in some dependencies below
# < 2.24.0 was specified for it. Solution in such case is to add such limiting requirement to
# install_requires in setup.cfg (we've added requests<2.24.0 there to limit requests library).
# This should be done with appropriate comment explaining why the requirement was added.
############################################################################################################


# Dict of all providers which are part of the Apache Airflow repository together with their requirements
PROVIDERS_REQUIREMENTS: Dict[str, List[str]] = {
    'amazon': amazon,
    'apache.cassandra': cassandra,
    'apache.druid': druid,
    'apache.hdfs': hdfs,
    'apache.hive': hive,
    'apache.kylin': kylin,
    'apache.livy': [],
    'apache.pig': [],
    'apache.pinot': pinot,
    'apache.spark': spark,
    'apache.sqoop': [],
    'celery': celery,
    'cloudant': cloudant,
    'cncf.kubernetes': kubernetes,
    'databricks': databricks,
    'datadog': datadog,
    'dingding': [],
    'discord': [],
    'docker': docker,
    'elasticsearch': elasticsearch,
    'exasol': exasol,
    'facebook': facebook,
    'ftp': [],
    'google': google,
    'grpc': grpc,
    'hashicorp': hashicorp,
    'http': [],
    'imap': [],
    'jdbc': jdbc,
    'jenkins': jenkins,
    'jira': jira,
    'microsoft.azure': azure,
    'microsoft.mssql': mssql,
    'microsoft.winrm': winrm,
    'mongo': mongo,
    'mysql': mysql,
    'odbc': odbc,
    'openfaas': [],
    'opsgenie': [],
    'oracle': oracle,
    'pagerduty': pagerduty,
    'papermill': papermill,
    'plexus': plexus,
    'postgres': postgres,
    'presto': presto,
    'qubole': qubole,
    'redis': redis,
    'salesforce': salesforce,
    'samba': samba,
    'segment': segment,
    'sendgrid': sendgrid,
    'sftp': ssh,
    'singularity': singularity,
    'slack': slack,
    'snowflake': snowflake,
    'sqlite': [],
    'ssh': ssh,
    'telegram': telegram,
    'vertica': vertica,
    'yandex': yandex,
    'zendesk': zendesk,
}


# Those are all extras which do not have own 'providers'
EXTRAS_REQUIREMENTS: Dict[str, List[str]] = {
    'apache.atlas': atlas,
    'apache.beam': apache_beam,
    'apache.webhdfs': webhdfs,
    'async': async_packages,
    'cgroups': cgroups,
    'dask': dask,
    'github_enterprise': flask_oauth,
    'google_auth': flask_oauth,
    'kerberos': kerberos,
    'ldap': ldap,
    'password': password,
    'rabbitmq': rabbitmq,
    'sentry': sentry,
    'statsd': statsd,
    'tableau': tableau,
    'virtualenv': virtualenv,
}

# Add extras for all providers. For all providers the extras name = providers name
for provider_name, provider_requirement in PROVIDERS_REQUIREMENTS.items():
    EXTRAS_REQUIREMENTS[provider_name] = provider_requirement

#############################################################################################################
#  The whole section can be removed in Airflow 3.0 as those old aliases are deprecated in 2.* series
#############################################################################################################

# Dictionary of aliases from 1.10 - deprecated in Airflow 2.*
EXTRAS_DEPRECATED_ALIASES: Dict[str, str] = {
    'atlas': 'apache.atlas',
    'aws': 'amazon',
    'azure': 'microsoft.azure',
    'cassandra': 'apache.cassandra',
    'crypto': '',  # All crypto requirements are installation requirements of core Airflow
    'druid': 'apache.druid',
    'gcp': 'google',
    'gcp_api': 'google',
    'hdfs': 'apache.hdfs',
    'hive': 'apache.hive',
    'kubernetes': 'cncf.kubernetes',
    'mssql': 'microsoft.mssql',
    'pinot': 'apache.pinot',
    'qds': 'qubole',
    's3': 'amazon',
    'spark': 'apache.spark',
    'webhdfs': 'apache.webhdfs',
    'winrm': 'microsoft.winrm',
}


def find_requirements_for_alias(alias_to_look_for: Tuple[str, str]) -> List[str]:
    """Finds requirements for an alias"""
    deprecated_extra = alias_to_look_for[0]
    new_extra = alias_to_look_for[1]
    if new_extra == '':  # Handle case for crypto
        return []
    try:
        return EXTRAS_REQUIREMENTS[new_extra]
    except KeyError:  # noqa
        raise Exception(f"The extra {new_extra} is missing for alias {deprecated_extra}")


# Add extras for all deprecated aliases. Requirements for those deprecated aliases are the same
# as the extras they are replaced with
for alias, extra in EXTRAS_DEPRECATED_ALIASES.items():
    requirements = EXTRAS_REQUIREMENTS.get(extra) if extra != '' else []
    if requirements is None:
        raise Exception(f"The extra {extra} is missing for deprecated alias {alias}")
    # Note the requirements are not copies - those are the same lists as for the new extras. This is intended.
    # Thanks to that if the original extras are later extended with providers, aliases are extended as well.
    EXTRAS_REQUIREMENTS[alias] = requirements

#############################################################################################################
#  End of deprecated section
#############################################################################################################

# This is list of all providers. It's a shortcut for anyone who would like to easily get list of
# All providers. It is used by pre-commits.
ALL_PROVIDERS = list(PROVIDERS_REQUIREMENTS.keys())

ALL_DB_PROVIDERS = [
    'apache.cassandra',
    'apache.druid',
    'apache.hdfs',
    'apache.hive',
    'apache.pinot',
    'cloudant',
    'exasol',
    'microsoft.mssql',
    'mongo',
    'mysql',
    'postgres',
    'presto',
    'vertica',
]

# Special requirements for all database-related providers. They are de-duplicated.
all_dbs = list({req for db_provider in ALL_DB_PROVIDERS for req in PROVIDERS_REQUIREMENTS[db_provider]})

# Requirements for all "user" extras (no devel). They are de-duplicated. Note that we do not need
# to separately add providers requirements - they have been already added as 'providers' extras above
_all_requirements = list({req for extras_reqs in EXTRAS_REQUIREMENTS.values() for req in extras_reqs})

# All user extras here
EXTRAS_REQUIREMENTS["all"] = _all_requirements

# All db user extras here
EXTRAS_REQUIREMENTS["all_dbs"] = all_dbs

# This can be simplified to devel_hadoop + _all_requirements due to inclusions
# but we keep it for explicit sake. We are de-duplicating it anyway.
devel_all = list(set(_all_requirements + doc + devel_minreq + devel_hadoop))

# Those are packages excluded for "all" dependencies
PACKAGES_EXCLUDED_FOR_ALL = []
PACKAGES_EXCLUDED_FOR_ALL.extend(
    [
        'snakebite',
    ]
)

# Those packages are excluded because they break tests (downgrading mock) and they are
# not needed to run our test suite. This can be removed as soon as we get non-conflicting
# requirements for the apache-beam as well. This waits for azure + snowflake fixes:
#
# * Azure: https://github.com/apache/airflow/issues/11968
# * Snowflake: https://github.com/apache/airflow/issues/12881
#
PACKAGES_EXCLUDED_FOR_CI = [
    'apache-beam',
]


def is_package_excluded(package: str, exclusion_list: List[str]):
    """
    Checks if package should be excluded.

    :param package: package name (beginning of it)
    :param exclusion_list: list of excluded packages
    :return: true if package should be excluded
    """
    return any(package.startswith(excluded_package) for excluded_package in exclusion_list)


devel_all = [
    package
    for package in devel_all
    if not is_package_excluded(package=package, exclusion_list=PACKAGES_EXCLUDED_FOR_ALL)
]

devel_ci = [
    package
    for package in devel_all
    if not is_package_excluded(
        package=package, exclusion_list=PACKAGES_EXCLUDED_FOR_CI + PACKAGES_EXCLUDED_FOR_ALL
    )
]


# Those are extras that we have to add for development purposes
# They can be use to install some predefined set of dependencies.
EXTRAS_REQUIREMENTS["doc"] = doc
EXTRAS_REQUIREMENTS["devel"] = devel_minreq  # devel_minreq already includes doc
EXTRAS_REQUIREMENTS["devel_hadoop"] = devel_hadoop  # devel_hadoop already includes devel_minreq
EXTRAS_REQUIREMENTS["devel_all"] = devel_all
EXTRAS_REQUIREMENTS["devel_ci"] = devel_ci

# For Python 3.6+ the dictionary order remains when keys() are retrieved.
# Sort both: extras and list of dependencies to make it easier to analyse problems
# external packages will be first, then if providers are added they are added at the end of the lists.
EXTRAS_REQUIREMENTS = dict(sorted(EXTRAS_REQUIREMENTS.items()))  # noqa
for extra_list in EXTRAS_REQUIREMENTS.values():
    extra_list.sort()

# A set that keeps all extras that install some providers.
# It is used by pre-commit that verifies if documentation in docs/apache-airflow/extra-packages-ref.rst
# are synchronized.
EXTRAS_WITH_PROVIDERS: Set[str] = set()

# Those providers are pre-installed always when airflow is installed.
# Those providers do not have dependency on airflow2.0 because that would lead to circular dependencies.
# This is not a problem for PIP but some tools (pipdeptree) show that as a warning.
PREINSTALLED_PROVIDERS = [
    'ftp',
    'http',
    'imap',
    'sqlite',
]


def get_provider_package_from_package_id(package_id: str):
    """
    Builds the name of provider package out of the package id provided/

    :param package_id: id of the package (like amazon or microsoft.azure)
    :return: full name of package in PyPI
    """
    package_suffix = package_id.replace(".", "-")
    return f"apache-airflow-providers-{package_suffix}"


class AirflowDistribution(Distribution):
    """setuptools.Distribution subclass with Airflow specific behaviour"""

    # https://github.com/PyCQA/pylint/issues/3737
    def parse_config_files(self, *args, **kwargs):  # pylint: disable=signature-differs
        """
        Ensure that when we have been asked to install providers from sources
        that we don't *also* try to install those providers from PyPI
        """
        super().parse_config_files(*args, **kwargs)
        if os.getenv('INSTALL_PROVIDERS_FROM_SOURCES') == 'true':
            self.install_requires = [  # noqa  pylint: disable=attribute-defined-outside-init
                req for req in self.install_requires if not req.startswith('apache-airflow-providers-')
            ]
        else:
            self.install_requires.extend(
                [get_provider_package_from_package_id(package_id) for package_id in PREINSTALLED_PROVIDERS]
            )


def add_provider_packages_to_requirements(extra_with_providers: str, providers: List[str]):
    """
    Adds provider packages to requirements

    :param extra_with_providers: Name of the extra to add providers to
    :param providers: list of provider names
    """
    EXTRAS_WITH_PROVIDERS.add(extra_with_providers)
    EXTRAS_REQUIREMENTS[extra_with_providers].extend(
        [get_provider_package_from_package_id(package_name) for package_name in providers]
    )


def add_all_provider_packages():
    """
    In case of regular installation (when INSTALL_PROVIDERS_FROM_SOURCES is false), we should
    add extra dependencies to Airflow - to get the providers automatically installed when
    those extras are installed.

    """
    for provider in ALL_PROVIDERS:
        add_provider_packages_to_requirements(provider, [provider])
    add_provider_packages_to_requirements("all", ALL_PROVIDERS)
    add_provider_packages_to_requirements("devel_ci", ALL_PROVIDERS)
    add_provider_packages_to_requirements("devel_all", ALL_PROVIDERS)
    add_provider_packages_to_requirements("all_dbs", ALL_DB_PROVIDERS)
    add_provider_packages_to_requirements("devel_hadoop", ["apache.hdfs", "apache.hive", "presto"])


def do_setup():
    """Perform the Airflow package setup."""
    setup_kwargs = {}

    if os.getenv('INSTALL_PROVIDERS_FROM_SOURCES') == 'true':
        # Only specify this if we need this option, otherwise let default from
        # setup.cfg control this (kwargs in setup() call take priority)
        setup_kwargs['packages'] = find_namespace_packages(include=['airflow*'])
    else:
        add_all_provider_packages()
    write_version()
    setup(
        distclass=AirflowDistribution,
        # Most values come from setup.cfg -- see
        # https://setuptools.readthedocs.io/en/latest/userguide/declarative_config.html
        version=version,
        extras_require=EXTRAS_REQUIREMENTS,
        download_url=('https://archive.apache.org/dist/airflow/' + version),
        cmdclass={
            'extra_clean': CleanCommand,
            'compile_assets': CompileAssets,
            'list_extras': ListExtras,
        },
        test_suite='setup.airflow_test_suite',
        **setup_kwargs,
    )


if __name__ == "__main__":
    do_setup()
