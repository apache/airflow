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
import sys
import unittest
from os.path import dirname
from textwrap import wrap
from typing import Dict, Iterable, List

from setuptools import Command, find_namespace_packages, find_packages, setup

logger = logging.getLogger(__name__)

# This is automatically maintained in sync via pre-commit from airflow/version.py
version = '2.0.0b2'

PY3 = sys.version_info[0] == 3

my_dir = dirname(__file__)

try:
    with open(os.path.join(my_dir, 'README.md'), encoding='utf-8') as f:
        long_description = f.read()
except FileNotFoundError:
    long_description = ''


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


# 'Start dependencies group' and 'Start dependencies group' are mark for ./scripts/ci/check_order_setup.py
# If you change this mark you should also change ./scripts/ci/check_order_setup.py
# Start dependencies group
amazon = [
    'boto3>=1.12.0,<2.0.0',
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
    'azure-storage-blob<12.0',
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
    'sphinx-argparse>=0.1.13',
    'sphinx-autoapi==1.0.0',
    'sphinx-copybutton',
    'sphinx-jinja~=1.1',
    'sphinx-rtd-theme>=0.1.6',
    'sphinxcontrib-httpdomain>=1.7.0',
    "sphinxcontrib-redoc>=1.6.0",
    "sphinxcontrib-spelling==5.2.1",
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
    'requests-oauthlib==1.1.0',
]
google = [
    'PyOpenSSL',
    'google-ads>=4.0.0',
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
    'papermill[all]>=1.2.1',
    'nteract-scrapbook[all]>=0.3.1',
]
password = [
    'bcrypt>=2.0.0',
    'flask-bcrypt>=0.7.1',
]
pinot = [
    'pinotdb==0.1.1',
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
    'amqp',
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
    'snowflake-connector-python>=1.5.2',
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
yandexcloud = [
    'yandexcloud>=0.22.0',
]
zendesk = [
    'zdesk',
]
# End dependencies group

all_dbs = (
    cassandra
    + cloudant
    + druid
    + exasol
    + hdfs
    + hive
    + mongo
    + mssql
    + mysql
    + pinot
    + postgres
    + presto
    + vertica
)

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
    'ipdb',
    'jira',
    'mongomock',
    'moto==1.3.14',  # TODO - fix Datasync issues to get higher version of moto:
    #        See: https://github.com/apache/airflow/issues/10985
    'parameterized',
    'paramiko',
    'pipdeptree',
    'pre-commit',
    'pylint==2.5.3',
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
    'setuptools',
    'testfixtures',
    'wheel',
    'yamllint',
]
############################################################################################################
# IMPORTANT NOTE!!!!!!!!!!!!!!!
# IF you are removing dependencies from the above list, please make sure that you also increase
# DEPENDENCIES_EPOCH_NUMBER in the Dockerfile.ci
############################################################################################################

if PY3:
    devel += ['mypy==0.770']
else:
    devel += ['unittest2']

devel_minreq = cgroups + devel + doc + kubernetes + mysql + password
devel_hadoop = devel_minreq + hdfs + hive + kerberos + presto + webhdfs

PROVIDERS_REQUIREMENTS: Dict[str, Iterable[str]] = {
    "amazon": amazon,
    "apache.cassandra": cassandra,
    "apache.druid": druid,
    "apache.hdfs": hdfs,
    "apache.hive": hive,
    "apache.kylin": kylin,
    "apache.livy": [],
    "apache.pig": [],
    "apache.pinot": pinot,
    "apache.spark": spark,
    "apache.sqoop": [],
    "celery": celery,
    "cloudant": cloudant,
    "cncf.kubernetes": kubernetes,
    "databricks": databricks,
    "datadog": datadog,
    "dingding": [],
    "discord": [],
    "docker": docker,
    "elasticsearch": [],
    "exasol": exasol,
    "facebook": facebook,
    "ftp": [],
    "google": google,
    "grpc": grpc,
    "hashicorp": hashicorp,
    "http": [],
    "imap": [],
    "jdbc": jdbc,
    "jenkins": jenkins,
    "jira": jira,
    "microsoft.azure": azure,
    "microsoft.mssql": mssql,
    "microsoft.winrm": winrm,
    "mongo": mongo,
    "mysql": mysql,
    "odbc": odbc,
    "openfaas": [],
    "opsgenie": [],
    "oracle": oracle,
    "pagerduty": pagerduty,
    "papermill": papermill,
    "plexus": plexus,
    "postgres": postgres,
    "presto": presto,
    "qubole": qubole,
    "redis": redis,
    "salesforce": salesforce,
    "samba": samba,
    "segment": segment,
    "sendgrid": sendgrid,
    "sftp": ssh,
    "singularity": singularity,
    "slack": slack,
    "snowflake": snowflake,
    "sqlite": [],
    "ssh": ssh,
    "vertica": vertica,
    "yandex": yandexcloud,
    "zendesk": zendesk,
}

EXTRAS_REQUIREMENTS: Dict[str, List[str]] = {
    'all_dbs': all_dbs,
    'amazon': amazon,
    'apache.atlas': atlas,
    'apache.beam': apache_beam,
    "apache.cassandra": cassandra,
    "apache.druid": druid,
    "apache.hdfs": hdfs,
    "apache.hive": hive,
    "apache.kylin": kylin,
    "apache.pinot": pinot,
    "apache.spark": spark,
    "apache.webhdfs": webhdfs,
    'async': async_packages,
    'atlas': atlas,  # TODO: remove this in Airflow 2.1
    'aws': amazon,  # TODO: remove this in Airflow 2.1
    'azure': azure,  # TODO: remove this in Airflow 2.1
    'cassandra': cassandra,  # TODO: remove this in Airflow 2.1
    'celery': celery,
    'cgroups': cgroups,
    'cloudant': cloudant,
    'cncf.kubernetes': kubernetes,
    'dask': dask,
    'databricks': databricks,
    'datadog': datadog,
    'devel': devel_minreq,
    'devel_hadoop': devel_hadoop,
    'doc': doc,
    'docker': docker,
    'druid': druid,  # TODO: remove this in Airflow 2.1
    'elasticsearch': elasticsearch,
    'exasol': exasol,
    'facebook': facebook,
    'gcp': google,  # TODO: remove this in Airflow 2.1
    'gcp_api': google,  # TODO: remove this in Airflow 2.1
    'github_enterprise': flask_oauth,
    'google': google,
    'google_auth': flask_oauth,
    'grpc': grpc,
    'hashicorp': hashicorp,
    'hdfs': hdfs,  # TODO: remove this in Airflow 2.1
    'hive': hive,  # TODO: remove this in Airflow 2.1
    'jdbc': jdbc,
    'jira': jira,
    'kerberos': kerberos,
    'kubernetes': kubernetes,  # TODO: remove this in Airflow 2.1
    'ldap': ldap,
    "microsoft.azure": azure,
    "microsoft.mssql": mssql,
    "microsoft.winrm": winrm,
    'mongo': mongo,
    'mssql': mssql,  # TODO: remove this in Airflow 2.1
    'mysql': mysql,
    'odbc': odbc,
    'oracle': oracle,
    'pagerduty': pagerduty,
    'papermill': papermill,
    'password': password,
    'pinot': pinot,  # TODO: remove this in Airflow 2.1
    'plexus': plexus,
    'postgres': postgres,
    'presto': presto,
    'qds': qubole,  # TODO: remove this in Airflow 2.1
    'qubole': qubole,
    'rabbitmq': rabbitmq,
    'redis': redis,
    'salesforce': salesforce,
    'samba': samba,
    'segment': segment,
    'sendgrid': sendgrid,
    'sentry': sentry,
    'singularity': singularity,
    'slack': slack,
    'snowflake': snowflake,
    'spark': spark,
    'ssh': ssh,
    'statsd': statsd,
    'tableau': tableau,
    'vertica': vertica,
    'virtualenv': virtualenv,
    'webhdfs': webhdfs,  # TODO: remove this in Airflow 2.1
    'winrm': winrm,  # TODO: remove this in Airflow 2.1
    'yandexcloud': yandexcloud,
}

EXTRAS_PROVIDERS_PACKAGES: Dict[str, Iterable[str]] = {
    'all': list(PROVIDERS_REQUIREMENTS.keys()),
    # this is not 100% accurate with devel_ci definition, but we really want to have all providers
    # when devel_ci extra is installed!
    'devel_ci': list(PROVIDERS_REQUIREMENTS.keys()),
    'all_dbs': [
        "apache.cassandra",
        "apache.druid",
        "apache.hdfs",
        "apache.hive",
        "apache.pinot",
        "cloudant",
        "exasol",
        "mongo",
        "microsoft.mssql",
        "mysql",
        "postgres",
        "presto",
        "vertica",
    ],
    'amazon': ["amazon"],
    'apache.atlas': [],
    'apache.beam': [],
    "apache.cassandra": ["apache.cassandra"],
    "apache.druid": ["apache.druid"],
    "apache.hdfs": ["apache.hdfs"],
    "apache.hive": ["apache.hive"],
    "apache.kylin": ["apache.kylin"],
    "apache.pinot": ["apache.pinot"],
    "apache.spark": ["apache.spark"],
    "apache.webhdfs": ["apache.hdfs"],
    'async': [],
    'atlas': [],  # TODO: remove this in Airflow 2.1
    'aws': ["amazon"],  # TODO: remove this in Airflow 2.1
    'azure': ["microsoft.azure"],  # TODO: remove this in Airflow 2.1
    'cassandra': ["apache.cassandra"],  # TODO: remove this in Airflow 2.1
    'celery': ["celery"],
    'cgroups': [],
    'cloudant': ["cloudant"],
    'cncf.kubernetes': ["cncf.kubernetes"],
    'dask': [],
    'databricks': ["databricks"],
    'datadog': ["datadog"],
    'devel': ["cncf.kubernetes", "mysql"],
    'devel_hadoop': ["apache.hdfs", "apache.hive", "presto"],
    'doc': [],
    'docker': ["docker"],
    'druid': ["apache.druid"],  # TODO: remove this in Airflow 2.1
    'elasticsearch': ["elasticsearch"],
    'exasol': ["exasol"],
    'facebook': ["facebook"],
    'gcp': ["google"],  # TODO: remove this in Airflow 2.1
    'gcp_api': ["google"],  # TODO: remove this in Airflow 2.1
    'github_enterprise': [],
    'google': ["google"],
    'google_auth': [],
    'grpc': ["grpc"],
    'hashicorp': ["hashicorp"],
    'hdfs': ["apache.hdfs"],  # TODO: remove this in Airflow 2.1
    'hive': ["apache.hive"],  # TODO: remove this in Airflow 2.1
    'jdbc': ["jdbc"],
    'jira': ["jira"],
    'kerberos': [],
    'kubernetes': ["cncf.kubernetes"],  # TODO: remove this in Airflow 2.1
    'ldap': [],
    "microsoft.azure": ["microsoft.azure"],
    "microsoft.mssql": ["microsoft.mssql"],
    "microsoft.winrm": ["microsoft.winrm"],
    'mongo': ["mongo"],
    'mssql': ["microsoft.mssql"],  # TODO: remove this in Airflow 2.1
    'mysql': ["microsoft.mssql"],
    'odbc': ["odbc"],
    'oracle': ["oracle"],
    'pagerduty': ["pagerduty"],
    'papermill': ["papermill"],
    'password': [],
    'pinot': ["apache.pinot"],  # TODO: remove this in Airflow 2.1
    'plexus': ["plexus"],
    'postgres': ["postgres"],
    'presto': ["presto"],
    'qds': ["qubole"],  # TODO: remove this in Airflow 2.1
    'qubole': ["qubole"],
    'rabbitmq': [],
    'redis': ["redis"],
    'salesforce': ["salesforce"],
    'samba': ["samba"],
    'segment': ["segment"],
    'sendgrid': ["sendgrid"],
    'sentry': [],
    'singularity': ["singularity"],
    'slack': ["slack"],
    'snowflake': ["snowflake"],
    'spark': ["apache.spark"],
    'ssh': ["ssh"],
    'statsd': [],
    'tableau': [],
    'vertica': ["vertica"],
    'virtualenv': [],
    'webhdfs': ["apache.hdfs"],  # TODO: remove this in Airflow 2.1
    'winrm': ["microsoft.winrm"],  # TODO: remove this in Airflow 2.1
    'yandexcloud': ["yandex"],  # TODO: remove this in Airflow 2.1
    'yandex': ["yandex"],
}


# Make devel_all contain all providers + extras + unique
devel_all = list(
    set(
        devel
        + [req for req_list in EXTRAS_REQUIREMENTS.values() for req in req_list]
        + [req for req_list in PROVIDERS_REQUIREMENTS.values() for req in req_list]
    )
)

PACKAGES_EXCLUDED_FOR_ALL = []

if PY3:
    PACKAGES_EXCLUDED_FOR_ALL.extend(
        [
            'snakebite',
        ]
    )

# Those packages are excluded because they break tests (downgrading mock) and they are
# not needed to run our test suite.
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

EXTRAS_REQUIREMENTS.update(
    {
        'all': devel_all,
        'devel_ci': devel_ci,
    }
)

#####################################################################################################
# IMPORTANT NOTE!!!!!!!!!!!!!!!
# IF you are removing dependencies from this list, please make sure that you also increase
# DEPENDENCIES_EPOCH_NUMBER in the Dockerfile.ci
#####################################################################################################
INSTALL_REQUIREMENTS = [
    'alembic>=1.2, <2.0',
    'argcomplete~=1.10',
    'attrs>=20.0, <21.0',
    'cached_property~=1.5',
    # cattrs >= 1.1.0 dropped support for Python 3.6
    'cattrs>=1.0, <1.1.0;python_version<="3.6"',
    'cattrs>=1.0, <2.0;python_version>"3.6"',
    'colorlog==4.0.2',
    'connexion[swagger-ui,flask]>=2.6.0,<3',
    'croniter>=0.3.17, <0.4',
    'cryptography>=0.9.3',
    'dill>=0.2.2, <0.4',
    'flask>=1.1.0, <2.0',
    'flask-appbuilder~=3.1.1',
    'flask-caching>=1.5.0, <2.0.0',
    'flask-login>=0.3, <0.5',
    'flask-swagger==0.2.13',
    'flask-wtf>=0.14.3, <0.15',
    'funcsigs>=1.0.0, <2.0.0',
    'graphviz>=0.12',
    'gunicorn>=19.5.0, <20.0',
    'iso8601>=0.1.12',
    'jinja2>=2.10.1, <2.12.0',
    'json-merge-patch==0.2',
    'jsonschema~=3.0',
    'lazy_object_proxy~=1.3',
    'lockfile>=0.12.2',
    'markdown>=2.5.2, <4.0',
    'markupsafe>=1.1.1, <2.0',
    'marshmallow-oneofschema>=2.0.1',
    'pandas>=0.17.1, <2.0',
    'pendulum~=2.0',
    'pep562~=1.0;python_version<"3.7"',
    'psutil>=4.2.0, <6.0.0',
    'pygments>=2.0.1, <3.0',
    'python-daemon>=2.1.1',
    'python-dateutil>=2.3, <3',
    'python-nvd3~=0.15.0',
    'python-slugify>=3.0.0,<5.0',
    'requests>=2.20.0, <3',
    'rich==9.2.0',
    'setproctitle>=1.1.8, <2',
    'sqlalchemy>=1.3.18, <2',
    'sqlalchemy_jsonfield~=0.9',
    'tabulate>=0.7.5, <0.9',
    'tenacity~=6.2.0',
    'termcolor>=1.1.0',
    'thrift>=0.9.2',
    'typing;python_version<"3.6"',
    'typing-extensions>=3.7.4;python_version<"3.8"',
    'tzlocal>=1.4,<2.0.0',
    'unicodecsv>=0.14.1',
    'werkzeug~=1.0, >=1.0.1',
]


def get_provider_package_from_package_id(package_id: str):
    """
    Builds the name of provider package out of the package id provided/

    :param package_id: id of the package (like amazon or microsoft.azure)
    :return: full name of package in PyPI
    """
    package_suffix = package_id.replace(".", "-")
    return f"apache-airflow-providers-{package_suffix}"


def do_setup():
    """Perform the Airflow package setup."""
    install_providers_from_sources = os.getenv('INSTALL_PROVIDERS_FROM_SOURCES')
    exclude_patterns = (
        []
        if install_providers_from_sources and install_providers_from_sources == 'true'
        else ['airflow.providers', 'airflow.providers.*']
    )
    write_version()
    if not install_providers_from_sources:
        for key, value in EXTRAS_PROVIDERS_PACKAGES.items():
            EXTRAS_REQUIREMENTS[key].extend(
                [get_provider_package_from_package_id(package_name) for package_name in value]
            )
    packages_to_install = (
        find_namespace_packages(include=['airflow*'], exclude=exclude_patterns)
        if install_providers_from_sources
        else find_packages(include=['airflow*'], exclude=exclude_patterns)
    )
    setup(
        name='apache-airflow',
        description='Programmatically author, schedule and monitor data pipelines',
        long_description=long_description,
        long_description_content_type='text/markdown',
        license='Apache License 2.0',
        version=version,
        packages=packages_to_install,
        package_data={
            'airflow': ['py.typed'],
            '': [
                'airflow/alembic.ini',
                "airflow/git_version",
                "*.ipynb",
                "airflow/providers/cncf/kubernetes/example_dags/*.yaml",
            ],
            'airflow.api_connexion.openapi': ['*.yaml'],
            'airflow.serialization': ["*.json"],
        },
        include_package_data=True,
        zip_safe=False,
        entry_points={
            "console_scripts": [
                "airflow = airflow.__main__:main",
            ],
        },
        install_requires=INSTALL_REQUIREMENTS,
        setup_requires=[
            'bowler',
            'docutils',
            'gitpython',
            'setuptools',
            'wheel',
        ],
        extras_require=EXTRAS_REQUIREMENTS,
        classifiers=[
            'Development Status :: 5 - Production/Stable',
            'Environment :: Console',
            'Environment :: Web Environment',
            'Intended Audience :: Developers',
            'Intended Audience :: System Administrators',
            'License :: OSI Approved :: Apache Software License',
            'Programming Language :: Python :: 3.6',
            'Programming Language :: Python :: 3.7',
            'Programming Language :: Python :: 3.8',
            'Topic :: System :: Monitoring',
        ],
        author='Apache Software Foundation',
        author_email='dev@airflow.apache.org',
        url='http://airflow.apache.org/',
        download_url=('https://archive.apache.org/dist/airflow/' + version),
        cmdclass={
            'extra_clean': CleanCommand,
            'compile_assets': CompileAssets,
            'list_extras': ListExtras,
        },
        test_suite='setup.airflow_test_suite',
        python_requires='~=3.6',
        project_urls={
            'Documentation': 'https://airflow.apache.org/docs/',
            'Bug Tracker': 'https://github.com/apache/airflow/issues',
            'Source Code': 'https://github.com/apache/airflow',
        },
    )


if __name__ == "__main__":
    do_setup()
