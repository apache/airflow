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

import io
import logging
import os
import subprocess
import sys
import unittest
from importlib import util
from typing import List

from setuptools import Command, find_packages, setup

logger = logging.getLogger(__name__)

# Kept manually in sync with airflow.__version__
# noinspection PyUnresolvedReferences
spec = util.spec_from_file_location("airflow.version", os.path.join('airflow', 'version.py'))
# noinspection PyUnresolvedReferences
mod = util.module_from_spec(spec)
spec.loader.exec_module(mod)  # type: ignore
version = mod.version  # type: ignore

PY3 = sys.version_info[0] == 3

# noinspection PyUnboundLocalVariable
try:
    with io.open('README.md', encoding='utf-8') as f:
        long_description = f.read()
except FileNotFoundError:
    long_description = ''


def airflow_test_suite():
    """Test suite for Airflow tests"""
    test_loader = unittest.TestLoader()
    test_suite = test_loader.discover('tests', pattern='test_*.py')
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

    # noinspection PyMethodMayBeStatic
    def run(self):
        """Run command to remove temporary files and directories."""
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

    # noinspection PyMethodMayBeStatic
    def run(self):
        """Run a command to compile and build assets."""
        subprocess.check_call('./airflow/www/compile_assets.sh')


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
            repo = git.Repo('.git')
        except git.NoSuchPathError:
            logger.warning('.git directory not found: Cannot compute the git version')
            return ''
    except ImportError:
        logger.warning('gitpython not found: Cannot compute the git version.')
        return ''
    if repo:
        sha = repo.head.commit.hexsha
        if repo.is_dirty():
            return '.dev0+{sha}.dirty'.format(sha=sha)
        # commit is clean
        return '.release:{version}+{sha}'.format(version=version_, sha=sha)
    else:
        return 'no_git_version'


def write_version(filename: str = os.path.join(*["airflow", "git_version"])):
    """
    Write the Semver version + git hash to file, e.g. ".dev0+2f635dc265e78db6708f59f68e8009abb92c1e65".

    :param str filename: Destination file to write
    """
    text = "{}".format(git_version(version))
    with open(filename, 'w') as file:
        file.write(text)


# 'Start dependencies group' and 'Start dependencies group' are mark for ./test/test_order_setup.py
# If you change this mark you should also change ./test/test_order_setup.py function test_main_dependent_group
# Start dependencies group
async_packages = [
    'eventlet>= 0.9.7',
    'gevent>=0.13',
    'greenlet>=0.4.9',
]
atlas = [
    'atlasclient>=0.1.2',
]
aws = [
    'boto3~=1.10',
]
azure = [
    'azure-cosmos>=3.0.1',
    'azure-datalake-store>=0.0.45',
    'azure-mgmt-containerinstance>=1.5.0',
    'azure-mgmt-datalake-store>=0.5.0',
    'azure-mgmt-resource>=2.2.0',
    'azure-storage>=0.34.0',
    'azure-storage-blob<12.0',
]
cassandra = [
    'cassandra-driver>=3.13.0,<3.21.0',
]
celery = [
    'celery~=4.3',
    'flower>=0.7.3, <1.0',
    'tornado>=4.2.0, <6.0',  # Dep of flower. Pin to a version that works on Py3.5.2
]
cgroups = [
    'cgroupspy>=0.1.4',
]
cloudant = [
    'cloudant>=2.0',
]
dask = [
    'distributed>=1.17.1, <2',
]
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
    'sphinx-jinja~=1.1',
    'sphinx-rtd-theme>=0.1.6',
    'sphinxcontrib-httpdomain>=1.7.0',
]
docker = [
    'docker~=3.0',
]
druid = [
    'pydruid>=0.4.1',
]
elasticsearch = [
    'elasticsearch>=5.0.0,<6.0.0',
    'elasticsearch-dsl>=5.0.0,<6.0.0',
]
flask_oauth = [
    'Flask-OAuthlib>=0.9.1',
    'oauthlib!=2.0.3,!=2.0.4,!=2.0.5,<3.0.0,>=1.1.2',
    'requests-oauthlib==1.1.0',
]
gcp = [
    'PyOpenSSL',
    'google-api-python-client>=1.6.0, <2.0.0dev',
    'google-auth>=1.0.0, <2.0.0dev',
    'google-auth-httplib2>=0.0.1',
    'google-cloud-automl>=0.4.0',
    'google-cloud-bigquery-datatransfer>=0.4.0',
    'google-cloud-bigtable>=1.0.0',
    'google-cloud-container>=0.1.1',
    'google-cloud-dataproc>=0.5.0',
    'google-cloud-dlp>=0.11.0',
    'google-cloud-kms>=1.2.1',
    'google-cloud-language>=1.1.1',
    'google-cloud-pubsub>=1.0.0',
    'google-cloud-redis>=0.3.0',
    'google-cloud-spanner>=1.10.0',
    'google-cloud-speech>=0.36.3',
    'google-cloud-storage>=1.16',
    'google-cloud-tasks>=1.2.1',
    'google-cloud-texttospeech>=0.4.0',
    'google-cloud-translate>=1.5.0',
    'google-cloud-videointelligence>=1.7.0',
    'google-cloud-vision>=0.35.2',
    'grpcio-gcp>=0.2.2',
    'httplib2~=0.15',  # not sure we're ready for 1.0 here; test before updating
    'pandas-gbq',
]
grpc = [
    'grpcio>=1.15.0',
]
hdfs = [
    'snakebite>=2.7.8',
]
hive = [
    'hmsclient>=0.1.0',
    'pyhive>=0.6.0',
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
    'snakebite[kerberos]>=2.7.8',
    'thrift_sasl>=0.2.0',
]
kubernetes = [
    'cryptography>=2.0.0',
    'kubernetes>=3.0.0',
]
ldap = [
    'ldap3>=2.5.1',
]
mongo = [
    'dnspython>=1.13.0,<2.0.0',
    'pymongo>=3.6.0',
]
mssql = [
    'pymssql~=2.1.1',
]
mysql = [
    'mysqlclient>=1.3.6,<1.4',
]
odbc = [
    'pyodbc',
]
oracle = [
    'cx_Oracle>=5.1.2',
]
pagerduty = [
    'pypd>=1.1.0',
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
postgres = [
    'psycopg2-binary>=2.7.4',
]
presto = [
    'presto-python-client>=0.7.0,<0.8'
]
qds = [
    'qds-sdk>=1.10.4',
]
rabbitmq = [
    'librabbitmq>=1.6.1',
]
redis = [
    'redis~=3.2',
]
salesforce = [
    'simple-salesforce>=0.72',
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
slack = [
    'slackclient>=1.0.0,<2.0.0',
]
snowflake = [
    'snowflake-connector-python>=1.5.2',
    'snowflake-sqlalchemy>=1.1.0',
]
ssh = [
    'paramiko>=2.1.1',
    'pysftp>=0.2.9',
    'sshtunnel>=0.1.4,<0.2',
]
statsd = [
    'statsd>=3.3.0, <4.0',
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
zendesk = [
    'zdesk',
]
# End dependencies group

all_dbs = (cassandra + cloudant + druid + hdfs + hive + mongo + mssql + mysql +
           pinot + postgres + presto + vertica)

############################################################################################################
# IMPORTANT NOTE!!!!!!!!!!!!!!!
# IF you are removing dependencies from this list, please make sure that you also increase
# DEPENDENCIES_EPOCH_NUMBER in the Dockerfile
############################################################################################################
devel = [
    'beautifulsoup4~=4.7.1',
    'click==6.7',
    'contextdecorator;python_version<"3.4"',
    'coverage',
    'flake8>=3.6.0',
    'flake8-colors',
    'flaky',
    'freezegun',
    'ipdb',
    'jira',
    'mongomock',
    'moto>=1.3.14,<2.0.0',
    'parameterized',
    'paramiko',
    'pre-commit',
    'pylint~=2.4',
    'pysftp',
    'pytest',
    'pytest-cov',
    'pytest-instafail',
    'pywinrm',
    'qds-sdk>=1.9.6',
    'requests_mock',
    'yamllint',
]
############################################################################################################
# IMPORTANT NOTE!!!!!!!!!!!!!!!
# IF you are removing dependencies from the above list, please make sure that you also increase
# DEPENDENCIES_EPOCH_NUMBER in the Dockerfile
############################################################################################################

if PY3:
    devel += ['mypy==0.740']
else:
    devel += ['unittest2']

devel_minreq = cgroups + devel + doc + kubernetes + mysql + password
devel_hadoop = devel_minreq + hdfs + hive + kerberos + presto + webhdfs
devel_all = (all_dbs + atlas + aws + azure + celery + cgroups + datadog + devel +
             doc + docker + druid + elasticsearch + gcp + grpc + jdbc + jenkins +
             kerberos + kubernetes + ldap + odbc + oracle + pagerduty + papermill +
             password + pinot + redis + salesforce + samba + segment + sendgrid +
             sentry + slack + snowflake + ssh + statsd + virtualenv + webhdfs + zendesk)

# Snakebite are not Python 3 compatible :'(
if PY3:
    devel_ci = [package for package in devel_all if package not in
                ['snakebite>=2.7.8', 'snakebite[kerberos]>=2.7.8']]
else:
    devel_ci = devel_all


def do_setup():
    """Perform the Airflow package setup."""
    write_version()
    setup(
        name='apache-airflow',
        description='Programmatically author, schedule and monitor data pipelines',
        long_description=long_description,
        long_description_content_type='text/markdown',
        license='Apache License 2.0',
        version=version,
        packages=find_packages(exclude=['tests*']),
        package_data={
            '': ['airflow/alembic.ini', "airflow/git_version", "*.ipynb"],
            'airflow.serialization': ["*.json"],
        },
        include_package_data=True,
        zip_safe=False,
        scripts=['airflow/bin/airflow'],
        #####################################################################################################
        # IMPORTANT NOTE!!!!!!!!!!!!!!!
        # IF you are removing dependencies from this list, please make sure that you also increase
        # DEPENDENCIES_EPOCH_NUMBER in the Dockerfile
        #####################################################################################################
        install_requires=[
            'alembic>=1.2, <2.0',
            'argcomplete~=1.10',
            'attrs~=19.3',
            'cached_property~=1.5',
            'cattrs~=1.0',
            'colorlog==4.0.2',
            'croniter>=0.3.17, <0.4',
            'cryptography>=0.9.3',
            'dill>=0.2.2, <0.4',
            'flask>=1.1.0, <2.0',
            'flask-appbuilder~=2.2',
            'flask-caching>=1.3.3, <1.4.0',
            'flask-login>=0.3, <0.5',
            'flask-swagger==0.2.13',
            'flask-wtf>=0.14.2, <0.15',
            'funcsigs>=1.0.0, <2.0.0',
            'graphviz>=0.12',
            'gunicorn>=19.5.0, <20.0',
            'iso8601>=0.1.12',
            'jinja2>=2.10.1, <2.11.0',
            'json-merge-patch==0.2',
            'jsonschema~=3.0',
            'lazy_object_proxy~=1.3',
            'lockfile>=0.12.2',
            'markdown>=2.5.2, <3.0',
            'pandas>=0.17.1, <1.0.0',
            'pendulum==1.4.4',
            'psutil>=4.2.0, <6.0.0',
            'pygments>=2.0.1, <3.0',
            'python-daemon>=2.1.1, <2.2',
            'python-dateutil>=2.3, <3',
            'requests>=2.20.0, <3',
            'setproctitle>=1.1.8, <2',
            'sqlalchemy~=1.3',
            'sqlalchemy_jsonfield~=0.9',
            'tabulate>=0.7.5, <0.9',
            'tenacity==4.12.0',
            'termcolor==1.1.0',
            'text-unidecode==1.3',
            'thrift>=0.9.2',
            'typing;python_version<"3.6"',
            'typing-extensions>=3.7.4;python_version<"3.8"',
            'tzlocal>=1.4,<2.0.0',
            'unicodecsv>=0.14.1',
            'zope.deprecation>=4.0, <5.0',
        ],
        #####################################################################################################
        # IMPORTANT NOTE!!!!!!!!!!!!!!!
        # IF you are removing dependencies from this list, please make sure that you also increase
        # DEPENDENCIES_EPOCH_NUMBER in the Dockerfile
        #####################################################################################################
        setup_requires=[
            'docutils>=0.14, <1.0',
            'gitpython>=2.0.2',
        ],
        extras_require={
            'all': devel_all,
            'all_dbs': all_dbs,
            'async': async_packages,
            'atlas': atlas,
            'aws': aws,
            'azure': azure,
            'cassandra': cassandra,
            'celery': celery,
            'cgroups': cgroups,
            'cloudant': cloudant,
            'dask': dask,
            'databricks': databricks,
            'datadog': datadog,
            'devel': devel_minreq,
            'devel_ci': devel_ci,
            'devel_hadoop': devel_hadoop,
            'doc': doc,
            'docker': docker,
            'druid': druid,
            'elasticsearch': elasticsearch,
            'gcp': gcp,
            'gcp_api': gcp,  # TODO: remove this in Airflow 2.1
            'github_enterprise': flask_oauth,
            'google_auth': flask_oauth,
            'grpc': grpc,
            'hdfs': hdfs,
            'hive': hive,
            'jdbc': jdbc,
            'jira': jira,
            'kerberos': kerberos,
            'kubernetes': kubernetes,
            'ldap': ldap,
            'mongo': mongo,
            'mssql': mssql,
            'mysql': mysql,
            'odbc': odbc,
            'oracle': oracle,
            'pagerduty': pagerduty,
            'papermill': papermill,
            'password': password,
            'pinot': pinot,
            'postgres': postgres,
            'presto': presto,
            'qds': qds,
            'rabbitmq': rabbitmq,
            'redis': redis,
            'salesforce': salesforce,
            'samba': samba,
            'segment': segment,
            'sendgrid': sendgrid,
            'sentry': sentry,
            'slack': slack,
            'snowflake': snowflake,
            'ssh': ssh,
            'statsd': statsd,
            'vertica': vertica,
            'webhdfs': webhdfs,
            'winrm': winrm,
        },
        classifiers=[
            'Development Status :: 5 - Production/Stable',
            'Environment :: Console',
            'Environment :: Web Environment',
            'Intended Audience :: Developers',
            'Intended Audience :: System Administrators',
            'License :: OSI Approved :: Apache Software License',
            'Programming Language :: Python :: 3.6',
            'Programming Language :: Python :: 3.7',
            'Topic :: System :: Monitoring',
        ],
        author='Apache Software Foundation',
        author_email='dev@airflow.apache.org',
        url='http://airflow.apache.org/',
        download_url=(
            'https://dist.apache.org/repos/dist/release/airflow/' + version),
        cmdclass={
            'extra_clean': CleanCommand,
            'compile_assets': CompileAssets
        },
        test_suite='setup.airflow_test_suite',
        python_requires='~=3.6',
    )


if __name__ == "__main__":
    do_setup()
