# -*- coding: utf-8 -*-
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

import imp
import io
import logging
import os
import sys
import subprocess
import unittest

from setuptools import setup, find_packages, Command

logger = logging.getLogger(__name__)

# noinspection PyUnresolvedReferences
version = imp.load_source('airflow.version', os.path.join('airflow', 'version.py')).version  # type: ignore

PY3 = sys.version_info[0] == 3

if not PY3:
    # noinspection PyShadowingBuiltins
    FileNotFoundError = IOError

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
    user_options = []  # type: ignore

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
    Compile and build the frontend assets using npm and webpack.
    Registered as cmdclass in setup() so it can be called with ``python setup.py compile_assets``.
    """

    description = "Compile and build the frontend assets"
    user_options = []  # type: ignore

    def initialize_options(self):
        """Set default values for options."""

    def finalize_options(self):
        """Set final values for options."""

    # noinspection PyMethodMayBeStatic
    def run(self):
        subprocess.call('./airflow/www_rbac/compile_assets.sh')


def git_version(version_):
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


def write_version(filename=os.path.join(*["airflow", "git_version"])):
    """
    Write the Semver version + git hash to file, e.g. ".dev0+2f635dc265e78db6708f59f68e8009abb92c1e65".

    :param str filename: Destination file to write
    """
    text = "{}".format(git_version(version))
    with open(filename, 'w') as file:
        file.write(text)


async_packages = [
    'greenlet>=0.4.9',
    'eventlet>= 0.9.7',
    'gevent>=0.13'
]
atlas = ['atlasclient>=0.1.2']
azure_blob_storage = ['azure-storage>=0.34.0']
azure_data_lake = [
    'azure-mgmt-resource>=2.2.0',
    'azure-mgmt-datalake-store>=0.5.0',
    'azure-datalake-store>=0.0.45'
]
azure_cosmos = ['azure-cosmos>=3.0.1']
azure_container_instances = ['azure-mgmt-containerinstance>=1.5.0']
cassandra = ['cassandra-driver>=3.13.0']
celery = [
    'celery~=4.3',
    'flower>=0.7.3, <1.0',
    'tornado>=4.2.0, <6.0',  # Dep of flower. Pin to a version that works on Py3.5.2
    'kombu==4.6.3'
]
cgroups = [
    'cgroupspy>=0.1.4',
]
# major update coming soon, clamp to 0.x
cloudant = ['cloudant>=0.5.9,<2.0']
crypto = ['cryptography>=0.9.3']
dask = [
    'distributed>=1.17.1, <2'
]
databricks = ['requests>=2.20.0, <3']
datadog = ['datadog>=0.14.0']
doc = [
    'sphinx-argparse>=0.1.13',
    'sphinx-autoapi==1.0.0',
    'sphinx-rtd-theme>=0.1.6',
    'sphinx>=2.1.2;python_version>="3.0"',
    'sphinx==1.8.5;python_version<"3.0"',
    'sphinxcontrib-httpdomain>=1.7.0',
]
docker = ['docker~=3.0']
druid = ['pydruid>=0.4.1']
elasticsearch = [
    'elasticsearch>=5.0.0,<6.0.0',
    'elasticsearch-dsl>=5.0.0,<6.0.0'
]
emr = ['boto3>=1.0.0, <1.8.0']
gcp = [
    'google-api-python-client>=1.6.0, <2.0.0dev',
    'google-auth-httplib2>=0.0.1',
    'google-auth>=1.0.0, <2.0.0dev',
    'google-cloud-bigtable==0.33.0',
    'google-cloud-container>=0.1.1',
    'google-cloud-dlp>=0.11.0',
    'google-cloud-language>=1.1.1',
    'google-cloud-spanner>=1.7.1, <1.10.0',
    'google-cloud-storage~=1.16',
    'google-cloud-translate>=1.3.3',
    'google-cloud-videointelligence>=1.7.0',
    'google-cloud-vision>=0.35.2',
    'google-cloud-texttospeech>=0.4.0',
    'google-cloud-speech>=0.36.3',
    'grpcio-gcp>=0.2.2',
    'httplib2~=0.9',
    'pandas-gbq',
    'PyOpenSSL',
]
grpc = ['grpcio>=1.15.0']
flask_oauth = [
    'Flask-OAuthlib>=0.9.1',
    'oauthlib!=2.0.3,!=2.0.4,!=2.0.5,<3.0.0,>=1.1.2',
    'requests-oauthlib==1.1.0'
]
hdfs = ['snakebite>=2.7.8']
hive = [
    'hmsclient>=0.1.0',
    'pyhive>=0.6.0',
]
jdbc = ['jaydebeapi>=1.1.1']
jenkins = ['python-jenkins>=1.0.0']
jira = ['JIRA>1.0.7']
kerberos = ['pykerberos>=1.1.13',
            'requests_kerberos>=0.10.0',
            'thrift_sasl>=0.2.0',
            'snakebite[kerberos]>=2.7.8']
kubernetes = ['kubernetes>=3.0.0',
              'cryptography>=2.0.0']
ldap = ['ldap3>=2.5.1']
mssql = ['pymssql>=2.1.1']
mysql = ['mysqlclient>=1.3.6,<1.4']
oracle = ['cx_Oracle>=5.1.2']
papermill = ['papermill[all]>=1.0.0',
             'nteract-scrapbook[all]>=0.2.1']
password = [
    'bcrypt>=2.0.0',
    'flask-bcrypt>=0.7.1',
]
pinot = ['pinotdb==0.1.1']
postgres = ['psycopg2-binary>=2.7.4']
qds = ['qds-sdk>=1.10.4']
rabbitmq = ['librabbitmq>=1.6.1']
redis = ['redis~=3.2']
s3 = ['boto3>=1.7.0, <1.8.0']
salesforce = ['simple-salesforce>=0.72']
samba = ['pysmbclient>=0.1.3']
segment = ['analytics-python>=1.2.9']
sendgrid = ['sendgrid>=5.2.0,<6']
sentry = ['sentry-sdk>=0.8.0', "blinker>=1.1"]
slack = ['slackclient>=1.0.0,<2.0.0']
mongo = ['pymongo>=3.6.0', 'dnspython>=1.13.0,<2.0.0']
snowflake = ['snowflake-connector-python>=1.5.2',
             'snowflake-sqlalchemy>=1.1.0']
ssh = ['paramiko>=2.1.1', 'pysftp>=0.2.9', 'sshtunnel>=0.1.4,<0.2']
statsd = ['statsd>=3.3.0, <4.0']
vertica = ['vertica-python>=0.5.1']
virtualenv = ['virtualenv']
webhdfs = ['hdfs[dataframe,avro,kerberos]>=2.0.4']
winrm = ['pywinrm==0.2.2']
zendesk = ['zdesk']

all_dbs = postgres + mysql + hive + mssql + hdfs + vertica + cloudant + druid + pinot \
    + cassandra + mongo

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
    'dumb-init>=1.2.2',
    'flake8>=3.6.0',
    'flake8-colors',
    'freezegun',
    'ipdb',
    'jira',
    'mock;python_version<"3.3"',
    'mongomock',
    'moto==1.3.5',
    'nose',
    'nose-ignore-docstring==0.2',
    'nose-timer',
    'parameterized',
    'paramiko',
    'pre-commit',
    'pysftp',
    'pywinrm',
    'qds-sdk>=1.9.6',
    'rednose',
    'requests_mock',
    'yamllint'
]
############################################################################################################
# IMPORTANT NOTE!!!!!!!!!!!!!!!
# IF you are removing dependencies from the above list, please make sure that you also increase
# DEPENDENCIES_EPOCH_NUMBER in the Dockerfile
############################################################################################################

if PY3:
    devel += ['mypy==0.720']
else:
    devel += ['unittest2']

devel_minreq = devel + kubernetes + mysql + doc + password + s3 + cgroups
devel_hadoop = devel_minreq + hive + hdfs + webhdfs + kerberos
devel_azure = devel_minreq + azure_data_lake + azure_cosmos
devel_all = (sendgrid + devel + all_dbs + doc + samba + s3 + slack + crypto + oracle +
             docker + ssh + kubernetes + celery + azure_blob_storage + redis + gcp + grpc +
             datadog + zendesk + jdbc + ldap + kerberos + password + webhdfs + jenkins +
             druid + pinot + segment + snowflake + elasticsearch + sentry + azure_data_lake + azure_cosmos +
             atlas + azure_container_instances + cgroups + papermill + virtualenv)

# Snakebite & Google Cloud Dataflow are not Python 3 compatible :'(
if PY3:
    devel_all = [package for package in devel_all if package not in
                 ['snakebite>=2.7.8', 'snakebite[kerberos]>=2.7.8']]
    devel_ci = devel_all

else:
    devel_ci = devel_all + ['unittest2']


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
            '': ['airflow/alembic.ini', "airflow/git_version"],
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
            'alembic>=1.0, <2.0',
            'argcomplete~=1.10',
            'cached_property~=1.5',
            'colorlog==4.0.2',
            'configparser>=3.5.0, <3.6.0',
            'croniter>=0.3.17, <0.4',
            'dill>=0.2.2, <0.4',
            'enum34~=1.1.6;python_version<"3.4"',
            'flask>=1.1.0, <2.0',
            'flask-appbuilder>=1.12.5, <2.0.0',
            'flask-admin==1.5.3',
            'flask-caching>=1.3.3, <1.4.0',
            'flask-login>=0.3, <0.5',
            'flask-swagger==0.2.13',
            'flask-wtf>=0.14.2, <0.15',
            'funcsigs==1.0.0',
            'future>=0.16.0, <0.17',
            'graphviz>=0.12',
            'gunicorn>=19.5.0, <20.0',
            'iso8601>=0.1.12',
            'jsonschema~=3.0',
            'json-merge-patch==0.2',
            'jinja2>=2.10.1, <2.11.0',
            'lazy_object_proxy~=1.3',
            'markdown>=2.5.2, <3.0',
            'marshmallow-sqlalchemy>=0.16.1, <0.19.0',
            'pandas>=0.17.1, <1.0.0',
            'pendulum==1.4.4',
            'psutil>=4.2.0, <6.0.0',
            'pygments>=2.0.1, <3.0',
            'python-daemon>=2.1.1, <2.2',
            'python-dateutil>=2.3, <3',
            'requests>=2.20.0, <3',
            'setproctitle>=1.1.8, <2',
            'sqlalchemy~=1.3',
            'tabulate>=0.7.5, <0.9',
            'tenacity==4.12.0',
            'termcolor==1.1.0',
            'text-unidecode==1.2',
            'typing;python_version<"3.5"',
            'typing-extensions>=3.7.4;python_version<"3.8"',
            'thrift>=0.9.2',
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
            'devel_ci': devel_ci,
            'all_dbs': all_dbs,
            'atlas': atlas,
            'async': async_packages,
            'azure_blob_storage': azure_blob_storage,
            'azure_data_lake': azure_data_lake,
            'azure_cosmos': azure_cosmos,
            'azure_container_instances': azure_container_instances,
            'cassandra': cassandra,
            'celery': celery,
            'cgroups': cgroups,
            'cloudant': cloudant,
            'crypto': crypto,
            'dask': dask,
            'databricks': databricks,
            'datadog': datadog,
            'devel': devel_minreq,
            'devel_hadoop': devel_hadoop,
            'devel_azure': devel_azure,
            'doc': doc,
            'docker': docker,
            'druid': druid,
            'elasticsearch': elasticsearch,
            'emr': emr,
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
            'oracle': oracle,
            'papermill': papermill,
            'password': password,
            'pinot': pinot,
            'postgres': postgres,
            'qds': qds,
            'rabbitmq': rabbitmq,
            'redis': redis,
            's3': s3,
            'salesforce': salesforce,
            'samba': samba,
            'sendgrid': sendgrid,
            'sentry': sentry,
            'segment': segment,
            'slack': slack,
            'snowflake': snowflake,
            'ssh': ssh,
            'statsd': statsd,
            'vertica': vertica,
            'virtualenv': virtualenv,
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
            'Programming Language :: Python :: 2.7',
            'Programming Language :: Python :: 3.5',
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
        python_requires='>=2.7,!=3.0.*,!=3.1.*,!=3.2.*,!=3.3.*,!=3.4.*',
    )


if __name__ == "__main__":
    # Warn about py2 support going away. This likely won't show up if installed
    # via pip, but we may as well have it here
    if sys.version_info[0] == 2:
        sys.stderr.writelines(
            "DEPRECATION: Python 2.7 will reach the end of its life on January 1st, 2020. Airflow 1.10 "
            "will be the last release series to support Python 2\n"
        )
    do_setup()
