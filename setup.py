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
from os.path import dirname
from textwrap import wrap

from setuptools import setup, find_packages, Command

logger = logging.getLogger(__name__)

# noinspection PyUnresolvedReferences
version = imp.load_source('airflow.version', os.path.join('airflow', 'version.py')).version  # type: ignore

PY3 = sys.version_info[0] == 3
PY38 = PY3 and sys.version_info[1] >= 8

my_dir = dirname(__file__)

if not PY3:
    # noinspection PyShadowingBuiltins
    FileNotFoundError = IOError

# noinspection PyUnboundLocalVariable
try:
    with io.open(os.path.join(my_dir, 'README.md'), encoding='utf-8') as f:
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
    user_options = []  # type: ignore

    def initialize_options(self):
        """Set default values for options."""

    def finalize_options(self):
        """Set final values for options."""

    # noinspection PyMethodMayBeStatic
    def run(self):
        """Run command to remove temporary files and directories."""
        os.chdir(my_dir)
        os.system('rm -vrf ./build ./dist ./*.pyc ./*.tgz ./*.egg-info')


class CompileAssets(Command):
    """
    Compile and build the frontend assets using yarn and webpack.
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
        """Run a command to compile and build assets."""
        subprocess.check_call('./airflow/www_rbac/compile_assets.sh')


class ListExtras(Command):
    """
    List all available extras
    Registered as cmdclass in setup() so it can be called with ``python setup.py list_extras``.
    """

    description = "List available extras"
    user_options = []  # type: ignore

    def initialize_options(self):
        """Set default values for options."""

    def finalize_options(self):
        """Set final values for options."""

    # noinspection PyMethodMayBeStatic
    def run(self):
        """List extras."""
        print("\n".join(wrap(", ".join(EXTRAS_REQUIREMENTS.keys()), 100)))


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
            return '.dev0+{sha}.dirty'.format(sha=sha)
        # commit is clean
        return '.release:{version}+{sha}'.format(version=version_, sha=sha)
    else:
        return 'no_git_version'


def write_version(filename=os.path.join(*[my_dir, "airflow", "git_version"])):
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
    'boto3~=1.10,<1.11',  # required by snowflake
]
azure_blob_storage = [
    'azure-storage>=0.34.0, <0.37.0',
    'azure-storage-blob<12.0.0;python_version<"3.6"',
    'azure-storage-blob;python_version>="3.6"',
    'azure-storage-common',
]
azure_container_instances = [
    'azure-mgmt-containerinstance>=1.5.0,<2'
]
azure_cosmos = [
    'azure-cosmos>=3.0.1,<4',
]
azure_data_lake = [
    'azure-datalake-store>=0.0.45'
    'azure-mgmt-datalake-store>=0.5.0',
    'azure-mgmt-resource>=2.2.0',
    'cffi<1.14.0;python_version<"3.0"'
]
azure_secrets = [
    'azure-identity>=1.3.1',
    'azure-keyvault>=4.1.0',
]
cassandra = [
    'cassandra-driver>=3.13.0,<3.21.0',
]
celery = [
    'celery~=4.3;python_version>="3.0"',
    'celery==4.3.1;python_version<"3.0"',
    'flower>=0.7.3, <1.0',
    'kombu==4.6.3;python_version<"3.0"',
    'tornado>=4.2.0, <6.0',  # Dep of flower. Pin to a version that works on Py3.5.2
    'vine~=1.3',  # https://stackoverflow.com/questions/32757259/celery-no-module-named-five
]
cgroups = [
    'cgroupspy>=0.1.4',
]
cloudant = [
    'cloudant>=0.5.9,<2.0',
]
crypto = [
    # Cryptography 3.2 for python 2.7 is broken
    # https://github.com/pyca/cryptography/issues/5359#issuecomment-727622403
    # Snowflake requires <3.0
    'cryptography>=0.9.3,<3.0; python_version<"3.0"',
    'cryptography>=0.9.3;python_version>="3.0"',
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
    'sphinx>=2.1.2;python_version>="3.0"',
    'sphinx==1.8.5;python_version<"3.0"',
    'sphinx-argparse>=0.1.13',
    'sphinx-autoapi==1.0.0',
    'sphinx-copybutton;python_version>="3.6"',
    'sphinx-jinja~=1.1',
    'sphinx-rtd-theme>=0.1.6',
    'sphinxcontrib-httpdomain>=1.7.0',
]
docker = [
    'docker~=3.0',
]
druid = [
    'pydruid>=0.4.1,<=0.5.8',
]
elasticsearch = [
    'elasticsearch>=5.0.0,<6.0.0',
    'elasticsearch-dsl>=5.0.0,<6.0.0',
]
flask_oauth = [
    'Flask-OAuthlib>=0.9.1,<0.9.6',  # Flask OAuthLib 0.9.6 requires Flask-Login 0.5.0 - breaks FAB
    'oauthlib!=2.0.3,!=2.0.4,!=2.0.5,<3.0.0,>=1.1.2',
    'requests-oauthlib==1.1.0',
]
gcp = [
    'PyOpenSSL<20.0.0;python_version<"3.0"',
    'PyOpenSSL;python_version>="3.0"',
    'google-api-python-client>=1.6.0, <2.0.0',
    'google-auth>=1.0.0, <2.0.0',
    'google-auth-httplib2>=0.0.1',
    'google-cloud-bigtable>=1.0.0,<2.0.0',
    'google-cloud-container>=0.1.1,<2.0.0',
    'google-cloud-dlp>=0.11.0,<2.0.0',
    'google-cloud-language>=1.1.1,<2.0.0',
    'google-cloud-secret-manager>=0.2.0,<2.0.0',
    'google-cloud-spanner>=1.10.0,<2.0.0',
    'google-cloud-speech>=0.36.3,<2.0.0',
    'google-cloud-storage>=1.16,<2.0.0',
    'google-cloud-texttospeech>=0.4.0,<2',
    'google-cloud-translate>=1.3.3,<2.0.0',
    'google-cloud-videointelligence>=1.7.0,<2.0.0',
    'google-cloud-vision>=0.35.2,<2.0.0',
    'grpcio-gcp>=0.2.2',
    'pandas-gbq',
]
grpc = [
    'grpcio>=1.15.0',
]
hashicorp = [
    'hvac~=0.10',
]
hdfs = [
    'snakebite>=2.7.8;python_version<"3.0"',
    'snakebite-py3;python_version>="3.0"'
]
hive = [
    'hmsclient>=0.1.0',
    'pyhive[hive]>=0.6.0',
]
jdbc = [
    'JPype1==0.7.1',
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
    'thrift_sasl>=0.2.0,<0.4.1;python_version<"3.0"',
    'thrift_sasl>=0.2.0;python_version>="3.0"',
]
kubernetes = [
    'cryptography>=2.0.0',
    'kubernetes>=3.0.0, <12.0.0',
]
ldap = [
    'ldap3>=2.5.1',
]
mongo = [
    'dnspython>=1.13.0,<2.0.0',
    'pymongo>=3.6.0,<3.11.0',
]
mssql = [
    'pymssql~=2.1.1',
]
mysql = [
    'mysqlclient>=1.3.6,<1.4',
]
oracle = [
    'cx_Oracle>=5.1.2, <8.0;python_version<"3.0"',
    'cx_Oracle>=5.1.2;python_version>="3.0"',
]
pagerduty = [
    'pypd>=1.1.0',
]
papermill = [
    'papermill[all]>=1.0.0',
    'nteract-scrapbook[all]>=0.2.1',
    'pyarrow<1.0.0',
    'fsspec<0.8.0;python_version=="3.5"',
    'black==20.8b0;python_version>="3.6"'  # we need to limit black version as we have click < 7

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
    'amqp<5.0.0',
]
redis = [
    'redis~=3.2',
]
salesforce = [
    'simple-salesforce>=0.72,<1.0.0',
]
samba = [
    'pysmbclient>=0.1.3',
]
segment = [
    'analytics-python>=1.2.9',
]
sendgrid = [
    'sendgrid>=5.2.0,<6',
]
sentry = [
    'blinker>=1.1',
    'sentry-sdk>=0.8.0',
]
slack = [
    'slackclient>=1.0.0,<2.0.0',
    'websocket-client<0.55.0'
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
# DEPENDENCIES_EPOCH_NUMBER in the Dockerfile.ci
############################################################################################################
devel = [
    'beautifulsoup4~=4.7.1',
    'click==6.7',
    'contextdecorator;python_version<"3.4"',
    'coverage',
    'docutils>=0.14, <0.16',
    'ecdsa<0.15',  # Required for moto 1.3.14
    'flake8>=3.6.0',
    'flake8-colors',
    'flaky',
    'freezegun',
    'gitpython',
    'idna<2.9',  # Required for moto 1.3.14
    'importlib-metadata~=2.0; python_version<"3.8"',
    'ipdb',
    'jira',
    'mock;python_version<"3.3"',
    'mongomock',
    'moto==1.3.14',  # TODO - fix Datasync issues to get higher version of moto:
                     #        See: https://github.com/apache/airflow/issues/10985
    'packaging',
    'parameterized',
    'paramiko',
    'pipdeptree',
    'pre-commit',
    'pyrsistent<=0.16.0;python_version<"3.0"',
    'pyrsistent;python_version>="3.0"',
    'pysftp',
    'pytest<6.0.0',  # FIXME: pylint complaining for pytest.mark.* on v6.0
    'pytest-cov',
    'pytest-instafail',
    'pytest-timeouts',
    'pywinrm',
    'qds-sdk>=1.9.6',
    'requests_mock',
    'yamllint',
]
############################################################################################################
# IMPORTANT NOTE!!!!!!!!!!!!!!!
# IF you are removing dependencies from the above list, please make sure that you also increase
# DEPENDENCIES_EPOCH_NUMBER in the Dockerfile.ci
############################################################################################################

if PY3:
    devel += ['mypy==0.720']
else:
    devel += ['unittest2']

devel_minreq = aws + cgroups + devel + doc + kubernetes + mysql + password
devel_hadoop = devel_minreq + hdfs + hive + kerberos + presto + webhdfs

devel_azure = azure_blob_storage + azure_container_instances + azure_cosmos + azure_data_lake + azure_secrets + devel_minreq  # noqa
devel_all = (all_dbs + atlas + aws +
             devel_azure +
             celery + cgroups + crypto + datadog + devel + doc + docker +
             elasticsearch + gcp + grpc + hashicorp + jdbc + jenkins + kerberos + kubernetes + ldap +
             oracle + papermill + password +
             rabbitmq + redis + samba + segment + sendgrid + sentry + slack + snowflake + ssh +
             virtualenv + webhdfs + zendesk)

# Snakebite is not Python 3 compatible :'(
if PY3:
    package_to_excludes = ['snakebite>=2.7.8', 'snakebite[kerberos]>=2.7.8']
    if PY38:
        package_to_excludes.extend(['pymssql~=2.1.1'])
    devel_all = [package for package in devel_all if package not in package_to_excludes]
    devel_ci = devel_all
else:
    devel_ci = devel_all + ['unittest2']


#####################################################################################################
# IMPORTANT NOTE!!!!!!!!!!!!!!!
# IF you are removing dependencies from this list, please make sure that you also increase
# DEPENDENCIES_EPOCH_NUMBER in the Dockerfile
#####################################################################################################
EXTRAS_REQUIREMENTS = {
    'all': devel_all,
    'all_dbs': all_dbs,
    'amazon': aws,
    'apache.atlas': atlas,
    "apache.cassandra": cassandra,
    "apache.druid": druid,
    "apache.hdfs": hdfs,
    "apache.hive": hive,
    "apache.pinot": pinot,
    "apache.presto": presto,
    "apache.webhdfs": webhdfs,
    'async': async_packages,
    'atlas': atlas,
    'aws': aws,
    'azure': azure_blob_storage + azure_container_instances + azure_cosmos + azure_data_lake + azure_secrets,
    'azure_blob_storage': azure_blob_storage,
    'azure_container_instances': azure_container_instances,
    'azure_cosmos': azure_cosmos,
    'azure_data_lake': azure_data_lake,
    'azure_secrets': azure_secrets,
    'cassandra': cassandra,
    'celery': celery,
    'cgroups': cgroups,
    'cloudant': cloudant,
    'cncf.kubernetes': kubernetes,
    'crypto': crypto,
    'dask': dask,
    'databricks': databricks,
    'datadog': datadog,
    'devel': devel_minreq,
    'devel_all': devel_all,
    'devel_azure': devel_azure,
    'devel_ci': devel_ci,
    'devel_hadoop': devel_hadoop,
    'doc': doc,
    'docker': docker,
    'druid': druid,
    'elasticsearch': elasticsearch,
    'emr': aws,
    'gcp': gcp,
    'gcp_api': gcp,
    'github_enterprise': flask_oauth,
    'google': gcp,
    'google_auth': flask_oauth,
    'grpc': grpc,
    'hashicorp': hashicorp,
    'hdfs': hdfs,
    'hive': hive,
    'jdbc': jdbc,
    'jira': jira,
    'kerberos': kerberos,
    'kubernetes': kubernetes,
    'ldap': ldap,
    'mongo': mongo,
    'mssql': mssql,
    'microsoft.azure':
    azure_blob_storage + azure_container_instances + azure_cosmos + azure_data_lake + azure_secrets,
    'microsoft.mssql': mssql,
    'microsoft.winrm': winrm,
    'mysql': mysql,
    'oracle': oracle,
    'papermill': papermill,
    'password': password,
    'pinot': pinot,
    'postgres': postgres,
    'presto': presto,
    'qds': qds,
    'rabbitmq': rabbitmq,
    'redis': redis,
    's3': aws,
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
    'virtualenv': virtualenv,
    'webhdfs': webhdfs,
    'winrm': winrm
}

#####################################################################################################
# IMPORTANT NOTE!!!!!!!!!!!!!!!
# IF you are removing dependencies from this list, please make sure that you also increase
# DEPENDENCIES_EPOCH_NUMBER in the Dockerfile.ci
#####################################################################################################
INSTALL_REQUIREMENTS = [
    'alembic>=1.0, <2.0',
    'argcomplete~=1.10',
    'attrs>=20.0, <21.0',
    'cached_property~=1.5',
    # cattrs >= 1.1.0 dropped support for Python 3.6
    'cattrs>=1.0, <1.1.0;python_version<="3.6"',
    'cattrs>=1.0, <2.0;python_version>"3.6"',
    'colorlog==4.0.2',
    'configparser>=3.5.0, <3.6.0',
    'croniter>=0.3.17, <0.4',
    'cryptography>=0.9.3,<3.0; python_version<"3.0"',  # required by snowflake
    'cryptography>=0.9.3;python_version>="3.0"',
    'dill>=0.2.2, <0.4',
    'email-validator',
    'enum34~=1.1.6;python_version<"3.4"',
    'flask>=1.1.0, <2.0',
    'flask-admin==1.5.4',
    'flask-appbuilder>=1.12.2, <2.0.0;python_version<"3.6"',
    'flask-appbuilder~=2.2;python_version>="3.6"',
    'flask-caching>=1.3.3, <1.4.0',
    'flask-login>=0.3, <0.5',
    'flask-swagger>=0.2.13, <0.3',
    'flask-wtf>=0.14.2, <0.15',
    'funcsigs>=1.0.0, <2.0.0',
    'future>=0.16.0, <0.19',
    'graphviz>=0.12',
    'gunicorn>=19.5.0, <21.0',
    'importlib-metadata~=2.0; python_version<"3.8"',
    'importlib_resources~=1.4',
    'iso8601>=0.1.12',
    'jinja2>=2.10.1, <2.12.0',
    'json-merge-patch==0.2',
    'jsonschema~=3.0',
    'lazy_object_proxy<1.5.0',  # Required to keep pip-check happy with astroid
    'markdown>=2.5.2, <3.0',
    'marshmallow-sqlalchemy>=0.16.1, <0.24.0;python_version>="3.6"',
    'marshmallow-sqlalchemy>=0.16.1, <0.19.0;python_version<"3.6"',
    'packaging',
    'pandas>=0.17.1, <2.0',
    'pendulum==1.4.4',
    'pep562~=1.0;python_version<"3.7"',
    'psutil>=4.2.0, <6.0.0',
    'pygments>=2.0.1, <3.0',
    'python-daemon>=2.1.1',
    'python-dateutil>=2.3, <3',
    'python-nvd3~=0.15.0',
    'python-slugify>=3.0.0,<5.0',
    'requests>=2.20.0, <2.23.0;python_version<"3.0"',  # Required to keep snowflake happy
    'requests>=2.20.0, <2.24.0;python_version>="3.0"',  # Required to keep snowflake happy
    'setproctitle>=1.1.8, <2',
    'sqlalchemy~=1.3',
    'sqlalchemy_jsonfield==0.8.0;python_version<"3.5"',
    'sqlalchemy_jsonfield~=0.9;python_version>="3.5"',
    'tabulate>=0.7.5, <0.9',
    'tenacity==4.12.0',
    'thrift>=0.11.0',
    'typing;python_version<"3.5"',
    'typing-extensions>=3.7.4;python_version<"3.8"',
    'tzlocal>=1.4,<2.0.0',
    'unicodecsv>=0.14.1',
    'werkzeug<1.0.0',
    'zope.deprecation>=4.0, <5.0',
]


def get_dependency_name(dep):
    """Get name of a dependency."""
    return dep.replace(">", '=').replace("<", "=").split("=")[0]


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
        packages=find_packages(exclude=['tests*', 'airflow.upgrade*']),
        package_data={
            '': ['airflow/alembic.ini', "airflow/git_version", "*.ipynb",
                 "airflow/providers/cncf/kubernetes/example_dags/*.yaml"],
            'airflow.serialization': ["*.json"],
        },
        include_package_data=True,
        zip_safe=False,
        scripts=['airflow/bin/airflow'],
        install_requires=INSTALL_REQUIREMENTS,
        setup_requires=[
            'bowler',
            'docutils>=0.14, <0.16'
            'gitpython>=2.0.2',
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
            'Programming Language :: Python :: 2.7',
            'Programming Language :: Python :: 3.5',
            'Programming Language :: Python :: 3.6',
            'Programming Language :: Python :: 3.7',
            'Programming Language :: Python :: 3.8',
            'Topic :: System :: Monitoring',
        ],
        author='Apache Software Foundation',
        author_email='dev@airflow.apache.org',
        url='http://airflow.apache.org/',
        download_url=(
            'https://archive.apache.org/dist/airflow/' + version),
        cmdclass={
            'extra_clean': CleanCommand,
            'compile_assets': CompileAssets,
            'list_extras': ListExtras,
        },
        test_suite='setup.airflow_test_suite',
        python_requires='>=2.7,!=3.0.*,!=3.1.*,!=3.2.*,!=3.3.*,!=3.4.*',
        project_urls={
            'Documentation': 'https://airflow.apache.org/docs/',
            'Bug Tracker': 'https://github.com/apache/airflow/issues',
            'Source Code': 'https://github.com/apache/airflow',
        },
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
