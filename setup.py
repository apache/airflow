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

"""Setup for the Airflow library."""

import importlib
import io
import logging
import os
import subprocess
import sys

from setuptools import setup, find_packages, Command
from setuptools.command.test import test as TestCommand

logger = logging.getLogger(__name__)

# Kept manually in sync with airflow.__version__
spec = importlib.util.spec_from_file_location("airflow.version", os.path.join('airflow', 'version.py'))
mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mod)
version = mod.version

PY3 = sys.version_info[0] == 3

# noinspection PyUnboundLocalVariable
try:
    with io.open('README.md', encoding='utf-8') as f:
        long_description = f.read()
except FileNotFoundError:
    long_description = ''


class Tox(TestCommand):
    """
    Command class to run Tox via setup.py.
    Registered as cmdclass in setup() so it can be called with ``python setup.py test``.
    """

    user_options = [('tox-args=', None, "Arguments to pass to tox")]

    def __init__(self, dist, **kw):
        super().__init__(dist, **kw)
        self.test_suite = True
        self.test_args = []
        self.tox_args = ''

    def initialize_options(self):
        TestCommand.initialize_options(self)

    def finalize_options(self):
        TestCommand.finalize_options(self)

    def run_tests(self):
        # import here, cause outside the eggs aren't loaded
        import tox
        errno = tox.cmdline(args=self.tox_args.split())
        sys.exit(errno)


class CleanCommand(Command):
    """
    Command to tidy up the project root.
    Registered as cmdclass in setup() so it can be called with ``python setup.py extra_clean``.
    """

    description = "Tidy up the project root"
    user_options = []

    def initialize_options(self):
        """Set default values for options."""

    def finalize_options(self):
        """Set final values for options."""

    def run(self):
        """Run command to remove temporary files and directories."""
        os.system('rm -vrf ./build ./dist ./*.pyc ./*.tgz ./*.egg-info')


class CompileAssets(Command):
    """
    Compile and build the frontend assets using npm and webpack.
    Registered as cmdclass in setup() so it can be called with ``python setup.py compile_assets``.
    """

    description = "Compile and build the frontend assets"
    user_options = []

    def initialize_options(self):
        """Set default values for options."""

    def finalize_options(self):
        """Set final values for options."""

    def run(self):
        """Run a command to compile and build assets."""
        subprocess.call('./airflow/www/compile_assets.sh')


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
        repo = git.Repo('.git')
    except ImportError:
        logger.warning('gitpython not found: Cannot compute the git version.')
        return ''
    except git.exc.NoSuchPathError:
        logger.warning('.git directory not found: Cannot compute the git version')
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


async_packages = [
    'greenlet>=0.4.9',
    'eventlet>= 0.9.7',
    'gevent>=0.13'
]
atlas = ['atlasclient>=0.1.2']
aws = [
    'boto3>=1.7.0, <1.8.0',
]
azure = [
    'azure-storage>=0.34.0',
    'azure-mgmt-resource>=2.2.0',
    'azure-mgmt-datalake-store>=0.5.0',
    'azure-datalake-store>=0.0.45',
    'azure-cosmos>=3.0.1',
    'azure-mgmt-containerinstance>=1.5.0',
]
cassandra = ['cassandra-driver>=3.13.0']
celery = [
    'celery~=4.3',
    'flower>=0.7.3, <1.0',
    'tornado>=4.2.0, <6.0',  # Dep of flower. Pin to a version that works on Py3.5.2
]
cgroups = [
    'cgroupspy>=0.1.4',
]
cloudant = ['cloudant>=2.0']
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
    'sphinx>=1.2.3',
    'sphinxcontrib-httpdomain>=1.7.0',
]
docker = ['docker~=3.0']
druid = ['pydruid>=0.4.1']
elasticsearch = [
    'elasticsearch>=5.0.0,<6.0.0',
    'elasticsearch-dsl>=5.0.0,<6.0.0'
]
gcp = [
    'google-api-python-client>=1.6.0, <2.0.0dev',
    'google-auth-httplib2>=0.0.1',
    'google-auth>=1.0.0, <2.0.0dev',
    'google-cloud-bigtable==0.33.0',
    'google-cloud-container>=0.1.1',
    'google-cloud-language>=1.1.1',
    'google-cloud-spanner>=1.7.1',
    'google-cloud-storage~=1.16',
    'google-cloud-translate>=1.3.3',
    'google-cloud-videointelligence>=1.7.0',
    'google-cloud-vision>=0.35.2',
    'google-cloud-texttospeech>=0.4.0',
    'google-cloud-speech>=0.36.3',
    'grpcio-gcp>=0.2.2',
    'httplib2~=0.9.2',
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
postgres = ['psycopg2>=2.7.4,<2.8']
qds = ['qds-sdk>=1.10.4']
rabbitmq = ['librabbitmq>=1.6.1']
redis = ['redis~=3.2']
salesforce = ['simple-salesforce>=0.72']
samba = ['pysmbclient>=0.1.3']
segment = ['analytics-python>=1.2.9']
sendgrid = ['sendgrid>=5.2.0,<6']
slack = ['slackclient>=1.0.0,<2.0.0']
mongo = ['pymongo>=3.6.0', 'dnspython>=1.13.0,<2.0.0']
snowflake = ['snowflake-connector-python>=1.5.2',
             'snowflake-sqlalchemy>=1.1.0']
ssh = ['paramiko>=2.1.1', 'pysftp>=0.2.9', 'sshtunnel>=0.1.4,<0.2']
statsd = ['statsd>=3.0.1, <4.0']
vertica = ['vertica-python>=0.5.1']
virtualenv = ['virtualenv']
webhdfs = ['hdfs[dataframe,avro,kerberos]>=2.0.4']
winrm = ['pywinrm==0.2.2']
zendesk = ['zdesk']

all_dbs = postgres + mysql + hive + mssql + hdfs + vertica + cloudant + druid + pinot \
    + cassandra + mongo

devel = [
    'beautifulsoup4~=4.7.1',
    'click==6.7',
    'flake8>=3.6.0',
    'freezegun',
    'jira',
    'mongomock',
    'moto==1.3.5',
    'nose',
    'nose-ignore-docstring==0.2',
    'nose-timer',
    'parameterized',
    'paramiko',
    'pylint~=2.3.1',  # Ensure the same version as in .travis.yml
    'pysftp',
    'pywinrm',
    'qds-sdk>=1.9.6',
    'rednose',
    'requests_mock'
]

if PY3:
    devel += ['mypy']
else:
    devel += ['unittest2']

devel_minreq = devel + kubernetes + mysql + doc + password + cgroups
devel_hadoop = devel_minreq + hive + hdfs + webhdfs + kerberos
devel_all = (sendgrid + devel + all_dbs + doc + samba + slack + crypto + oracle +
             docker + ssh + kubernetes + celery + redis + gcp + grpc +
             datadog + zendesk + jdbc + ldap + kerberos + password + webhdfs + jenkins +
             druid + pinot + segment + snowflake + elasticsearch +
             atlas + azure + aws + salesforce + cgroups + papermill + virtualenv)

# Snakebite & Google Cloud Dataflow are not Python 3 compatible :'(
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
        package_data={'': ['airflow/alembic.ini', "airflow/git_version"]},
        include_package_data=True,
        zip_safe=False,
        scripts=['airflow/bin/airflow'],
        install_requires=[
            'alembic>=1.0, <2.0',
            'cached_property~=1.5',
            'configparser>=3.5.0, <3.6.0',
            'croniter>=0.3.17, <0.4',
            'dill>=0.2.2, <0.3',
            'dumb-init>=1.2.2',
            'flask>=1.0, <2.0',
            'flask-appbuilder>=1.12.5, <2.0.0',
            'flask-caching>=1.3.3, <1.4.0',
            'flask-login>=0.3, <0.5',
            'flask-swagger==0.2.13',
            'flask-wtf>=0.14.2, <0.15',
            'funcsigs==1.0.0',
            'gitpython>=2.0.2',
            'gunicorn>=19.5.0, <20.0',
            'iso8601>=0.1.12',
            'json-merge-patch==0.2',
            'jinja2>=2.10.1, <2.11.0',
            'lazy_object_proxy~=1.3',
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
            'tabulate>=0.7.5, <0.9',
            'tenacity==4.12.0',
            'text-unidecode==1.2',
            'typing;python_version<"3.5"',
            'thrift>=0.9.2',
            'tzlocal>=1.4',
            'unicodecsv>=0.14.1',
            'zope.deprecation>=4.0, <5.0',
        ],
        setup_requires=[
            'docutils>=0.14, <1.0',
        ],
        extras_require={
            'all': devel_all,
            'devel_ci': devel_ci,
            'all_dbs': all_dbs,
            'atlas': atlas,
            'async': async_packages,
            'aws': aws,
            'azure': azure,
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
            'oracle': oracle,
            'papermill': papermill,
            'password': password,
            'pinot': pinot,
            'postgres': postgres,
            'qds': qds,
            'rabbitmq': rabbitmq,
            'redis': redis,
            'salesforce': salesforce,
            'samba': samba,
            'sendgrid': sendgrid,
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
            'test': Tox,
            'extra_clean': CleanCommand,
            'compile_assets': CompileAssets
        },
        python_requires='~=3.5',
    )


if __name__ == "__main__":
    do_setup()
