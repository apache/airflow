# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from setuptools import setup, find_packages, Command
from setuptools.command.test import test as TestCommand

import imp
import logging
import os
import sys

logger = logging.getLogger(__name__)

version = imp.load_source(
    'airflow_version', os.path.join('airflow', 'version.py')).version


class Tox(TestCommand):
    user_options = [('tox-args=', None, "Arguments to pass to tox")]
    def initialize_options(self):
        TestCommand.initialize_options(self)
        self.tox_args = ''
    def finalize_options(self):
        TestCommand.finalize_options(self)
        self.test_args = []
        self.test_suite = True
    def run_tests(self):
        #import here, cause outside the eggs aren't loaded
        import tox
        errno = tox.cmdline(args=self.tox_args.split())
        sys.exit(errno)


class CleanCommand(Command):
    """Custom clean command to tidy up the project root."""
    user_options = []
    def initialize_options(self):
        pass
    def finalize_options(self):
        pass
    def run(self):
        os.system('rm -vrf ./build ./dist ./*.pyc ./*.tgz ./*.egg-info')


def git_version(version):
    """
    Return a version to identify the state of the underlying git repo. The version will
    indicate whether the head of the current git-backed working directory is tied to a
    release tag or not : it will indicate the former with a 'release:{version}' prefix
    and the latter with a 'dev0' prefix. Following the prefix will be a sha of the current
    branch head. Finally, a "dirty" suffix is appended to indicate that uncommitted changes
    are present.
    """
    repo = None
    try:
        import git
        repo = git.Repo('.git')
    except ImportError:
        logger.warn('gitpython not found: Cannot compute the git version.')
        return ''
    except Exception as e:
        logger.warn('Git repo not found: Cannot compute the git version.')
        return ''
    if repo:
        sha = repo.head.commit.hexsha
        if repo.is_dirty():
            return '.dev0+{sha}.dirty'.format(sha=sha)
        # commit is clean
        # is it release of `version` ?
        try:
            tag = repo.git.describe(
                match='[0-9]*', exact_match=True,
                tags=True, dirty=True)
            assert tag == version, (tag, version)
            return '.release:{version}+{sha}'.format(version=version,
                                                     sha=sha)
        except git.GitCommandError:
            return '.dev0+{sha}'.format(sha=sha)
    else:
        return 'no_git_version'


def write_version(filename=os.path.join(*['airflow',
                                          'git_version'])):
    text = "{}".format(git_version(version))
    with open(filename, 'w') as a:
        a.write(text)


async = [
    'greenlet==0.4.10',
    'eventlet==0.19.0',
    'gevent==1.1.2',
]
celery = [
    'celery==3.1.23',
    'flower==0.9.1',
]
crypto = ['cryptography==1.5']
doc = [
    'sphinx==1.4.6',
    'sphinx-argparse==0.1.15',
    'sphinx-rtd-theme==0.1.9',
    'Sphinx-PyPI-upload>=0.2.1',
]
docker = ['docker-py==1.10.3']
druid = ['pydruid==0.3.0']
emr = ['boto3==1.4.0']
gcp_api = [
    'httplib2==0.9.2',
    'google-api-python-client==1.5.3',
    'oauth2client==2.0.2',
    'PyOpenSSL==16.1.0',
]
hdfs = ['snakebite==2.11.0']
webhdfs = ['hdfs[dataframe,avro,kerberos]==2.0.11']
hive = [
    'hive-thrift-py==0.0.1',
    'pyhive==0.2.1',
    'impyla==0.13.8',
    'unicodecsv==0.14.1',
]
jdbc = ['jaydebeapi==0.2.0']
mssql = ['pymssql==2.1.3', 'unicodecsv==0.14.1']
mysql = ['mysqlclient==1.3.7']
rabbitmq = ['librabbitmq==1.6.1']
oracle = ['cx_Oracle==5.2.1']
postgres = ['psycopg2==2.6.2']
s3 = [
    'boto==2.42.0',
    'filechunkio==1.8',
]
samba = ['pysmbclient==0.1.3']
slack = ['slackclient==1.0.1']
statsd = ['statsd==3.2.1']
vertica = ['vertica-python==0.6.6']
ldap = ['ldap3==1.4.0']
kerberos = [
    'pykerberos==1.1.13',
    'thrift_sasl==0.2.1',
    'snakebite[kerberos]==2.11.0',
]
password = [
    'bcrypt==3.1.1',
    'flask-bcrypt==0.7.1',
]
github_enterprise = ['Flask-OAuthlib>=0.9.1']
qds = ['qds-sdk>=1.9.0']
cloudant = ['cloudant>=0.5.9,<2.0'] # major update coming soon, clamp to 0.x

all_dbs = postgres + mysql + hive + mssql + hdfs + vertica + cloudant
devel = ['lxml>=3.3.4', 'nose', 'nose-parameterized', 'mock', 'click', 'jira', 'moto', 'freezegun']
devel_minreq = devel + mysql + doc + password + s3
devel_hadoop = devel_minreq + hive + hdfs + webhdfs + kerberos
devel_all = devel + all_dbs + doc + samba + s3 + slack + crypto + oracle + docker


def do_setup():
    write_version()
    setup(
        name='airflow',
        description='Programmatically author, schedule and monitor data pipelines',
        license='Apache License 2.0',
        version=version,
        packages=find_packages(),
        package_data={'': ['airflow/alembic.ini', "airflow/git_version"]},
        include_package_data=True,
        zip_safe=False,
        scripts=['airflow/bin/airflow'],
        setup_requires=[
            'numpy==1.11.0',
        ],
        install_requires=[
            'alembic==0.8.8',
            'croniter==0.3.12',
            'dill==0.2.5',
            'flask-admin==1.4.1',
            'flask-cache==0.13.1',
            'flask-login==0.2.11',
            'flask-wtf==0.12',
            'flask==0.11.1',
            'funcsigs==1.0.2',
            'future==0.15.2',
            'gitpython==2.0.8',
            'gunicorn==19.3.0',  # 19.4.? seemed to have issues
            'jinja2==2.8',
            'lxml==3.6.0',
            'markdown==2.6.6',
            'pandas==0.18.1',
            'psutil==4.3.1',
            'pygments==2.1.3',
            'python-daemon==1.6.1',  # >2.0 < 2.1.1 has installation issues
            'python-dateutil==2.5.3',
            'python-nvd3==0.14.2',
            'requests==2.11.1',
            'setproctitle==1.1.10',
            'sqlalchemy==1.0.15',
            'tabulate==0.7.5',
            'thrift==0.9.3',
            'zope.deprecation==4.1.2',
        ],
        extras_require={
            'all': devel_all,
            'all_dbs': all_dbs,
            'async': async,
            'celery': celery,
            'cloudant': cloudant,
            'crypto': crypto,
            'devel': devel_minreq,
            'devel_hadoop': devel_hadoop,
            'doc': doc,
            'docker': docker,
            'druid': druid,
            'gcp_api': gcp_api,
            'github_enterprise': github_enterprise,
            'hdfs': hdfs,
            'hive': hive,
            'jdbc': jdbc,
            'kerberos': kerberos,
            'ldap': ldap,
            'mssql': mssql,
            'mysql': mysql,
            'oracle': oracle,
            'password': password,
            'postgres': postgres,
            'qds': qds,
            'rabbitmq': rabbitmq,
            's3': s3,
            'emr': emr,
            'samba': samba,
            'slack': slack,
            'statsd': statsd,
            'vertica': vertica,
            'webhdfs': webhdfs,
        },
        classifiers=[
            'Development Status :: 5 - Production/Stable',
            'Environment :: Console',
            'Environment :: Web Environment',
            'Intended Audience :: Developers',
            'Intended Audience :: System Administrators',
            'License :: OSI Approved :: Apache Software License',
            'Programming Language :: Python :: 2.7',
            'Programming Language :: Python :: 3.4',
            'Topic :: System :: Monitoring',
        ],
        author='Maxime Beauchemin',
        author_email='maximebeauchemin@gmail.com',
        url='https://github.com/apache/incubator-airflow',
        download_url=(
            'https://github.com/apache/incubator-airflow/tarball/' + version),
        cmdclass={
            'test': Tox,
            'extra_clean': CleanCommand,
        },
    )


if __name__ == "__main__":
    do_setup()
