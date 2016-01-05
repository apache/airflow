from setuptools import setup, find_packages, Command
from setuptools.command.test import test as TestCommand

import os
import sys

# Kept manually in sync with airflow.__version__
version = '1.6.1'


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


async = [
    'greenlet>=0.4.9',
    'eventlet>= 0.9.7',
    'gevent>=0.13'
]
celery = [
    'celery>=3.1.17',
    'flower>=0.7.3'
]
crypto = ['cryptography>=0.9.3']
doc = [
    'sphinx>=1.2.3',
    'sphinx-argparse>=0.1.13',
    'sphinx-rtd-theme>=0.1.6',
    'Sphinx-PyPI-upload>=0.2.1'
]
druid = ['pydruid>=0.2.1']
hdfs = ['snakebite>=2.4.13']
webhdfs = ['hdfs[dataframe,avro,kerberos]>=2.0.4']
hive = [
    'hive-thrift-py>=0.0.1',
    'pyhive>=0.1.3',
    'pyhs2>=0.6.0',
]
jdbc = ['jaydebeapi>=0.2.0']
mssql = ['pymssql>=2.1.1', 'unicodecsv>=0.13.0']
mysql = ['mysqlclient>=1.3.6']
optional = ['librabbitmq>=1.6.1']
oracle = ['cx_Oracle>=5.1.2']
postgres = ['psycopg2>=2.6']
s3 = [
    'boto>=2.36.0',
    'filechunkio>=1.6',
]
samba = ['pysmbclient>=0.1.3']
slack = ['slackclient>=0.15']
statsd = ['statsd>=3.0.1, <4.0']
vertica = ['vertica-python>=0.5.1']
ldap = ['ldap3>=0.9.9.1']
devel = ['lxml>=3.3.4']
kerberos = ['pykerberos>=1.1.8']
password = [
    'bcrypt>=2.0.0',
    'flask-bcrypt>=0.7.1',
]

all_dbs = postgres + mysql + hive + mssql + hdfs + vertica
devel = all_dbs + doc + samba + s3 + ['nose'] + slack + crypto + oracle

setup(
    name='airflow',
    description='Programmatically author, schedule and monitor data pipelines',
    version=version,
    packages=find_packages(),
    package_data={'': ['airflow/alembic.ini']},
    include_package_data=True,
    zip_safe=False,
    scripts=['airflow/bin/airflow'],
    install_requires=[
        'alembic>=0.8.3, <0.9',
        'babel>=1.3, <2.0',
        'chartkick>=0.4.2, < 0.5',
        'croniter>=0.3.8, <0.4',
        'dill>=0.2.2, <0.3',
        'flask>=0.10.1, <0.11',
        'flask-admin==1.2.0',
        'flask-cache>=0.13.1, <0.14',
        'flask-login==0.2.11',
        'future>=0.15.0, <0.16',
        'gunicorn>=19.3.0, <20.0',
        'jinja2>=2.7.3, <3.0',
        'markdown>=2.5.2, <3.0',
        'pandas>=0.15.2, <1.0.0',
        'pygments>=2.0.1, <3.0',
        'python-dateutil>=2.3, <3',
        'requests>=2.5.1, <3',
        'setproctitle>=1.1.8, <2',
        'sqlalchemy>=0.9.8',
        'thrift>=0.9.2, <0.10',
        'Flask-WTF==0.12'
    ],
    extras_require={
        'all': devel + optional,
        'all_dbs': all_dbs,
        'async': async,
        'celery': celery,
        'crypto': crypto,
        'devel': devel,
        'doc': doc,
        'druid': druid,
        'hdfs': hdfs,
        'hive': hive,
        'jdbc': jdbc,
        'mssql': mssql,
        'mysql': mysql,
        'oracle': oracle,
        'postgres': postgres,
        's3': s3,
        'samba': samba,
        'slack': slack,
        'statsd': statsd,
        'vertica': vertica,
        'ldap': ldap,
        'webhdfs': webhdfs,
        'kerberos': kerberos,
        'password': password,
    },
    author='Maxime Beauchemin',
    author_email='maximebeauchemin@gmail.com',
    url='https://github.com/airbnb/airflow',
    download_url=(
        'https://github.com/airbnb/airflow/tarball/' + version),
    cmdclass={'test': Tox,
              'extra_clean': CleanCommand,
              },
)
