from setuptools import setup, find_packages

# Kept manually in sync with airflow.__version__
version = '1.0.1'

doc = [
    'sphinx>=1.2.3',
    'sphinx-argparse>=0.1.13',
    'sphinx-rtd-theme>=0.1.6',
    'Sphinx-PyPI-upload>=0.2.1'
]
hive = [
    'hive-thrift-py>=0.0.1',
    'pyhive>=0.1.3',
    'pyhs2>=0.6.0',
]
mysql = ['mysql-python>=1.2.5']
postgres = ['psycopg2>=2.6']
optional = ['librabbitmq>=1.6.1']
samba = ['pysmbclient>=0.1.3']
s3 = ['boto>=2.36.0']

all_dbs = postgres + mysql + hive
devel = all_dbs + doc + samba + s3

setup(
    name='airflow',
    description='Programmatically author, schedule and monitor data pipelines',
    version=version,
    packages=find_packages(),
    include_package_data=True,
    zip_safe=False,
    scripts=['airflow/bin/airflow'],
    install_requires=[
        'celery>=3.1.17',
        'chartkick>=0.4.2',
        'dill>=0.2.2',
        'flask>=0.10.1',
        'flask-admin>=1.0.9',
        'flask-cache>=0.13.1',
        'flask-login>=0.2.11',
        'flower>=0.7.3',
        'jinja2>=2.7.3',
        'markdown>=2.5.2',
        'pandas>=0.15.2',
        'pygments>=2.0.1',
        'python-dateutil>=2.3',
        'requests>=2.5.1',
        'setproctitle>=1.1.8',
        'snakebite>=2.4.13',
        'sqlalchemy>=0.9.8',
        'statsd>=3.0.1',
        'thrift>=0.9.2',
        'tornado>=4.0.2',
    ],
    extras_require={
        'all': devel + optional,
        'all_dbs': all_dbs,
        'doc': doc,
        'devel': devel,
        'hive': hive,
        'mysql': mysql,
        'postgres': postgres,
        's3': s3,
        'samba': samba,
    },
    author='Maxime Beauchemin',
    author_email='maximebeauchemin@gmail.com',
    url='https://github.com/airbnb/airflow',
    download_url=(
        'https://github.com/airbnb/airflow/tarball/' + version),
)
