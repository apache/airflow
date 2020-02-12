# This file is Not licensed to ASF
# SKIP LICENSE INSERTION

from collections import defaultdict
from os.path import join
from packaging.version import Version
import re
import sys
from setuptools import setup
from wheel.wheelfile import WheelFile
from email.parser import BytesParser


def get_info_from_airflow_wheel(filepath):
    with WheelFile(filepath, 'r') as whl:
        # Find the wheel METADATA file
        with whl.open(join(whl.dist_info_path + '/METADATA')) as fh:
            metadata = BytesParser().parse(fh, headersonly=True)

    airflow_version = metadata.get('Version')

    # If AC is already installed, and someone tries to install a "new" extra
    # package via `pip install astronomer-certified[foo]`, pip won't "follow"
    # extras, even ,if we made `foo` depend on `apache-airflow[foo]`. i.e. pip
    # sees apache-airflow of the right version is already installed, and won't
    # look at the extra of apache-airflow
    #
    # So we need to include the _full_ extras including all deps here

    # Look at the "Requires-Dist" lines, which have the form of
    #    "hdfs[avro,dataframe,kerberos] (>=2.0.4) ; extra == 'webhdfs'"
    # looking for the `extra` suffixes:

    extras = defaultdict(list)
    extra_dep_re = re.compile(r'''
        ^
        (?P<specifier> .*? )
        \s*
        ;
        \s*
        extra \s* == \s* '(?P<extra> .*? )'
        \s*
        $
    ''', re.VERBOSE)

    for requires in metadata.get_all('Requires-Dist'):
        match = extra_dep_re.match(requires)
        if not match:
            continue

        extras[match['extra']].append(match['specifier'])

    return extras, airflow_version


def transform_airflow_version(airflow_ver):

    parsed = Version(airflow_ver)

    ver = '.'.join(str(x) for x in parsed.release)

    if parsed.local:
        local = parsed._version.local[1]
        ver += '-' + str(local)

    if parsed.is_devrelease:
        ver += '.dev' + str(parsed.dev)

    return ver


if not sys.argv[-1].endswith('.whl'):
    exit('Must pass path to apache-airflow .whl file as last argument')

extras, airflow_version = get_info_from_airflow_wheel(sys.argv[-1])
sys.argv.pop()

version = transform_airflow_version(airflow_version)

setup(
    name='astronomer-certified',
    description='Programmatically author, schedule and monitor data pipelines',
    license='Apache License 2.0',
    version=version,
    url='https://www.astronomer.io/docs/ac-local/',
    install_requires=[
        'apache-airflow==' + airflow_version,
        'astronomer-airflow-version-check~=1.0',
    ],
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Environment :: Console',
        'Environment :: Web Environment',
        'Intended Audience :: Developers',
        'Intended Audience :: System Administrators',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Topic :: System :: Monitoring',
    ],
    extras_require=extras,
    author='Astronomer Inc',
    author_email='humans@astronomer.io',
    python_requires='>=3.6',
)
