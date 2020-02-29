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
"""Setup.py for the Backport packages of Airflow project."""

import io
import itertools
import logging
import os
import sys
import textwrap
from importlib import util
from os.path import dirname
from shutil import copytree, rmtree
from typing import List

from setuptools import Command, find_packages, setup as setuptools_setup

sys.path.append(os.path.join(dirname(__file__), os.pardir))


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
        os.chdir(dirname(__file__))
        os.system('rm -vrf ./build ./dist ./*.pyc ./*.tgz ./*.egg-info')


def get_providers_dependencies():
    import setup  # From AIRFLOW_SOURCES/setup.py

    return {
        "amazon": [setup.aws],
        "apache.cassandra": [setup.cassandra],
        "apache.druid": [setup.druid],
        "apache.hdfs": [setup.hdfs],
        "apache.hive": [setup.hive],
        "apache.pig": [],
        "apache.pinot": [setup.pinot],
        "apache.spark": [],
        "apache.sqoop": [],
        "celery": [setup.celery],
        "cloudant": [setup.cloudant],
        "cncf.kubernetes": [setup.kubernetes],
        "databricks": [setup.databricks],
        "datadog": [setup.datadog],
        "dingding": [],
        "discord": [],
        "docker": [setup.docker],
        "email": [],
        "ftp": [],
        "google.cloud": [setup.gcp],
        "google.marketing_platform": [setup.gcp],
        "google.suite": [setup.gcp],
        "grpc": [setup.grpc],
        "http": [],
        "imap": [],
        "jdbc": [setup.jdbc],
        "jenkins": [setup.jenkins],
        "jira": [setup.jira],
        "microsoft.azure": [setup.azure],
        "microsoft.mssql": [setup.mssql],
        "microsoft.winrm": [setup.winrm],
        "mongo": [setup.mongo],
        "mysql": [setup.mysql],
        "odbc": [setup.odbc],
        "openfass": [],
        "opsgenie": [],
        "oracle": [setup.oracle],
        "pagerduty": [setup.pagerduty],
        "papermill": [setup.papermill],
        "postgres": [setup.postgres],
        "presto": [setup.presto],
        "qubole": [setup.qds],
        "redis": [setup.redis],
        "salesforce": [setup.salesforce],
        "samba": [setup.samba],
        "segment": [setup.segment],
        "sftp": [setup.ssh],
        "slack": [setup.slack],
        "snowflake": [setup.snowflake],
        "sqlite": [],
        "ssh": [setup.ssh],
        "vertica": [setup.vertica],
        "zendesk": [setup.zendesk],
    }


PROVIDERS_DEPENDENCIES = get_providers_dependencies()


def copy_provider_sources():
    build_dir = os.path.join(dirname(__file__), "build")
    if os.path.isdir(build_dir):
        rmtree(build_dir)
    package_providers_dir = os.path.join(dirname(__file__), "airflow", "providers")
    if os.path.isdir(package_providers_dir):
        rmtree(package_providers_dir)
    copytree(os.path.join(dirname(__file__), os.pardir, "airflow", "providers"),
             os.path.join(dirname(__file__), "airflow", "providers"))


def do_setup_package_providers(provider_module: str, deps: List[str]):
    """Set up package providers"""
    import setup  # From AIRFLOW_SOURCES/setup.py
    setup.write_version()
    copy_provider_sources()
    provider_package_name = provider_module.replace(".", "_")
    package_name = f'apache-airflow-providers-{provider_package_name}' if provider_module != "providers" \
        else f'apache-airflow-providers'
    package_prefix = f'airflow.providers.{provider_module}' if provider_module != 'providers' \
        else 'airflow.providers'
    found_packages = find_packages()
    found_packages = [package for package in found_packages if package.startswith(package_prefix)]
    setuptools_setup(
        name=package_name,
        description=f'Back-porting ${package_name} package for Airflow 1.10.*',
        long_description=f"""
Back-ported {package_name} to 1.10.* series of Airflow.
""",
        long_description_content_type='text/markdown',
        license='Apache License 2.0',
        version='0.0.1',
        packages=found_packages,
        include_package_data=True,
        zip_safe=False,
        install_requires=['apache-airflow~=1.10'] + deps,
        classifiers=[
            'Development Status :: 5 - Production/Stable',
            'Environment :: Console',
            'Intended Audience :: Developers',
            'Intended Audience :: System Administrators',
            'License :: OSI Approved :: Apache Software License',
            'Programming Language :: Python :: 3.6',
            'Programming Language :: Python :: 3.7',
            'Topic :: System :: Monitoring',
        ],
        python_requires='>=3.6',
    )


def find_package_dependencies(package):
    """Finds dependencies for the packages"""
    if package != 'providers':
        return PROVIDERS_DEPENDENCIES.get(package)
    else:
        return list(itertools.chain(PROVIDERS_DEPENDENCIES.values()))


def get_provider_packages():
    """Returns all packages available in providers"""
    packages = list(PROVIDERS_DEPENDENCIES)
    return ['providers'] + packages


def usage():
    print()
    print("You should provide PACKAGE as first of the setup.py arguments")
    packages = get_provider_packages()
    out = ""
    for package in packages:
        out += f"{package} "
    out_array = textwrap.wrap(out, 80)
    print(f"Available packages: ")
    print()
    for text in out_array:
        print(text)
    print()
    print("You can see all packages configured by specifying list-backport-packages as first argument")


if __name__ == "__main__":
    LIST_BACKPORT_PACKAGES = "list-backport-packages"

    possible_first_params = get_provider_packages()
    possible_first_params.append(LIST_BACKPORT_PACKAGES)
    if len(sys.argv) == 1:
        print()
        print("ERROR! Mising first param")
        print()
        usage()
    elif sys.argv[1] not in possible_first_params:
        print()
        print(f"ERROR! Wrong first param: {sys.argv[1]}")
        print()
        usage()
    elif "--help" in sys.argv or "-h" in sys.argv or \
            len(sys.argv) < 2:
        usage()
    elif len(sys.argv) > 1 and sys.argv[1] == LIST_BACKPORT_PACKAGES:
        for key in PROVIDERS_DEPENDENCIES:
            print(key)
    else:
        provider_package = sys.argv[1]
        if provider_package not in get_provider_packages():
            raise Exception(f"The package {provider_package} is not a backport package. "
                            f"Use one of {get_provider_packages()}")
        del sys.argv[1]
        print(f"Building backport package: {provider_package}")
        dependencies = find_package_dependencies(package=provider_package)
        do_setup_package_providers(provider_module=provider_package, deps=dependencies)
