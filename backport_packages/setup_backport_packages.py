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
import json
import logging
import os
import sys
import textwrap
from importlib import util
from os.path import dirname
from shutil import copyfile, copytree, rmtree
from typing import Dict, List

from setuptools import Command, find_packages, setup as setuptools_setup

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


sys.path.append(os.path.join(dirname(__file__), os.pardir))

import setup  # From AIRFLOW_SOURCES/setup.py # noqa  # isort:skip


PROVIDERS_DEPENDENCIES: Dict[str, List[str]] = {
    "amazon": setup.aws,
    "apache.cassandra": setup.cassandra,
    "apache.druid": setup.druid,
    "apache.hdfs": setup.hdfs,
    "apache.hive": setup.hive,
    "apache.livy": [],
    "apache.pig": [],
    "apache.pinot": setup.pinot,
    "apache.spark": [],
    "apache.sqoop": [],
    "celery": setup.celery,
    "cloudant": setup.cloudant,
    "cncf.kubernetes": setup.kubernetes,
    "databricks": setup.databricks,
    "datadog": setup.datadog,
    "dingding": [],
    "discord": [],
    "docker": setup.docker,
    "email": [],
    "elasticsearch": [],
    "exasol": setup.exasol,
    "facebook": setup.facebook,
    "ftp": [],
    "google": setup.gcp,
    "grpc": setup.grpc,
    "hashicorp": setup.hashicorp,
    "http": [],
    "imap": [],
    "jdbc": setup.jdbc,
    "jenkins": setup.jenkins,
    "jira": setup.jira,
    "microsoft.azure": setup.azure,
    "microsoft.mssql": setup.mssql,
    "microsoft.winrm": setup.winrm,
    "mongo": setup.mongo,
    "mysql": setup.mysql,
    "odbc": setup.odbc,
    "openfaas": [],
    "opsgenie": [],
    "oracle": setup.oracle,
    "pagerduty": setup.pagerduty,
    "papermill": setup.papermill,
    "postgres": setup.postgres,
    "presto": setup.presto,
    "qubole": setup.qds,
    "redis": setup.redis,
    "salesforce": setup.salesforce,
    "samba": setup.samba,
    "segment": setup.segment,
    "sftp": setup.ssh,
    "singularity": setup.singularity,
    "slack": setup.slack,
    "snowflake": setup.snowflake,
    "sqlite": [],
    "ssh": setup.ssh,
    "vertica": setup.vertica,
    "yandex": [],
    "zendesk": setup.zendesk,
}

DEPENDENCIES_JSON_FILE = os.path.join(os.pardir, "airflow", "providers", "dependencies.json")


def change_import_paths_to_deprecated():
    from bowler import LN, TOKEN, Capture, Filename, Query
    from fissix.pytree import Leaf
    from fissix.fixer_util import KeywordArg, Name, Comma

    def remove_tags_modifier(node: LN, capture: Capture, filename: Filename) -> None:
        for node in capture['function_arguments'][0].post_order():
            if isinstance(node, Leaf) and node.value == "tags" and node.type == TOKEN.NAME:
                if node.parent.next_sibling and node.parent.next_sibling.value == ",":
                    node.parent.next_sibling.remove()
                node.parent.remove()

    def pure_airflow_models_filter(node: LN, capture: Capture, filename: Filename) -> bool:
        """Check if select is exactly [airflow, . , models]"""
        return len([ch for ch in node.children[1].leaves()]) == 3

    def remove_super_init_call(node: LN, capture: Capture, filename: Filename) -> None:
        for ch in node.post_order():
            if isinstance(ch, Leaf) and ch.value == "super":
                if any(c.value for c in ch.parent.post_order() if isinstance(c, Leaf)):
                    ch.parent.remove()

    def add_provide_context_to_python_operator(node: LN, capture: Capture, filename: Filename) -> None:
        fn_args = capture['function_arguments'][0]
        fn_args.append_child(Comma())

        provide_context_arg = KeywordArg(Name('provide_context'), Name('True'))
        provide_context_arg.prefix = fn_args.children[0].prefix
        fn_args.append_child(provide_context_arg)

    def remove_class(qry, class_name) -> None:
        def _remover(node: LN, capture: Capture, filename: Filename) -> None:
            if node.type not in (300, 311):  # remove only definition
                node.remove()

        qry.select_class(class_name).modify(_remover)

    changes = [
        ("airflow.operators.bash", "airflow.operators.bash_operator"),
        ("airflow.operators.python", "airflow.operators.python_operator"),
        ("airflow.utils.session", "airflow.utils.db"),
        (
            "airflow.providers.cncf.kubernetes.operators.kubernetes_pod",
            "airflow.contrib.operators.kubernetes_pod_operator"
        ),
    ]

    qry = Query()
    for new, old in changes:
        qry.select_module(new).rename(old)

    # Move and refactor imports for Dataflow
    copyfile(
        os.path.join(dirname(__file__), os.pardir, "airflow", "utils", "python_virtualenv.py"),
        os.path.join(
            dirname(__file__), "airflow", "providers", "google", "cloud", "utils", "python_virtualenv.py"
        )
    )
    (
        qry
        .select_module("airflow.utils.python_virtualenv")
        .rename("airflow.providers.google.cloud.utils.python_virtualenv")
    )
    copyfile(
        os.path.join(dirname(__file__), os.pardir, "airflow", "utils", "process_utils.py"),
        os.path.join(
            dirname(__file__), "airflow", "providers", "google", "cloud", "utils", "process_utils.py"
        )
    )
    (
        qry
        .select_module("airflow.utils.process_utils")
        .rename("airflow.providers.google.cloud.utils.process_utils")
    )

    # Remove tags
    qry.select_method("DAG").is_call().modify(remove_tags_modifier)

    # Fix AWS import in Google Cloud Transfer Service
    (
        qry
        .select_module("airflow.providers.amazon.aws.hooks.base_aws")
        .is_filename(include=r"cloud_storage_transfer_service\.py")
        .rename("airflow.contrib.hooks.aws_hook")
    )

    (
        qry
        .select_class("AwsBaseHook")
        .is_filename(include=r"cloud_storage_transfer_service\.py")
        .filter(lambda n, c, f: n.type == 300)
        .rename("AwsHook")
    )

    # Fix BaseOperatorLinks imports
    files = r"bigquery\.py|mlengine\.py"  # noqa
    qry.select_module("airflow.models").is_filename(include=files).filter(pure_airflow_models_filter).rename(
        "airflow.models.baseoperator")

    # Fix super().__init__() call in hooks
    qry.select_subclass("BaseHook").modify(remove_super_init_call)

    (
        qry.select_function("PythonOperator")
        .is_call()
        .is_filename(include=r"mlengine_operator_utils.py$")
        .modify(add_provide_context_to_python_operator)
    )

    # Remove new class and rename usages of old
    remove_class(qry, "GKEStartPodOperator")
    (
        qry
        .select_class("GKEStartPodOperator")
        .is_filename(include=r"example_kubernetes_engine\.py")
        .rename("GKEPodOperator")
    )

    qry.execute(write=True, silent=False, interactive=False)

    # Add old import to GKE
    gke_path = os.path.join(
        dirname(__file__), "airflow", "providers", "google", "cloud", "operators", "kubernetes_engine.py"
    )
    with open(gke_path, "a") as f:
        f.writelines(["", "from airflow.contrib.operators.gcp_container_operator import GKEPodOperator"])

    gke_path = os.path.join(
        dirname(__file__), "airflow", "providers", "google", "cloud", "operators", "kubernetes_engine.py"
    )


def get_source_providers_folder():
    return os.path.join(dirname(__file__), os.pardir, "airflow", "providers")


def get_providers_folder():
    return os.path.join(dirname(__file__), "airflow", "providers")


def get_providers_package_folder(provider_package: str):
    return os.path.join(get_providers_folder(), *provider_package.split("."))


def get_provider_package_name(provider_package: str):
    return "apache-airflow-providers-" + provider_package.replace(".", "-")


def copy_provider_sources():
    rm_build_dir()
    package_providers_dir = get_providers_folder()
    if os.path.isdir(package_providers_dir):
        rmtree(package_providers_dir)
    copytree(get_source_providers_folder(), get_providers_folder())


def rm_build_dir():
    build_dir = os.path.join(dirname(__file__), "build")
    if os.path.isdir(build_dir):
        rmtree(build_dir)


def copy_and_refactor_sources():
    copy_provider_sources()
    change_import_paths_to_deprecated()


def get_long_description(provider_package: str):
    providers_folder = get_providers_folder()
    package_name = get_provider_package_name(provider_package)
    package_folder = get_providers_package_folder(provider_package)
    with open(os.path.join(providers_folder, "dependencies.json"), "rt") as dependencies_file:
        dependent_packages = json.load(dependencies_file).get(provider_package) or ""
    package_dependencies = ""
    if dependent_packages:
        package_dependencies = "This package has those optional dependencies:\n"
        for dependent_package in dependent_packages:
            package_dependencies += f"  * {get_provider_package_name(dependent_package)}\n"
    package_backport_readme = ""
    backport_readme_file_path = os.path.join(package_folder, "BACKPORT_README.md")
    if os.path.isfile(backport_readme_file_path):
        with open(backport_readme_file_path, "tr") as backport_readme:
            package_backport_readme = backport_readme.read()
    # No jinja here - we do not have any more dependencies in setup.py
    return long_description.replace("{{ PACKAGE_NAME }}", package_name) \
        .replace("{{ PACKAGE_BACKPORT_README }}", package_backport_readme) \
        .replace("{{ PACKAGE_DEPENDENCIES }}", package_dependencies)


def do_setup_package_providers(provider_package: str, deps: List[str], extras: Dict[str, List[str]]):
    setup.write_version()
    provider_package_name = get_provider_package_name(provider_package)
    package_name = f'{provider_package_name}' if provider_package != "providers" \
        else f'apache-airflow-providers'
    package_prefix = f'airflow.providers.{provider_package}' if provider_package != 'providers' \
        else 'airflow.providers'
    found_packages = find_packages()
    found_packages = [package for package in found_packages if package.startswith(package_prefix)]
    setuptools_setup(
        name=package_name,
        description=f'Back-ported ${package_name} package for Airflow 1.10.*',
        long_description=get_long_description(provider_package),
        long_description_content_type='text/markdown',
        license='Apache License 2.0',
        version='2020.04.27.rc1',
        packages=found_packages,
        package_data={
            '': ["airflow/providers/cncf/kubernetes/example_dags/*.yaml"],
        },

        include_package_data=True,
        zip_safe=False,
        install_requires=['apache-airflow~=1.10'] + deps,
        extras_require=extras,
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
        setup_requires=[
            'bowler',
            'docutils',
            'gitpython',
            'setuptools',
            'wheel',
        ],
        python_requires='>=3.6',
    )


def find_package_dependencies(package: str) -> List[str]:
    """Finds dependencies for the packages"""
    if package != 'providers':
        return PROVIDERS_DEPENDENCIES[package]
    else:
        return list(itertools.chain(*PROVIDERS_DEPENDENCIES.values()))


def find_package_extras(package: str) -> Dict[str, List[str]]:
    """Finds extras for the packages"""
    if package == 'providers':
        return {}
    with open(DEPENDENCIES_JSON_FILE, "rt") as dependencies_file:
        cross_provider_dependencies: Dict[str, List[str]] = json.load(dependencies_file)
    extras_dict = {module: [get_provider_package_name(module)]
                   for module in cross_provider_dependencies[package]} \
        if cross_provider_dependencies.get(package) else {}
    return extras_dict


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
        print("ERROR! Missing first param")
        print()
        usage()
    elif sys.argv[1] == "prepare":
        print("Copying sources and doing refactor")
        copy_and_refactor_sources()
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
        do_setup_package_providers(provider_package=provider_package,
                                   deps=dependencies,
                                   extras=find_package_extras(provider_package))
