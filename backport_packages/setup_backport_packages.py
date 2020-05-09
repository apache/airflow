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
import collections
import json
import logging
import os
import pkgutil
import re
import subprocess
import sys
import textwrap
from importlib import util
from inspect import isclass
from os import listdir
from os.path import dirname
from shutil import copyfile, copytree, rmtree
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple, Type

from setup import PROVIDERS_REQUIREMENTS
from setuptools import Command, find_packages, setup as setuptools_setup

from tests.test_core_to_contrib import HOOK, OPERATOR, PROTOCOLS, SECRETS, SENSOR

# noinspection DuplicatedCode
logger = logging.getLogger(__name__)  # noqa

# Kept manually in sync with airflow.__version__
# noinspection PyUnresolvedReferences
spec = util.spec_from_file_location("airflow.version", os.path.join('airflow', 'version.py'))
# noinspection PyUnresolvedReferences
mod = util.module_from_spec(spec)
spec.loader.exec_module(mod)  # type: ignore
version = mod.version  # type: ignore

PY3 = sys.version_info[0] == 3

MY_DIR_PATH = os.path.dirname(__file__)
SOURCE_DIR_PATH = os.path.abspath(os.path.join(MY_DIR_PATH, os.pardir))
AIRFLOW_PATH = os.path.join(SOURCE_DIR_PATH, "airflow")
PROVIDERS_PATH = os.path.join(AIRFLOW_PATH, "providers")


class CleanCommand(Command):
    """
    Command to tidy up the project root.
    Registered as cmd class in setup() so it can be called with ``python setup.py extra_clean``.
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


sys.path.insert(0, SOURCE_DIR_PATH)

import setup  # From AIRFLOW_SOURCES/setup.py # noqa  # isort:skip


DEPENDENCIES_JSON_FILE = os.path.join(PROVIDERS_PATH, "dependencies.json")

MOVED_OPERATORS_DICT = {value[0]: value[1] for value in OPERATOR}
MOVED_SENSORS_DICT = {value[0]: value[1] for value in SENSOR}
MOVED_HOOKS_DICT = {value[0]: value[1] for value in HOOK}
MOVED_PROTOCOLS_DICT = {value[0]: value[1] for value in PROTOCOLS}
MOVED_SECRETS_DICT = {value[0]: value[1] for value in SECRETS}


def refactor_to_airflow_1_10() -> None:
    """
    Refactors the code of providers, so that it works in 1.10.

    """
    from bowler import LN, TOKEN, Capture, Filename, Query
    from fissix.pytree import Leaf
    from fissix.fixer_util import KeywordArg, Name, Comma

    # noinspection PyUnusedLocal
    def remove_tags_modifier(_: LN, capture: Capture, filename: Filename) -> None:
        for node in capture['function_arguments'][0].post_order():
            if isinstance(node, Leaf) and node.value == "tags" and node.type == TOKEN.NAME:
                if node.parent.next_sibling and node.parent.next_sibling.value == ",":
                    node.parent.next_sibling.remove()
                node.parent.remove()

    # noinspection PyUnusedLocal
    def pure_airflow_models_filter(node: LN, capture: Capture, filename: Filename) -> bool:
        """Check if select is exactly [airflow, . , models]"""
        return len([ch for ch in node.children[1].leaves()]) == 3

    # noinspection PyUnusedLocal
    def remove_super_init_call(node: LN, capture: Capture, filename: Filename) -> None:
        for ch in node.post_order():
            if isinstance(ch, Leaf) and ch.value == "super":
                if any(c.value for c in ch.parent.post_order() if isinstance(c, Leaf)):
                    ch.parent.remove()

    # noinspection PyUnusedLocal
    def add_provide_context_to_python_operator(node: LN, capture: Capture, filename: Filename) -> None:
        fn_args = capture['function_arguments'][0]
        fn_args.append_child(Comma())

        provide_context_arg = KeywordArg(Name('provide_context'), Name('True'))
        provide_context_arg.prefix = fn_args.children[0].prefix
        fn_args.append_child(provide_context_arg)

    def remove_class(query, class_name) -> None:
        # noinspection PyUnusedLocal
        def _remover(node: LN, capture: Capture, filename: Filename) -> None:
            if node.type not in (300, 311):  # remove only definition
                node.remove()

        query.select_class(class_name).modify(_remover)

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
        qry.select_module("airflow.utils.python_virtualenv")
           .rename("airflow.providers.google.cloud.utils.python_virtualenv")
    )
    copyfile(
        os.path.join(dirname(__file__), os.pardir, "airflow", "utils", "process_utils.py"),
        os.path.join(
            dirname(__file__), "airflow", "providers", "google", "cloud", "utils", "process_utils.py"
        )
    )
    (
        qry.select_module("airflow.utils.process_utils")
           .rename("airflow.providers.google.cloud.utils.process_utils")
    )

    # Remove tags
    qry.select_method("DAG").is_call().modify(remove_tags_modifier)

    # Fix AWS import in Google Cloud Transfer Service
    (
        qry.select_module("airflow.providers.amazon.aws.hooks.base_aws")
           .is_filename(include=r"cloud_storage_transfer_service\.py")
           .rename("airflow.contrib.hooks.aws_hook")
    )

    (
        qry.select_class("AwsBaseHook")
           .is_filename(include=r"cloud_storage_transfer_service\.py")
           .filter(lambda n, c, f: n.type == 300)  # noqa
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

    (
        qry.select_function("BranchPythonOperator")
           .is_call()
           .is_filename(include=r"example_google_api_to_s3_transfer_advanced.py$")
           .modify(add_provide_context_to_python_operator)
    )

    # Remove new class and rename usages of old
    remove_class(qry, "GKEStartPodOperator")
    (
        qry.select_class("GKEStartPodOperator")
           .is_filename(include=r"example_kubernetes_engine\.py")
           .rename("GKEPodOperator")
    )

    qry.execute(write=True, silent=False, interactive=False)

    # Add old import to GKE
    gke_path = os.path.join(
        dirname(__file__), "airflow", "providers", "google", "cloud", "operators", "kubernetes_engine.py"
    )
    with open(gke_path, "a") as f:  # noqa
        f.writelines(["", "from airflow.contrib.operators.gcp_container_operator import GKEPodOperator"])


def get_source_providers_folder() -> str:
    """
    Returns source directory for providers (from the main airflow project).

    :return: the folder path
    """
    return os.path.join(dirname(__file__), os.pardir, "airflow", "providers")


def get_target_providers_folder() -> str:
    """
    Returns target directory for providers (in the backport_packages folder)

    :return: the folder path
    """
    return os.path.join(dirname(__file__), "airflow", "providers")


def get_providers_package_folder(provider_package_id: str) -> str:
    """
    Returns target package folder based on package_id

    :return: the folder path
    """
    return os.path.join(get_target_providers_folder(), *provider_package_id.split("."))


def get_pip_package_name(provider_package_id: str) -> str:
    """
    Returns PIP package name for the package id.

    :param provider_package_id: id of the package
    :return: the name of pip package
    """
    return "apache-airflow-backport-providers-" + provider_package_id.replace(".", "-")


def is_bigquery_non_dts_module(module_name: str) -> bool:
    """
    Returns true if the module name indicates this is a bigquery module that should be skipped
    for now.
    TODO: this method should be removed as soon as BigQuery rewrite is finished.

    :param module_name: name of the module
    :return: true if module is a bigquery module (but not bigquery_dts)
    """
    return module_name.startswith("bigquery") and "bigquery_dts" not in module_name \
        or "_to_bigquery" in module_name


def copy_provider_sources() -> None:
    """
    Copies provider sources to directory where they will be refactored.
    """
    def ignore_bigquery_files(src: str, names: List[str]) -> List[str]:
        if src.endswith('hooks') or src.endswith('operators') or src.endswith("sensors"):
            ignored_names = [name for name in names
                             if is_bigquery_non_dts_module(module_name=name)]
            return ignored_names
        return []

    rm_build_dir()
    package_providers_dir = get_target_providers_folder()
    if os.path.isdir(package_providers_dir):
        rmtree(package_providers_dir)
    copytree(get_source_providers_folder(), get_target_providers_folder(), ignore=ignore_bigquery_files)


def rm_build_dir() -> None:
    """
    Removes build directory.
    """
    build_dir = os.path.join(dirname(__file__), "build")
    if os.path.isdir(build_dir):
        rmtree(build_dir)


def copy_and_refactor_sources() -> None:
    """
    Copy sources to airflow subdirectory and refactors it,
    """
    copy_provider_sources()
    refactor_to_airflow_1_10()


def get_long_description(provider_package_id: str) -> str:
    """
    Gets long description of the package.

    :param provider_package_id: package id
    :return: content of the description (README file)
    """
    package_folder = get_providers_package_folder(provider_package_id)
    with open(os.path.join(package_folder, "README.md"), encoding='utf-8') as file:
        readme_contents = file.read()
    copying = True
    long_description = ""
    for line in readme_contents.splitlines(keepends=True):
        if line.startswith("**Table of contents**"):
            copying = False
            continue
        if line.startswith("## Backport package"):
            copying = True
        if copying:
            long_description += line
    return long_description


def get_package_release_version(provider_package_id: str, version_suffix: str = "") -> str:
    """
    Returns release version including optional suffix.

    :param provider_package_id: package id
    :param version_suffix: optional suffix (rc1, rc2 etc).
    :return:
    """
    return get_latest_release(
        get_package_path(provider_package_id=provider_package_id)).release_version + version_suffix


def do_setup_package_providers(provider_package_id: str,
                               version_suffix: str,
                               package_dependencies: Iterable[str],
                               extras: Dict[str, List[str]]) -> None:
    """
    The main setup method for package.

    :param provider_package_id: id of the provider package
    :param version_suffix: version suffix to be added to the release version (for example rc1)
    :param package_dependencies: dependencies of the package
    :param extras: extras of the package

    """
    setup.write_version()
    provider_package_name = get_pip_package_name(provider_package_id)
    package_name = f'{provider_package_name}'
    package_prefix = f'airflow.providers.{provider_package_id}'
    found_packages = find_packages()
    found_packages = [package for package in found_packages if package.startswith(package_prefix)]
    install_requires = ['apache-airflow~=1.10']
    install_requires.extend(package_dependencies),
    setuptools_setup(
        name=package_name,
        description=f'Back-ported {package_name} package for Airflow 1.10.*',
        long_description=get_long_description(provider_package_id),
        long_description_content_type='text/markdown',
        license='Apache License 2.0',
        version=get_package_release_version(
            provider_package_id=provider_package_id,
            version_suffix=version_suffix),
        packages=found_packages,
        package_data={
            '': ["airflow/providers/cncf/kubernetes/example_dags/*.yaml"],
        },

        include_package_data=True,
        zip_safe=False,
        install_requires=install_requires,
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


def find_package_extras(package: str) -> Dict[str, List[str]]:
    """
    Finds extras for the package specified.

    """
    if package == 'providers':
        return {}
    with open(DEPENDENCIES_JSON_FILE, "rt") as dependencies_file:
        cross_provider_dependencies: Dict[str, List[str]] = json.load(dependencies_file)
    extras_dict = {module: [get_pip_package_name(module)]
                   for module in cross_provider_dependencies[package]} \
        if cross_provider_dependencies.get(package) else {}
    return extras_dict


def get_provider_packages():
    """
    Returns all provider packages.

    """
    return list(PROVIDERS_REQUIREMENTS)


def usage() -> None:
    """
    Prints usage for the package.

    """
    print()
    print("You should provide PACKAGE as first of the setup.py arguments")
    packages = get_provider_packages()
    out = ""
    for package in packages:
        out += f"{package} "
    out_array = textwrap.wrap(out, 80)
    print("Available packages: ")
    print()
    for text in out_array:
        print(text)
    print()
    print("You can see all packages configured by specifying list-backport-packages as first argument")
    print()
    print("You can generate release notes by specifying: update-package-release-notes YYYY.MM.DD [PACKAGES]")
    print()
    print("You can specify additional suffix when generating packages by using --version-suffix <SUFFIX>\n"
          "before package name when you run sdist or bdist\n")


def is_imported_from_same_module(the_class: str, imported_name: str) -> bool:
    """
    Is the class imported from another module?

    :param the_class: the class object itself
    :param imported_name: name of the imported class
    :return: true if the class was imported from another module
    """
    return ".".join(imported_name.split(".")[:-1]) == the_class.__module__


def is_example_dag(imported_name: str) -> bool:
    """
    Is the class an example_dag class?

    :param imported_name: name where the class is imported from
    :return: true if it is an example_dags class
    """
    return ".example_dags." in imported_name


def is_from_the_expected_package(the_class: Type, expected_package: str) -> bool:
    """
    Returns true if the class is from the package expected.
    :param the_class: the class object
    :param expected_package: package expected for the class
    :return:
    """
    return the_class.__module__.startswith(expected_package)


def inherits_from(the_class: Type, expected_ancestor: Type) -> bool:
    """
    Returns true if the class inherits (directly or indirectly) from the class specified.
    :param the_class: The class to check
    :param expected_ancestor: expected class to inherit from
    :return: true is the class inherits from the class expected
    """
    if expected_ancestor is None:
        return False
    import inspect
    mro = inspect.getmro(the_class)
    return the_class is not expected_ancestor and expected_ancestor in mro


def is_class(the_class: Type) -> bool:
    """
    Returns true if the object passed is a class
    :param the_class: the class to pass
    :return: true if it is a class
    """
    import inspect
    return inspect.isclass(the_class)


def is_bigquery_class(imported_name: str) -> bool:
    """
    Returns true if the object passed is a class
    :param imported_name: name of the class imported
    :return: true if it is a class
    """
    return is_bigquery_non_dts_module(module_name=imported_name.split(".")[-2])


def has_expected_string_in_name(the_class: Type, expected_string: Optional[str]) -> bool:
    """
    In case expected_string is different than None then it checks for presence of the string in the
    imported_name.
    :param the_class: name of the imported object
    :param expected_string: string to expect
    :return: true if the expected_string is None or the expected string is found in the imported name
    """
    return expected_string is None or expected_string in the_class.__module__


def find_all_subclasses(expected_package: str,
                        expected_ancestor: Type,
                        expected_string: Optional[str] = None,
                        exclude_class_type=None) -> Set[str]:
    """
    Returns set of classes containing all subclasses in package specified.

    :param expected_package: full package name where to look for the classes
    :param expected_ancestor: type of the object the method looks for
    :param expected_string: this string is expected to appear in the package name
    :param exclude_class_type: exclude class of this type (Sensor are also Operators so they should be
           excluded from the Operator list)
    """
    subclasses = set()
    for imported_name, the_class in globals().items():
        if is_class(the_class=the_class) \
            and not is_example_dag(imported_name=imported_name) \
            and is_from_the_expected_package(the_class=the_class, expected_package=expected_package) \
            and is_imported_from_same_module(the_class=the_class, imported_name=imported_name) \
            and has_expected_string_in_name(the_class=the_class, expected_string=expected_string) \
            and inherits_from(the_class=the_class, expected_ancestor=expected_ancestor) \
            and not inherits_from(the_class=the_class, expected_ancestor=exclude_class_type) \
                and not is_bigquery_class(imported_name=imported_name):
            subclasses.add(imported_name)
    return subclasses


def get_new_and_moved_classes(classes: Set[str],
                              dict_of_moved_classes: Dict[str, str]) -> Tuple[List[str], Dict[str, str]]:
    """
    Splits the set of classes into new and moved, depending on their presence in the dict of objects
    retrieved from the test_contrib_to_core.

    :param classes: set of classes found
    :param dict_of_moved_classes: dictionary of classes that were moved from contrib to core
    :return:
    """
    new_objects = []
    moved_objects = {}
    for obj in classes:
        if obj in dict_of_moved_classes:
            moved_objects[obj] = dict_of_moved_classes[obj]
            del dict_of_moved_classes[obj]
        else:
            new_objects.append(obj)
    new_objects.sort()
    return new_objects, moved_objects


def strip_package_from_class(base_package: str, class_name: str) -> str:
    """
    Strips base package name from the class (if it starts with the package name).
    """
    if class_name.startswith(base_package):
        return class_name[len(base_package) + 1:]
    else:
        return class_name


def convert_class_name_to_url(base_url: str, class_name) -> str:
    """
    Converts the class name to URL that the class can be reached

    :param base_url: base URL to use
    :param class_name: name of the class
    :return: URL to the class
    """
    return base_url + "/".join(class_name.split(".")[:-1]) + ".py"


def get_class_code_link(base_package: str, class_name: str, git_tag: str) -> str:
    """
    Provides markdown link for the class passed as parameter.

    :param base_package: base package to strip from most names
    :param class_name: name of the class
    :param git_tag: tag to use for the URL link
    :return: URL to the class
    """
    url_prefix = f'https://github.com/apache/airflow/blob/{git_tag}/'
    return f'[{strip_package_from_class(base_package, class_name)}]' \
           f'({convert_class_name_to_url(url_prefix, class_name)})'


def convert_new_classes_to_table(class_list: List[str], full_package_name: str, class_type: str) -> str:
    """
    Converts new classes tp a markdown table.

    :param class_list: list of classes to convert to markup
    :param full_package_name: name of the provider package
    :param class_type: type of classes -> operators, sensors etc.
    :return:
    """
    from tabulate import tabulate
    headers = [f"New Airflow 2.0 {class_type}: `{full_package_name}` package"]
    table = [(get_class_code_link(full_package_name, obj, "master"),) for obj in class_list]
    return tabulate(table, headers=headers, tablefmt="pipe")


def convert_moved_objects_to_table(class_dict: Dict[str, str],
                                   full_package_name: str, class_type: str) -> str:
    """
    Converts moved classes to a markdown table
    :param class_dict: dictionary of classes (to -> from)
    :param full_package_name: name of the provider package
    :param class_type: type of classes -> operators, sensors etc.
    :return:
    """
    from tabulate import tabulate
    headers = [f"Airflow 2.0 {class_type}: `{full_package_name}` package",
               "Airflow 1.10.* previous location (usually `airflow.contrib`)"]
    table = [
        (get_class_code_link(full_package_name, obj, "master"),
         get_class_code_link("airflow", class_dict[obj], "v1-10-stable"))
        for obj in sorted(class_dict.keys())
    ]
    return tabulate(table, headers=headers, tablefmt="pipe")


def get_package_class_summary(full_package_name: str) -> Dict[str, Any]:
    """
    Gets summary of the package in the form of dictionary containing all types of classes
    :param full_package_name: full package name
    :return: dictionary of objects usable as context for Jinja2 templates
    """
    from airflow.secrets import BaseSecretsBackend
    from airflow.sensors.base_sensor_operator import BaseSensorOperator
    from airflow.hooks.base_hook import BaseHook
    from airflow.models.baseoperator import BaseOperator
    from typing_extensions import Protocol
    operators = find_all_subclasses(expected_package=full_package_name,
                                    expected_ancestor=BaseOperator,
                                    expected_string=".operators.",
                                    exclude_class_type=BaseSensorOperator)
    sensors = find_all_subclasses(expected_package=full_package_name,
                                  expected_ancestor=BaseSensorOperator,
                                  expected_string='.sensors.')
    hooks = find_all_subclasses(expected_package=full_package_name,
                                expected_ancestor=BaseHook,
                                expected_string='.hooks.')
    protocols = find_all_subclasses(expected_package=full_package_name,
                                    expected_ancestor=Protocol)
    secrets = find_all_subclasses(expected_package=full_package_name,
                                  expected_ancestor=BaseSecretsBackend)
    new_operators, moved_operators = get_new_and_moved_classes(operators, MOVED_OPERATORS_DICT)
    new_sensors, moved_sensors = get_new_and_moved_classes(sensors, MOVED_SENSORS_DICT)
    new_hooks, moved_hooks = get_new_and_moved_classes(hooks, MOVED_HOOKS_DICT)
    new_protocols, moved_protocols = get_new_and_moved_classes(protocols, MOVED_PROTOCOLS_DICT)
    new_secrets, moved_secrets = get_new_and_moved_classes(secrets, MOVED_SECRETS_DICT)
    class_summary = {
        "NEW_OPERATORS": new_operators,
        "MOVED_OPERATORS": moved_operators,
        "NEW_SENSORS": new_sensors,
        "MOVED_SENSORS": moved_sensors,
        "NEW_HOOKS": new_hooks,
        "MOVED_HOOKS": moved_hooks,
        "NEW_PROTOCOLS": new_protocols,
        "MOVED_PROTOCOLS": moved_protocols,
        "NEW_SECRETS": new_secrets,
        "MOVED_SECRETS": moved_secrets,
    }
    for from_name, to_name, object_type in [
        ("NEW_OPERATORS", "NEW_OPERATORS_TABLE", "operators"),
        ("NEW_SENSORS", "NEW_SENSORS_TABLE", "sensors"),
        ("NEW_HOOKS", "NEW_HOOKS_TABLE", "hooks"),
        ("NEW_PROTOCOLS", "NEW_PROTOCOLS_TABLE", "protocols"),
        ("NEW_SECRETS", "NEW_SECRETS_TABLE", "secrets"),
    ]:
        class_summary[to_name] = convert_new_classes_to_table(class_summary[from_name],
                                                              full_package_name,
                                                              object_type)
    for from_name, to_name, object_type in [
        ("MOVED_OPERATORS", "MOVED_OPERATORS_TABLE", "operators"),
        ("MOVED_SENSORS", "MOVED_SENSORS_TABLE", "sensors"),
        ("MOVED_HOOKS", "MOVED_HOOKS_TABLE", "hooks"),
        ("MOVED_PROTOCOLS", "MOVED_PROTOCOLS_TABLE", "protocols"),
        ("MOVED_SECRETS", "MOVED_SECRETS_TABLE", "protocols"),
    ]:
        class_summary[to_name] = convert_moved_objects_to_table(class_summary[from_name],
                                                                full_package_name,
                                                                object_type)
    return class_summary


def render_template(template_name: str, context: Dict[str, Any]) -> str:
    """
    Renders template based on it's name. Reads the template from <name>_TEMPLATE.md.jinja2 in current dir.
    :param template_name: name of the template to use
    :param context: Jinja2 context
    :return: rendered template
    """
    import jinja2
    template_loader = jinja2.FileSystemLoader(searchpath=MY_DIR_PATH)
    template_env = jinja2.Environment(
        loader=template_loader,
        undefined=jinja2.StrictUndefined,
        autoescape=True
    )
    template = template_env.get_template(f"{template_name}_TEMPLATE.md.jinja2")
    content: str = template.render(context)
    return content


def convert_git_changes_to_table(changes: str, base_url: str) -> str:
    """
    Converts list of changes from it's string form to markdown table.

    The changes are in the form of multiple lines where each line consists of:
    FULL_COMMIT_HASH SHORT_COMMIT_HASH COMMIT_DATE COMMIT_SUBJECT

    The subject can contain spaces but one of the preceding values can, so we can make split
    3 times on spaces to break it up.
    :param changes: list of changes in a form of multiple-line string
    :param base_url: base url for the commit URL
    :return: markdown-formatted table
    """
    from tabulate import tabulate
    lines = changes.split("\n")
    headers = ["Commit", "Committed", "Subject"]
    table_data = []
    for line in lines:
        full_hash, short_hash, date, message = line.split(" ", maxsplit=3)
        table_data.append((f"[{short_hash}]({base_url}{full_hash})", date, message))
    return tabulate(table_data, headers=headers, tablefmt="pipe")


def convert_pip_requirements_to_table(requirements: Iterable[str]) -> str:
    """
    Converts PIP requirement list to a markdown table.
    :param requirements: requirements list
    :return: markdown-formatted table
    """
    from tabulate import tabulate
    headers = ["PIP package", "Version required"]
    table_data = []
    for dependency in requirements:
        found = re.match(r"(^[^<=>~]*)([^<=>~]?.*)$", dependency)
        if found:
            package = found.group(1)
            version_required = found.group(2)
            table_data.append((package, version_required))
        else:
            table_data.append((dependency, ""))
    return tabulate(table_data, headers=headers, tablefmt="pipe")


def convert_cross_package_dependencies_to_table(
        cross_package_dependencies: List[str], base_url: str) -> str:
    """
    Converts cross-package dependencies to a markdown table
    :param cross_package_dependencies: list of cross-package dependencies
    :param base_url: base url to use for links
    :return: markdown-formatted table
    """
    from tabulate import tabulate
    headers = ["Dependent package", "Extra"]
    table_data = []
    for dependency in cross_package_dependencies:
        pip_package_name = f"apache-airflow-backport-providers-{dependency.replace('.','-')}"
        url_suffix = f"{dependency.replace('.','/')}"
        table_data.append((f"[{pip_package_name}]({base_url}{url_suffix})", dependency))
    return tabulate(table_data, headers=headers, tablefmt="pipe")


LICENCE = """<!--
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.
 -->
"""

PROVIDERS_CHANGES_PREFIX = "PROVIDERS_CHANGES_"

"""
Keeps information about historical releases.
"""
ReleaseInfo = collections.namedtuple("ReleaseInfo", "release_version last_commit_hash content file_name")


def get_all_releases(provider_package_path: str) -> List[ReleaseInfo]:
    """
    Returns information about past releases (retrieved from PROVIDERS_CHANGES_ files stored in the
    package folder.
    :param provider_package_path: path of the package
    :return: list of releases made so far.
    """
    past_releases: List[ReleaseInfo] = []
    changes_file_names = listdir(provider_package_path)
    for file_name in sorted(changes_file_names, reverse=True):
        if file_name.startswith(PROVIDERS_CHANGES_PREFIX) and file_name.endswith(".md"):
            changes_file_path = os.path.join(provider_package_path, file_name)
            with open(changes_file_path, "rt") as changes_file:
                content = changes_file.read()
            found = re.search(r'/([a-z0-9]*)\)', content, flags=re.MULTILINE)
            if not found:
                raise Exception(f"Commit not found in {changes_file_path}. Something is wrong there.")
            last_commit_hash = found.group(1)
            release_version = file_name[len(PROVIDERS_CHANGES_PREFIX):][:-3]
            past_releases.append(ReleaseInfo(release_version=release_version,
                                             last_commit_hash=last_commit_hash,
                                             content=content,
                                             file_name=file_name))
    return past_releases


def get_latest_release(provider_package_path: str) -> ReleaseInfo:
    """
    Gets information about the latest release.

    :param provider_package_path: path of package
    :return: latest release information
    """
    return get_all_releases(provider_package_path=provider_package_path)[0]


def get_previous_release_info(last_release_version: str,
                              past_releases: List[ReleaseInfo],
                              current_release_version: str) -> Optional[str]:
    """
    Find previous release. In case we are re-running current release we assume that last release was
    the previous one. This is needed so that we can generate list of changes since the previous release.
    :param last_release_version: known last release version
    :param past_releases: list of past releases
    :param current_release_version: release that we are working on currently
    :return:
    """
    previous_release = None
    if last_release_version == current_release_version:
        # Re-running for current release - use previous release as base for git log
        if len(past_releases) > 1:
            previous_release = past_releases[1].last_commit_hash
    else:
        previous_release = past_releases[0].last_commit_hash if past_releases else None
    return previous_release


def check_if_release_version_ok(
        past_releases: List[ReleaseInfo], current_release_version: str) -> Optional[str]:
    """
    Check if the release version passed is not later than the last release version
    :param past_releases: all past releases (if there are any)
    :param current_release_version: release version to check
    :return: last_release (might be None if there are no past releases)
    """
    last_release = past_releases[0].release_version if past_releases else None
    if last_release and last_release > current_release_version:
        print(f"The release {current_release_version} must be not less than "
              f"{last_release} - last release for the package")
        sys.exit(2)
    return last_release


def get_cross_provider_dependent_packages(provider_package_id: str) -> List[str]:
    """
    Returns cross-provider dependencies for the package.
    :param provider_package_id: package id
    :return: list of cross-provider dependencies
    """
    with open(os.path.join(PROVIDERS_PATH, "dependencies.json"), "rt") as dependencies_file:
        dependent_packages = json.load(dependencies_file).get(provider_package_id) or []
    return dependent_packages


def make_sure_remote_apache_exists():
    """
    Make sure that apache remote exist in git. We need to take a log from the master of apache
    repository - not locally - because when we commit this change and run it, our log will include the
    current commit - which is going to have different commit id once we merge. So it is a bit
    catch-22.

    :return:
    """
    try:
        subprocess.check_call(["git", "remote", "add", "apache", "https://github.com/apache/airflow.git"])
    except subprocess.CalledProcessError as e:
        if e.returncode == 128:
            print("The remote `apache` already exists. If you have trouble running git log delete the remote")
        else:
            raise
    subprocess.check_call(["git", "fetch", "apache"])


def get_git_command(base_commit: Optional[str]) -> List[str]:
    """
    Get git command to run for the current repo from the current folder (which is the package folder).
    :param base_commit: if present - base commit from which to start the log from
    :return: git command to run
    """
    git_cmd = ["git", "log", "apache/master", "--pretty=format:%H %h %cd %s", "--date=short"]
    if base_commit:
        git_cmd.append(f"{base_commit}...HEAD")
    git_cmd.extend(['--', '.'])
    return git_cmd


def store_current_changes(provider_package_path: str,
                          current_release_version: str, current_changes: str) -> None:
    """
    Stores current changes in the PROVIDERS_CHANGES_YYYY.MM.DD.md file.

    :param provider_package_path: path for the package
    :param current_release_version: release version to build
    :param current_changes: list of changes formatted in markdown format
    """
    current_changes_file_path = os.path.join(provider_package_path,
                                             PROVIDERS_CHANGES_PREFIX + current_release_version + ".md")
    with open(current_changes_file_path, "wt") as current_changes_file:
        current_changes_file.write(current_changes)
        current_changes_file.write("\n")


def get_package_path(provider_package_id: str) -> str:
    """
    Retrieves package path from package id.
    :param provider_package_id: id of the package
    :return: path of the providers folder
    """
    provider_package_path = os.path.join(PROVIDERS_PATH, *provider_package_id.split("."))
    return provider_package_path


def update_release_notes_for_package(provider_package_id: str, current_release_version: str) -> None:
    """
    Updates release notes (README.md) for the package.

    :param provider_package_id: id of the package
    :param current_release_version: release version
    """
    full_package_name = f"airflow.providers.{provider_package_id}"
    provider_package_path = get_package_path(provider_package_id)
    class_summary = get_package_class_summary(full_package_name)
    past_releases = get_all_releases(provider_package_path=provider_package_path)
    last_release = check_if_release_version_ok(past_releases, current_release_version)
    cross_providers_dependencies = \
        get_cross_provider_dependent_packages(provider_package_id=provider_package_id)
    previous_release = get_previous_release_info(last_release_version=last_release,
                                                 past_releases=past_releases,
                                                 current_release_version=current_release_version)
    git_cmd = get_git_command(previous_release)
    changes = subprocess.check_output(git_cmd, cwd=provider_package_path, universal_newlines=True)
    if changes == "":
        print(f"The code has not changed since last release {last_release}. Skipping generating README.")
        return
    changes_table = convert_git_changes_to_table(
        changes,
        base_url="https://github.com/apache/airflow/commit/")
    pip_requirements_table = convert_pip_requirements_to_table(PROVIDERS_REQUIREMENTS[provider_package_id])
    cross_providers_dependencies_table = \
        convert_cross_package_dependencies_to_table(
            cross_providers_dependencies,
            base_url="https://github.com/apache/airflow/tree/master/airflow/providers/")
    context: Dict[str, Any] = {
        "PROVIDER_PACKAGE_ID": provider_package_id,
        "PACKAGE_PIP_NAME": f"apache-airflow-backport-providers-{provider_package_id.replace('.', '-')}",
        "FULL_PACKAGE_NAME": full_package_name,
        "RELEASE": current_release_version,
        "CURRENT_CHANGES_TABLE": changes_table,
        "CROSS_PROVIDERS_DEPENDENCIES": cross_providers_dependencies,
        "CROSS_PROVIDERS_DEPENDENCIES_TABLE": cross_providers_dependencies_table,
        "PIP_REQUIREMENTS": PROVIDERS_REQUIREMENTS[provider_package_id],
        "PIP_REQUIREMENTS_TABLE": pip_requirements_table
    }
    current_changes = render_template(template_name="PROVIDERS_CHANGES", context=context)
    store_current_changes(provider_package_path=provider_package_path,
                          current_release_version=current_release_version,
                          current_changes=current_changes)
    context.update(class_summary)
    all_releases = get_all_releases(provider_package_path)
    context["RELEASES"] = all_releases
    readme = LICENCE
    readme += render_template(template_name="PROVIDERS_README", context=context)
    readme += render_template(template_name="PROVIDERS_CLASSES", context=context)
    for a_release in all_releases:
        readme += a_release.content
    readme_file_path = os.path.join(provider_package_path, "README.md")
    with open(readme_file_path, "wt") as readme_file:
        readme_file.write(readme)
    print()
    print(f"Generated {readme_file_path} file for the {provider_package_id} provider")
    print()


def import_all_classes(provider_ids: List[str]) -> None:
    """
    Imports all classes in providers packages. This method loads and imports
    all teh classes found in providers, so that we can find all the subclasses
    of operators/sensors etc.

    :param provider_ids - paths of providers that should be loaded. If empty - all providers
                          are loaded
    """
    prefixed_provider_ids = ["airflow.providers." + provider_id for provider_id in provider_ids]

    for loader, module_name, is_pkg in pkgutil.walk_packages([SOURCE_DIR_PATH]):
        if module_name.startswith("airflow.providers"):
            if prefixed_provider_ids and all([not module_name.startswith(prefix_provider_id)
                                              for prefix_provider_id in prefixed_provider_ids]):
                # Skip loading module if it is not in the list of providers that we are running the
                # backport readme package preparation
                continue
            _module = loader.find_module(module_name).load_module(module_name)
            globals()[module_name] = _module
            for attribute_name in dir(_module):
                attribute = getattr(_module, attribute_name)
                if isclass(attribute):
                    globals()[module_name + "." + attribute_name] = attribute


def update_release_notes_for_packages(package_ids: List[str], release_version: str):
    """
    Updates release notes for the list of packages specified.
    :param package_ids: list of packages
    :param release_version: version to release
    :return:
    """
    import_all_classes(package_ids)
    make_sure_remote_apache_exists()
    if len(package_ids) == 0:
        package_ids = list(PROVIDERS_REQUIREMENTS.keys())
    for package in package_ids:
        update_release_notes_for_package(package, release_version)


if __name__ == "__main__":
    PREPARE = "prepare"
    LIST_BACKPORT_PACKAGES = "list-backport-packages"
    UPDATE_PACKAGE_RELEASE_NOTES = "update-package-release-notes"
    suffix = ""

    possible_first_params = get_provider_packages()
    possible_first_params.append(LIST_BACKPORT_PACKAGES)
    possible_first_params.append(UPDATE_PACKAGE_RELEASE_NOTES)
    possible_first_params.append(PREPARE)
    if len(sys.argv) == 1:
        print()
        print("ERROR! Missing first param")
        print()
        usage()
        exit(1)
    if sys.argv[1] == "--version-suffix":
        if len(sys.argv) < 3:
            print()
            print("ERROR! --version-suffix needs parameter!")
            print()
            usage()
        suffix = sys.argv[2]
        sys.argv = [sys.argv[0]] + sys.argv[3:]
    elif "--help" in sys.argv or "-h" in sys.argv or len(sys.argv) < 2:
        usage()
        exit(0)

    if sys.argv[1] not in possible_first_params:
        print()
        print(f"ERROR! Wrong first param: {sys.argv[1]}")
        print()
        usage()
        print()
        exit(1)

    if sys.argv[1] == PREPARE:
        print("Copying sources and doing refactor")
        copy_and_refactor_sources()
        exit(0)
    elif sys.argv[1] == LIST_BACKPORT_PACKAGES:
        for key in PROVIDERS_REQUIREMENTS:
            print(key)
        exit(0)
    elif sys.argv[1] == UPDATE_PACKAGE_RELEASE_NOTES:
        if len(sys.argv) == 2 or not re.match(r'\d{4}\.\d{2}\.\d{2}', sys.argv[2]):
            print("Please provide release tag as parameter in the form of YYYY.MM.DD")
            sys.exit(1)
        release = sys.argv[2]
        update_release_notes_for_packages(sys.argv[3:], release_version=release)
        exit(0)

    provider_package = sys.argv[1]
    if provider_package not in get_provider_packages():
        raise Exception(f"The package {provider_package} is not a backport package. "
                        f"Use one of {get_provider_packages()}")
    del sys.argv[1]
    print(f"Building backport package: {provider_package}")
    dependencies = PROVIDERS_REQUIREMENTS[provider_package]
    do_setup_package_providers(provider_package_id=provider_package,
                               package_dependencies=dependencies,
                               extras=find_package_extras(provider_package),
                               version_suffix=suffix)
