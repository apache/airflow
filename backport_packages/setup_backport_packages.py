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
import importlib
import json
import logging
import os
import re
import subprocess
import sys
import tempfile
import textwrap
from datetime import datetime, timedelta
from os import listdir
from os.path import dirname
from shutil import copyfile
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple, Type

from backport_packages.import_all_provider_classes import import_all_provider_classes
from setup import PROVIDERS_REQUIREMENTS
from setuptools import Command, find_packages, setup as setuptools_setup

from tests.deprecated_classes import HOOKS, OPERATORS, SECRETS, SENSORS, TRANSFERS

# Note - we do not test protocols as they are not really part of the official API of
# Apache Airflow

# noinspection DuplicatedCode
logger = logging.getLogger(__name__)  # noqa

PY3 = sys.version_info[0] == 3

MY_DIR_PATH = os.path.dirname(__file__)

SOURCE_DIR_PATH = os.path.abspath(os.path.join(MY_DIR_PATH, os.pardir))
AIRFLOW_PATH = os.path.join(SOURCE_DIR_PATH, "airflow")
PROVIDERS_PATH = os.path.join(AIRFLOW_PATH, "providers")


OPERATORS_PATTERN = r".*Operator$"
SENSORS_PATTERN = r".*Sensor$"
HOOKS_PATTERN = r".*Hook$"
SECRETS_PATTERN = r".*Backend$"
TRANSFERS_PATTERN = r".*To[A-Z0-9].*Operator$"
WRONG_TRANSFERS_PATTERN = r".*Transfer$|.*TransferOperator$"

ALL_PATTERNS = {
    OPERATORS_PATTERN,
    SENSORS_PATTERN,
    HOOKS_PATTERN,
    SECRETS_PATTERN,
    TRANSFERS_PATTERN,
    WRONG_TRANSFERS_PATTERN,
}


def get_source_airflow_folder() -> str:
    """
    Returns source directory for whole airflow (from the main airflow project).

    :return: the folder path
    """
    return os.path.abspath(os.path.join(dirname(__file__), os.pardir))


def get_source_providers_folder() -> str:
    """
    Returns source directory for providers (from the main airflow project).

    :return: the folder path
    """
    return os.path.join(get_source_airflow_folder(), "airflow", "providers")


def get_target_providers_folder() -> str:
    """
    Returns target directory for providers (in the backport_packages folder)

    :return: the folder path
    """
    return os.path.abspath(os.path.join(dirname(__file__), "airflow", "providers"))


def get_target_providers_package_folder(provider_package_id: str) -> str:
    """
    Returns target package folder based on package_id

    :return: the folder path
    """
    return os.path.join(get_target_providers_folder(), *provider_package_id.split("."))


class CleanCommand(Command):
    """
    Command to tidy up the project root.
    Registered as cmd class in setup() so it can be called with ``python setup.py extra_clean``.
    """

    description = "Tidy up the project root"
    user_options: List[str] = []

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

MOVED_OPERATORS_DICT = {value[0]: value[1] for value in OPERATORS}
MOVED_SENSORS_DICT = {value[0]: value[1] for value in SENSORS}
MOVED_HOOKS_DICT = {value[0]: value[1] for value in HOOKS}
MOVED_SECRETS_DICT = {value[0]: value[1] for value in SECRETS}
MOVED_TRANSFERS_DICT = {value[0]: value[1] for value in TRANSFERS}


def get_pip_package_name(provider_package_id: str) -> str:
    """
    Returns PIP package name for the package id.

    :param provider_package_id: id of the package
    :return: the name of pip package
    """
    return "apache-airflow-backport-providers-" + provider_package_id.replace(".", "-")


def get_long_description(provider_package_id: str) -> str:
    """
    Gets long description of the package.

    :param provider_package_id: package id
    :return: content of the description (README file)
    """
    package_folder = get_target_providers_package_folder(provider_package_id)
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
    install_requires.extend(package_dependencies)
    setuptools_setup(
        name=package_name,
        description=f'Back-ported {package_prefix}.* package for Airflow 1.10.*',
        long_description=get_long_description(provider_package_id),
        long_description_content_type='text/markdown',
        license='Apache License 2.0',
        version=get_package_release_version(
            provider_package_id=provider_package_id,
            version_suffix=version_suffix),
        packages=found_packages,
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
    print("Additional commands:")
    print()
    print("  list-providers-packages       - lists all provider packages")
    print("  list-backportable-packages    - lists all packages that are backportable")
    print("  update-package-release-notes [YYYY.MM.DD] [PACKAGES] - updates package release notes")
    print("  --version-suffix <SUFFIX>     - adds version suffix to version of the packages.")
    print()


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


def is_from_the_expected_base_package(the_class: Type, expected_package: str) -> bool:
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


def package_name_matches(the_class: Type, expected_pattern: Optional[str]) -> bool:
    """
    In case expected_pattern is set, it checks if the package name matches the pattern.
    .
    :param the_class: imported class
    :param expected_pattern: the pattern that should match the package
    :return: true if the expected_pattern is None or the pattern matches the package
    """
    return expected_pattern is None or re.match(expected_pattern, the_class.__module__)


def find_all_classes(imported_classes: List[str],
                     base_package: str,
                     ancestor_match: Type,
                     sub_package_pattern_match: str,
                     expected_class_name_pattern: str,
                     unexpected_class_name_patterns: Set[str],
                     exclude_class_type: Type = None,
                     false_positive_class_names: Optional[Set[str]] = None,
                     ) -> Tuple[Set[str], List[Tuple[type, str]]]:
    """
    Returns set of classes containing all subclasses in package specified.

    :param imported_classes: classes imported from providers
    :param base_package: base package name where to start looking for the classes
    :param sub_package_pattern_match: this string is expected to appear in the sub-package name
    :param ancestor_match: type of the object the method looks for
    :param expected_class_name_pattern: regexp of class name pattern to expect
    :param unexpected_class_name_patterns: set of regexp of class name pattern that are not expected
    :param exclude_class_type: exclude class of this type (Sensor are also Operators so
           they should be excluded from the list)
    :param false_positive_class_names: set of class names that are wrongly recognised as badly named
    """
    found_classes: Set[str] = set()
    wrong_classes: List[Tuple[type, str]] = []
    for imported_name in imported_classes:
        module, class_name = imported_name.rsplit(".", maxsplit=1)
        the_class = getattr(importlib.import_module(module), class_name)
        if is_class(the_class=the_class) \
            and not is_example_dag(imported_name=imported_name) \
            and is_from_the_expected_base_package(the_class=the_class, expected_package=base_package) \
            and is_imported_from_same_module(the_class=the_class, imported_name=imported_name) \
            and inherits_from(the_class=the_class, expected_ancestor=ancestor_match) \
            and not inherits_from(the_class=the_class, expected_ancestor=exclude_class_type) \
                and package_name_matches(the_class=the_class, expected_pattern=sub_package_pattern_match):

            if not false_positive_class_names or class_name not in false_positive_class_names:
                if not re.match(expected_class_name_pattern, class_name):
                    wrong_classes.append(
                        (the_class, f"The class name {class_name} is wrong. "
                                    f"It should match {expected_class_name_pattern}"))
                    continue
                if unexpected_class_name_patterns:
                    for unexpected_class_name_pattern in unexpected_class_name_patterns:
                        if re.match(unexpected_class_name_pattern, class_name):
                            wrong_classes.append(
                                (the_class,
                                 f"The class name {class_name} is wrong. "
                                 f"It should not match {unexpected_class_name_pattern}"))
                        continue
            found_classes.add(imported_name)
    return found_classes, wrong_classes


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
    return base_url + os.path.sep.join(class_name.split(".")[:-1]) + ".py"


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


def print_wrong_naming(class_type: str, wrong_classes: List[Tuple[type, str]]):
    """
    Prints wrong classes of a given type if there are any
    :param class_type: type of the class to print
    :param wrong_classes: list of wrong classes
    """
    if wrong_classes:
        print(f"\nThere are wrongly named classes of type {class_type}:\n", file=sys.stderr)
        for class_type, message in wrong_classes:
            print(f"{class_type}: {message}", file=sys.stderr)


def get_package_class_summary(full_package_name: str, imported_classes: List[str]) \
        -> Tuple[Dict[str, Any], int]:
    """
    Gets summary of the package in the form of dictionary containing all types of classes
    :param full_package_name: full package name
    :param imported_classes: classes imported_from providers
    :return: dictionary of objects usable as context for Jinja2 templates - or None if there are some errors
    """
    from airflow.secrets import BaseSecretsBackend
    from airflow.sensors.base_sensor_operator import BaseSensorOperator
    from airflow.hooks.base_hook import BaseHook
    from airflow.models.baseoperator import BaseOperator

    operators, wrong_operators = find_all_classes(
        imported_classes=imported_classes,
        base_package=full_package_name,
        sub_package_pattern_match=r".*\.operators\..*",
        ancestor_match=BaseOperator,
        expected_class_name_pattern=OPERATORS_PATTERN,
        unexpected_class_name_patterns=ALL_PATTERNS - {OPERATORS_PATTERN},
        exclude_class_type=BaseSensorOperator,
        false_positive_class_names={
            'CloudVisionAddProductToProductSetOperator',
            'CloudDataTransferServiceGCSToGCSOperator',
            'CloudDataTransferServiceS3ToGCSOperator',
            'BigQueryCreateDataTransferOperator',
            'CloudTextToSpeechSynthesizeOperator',
            'CloudSpeechToTextRecognizeSpeechOperator',
        }
    )
    sensors, wrong_sensors = find_all_classes(
        imported_classes=imported_classes,
        base_package=full_package_name,
        sub_package_pattern_match=r".*\.sensors\..*",
        ancestor_match=BaseSensorOperator,
        expected_class_name_pattern=SENSORS_PATTERN,
        unexpected_class_name_patterns=ALL_PATTERNS - {OPERATORS_PATTERN, SENSORS_PATTERN}
    )
    hooks, wrong_hooks = find_all_classes(
        imported_classes=imported_classes,
        base_package=full_package_name,
        sub_package_pattern_match=r".*\.hooks\..*",
        ancestor_match=BaseHook,
        expected_class_name_pattern=HOOKS_PATTERN,
        unexpected_class_name_patterns=ALL_PATTERNS - {HOOKS_PATTERN}
    )
    secrets, wrong_secrets = find_all_classes(
        imported_classes=imported_classes,
        sub_package_pattern_match=r".*\.secrets\..*",
        base_package=full_package_name,
        ancestor_match=BaseSecretsBackend,
        expected_class_name_pattern=SECRETS_PATTERN,
        unexpected_class_name_patterns=ALL_PATTERNS - {SECRETS_PATTERN},
    )
    transfers, wrong_transfers = find_all_classes(
        imported_classes=imported_classes,
        base_package=full_package_name,
        sub_package_pattern_match=r".*\.transfers\..*",
        ancestor_match=BaseOperator,
        expected_class_name_pattern=TRANSFERS_PATTERN,
        unexpected_class_name_patterns=ALL_PATTERNS - {OPERATORS_PATTERN, TRANSFERS_PATTERN},
    )
    print_wrong_naming("Operators", wrong_operators)
    print_wrong_naming("Sensors", wrong_sensors)
    print_wrong_naming("Hooks", wrong_hooks)
    print_wrong_naming("Secrets", wrong_secrets)
    print_wrong_naming("Transfers", wrong_transfers)

    num_errors = len(wrong_operators) + len(wrong_sensors) + len(wrong_hooks) + \
        len(wrong_secrets) + len(wrong_transfers)

    new_operators, moved_operators = get_new_and_moved_classes(operators, MOVED_OPERATORS_DICT)
    new_sensors, moved_sensors = get_new_and_moved_classes(sensors, MOVED_SENSORS_DICT)
    new_hooks, moved_hooks = get_new_and_moved_classes(hooks, MOVED_HOOKS_DICT)
    new_secrets, moved_secrets = get_new_and_moved_classes(secrets, MOVED_SECRETS_DICT)
    new_transfers, moved_transfers = get_new_and_moved_classes(transfers, MOVED_TRANSFERS_DICT)
    class_summary = {
        "NEW_OPERATORS": new_operators,
        "MOVED_OPERATORS": moved_operators,
        "NEW_SENSORS": new_sensors,
        "MOVED_SENSORS": moved_sensors,
        "NEW_HOOKS": new_hooks,
        "MOVED_HOOKS": moved_hooks,
        "NEW_SECRETS": new_secrets,
        "MOVED_SECRETS": moved_secrets,
        "NEW_TRANSFERS": new_transfers,
        "MOVED_TRANSFERS": moved_transfers,
        "OPERATORS": operators,
        "HOOKS": hooks,
        "SENSORS": sensors,
        "SECRETS": secrets,
        "TRANSFERS": transfers,
    }
    for from_name, to_name, object_type in [
        ("NEW_OPERATORS", "NEW_OPERATORS_TABLE", "operators"),
        ("NEW_SENSORS", "NEW_SENSORS_TABLE", "sensors"),
        ("NEW_HOOKS", "NEW_HOOKS_TABLE", "hooks"),
        ("NEW_SECRETS", "NEW_SECRETS_TABLE", "secrets"),
        ("NEW_TRANSFERS", "NEW_TRANSFERS_TABLE", "transfers"),
    ]:
        class_summary[to_name] = convert_new_classes_to_table(class_summary[from_name],
                                                              full_package_name,
                                                              object_type)
    for from_name, to_name, object_type in [
        ("MOVED_OPERATORS", "MOVED_OPERATORS_TABLE", "operators"),
        ("MOVED_SENSORS", "MOVED_SENSORS_TABLE", "sensors"),
        ("MOVED_HOOKS", "MOVED_HOOKS_TABLE", "hooks"),
        ("MOVED_SECRETS", "MOVED_SECRETS_TABLE", "protocols"),
        ("MOVED_TRANSFERS", "MOVED_TRANSFERS_TABLE", "transfers"),
    ]:
        class_summary[to_name] = convert_moved_objects_to_table(class_summary[from_name],
                                                                full_package_name,
                                                                object_type)
    return class_summary, num_errors


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
ReleaseInfo = collections.namedtuple(
    "ReleaseInfo",
    "release_version release_version_no_leading_zeros last_commit_hash content file_name")


def strip_leading_zeros(release_version: str) -> str:
    return release_version.replace(".0", ".")


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
            past_releases.append(
                ReleaseInfo(release_version=release_version,
                            release_version_no_leading_zeros=strip_leading_zeros(release_version),
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


def get_previous_release_info(previous_release_version: str,
                              past_releases: List[ReleaseInfo],
                              current_release_version: str) -> Optional[str]:
    """
    Find previous release. In case we are re-running current release we assume that last release was
    the previous one. This is needed so that we can generate list of changes since the previous release.
    :param previous_release_version: known last release version
    :param past_releases: list of past releases
    :param current_release_version: release that we are working on currently
    :return:
    """
    previous_release = None
    if previous_release_version == current_release_version:
        # Re-running for current release - use previous release as base for git log
        if len(past_releases) > 1:
            previous_release = past_releases[1].last_commit_hash
    else:
        previous_release = past_releases[0].last_commit_hash if past_releases else None
    return previous_release


def check_if_release_version_ok(
        past_releases: List[ReleaseInfo],
        current_release_version: str) -> Tuple[str, Optional[str]]:
    """
    Check if the release version passed is not later than the last release version
    :param past_releases: all past releases (if there are any)
    :param current_release_version: release version to check
    :return: Tuple of current/previous_release (previous might be None if there are no releases)
    """
    previous_release_version = past_releases[0].release_version if past_releases else None
    if current_release_version == '':
        if previous_release_version:
            current_release_version = previous_release_version
        else:
            current_release_version = (datetime.today() + timedelta(days=5)).strftime('%Y.%m.%d')
    if previous_release_version and previous_release_version > current_release_version:
        print(f"The release {current_release_version} must be not less than "
              f"{previous_release_version} - last release for the package", file=sys.stderr)
        sys.exit(2)
    return current_release_version, previous_release_version


def get_cross_provider_dependent_packages(provider_package_id: str) -> List[str]:
    """
    Returns cross-provider dependencies for the package.
    :param provider_package_id: package id
    :return: list of cross-provider dependencies
    """
    with open(os.path.join(PROVIDERS_PATH, "dependencies.json"), "rt") as dependencies_file:
        dependent_packages = json.load(dependencies_file).get(provider_package_id) or []
    return dependent_packages


def make_sure_remote_apache_exists_and_fetch():
    """
    Make sure that apache remote exist in git. We need to take a log from the master of apache
    repository - not locally - because when we commit this change and run it, our log will include the
    current commit - which is going to have different commit id once we merge. So it is a bit
    catch-22.

    :return:
    """
    try:
        subprocess.check_call(["git", "remote", "add", "apache", "https://github.com/apache/airflow.git"],
                              stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    except subprocess.CalledProcessError as e:
        if e.returncode == 128:
            print("The remote `apache` already exists. If you have trouble running "
                  "git log delete the remote", file=sys.stderr)
        else:
            raise
    subprocess.check_call(["git", "fetch", "apache"],
                          stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


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


def get_additional_package_info(provider_package_path: str) -> str:
    """
    Returns additional info for the package.

    :param provider_package_path: path for the package
    :return: additional information for the path (empty string if missing)
    """
    additional_info_file_path = os.path.join(provider_package_path, "ADDITIONAL_INFO.md")
    if os.path.isfile(additional_info_file_path):
        with open(additional_info_file_path, "rt") as additional_info_file:
            additional_info = additional_info_file.read()

        additional_info_lines = additional_info.splitlines(keepends=True)
        result = ""
        skip_comment = True
        for line in additional_info_lines:
            if line.startswith(" -->"):
                skip_comment = False
                continue
            if not skip_comment:
                result += line
        return result
    return ""


EXPECTED_SUFFIXES: Dict[str, str] = {
    "OPERATORS": "Operator",
    "HOOKS": "Hook",
    "SENSORS": "Sensor",
    "SECRETS": "Backend",
    "TRANSFERS": "Operator",
}


def is_camel_case_with_acronyms(s: str):
    """
    Checks if the string passed is Camel Case (with capitalised acronyms allowed).
    :param s: string to check
    :return: true if the name looks cool as Class name.
    """
    return s != s.lower() and s != s.upper() and "_" not in s and s[0].upper() == s[0]


def check_if_classes_are_properly_named(class_summary: Dict[str, List[str]]) -> Tuple[int, int]:
    """
    Check if all classes in the dictionary are named properly. It prints names at the output
    and returns the status of class names.

    :param class_summary: dictionary of class names to check, grouped by types.
    :return: Tuple of 2 ints = total number of classes and number of badly named classes
    """
    total_class_number = 0
    badly_named_class_number = 0
    for key, class_suffix in EXPECTED_SUFFIXES.items():
        for class_full_name in class_summary[key]:
            _, class_name = class_full_name.rsplit(".", maxsplit=1)
            error_encountered = False
            if not is_camel_case_with_acronyms(class_name):
                print(f"The class {class_full_name} is wrongly named. The "
                      f"class name should be CamelCaseWithACRONYMS !")
                error_encountered = True
            if not class_name.endswith(class_suffix):
                print(f"The class {class_full_name} is wrongly named. It is one of the {key} so "
                      f"it should end with {class_suffix}")
                error_encountered = True
            total_class_number += 1
            if error_encountered:
                badly_named_class_number += 1
    return total_class_number, badly_named_class_number


def update_release_notes_for_package(provider_package_id: str, current_release_version: str,
                                     imported_classes: List[str]) -> Tuple[int, int]:
    """
    Updates release notes (README.md) for the package. returns Tuple of total number of classes
    and badly named classes.

    :param provider_package_id: id of the package
    :param current_release_version: release version
    :param imported_classes - classes that have been imported from providers

    :return: Tuple of total/bad number of classes
    """
    full_package_name = f"airflow.providers.{provider_package_id}"
    provider_package_path = get_package_path(provider_package_id)
    class_summary, num_errors = get_package_class_summary(full_package_name, imported_classes)
    past_releases = get_all_releases(provider_package_path=provider_package_path)
    current_release_version, previous_release = check_if_release_version_ok(
        past_releases, current_release_version)
    cross_providers_dependencies = \
        get_cross_provider_dependent_packages(provider_package_id=provider_package_id)
    previous_release = get_previous_release_info(previous_release_version=previous_release,
                                                 past_releases=past_releases,
                                                 current_release_version=current_release_version)
    git_cmd = get_git_command(previous_release)
    changes = subprocess.check_output(git_cmd, cwd=provider_package_path, universal_newlines=True)
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
        "RELEASE_NO_LEADING_ZEROS": strip_leading_zeros(current_release_version),
        "CURRENT_CHANGES_TABLE": changes_table,
        "ADDITIONAL_INFO": get_additional_package_info(provider_package_path=provider_package_path),
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
    old_text = ""
    if os.path.isfile(readme_file_path):
        with open(readme_file_path, "rt") as readme_file_read:
            old_text = readme_file_read.read()
    if old_text != readme:
        _, temp_file_path = tempfile.mkstemp(".md")
        try:
            if os.path.isfile(readme_file_path):
                copyfile(readme_file_path, temp_file_path)
            with open(readme_file_path, "wt") as readme_file:
                readme_file.write(readme)
            print()
            print(f"Generated {readme_file_path} file for the {provider_package_id} provider")
            print()
            if old_text != "":
                subprocess.call(["diff", "--color=always", temp_file_path, readme_file_path])
        finally:
            os.remove(temp_file_path)
    total, bad = check_if_classes_are_properly_named(class_summary)
    bad = bad + num_errors
    if bad != 0:
        print()
        print(f"ERROR! There are {bad} errors of {total} classes for {provider_package_id}")
        print()
    return total, bad


def update_release_notes_for_packages(provider_ids: List[str], release_version: str):
    """
    Updates release notes for the list of packages specified.
    :param provider_ids: list of provider ids
    :param release_version: version to release
    :return:
    """
    imported_classes = import_all_provider_classes(
        source_path=SOURCE_DIR_PATH, provider_ids=provider_ids, print_imports=False)
    make_sure_remote_apache_exists_and_fetch()
    if len(provider_ids) == 0:
        provider_ids = get_all_backportable_providers()
    total = 0
    bad = 0
    print()
    print("Generating README files and checking if classes are correctly named.")
    print()
    print("Providers to generate:")
    for provider_id in provider_ids:
        print(provider_id)
    print()
    for package in provider_ids:
        inc_total, inc_bad = update_release_notes_for_package(package, release_version, imported_classes)
        total += inc_total
        bad += inc_bad
    if bad == 0:
        print()
        print(f"All good! All {total} classes are properly named")
        print()
    else:
        print()
        print(f"ERROR! There are in total: {bad} classes badly named out of {total} classes ")
        print()
        exit(1)


def get_all_backportable_providers() -> List[str]:
    """
    Returns all providers that should be taken into account when preparing backports.
    For now remove cncf.kubernetes as it has no chances to work with current core of Airflow 2.0
    And Papermill as it is deeply linked with Lineage in Airflow core and it won't work with lineage
    for Airflow 1.10 anyway.
    :return: list of providers that are considered for backport packages
    """
    # TODO: Maybe we should fix it and release cncf.kubernetes separately
    excluded_providers = ["cncf.kubernetes", "papermill"]
    return [prov for prov in PROVIDERS_REQUIREMENTS.keys() if prov not in excluded_providers]


if __name__ == "__main__":
    LIST_PROVIDERS_PACKAGES = "list-providers-packages"
    LIST_BACKPORTABLE_PACKAGES = "list-backportable-packages"
    UPDATE_PACKAGE_RELEASE_NOTES = "update-package-release-notes"
    suffix = ""

    possible_first_params = get_provider_packages()
    possible_first_params.append(LIST_PROVIDERS_PACKAGES)
    possible_first_params.append(LIST_BACKPORTABLE_PACKAGES)
    possible_first_params.append(UPDATE_PACKAGE_RELEASE_NOTES)
    if len(sys.argv) == 1:
        print("""
ERROR! Missing first param"
""", file=sys.stderr)
        usage()
        exit(1)
    if sys.argv[1] == "--version-suffix":
        if len(sys.argv) < 3:
            print("""
ERROR! --version-suffix needs parameter!
""", file=sys.stderr)
            usage()
            exit(1)
        suffix = sys.argv[2]
        sys.argv = [sys.argv[0]] + sys.argv[3:]
    elif "--help" in sys.argv or "-h" in sys.argv or len(sys.argv) < 2:
        usage()
        exit(0)

    if sys.argv[1] not in possible_first_params:
        print(f"""
ERROR! Wrong first param: {sys.argv[1]}
""", file=sys.stderr)
        usage()
        print()
        exit(1)

    if sys.argv[1] == LIST_PROVIDERS_PACKAGES:
        providers = PROVIDERS_REQUIREMENTS.keys()
        for provider in providers:
            print(provider)
        exit(0)
    elif sys.argv[1] == LIST_BACKPORTABLE_PACKAGES:
        providers = get_all_backportable_providers()
        for provider in providers:
            print(provider)
        exit(0)
    elif sys.argv[1] == UPDATE_PACKAGE_RELEASE_NOTES:
        release_ver = ""
        if len(sys.argv) > 2 and re.match(r'\d{4}\.\d{2}\.\d{2}', sys.argv[2]):
            release_ver = sys.argv[2]
            print()
            print()
            print(f"Preparing release version: {release_ver}")
            package_list = sys.argv[3:]
        else:
            print()
            print()
            print("Updating latest release version.")
            package_list = sys.argv[2:]
        print()
        update_release_notes_for_packages(package_list, release_version=release_ver)
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
