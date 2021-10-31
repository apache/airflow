#!/usr/bin/env python3

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
"""Setup.py for the Provider packages of Airflow project."""
import collections
import difflib
import glob
import importlib
import json
import logging
import os
import re
import shutil
import subprocess
import sys
import tempfile
import textwrap
import warnings
from contextlib import contextmanager
from copy import deepcopy
from datetime import datetime, timedelta
from enum import Enum
from functools import lru_cache
from os.path import dirname, relpath
from pathlib import Path
from shutil import copyfile
from typing import Any, Dict, Iterable, List, NamedTuple, Optional, Set, Tuple, Type

import click
import jsonschema
from github import Github, PullRequest, UnknownObjectException
from packaging.version import Version
from rich.console import Console
from rich.progress import Progress
from rich.syntax import Syntax

from airflow.utils.yaml import safe_load

ALL_PYTHON_VERSIONS = ["3.6", "3.7", "3.8", "3.9"]

INITIAL_CHANGELOG_CONTENT = """


 .. Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

 ..   http://www.apache.org/licenses/LICENSE-2.0

 .. Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.


Changelog
---------

1.0.0
.....

Initial version of the provider.
"""

HTTPS_REMOTE = "apache-https-for-providers"
HEAD_OF_HTTPS_REMOTE = f"{HTTPS_REMOTE}/main"

MY_DIR_PATH = os.path.dirname(__file__)
SOURCE_DIR_PATH = os.path.abspath(os.path.join(MY_DIR_PATH, os.pardir, os.pardir))
AIRFLOW_PATH = os.path.join(SOURCE_DIR_PATH, "airflow")
PROVIDERS_PATH = os.path.join(AIRFLOW_PATH, "providers")
DOCUMENTATION_PATH = os.path.join(SOURCE_DIR_PATH, "docs")
TARGET_PROVIDER_PACKAGES_PATH = os.path.join(SOURCE_DIR_PATH, "provider_packages")
GENERATED_AIRFLOW_PATH = os.path.join(TARGET_PROVIDER_PACKAGES_PATH, "airflow")
GENERATED_PROVIDERS_PATH = os.path.join(GENERATED_AIRFLOW_PATH, "providers")

PROVIDER_RUNTIME_DATA_SCHEMA_PATH = os.path.join(SOURCE_DIR_PATH, "airflow", "provider_info.schema.json")

sys.path.insert(0, SOURCE_DIR_PATH)

# those imports need to come after the above sys.path.insert to make sure that Airflow
# sources are importable without having to add the airflow sources to the PYTHONPATH before
# running the script
import tests.deprecated_classes  # noqa # isort:skip
from dev.import_all_classes import import_all_classes  # noqa # isort:skip
from setup import PROVIDERS_REQUIREMENTS, PREINSTALLED_PROVIDERS  # noqa # isort:skip

# Note - we do not test protocols as they are not really part of the official API of
# Apache Airflow

logger = logging.getLogger(__name__)

PY3 = sys.version_info[0] == 3

console = Console(width=400, color_system="standard")


@click.group(context_settings={'help_option_names': ['-h', '--help'], 'max_content_width': 500})
def cli():
    ...


option_git_update = click.option(
    '--git-update/--no-git-update',
    default=True,
    is_flag=True,
    help=f"If the git remote {HTTPS_REMOTE} already exists, don't try to update it",
)

option_interactive = click.option(
    '--interactive/--non-interactive',
    default=True,
    is_flag=True,
    help="If the script should interactively ask the user what to do in case of doubt",
)

option_version_suffix = click.option(
    "--version-suffix",
    metavar="suffix",
    help=textwrap.dedent(
        """
        adds version suffix to version of the packages.
        only useful when generating rc candidates for pypi."""
    ),
)
option_verbose = click.option(
    "--verbose",
    is_flag=True,
    help="Print verbose information about performed steps",
)
option_force = click.option(
    "--force",
    is_flag=True,
    help="Forces regeneration of already generated documentation",
)
argument_package_id = click.argument('package_id')
argument_changelog_files = click.argument('changelog_files', nargs=-1)
argument_package_ids = click.argument('package_ids', nargs=-1)


@contextmanager
def with_group(title):
    """
    If used in GitHub Action, creates an expandable group in the GitHub Action log.
    Otherwise, display simple text groups.

    For more information, see:
    https://docs.github.com/en/free-pro-team@latest/actions/reference/workflow-commands-for-github-actions#grouping-log-lines
    """
    if os.environ.get('GITHUB_ACTIONS', 'false') != "true":
        console.print("[blue]" + "#" * 10 + ' ' + title + ' ' + "#" * 10 + "[/]")
        yield
        return
    console.print(f"::group::{title}")
    yield
    console.print("::endgroup::")


class EntityType(Enum):
    Operators = "Operators"
    Transfers = "Transfers"
    Sensors = "Sensors"
    Hooks = "Hooks"
    Secrets = "Secrets"


class EntityTypeSummary(NamedTuple):
    entities: List[str]
    new_entities_table: str
    wrong_entities: List[Tuple[type, str]]


class VerifiedEntities(NamedTuple):
    all_entities: Set[str]
    wrong_entities: List[Tuple[type, str]]


class ProviderPackageDetails(NamedTuple):
    provider_package_id: str
    full_package_name: str
    pypi_package_name: str
    source_provider_package_path: str
    documentation_provider_package_path: str
    provider_description: str
    versions: List[str]
    excluded_python_versions: List[str]


ENTITY_NAMES = {
    EntityType.Operators: "Operators",
    EntityType.Transfers: "Transfer Operators",
    EntityType.Sensors: "Sensors",
    EntityType.Hooks: "Hooks",
    EntityType.Secrets: "Secrets",
}

TOTALS: Dict[EntityType, int] = {
    EntityType.Operators: 0,
    EntityType.Hooks: 0,
    EntityType.Sensors: 0,
    EntityType.Transfers: 0,
    EntityType.Secrets: 0,
}

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

EXPECTED_SUFFIXES: Dict[EntityType, str] = {
    EntityType.Operators: "Operator",
    EntityType.Hooks: "Hook",
    EntityType.Sensors: "Sensor",
    EntityType.Secrets: "Backend",
    EntityType.Transfers: "Operator",
}


def get_source_airflow_folder() -> str:
    """
    Returns source directory for whole airflow (from the main airflow project).

    :return: the folder path
    """
    return os.path.abspath(SOURCE_DIR_PATH)


def get_source_providers_folder() -> str:
    """
    Returns source directory for providers (from the main airflow project).

    :return: the folder path
    """
    return os.path.join(get_source_airflow_folder(), "airflow", "providers")


def get_target_folder() -> str:
    """
    Returns target directory for providers (in the provider_packages folder)

    :return: the folder path
    """
    return os.path.abspath(os.path.join(dirname(__file__), os.pardir, os.pardir, "provider_packages"))


def get_target_providers_folder() -> str:
    """
    Returns target directory for providers (in the provider_packages folder)

    :return: the folder path
    """
    return os.path.abspath(os.path.join(get_target_folder(), "airflow", "providers"))


def get_target_providers_package_folder(provider_package_id: str) -> str:
    """
    Returns target package folder based on package_id

    :return: the folder path
    """
    return os.path.join(get_target_providers_folder(), *provider_package_id.split("."))


DEPENDENCIES_JSON_FILE = os.path.join(PROVIDERS_PATH, "dependencies.json")


def get_pip_package_name(provider_package_id: str) -> str:
    """
    Returns PIP package name for the package id.

    :param provider_package_id: id of the package
    :return: the name of pip package
    """
    return "apache-airflow-providers-" + provider_package_id.replace(".", "-")


def get_wheel_package_name(provider_package_id: str) -> str:
    """
    Returns PIP package name for the package id.

    :param provider_package_id: id of the package
    :return: the name of pip package
    """
    return "apache_airflow_providers_" + provider_package_id.replace(".", "_")


def get_long_description(provider_package_id: str) -> str:
    """
    Gets long description of the package.

    :param provider_package_id: package id
    :return: content of the description: README file
    """
    package_folder = get_target_providers_package_folder(provider_package_id)
    readme_file = os.path.join(package_folder, "README.md")
    if not os.path.exists(readme_file):
        return ""
    with open(readme_file, encoding='utf-8') as file:
        readme_contents = file.read()
    copying = True
    long_description = ""
    for line in readme_contents.splitlines(keepends=True):
        if line.startswith("**Table of contents**"):
            copying = False
            continue
        header_line = "## Provider package"
        if line.startswith(header_line):
            copying = True
        if copying:
            long_description += line
    return long_description


def get_install_requirements(provider_package_id: str, version_suffix: str) -> List[str]:
    """
    Returns install requirements for the package.

    :param provider_package_id: id of the provider package
    :param version_suffix: optional version suffix for packages

    :return: install requirements of the package
    """
    dependencies = PROVIDERS_REQUIREMENTS[provider_package_id]
    provider_yaml = get_provider_yaml(provider_package_id)
    install_requires = []
    if "additional-dependencies" in provider_yaml:
        additional_dependencies = provider_yaml['additional-dependencies']
        if version_suffix:
            # In case we are preparing "rc" or dev0 packages, we should also
            # make sure that cross-dependency with Airflow or Airflow Providers will
            # contain the version suffix, otherwise we will have conflicting dependencies.
            # For example if (in sftp) we have ssh>=2.0.1 and release ssh==2.0.1
            # we want to turn this into ssh>=2.0.1.dev0 if we build dev0 version of the packages
            # or >=2.0.1rc1 if we build rc1 version of the packages.
            for dependency in additional_dependencies:
                if dependency.startswith("apache-airflow") and ">=" in dependency:
                    dependency = dependency + version_suffix
                install_requires.append(dependency)
        else:
            install_requires.extend(additional_dependencies)

    install_requires.extend(dependencies)
    return install_requires


def get_setup_requirements() -> List[str]:
    """
    Returns setup requirements (common for all package for now).
    :return: setup requirements
    """
    return ['setuptools', 'wheel']


def get_package_extras(provider_package_id: str) -> Dict[str, List[str]]:
    """
    Finds extras for the package specified.

    :param provider_package_id: id of the package
    """
    if provider_package_id == 'providers':
        return {}
    with open(DEPENDENCIES_JSON_FILE) as dependencies_file:
        cross_provider_dependencies: Dict[str, List[str]] = json.load(dependencies_file)
    extras_dict = (
        {
            module: [get_pip_package_name(module)]
            for module in cross_provider_dependencies[provider_package_id]
        }
        if cross_provider_dependencies.get(provider_package_id)
        else {}
    )
    provider_yaml_dict = get_provider_yaml(provider_package_id)
    additional_extras = provider_yaml_dict.get('additional-extras')
    if additional_extras:
        for key in additional_extras:
            if key in extras_dict:
                extras_dict[key].append(additional_extras[key])
            else:
                extras_dict[key] = additional_extras[key]
    return extras_dict


def get_provider_packages() -> List[str]:
    """
    Returns all provider packages.

    """
    return list(PROVIDERS_REQUIREMENTS.keys())


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


def inherits_from(the_class: Type, expected_ancestor: Optional[Type] = None) -> bool:
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


def package_name_matches(the_class: Type, expected_pattern: Optional[str] = None) -> bool:
    """
    In case expected_pattern is set, it checks if the package name matches the pattern.
    .
    :param the_class: imported class
    :param expected_pattern: the pattern that should match the package
    :return: true if the expected_pattern is None or the pattern matches the package
    """
    return expected_pattern is None or re.match(expected_pattern, the_class.__module__) is not None


def find_all_entities(
    imported_classes: List[str],
    base_package: str,
    ancestor_match: Type,
    sub_package_pattern_match: str,
    expected_class_name_pattern: str,
    unexpected_class_name_patterns: Set[str],
    exclude_class_type: Optional[Type] = None,
    false_positive_class_names: Optional[Set[str]] = None,
) -> VerifiedEntities:
    """
    Returns set of entities containing all subclasses in package specified.

    :param imported_classes: entities imported from providers
    :param base_package: base package name where to start looking for the entities
    :param sub_package_pattern_match: this string is expected to appear in the sub-package name
    :param ancestor_match: type of the object the method looks for
    :param expected_class_name_pattern: regexp of class name pattern to expect
    :param unexpected_class_name_patterns: set of regexp of class name pattern that are not expected
    :param exclude_class_type: exclude class of this type (Sensor are also Operators so
           they should be excluded from the list)
    :param false_positive_class_names: set of class names that are wrongly recognised as badly named
    """
    found_entities: Set[str] = set()
    wrong_entities: List[Tuple[type, str]] = []
    for imported_name in imported_classes:
        module, class_name = imported_name.rsplit(".", maxsplit=1)
        the_class = getattr(importlib.import_module(module), class_name)
        if (
            is_class(the_class=the_class)
            and not is_example_dag(imported_name=imported_name)
            and is_from_the_expected_base_package(the_class=the_class, expected_package=base_package)
            and is_imported_from_same_module(the_class=the_class, imported_name=imported_name)
            and inherits_from(the_class=the_class, expected_ancestor=ancestor_match)
            and not inherits_from(the_class=the_class, expected_ancestor=exclude_class_type)
            and package_name_matches(the_class=the_class, expected_pattern=sub_package_pattern_match)
        ):

            if not false_positive_class_names or class_name not in false_positive_class_names:
                if not re.match(expected_class_name_pattern, class_name):
                    wrong_entities.append(
                        (
                            the_class,
                            f"The class name {class_name} is wrong. "
                            f"It should match {expected_class_name_pattern}",
                        )
                    )
                    continue
                if unexpected_class_name_patterns:
                    for unexpected_class_name_pattern in unexpected_class_name_patterns:
                        if re.match(unexpected_class_name_pattern, class_name):
                            wrong_entities.append(
                                (
                                    the_class,
                                    f"The class name {class_name} is wrong. "
                                    f"It should not match {unexpected_class_name_pattern}",
                                )
                            )
                        continue
            found_entities.add(imported_name)
    return VerifiedEntities(all_entities=found_entities, wrong_entities=wrong_entities)


def convert_classes_to_table(entity_type: EntityType, entities: List[str], full_package_name: str) -> str:
    """
    Converts new entities tp a markdown table.

    :param entity_type: entity type to convert to markup
    :param entities: list of  entities
    :param full_package_name: name of the provider package
    :return: table of new classes
    """
    from tabulate import tabulate

    headers = [f"New Airflow 2.0 {entity_type.value.lower()}: `{full_package_name}` package"]
    table = [(get_class_code_link(full_package_name, class_name, "main"),) for class_name in entities]
    return tabulate(table, headers=headers, tablefmt="pipe")


def get_details_about_classes(
    entity_type: EntityType,
    entities: Set[str],
    wrong_entities: List[Tuple[type, str]],
    full_package_name: str,
) -> EntityTypeSummary:
    """
    Get details about entities..

    :param entity_type: type of entity (Operators, Hooks etc.)
    :param entities: set of entities found
    :param wrong_entities: wrong entities found for that type
    :param full_package_name: full package name
    :return:
    """
    all_entities = list(entities)
    all_entities.sort()
    TOTALS[entity_type] += len(all_entities)
    return EntityTypeSummary(
        entities=all_entities,
        new_entities_table=convert_classes_to_table(
            entity_type=entity_type,
            entities=all_entities,
            full_package_name=full_package_name,
        ),
        wrong_entities=wrong_entities,
    )


def strip_package_from_class(base_package: str, class_name: str) -> str:
    """
    Strips base package name from the class (if it starts with the package name).
    """
    if class_name.startswith(base_package):
        return class_name[len(base_package) + 1 :]
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
    return (
        f'[{strip_package_from_class(base_package, class_name)}]'
        f'({convert_class_name_to_url(url_prefix, class_name)})'
    )


def print_wrong_naming(entity_type: EntityType, wrong_classes: List[Tuple[type, str]]):
    """
    Prints wrong entities of a given entity type if there are any
    :param entity_type: type of the class to print
    :param wrong_classes: list of wrong entities
    """
    if wrong_classes:
        console.print(f"\n[red]There are wrongly named entities of type {entity_type}:[/]\n")
        for wrong_entity_type, message in wrong_classes:
            console.print(f"{wrong_entity_type}: {message}")


def get_package_class_summary(
    full_package_name: str, imported_classes: List[str]
) -> Dict[EntityType, EntityTypeSummary]:
    """
    Gets summary of the package in the form of dictionary containing all types of entities
    :param full_package_name: full package name
    :param imported_classes: entities imported_from providers
    :return: dictionary of objects usable as context for JINJA2 templates - or None if there are some errors
    """
    from airflow.hooks.base import BaseHook
    from airflow.models.baseoperator import BaseOperator
    from airflow.secrets import BaseSecretsBackend
    from airflow.sensors.base import BaseSensorOperator

    all_verified_entities: Dict[EntityType, VerifiedEntities] = {
        EntityType.Operators: find_all_entities(
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
            },
        ),
        EntityType.Sensors: find_all_entities(
            imported_classes=imported_classes,
            base_package=full_package_name,
            sub_package_pattern_match=r".*\.sensors\..*",
            ancestor_match=BaseSensorOperator,
            expected_class_name_pattern=SENSORS_PATTERN,
            unexpected_class_name_patterns=ALL_PATTERNS - {OPERATORS_PATTERN, SENSORS_PATTERN},
        ),
        EntityType.Hooks: find_all_entities(
            imported_classes=imported_classes,
            base_package=full_package_name,
            sub_package_pattern_match=r".*\.hooks\..*",
            ancestor_match=BaseHook,
            expected_class_name_pattern=HOOKS_PATTERN,
            unexpected_class_name_patterns=ALL_PATTERNS - {HOOKS_PATTERN},
        ),
        EntityType.Secrets: find_all_entities(
            imported_classes=imported_classes,
            sub_package_pattern_match=r".*\.secrets\..*",
            base_package=full_package_name,
            ancestor_match=BaseSecretsBackend,
            expected_class_name_pattern=SECRETS_PATTERN,
            unexpected_class_name_patterns=ALL_PATTERNS - {SECRETS_PATTERN},
        ),
        EntityType.Transfers: find_all_entities(
            imported_classes=imported_classes,
            base_package=full_package_name,
            sub_package_pattern_match=r".*\.transfers\..*",
            ancestor_match=BaseOperator,
            expected_class_name_pattern=TRANSFERS_PATTERN,
            unexpected_class_name_patterns=ALL_PATTERNS - {OPERATORS_PATTERN, TRANSFERS_PATTERN},
        ),
    }
    for entity in EntityType:
        print_wrong_naming(entity, all_verified_entities[entity].wrong_entities)

    entities_summary: Dict[EntityType, EntityTypeSummary] = {}

    for entity_type in EntityType:
        entities_summary[entity_type] = get_details_about_classes(
            entity_type,
            all_verified_entities[entity_type].all_entities,
            all_verified_entities[entity_type].wrong_entities,
            full_package_name,
        )

    return entities_summary


def render_template(
    template_name: str,
    context: Dict[str, Any],
    extension: str,
    autoescape: bool = True,
    keep_trailing_newline: bool = False,
) -> str:
    """
    Renders template based on it's name. Reads the template from <name>_TEMPLATE.md.jinja2 in current dir.
    :param template_name: name of the template to use
    :param context: Jinja2 context
    :param extension: Target file extension
    :param autoescape: Whether to autoescape HTML
    :param keep_trailing_newline: Whether to keep the newline in rendered output
    :return: rendered template
    """
    import jinja2

    template_loader = jinja2.FileSystemLoader(searchpath=MY_DIR_PATH)
    template_env = jinja2.Environment(
        loader=template_loader,
        undefined=jinja2.StrictUndefined,
        autoescape=autoescape,
        keep_trailing_newline=keep_trailing_newline,
    )
    template = template_env.get_template(f"{template_name}_TEMPLATE{extension}.jinja2")
    content: str = template.render(context)
    return content


PR_PATTERN = re.compile(r".*\(#([0-9]+)\)")


class Change(NamedTuple):
    """Stores details about commits"""

    full_hash: str
    short_hash: str
    date: str
    version: str
    message: str
    message_without_backticks: str
    pr: Optional[str]


def get_change_from_line(line: str, version: str):
    split_line = line.split(" ", maxsplit=3)
    message = split_line[3]
    pr = None
    pr_match = PR_PATTERN.match(message)
    if pr_match:
        pr = pr_match.group(1)
    return Change(
        full_hash=split_line[0],
        short_hash=split_line[1],
        date=split_line[2],
        version=version,
        message=message,
        message_without_backticks=message.replace("`", "'").replace("&39;", "'"),
        pr=pr,
    )


def convert_git_changes_to_table(
    version: str, changes: str, base_url: str, markdown: bool = True
) -> Tuple[str, List[Change]]:
    """
    Converts list of changes from it's string form to markdown/RST table and array of change information

    The changes are in the form of multiple lines where each line consists of:
    FULL_COMMIT_HASH SHORT_COMMIT_HASH COMMIT_DATE COMMIT_SUBJECT

    The subject can contain spaces but one of the preceding values can, so we can make split
    3 times on spaces to break it up.
    :param version: Version from which the changes are
    :param changes: list of changes in a form of multiple-line string
    :param base_url: base url for the commit URL
    :param markdown: if True, markdown format is used else rst
    :return: formatted table + list of changes (starting from the latest)
    """
    from tabulate import tabulate

    lines = changes.split("\n")
    headers = ["Commit", "Committed", "Subject"]
    table_data = []
    changes_list: List[Change] = []
    for line in lines:
        if line == "":
            continue
        change = get_change_from_line(line, version)
        table_data.append(
            (
                f"[{change.short_hash}]({base_url}{change.full_hash})"
                if markdown
                else f"`{change.short_hash} <{base_url}{change.full_hash}>`_",
                change.date,
                f"`{change.message_without_backticks}`"
                if markdown
                else f"``{change.message_without_backticks}``",
            )
        )
        changes_list.append(change)
    header = ""
    if not table_data:
        return header, []
    table = tabulate(table_data, headers=headers, tablefmt="pipe" if markdown else "rst")
    if not markdown:
        header += f"\n\n{version}\n" + "." * len(version) + "\n\n"
        release_date = table_data[0][1]
        header += f"Latest change: {release_date}\n\n"
    return header + table, changes_list


def convert_pip_requirements_to_table(requirements: Iterable[str], markdown: bool = True) -> str:
    """
    Converts PIP requirement list to a markdown table.
    :param requirements: requirements list
    :param markdown: if True, markdown format is used else rst
    :return: formatted table
    """
    from tabulate import tabulate

    headers = ["PIP package", "Version required"]
    table_data = []
    for dependency in requirements:
        found = re.match(r"(^[^<=>~]*)([^<=>~]?.*)$", dependency)
        if found:
            package = found.group(1)
            version_required = found.group(2)
            if version_required != "":
                version_required = f"`{version_required}`" if markdown else f'``{version_required}``'
            table_data.append((f"`{package}`" if markdown else f"``{package}``", version_required))
        else:
            table_data.append((dependency, ""))
    return tabulate(table_data, headers=headers, tablefmt="pipe" if markdown else "rst")


def convert_cross_package_dependencies_to_table(
    cross_package_dependencies: List[str],
    markdown: bool = True,
) -> str:
    """
    Converts cross-package dependencies to a markdown table
    :param cross_package_dependencies: list of cross-package dependencies
    :param markdown: if True, markdown format is used else rst
    :return: formatted table
    """
    from tabulate import tabulate

    headers = ["Dependent package", "Extra"]
    table_data = []
    prefix = "apache-airflow-providers-"
    base_url = "https://airflow.apache.org/docs/"
    for dependency in cross_package_dependencies:
        pip_package_name = f"{prefix}{dependency.replace('.','-')}"
        url_suffix = f"{dependency.replace('.','-')}"
        if markdown:
            url = f"[{pip_package_name}]({base_url}{url_suffix})"
        else:
            url = f"`{pip_package_name} <{base_url}{prefix}{url_suffix}>`_"
        table_data.append((url, f"`{dependency}`" if markdown else f"``{dependency}``"))
    return tabulate(table_data, headers=headers, tablefmt="pipe" if markdown else "rst")


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

LICENCE_RST = """
.. Licensed to the Apache Software Foundation (ASF) under one
   or more contributor license agreements.  See the NOTICE file
   distributed with this work for additional information
   regarding copyright ownership.  The ASF licenses this file
   to you under the Apache License, Version 2.0 (the
   "License"); you may not use this file except in compliance
   with the License.  You may obtain a copy of the License at

..   http://www.apache.org/licenses/LICENSE-2.0

.. Unless required by applicable law or agreed to in writing,
   software distributed under the License is distributed on an
   "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
   KIND, either express or implied.  See the License for the
   specific language governing permissions and limitations
   under the License.
"""

"""
Keeps information about historical releases.
"""
ReleaseInfo = collections.namedtuple(
    "ReleaseInfo", "release_version release_version_no_leading_zeros last_commit_hash content file_name"
)


def strip_leading_zeros(version: str) -> str:
    """
    Strips leading zeros from version number.

    This converts 1974.04.03 to 1974.4.3 as the format with leading month and day zeros is not accepted
    by PIP versioning.

    :param version: version number in CALVER format (potentially with leading 0s in date and month)
    :return: string with leading 0s after dot replaced.
    """
    return ".".join(str(int(i)) for i in version.split("."))


def get_previous_release_info(
    previous_release_version: Optional[str], past_releases: List[ReleaseInfo], current_release_version: str
) -> Optional[str]:
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
    current_release_version: str,
) -> Tuple[str, Optional[str]]:
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
    if previous_release_version:
        if Version(current_release_version) < Version(previous_release_version):
            console.print(
                f"[red]The release {current_release_version} must be not less than "
                f"{previous_release_version} - last release for the package[/]"
            )
            raise Exception("Bad release version")
    return current_release_version, previous_release_version


def get_cross_provider_dependent_packages(provider_package_id: str) -> List[str]:
    """
    Returns cross-provider dependencies for the package.
    :param provider_package_id: package id
    :return: list of cross-provider dependencies
    """
    with open(os.path.join(PROVIDERS_PATH, "dependencies.json")) as dependencies_file:
        dependent_packages = json.load(dependencies_file).get(provider_package_id) or []
    return dependent_packages


def make_sure_remote_apache_exists_and_fetch(git_update: bool, verbose: bool):
    """
    Make sure that apache remote exist in git. We need to take a log from the apache
    repository - not locally.

    Also the local repo might be shallow so we need to unshallow it.

    This will:
    * check if the remote exists and add if it does not
    * check if the local repo is shallow, mark it to be unshallowed in this case
    * fetch from the remote including all tags and overriding local tags in case they are set differently

    :param git_update: If the git remote already exists, should we try to update it
    :param verbose: print verbose messages while fetching
    """
    try:
        check_remote_command = ["git", "remote", "get-url", HTTPS_REMOTE]
        if verbose:
            console.print(f"Running command: '{' '.join(check_remote_command)}'")
        subprocess.check_call(
            check_remote_command,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )

        # Remote already exists, don't update it again!
        if not git_update:
            return
    except subprocess.CalledProcessError as ex:
        if ex.returncode == 128:
            remote_add_command = [
                "git",
                "remote",
                "add",
                HTTPS_REMOTE,
                "https://github.com/apache/airflow.git",
            ]
            if verbose:
                console.print(f"Running command: '{' '.join(remote_add_command)}'")
            try:
                subprocess.check_output(
                    remote_add_command,
                    stderr=subprocess.STDOUT,
                )
            except subprocess.CalledProcessError as ex:
                console.print("[red]Error: when adding remote:[/]", ex)
        else:
            raise
    if verbose:
        console.print("Fetching full history and tags from remote. ")
        console.print("This might override your local tags!")
    is_shallow_repo = (
        subprocess.check_output(["git", "rev-parse", "--is-shallow-repository"], stderr=subprocess.DEVNULL)
        == 'true'
    )
    fetch_command = ["git", "fetch", "--tags", "--force", HTTPS_REMOTE]
    if is_shallow_repo:
        if verbose:
            console.print(
                "This will also unshallow the repository, "
                "making all history available and increasing storage!"
            )
        fetch_command.append("--unshallow")
    if verbose:
        console.print(f"Running command: '{' '.join(fetch_command)}'")
    subprocess.check_call(
        fetch_command,
        stderr=subprocess.DEVNULL,
    )


def get_git_log_command(
    verbose: bool, from_commit: Optional[str] = None, to_commit: Optional[str] = None
) -> List[str]:
    """
    Get git command to run for the current repo from the current folder (which is the package folder).
    :param verbose: whether to print verbose info while getting the command
    :param from_commit: if present - base commit from which to start the log from
    :param to_commit: if present - final commit which should be the start of the log
    :return: git command to run
    """
    git_cmd = [
        "git",
        "log",
        "--pretty=format:%H %h %cd %s",
        "--date=short",
    ]
    if from_commit and to_commit:
        git_cmd.append(f"{from_commit}...{to_commit}")
    elif from_commit:
        git_cmd.append(from_commit)
    git_cmd.extend(['--', '.'])
    if verbose:
        console.print(f"Command to run: '{' '.join(git_cmd)}'")
    return git_cmd


def get_git_tag_check_command(tag: str) -> List[str]:
    """
    Get git command to check if tag exits.
    :param tag: Tag to check
    :return: git command to run
    """
    return [
        "git",
        "rev-parse",
        tag,
    ]


def get_source_package_path(provider_package_id: str) -> str:
    """
    Retrieves source package path from package id.
    :param provider_package_id: id of the package
    :return: path of the providers folder
    """
    return os.path.join(PROVIDERS_PATH, *provider_package_id.split("."))


def get_documentation_package_path(provider_package_id: str) -> str:
    """
    Retrieves documentation package path from package id.
    :param provider_package_id: id of the package
    :return: path of the documentation folder
    """
    return os.path.join(
        DOCUMENTATION_PATH, f"apache-airflow-providers-{provider_package_id.replace('.','-')}"
    )


def get_generated_package_path(provider_package_id: str) -> str:
    """
    Retrieves generated package path from package id.
    :param provider_package_id: id of the package
    :return: path of the providers folder
    """
    provider_package_path = os.path.join(GENERATED_PROVIDERS_PATH, *provider_package_id.split("."))
    return provider_package_path


def get_additional_package_info(provider_package_path: str) -> str:
    """
    Returns additional info for the package.

    :param provider_package_path: path for the package
    :return: additional information for the path (empty string if missing)
    """
    additional_info_file_path = os.path.join(provider_package_path, "ADDITIONAL_INFO.md")
    if os.path.isfile(additional_info_file_path):
        with open(additional_info_file_path) as additional_info_file:
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


def is_camel_case_with_acronyms(s: str):
    """
    Checks if the string passed is Camel Case (with capitalised acronyms allowed).
    :param s: string to check
    :return: true if the name looks cool as Class name.
    """
    return s != s.lower() and s != s.upper() and "_" not in s and s[0].upper() == s[0]


def check_if_classes_are_properly_named(
    entity_summary: Dict[EntityType, EntityTypeSummary]
) -> Tuple[int, int]:
    """
    Check if all entities in the dictionary are named properly. It prints names at the output
    and returns the status of class names.

    :param entity_summary: dictionary of class names to check, grouped by types.
    :return: Tuple of 2 ints = total number of entities and number of badly named entities
    """
    total_class_number = 0
    badly_named_class_number = 0
    for entity_type, class_suffix in EXPECTED_SUFFIXES.items():
        for class_full_name in entity_summary[entity_type].entities:
            _, class_name = class_full_name.rsplit(".", maxsplit=1)
            error_encountered = False
            if not is_camel_case_with_acronyms(class_name):
                console.print(
                    f"[red]The class {class_full_name} is wrongly named. The "
                    f"class name should be CamelCaseWithACRONYMS ![/]"
                )
                error_encountered = True
            if not class_name.endswith(class_suffix):
                console.print(
                    f"[red]The class {class_full_name} is wrongly named. It is one of the {entity_type.value}"
                    f" so it should end with {class_suffix}[/]"
                )
                error_encountered = True
            total_class_number += 1
            if error_encountered:
                badly_named_class_number += 1
    return total_class_number, badly_named_class_number


def get_package_pip_name(provider_package_id: str):
    return f"apache-airflow-providers-{provider_package_id.replace('.', '-')}"


def validate_provider_info_with_runtime_schema(provider_info: Dict[str, Any]) -> None:
    """
    Validates provider info against the runtime schema. This way we check if the provider info in the
    packages is future-compatible. The Runtime Schema should only change when there is a major version
    change.

    :param provider_info: provider info to validate
    """

    with open(PROVIDER_RUNTIME_DATA_SCHEMA_PATH) as schema_file:
        schema = json.load(schema_file)
    try:
        jsonschema.validate(provider_info, schema=schema)
    except jsonschema.ValidationError as ex:
        console.print("[red]Provider info not validated against runtime schema[/]")
        raise Exception(
            "Error when validating schema. The schema must be compatible with "
            + "airflow/provider_info.schema.json.",
            ex,
        )


def get_provider_yaml(provider_package_id: str) -> Dict[str, Any]:
    """
    Retrieves provider info from the provider yaml file. The provider yaml file contains more information
    than provider_info that is used at runtime. This method converts the full provider yaml file into
    stripped-down provider info and validates it against deprecated 2.0.0 schema and runtime schema.
    :param provider_package_id: package id to retrieve provider.yaml from
    :return: provider_info dictionary
    """
    provider_yaml_file_name = os.path.join(get_source_package_path(provider_package_id), "provider.yaml")
    if not os.path.exists(provider_yaml_file_name):
        raise Exception(f"The provider.yaml file is missing: {provider_yaml_file_name}")
    with open(provider_yaml_file_name) as provider_file:
        provider_yaml_dict = safe_load(provider_file)
    return provider_yaml_dict


def get_provider_info_from_provider_yaml(provider_package_id: str) -> Dict[str, Any]:
    """
    Retrieves provider info from the provider yaml file.
    :param provider_package_id: package id to retrieve provider.yaml from
    :return: provider_info dictionary
    """
    provider_yaml_dict = get_provider_yaml(provider_package_id=provider_package_id)
    validate_provider_info_with_runtime_schema(provider_yaml_dict)
    return provider_yaml_dict


def get_version_tag(version: str, provider_package_id: str, version_suffix: str = ''):
    if version_suffix is None:
        version_suffix = ''
    return f"providers-{provider_package_id.replace('.','-')}/{version}{version_suffix}"


def print_changes_table(changes_table):
    syntax = Syntax(changes_table, "rst", theme="ansi_dark")
    console.print(syntax)


def get_all_changes_for_package(
    versions: List[str],
    provider_package_id: str,
    source_provider_package_path: str,
    verbose: bool,
) -> Tuple[bool, Optional[List[List[Change]]], str]:
    """
    Retrieves all changes for the package.
    :param versions: list of versions
    :param provider_package_id: provider package id
    :param source_provider_package_path: path where package is located
    :param verbose: whether to print verbose messages

    """
    current_version = versions[0]
    current_tag_no_suffix = get_version_tag(current_version, provider_package_id)
    if verbose:
        console.print(f"Checking if tag '{current_tag_no_suffix}' exist.")
    if not subprocess.call(
        get_git_tag_check_command(current_tag_no_suffix),
        cwd=source_provider_package_path,
        stderr=subprocess.DEVNULL,
    ):
        if verbose:
            console.print(f"The tag {current_tag_no_suffix} exists.")
        # The tag already exists
        changes = subprocess.check_output(
            get_git_log_command(verbose, HEAD_OF_HTTPS_REMOTE, current_tag_no_suffix),
            cwd=source_provider_package_path,
            universal_newlines=True,
        )
        if changes:
            provider_details = get_provider_details(provider_package_id)
            doc_only_change_file = os.path.join(
                provider_details.source_provider_package_path, ".latest-doc-only-change.txt"
            )
            if os.path.exists(doc_only_change_file):
                with open(doc_only_change_file) as f:
                    last_doc_only_hash = f.read().strip()
                try:
                    changes_since_last_doc_only_check = subprocess.check_output(
                        get_git_log_command(verbose, HEAD_OF_HTTPS_REMOTE, last_doc_only_hash),
                        cwd=source_provider_package_path,
                        universal_newlines=True,
                    )
                    if not changes_since_last_doc_only_check:
                        console.print()
                        console.print(
                            "[yellow]The provider has doc-only changes since the last release. Skipping[/]"
                        )
                        # Returns 66 in case of doc-only changes
                        sys.exit(66)
                except subprocess.CalledProcessError:
                    # ignore when the commit mentioned as last doc-only change is obsolete
                    pass
            console.print(f"[yellow]The provider {provider_package_id} has changes since last release[/]")
            console.print()
            console.print(
                "[yellow]Please update version in "
                f"'airflow/providers/{provider_package_id.replace('-','/')}/'"
                "provider.yaml'[/]\n"
            )
            console.print("[yellow]Or mark the changes as doc-only[/]")
            changes_table, array_of_changes = convert_git_changes_to_table(
                "UNKNOWN",
                changes,
                base_url="https://github.com/apache/airflow/commit/",
                markdown=False,
            )
            print_changes_table(changes_table)
            return False, array_of_changes[0], changes_table
        else:
            console.print(f"No changes for {provider_package_id}")
            return False, None, ""
    if verbose:
        console.print("The tag does not exist. ")
    if len(versions) == 1:
        console.print(
            f"The provider '{provider_package_id}' has never been released but it is ready to release!\n"
        )
    else:
        console.print(f"New version of the '{provider_package_id}' package is ready to be released!\n")
    next_version_tag = HEAD_OF_HTTPS_REMOTE
    changes_table = ''
    current_version = versions[0]
    list_of_list_of_changes: List[List[Change]] = []
    for version in versions[1:]:
        version_tag = get_version_tag(version, provider_package_id)
        changes = subprocess.check_output(
            get_git_log_command(verbose, next_version_tag, version_tag),
            cwd=source_provider_package_path,
            universal_newlines=True,
        )
        changes_table_for_version, array_of_changes_for_version = convert_git_changes_to_table(
            current_version, changes, base_url="https://github.com/apache/airflow/commit/", markdown=False
        )
        changes_table += changes_table_for_version
        list_of_list_of_changes.append(array_of_changes_for_version)
        next_version_tag = version_tag
        current_version = version
    changes = subprocess.check_output(
        get_git_log_command(verbose, next_version_tag),
        cwd=source_provider_package_path,
        universal_newlines=True,
    )
    changes_table_for_version, array_of_changes_for_version = convert_git_changes_to_table(
        current_version, changes, base_url="https://github.com/apache/airflow/commit/", markdown=False
    )
    changes_table += changes_table_for_version
    if verbose:
        print_changes_table(changes_table)
    return True, list_of_list_of_changes if len(list_of_list_of_changes) > 0 else None, changes_table


def get_provider_details(provider_package_id: str) -> ProviderPackageDetails:
    provider_info = get_provider_info_from_provider_yaml(provider_package_id)
    return ProviderPackageDetails(
        provider_package_id=provider_package_id,
        full_package_name=f"airflow.providers.{provider_package_id}",
        pypi_package_name=f"apache-airflow-providers-{provider_package_id.replace('.', '-')}",
        source_provider_package_path=get_source_package_path(provider_package_id),
        documentation_provider_package_path=get_documentation_package_path(provider_package_id),
        provider_description=provider_info['description'],
        versions=provider_info['versions'],
        excluded_python_versions=provider_info.get("excluded-python-versions") or [],
    )


def get_provider_requirements(provider_package_id: str) -> List[str]:
    provider_yaml = get_provider_yaml(provider_package_id)
    requirements = (
        provider_yaml['additional-dependencies'].copy() if 'additional-dependencies' in provider_yaml else []
    )
    requirements.extend(PROVIDERS_REQUIREMENTS[provider_package_id])
    return requirements


def get_provider_jinja_context(
    provider_info: Dict[str, Any],
    provider_details: ProviderPackageDetails,
    current_release_version: str,
    version_suffix: str,
):
    verify_provider_package(provider_details.provider_package_id)
    changelog_path = verify_changelog_exists(provider_details.provider_package_id)
    cross_providers_dependencies = get_cross_provider_dependent_packages(
        provider_package_id=provider_details.provider_package_id
    )
    release_version_no_leading_zeros = strip_leading_zeros(current_release_version)
    pip_requirements_table = convert_pip_requirements_to_table(
        get_provider_requirements(provider_details.provider_package_id)
    )
    pip_requirements_table_rst = convert_pip_requirements_to_table(
        get_provider_requirements(provider_details.provider_package_id), markdown=False
    )
    cross_providers_dependencies_table = convert_cross_package_dependencies_to_table(
        cross_providers_dependencies
    )
    cross_providers_dependencies_table_rst = convert_cross_package_dependencies_to_table(
        cross_providers_dependencies, markdown=False
    )
    with open(changelog_path) as changelog_file:
        changelog = changelog_file.read()
    supported_python_versions = [
        p for p in ALL_PYTHON_VERSIONS if p not in provider_details.excluded_python_versions
    ]
    python_requires = "~=3.6"
    for p in provider_details.excluded_python_versions:
        python_requires += f", !={p}"
    context: Dict[str, Any] = {
        "ENTITY_TYPES": list(EntityType),
        "README_FILE": "README.rst",
        "PROVIDER_PACKAGE_ID": provider_details.provider_package_id,
        "PACKAGE_PIP_NAME": get_pip_package_name(provider_details.provider_package_id),
        "PACKAGE_WHEEL_NAME": get_wheel_package_name(provider_details.provider_package_id),
        "FULL_PACKAGE_NAME": provider_details.full_package_name,
        "PROVIDER_PATH": provider_details.full_package_name.replace(".", "/"),
        "RELEASE": current_release_version,
        "RELEASE_NO_LEADING_ZEROS": release_version_no_leading_zeros,
        "VERSION_SUFFIX": version_suffix or '',
        "ADDITIONAL_INFO": get_additional_package_info(
            provider_package_path=provider_details.source_provider_package_path
        ),
        "CROSS_PROVIDERS_DEPENDENCIES": cross_providers_dependencies,
        "PIP_REQUIREMENTS": PROVIDERS_REQUIREMENTS[provider_details.provider_package_id],
        "PROVIDER_TYPE": "Provider",
        "PROVIDERS_FOLDER": "providers",
        "PROVIDER_DESCRIPTION": provider_details.provider_description,
        "INSTALL_REQUIREMENTS": get_install_requirements(
            provider_package_id=provider_details.provider_package_id, version_suffix=version_suffix
        ),
        "SETUP_REQUIREMENTS": get_setup_requirements(),
        "EXTRAS_REQUIREMENTS": get_package_extras(provider_package_id=provider_details.provider_package_id),
        "CROSS_PROVIDERS_DEPENDENCIES_TABLE": cross_providers_dependencies_table,
        "CROSS_PROVIDERS_DEPENDENCIES_TABLE_RST": cross_providers_dependencies_table_rst,
        "PIP_REQUIREMENTS_TABLE": pip_requirements_table,
        "PIP_REQUIREMENTS_TABLE_RST": pip_requirements_table_rst,
        "PROVIDER_INFO": provider_info,
        "CHANGELOG_RELATIVE_PATH": relpath(
            provider_details.source_provider_package_path,
            provider_details.documentation_provider_package_path,
        ),
        "CHANGELOG": changelog,
        "SUPPORTED_PYTHON_VERSIONS": supported_python_versions,
        "PYTHON_REQUIRES": python_requires,
    }
    return context


def prepare_readme_file(context):
    readme_content = LICENCE_RST + render_template(
        template_name="PROVIDER_README", context=context, extension=".rst"
    )
    readme_file_path = os.path.join(TARGET_PROVIDER_PACKAGES_PATH, "README.rst")
    with open(readme_file_path, "wt") as readme_file:
        readme_file.write(readme_content)


def confirm(message: str):
    """
    Ask user to confirm (case-insensitive).
    :return: True if the answer is Y. Exits with 65 exit code if Q is chosen.
    :rtype: bool
    """
    answer = ""
    while answer not in ["y", "n", "q"]:
        console.print(f"[yellow]{message}[Y/N/Q]?[/] ", end='')
        answer = input("").lower()
    if answer == "q":
        # Returns 65 in case user decided to quit
        sys.exit(65)
    return answer == "y"


def mark_latest_changes_as_documentation_only(
    provider_details: ProviderPackageDetails, latest_change: Change
):
    console.print(
        f"Marking last change: {latest_change.short_hash} and all above changes since the last release "
        "as doc-only changes!"
    )
    with open(
        os.path.join(provider_details.source_provider_package_path, ".latest-doc-only-change.txt"), "tw"
    ) as f:
        f.write(latest_change.full_hash + "\n")
        # exit code 66 marks doc-only change marked
        sys.exit(66)


def update_release_notes(
    provider_package_id: str,
    version_suffix: str,
    force: bool,
    verbose: bool,
    interactive: bool,
) -> bool:
    """
    Updates generated files (readme, changes and/or setup.cfg/setup.py/manifest.in/provider_info)

    :param provider_package_id: id of the package
    :param version_suffix: version suffix corresponding to the version in the code
    :param force: regenerate already released documentation
    :param verbose: whether to print verbose messages
    :param interactive: whether the script should ask the user in case of doubt
    :returns False if the package should be skipped, True if everything generated properly
    """
    verify_provider_package(provider_package_id)
    provider_details = get_provider_details(provider_package_id)
    provider_info = get_provider_info_from_provider_yaml(provider_package_id)
    current_release_version = provider_details.versions[0]
    jinja_context = get_provider_jinja_context(
        provider_info=provider_info,
        provider_details=provider_details,
        current_release_version=current_release_version,
        version_suffix=version_suffix,
    )
    proceed, latest_change, changes = get_all_changes_for_package(
        provider_details.versions,
        provider_package_id,
        provider_details.source_provider_package_path,
        verbose,
    )
    if not force:
        if proceed:
            if interactive and not confirm("Provider marked for release. Proceed?"):
                return False
        elif not latest_change:
            console.print()
            console.print(
                f"[yellow]Provider: {provider_package_id} - skipping documentation generation. No changes![/]"
            )
            console.print()
            return False
        else:
            if interactive and confirm("Are those changes documentation-only?"):
                mark_latest_changes_as_documentation_only(provider_details, latest_change)
            return False

    jinja_context["DETAILED_CHANGES_RST"] = changes
    jinja_context["DETAILED_CHANGES_PRESENT"] = len(changes) > 0
    update_commits_rst(
        jinja_context, provider_package_id, provider_details.documentation_provider_package_path
    )
    return True


def update_setup_files(
    provider_package_id: str,
    version_suffix: str,
):
    """
    Updates generated setup.cfg/setup.py/manifest.in/provider_info) for packages

    :param provider_package_id: id of the package
    :param version_suffix: version suffix corresponding to the version in the code
    :returns False if the package should be skipped, True if everything generated properly
    """
    verify_provider_package(provider_package_id)
    provider_details = get_provider_details(provider_package_id)
    provider_info = get_provider_info_from_provider_yaml(provider_package_id)
    current_release_version = provider_details.versions[0]
    jinja_context = get_provider_jinja_context(
        provider_info=provider_info,
        provider_details=provider_details,
        current_release_version=current_release_version,
        version_suffix=version_suffix,
    )
    console.print()
    console.print(f"Generating setup files for {provider_package_id}")
    console.print()
    prepare_setup_py_file(jinja_context)
    prepare_setup_cfg_file(jinja_context)
    prepare_get_provider_info_py_file(jinja_context, provider_package_id)
    prepare_manifest_in_file(jinja_context)
    prepare_readme_file(jinja_context)
    return True


def replace_content(file_path, old_text, new_text, provider_package_id):
    if new_text != old_text:
        _, temp_file_path = tempfile.mkstemp()
        try:
            if os.path.isfile(file_path):
                copyfile(file_path, temp_file_path)
            with open(file_path, "wt") as readme_file:
                readme_file.write(new_text)
            console.print()
            console.print(f"Generated {file_path} file for the {provider_package_id} provider")
            console.print()
            if old_text != "":
                subprocess.call(["diff", "--color=always", temp_file_path, file_path])
        finally:
            os.remove(temp_file_path)


AUTOMATICALLY_GENERATED_MARKER = "AUTOMATICALLY GENERATED"
AUTOMATICALLY_GENERATED_CONTENT = (
    f".. THE REMAINDER OF THE FILE IS {AUTOMATICALLY_GENERATED_MARKER}. "
    f"IT WILL BE OVERWRITTEN AT RELEASE TIME!"
)


def update_index_rst(
    context,
    provider_package_id,
    target_path,
):
    index_update = render_template(
        template_name="PROVIDER_INDEX", context=context, extension='.rst', keep_trailing_newline=True
    )
    index_file_path = os.path.join(target_path, "index.rst")
    old_text = ""
    if os.path.isfile(index_file_path):
        with open(index_file_path) as readme_file_read:
            old_text = readme_file_read.read()
    new_text = deepcopy(old_text)
    lines = old_text.splitlines(keepends=False)
    for index, line in enumerate(lines):
        if AUTOMATICALLY_GENERATED_MARKER in line:
            new_text = "\n".join(lines[:index])
    new_text += "\n" + AUTOMATICALLY_GENERATED_CONTENT + "\n"
    new_text += index_update
    replace_content(index_file_path, old_text, new_text, provider_package_id)


def update_commits_rst(
    context,
    provider_package_id,
    target_path,
):
    new_text = render_template(
        template_name="PROVIDER_COMMITS", context=context, extension='.rst', keep_trailing_newline=True
    )
    index_file_path = os.path.join(target_path, "commits.rst")
    old_text = ""
    if os.path.isfile(index_file_path):
        with open(index_file_path) as readme_file_read:
            old_text = readme_file_read.read()
    replace_content(index_file_path, old_text, new_text, provider_package_id)


@lru_cache(maxsize=None)
def black_mode():
    from black import Mode, parse_pyproject_toml, target_version_option_callback

    config = parse_pyproject_toml(os.path.join(SOURCE_DIR_PATH, "pyproject.toml"))

    target_versions = set(
        target_version_option_callback(None, None, config.get('target_version', [])),
    )

    return Mode(
        target_versions=target_versions,
        line_length=config.get('line_length', Mode.line_length),
        is_pyi=config.get('is_pyi', Mode.is_pyi),
        string_normalization=not config.get('skip_string_normalization', not Mode.string_normalization),
        experimental_string_processing=config.get(
            'experimental_string_processing', Mode.experimental_string_processing
        ),
    )


def black_format(content) -> str:
    from black import format_str

    return format_str(content, mode=black_mode())


def prepare_setup_py_file(context):
    setup_py_template_name = "SETUP"
    setup_py_file_path = os.path.abspath(os.path.join(get_target_folder(), "setup.py"))
    setup_py_content = render_template(
        template_name=setup_py_template_name, context=context, extension='.py', autoescape=False
    )
    with open(setup_py_file_path, "wt") as setup_py_file:
        setup_py_file.write(black_format(setup_py_content))


def prepare_setup_cfg_file(context):
    setup_cfg_template_name = "SETUP"
    setup_cfg_file_path = os.path.abspath(os.path.join(get_target_folder(), "setup.cfg"))
    setup_cfg_content = render_template(
        template_name=setup_cfg_template_name,
        context=context,
        extension='.cfg',
        autoescape=False,
        keep_trailing_newline=True,
    )
    with open(setup_cfg_file_path, "wt") as setup_cfg_file:
        setup_cfg_file.write(setup_cfg_content)


def prepare_get_provider_info_py_file(context, provider_package_id: str):
    get_provider_template_name = "get_provider_info"
    get_provider_file_path = os.path.abspath(
        os.path.join(
            get_target_providers_package_folder(provider_package_id),
            "get_provider_info.py",
        )
    )
    get_provider_content = render_template(
        template_name=get_provider_template_name,
        context=context,
        extension='.py',
        autoescape=False,
        keep_trailing_newline=True,
    )
    with open(get_provider_file_path, "wt") as get_provider_file:
        get_provider_file.write(black_format(get_provider_content))


def prepare_manifest_in_file(context):
    target = os.path.abspath(os.path.join(get_target_folder(), "MANIFEST.in"))
    content = render_template(
        template_name="MANIFEST",
        context=context,
        extension='.in',
        autoescape=False,
        keep_trailing_newline=True,
    )
    with open(target, "wt") as fh:
        fh.write(content)


def get_all_providers() -> List[str]:
    """
    Returns all providers for regular packages.
    :return: list of providers that are considered for provider packages
    """
    return list(PROVIDERS_REQUIREMENTS.keys())


def verify_provider_package(provider_package_id: str) -> str:
    """
    Verifies if the provider package is good.
    :param provider_package_id: package id to verify
    :return: None
    """
    if provider_package_id not in get_provider_packages():
        console.print(f"[red]Wrong package name: {provider_package_id}[/]")
        console.print("Use one of:")
        console.print(get_provider_packages())
        raise Exception(f"The package {provider_package_id} is not a provider package.")


def verify_changelog_exists(package: str) -> str:
    provider_details = get_provider_details(package)
    changelog_path = os.path.join(provider_details.source_provider_package_path, "CHANGELOG.rst")
    if not os.path.isfile(changelog_path):
        console.print(f"[red]ERROR: Missing ${changelog_path}[/]")
        console.print("Please add the file with initial content:")
        console.print()
        syntax = Syntax(
            INITIAL_CHANGELOG_CONTENT,
            "rst",
            theme="ansi_dark",
        )
        console.print(syntax)
        console.print()
        raise Exception(f"Missing {changelog_path}")
    return changelog_path


@cli.command()
def list_providers_packages():
    """List all provider packages."""
    providers = get_all_providers()
    for provider in providers:
        console.print(provider)


@cli.command()
@option_version_suffix
@option_git_update
@option_interactive
@argument_package_id
@option_force
@option_verbose
def update_package_documentation(
    version_suffix: str,
    git_update: bool,
    interactive: bool,
    package_id: str,
    force: bool,
    verbose: bool,
):
    """
    Updates package documentation.

    See `list-providers-packages` subcommand for the possible PACKAGE_ID values
    """
    provider_package_id = package_id
    verify_provider_package(provider_package_id)
    with with_group(f"Update release notes for package '{provider_package_id}' "):
        console.print("Updating documentation for the latest release version.")
        make_sure_remote_apache_exists_and_fetch(git_update, verbose)
        if not update_release_notes(
            provider_package_id, version_suffix, force=force, verbose=verbose, interactive=interactive
        ):
            # Returns 64 in case of skipped package
            sys.exit(64)


def tag_exists_for_version(provider_package_id: str, current_tag: str, verbose: bool):
    provider_details = get_provider_details(provider_package_id)
    if verbose:
        console.print(f"Checking if tag `{current_tag}` exists.")
    if not subprocess.call(
        get_git_tag_check_command(current_tag),
        cwd=provider_details.source_provider_package_path,
        stderr=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
    ):
        if verbose:
            console.print(f"Tag `{current_tag}` exists.")
        return True
    if verbose:
        console.print(f"Tag `{current_tag}` does not exist.")
    return False


@cli.command()
@option_version_suffix
@option_git_update
@argument_package_id
@option_verbose
def generate_setup_files(version_suffix: str, git_update: bool, package_id: str, verbose: bool):
    """
    Generates setup files for the package.

    See `list-providers-packages` subcommand for the possible PACKAGE_ID values
    """
    provider_package_id = package_id
    with with_group(f"Generate setup files for '{provider_package_id}'"):
        current_tag = get_current_tag(provider_package_id, version_suffix, git_update, verbose)
        if tag_exists_for_version(provider_package_id, current_tag, verbose):
            console.print(f"[yellow]The tag {current_tag} exists. Not preparing the package.[/]")
            # Returns 1 in case of skipped package
            sys.exit(1)
        else:
            if update_setup_files(
                provider_package_id,
                version_suffix,
            ):
                console.print(f"[green]Generated regular package setup files for {provider_package_id}[/]")
            else:
                # Returns 64 in case of skipped package
                sys.exit(64)


def get_current_tag(provider_package_id: str, suffix: str, git_update: bool, verbose: bool):
    verify_provider_package(provider_package_id)
    make_sure_remote_apache_exists_and_fetch(git_update, verbose)
    provider_info = get_provider_info_from_provider_yaml(provider_package_id)
    versions: List[str] = provider_info['versions']
    current_version = versions[0]
    current_tag = get_version_tag(current_version, provider_package_id, suffix)
    return current_tag


def cleanup_remnants(verbose: bool):
    if verbose:
        console.print("Cleaning remnants")
    files = glob.glob("*.egg-info")
    for file in files:
        shutil.rmtree(file, ignore_errors=True)
    files = glob.glob("build")
    for file in files:
        shutil.rmtree(file, ignore_errors=True)


def verify_setup_py_prepared(provider_package):
    with open("setup.py") as f:
        setup_content = f.read()
    search_for = f"providers-{provider_package.replace('.','-')} for Apache Airflow"
    if search_for not in setup_content:
        console.print(
            f"[red]The setup.py is probably prepared for another package. "
            f"It does not contain [bold]{search_for}[/bold]![/]"
        )
        console.print(
            f"\nRun:\n\n[bold]./dev/provider_packages/prepare_provider_packages.py "
            f"generate-setup-files {provider_package}[/bold]\n"
        )
        raise Exception("Wrong setup!")


@cli.command()
@click.option(
    '--package-format',
    type=click.Choice(['sdist', 'wheel', 'both']),
    default='wheel',
    help='Optional format - only used in case of building packages (default: wheel)',
)
@option_git_update
@option_version_suffix
@argument_package_id
@option_verbose
def build_provider_packages(
    package_format: str,
    git_update: bool,
    version_suffix: str,
    package_id: str,
    verbose: bool,
):
    """
    Builds provider package.

    See `list-providers-packages` subcommand for the possible PACKAGE_ID values
    """

    import tempfile

    # we cannot use context managers because if the directory gets deleted (which bdist_wheel does),
    # the context manager will throw an exception when trying to delete it again
    tmp_build_dir = tempfile.TemporaryDirectory().name
    tmp_dist_dir = tempfile.TemporaryDirectory().name
    try:
        provider_package_id = package_id
        with with_group(f"Prepare provider package for '{provider_package_id}'"):
            current_tag = get_current_tag(provider_package_id, version_suffix, git_update, verbose)
            if tag_exists_for_version(provider_package_id, current_tag, verbose):
                console.print(f"[yellow]The tag {current_tag} exists. Skipping the package.[/]")
                return False
            console.print(f"Changing directory to ${TARGET_PROVIDER_PACKAGES_PATH}")
            os.chdir(TARGET_PROVIDER_PACKAGES_PATH)
            cleanup_remnants(verbose)
            provider_package = package_id
            verify_setup_py_prepared(provider_package)

            console.print(f"Building provider package: {provider_package} in format {package_format}")
            command = ["python3", "setup.py", "build", "--build-temp", tmp_build_dir]
            if version_suffix is not None:
                command.extend(['egg_info', '--tag-build', version_suffix])
            if package_format in ['sdist', 'both']:
                command.append("sdist")
            if package_format in ['wheel', 'both']:
                command.extend(["bdist_wheel", "--bdist-dir", tmp_dist_dir])
            console.print(f"Executing command: '{' '.join(command)}'")
            try:
                subprocess.check_call(command, stdout=subprocess.DEVNULL)
            except subprocess.CalledProcessError as ex:
                console.print(ex.output.decode())
                raise Exception("The command returned an error %s", command)
            console.print(
                f"[green]Prepared provider package {provider_package} in format {package_format}[/]"
            )
    finally:
        shutil.rmtree(tmp_build_dir, ignore_errors=True)
        shutil.rmtree(tmp_dist_dir, ignore_errors=True)


def verify_provider_classes_for_single_provider(imported_classes: List[str], provider_package_id: str):
    """Verify naming of provider classes for single provider."""
    full_package_name = f"airflow.providers.{provider_package_id}"
    entity_summaries = get_package_class_summary(full_package_name, imported_classes)
    total, bad = check_if_classes_are_properly_named(entity_summaries)
    bad += sum(len(entity_summary.wrong_entities) for entity_summary in entity_summaries.values())
    if bad != 0:
        console.print()
        console.print(f"[red]There are {bad} errors of {total} entities for {provider_package_id}[/]")
        console.print()
    return total, bad


def summarise_total_vs_bad_and_warnings(total: int, bad: int, warns: List[warnings.WarningMessage]) -> bool:
    """Summarises Bad/Good class names for providers and warnings"""
    raise_error = False
    if bad == 0:
        console.print()
        console.print(f"[green]OK: All {total} entities are properly named[/]")
        console.print()
        console.print("Totals:")
        console.print()
        for entity in EntityType:
            console.print(f"{entity.value}: {TOTALS[entity]}")
        console.print()
    else:
        console.print()
        console.print(
            f"[red]ERROR! There are in total: {bad} entities badly named out of {total} entities[/]"
        )
        console.print()
        raise_error = True
    if warns:
        if os.environ.get('GITHUB_ACTIONS'):
            # Ends group in GitHub Actions so that the errors are immediately visible in CI log
            console.print("::endgroup::")
        console.print()
        console.print("[red]Unknown warnings generated:[/]")
        console.print()
        for w in warns:
            one_line_message = str(w.message).replace('\n', ' ')
            console.print(f"{w.filename}:{w.lineno}:[yellow]{one_line_message}[/]")
        console.print()
        console.print(f"[red]ERROR! There were {len(warns)} warnings generated during the import[/]")
        console.print()
        console.print("[yellow]Ideally, fix it, so that no warnings are generated during import.[/]")
        console.print("[yellow]There are two cases that are legitimate deprecation warnings though:[/]")
        console.print("[yellow] 1) when you deprecate whole module or class and replace it in provider[/]")
        console.print("[yellow] 2) when 3rd-party module generates Deprecation and you cannot upgrade it[/]")
        console.print()
        console.print(
            "[yellow]In case 1), add the deprecation message to "
            "the KNOWN_DEPRECATED_DIRECT_IMPORTS in prepare_provider_packages.py[/]"
        )
        console.print(
            "[yellow]In case 2), add the deprecation message together with module it generates to "
            "the KNOWN_DEPRECATED_MESSAGES in prepare_provider_packages.py[/]"
        )
        console.print()
        raise_error = True
    else:
        console.print()
        console.print("[green]OK: No warnings generated[/]")
        console.print()

    if raise_error:
        console.print("[red]Please fix the problems listed above [/]")
        return False
    return True


# The set of known deprecation messages that we know about.
# It contains tuples of "message" and the module that generates the warning - so when the
# Same warning is generated by different module, it is not treated as "known" warning.
KNOWN_DEPRECATED_MESSAGES: Set[Tuple[str, str]] = {
    (
        'This version of Apache Beam has not been sufficiently tested on Python 3.9. '
        'You may encounter bugs or missing features.',
        "apache_beam",
    ),
    (
        "Using or importing the ABCs from 'collections' instead of from 'collections.abc' is deprecated since"
        " Python 3.3,and in 3.9 it will stop working",
        "apache_beam",
    ),
    (
        'pyarrow.HadoopFileSystem is deprecated as of 2.0.0, please use pyarrow.fs.HadoopFileSystem instead.',
        "papermill",
    ),
    (
        "You have an incompatible version of 'pyarrow' installed (4.0.1), please install a version that "
        "adheres to: 'pyarrow<3.1.0,>=3.0.0; extra == \"pandas\"'",
        "apache_beam",
    ),
    (
        "You have an incompatible version of 'pyarrow' installed (4.0.1), please install a version that "
        "adheres to: 'pyarrow<5.1.0,>=5.0.0; extra == \"pandas\"'",
        "snowflake",
    ),
    ("dns.hash module will be removed in future versions. Please use hashlib instead.", "dns"),
    ("PKCS#7 support in pyOpenSSL is deprecated. You should use the APIs in cryptography.", "eventlet"),
    ("PKCS#12 support in pyOpenSSL is deprecated. You should use the APIs in cryptography.", "eventlet"),
    (
        "the imp module is deprecated in favour of importlib; see the module's documentation"
        " for alternative uses",
        "hdfs",
    ),
    ("This operator is deprecated. Please use `airflow.providers.tableau.operators.tableau`.", "salesforce"),
    (
        "You have an incompatible version of 'pyarrow' installed (4.0.1), please install a version that"
        " adheres to: 'pyarrow<3.1.0,>=3.0.0; extra == \"pandas\"'",
        "snowflake",
    ),
    ("SelectableGroups dict interface is deprecated. Use select.", "kombu"),
    ("The module cloudant is now deprecated. The replacement is ibmcloudant.", "cloudant"),
}

# The set of warning messages generated by direct importing of some deprecated modules. We should only
# ignore those messages when the warnings are generated directly by importlib - which means that
# we imported it directly during module walk by the importlib library
KNOWN_DEPRECATED_DIRECT_IMPORTS: Set[str] = {
    "This module is deprecated. Please use `airflow.providers.microsoft.azure.hooks.data_factory`.",
    "This module is deprecated. Please use `airflow.providers.amazon.aws.hooks.dynamodb`.",
    "This module is deprecated. Please use `airflow.providers.microsoft.azure.transfers.local_to_wasb`.",
    "This module is deprecated. Please use `airflow.providers.tableau.operators.tableau_refresh_workbook`.",
    "This module is deprecated. Please use `airflow.providers.tableau.sensors.tableau_job_status`.",
    "This module is deprecated. Please use `airflow.providers.tableau.hooks.tableau`.",
    "This module is deprecated. Please use `kubernetes.client.models.V1Volume`.",
    "This module is deprecated. Please use `kubernetes.client.models.V1VolumeMount`.",
    'numpy.ufunc size changed, may indicate binary incompatibility. Expected 192 from C header,'
    ' got 216 from PyObject',
}


def filter_known_warnings(warn: warnings.WarningMessage) -> bool:
    msg_string = str(warn.message).replace("\n", " ")
    for m in KNOWN_DEPRECATED_MESSAGES:
        expected_package_string = "/" + m[1] + "/"
        if msg_string == m[0] and warn.filename.find(expected_package_string) != -1:
            return False
    return True


def filter_direct_importlib_warning(warn: warnings.WarningMessage) -> bool:
    msg_string = str(warn.message).replace("\n", " ")
    for m in KNOWN_DEPRECATED_DIRECT_IMPORTS:
        if msg_string == m and warn.filename.find("/importlib/") != -1:
            return False
    return True


@cli.command()
def verify_provider_classes():
    """Verifies names for all provider classes."""
    with with_group("Verifies names for all provider classes"):
        provider_ids = get_all_providers()
        imported_classes, warns = import_all_classes(
            provider_ids=provider_ids,
            print_imports=True,
            paths=[PROVIDERS_PATH],
            prefix="airflow.providers.",
        )
        total = 0
        bad = 0
        for provider_package_id in provider_ids:
            inc_total, inc_bad = verify_provider_classes_for_single_provider(
                imported_classes, provider_package_id
            )
            total += inc_total
            bad += inc_bad
        warns = list(filter(filter_known_warnings, warns))
        warns = list(filter(filter_direct_importlib_warning, warns))
        if not summarise_total_vs_bad_and_warnings(total, bad, warns):
            sys.exit(1)


def find_insertion_index_for_version(content: List[str], version: str) -> Tuple[int, bool]:
    """
    Finds insertion index for the specified version from the .rst changelog content.

    :param content: changelog split into separate lines
    :param version: version to look for

    :return: Tuple : insertion_index, append (whether to append or insert the changelog)
    """
    changelog_found = False
    skip_next_line = False
    index = 0
    for index, line in enumerate(content):
        if not changelog_found and line.strip() == version:
            changelog_found = True
            skip_next_line = True
        elif not skip_next_line and line and all(char == '.' for char in line):
            return index - 2, changelog_found
        else:
            skip_next_line = False
    return index, changelog_found


class ClassifiedChanges(NamedTuple):
    """Stores lists of changes classified automatically"""

    fixes: List[Change] = []
    features: List[Change] = []
    breaking_changes: List[Change] = []
    other: List[Change] = []


def get_changes_classified(changes: List[Change]) -> ClassifiedChanges:
    """
    Pre-classifies changes based on commit message, it's wildly guessing now,
    but if we switch to semantic commits, it could be automated. This list is supposed to be manually
    reviewed and re-classified by release manager anyway.

    :param changes: list of changes
    :return: list of changes classified semi-automatically to the fix/feature/breaking/other buckets
    """
    classified_changes = ClassifiedChanges()
    for change in changes:
        if "fix" in change.message.lower():
            classified_changes.fixes.append(change)
        elif "add" in change.message.lower():
            classified_changes.features.append(change)
        elif "breaking" in change.message.lower():
            classified_changes.breaking_changes.append(change)
        else:
            classified_changes.other.append(change)
    return classified_changes


@cli.command()
@argument_package_id
@option_verbose
def update_changelog(package_id: str, verbose: bool):
    """Updates changelog for the provider."""
    if _update_changelog(package_id, verbose):
        sys.exit(64)


def _update_changelog(package_id: str, verbose: bool) -> bool:
    """
    Internal update changelog method
    :param package_id: package id
    :param verbose: verbose flag
    :return: true if package is skipped
    """
    with with_group("Updates changelog for last release"):
        verify_provider_package(package_id)
        provider_details = get_provider_details(package_id)
        provider_info = get_provider_info_from_provider_yaml(package_id)
        current_release_version = provider_details.versions[0]
        jinja_context = get_provider_jinja_context(
            provider_info=provider_info,
            provider_details=provider_details,
            current_release_version=current_release_version,
            version_suffix='',
        )
        changelog_path = os.path.join(provider_details.source_provider_package_path, "CHANGELOG.rst")
        proceed, changes, _ = get_all_changes_for_package(
            provider_details.versions,
            package_id,
            provider_details.source_provider_package_path,
            verbose,
        )
        if not proceed:
            console.print(
                f"[yellow]The provider {package_id} is not being released. Skipping the package.[/]"
            )
            return True
        generate_new_changelog(package_id, provider_details, changelog_path, changes)
        console.print()
        console.print(f"Update index.rst for {package_id}")
        console.print()
        update_index_rst(jinja_context, package_id, provider_details.documentation_provider_package_path)
        return False


def generate_new_changelog(package_id, provider_details, changelog_path, changes):
    latest_version = provider_details.versions[0]
    with open(changelog_path) as changelog:
        current_changelog = changelog.read()
    current_changelog_lines = current_changelog.splitlines()
    insertion_index, append = find_insertion_index_for_version(current_changelog_lines, latest_version)
    if append:
        if not changes:
            console.print(
                f"[green]The provider {package_id} changelog for `{latest_version}` "
                "has first release. Not updating the changelog.[/]"
            )
            return
        new_changes = [
            change for change in changes[0] if change.pr and "(#" + change.pr + ")" not in current_changelog
        ]
        if not new_changes:
            console.print(
                f"[green]The provider {package_id} changelog for `{latest_version}` "
                "has no new changes. Not updating the changelog.[/]"
            )
            return
        context = {"new_changes": new_changes}
        generated_new_changelog = render_template(
            template_name='UPDATE_CHANGELOG', context=context, extension=".rst"
        )
    else:
        classified_changes = get_changes_classified(changes[0])
        context = {
            "version": latest_version,
            "version_header": "." * len(latest_version),
            "classified_changes": classified_changes,
        }
        generated_new_changelog = render_template(
            template_name='CHANGELOG', context=context, extension=".rst"
        )
    new_changelog_lines = current_changelog_lines[0:insertion_index]
    new_changelog_lines.extend(generated_new_changelog.splitlines())
    new_changelog_lines.extend(current_changelog_lines[insertion_index:])
    diff = "\n".join(difflib.context_diff(current_changelog_lines, new_changelog_lines, n=5))
    syntax = Syntax(diff, "diff")
    console.print(syntax)
    if not append:
        console.print(
            f"[green]The provider {package_id} changelog for `{latest_version}` "
            "version is missing. Generating fresh changelog.[/]"
        )
    else:
        console.print(
            f"[green]Appending the provider {package_id} changelog for" f"`{latest_version}` version.[/]"
        )
    with open(changelog_path, "wt") as changelog:
        changelog.write("\n".join(new_changelog_lines))
        changelog.write("\n")


def get_package_from_changelog(changelog_path: str):
    folder = Path(changelog_path).parent
    package = ''
    separator = ''
    while not os.path.basename(folder) == 'providers':
        package = os.path.basename(folder) + separator + package
        separator = '.'
        folder = Path(folder).parent
    return package


@cli.command()
@argument_changelog_files
@option_git_update
@option_verbose
def update_changelogs(changelog_files: List[str], git_update: bool, verbose: bool):
    """Updates changelogs for multiple packages."""
    if git_update:
        make_sure_remote_apache_exists_and_fetch(git_update, verbose)
    for changelog_file in changelog_files:
        package_id = get_package_from_changelog(changelog_file)
        _update_changelog(package_id=package_id, verbose=verbose)


def get_prs_for_package(package_id: str) -> List[int]:
    pr_matcher = re.compile(r".*\(#([0-9]*)\)``$")
    verify_provider_package(package_id)
    changelog_path = verify_changelog_exists(package_id)
    provider_details = get_provider_details(package_id)
    current_release_version = provider_details.versions[0]
    prs = []
    with open(changelog_path) as changelog_file:
        changelog_lines = changelog_file.readlines()
        extract_prs = False
        skip_line = False
        for line in changelog_lines:
            if skip_line:
                # Skip first "....." header
                skip_line = False
                continue
            if line.strip() == current_release_version:
                extract_prs = True
                skip_line = True
                continue
            if extract_prs:
                if all(c == '.' for c in line):
                    # Header for next version reached
                    break
                if line.startswith('.. Below changes are excluded from the changelog'):
                    # The reminder of PRs is not important skipping it
                    break
                match_result = pr_matcher.match(line.strip())
                if match_result:
                    prs.append(int(match_result.group(1)))
    return prs


class ProviderPRInfo(NamedTuple):
    provider_details: ProviderPackageDetails
    pr_list: List[PullRequest.PullRequest]


@cli.command()
@click.option('--github-token', envvar='GITHUB_TOKEN')
@click.option('--suffix', default='rc1')
@click.option('--excluded-pr-list', type=str, help="Coma-separated list of PRs to exclude from the issue.")
@argument_package_ids
def generate_issue_content(package_ids: List[str], github_token: str, suffix: str, excluded_pr_list: str):
    if not package_ids:
        package_ids = get_all_providers()
    """Generates content for issue to test the release."""
    with with_group("Generates GitHub issue content with people who can test it"):
        if excluded_pr_list:
            excluded_prs = [int(pr) for pr in excluded_pr_list.split(",")]
        else:
            excluded_prs = []
        all_prs: Set[int] = set()
        provider_prs: Dict[str, List[int]] = {}
        for package_id in package_ids:
            console.print(f"Extracting PRs for provider {package_id}")
            prs = get_prs_for_package(package_id)
            provider_prs[package_id] = list(filter(lambda pr: pr not in excluded_prs, prs))
            all_prs.update(provider_prs[package_id])
        g = Github(github_token)
        repo = g.get_repo("apache/airflow")
        pull_requests: Dict[int, PullRequest.PullRequest] = {}
        with Progress(console=console) as progress:
            task = progress.add_task(f"Retrieving {len(all_prs)} PRs ", total=len(all_prs))
            pr_list = list(all_prs)
            for i in range(len(pr_list)):
                pr_number = pr_list[i]
                progress.console.print(
                    f"Retrieving PR#{pr_number}: " f"https://github.com/apache/airflow/pull/{pr_number}"
                )
                try:
                    pull_requests[pr_number] = repo.get_pull(pr_number)
                except UnknownObjectException:
                    # Fallback to issue if PR not found
                    try:
                        pull_requests[pr_number] = repo.get_issue(pr_number)  # (same fields as PR)
                    except UnknownObjectException:
                        console.print(f"[red]The PR #{pr_number} could not be found[/]")
                progress.advance(task)
        interesting_providers: Dict[str, ProviderPRInfo] = {}
        non_interesting_providers: Dict[str, ProviderPRInfo] = {}
        for package_id in package_ids:
            pull_request_list = [pull_requests[pr] for pr in provider_prs[package_id] if pr in pull_requests]
            provider_details = get_provider_details(package_id)
            if pull_request_list:
                interesting_providers[package_id] = ProviderPRInfo(provider_details, pull_request_list)
            else:
                non_interesting_providers[package_id] = ProviderPRInfo(provider_details, pull_request_list)
        context = {
            'interesting_providers': interesting_providers,
            'date': datetime.now(),
            'suffix': suffix,
            'non_interesting_providers': non_interesting_providers,
        }
        issue_content = render_template(template_name="PROVIDER_ISSUE", context=context, extension=".md")
        console.print()
        console.print(
            "[green]Below you can find the issue content that you can use "
            "to ask contributor to test providers![/]"
        )
        console.print()
        console.print()
        console.print(
            "Issue title: [yellow]Status of testing Providers that were "
            f"prepared on { datetime.now().strftime('%B %d, %Y') }[/]"
        )
        console.print()
        syntax = Syntax(issue_content, "markdown", theme="ansi_dark")
        console.print(syntax)
        console.print()
        users: Set[str] = set()
        for provider_info in interesting_providers.values():
            for pr in provider_info.pr_list:
                users.add("@" + pr.user.login)
        console.print("All users involved in the PRs:")
        console.print(" ".join(users))


if __name__ == "__main__":
    # The cli exit code is:
    #   * 0 in case of success
    #   * 1 in case of error
    #   * 64 in case of skipped package
    #   * 65 in case user decided to quit
    #   * 66 in case package has doc-only changes
    cli()
