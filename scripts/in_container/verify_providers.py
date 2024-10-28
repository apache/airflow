#!/usr/bin/env python3
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
from __future__ import annotations

import importlib
import json
import logging
import os
import pkgutil
import re
import subprocess
import sys
import traceback
import warnings
from enum import Enum
from inspect import isclass
from pathlib import Path
from typing import NamedTuple

from rich.console import Console

from airflow.exceptions import AirflowOptionalProviderFeatureException
from airflow.secrets import BaseSecretsBackend

console = Console(width=400, color_system="standard")

AIRFLOW_SOURCES_ROOT = Path(__file__).parents[2].resolve()
PROVIDERS_PATH = AIRFLOW_SOURCES_ROOT / "airflow" / "providers"
GENERATED_PROVIDERS_DEPENDENCIES_FILE = (
    AIRFLOW_SOURCES_ROOT / "generated" / "provider_dependencies.json"
)
ALL_DEPENDENCIES = json.loads(GENERATED_PROVIDERS_DEPENDENCIES_FILE.read_text())

USE_AIRFLOW_VERSION = os.environ.get("USE_AIRFLOW_VERSION") or ""
IS_AIRFLOW_VERSION_PROVIDED = re.match(r"^(\d+)\.(\d+)\.(\d+)\S*$", USE_AIRFLOW_VERSION)


class EntityType(Enum):
    Operators = "Operators"
    Transfers = "Transfers"
    Sensors = "Sensors"
    Hooks = "Hooks"
    Secrets = "Secrets"
    Trigger = "Trigger"
    Notification = "Notification"


class EntityTypeSummary(NamedTuple):
    entities: list[str]
    new_entities_table: str
    wrong_entities: list[tuple[type, str]]


class VerifiedEntities(NamedTuple):
    all_entities: set[str]
    wrong_entities: list[tuple[type, str]]


class ProviderPackageDetails(NamedTuple):
    provider_package_id: str
    full_package_name: str
    pypi_package_name: str
    source_provider_package_path: str
    documentation_provider_package_path: str
    provider_description: str
    versions: list[str]
    excluded_python_versions: list[str]


ENTITY_NAMES = {
    EntityType.Operators: "Operators",
    EntityType.Transfers: "Transfer Operators",
    EntityType.Sensors: "Sensors",
    EntityType.Hooks: "Hooks",
    EntityType.Secrets: "Secrets",
    EntityType.Trigger: "Trigger",
    EntityType.Notification: "Notification",
}

TOTALS: dict[EntityType, int] = {
    EntityType.Operators: 0,
    EntityType.Hooks: 0,
    EntityType.Sensors: 0,
    EntityType.Transfers: 0,
    EntityType.Secrets: 0,
    EntityType.Trigger: 0,
    EntityType.Notification: 0,
}

OPERATORS_PATTERN = r".*Operator$"
SENSORS_PATTERN = r".*Sensor$"
HOOKS_PATTERN = r".*Hook$"
SECRETS_PATTERN = r".*Backend$"
TRANSFERS_PATTERN = r".*To[A-Z0-9].*Operator$"
WRONG_TRANSFERS_PATTERN = r".*Transfer$|.*TransferOperator$"
TRIGGER_PATTERN = r".*Trigger$"
NOTIFICATION_PATTERN = r".*Notifier|.*send_.*_notification$"

ALL_PATTERNS = {
    OPERATORS_PATTERN,
    SENSORS_PATTERN,
    HOOKS_PATTERN,
    SECRETS_PATTERN,
    TRANSFERS_PATTERN,
    WRONG_TRANSFERS_PATTERN,
    TRIGGER_PATTERN,
    NOTIFICATION_PATTERN,
}

EXPECTED_SUFFIXES: dict[EntityType, str] = {
    EntityType.Operators: "Operator",
    EntityType.Hooks: "Hook",
    EntityType.Sensors: "Sensor",
    EntityType.Secrets: "Backend",
    EntityType.Transfers: "Operator",
    EntityType.Trigger: "Trigger",
    EntityType.Notification: "Notifier",
}


def get_all_providers() -> list[str]:
    return list(ALL_DEPENDENCIES.keys())


def import_all_classes(
    walkable_paths_and_prefixes: dict[str, str],
    prefix: str,
    provider_ids: list[str] | None = None,
    print_imports: bool = False,
    print_skips: bool = False,
) -> tuple[list[str], list[str]]:
    """Imports all classes in providers packages.

    This method loads and imports all the classes found in providers, so that we
    can find all the subclasses of operators/sensors etc.

    :param walkable_paths_and_prefixes: dict of paths with accompanying prefixes
        to look the provider packages in
    :param prefix: prefix to add
    :param provider_ids - provider ids that should be loaded.
    :param print_imports - if imported class should also be printed in output
    :param print_skips - if skipped classes should also be printed in output
    :return: tuple of list of all imported classes and
    """
    console.print()
    console.print(f"Walking all package with prefixes in {walkable_paths_and_prefixes}")
    console.print()
    imported_classes = []
    classes_with_potential_circular_import = []
    tracebacks: list[tuple[str, str]] = []
    printed_packages: set[str] = set()

    def mk_prefix(provider_id):
        return f"{prefix}{provider_id}"

    if provider_ids:
        provider_prefixes = tuple(mk_prefix(provider_id) for provider_id in provider_ids)
    else:
        provider_prefixes = (prefix,)

    def onerror(_):
        nonlocal tracebacks
        exception_string = traceback.format_exc()
        for provider_prefix in provider_prefixes:
            if provider_prefix in exception_string:
                start_index = exception_string.find(provider_prefix)
                end_index = exception_string.find(
                    "\n", start_index + len(provider_prefix)
                )
                package = exception_string[start_index:end_index]
                tracebacks.append((package, exception_string))
                break

    for path, prefix in walkable_paths_and_prefixes.items():
        for modinfo in pkgutil.walk_packages(path=[path], prefix=prefix, onerror=onerror):
            if not modinfo.name.startswith(provider_prefixes):
                if print_skips:
                    console.print(f"Skipping module: {modinfo.name}")
                continue
            if print_imports:
                package_to_print = modinfo.name.rpartition(".")[0]
                if package_to_print not in printed_packages:
                    printed_packages.add(package_to_print)
                    console.print(f"Importing package: {package_to_print}")
            try:
                with warnings.catch_warnings(record=True):
                    warnings.filterwarnings("always", category=DeprecationWarning)
                    _module = importlib.import_module(modinfo.name)
                    for attribute_name in dir(_module):
                        class_name = modinfo.name + "." + attribute_name
                        attribute = getattr(_module, attribute_name)
                        if isclass(attribute):
                            imported_classes.append(class_name)
                        if isclass(attribute) and (
                            issubclass(attribute, logging.Handler)
                            or issubclass(attribute, BaseSecretsBackend)
                        ):
                            classes_with_potential_circular_import.append(class_name)
            except AirflowOptionalProviderFeatureException:
                # We ignore optional features
                ...
            except Exception as e:
                # skip the check as we are temporary vendoring in the google ads client with wrong package
                # skip alembic.context which is only available when alembic command is executed from a folder
                # containing the alembic.ini file
                if "No module named 'google.ads.googleads.v12'" not in str(
                    e
                ) and "module 'alembic.context' has no attribute 'config'" not in str(e):
                    exception_str = traceback.format_exc()
                    tracebacks.append((modinfo.name, exception_str))

    if tracebacks:
        if IS_AIRFLOW_VERSION_PROVIDED:
            console.print(
                f"""
[red]ERROR: There were some import errors[/]

[yellow]Detected that this job is about installing providers in {USE_AIRFLOW_VERSION}[/],
[yellow]most likely you are using features that are not available in Airflow {USE_AIRFLOW_VERSION}[/]
[yellow]and you must implement them in backwards-compatible way![/]

""",
            )
        console.print("[red]----------------------------------------[/]")
        for package, trace in tracebacks:
            console.print(f"Exception when importing: {package}\n\n")
            console.print(trace)
            console.print("[red]----------------------------------------[/]")
        sys.exit(1)
    else:
        return imported_classes, classes_with_potential_circular_import


def is_imported_from_same_module(the_class: str, imported_name: str) -> bool:
    """Is the class imported from another module?

    :param the_class: the class object itself
    :param imported_name: name of the imported class
    :return: true if the class was imported from another module
    """
    return imported_name.rpartition(":")[0] == the_class.__module__


def is_example_dag(imported_name: str) -> bool:
    """Is the class an example_dag class?

    :param imported_name: name where the class is imported from
    :return: true if it is an example_dags class
    """
    return ".example_dags." in imported_name


def is_from_the_expected_base_package(the_class: type, expected_package: str) -> bool:
    """Returns true if the class is from the package expected.

    :param the_class: the class object
    :param expected_package: package expected for the class
    """
    return the_class.__module__.startswith(expected_package)


def inherits_from(the_class: type, expected_ancestor: type | None = None) -> bool:
    """Returns true if the class inherits (directly or indirectly) from the class specified.

    :param the_class: The class to check
    :param expected_ancestor: expected class to inherit from
    :return: true is the class inherits from the class expected
    """
    if expected_ancestor is None:
        return False
    import inspect

    mro = inspect.getmro(the_class)
    return the_class is not expected_ancestor and expected_ancestor in mro


def is_class(the_class: type) -> bool:
    """Returns true if the object passed is a class.

    :param the_class: the class to pass
    :return: true if it is a class
    """
    import inspect

    return inspect.isclass(the_class)


def package_name_matches(the_class: type, expected_pattern: str | None = None) -> bool:
    """In case expected_pattern is set, it checks if the package name matches the pattern.

    :param the_class: imported class
    :param expected_pattern: the pattern that should match the package
    :return: true if the expected_pattern is None or the pattern matches the package
    """
    return (
        expected_pattern is None
        or re.match(expected_pattern, the_class.__module__) is not None
    )


def convert_classes_to_table(
    entity_type: EntityType, entities: list[str], full_package_name: str
) -> str:
    """Converts new entities to a Markdown table.

    :param entity_type: entity type to convert to markup
    :param entities: list of  entities
    :param full_package_name: name of the provider package
    :return: table of new classes
    """
    from tabulate import tabulate

    headers = [
        f"New Airflow 2.0 {entity_type.value.lower()}: `{full_package_name}` package"
    ]
    table = [
        (get_class_code_link(full_package_name, class_name, "main"),)
        for class_name in entities
    ]
    return tabulate(table, headers=headers, tablefmt="pipe")


def get_details_about_classes(
    entity_type: EntityType,
    entities: set[str],
    wrong_entities: list[tuple[type, str]],
    full_package_name: str,
) -> EntityTypeSummary:
    """Get details about entities.

    :param entity_type: type of entity (Operators, Hooks etc.)
    :param entities: set of entities found
    :param wrong_entities: wrong entities found for that type
    :param full_package_name: full package name
    """
    all_entities = sorted(entities)
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
    """Strips base package name from the class (if it starts with the package name)."""
    if class_name.startswith(base_package):
        return class_name[len(base_package) + 1 :]
    else:
        return class_name


def convert_class_name_to_url(base_url: str, class_name) -> str:
    """Converts the class name to URL that the class can be reached.

    :param base_url: base URL to use
    :param class_name: name of the class
    :return: URL to the class
    """
    return base_url + class_name.rpartition(".")[0].replace(".", "/") + ".py"


def get_class_code_link(base_package: str, class_name: str, git_tag: str) -> str:
    """Provides a Markdown link for the class passed as parameter.

    :param base_package: base package to strip from most names
    :param class_name: name of the class
    :param git_tag: tag to use for the URL link
    :return: URL to the class
    """
    url_prefix = f"https://github.com/apache/airflow/blob/{git_tag}/"
    return (
        f"[{strip_package_from_class(base_package, class_name)}]"
        f"({convert_class_name_to_url(url_prefix, class_name)})"
    )


def print_wrong_naming(entity_type: EntityType, wrong_classes: list[tuple[type, str]]):
    """Prints wrong entities of a given entity type if there are any.

    :param entity_type: type of the class to print
    :param wrong_classes: list of wrong entities
    """
    if wrong_classes:
        console.print(
            f"\n[red]There are wrongly named entities of type {entity_type}:[/]\n"
        )
        for wrong_entity_type, message in wrong_classes:
            console.print(f"{wrong_entity_type}: {message}")


def find_all_entities(
    imported_classes: list[str],
    base_package: str,
    ancestor_match: type,
    sub_package_pattern_match: str,
    expected_class_name_pattern: str,
    unexpected_class_name_patterns: set[str],
    exclude_class_type: type | None = None,
    false_positive_class_names: set[str] | None = None,
) -> VerifiedEntities:
    """Returns set of entities containing all subclasses in package specified.

    :param imported_classes: entities imported from providers
    :param base_package: base package name where to start looking for the entities
    :param sub_package_pattern_match: this string is expected to appear in the sub-package name
    :param ancestor_match: type of the object the method looks for
    :param expected_class_name_pattern: regexp of class name pattern to expect
    :param unexpected_class_name_patterns: set of regexp of class name pattern that are not expected
    :param exclude_class_type: exclude class of this type (Sensor are also Operators, so
           they should be excluded from the list)
    :param false_positive_class_names: set of class names that are wrongly recognised as badly named
    """
    found_entities: set[str] = set()
    wrong_entities: list[tuple[type, str]] = []
    for imported_name in imported_classes:
        module, class_name = imported_name.rsplit(".", maxsplit=1)
        the_class = getattr(importlib.import_module(module), class_name)
        if (
            is_class(the_class=the_class)
            and not is_example_dag(imported_name=imported_name)
            and is_from_the_expected_base_package(
                the_class=the_class, expected_package=base_package
            )
            and is_imported_from_same_module(
                the_class=the_class, imported_name=imported_name
            )
            and inherits_from(the_class=the_class, expected_ancestor=ancestor_match)
            and not inherits_from(
                the_class=the_class, expected_ancestor=exclude_class_type
            )
            and package_name_matches(
                the_class=the_class, expected_pattern=sub_package_pattern_match
            )
        ):
            if (
                not false_positive_class_names
                or class_name not in false_positive_class_names
            ):
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
            found_entities.add(imported_name)
    return VerifiedEntities(all_entities=found_entities, wrong_entities=wrong_entities)


def get_package_class_summary(
    full_package_name: str, imported_classes: list[str]
) -> dict[EntityType, EntityTypeSummary]:
    """Gets summary of the package in the form of dictionary containing all types of entities.

    :param full_package_name: full package name
    :param imported_classes: entities imported_from providers
    :return: dictionary of objects usable as context for JINJA2 templates, or
        None if there are some errors
    """
    from airflow.hooks.base import BaseHook
    from airflow.models.baseoperator import BaseOperator
    from airflow.secrets import BaseSecretsBackend
    from airflow.sensors.base import BaseSensorOperator
    from airflow.triggers.base import BaseTrigger

    # Remove this conditional check after providers are 2.6+ compatible
    try:
        from airflow.notifications.basenotifier import BaseNotifier

        has_notifier = True
    except ImportError:
        has_notifier = False

    all_verified_entities: dict[EntityType, VerifiedEntities] = {
        EntityType.Operators: find_all_entities(
            imported_classes=imported_classes,
            base_package=full_package_name,
            sub_package_pattern_match=r".*\.operators\..*",
            ancestor_match=BaseOperator,
            expected_class_name_pattern=OPERATORS_PATTERN,
            unexpected_class_name_patterns=ALL_PATTERNS - {OPERATORS_PATTERN},
            exclude_class_type=BaseSensorOperator,
            false_positive_class_names={
                "ProduceToTopicOperator",
                "CloudVisionAddProductToProductSetOperator",
                "CloudDataTransferServiceGCSToGCSOperator",
                "CloudDataTransferServiceS3ToGCSOperator",
                "BigQueryCreateDataTransferOperator",
                "CloudTextToSpeechSynthesizeOperator",
                "CloudSpeechToTextRecognizeSpeechOperator",
            },
        ),
        EntityType.Sensors: find_all_entities(
            imported_classes=imported_classes,
            base_package=full_package_name,
            sub_package_pattern_match=r".*\.sensors\..*",
            ancestor_match=BaseSensorOperator,
            expected_class_name_pattern=SENSORS_PATTERN,
            unexpected_class_name_patterns=ALL_PATTERNS
            - {OPERATORS_PATTERN, SENSORS_PATTERN},
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
            unexpected_class_name_patterns=ALL_PATTERNS
            - {OPERATORS_PATTERN, TRANSFERS_PATTERN},
        ),
        EntityType.Trigger: find_all_entities(
            imported_classes=imported_classes,
            base_package=full_package_name,
            sub_package_pattern_match=r".*\.triggers\..*",
            ancestor_match=BaseTrigger,
            expected_class_name_pattern=TRIGGER_PATTERN,
            unexpected_class_name_patterns=ALL_PATTERNS - {TRIGGER_PATTERN},
        ),
    }
    if has_notifier:
        all_verified_entities[EntityType.Notification] = find_all_entities(
            imported_classes=imported_classes,
            base_package=full_package_name,
            sub_package_pattern_match=r".*\.notifications\..*",
            ancestor_match=BaseNotifier,
            expected_class_name_pattern=NOTIFICATION_PATTERN,
            unexpected_class_name_patterns=ALL_PATTERNS - {NOTIFICATION_PATTERN},
        )
    else:
        all_verified_entities[EntityType.Notification] = VerifiedEntities(
            all_entities=set(), wrong_entities=[]
        )

    for entity in EntityType:
        print_wrong_naming(entity, all_verified_entities[entity].wrong_entities)

    entities_summary: dict[EntityType, EntityTypeSummary] = {}

    for entity_type in EntityType:
        entities_summary[entity_type] = get_details_about_classes(
            entity_type,
            all_verified_entities[entity_type].all_entities,
            all_verified_entities[entity_type].wrong_entities,
            full_package_name,
        )

    return entities_summary


def is_camel_case_with_acronyms(s: str):
    """Checks if the string passed is Camel Case (with capitalised acronyms allowed).

    :param s: string to check
    :return: true if the name looks cool as Class name.
    """
    if s and s[0] == "_":  # Leading underscores are fine.
        s = s[1:]
    if not s:
        return True
    return s[0].isupper() and not (s.islower() or s.isupper() or "_" in s)


def check_if_classes_are_properly_named(
    entity_summary: dict[EntityType, EntityTypeSummary],
) -> tuple[int, int]:
    """Check if all entities in the dictionary are named properly.

    It prints names at the output and returns the status of class names.

    :param entity_summary: dictionary of class names to check, grouped by types.
    :return: Tuple of 2 ints = total number of entities and number of badly named entities
    """
    total_class_number = 0
    badly_named_class_number = 0
    for entity_type, class_suffix in EXPECTED_SUFFIXES.items():
        for class_full_name in entity_summary[entity_type].entities:
            _, class_name = class_full_name.rsplit(".", maxsplit=1)
            error_encountered = False
            if (
                class_name.startswith("send_")
                and class_name.endswith("_notification")
                and entity_type == EntityType.Notification
            ):
                continue
            if not is_camel_case_with_acronyms(class_name):
                console.print(
                    f"[red]The class {class_full_name} is wrongly named. The "
                    f"class name should be CamelCaseWithACRONYMS optionally "
                    f"with a single leading underscore[/]"
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


def verify_provider_classes_for_single_provider(
    imported_classes: list[str], provider_package_id: str
):
    """Verify naming of provider classes for single provider."""
    full_package_name = f"airflow.providers.{provider_package_id}"
    entity_summaries = get_package_class_summary(full_package_name, imported_classes)
    total, bad = check_if_classes_are_properly_named(entity_summaries)
    bad += sum(
        len(entity_summary.wrong_entities) for entity_summary in entity_summaries.values()
    )
    if bad != 0:
        console.print()
        console.print(
            f"[red]There are {bad} errors of {total} entities for {provider_package_id}[/]"
        )
        console.print()
    return total, bad


def summarise_total_vs_bad(total: int, bad: int) -> bool:
    """Summarises Bad/Good class names for providers"""
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
        if os.environ.get("CI") != "":
            console.print("::endgroup::")
        console.print(
            f"[red]ERROR! There are in total: {bad} entities badly named out of {total} entities[/]"
        )
        console.print()
        console.print("[red]Please fix the problems listed above [/]")
        return False
    return True


def get_providers_paths() -> list[str]:
    import airflow.providers

    # handle providers in sources
    paths = [str(PROVIDERS_PATH)]
    # as well as those installed via packages
    paths.extend(airflow.providers.__path__)  # type: ignore[attr-defined]
    return paths


def add_all_namespaced_packages(
    walkable_paths_and_prefixes: dict[str, str], provider_path: str, provider_prefix: str
):
    """Find namespace packages.

    This needs to be done manually as ``walk_packages`` does not support
    namespaced packages and PEP 420.

    :param walkable_paths_and_prefixes: pats
    :param provider_path:
    :param provider_prefix:
    """
    main_path = Path(provider_path).resolve()
    for candidate_path in main_path.rglob("*"):
        if candidate_path.name == "__pycache__":
            continue
        if candidate_path.is_dir() and not (candidate_path / "__init__.py").exists():
            subpackage = str(candidate_path.relative_to(main_path)).replace(os.sep, ".")
            walkable_paths_and_prefixes[str(candidate_path)] = (
                provider_prefix + subpackage + "."
            )


def verify_provider_classes() -> tuple[list[str], list[str]]:
    """Verifies all provider classes.

    :return: Tuple: list of all classes and list of all classes that have potential recursion side effects
    """
    provider_ids = get_all_providers()
    walkable_paths_and_prefixes: dict[str, str] = {}
    provider_prefix = "airflow.providers."
    for provider_path in get_providers_paths():
        walkable_paths_and_prefixes[provider_path] = provider_prefix
        add_all_namespaced_packages(
            walkable_paths_and_prefixes, provider_path, provider_prefix
        )
    imported_classes, classes_with_potential_circular_import = import_all_classes(
        walkable_paths_and_prefixes=walkable_paths_and_prefixes,
        provider_ids=provider_ids,
        print_imports=True,
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
    if not summarise_total_vs_bad(total, bad):
        sys.exit(1)

    if not imported_classes:
        console.print("[red]Something is seriously wrong - no classes imported[/]")
        sys.exit(1)
    console.print()
    console.print("[green]SUCCESS: All provider packages are importable![/]\n")
    console.print(f"Imported {len(imported_classes)} classes.")
    console.print()
    return imported_classes, classes_with_potential_circular_import


def run_provider_discovery():
    console.print("[bright_blue]List all providers[/]\n")
    subprocess.run(["airflow", "providers", "list"], check=True)
    console.print("[bright_blue]List all hooks[/]\n")
    subprocess.run(["airflow", "providers", "hooks"], check=True)
    console.print("[bright_blue]List all behaviours[/]\n")
    subprocess.run(["airflow", "providers", "behaviours"], check=True)
    console.print("[bright_blue]List all widgets[/]\n")
    subprocess.run(["airflow", "providers", "widgets"], check=True)
    console.print("[bright_blue]List all extra links[/]\n")
    subprocess.run(["airflow", "providers", "links"], check=True)
    console.print("[bright_blue]List all logging[/]\n")
    subprocess.run(["airflow", "providers", "logging"], check=True)
    console.print("[bright_blue]List all secrets[/]\n")
    subprocess.run(["airflow", "providers", "secrets"], check=True)
    console.print("[bright_blue]List all auth backends[/]\n")
    subprocess.run(["airflow", "providers", "auth"], check=True)
    console.print("[bright_blue]List all triggers[/]\n")
    subprocess.run(["airflow", "providers", "triggers"], check=True)
    console.print("[bright_blue]List all executors[/]\n")
    subprocess.run(["airflow", "providers", "executors"], check=True)


AIRFLOW_LOCAL_SETTINGS_PATH = Path("/opt/airflow") / "airflow_local_settings.py"


if __name__ == "__main__":
    sys.path.insert(0, str(AIRFLOW_SOURCES_ROOT))
    all_imported_classes, all_classes_with_potential_for_circular_import = (
        verify_provider_classes()
    )
    try:
        AIRFLOW_LOCAL_SETTINGS_PATH.write_text(
            "\n".join(
                [
                    "from {} import {}".format(*class_name.rsplit(".", 1))
                    for class_name in all_classes_with_potential_for_circular_import
                ]
            )
        )
        console.print(
            "[bright_blue]Importing all provider classes with potential for circular imports"
            " via airflow_local_settings.py:\n\n"
        )
        console.print(AIRFLOW_LOCAL_SETTINGS_PATH.read_text())
        console.print("\n")
        proc = subprocess.run([sys.executable, "-c", "import airflow"], check=False)
        if proc.returncode != 0:
            console.print(
                "[red] Importing all provider classes with potential for recursion  "
                "via airflow_local_settings.py failed!\n\n"
            )
            console.print(
                "\n[bright_blue]If you see AttributeError or ImportError, it might mean that there is "
                "a circular import from a provider that should be solved\n"
            )
            console.print(
                "\nThe reason for the circular imports might be that if Airflow is configured "
                "to use some of the provider's logging/secret backends in settings\n"
                "the extensions might attempt to import airflow configuration, "
                "version or settings packages.\n"
                "Accessing those packages will trigger attribute/import errors, because "
                "they are not fully imported at this time.\n"
            )
            console.print(
                "\n[info]Look at the stack trace above and see where `airflow` core classes have failed to be"
                "imported from and fix it so that the class does not do it.\n"
            )
            sys.exit(proc.returncode)
    finally:
        AIRFLOW_LOCAL_SETTINGS_PATH.unlink()
    run_provider_discovery()
