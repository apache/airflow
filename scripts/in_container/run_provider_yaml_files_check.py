#!/usr/bin/env python
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

import functools
import importlib
import inspect
import itertools
import json
import os
import pathlib
import platform
import sys
import textwrap
import warnings
from collections import Counter
from enum import Enum
from typing import Any, Callable, Iterable

import jsonschema
import yaml
from jsonpath_ng.ext import parse
from rich.console import Console
from tabulate import tabulate

from airflow.cli.commands.info_command import Architecture
from airflow.exceptions import AirflowProviderDeprecationWarning
from airflow.providers_manager import ProvidersManager

# Those are deprecated modules that contain removed Hooks/Sensors/Operators that we left in the code
# so that users can get a very specific error message when they try to use them.

DEPRECATED_MODULES = [
    "airflow.providers.apache.hdfs.sensors.hdfs",
    "airflow.providers.apache.hdfs.hooks.hdfs",
    "airflow.providers.cncf.kubernetes.triggers.kubernetes_pod",
    "airflow.providers.cncf.kubernetes.operators.kubernetes_pod",
]

KNOWN_DEPRECATED_CLASSES = [
    "airflow.providers.google.cloud.links.dataproc.DataprocLink",
]

try:
    from yaml import CSafeLoader as SafeLoader
except ImportError:
    from yaml import SafeLoader  # type: ignore

if __name__ != "__main__":
    raise Exception(
        "This file is intended to be executed as an executable program. You cannot use it as a module."
    )

ROOT_DIR = pathlib.Path(__file__).resolve().parents[2]
DOCS_DIR = ROOT_DIR.joinpath("docs")
PROVIDER_DATA_SCHEMA_PATH = ROOT_DIR.joinpath("airflow", "provider.yaml.schema.json")
PROVIDER_ISSUE_TEMPLATE_PATH = ROOT_DIR.joinpath(
    ".github", "ISSUE_TEMPLATE", "airflow_providers_bug_report.yml"
)
CORE_INTEGRATIONS = ["SQL", "Local"]

errors: list[str] = []

console = Console(width=400, color_system="standard")
# you need to enable warnings for all deprecations - needed by importlib library to show deprecations
if os.environ.get("PYTHONWARNINGS") != "default":
    console.print(
        "[red]Error: PYTHONWARNINGS not set[/]\n"
        "You must set `PYTHONWARNINGS=default` environment variable to run this script"
    )
    sys.exit(1)

suspended_providers: set[str] = set()
suspended_logos: set[str] = set()
suspended_integrations: set[str] = set()


def _filepath_to_module(filepath: pathlib.Path) -> str:
    p = filepath.resolve().relative_to(ROOT_DIR).as_posix()
    if p.endswith(".py"):
        p = p[:-3]
    return p.replace("/", ".")


def _load_schema() -> dict[str, Any]:
    with PROVIDER_DATA_SCHEMA_PATH.open() as schema_file:
        content = json.load(schema_file)
    return content


def _load_package_data(package_paths: Iterable[str]):
    schema = _load_schema()
    result = {}
    for provider_yaml_path in package_paths:
        with open(provider_yaml_path) as yaml_file:
            provider = yaml.load(yaml_file, SafeLoader)
        rel_path = pathlib.Path(provider_yaml_path).relative_to(ROOT_DIR).as_posix()
        try:
            jsonschema.validate(provider, schema=schema)
        except jsonschema.ValidationError:
            raise Exception(f"Unable to parse: {rel_path}.")
        if not provider.get("suspended"):
            result[rel_path] = provider
        else:
            suspended_providers.add(provider["package-name"])
            for integration in provider["integrations"]:
                suspended_integrations.add(integration["integration-name"])
                if "logo" in integration:
                    suspended_logos.add(integration["logo"])
    return result


def get_all_integration_names(yaml_files) -> list[str]:
    all_integrations = [
        i["integration-name"] for f in yaml_files.values() if "integrations" in f for i in f["integrations"]
    ]
    all_integrations += ["Local"]
    return all_integrations


def assert_sets_equal(
    set1: set[str],
    set_name_1: str,
    set2: set[str],
    set_name_2: str,
    allow_extra_in_set2=False,
    extra_message: str = "",
):
    try:
        difference1 = set1.difference(set2)
    except TypeError as e:
        raise AssertionError(f"invalid type when attempting set difference: {e}")
    except AttributeError as e:
        raise AssertionError(f"first argument does not support set difference: {e}")

    try:
        difference2 = set2.difference(set1)
    except TypeError as e:
        raise AssertionError(f"invalid type when attempting set difference: {e}")
    except AttributeError as e:
        raise AssertionError(f"second argument does not support set difference: {e}")

    if difference1 or (difference2 and not allow_extra_in_set2):
        lines = []
        lines.append(f" Left set:{set_name_1}")
        lines.append(f" Right set:{set_name_2}")
        if difference1:
            lines.append("    Items in the left set but not the right:")
            for item in sorted(difference1):
                lines.append(f"       {item!r}")
        if difference2 and not allow_extra_in_set2:
            lines.append("    Items in the right set but not the left:")
            for item in sorted(difference2):
                lines.append(f"       {item!r}")

        standard_msg = "\n".join(lines)
        if extra_message:
            standard_msg += f"\n{extra_message}"
        raise AssertionError(standard_msg)


class ObjectType(Enum):
    MODULE = "module"
    CLASS = "class"


def check_if_object_exist(
    object_name: str, resource_type: str, yaml_file_path: str, object_type: ObjectType
) -> int:
    num_errors = 0
    try:
        if object_type == ObjectType.CLASS:
            module_name, class_name = object_name.rsplit(".", maxsplit=1)
            with warnings.catch_warnings(record=True) as w:
                the_class = getattr(importlib.import_module(module_name), class_name)
            for warn in w:
                if warn.category == AirflowProviderDeprecationWarning:
                    if object_name in KNOWN_DEPRECATED_CLASSES:
                        console.print(
                            f"[yellow]The {object_name} class is deprecated and we know about it. "
                            f"It should be removed in the future."
                        )
                        continue
                    errors.append(
                        f"The `{class_name}` class in {resource_type} list in {yaml_file_path} "
                        f"is deprecated with this message: '{warn.message}'.\n"
                        f"[yellow]How to fix it[/]: Please remove it from provider.yaml and replace with "
                        f"the new class."
                    )
                    num_errors += 1
            if the_class and inspect.isclass(the_class):
                return num_errors
        elif object_type == ObjectType.MODULE:
            with warnings.catch_warnings(record=True) as w:
                module = importlib.import_module(object_name)
            for warn in w:
                if warn.category == AirflowProviderDeprecationWarning:
                    errors.append(
                        f"The `{object_name}` module in {resource_type} list in {yaml_file_path} "
                        f"is deprecated with this message: '{warn.message}'.\n"
                        f"[yellow]How to fix it[/]: Please remove it from provider.yaml and replace it "
                        f"with the new module. If you see warnings in classes - fix the classes so that "
                        f"they are not raising Deprecation Warnings when module is imported."
                    )
                    num_errors += 1
            if inspect.ismodule(module):
                return num_errors
        else:
            raise RuntimeError(f"Wrong enum {object_type}???")
    except Exception as e:
        errors.append(
            f"The `{object_name}` object in {resource_type} list in {yaml_file_path} does not exist "
            f"or is not a {object_type.value}: {e}"
        )
        num_errors += 1
    else:
        errors.append(
            f"The `{object_name}` object in {resource_type} list in {yaml_file_path} does not exist "
            f"or is not a {object_type.value}."
        )
        num_errors += 1
    return num_errors


def check_if_objects_exist_and_belong_to_package(
    object_names: set[str],
    provider_package: str,
    yaml_file_path: str,
    resource_type: str,
    object_type: ObjectType,
) -> int:
    num_errors = 0
    for object_name in object_names:
        if os.environ.get("VERBOSE"):
            console.print(
                f"[bright_blue]Checking if {object_name} of {resource_type} "
                f"in {yaml_file_path} is {object_type.value} and belongs to {provider_package} package"
            )
        if not object_name.startswith(provider_package):
            errors.append(
                f"The `{object_name}` object in {resource_type} list in {yaml_file_path} does not start"
                f" with the expected {provider_package}."
            )
            num_errors += 1
        num_errors += check_if_object_exist(object_name, resource_type, yaml_file_path, object_type)
    return num_errors


def parse_module_data(provider_data, resource_type, yaml_file_path):
    package_dir = ROOT_DIR.joinpath(yaml_file_path).parent
    provider_package = pathlib.Path(yaml_file_path).parent.as_posix().replace("/", ".")
    py_files = itertools.chain(
        package_dir.glob(f"**/{resource_type}/*.py"),
        package_dir.glob(f"{resource_type}/*.py"),
        package_dir.glob(f"**/{resource_type}/**/*.py"),
        package_dir.glob(f"{resource_type}/**/*.py"),
    )
    expected_modules = {_filepath_to_module(f) for f in py_files if f.name != "__init__.py"}
    resource_data = provider_data.get(resource_type, [])
    return expected_modules, provider_package, resource_data


def run_check(title: str):
    def inner(func: Callable[..., tuple[int, int]]):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            console.print(f"\n[magenta]Starting check:[/] {title}\n")
            check_num, error_num = func(*args, **kwargs)
            console.print(
                f"\n[magenta]Finished check:[/] Found {error_num} "
                f"error{'' if error_num == 1 else 's'} in {check_num} "
                f"checked items\n"
            )
            return check_num, error_num

        return wrapper

    return inner


@run_check("Checking integration duplicates")
def check_integration_duplicates(yaml_files: dict[str, dict]) -> tuple[int, int]:
    """Integration names must be globally unique."""
    num_errors = 0
    all_integrations = get_all_integration_names(yaml_files)
    num_integrations = len(all_integrations)
    duplicates = [(k, v) for (k, v) in Counter(all_integrations).items() if v > 1]

    if duplicates:
        console.print(
            "Duplicate integration names found. Integration names must be globally unique. "
            "Please delete duplicates."
        )
        errors.append(tabulate(duplicates, headers=["Integration name", "Number of occurrences"]))
        num_errors += 1
    return num_integrations, num_errors


@run_check("Checking completeness of list of {sensors, hooks, operators, triggers}")
def check_correctness_of_list_of_sensors_operators_hook_trigger_modules(
    yaml_files: dict[str, dict]
) -> tuple[int, int]:
    num_errors = 0
    num_modules = 0
    for (yaml_file_path, provider_data), resource_type in itertools.product(
        yaml_files.items(), ["sensors", "operators", "hooks", "triggers"]
    ):
        expected_modules, provider_package, resource_data = parse_module_data(
            provider_data, resource_type, yaml_file_path
        )
        expected_modules = {module for module in expected_modules if module not in DEPRECATED_MODULES}
        current_modules = {str(i) for r in resource_data for i in r.get("python-modules", [])}
        num_modules += len(current_modules)
        num_errors += check_if_objects_exist_and_belong_to_package(
            current_modules, provider_package, yaml_file_path, resource_type, ObjectType.MODULE
        )
        try:
            package_name = os.fspath(ROOT_DIR.joinpath(yaml_file_path).parent.relative_to(ROOT_DIR)).replace(
                "/", "."
            )
            assert_sets_equal(
                set(expected_modules),
                f"Found list of {resource_type} modules in provider package: {package_name}",
                set(current_modules),
                f"Currently configured list of {resource_type} modules in {yaml_file_path}",
                extra_message="[yellow]Additional check[/]: If there are deprecated modules in the list,"
                "please add them to DEPRECATED_MODULES in "
                f"{pathlib.Path(__file__).relative_to(ROOT_DIR)}",
            )
        except AssertionError as ex:
            nested_error = textwrap.indent(str(ex), "  ")
            errors.append(
                f"Incorrect content of key '{resource_type}/python-modules' "
                f"in file: {yaml_file_path}\n{nested_error}"
            )
            num_errors += 1
    return num_modules, num_errors


@run_check("Checking for duplicates in list of {sensors, hooks, operators, triggers}")
def check_duplicates_in_integrations_names_of_hooks_sensors_operators(
    yaml_files: dict[str, dict]
) -> tuple[int, int]:
    num_errors = 0
    num_integrations = 0
    for (yaml_file_path, provider_data), resource_type in itertools.product(
        yaml_files.items(), ["sensors", "operators", "hooks", "triggers"]
    ):
        resource_data = provider_data.get(resource_type, [])
        count_integrations = Counter(r.get("integration-name", "") for r in resource_data)
        num_integrations += len(count_integrations)
        for integration, count in count_integrations.items():
            if count > 1:
                errors.append(
                    f"Duplicated content of '{resource_type}/integration-name/{integration}' "
                    f"in file: {yaml_file_path}"
                )
                num_errors += 1
    return num_integrations, num_errors


@run_check("Checking completeness of list of transfers")
def check_completeness_of_list_of_transfers(yaml_files: dict[str, dict]) -> tuple[int, int]:
    resource_type = "transfers"
    num_errors = 0
    num_transfers = 0
    for yaml_file_path, provider_data in yaml_files.items():
        expected_modules, provider_package, resource_data = parse_module_data(
            provider_data, resource_type, yaml_file_path
        )
        expected_modules = {module for module in expected_modules if module not in DEPRECATED_MODULES}
        current_modules = {r.get("python-module") for r in resource_data}
        num_transfers += len(current_modules)
        num_errors += check_if_objects_exist_and_belong_to_package(
            current_modules, provider_package, yaml_file_path, resource_type, ObjectType.MODULE
        )
        try:
            package_name = os.fspath(ROOT_DIR.joinpath(yaml_file_path).parent.relative_to(ROOT_DIR)).replace(
                "/", "."
            )
            assert_sets_equal(
                set(expected_modules),
                f"Found list of transfer modules in provider package: {package_name}",
                set(current_modules),
                f"Currently configured list of transfer modules in {yaml_file_path}",
            )
        except AssertionError as ex:
            nested_error = textwrap.indent(str(ex), "  ")
            errors.append(
                f"Incorrect content of key '{resource_type}/python-module' "
                f"in file: {yaml_file_path}\n{nested_error}"
            )
            num_errors += 1
    return num_transfers, num_errors


@run_check("Checking if hook classes specified by hook-class-name in connection type are importable")
def check_hook_class_name_entries_in_connection_types(yaml_files: dict[str, dict]) -> tuple[int, int]:
    resource_type = "connection-types"
    num_errors = 0
    num_connection_types = 0
    for yaml_file_path, provider_data in yaml_files.items():
        provider_package = pathlib.Path(yaml_file_path).parent.as_posix().replace("/", ".")
        connection_types = provider_data.get(resource_type)
        if connection_types:
            num_connection_types += len(connection_types)
            hook_class_names = {connection_type["hook-class-name"] for connection_type in connection_types}
            num_errors += check_if_objects_exist_and_belong_to_package(
                hook_class_names, provider_package, yaml_file_path, resource_type, ObjectType.CLASS
            )
    return num_connection_types, num_errors


@run_check("Checking plugin classes belong to package are importable and belong to package")
def check_plugin_classes(yaml_files: dict[str, dict]) -> tuple[int, int]:
    resource_type = "plugins"
    num_errors = 0
    num_plugins = 0
    for yaml_file_path, provider_data in yaml_files.items():
        provider_package = pathlib.Path(yaml_file_path).parent.as_posix().replace("/", ".")
        plugins = provider_data.get(resource_type)
        if plugins:
            num_plugins += len(plugins)
            num_errors += check_if_objects_exist_and_belong_to_package(
                {plugin["plugin-class"] for plugin in plugins},
                provider_package,
                yaml_file_path,
                resource_type,
                ObjectType.CLASS,
            )
    return num_plugins, num_errors


@run_check("Checking extra-links belong to package, exist and are classes")
def check_extra_link_classes(yaml_files: dict[str, dict]) -> tuple[int, int]:
    resource_type = "extra-links"
    num_errors = 0
    num_extra_links = 0
    for yaml_file_path, provider_data in yaml_files.items():
        provider_package = pathlib.Path(yaml_file_path).parent.as_posix().replace("/", ".")
        extra_links = provider_data.get(resource_type)
        if extra_links:
            num_extra_links += len(extra_links)
            num_errors += check_if_objects_exist_and_belong_to_package(
                extra_links, provider_package, yaml_file_path, resource_type, ObjectType.CLASS
            )
    return num_extra_links, num_errors


@run_check("Checking notifications belong to package, exist and are classes")
def check_notification_classes(yaml_files: dict[str, dict]) -> tuple[int, int]:
    resource_type = "notifications"
    num_errors = 0
    num_notifications = 0
    for yaml_file_path, provider_data in yaml_files.items():
        provider_package = pathlib.Path(yaml_file_path).parent.as_posix().replace("/", ".")
        notifications = provider_data.get(resource_type)
        if notifications:
            num_notifications += len(notifications)
            num_errors += check_if_objects_exist_and_belong_to_package(
                notifications, provider_package, yaml_file_path, resource_type, ObjectType.CLASS
            )
    return num_notifications, num_errors


@run_check("Checking for duplicates in list of transfers")
def check_duplicates_in_list_of_transfers(yaml_files: dict[str, dict]) -> tuple[int, int]:
    resource_type = "transfers"
    num_errors = 0
    num_integrations = 0
    for yaml_file_path, provider_data in yaml_files.items():
        resource_data = provider_data.get(resource_type, [])
        count_integrations = Counter(
            (r.get("source-integration-name", ""), r.get("target-integration-name", ""))
            for r in resource_data
        )
        num_integrations += len(count_integrations)
        for (source, target), count in count_integrations.items():
            if count > 1:
                errors.append(
                    f"Duplicated content of \n"
                    f" '{resource_type}/source-integration-name/{source}' "
                    f" '{resource_type}/target-integration-name/{target}' "
                    f"in file: {yaml_file_path}"
                )
                num_errors += 1
    return num_integrations, num_errors


@run_check("Detect unregistered integrations")
def check_invalid_integration(yaml_files: dict[str, dict]) -> tuple[int, int]:
    all_integration_names = set(get_all_integration_names(yaml_files))
    num_errors = 0
    num_integrations = len(all_integration_names)
    for (yaml_file_path, provider_data), resource_type in itertools.product(
        yaml_files.items(), ["sensors", "operators", "hooks", "triggers"]
    ):
        resource_data = provider_data.get(resource_type, [])
        current_names = {r["integration-name"] for r in resource_data}
        invalid_names = current_names - all_integration_names
        if invalid_names:
            errors.append(
                f"Incorrect content of key '{resource_type}/integration-name' in file: {yaml_file_path}. "
                f"Invalid values: {invalid_names}"
            )
            num_errors += 1

    for (yaml_file_path, provider_data), key in itertools.product(
        yaml_files.items(), ["source-integration-name", "target-integration-name"]
    ):
        resource_data = provider_data.get("transfers", [])
        current_names = {r[key] for r in resource_data}
        invalid_names = current_names - all_integration_names - suspended_integrations
        if invalid_names:
            errors.append(
                f"Incorrect content of key 'transfers/{key}' in file: {yaml_file_path}. "
                f"Invalid values: {invalid_names}"
            )
            num_errors += 1
    return num_integrations, num_errors


@run_check("Checking doc files")
def check_doc_files(yaml_files: dict[str, dict]) -> tuple[int, int]:
    num_docs = 0
    num_errors = 0
    current_doc_urls: list[str] = []
    current_logo_urls: list[str] = []
    for provider in yaml_files.values():
        if "integrations" in provider:
            current_doc_urls.extend(
                guide
                for guides in provider["integrations"]
                if "how-to-guide" in guides
                for guide in guides["how-to-guide"]
            )
            current_logo_urls.extend(
                integration["logo"] for integration in provider["integrations"] if "logo" in integration
            )
        if "transfers" in provider:
            current_doc_urls.extend(
                op["how-to-guide"] for op in provider["transfers"] if "how-to-guide" in op
            )
    if suspended_providers:
        console.print("[yellow]Suspended providers:[/]")
        console.print(suspended_providers)

    expected_doc_files = itertools.chain(
        DOCS_DIR.glob("apache-airflow-providers-*/operators/**/*.rst"),
        DOCS_DIR.glob("apache-airflow-providers-*/transfer/**/*.rst"),
    )

    expected_doc_urls = {
        f"/docs/{f.relative_to(DOCS_DIR).as_posix()}"
        for f in expected_doc_files
        if f.name != "index.rst"
        and "_partials" not in f.parts
        and not f.relative_to(DOCS_DIR).as_posix().startswith(tuple(suspended_providers))
    } | {
        f"/docs/{f.relative_to(DOCS_DIR).as_posix()}"
        for f in DOCS_DIR.glob("apache-airflow-providers-*/operators.rst")
        if not f.relative_to(DOCS_DIR).as_posix().startswith(tuple(suspended_providers))
    }
    if suspended_logos:
        console.print("[yellow]Suspended logos:[/]")
        console.print(suspended_logos)
        console.print()
    expected_logo_urls = {
        f"/{f.relative_to(DOCS_DIR).as_posix()}"
        for f in (DOCS_DIR / "integration-logos").rglob("*")
        if f.is_file() and not f"/{f.relative_to(DOCS_DIR).as_posix()}".startswith(tuple(suspended_logos))
    }

    try:
        console.print("Checking document urls")
        assert_sets_equal(
            set(expected_doc_urls),
            "Document urls found in airflow/docs",
            set(current_doc_urls),
            "Document urls configured in provider.yaml files",
        )
        console.print(f"Checked {len(current_doc_urls)} doc urls")
        console.print()
        console.print("Checking logo urls")
        assert_sets_equal(
            set(expected_logo_urls),
            "Logo urls found in airflow/docs/integration-logos",
            set(current_logo_urls),
            "Logo urls configured in provider.yaml files",
        )
        console.print(f"Checked {len(current_logo_urls)} logo urls")
        console.print()
    except AssertionError as ex:
        nested_error = textwrap.indent(str(ex), "  ")
        errors.append(
            f"Discrepancies between documentation/logos for providers and provider.yaml files "
            f"[yellow]How to fix it[/]: Please synchronize the docs/logs.\n{nested_error}"
        )
        num_errors += 1
    return num_docs, num_errors


@run_check("Checking if provider names are unique")
def check_unique_provider_name(yaml_files: dict[str, dict]) -> tuple[int, int]:
    num_errors = 0
    name_counter = Counter(d["name"] for d in yaml_files.values())
    duplicates = {k for k, v in name_counter.items() if v > 1}
    if duplicates:
        errors.append(f"Provider name must be unique. Duplicates: {duplicates}")
        num_errors += 1
    return len(name_counter.items()), num_errors


@run_check(f"Checking providers are mentioned in {PROVIDER_ISSUE_TEMPLATE_PATH}")
def check_providers_are_mentioned_in_issue_template(yaml_files: dict[str, dict]):
    num_errors = 0
    num_providers = 0
    prefix_len = len("apache-airflow-providers-")
    short_provider_names = [d["package-name"][prefix_len:] for d in yaml_files.values()]
    # exclude deprecated provider that shouldn't be in issue template
    deprecated_providers: list[str] = []
    for item in deprecated_providers:
        short_provider_names.remove(item)
    num_providers += len(short_provider_names)
    jsonpath_expr = parse('$.body[?(@.attributes.label == "Apache Airflow Provider(s)")]..options[*]')
    with PROVIDER_ISSUE_TEMPLATE_PATH.open() as issue_file:
        issue_template = yaml.safe_load(issue_file)
    all_mentioned_providers = [match.value for match in jsonpath_expr.find(issue_template)]
    try:
        # in case of suspended providers, we still want to have them in the issue template
        assert_sets_equal(
            set(short_provider_names),
            "Provider names found in provider.yaml files",
            set(all_mentioned_providers),
            f"Provider names mentioned in {PROVIDER_ISSUE_TEMPLATE_PATH}",
            allow_extra_in_set2=True,
        )
    except AssertionError as ex:
        nested_error = textwrap.indent(str(ex), "  ")
        errors.append(
            f"Discrepancies between providers available in `airflow/providers` and providers "
            f"in {PROVIDER_ISSUE_TEMPLATE_PATH}.\n"
            f"[yellow]How to fix it[/]: Please synchronize the list.\n{nested_error}"
        )
        num_errors += 1
    return num_providers, num_errors


@run_check("Checking providers have all documentation files")
def check_providers_have_all_documentation_files(yaml_files: dict[str, dict]):
    num_errors = 0
    num_providers = 0
    expected_files = ["commits.rst", "index.rst", "installing-providers-from-sources.rst"]
    for package_info in yaml_files.values():
        num_providers += 1
        package_name = package_info["package-name"]
        provider_dir = DOCS_DIR.joinpath(package_name)
        for file in expected_files:
            if not provider_dir.joinpath(file).is_file():
                errors.append(
                    f"The provider {package_name} misses `{file}` in documentation. "
                    f"[yellow]How to fix it[/]: Please add the file to {provider_dir}"
                )
                num_errors += 1
    return num_providers, num_errors


@run_check("Checking remove flag only set for suspended providers")
def check_removed_flag_only_set_for_suspended_providers(yaml_files: dict[str, dict]):
    num_errors = 0
    num_providers = 0
    for package_info in yaml_files.values():
        num_providers += 1
        package_name = package_info["package-name"]
        suspended = package_info["suspended"]
        removed = package_info.get("removed", False)
        if removed and not suspended:
            errors.append(
                f"The provider {package_name} has removed set to True in their provider.yaml file "
                f"but suspended flag is set to false. You should only set removed flag in order to "
                f"prepare last release for a provider that has been previously suspended. "
                f"[yellow]How to fix it[/]: Please suspend the provider first before removing it."
            )
            num_errors += 1
    return num_providers, num_errors


if __name__ == "__main__":
    ProvidersManager().initialize_providers_configuration()
    architecture = Architecture.get_current()
    console.print(f"Verifying packages on {architecture} architecture. Platform: {platform.machine()}.")
    provider_files_pattern = pathlib.Path(ROOT_DIR, "airflow", "providers").rglob("provider.yaml")
    all_provider_files = sorted(str(path) for path in provider_files_pattern)
    if len(sys.argv) > 1:
        paths = [os.fspath(ROOT_DIR / f) for f in sorted(sys.argv[1:])]
    else:
        paths = all_provider_files

    all_parsed_yaml_files: dict[str, dict] = _load_package_data(paths)

    all_files_loaded = len(all_provider_files) == len(paths)
    check_integration_duplicates(all_parsed_yaml_files)
    check_duplicates_in_list_of_transfers(all_parsed_yaml_files)
    check_duplicates_in_integrations_names_of_hooks_sensors_operators(all_parsed_yaml_files)

    check_completeness_of_list_of_transfers(all_parsed_yaml_files)
    check_hook_class_name_entries_in_connection_types(all_parsed_yaml_files)
    check_plugin_classes(all_parsed_yaml_files)
    check_extra_link_classes(all_parsed_yaml_files)
    check_correctness_of_list_of_sensors_operators_hook_trigger_modules(all_parsed_yaml_files)
    check_notification_classes(all_parsed_yaml_files)
    check_unique_provider_name(all_parsed_yaml_files)
    check_providers_have_all_documentation_files(all_parsed_yaml_files)
    check_removed_flag_only_set_for_suspended_providers(all_parsed_yaml_files)

    if all_files_loaded:
        # Only check those if all provider files are loaded
        check_doc_files(all_parsed_yaml_files)
        check_invalid_integration(all_parsed_yaml_files)
        check_providers_are_mentioned_in_issue_template(all_parsed_yaml_files)

    if errors:
        error_num = len(errors)
        console.print(f"[red]Found {error_num} error{'' if error_num == 1 else 's'} in providers[/]")
        for error in errors:
            console.print(f"[red]Error:[/] {error}")
        sys.exit(1)
