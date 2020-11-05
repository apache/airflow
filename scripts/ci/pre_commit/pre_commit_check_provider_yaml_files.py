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

import json
import os
import sys
import textwrap
from collections import Counter
from glob import glob
from itertools import chain, product
from typing import Any, Dict, Iterable

import jsonschema
import yaml
from tabulate import tabulate

if __name__ != "__main__":
    raise Exception(
        "This file is intended to be executed as an executable program. You cannot use it as a module."
    )

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir, os.pardir, os.pardir))
DOCS_DIR = os.path.join(ROOT_DIR, 'docs')
PROVIDER_DATA_SCHEMA_PATH = os.path.join(ROOT_DIR, "airflow", "provider.yaml.schema.json")
CORE_INTEGRATIONS = ["SQL", "Local"]


def _filepath_to_module(filepath: str):
    filepath = os.path.relpath(os.path.abspath(filepath), ROOT_DIR)
    if filepath.endswith(".py"):
        filepath = filepath[: -(len(".py"))]
    return filepath.replace("/", ".")


def _load_schema() -> Dict[str, Any]:
    with open(PROVIDER_DATA_SCHEMA_PATH) as schema_file:
        content = json.load(schema_file)
    return content


def _load_package_data(package_paths: Iterable[str]):
    schema = _load_schema()
    result = {}
    for provider_yaml_path in package_paths:
        with open(provider_yaml_path) as yaml_file:
            provider = yaml.safe_load(yaml_file)
        rel_path = os.path.relpath(provider_yaml_path, ROOT_DIR)
        try:
            jsonschema.validate(provider, schema=schema)
        except jsonschema.ValidationError:
            raise Exception(f"Unable to parse: {rel_path}.")
        result[rel_path] = provider
    return result


def get_all_integration_names(yaml_files):
    all_integrations = [
        i['integration-name'] for f in yaml_files.values() if 'integrations' in f for i in f["integrations"]
    ]
    all_integrations += ["SQL", "Local"]
    return all_integrations


def check_integration_duplicates(yaml_files: Dict[str, Dict]):
    """Integration names must be globally unique."""
    print("Checking integration duplicates")
    all_integrations = get_all_integration_names(yaml_files)

    duplicates = [(k, v) for (k, v) in Counter(all_integrations).items() if v > 1]

    if duplicates:
        print(
            "Duplicate integration names found. Integration names must be globally unique. "
            "Please delete duplicates."
        )
        print(tabulate(duplicates, headers=["Integration name", "Number of occurrences"]))
        sys.exit(0)


def assert_sets_equal(set1, set2):
    try:
        difference1 = set1.difference(set2)
    except TypeError as e:
        raise AssertionError('invalid type when attempting set difference: %s' % e)
    except AttributeError as e:
        raise AssertionError('first argument does not support set difference: %s' % e)

    try:
        difference2 = set2.difference(set1)
    except TypeError as e:
        raise AssertionError('invalid type when attempting set difference: %s' % e)
    except AttributeError as e:
        raise AssertionError('second argument does not support set difference: %s' % e)

    if not (difference1 or difference2):
        return

    lines = []
    if difference1:
        lines.append('Items in the first set but not the second:')
        for item in sorted(difference1):
            lines.append(repr(item))
    if difference2:
        lines.append('Items in the second set but not the first:')
        for item in sorted(difference2):
            lines.append(repr(item))

    standard_msg = '\n'.join(lines)
    raise AssertionError(standard_msg)


def check_completeness_of_list_of_hooks_sensors_hooks(yaml_files: Dict[str, Dict]):
    print("Checking completeness of list of {sensors, hooks, operators}")
    errors = []
    for (yaml_file_path, provider_data), resource_type in product(
        yaml_files.items(), ["sensors", "operators", "hooks"]
    ):
        package_dir = ROOT_DIR + "/" + os.path.dirname(yaml_file_path)
        py_files = chain(
            glob(f"{package_dir}/**/{resource_type}/*.py"), glob(f"{package_dir}/{resource_type}/*.py")
        )
        expected_modules = {_filepath_to_module(f) for f in py_files if not f.endswith("/__init__.py")}

        resource_data = provider_data.get(resource_type, [])

        current_modules = {i for r in resource_data for i in r.get('python-modules', [])}
        try:
            assert_sets_equal(set(expected_modules), set(current_modules))
        except AssertionError as ex:
            nested_error = textwrap.indent(str(ex), '  ')
            errors.append(
                f"Incorrect content of key '{resource_type}/python-modules' "
                f"in file: {yaml_file_path}\n{nested_error}"
            )
    if errors:
        print(f"Found {len(errors)} errors")
        for error in errors:
            print(error)
            print()
        sys.exit(1)


def check_completeness_of_list_of_transfers(yaml_files: Dict[str, Dict]):
    print("Checking completeness of list of transfers")
    errors = []
    resource_type = 'transfers'
    for yaml_file_path, provider_data in yaml_files.items():
        package_dir = ROOT_DIR + "/" + os.path.dirname(yaml_file_path)
        py_files = chain(
            glob(f"{package_dir}/**/{resource_type}/*.py"), glob(f"{package_dir}/{resource_type}/*.py")
        )
        expected_modules = {_filepath_to_module(f) for f in py_files if not f.endswith("/__init__.py")}

        resource_data = provider_data.get(resource_type, [])

        current_modules = {r.get('python-module') for r in resource_data}
        try:
            assert_sets_equal(set(expected_modules), set(current_modules))
        except AssertionError as ex:
            nested_error = textwrap.indent(str(ex), '  ')
            errors.append(
                f"Incorrect content of key '{resource_type}/python-module' "
                f"in file: {yaml_file_path}\n{nested_error}"
            )
    if errors:
        print(f"Found {len(errors)} errors")
        for error in errors:
            print(error)
            print()
        sys.exit(1)


def check_invalid_integration(yaml_files: Dict[str, Dict]):
    print("Detect unregistered integrations")
    errors = []
    all_integration_names = set(get_all_integration_names(yaml_files))

    for (yaml_file_path, provider_data), resource_type in product(
        yaml_files.items(), ["sensors", "operators", "hooks"]
    ):
        resource_data = provider_data.get(resource_type, [])
        current_names = {r['integration-name'] for r in resource_data}
        invalid_names = current_names - all_integration_names
        if invalid_names:
            errors.append(
                f"Incorrect content of key '{resource_type}/integration-name' in file: {yaml_file_path}. "
                f"Invalid values: {invalid_names}"
            )

    for (yaml_file_path, provider_data), key in product(
        yaml_files.items(), ['source-integration-name', 'target-integration-name']
    ):
        resource_data = provider_data.get('transfers', [])
        current_names = {r[key] for r in resource_data}
        invalid_names = current_names - all_integration_names
        if invalid_names:
            errors.append(
                f"Incorrect content of key 'transfers/{key}' in file: {yaml_file_path}. "
                f"Invalid values: {invalid_names}"
            )

    if errors:
        print(f"Found {len(errors)} errors")
        for error in errors:
            print(error)
            print()
        sys.exit(1)


# TODO: Delete after splitting the documentation for each provider.
DOC_FILES_EXCLUDE_LIST = {
    '/docs/howto/operator/bash.rst',
    '/docs/howto/operator/external_task_sensor.rst',
    '/docs/howto/operator/python.rst',
}


def check_doc_files(yaml_files: Dict[str, Dict]):
    print("Checking doc files")
    current_doc_urls = []
    for provider in yaml_files.values():
        if 'integrations' in provider:
            current_doc_urls.extend(
                guide
                for guides in provider['integrations']
                if 'how-to-guide' in guides
                for guide in guides['how-to-guide']
            )
        if 'transfers' in provider:
            current_doc_urls.extend(
                op['how-to-guide'] for op in provider['transfers'] if 'how-to-guide' in op
            )

    expected_doc_urls = {
        "/docs/" + os.path.relpath(f, start=DOCS_DIR)
        for f in glob(f"{DOCS_DIR}/howto/operator/**/*.rst", recursive=True)
        if not f.endswith("/index.rst") and '/_partials' not in f
    }
    expected_doc_urls |= {
        "/docs/" + os.path.relpath(f, start=DOCS_DIR)
        for f in glob(f"{DOCS_DIR}//apache-airflow-providers-*/operators/**/*.rst", recursive=True)
        if not f.endswith("/index.rst") and '/_partials' not in f
    }

    expected_doc_urls -= DOC_FILES_EXCLUDE_LIST
    try:
        assert_sets_equal(set(expected_doc_urls), set(current_doc_urls))
    except AssertionError as ex:
        print(ex)
        sys.exit(1)


if __name__ == '__main__':
    all_provider_files = sorted(glob(f"{ROOT_DIR}/airflow/providers/**/provider.yaml", recursive=True))
    if len(sys.argv) > 1:
        paths = sorted(sys.argv[1:])
    else:
        paths = all_provider_files

    all_parsed_yaml_files: Dict[str, Dict] = _load_package_data(paths)

    all_files_loaded = len(all_provider_files) == len(paths)
    check_integration_duplicates(all_parsed_yaml_files)

    check_completeness_of_list_of_hooks_sensors_hooks(all_parsed_yaml_files)
    check_completeness_of_list_of_transfers(all_parsed_yaml_files)

    if all_files_loaded:
        # Only check those if all provider files are loaded
        check_doc_files(all_parsed_yaml_files)
        check_invalid_integration(all_parsed_yaml_files)
