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
from ast import Import, ImportFrom, NodeVisitor, parse
from collections import defaultdict
from os.path import dirname, sep
from typing import Dict, List, Optional

from setup import PROVIDERS_REQUIREMENTS

sys.path.append(os.path.join(dirname(__file__), os.pardir))


AIRFLOW_PROVIDERS_FILE_PREFIX = "airflow" + sep + "providers" + sep
AIRFLOW_TESTS_PROVIDERS_FILE_PREFIX = "tests" + sep + "providers" + sep
AIRFLOW_PROVIDERS_IMPORT_PREFIX = "airflow.providers."

# List of information messages generated
infos: List[str] = []
# List of warnings generated
warnings: List[str] = []
# list of errors generated
errors: List[str] = []

# store dependencies
dependencies: Dict[str, List[str]] = defaultdict(list)


def find_provider(provider_elements: List[str]) -> Optional[str]:
    """
    Finds provider name from the list of elements provided. It looks the providers up
    in PROVIDERS_DEPENDENCIES map taken from the provider's package setup.

    :param provider_elements: array of elements of the path (split)
    :return: provider name or None if no provider could be found
    """
    provider = ""
    separator = ""
    provider_keys = PROVIDERS_REQUIREMENTS.keys()
    for element in provider_elements:
        provider = provider + separator + element
        if provider in provider_keys:
            return provider
        separator = "."
    return None


def get_provider_from_file_name(file_name: str) -> Optional[str]:
    """
    Retrieves provider name from file name
    :param file_name: name of the file
    :return: provider name or None if no provider could be found
    """
    if (
        AIRFLOW_PROVIDERS_FILE_PREFIX not in file_name
        and AIRFLOW_TESTS_PROVIDERS_FILE_PREFIX not in file_name
    ):
        # We should only check file that are provider
        errors.append(f"Wrong file not in the providers package = {file_name}")
        return None
    suffix = get_file_suffix(file_name)
    split_path = suffix.split(sep)[2:]
    provider = find_provider(split_path)
    if not provider and file_name.endswith("__init__.py"):
        infos.append(f"Skipped file = {file_name}")
    elif not provider:
        warnings.append(f"Provider not found for path = {file_name}")
    return provider


def get_file_suffix(file_name):
    if AIRFLOW_PROVIDERS_FILE_PREFIX in file_name:
        return file_name[file_name.find(AIRFLOW_PROVIDERS_FILE_PREFIX) :]
    if AIRFLOW_TESTS_PROVIDERS_FILE_PREFIX in file_name:
        return file_name[file_name.find(AIRFLOW_TESTS_PROVIDERS_FILE_PREFIX) :]
    return None


def get_provider_from_import(import_name: str) -> Optional[str]:
    """
    Retrieves provider name from file name
    :param import_name: name of the import
    :return: provider name or None if no provider could be found
    """
    if AIRFLOW_PROVIDERS_IMPORT_PREFIX not in import_name:
        # skip silently - we expect non-providers imports
        return None
    suffix = import_name[import_name.find(AIRFLOW_PROVIDERS_IMPORT_PREFIX) :]
    split_import = suffix.split(".")[2:]
    provider = find_provider(split_import)
    if not provider:
        warnings.append(f"Provider not found for import = {import_name}")
    return provider


class ImportFinder(NodeVisitor):
    """
    AST visitor that collects all imported names in its imports
    """

    def __init__(self, filename):
        self.imports: List[str] = []
        self.filename = filename
        self.handled_import_exception = List[str]
        self.tried_imports: List[str] = []

    def process_import(self, import_name: str):
        self.imports.append(import_name)

    def get_import_name_from_import_from(self, node: ImportFrom) -> List[str]:  # noqa
        """
        Retrieves import name from the "from" import.
        :param node: ImportFrom name
        :return: import name
        """
        import_names: List[str] = []
        for alias in node.names:
            name = alias.name
            fullname = f'{node.module}.{name}' if node.module else name
            import_names.append(fullname)
        return import_names

    def visit_Import(self, node: Import):  # pylint: disable=invalid-name
        for alias in node.names:
            self.process_import(alias.name)

    def visit_ImportFrom(self, node: ImportFrom):  # pylint: disable=invalid-name
        if node.module == '__future__':
            return
        for fullname in self.get_import_name_from_import_from(node):
            self.process_import(fullname)


def get_imports_from_file(file_name: str) -> List[str]:
    """
    Retrieves imports from file.
    :param file_name: name of the file
    :return: list of import names
    """
    try:
        with open(file_name, encoding="utf-8") as f:
            root = parse(f.read(), file_name)
    except Exception:
        print(f"Error when opening file {file_name}", file=sys.stderr)
        raise
    visitor = ImportFinder(file_name)
    visitor.visit(root)
    return visitor.imports


def check_if_different_provider_used(file_name: str):
    file_provider = get_provider_from_file_name(file_name)
    if not file_provider:
        return
    imports = get_imports_from_file(file_name)
    for import_name in imports:
        import_provider = get_provider_from_import(import_name)
        if import_provider and file_provider != import_provider:
            dependencies[file_provider].append(import_provider)


def parse_arguments():
    import argparse

    parser = argparse.ArgumentParser(
        description='Checks if dependencies between packages are handled correctly.'
    )
    parser.add_argument(
        "-f", "--provider-dependencies-file", help="Stores dependencies between providers in the file"
    )
    parser.add_argument(
        "-d", "--documentation-file", help="Updates package documentation in the file specified (.rst)"
    )
    parser.add_argument('files', nargs='*')
    args = parser.parse_args()

    if len(args.files) < 1:
        parser.print_usage()
        print()
        sys.exit(2)
    return args.files, args.provider_dependencies_file, args.documentation_file


PREFIX = "    "

HEADER = """
========================== ===========================
Package                    Extras
========================== ===========================
"""
FOOTER = """========================== ===========================

"""


def insert_documentation(deps_dict: Dict[str, List[str]], res: List[str]):
    res += HEADER.splitlines(keepends=True)
    for package, deps in deps_dict.items():
        deps_str = ",".join(deps)
        res.append(f"{package:27}{deps_str}\n")
    res += FOOTER.splitlines(keepends=True)


if __name__ == '__main__':
    print()
    files, provider_dependencies_file_name, documentation_file_name = parse_arguments()
    num_files = 0
    for file in files:
        check_if_different_provider_used(file)
        num_files += 1
    print(f"Verified {num_files} files.")
    if infos:
        print("\nInformation messages:\n")
        for info in infos:
            print(PREFIX + info)
        print(f"Total: {len(infos)} information messages.")
    if warnings:
        print("\nWarnings!\n")
        for warning in warnings:
            print(PREFIX + warning)
        print(f"Total: {len(warnings)} warnings.")
    if errors:
        print("\nErrors!\n")
        for error in errors:
            print(PREFIX + error)
        print(f"Total: {len(errors)} errors.")
    unique_sorted_dependencies: Dict[str, List[str]] = {}
    for key in sorted(dependencies.keys()):
        unique_sorted_dependencies[key] = sorted(set(dependencies[key]))
    if provider_dependencies_file_name:
        with open(provider_dependencies_file_name, "w") as providers_file:
            json.dump(unique_sorted_dependencies, providers_file, indent=2)
            providers_file.write("\n")
        print()
        print(f"Written provider dependencies to the file {provider_dependencies_file_name}")
        print()
    if documentation_file_name:
        with open(documentation_file_name, encoding="utf-8") as documentation_file:
            text = documentation_file.readlines()
        replacing = False
        result: List[str] = []
        for line in text:
            if line.startswith("  .. START PACKAGE DEPENDENCIES HERE"):
                replacing = True
                result.append(line)
                insert_documentation(unique_sorted_dependencies, result)
            if line.startswith("  .. END PACKAGE DEPENDENCIES HERE"):
                replacing = False
            if not replacing:
                result.append(line)
        with open(documentation_file_name, "w", encoding="utf-8") as documentation_file:
            documentation_file.write("".join(result))
        print()
        print(f"Written package extras to the file {documentation_file_name}")
        print()
    if errors:
        print()
        print("ERROR! Errors found during verification. Exiting!")
        print()
        sys.exit(1)
    print()
    print("Verification complete! Success!")
    print()
