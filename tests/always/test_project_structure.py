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
import ast
import glob
import itertools
import mmap
import os
import unittest
from typing import List

ROOT_FOLDER = os.path.realpath(
    os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir, os.pardir)
)


class TestProjectStructure(unittest.TestCase):
    def test_reference_to_providers_from_core(self):
        for filename in glob.glob(f"{ROOT_FOLDER}/example_dags/**/*.py", recursive=True):
            self.assert_file_not_contains(filename, "providers")

    def test_deprecated_packages(self):
        path_pattern = f"{ROOT_FOLDER}/airflow/contrib/**/*.py"

        for filename in glob.glob(path_pattern, recursive=True):
            if filename.endswith("/__init__.py"):
                self.assert_file_contains(filename, "This package is deprecated.")
            else:
                self.assert_file_contains(filename, "This module is deprecated.")

    def assert_file_not_contains(self, filename: str, pattern: str):
        with open(filename, 'rb', 0) as file, mmap.mmap(file.fileno(), 0, access=mmap.ACCESS_READ) as content:
            if content.find(bytes(pattern, 'utf-8')) != -1:
                self.fail(f"File {filename} not contains pattern - {pattern}")

    def assert_file_contains(self, filename: str, pattern: str):
        with open(filename, 'rb', 0) as file, mmap.mmap(file.fileno(), 0, access=mmap.ACCESS_READ) as content:
            if content.find(bytes(pattern, 'utf-8')) == -1:
                self.fail(f"File {filename} contains illegal pattern - {pattern}")

    def test_providers_modules_should_have_tests(self):
        """
        Assert every module in /airflow/providers has a corresponding test_ file in tests/airflow/providers.
        """
        # Deprecated modules that don't have corresponded test
        expected_missing_providers_modules = {
            (
                'airflow/providers/amazon/aws/hooks/aws_dynamodb.py',
                'tests/providers/amazon/aws/hooks/test_aws_dynamodb.py',
            )
        }

        # TODO: Should we extend this test to cover other directories?
        modules_files = glob.glob(f"{ROOT_FOLDER}/airflow/providers/**/*.py", recursive=True)

        # Make path relative
        modules_files = (os.path.relpath(f, ROOT_FOLDER) for f in modules_files)
        # Exclude example_dags
        modules_files = (f for f in modules_files if "/example_dags/" not in f)
        # Exclude __init__.py
        modules_files = (f for f in modules_files if not f.endswith("__init__.py"))
        # Change airflow/ to tests/
        expected_test_files = (
            f'tests/{f.partition("/")[2]}' for f in modules_files if not f.endswith("__init__.py")
        )
        # Add test_ prefix to filename
        expected_test_files = (
            f'{f.rpartition("/")[0]}/test_{f.rpartition("/")[2]}'
            for f in expected_test_files
            if not f.endswith("__init__.py")
        )

        current_test_files = glob.glob(f"{ROOT_FOLDER}/tests/providers/**/*.py", recursive=True)
        # Make path relative
        current_test_files = (os.path.relpath(f, ROOT_FOLDER) for f in current_test_files)
        # Exclude __init__.py
        current_test_files = (f for f in current_test_files if not f.endswith("__init__.py"))

        modules_files = set(modules_files)
        expected_test_files = set(expected_test_files)
        current_test_files = set(current_test_files)

        missing_tests_files = expected_test_files - expected_test_files.intersection(current_test_files)

        with self.subTest("Detect missing tests in providers module"):
            expected_missing_test_modules = {pair[1] for pair in expected_missing_providers_modules}
            missing_tests_files = missing_tests_files - set(expected_missing_test_modules)
            assert set() == missing_tests_files

        with self.subTest("Verify removed deprecated module also removed from deprecated list"):
            expected_missing_modules = {pair[0] for pair in expected_missing_providers_modules}
            removed_deprecated_module = expected_missing_modules - modules_files
            if removed_deprecated_module:
                self.fail(
                    "You've removed a deprecated module:\n"
                    f"{removed_deprecated_module}"
                    "\n"
                    "Thank you very much.\n"
                    "Can you remove it from the list of expected missing modules tests, please?"
                )


def get_imports_from_file(filepath: str):
    with open(filepath) as py_file:
        content = py_file.read()
    doc_node = ast.parse(content, filepath)
    import_names: List[str] = []
    for current_node in ast.walk(doc_node):
        if not isinstance(current_node, (ast.Import, ast.ImportFrom)):
            continue
        for alias in current_node.names:
            name = alias.name
            fullname = f'{current_node.module}.{name}' if isinstance(current_node, ast.ImportFrom) else name
            import_names.append(fullname)
    return import_names


def filepath_to_module(filepath: str):
    filepath = os.path.relpath(os.path.abspath(filepath), ROOT_FOLDER)
    return filepath.replace("/", ".")[: -(len('.py'))]


def get_classes_from_file(filepath: str):
    with open(filepath) as py_file:
        content = py_file.read()
    doc_node = ast.parse(content, filepath)
    module = filepath_to_module(filepath)
    results: List[str] = []
    for current_node in ast.walk(doc_node):
        if not isinstance(current_node, ast.ClassDef):
            continue
        name = current_node.name
        if not name.endswith("Operator") and not name.endswith("Sensor") and not name.endswith("Operator"):
            continue
        results.append(f"{module}.{name}")
    return results


class TestOperatorsHooks(unittest.TestCase):
    def test_no_illegal_suffixes(self):
        illegal_suffixes = ["_operator.py", "_hook.py", "_sensor.py"]
        files = itertools.chain(
            *(
                glob.glob(f"{ROOT_FOLDER}/{part}/providers/**/{resource_type}/*.py", recursive=True)
                for resource_type in ["operators", "hooks", "sensors", "example_dags"]
                for part in ["airflow", "tests"]
            )
        )

        invalid_files = [f for f in files if any(f.endswith(suffix) for suffix in illegal_suffixes)]

        assert [] == invalid_files
