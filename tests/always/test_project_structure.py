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

import ast
import glob
import itertools
import mmap
import os
import warnings

import pytest

ROOT_FOLDER = os.path.realpath(
    os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir, os.pardir)
)


class TestProjectStructure:
    @staticmethod
    def file_contains(filename: str, pattern: str):
        with open(filename, "rb", 0) as file, mmap.mmap(file.fileno(), 0, access=mmap.ACCESS_READ) as content:
            return content.find(bytes(pattern, "utf-8")) == -1

    def test_reference_to_providers_from_core(self):
        ref = [
            filename
            for filename in glob.glob(f"{ROOT_FOLDER}/example_dags/**/*.py", recursive=True)
            if not self.file_contains(filename, "providers")
        ]
        if ref:
            pytest.fail("\n".join(ref))

    def test_providers_modules_should_have_tests(self):
        """
        Assert every module in /airflow/providers has a corresponding test_ file in tests/airflow/providers.
        """
        assert_on_missing = False

        # TODO: Should we extend this test to cover other directories?
        modules_files = glob.glob(f"{ROOT_FOLDER}/airflow/providers/**/*.py", recursive=True)
        # Make path relative
        modules_files = (os.path.relpath(f, ROOT_FOLDER) for f in modules_files)
        # Exclude example_dags
        modules_files = (f for f in modules_files if "/example_dags/" not in f)
        # Exclude __init__.py
        modules_files = {f for f in modules_files if not f.endswith("__init__.py")}

        # Change airflow/ to tests/
        expected_test_files = (
            f'tests/{f.partition("/")[2]}' for f in modules_files if not f.endswith("__init__.py")
        )
        # Add test_ prefix to filename
        expected_test_files = {
            f'{f.rpartition("/")[0]}/test_{f.rpartition("/")[2]}'
            for f in expected_test_files
            if not f.endswith("__init__.py")
        }

        current_test_files = glob.glob(f"{ROOT_FOLDER}/tests/providers/**/*.py", recursive=True)
        # Make path relative
        current_test_files = (os.path.relpath(f, ROOT_FOLDER) for f in current_test_files)
        # Exclude __init__.py
        current_test_files = {f for f in current_test_files if not f.endswith("__init__.py")}

        missing_tests_files = expected_test_files - expected_test_files.intersection(current_test_files)

        if missing_tests_files:
            message = (
                "Detect missing tests in providers module.\n\n"
                f"Modules Files: {len(modules_files)}, Current Test Files: {len(current_test_files)}, "
                f"Missing Tests Files: {len(missing_tests_files)}.\n\n"
            )
            message += "\n".join(sorted(list(missing_tests_files)))
            if assert_on_missing:
                pytest.fail(message)
            else:
                warnings.warn(message, UserWarning, stacklevel=2)


class ProjectStructureTest:
    PROVIDER = "blank"
    CLASS_DIRS = {"operators", "sensors", "transfers"}
    CLASS_SUFFIXES = ["Operator", "Sensor"]

    @staticmethod
    def filepath_to_module(filepath: str):
        filepath = os.path.relpath(os.path.abspath(filepath), ROOT_FOLDER)
        return filepath.replace("/", ".")[: -(len(".py"))]

    @staticmethod
    def print_sorted(container: set, indent: str = "    ") -> None:
        sorted_container = sorted(container)
        print(f"{indent}" + f"\n{indent}".join(sorted_container))

    def class_paths(self):
        """Override this method if your classes are located under different paths"""
        for resource_type in self.CLASS_DIRS:
            python_files = glob.glob(
                f"{ROOT_FOLDER}/airflow/providers/{self.PROVIDER}/**/{resource_type}/**.py", recursive=True
            )
            # Make path relative
            resource_files = (os.path.relpath(f, ROOT_FOLDER) for f in python_files)
            resource_files = (f for f in resource_files if not f.endswith("__init__.py"))
            yield from resource_files

    def list_of_classes(self):
        classes = {}
        for operator_file in self.class_paths():
            operators_paths = self.get_classes_from_file(f"{ROOT_FOLDER}/{operator_file}")
            classes.update(operators_paths)
        return classes

    def get_classes_from_file(self, filepath: str):
        with open(filepath) as py_file:
            content = py_file.read()
        doc_node = ast.parse(content, filepath)
        module = self.filepath_to_module(filepath)
        results: dict = {}
        for current_node in ast.walk(doc_node):
            if not isinstance(current_node, ast.ClassDef):
                continue
            name = current_node.name
            if not any(name.endswith(suffix) for suffix in self.CLASS_SUFFIXES):
                continue
            results[f"{module}.{name}"] = current_node
        return results


class AssetsCoverageTest(ProjectStructureTest):
    """Checks that every operator have operator_extra_links attribute"""

    # These operators should not have assets
    ASSETS_NOT_REQUIRED: set = set()

    # Please add assets to following classes
    MISSING_ASSETS_FOR_CLASSES: set = set()

    def test_missing_assets(self):
        classes = self.list_of_classes()
        assets, no_assets = set(), set()
        for name, operator in classes.items():
            for attr in operator.body:
                if (
                    isinstance(attr, ast.Assign)
                    and attr.targets
                    and getattr(attr.targets[0], "id", "") == "operator_extra_links"
                ):
                    assets.add(name)
                    break
            else:
                no_assets.add(name)

        asset_should_be_missing = self.ASSETS_NOT_REQUIRED - no_assets
        no_assets -= self.ASSETS_NOT_REQUIRED
        no_assets -= self.MISSING_ASSETS_FOR_CLASSES
        if set() != no_assets:
            print("Classes with missing assets:")
            self.print_sorted(no_assets)
            pytest.fail("Some classes are missing assets")
        if set() != asset_should_be_missing:
            print("Classes that should not have assets:")
            self.print_sorted(asset_should_be_missing)
            pytest.fail("Class should not have assets")


class TestOperatorsHooks:
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
