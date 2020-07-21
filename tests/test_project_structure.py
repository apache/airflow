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

import glob
import itertools
import mmap
import os
import unittest

from parameterized import parameterized

ROOT_FOLDER = os.path.realpath(
    os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir)
)

MISSING_TEST_FILES = {
    'tests/providers/google/cloud/log/test_gcs_task_handler.py',
    'tests/providers/google/cloud/operators/test_datastore.py',
    'tests/providers/google/cloud/transfers/test_sql_to_gcs.py',
    'tests/providers/google/cloud/utils/test_field_sanitizer.py',
    'tests/providers/google/cloud/utils/test_field_validator.py',
    'tests/providers/google/cloud/utils/test_mlengine_prediction_summary.py',
    'tests/providers/microsoft/azure/sensors/test_azure_cosmos.py',
    'tests/providers/microsoft/azure/log/test_wasb_task_handler.py',
    'tests/providers/microsoft/mssql/hooks/test_mssql.py',
    'tests/providers/samba/hooks/test_samba.py'
}


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
        # TODO: Should we extend this test to cover other directories?
        expected_test_files = glob.glob(f"{ROOT_FOLDER}/airflow/providers/**/*.py", recursive=True)
        # Make path relative
        expected_test_files = (os.path.relpath(f, ROOT_FOLDER) for f in expected_test_files)
        # Exclude example_dags
        expected_test_files = (f for f in expected_test_files if "/example_dags/" not in f)
        # Exclude __init__.py
        expected_test_files = (f for f in expected_test_files if not f.endswith("__init__.py"))
        # Change airflow/ to tests/
        expected_test_files = (
            f'tests/{f.partition("/")[2]}'
            for f in expected_test_files if not f.endswith("__init__.py")
        )
        # Add test_ prefix to filename
        expected_test_files = (
            f'{f.rpartition("/")[0]}/test_{f.rpartition("/")[2]}'
            for f in expected_test_files if not f.endswith("__init__.py")
        )

        current_test_files = glob.glob(f"{ROOT_FOLDER}/tests/providers/**/*.py", recursive=True)
        # Make path relative
        current_test_files = (os.path.relpath(f, ROOT_FOLDER) for f in current_test_files)
        # Exclude __init__.py
        current_test_files = (f for f in current_test_files if not f.endswith("__init__.py"))

        expected_test_files = set(expected_test_files)
        current_test_files = set(current_test_files)

        missing_tests_files = expected_test_files - expected_test_files.intersection(current_test_files)
        self.assertEqual(set(), missing_tests_files - MISSING_TEST_FILES)

    def test_keep_missing_test_files_update(self):
        new_test_files = []
        for test_file in MISSING_TEST_FILES:
            if os.path.isfile(f"{ROOT_FOLDER}/{test_file}"):
                new_test_files.append(test_file)
        if new_test_files:
            new_files_text = '\n'.join(new_test_files)
            self.fail(
                "You've added a test file currently listed as missing:\n"
                f"{new_files_text}"
                "\n"
                "Thank you very much.\n"
                "Can you remove it from the list of missing tests, please?"
            )


class TestGoogleProviderProjectStructure(unittest.TestCase):
    MISSING_EXAMPLE_DAGS = {
        ('cloud', 'adls_to_gcs'),
        ('cloud', 'sql_to_gcs'),
        ('cloud', 's3_to_gcs'),
        ('cloud', 'bigquery_to_mysql'),
        ('cloud', 'cassandra_to_gcs'),
        ('cloud', 'mysql_to_gcs'),
        ('cloud', 'mssql_to_gcs'),
        ('cloud', 'gcs_to_local'),
        ('ads', 'ads_to_gcs'),
    }

    def test_example_dags(self):
        operators_modules = itertools.chain(
            *[self.find_resource_files(resource_type=d) for d in ["operators", "sensors", "transfers"]]
        )
        example_dags_files = self.find_resource_files(resource_type="example_dags")
        # Generate tuple of department and service e.g. ('marketing_platform', 'display_video')
        operator_sets = [
            (f.split("/")[-3], f.split("/")[-1].rsplit(".")[0]) for f in operators_modules
        ]
        example_sets = [
            (f.split("/")[-3], f.split("/")[-1].rsplit(".")[0].replace("example_", "", 1))
            for f in example_dags_files
        ]

        def has_example_dag(operator_set):
            for e in example_sets:
                if e[0] != operator_set[0]:
                    continue
                if e[1].startswith(operator_set[1]):
                    return True

            return False

        with self.subTest("Detect missing example dags"):
            missing_example = set(s for s in operator_sets if not has_example_dag(s))
            missing_example -= self.MISSING_EXAMPLE_DAGS
            self.assertEqual(set(), missing_example)

        with self.subTest("Keep update missing example dags list"):
            new_example_dag = set(example_sets).intersection(set(self.MISSING_EXAMPLE_DAGS))
            if new_example_dag:
                new_example_dag_text = '\n'.join(str(f) for f in new_example_dag)
                self.fail(
                    "You've added a example dag currently listed as missing:\n"
                    f"{new_example_dag_text}"
                    "\n"
                    "Thank you very much.\n"
                    "Can you remove it from the list of missing example, please?"
                )

        with self.subTest("Revmoe extra elements"):
            extra_example_dags = set(self.MISSING_EXAMPLE_DAGS) - set(operator_sets)
            if extra_example_dags:
                new_example_dag_text = '\n'.join(str(f) for f in extra_example_dags)
                self.fail(
                    "You've added a example dag currently listed as missing:\n"
                    f"{new_example_dag_text}"
                    "\n"
                    "Thank you very much.\n"
                    "Can you remove it from the list of missing example, please?"
                )

    @parameterized.expand(
        [
            (resource_type, suffix,)
            for suffix in ["_system.py", "_system_helper.py"]
            for resource_type in ["operators", "sensors", "tranfers"]
        ]
    )
    def test_detect_invalid_system_tests(self, resource_type, filename_suffix):
        operators_tests = self.find_resource_files(top_level_directory="tests", resource_type=resource_type)
        operators_files = self.find_resource_files(top_level_directory="airflow", resource_type=resource_type)

        files = {f for f in operators_tests if f.endswith(filename_suffix)}

        expected_files = (f"tests/{f[8:]}" for f in operators_files)
        expected_files = (
            f.replace(".py", filename_suffix).replace("/test_", "/") for f in expected_files
        )
        expected_files = {
            f'{f.rpartition("/")[0]}/test_{f.rpartition("/")[2]}' for f in expected_files
        }

        self.assertEqual(set(), files - expected_files)

    @staticmethod
    def find_resource_files(
        top_level_directory: str = "airflow",
        department: str = "*",
        resource_type: str = "*",
        service: str = "*"
    ):
        python_files = glob.glob(
            f"{ROOT_FOLDER}/{top_level_directory}/providers/google/{department}/{resource_type}/{service}.py"
        )
        # Make path relative
        resource_files = (os.path.relpath(f, ROOT_FOLDER) for f in python_files)
        # Exclude __init__.py and pycache
        resource_files = (f for f in resource_files if not f.endswith("__init__.py"))
        return resource_files


class TestOperatorsHooks(unittest.TestCase):
    def test_no_illegal_suffixes(self):
        illegal_suffixes = ["_operator.py", "_hook.py", "_sensor.py"]
        files = itertools.chain(*[
            glob.glob(f"{ROOT_FOLDER}/{part}/providers/**/{resource_type}/*.py", recursive=True)
            for resource_type in ["operators", "hooks", "sensors", "example_dags"]
            for part in ["airflow", "tests"]
        ])

        invalid_files = [
            f
            for f in files
            if any(f.endswith(suffix) for suffix in illegal_suffixes)
        ]

        self.assertEqual([], invalid_files)
