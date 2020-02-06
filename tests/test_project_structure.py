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
import mmap
import os
import unittest

ROOT_FOLDER = os.path.realpath(
    os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir)
)

MISSING_TEST_FILES = {
    'tests/providers/amazon/aws/hooks/test_athena.py',
    'tests/providers/apache/cassandra/sensors/test_record.py',
    'tests/providers/apache/cassandra/sensors/test_table.py',
    'tests/providers/apache/hdfs/sensors/test_web_hdfs.py',
    'tests/providers/apache/hive/operators/test_vertica_to_hive.py',
    'tests/providers/apache/hive/sensors/test_hive_partition.py',
    'tests/providers/apache/hive/sensors/test_metastore_partition.py',
    'tests/providers/apache/pig/operators/test_pig.py',
    'tests/providers/apache/spark/hooks/test_spark_jdbc_script.py',
    'tests/providers/cncf/kubernetes/operators/test_kubernetes_pod.py',
    'tests/providers/google/cloud/operators/test_datastore.py',
    'tests/providers/google/cloud/operators/test_gcs_to_bigquery.py',
    'tests/providers/google/cloud/operators/test_sql_to_gcs.py',
    'tests/providers/google/cloud/sensors/test_bigquery.py',
    'tests/providers/google/cloud/utils/test_field_sanitizer.py',
    'tests/providers/google/cloud/utils/test_field_validator.py',
    'tests/providers/google/cloud/utils/test_mlengine_operator_utils.py',
    'tests/providers/google/cloud/utils/test_mlengine_prediction_summary.py',
    'tests/providers/jenkins/hooks/test_jenkins.py',
    'tests/providers/microsoft/azure/sensors/test_azure_cosmos.py',
    'tests/providers/microsoft/mssql/hooks/test_mssql.py',
    'tests/providers/microsoft/mssql/operators/test_mssql.py',
    'tests/providers/oracle/operators/test_oracle.py',
    'tests/providers/presto/operators/test_presto_check.py',
    'tests/providers/qubole/hooks/test_qubole.py',
    'tests/providers/samba/hooks/test_samba.py',
    'tests/providers/sqlite/operators/test_sqlite.py',
    'tests/providers/vertica/hooks/test_vertica.py'
}


class TestProjectStructure(unittest.TestCase):
    def test_reference_to_providers_from_core(self):
        for filename in glob.glob(f"{ROOT_FOLDER}/example_dags/**/*.py", recursive=True):
            self.assert_file_not_contains(filename, "providers")

    def test_deprecated_packages(self):
        for directory in ["operator", "hooks", "sensors", "task_runner"]:
            path_pattern = f"{ROOT_FOLDER}/airflow/contrib/{directory}/*.py"

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
