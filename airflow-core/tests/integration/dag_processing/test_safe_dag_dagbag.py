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

import importlib.util
import os
import sys
import warnings

from airflow.dag_processing.dagbag import DagBag


class TestSafeDagIntegration:
    """
    Integration tests for safe_dag context manager with DagBag processing.
    """

    test_dir = os.path.dirname(os.path.abspath(__file__))
    dag_folder = os.path.join(test_dir, "dags")

    TEST_DAG_FILE = "safe_dag_test.py"
    TEST_DAG_DIRECT_EXECUTION_FILE = "safe_dag_direct_execution.py"

    EXPECTED_SUCCESSFUL_DAGS = ["success_dag_1", "success_dag_2", "direct_execution_dag"]

    def test_safe_dag_errors_collected_by_dagbag(self):
        """
        Test that errors from safe_dag context manager are properly collected by DagBag
        during DAG file processing.

        It verifies the end-to-end flow:
        1. DAG file uses safe_dag context manager with both successful and failing blocks
        2. Successful DAGs are loaded normally
        3. Errors from failing blocks are captured in import_errors
        4. Error messages include line numbers and stack traces
        """
        dagbag = DagBag(dag_folder=self.dag_folder, include_examples=False)

        self._assert_successful_dags_loaded(dagbag)
        self._assert_error_content(dagbag)

    def _assert_successful_dags_loaded(self, dagbag: DagBag) -> None:
        for dag_id in self.EXPECTED_SUCCESSFUL_DAGS:
            assert dag_id in dagbag.dags, (
                f"Expected DAG '{dag_id}' should be loaded. Available DAGs: {list(dagbag.dags.keys())}"
            )

        loaded_successful_count = len(
            [dag_id for dag_id in self.EXPECTED_SUCCESSFUL_DAGS if dag_id in dagbag.dags]
        )
        assert loaded_successful_count == len(self.EXPECTED_SUCCESSFUL_DAGS), (
            f"Expected all {len(self.EXPECTED_SUCCESSFUL_DAGS)} successful DAGs to be loaded, got {loaded_successful_count}"
        )

    def _assert_error_content(self, dagbag: DagBag) -> None:
        error_files = list(dagbag.import_errors.keys())

        for key in error_files:
            assert os.path.isabs(key), f"import_errors key '{key}' should be an absolute path"

        file_path = next(path for path in error_files if self.TEST_DAG_FILE in path)

        errors_from_mixed_file = dagbag.import_errors[file_path]
        assert len(errors_from_mixed_file) >= 1, (
            f"Expected at least 1 error from {self.TEST_DAG_FILE}, got {len(errors_from_mixed_file)}"
        )

        mixed_error_text = " ".join(errors_from_mixed_file)
        assert "failed to create dag at line" in mixed_error_text.lower(), (
            "Error should include line number information"
        )

    def test_safe_dag_without_dagbag_context_warns(self):
        """
        Verifies safe_dag gracefully handles being used outside of the
        DagBag processing context by importing a DAG file directly and validating
        that DAGs are created successfully while issuing appropriate warnings.
        """
        test_dag_file = os.path.join(self.dag_folder, self.TEST_DAG_DIRECT_EXECUTION_FILE)

        with warnings.catch_warnings(record=True) as warning_list:
            warnings.simplefilter("always")

            spec = importlib.util.spec_from_file_location("test_direct_execution", test_dag_file)
            test_module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(test_module)

            assert hasattr(test_module, "created_dags"), "Module should have created_dags list"
            assert len(test_module.created_dags) == 1, (
                f"Expected exactly 1 DAG, got {len(test_module.created_dags)}"
            )

            dag_ids = [dag.dag_id for dag in test_module.created_dags]
            assert "direct_execution_dag" in dag_ids, "direct_execution_dag should be created"

            dag = test_module.created_dags[0]
            assert dag.dag_id == "direct_execution_dag", "DAG should have correct ID"
            assert len(dag.tasks) == 1, "DAG should contain the created task"
            assert dag.tasks[0].task_id == "direct_task", "Task should have correct ID"

            safe_dag_warnings = [w for w in warning_list if "safe_dag()" in str(w.message)]
            assert len(safe_dag_warnings) > 0, (
                "Should issue warning about safe_dag usage outside DagBag context"
            )

            if "test_direct_execution" in sys.modules:
                del sys.modules["test_direct_execution"]
