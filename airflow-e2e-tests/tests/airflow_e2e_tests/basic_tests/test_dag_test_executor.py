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

import pytest


class TestDagTestExecutor:
    """Verifies that `dag.test()` executes cleanly via CLI and Python SDK across matrix configurations."""

    @pytest.mark.parametrize("execution_type", ["cli", "python_sdk"])
    @pytest.mark.parametrize("use_executor", ["true", "false"])
    def test_dag_test_matrix_execution(self, compose_instance, execution_type, use_executor):
        """
        Executes dag.test via both CLI and Python SDK modes with use_executor=True and False.
        This provides a true 4-case end-to-end integration verification.
        """
        if execution_type == "cli":
            cmd_str = (
                f"USE_EXECUTOR={use_executor} "
                "EXECUTE_DAG_TEST=true "
                "airflow dags test example_dag_test_executor"
            )
        else:
            cmd_str = (
                f"USE_EXECUTOR={use_executor} "
                "EXECUTE_DAG_TEST=true "
                "python /opt/airflow/airflow-e2e-tests/tests/airflow_e2e_tests/dags/example_dag_test_executor.py"
            )

        stdout, stderr, exit_code = compose_instance.exec_in_container(
            service_name="airflow-scheduler",
            command=["sh", "-c", cmd_str],
        )

        assert exit_code == 0, (
            f"dag.test failed for mode='{execution_type}' with use_executor='{use_executor}'.\n"
            f"Exit code: {exit_code}.\nStdout: {stdout}\nStderr: {stderr}"
        )
