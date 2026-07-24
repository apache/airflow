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


class TestDagTestExecutor:
    """Verifies that the `dag.test()` functionality executes cleanly via the Airflow CLI."""

    def test_dag_test_cli_execution(self, compose_instance):
        """
        Executes the 'airflow dags test' CLI command inside a running service container.
        This provides a true end-to-end integration verification of the dag.test engine
        within a fully initialized environment.
        """
        stdout, stderr, exit_code = compose_instance.exec_in_container(
            service_name="airflow-scheduler",
            command=["airflow", "dags", "test", "example_dag_test_executor"],
        )
        assert exit_code == 0, (
            f"dag.test CLI execution failed with exit code: {exit_code}.\nStdout: {stdout}\nStderr: {stderr}"
        )
