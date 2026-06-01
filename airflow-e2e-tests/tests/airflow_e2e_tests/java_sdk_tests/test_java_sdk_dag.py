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
"""E2E tests for the Java SDK via the annotation-based example bundle.

Run with::

    E2E_TEST_MODE=java_sdk uv run --project airflow-e2e-tests pytest \\
        tests/airflow_e2e_tests/java_sdk_tests/ -xvs

What is verified
----------------
The test triggers the ``java_annotation_example`` Dag, which has this task
graph::

    python_task_1 >> extract >> transform >> python_task_2

* ``extract`` and ``transform`` are ``@task.stub(queue="java")`` stubs whose
  implementations live in ``AnnotationExample.java``.  Both run via
  ``JavaCoordinator``, which spawns a JVM subprocess for each.
* ``extract`` reads an XCom from ``python_task_1``, fetches the ``test_http``
  connection, and returns a timestamp (long).
* ``transform`` reads the XCom from ``extract``, fetches the ``my_variable``
  Airflow variable, and returns a timestamp (long).

The test asserts that both Java task instances reach state ``success``, which
confirms:

1. ``JavaCoordinator`` correctly discovers and launches the JVM JAR.
2. The wire protocol (supervisor → JVM → supervisor) round-trips
   ``StartupDetails`` and the task result (``SucceedTask``/``TaskState``).
3. XCom reads and API calls (getXCom, getConnection, getVariable) work
   end-to-end through the Task Execution API.
"""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from airflow_e2e_tests.e2e_test_utils.clients import AirflowClient

# The Java extract task sleeps 6 s + coordinator startup; allow plenty of room.
_JAVA_TASK_TIMEOUT = 600


class TestJavaSDKAnnotationExample:
    """Verify the annotation-based Java SDK example executes correctly."""

    airflow_client = AirflowClient()

    @pytest.mark.parametrize("dag_id", ["java_annotation_example"])
    def test_java_tasks_execute_successfully(self, dag_id: str):
        """Both Java stubs in the annotation example must succeed."""
        resp = self.airflow_client.trigger_dag(
            dag_id,
            json={"logical_date": datetime.now(timezone.utc).isoformat()},
        )
        run_id = resp["dag_run_id"]

        # Wait for all tasks to finish (success or failed).
        dag_state = self.airflow_client.wait_for_dag_run(
            dag_id=dag_id,
            run_id=run_id,
            timeout=_JAVA_TASK_TIMEOUT,
        )

        # Collect task-instance states for a detailed failure message.
        ti_resp = self.airflow_client.get_task_instances(dag_id=dag_id, run_id=run_id)
        ti_map = {ti["task_id"]: ti for ti in ti_resp.get("task_instances", [])}

        extract_ti = ti_map.get("extract", {})
        transform_ti = ti_map.get("transform", {})

        assert extract_ti.get("state") == "success", (
            f"Java 'extract' task did not succeed.\n"
            f"  task state : {extract_ti.get('state')!r}\n"
            f"  dag state  : {dag_state!r}\n"
            f"  all tasks  : { {k: v.get('state') for k, v in ti_map.items()} }"
        )
        assert transform_ti.get("state") == "success", (
            f"Java 'transform' task did not succeed.\n"
            f"  task state : {transform_ti.get('state')!r}\n"
            f"  dag state  : {dag_state!r}\n"
            f"  all tasks  : { {k: v.get('state') for k, v in ti_map.items()} }"
        )

    def test_transform_xcom_is_numeric_timestamp(self):
        """The value returned by the Java 'transform' task must be a positive integer."""
        resp = self.airflow_client.trigger_dag(
            "java_annotation_example",
            json={"logical_date": datetime.now(timezone.utc).isoformat()},
        )
        run_id = resp["dag_run_id"]

        self.airflow_client.wait_for_dag_run(
            dag_id="java_annotation_example",
            run_id=run_id,
            timeout=_JAVA_TASK_TIMEOUT,
        )

        xcom = self.airflow_client.get_xcom_value(
            dag_id="java_annotation_example",
            task_id="transform",
            run_id=run_id,
            key="return_value",
        )
        value = xcom.get("value")
        assert isinstance(value, int), (
            f"Expected 'transform' XCom to be an integer (millisecond timestamp), got {value!r}"
        )
        assert value > 0, (
            f"Expected 'transform' XCom to be a positive integer (millisecond timestamp), got {value!r}"
        )
