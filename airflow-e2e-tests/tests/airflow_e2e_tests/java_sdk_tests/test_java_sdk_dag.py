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

    python_task_1 >> extract >> transform >> [load, python_task_2]

* ``extract`` and ``transform`` are ``@task.stub(queue="java")`` stubs whose
  implementations live in ``AnnotationExample.java``.  Both run via
  ``JavaCoordinator``, which spawns a JVM subprocess for each.
* ``extract`` reads an XCom from ``python_task_1``, fetches the ``test_http``
  connection, and returns a timestamp (long).
* ``transform`` reads the XCom from ``extract``, fetches the ``my_variable``
  Airflow variable, and returns a timestamp (long).
* ``load`` (``retries=1``) reads the XCom from ``transform``, throws on its
  first attempt and returns normally on the retry, exercising the UP_FOR_RETRY
  path through the Java coordinator.

The test asserts that the Java task instances reach state ``success``, which
confirms:

1. ``JavaCoordinator`` correctly discovers and launches the JVM JAR.
2. The wire protocol (supervisor → JVM → supervisor) round-trips
   ``StartupDetails`` and the task result (``SucceedTask``/``TaskState``).
3. XCom reads and API calls (getXCom, getConnection, getVariable) work
   end-to-end through the Task Execution API.
4. A Java task that throws with retries left returns ``RetryTask`` rather than a
   terminal ``FAILED``, so the supervisor marks it UP_FOR_RETRY and re-runs it;
   ``load`` therefore ends ``success`` on its second attempt (try_number 2).
"""

from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import TYPE_CHECKING

import pytest

from airflow_e2e_tests.e2e_test_utils.clients import AirflowClient

if TYPE_CHECKING:
    from collections.abc import Callable

# The Java extract task sleeps 6 s + coordinator startup; allow plenty of room.
_JAVA_TASK_TIMEOUT = 600
# Logs can lag slightly behind the task reaching a terminal state.
_LOG_FETCH_TIMEOUT = 60


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

    def test_concurrent_client_calls_succeed(self):
        """A Java task calling the client from many threads must succeed."""
        resp = self.airflow_client.trigger_dag(
            "java_annotation_example",
            json={"logical_date": datetime.now(timezone.utc).isoformat()},
        )
        run_id = resp["dag_run_id"]

        dag_state = self.airflow_client.wait_for_dag_run(
            dag_id="java_annotation_example",
            run_id=run_id,
            timeout=_JAVA_TASK_TIMEOUT,
        )

        ti_resp = self.airflow_client.get_task_instances(dag_id="java_annotation_example", run_id=run_id)
        ti_map = {ti["task_id"]: ti for ti in ti_resp.get("task_instances", [])}
        concurrent_ti = ti_map.get("concurrent", {})

        assert concurrent_ti.get("state") == "success", (
            f"Java 'concurrent' task did not succeed.\n"
            f"  task state : {concurrent_ti.get('state')!r}\n"
            f"  dag state  : {dag_state!r}\n"
            f"  all tasks  : { {k: v.get('state') for k, v in ti_map.items()} }"
        )

    def test_numeric_xcom_casting(self):
        """Numeric XComs are read across declared types (int -> long -> double, and a wire
        double back as a float), and a boxed param stays null when its XCom is absent."""
        resp = self.airflow_client.trigger_dag(
            "java_xcom_casting_example",
            json={"logical_date": datetime.now(timezone.utc).isoformat()},
        )
        run_id = resp["dag_run_id"]

        dag_state = self.airflow_client.wait_for_dag_run(
            dag_id="java_xcom_casting_example",
            run_id=run_id,
            timeout=_JAVA_TASK_TIMEOUT,
        )

        ti_resp = self.airflow_client.get_task_instances(dag_id="java_xcom_casting_example", run_id=run_id)
        ti_map = {ti["task_id"]: ti for ti in ti_resp.get("task_instances", [])}

        for task_id in ("widen_to_double", "consume_nullable", "consume_float"):
            assert ti_map.get(task_id, {}).get("state") == "success", (
                f"Java {task_id!r} task did not succeed.\n"
                f"  task state : {ti_map.get(task_id, {}).get('state')!r}\n"
                f"  dag state  : {dag_state!r}\n"
                f"  all tasks  : { {k: v.get('state') for k, v in ti_map.items()} }"
            )

    def test_load_retried_then_succeeded(self):
        """``load`` fails once (UP_FOR_RETRY) then succeeds on the second attempt.

        The Java coordinator must return ``RetryTask`` (not terminal ``FAILED``)
        when the task throws with retries left, so the supervisor re-runs it. The
        end state is ``success`` reached on ``try_number`` 2.
        """
        resp = self.airflow_client.trigger_dag(
            "java_annotation_example",
            json={"logical_date": datetime.now(timezone.utc).isoformat()},
        )
        run_id = resp["dag_run_id"]

        dag_state = self.airflow_client.wait_for_dag_run(
            dag_id="java_annotation_example",
            run_id=run_id,
            timeout=_JAVA_TASK_TIMEOUT,
        )

        ti_resp = self.airflow_client.get_task_instances(dag_id="java_annotation_example", run_id=run_id)
        ti_map = {ti["task_id"]: ti for ti in ti_resp.get("task_instances", [])}
        load_ti = ti_map.get("load", {})

        assert load_ti.get("state") == "success", (
            f"Java 'load' task should succeed on retry.\n"
            f"  task state : {load_ti.get('state')!r}\n"
            f"  dag state  : {dag_state!r}\n"
            f"  all tasks  : { {k: v.get('state') for k, v in ti_map.items()} }"
        )
        assert load_ti.get("try_number") == 2, (
            f"Java 'load' task should have run twice (fail then retry); "
            f"try_number={load_ti.get('try_number')!r}, ti: {load_ti}"
        )

    def _wait_for_transform_log_record(
        self, run_id: str, try_number: int, match: Callable[[dict], bool]
    ) -> tuple[dict | None, list[dict]]:
        """Poll the ``transform`` task logs until a record matching *match* appears.

        Logs can lag behind the terminal task state, and earlier records (e.g. the
        first transform line) arrive before the one under test, so returning on any
        record would race. Keep polling until the target record shows up or the
        deadline passes. Returns the matching record (or ``None``) and the last
        batch of records seen for diagnostics.
        """
        deadline = time.monotonic() + _LOG_FETCH_TIMEOUT
        records: list[dict] = []
        while True:
            resp = self.airflow_client.get_task_logs(
                dag_id="java_annotation_example", run_id=run_id, task_id="transform", try_number=try_number
            )
            records = [entry for entry in resp.get("content", []) if isinstance(entry, dict)]
            record = next((r for r in records if match(r)), None)
            if record is not None or time.monotonic() > deadline:
                return record, records
            time.sleep(3)

    def test_application_logs_preserve_their_level(self):
        """A Java task's SLF4J ``logger.info`` must reach the UI as INFO, not ERROR.

        Without the SDK's SLF4J binding the application's logs fall through to
        stderr and the supervisor tags every line ERROR. The binding routes them
        over the logs socket carrying the real level instead.
        """
        resp = self.airflow_client.trigger_dag(
            "java_annotation_example",
            json={"logical_date": datetime.now(timezone.utc).isoformat()},
        )
        run_id = resp["dag_run_id"]
        dag_state = self.airflow_client.wait_for_dag_run(
            dag_id="java_annotation_example",
            run_id=run_id,
            timeout=_JAVA_TASK_TIMEOUT,
        )

        # The log under test is emitted only if transform actually ran; assert it
        # succeeded and fetch the attempt that produced the logs (transform does
        # not retry, but read try_number rather than assuming attempt 1).
        ti_resp = self.airflow_client.get_task_instances(dag_id="java_annotation_example", run_id=run_id)
        ti_map = {ti["task_id"]: ti for ti in ti_resp.get("task_instances", [])}
        transform_ti = ti_map.get("transform", {})
        assert transform_ti.get("state") == "success", (
            f"Java 'transform' task must succeed to emit the log under test.\n"
            f"  task state : {transform_ti.get('state')!r}\n"
            f"  dag state  : {dag_state!r}\n"
            f"  all tasks  : { {k: v.get('state') for k, v in ti_map.items()} }"
        )

        # transform logs `logger.info("Got variable {}", variable)` -> "Got variable 123".
        record, records = self._wait_for_transform_log_record(
            run_id,
            transform_ti.get("try_number", 1),
            lambda r: str(r.get("event", "")).startswith("Got variable"),
        )
        assert record is not None, (
            f"transform should emit a 'Got variable' INFO record; "
            f"events seen: {[r.get('event') for r in records]}"
        )
        assert str(record.get("level", "")).lower() == "info", (
            f"application INFO log should keep its level, got {record.get('level')!r}; record: {record}"
        )


# Each Scala task spins up its own local SparkSession; allow generous time for
# three sequential JVM + Spark startups in a constrained CI container.
_SPARK_TASK_TIMEOUT = 1200

# Mirror the fixed dataset that is the single source of truth in
# ScalaSparkExample.scala (``SalesData.rows``): 5 sales rows whose amounts
# (100+200+300+150+250) sum to 1000. Keep these in sync if that dataset changes.
_SPARK_EXPECTED_ROW_COUNT = 5
_SPARK_EXPECTED_TOTAL_REVENUE = 1000


class TestJavaSDKScalaSparkExample:
    """Verify the Scala + Apache Spark ETL example bundle executes correctly."""

    airflow_client = AirflowClient()

    def test_spark_etl_pipeline(self):
        """The three Scala Spark stubs run in order and pass scalar results via XCom.

        Each runs in its own JVM through ``JavaCoordinator`` with real Spark.
        """
        resp = self.airflow_client.trigger_dag(
            "scala_spark_example",
            json={"logical_date": datetime.now(timezone.utc).isoformat()},
        )
        run_id = resp["dag_run_id"]

        dag_state = self.airflow_client.wait_for_dag_run(
            dag_id="scala_spark_example",
            run_id=run_id,
            timeout=_SPARK_TASK_TIMEOUT,
        )

        ti_resp = self.airflow_client.get_task_instances(dag_id="scala_spark_example", run_id=run_id)
        ti_map = {ti["task_id"]: ti for ti in ti_resp.get("task_instances", [])}

        for task_id in ("spark_extract", "spark_transform", "spark_load"):
            assert ti_map.get(task_id, {}).get("state") == "success", (
                f"Scala Spark {task_id!r} task did not succeed.\n"
                f"  task state : {ti_map.get(task_id, {}).get('state')!r}\n"
                f"  dag state  : {dag_state!r}\n"
                f"  all tasks  : { {k: v.get('state') for k, v in ti_map.items()} }"
            )

        extract_xcom = self.airflow_client.get_xcom_value(
            dag_id="scala_spark_example", task_id="spark_extract", run_id=run_id, key="return_value"
        )
        assert extract_xcom.get("value") == _SPARK_EXPECTED_ROW_COUNT, (
            f"Expected spark_extract to push row count {_SPARK_EXPECTED_ROW_COUNT}, "
            f"got {extract_xcom.get('value')!r}"
        )

        transform_xcom = self.airflow_client.get_xcom_value(
            dag_id="scala_spark_example", task_id="spark_transform", run_id=run_id, key="return_value"
        )
        assert transform_xcom.get("value") == _SPARK_EXPECTED_TOTAL_REVENUE, (
            f"Expected spark_transform to aggregate total revenue {_SPARK_EXPECTED_TOTAL_REVENUE}, "
            f"got {transform_xcom.get('value')!r}"
        )

        load_xcom = self.airflow_client.get_xcom_value(
            dag_id="scala_spark_example", task_id="spark_load", run_id=run_id, key="return_value"
        )
        assert load_xcom.get("value") == _SPARK_EXPECTED_TOTAL_REVENUE, (
            f"Expected spark_load to return total revenue {_SPARK_EXPECTED_TOTAL_REVENUE}, "
            f"got {load_xcom.get('value')!r}"
        )
