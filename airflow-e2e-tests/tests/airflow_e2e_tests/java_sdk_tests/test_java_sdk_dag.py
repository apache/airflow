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

Each Dag is triggered once by its own module-scoped fixture. Tests that inspect
the same Dag therefore share one completed run instead of repeating the full
coordinator workflow for every assertion.

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
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import TYPE_CHECKING

import pytest

from airflow_e2e_tests.e2e_test_utils.clients import AirflowClient

if TYPE_CHECKING:
    from collections.abc import Callable

# The Java extract task sleeps 6 s + coordinator startup; allow plenty of room.
_JAVA_TASK_TIMEOUT = 600
# Each Scala task spins up its own local SparkSession; allow generous time for
# three sequential JVM + Spark startups in a constrained CI container.
_SPARK_TASK_TIMEOUT = 1200
# Logs can lag slightly behind the task reaching a terminal state.
_LOG_FETCH_TIMEOUT = 60

_ANNOTATION_DAG_ID = "java_annotation_example"
_XCOM_CASTING_DAG_ID = "java_xcom_casting_example"
_SCALA_SPARK_DAG_ID = "scala_spark_example"


@dataclass
class _CompletedRun:
    """A completed Dag run shared by all assertions for that Dag."""

    client: AirflowClient
    dag_id: str
    run_id: str | None = None
    state: str | None = None
    ti_map: dict[str, dict] = field(default_factory=dict)
    ti_states: dict[str, str] = field(default_factory=dict)
    _error: Exception | None = field(default=None, repr=False)

    def _raise_if_failed(self) -> None:
        if self._error is not None:
            raise self._error

    def get_task_instance(self, task_id: str) -> dict:
        self._raise_if_failed()
        return self.ti_map.get(task_id, {})

    def get_xcom(self, task_id: str, key: str = "return_value"):
        self._raise_if_failed()
        if self.run_id is None:
            raise RuntimeError(f"Dag {self.dag_id!r} did not produce a run ID")
        return self.client.get_xcom_value(
            dag_id=self.dag_id,
            task_id=task_id,
            run_id=self.run_id,
            key=key,
        ).get("value")

    def wait_for_log_record(
        self, task_id: str, try_number: int, match: Callable[[dict], bool]
    ) -> tuple[dict | None, list[dict]]:
        """Poll task logs until a record matching *match* appears."""
        self._raise_if_failed()
        if self.run_id is None:
            raise RuntimeError(f"Dag {self.dag_id!r} did not produce a run ID")
        deadline = time.monotonic() + _LOG_FETCH_TIMEOUT
        records: list[dict] = []
        while True:
            resp = self.client.get_task_logs(
                dag_id=self.dag_id,
                run_id=self.run_id,
                task_id=task_id,
                try_number=try_number,
            )
            records = [entry for entry in resp.get("content", []) if isinstance(entry, dict)]
            record = next((candidate for candidate in records if match(candidate)), None)
            if record is not None or time.monotonic() > deadline:
                return record, records
            time.sleep(3)


def _trigger_and_wait_for_dag(dag_id: str, timeout: int) -> _CompletedRun:
    client = AirflowClient()
    run_id = None
    try:
        resp = client.trigger_dag(dag_id, json={"logical_date": datetime.now(timezone.utc).isoformat()})
        run_id = resp["dag_run_id"]
        state = client.wait_for_dag_run(dag_id=dag_id, run_id=run_id, timeout=timeout)
        ti_resp = client.get_task_instances(dag_id=dag_id, run_id=run_id)
        ti_map = {ti["task_id"]: ti for ti in ti_resp.get("task_instances", [])}
        ti_states = {task_id: ti.get("state") for task_id, ti in ti_map.items()}
    except Exception as error:
        # The E2E report captures call-phase failures, so defer fixture setup
        # errors until a test first reads the shared result.
        return _CompletedRun(client=client, dag_id=dag_id, run_id=run_id, _error=error)
    return _CompletedRun(
        client=client,
        dag_id=dag_id,
        run_id=run_id,
        state=state,
        ti_map=ti_map,
        ti_states=ti_states,
    )


@pytest.fixture(scope="module")
def annotation_example_run() -> _CompletedRun:
    """Trigger the annotation example once for all of its assertions."""
    return _trigger_and_wait_for_dag(_ANNOTATION_DAG_ID, _JAVA_TASK_TIMEOUT)


@pytest.fixture(scope="module")
def xcom_casting_example_run() -> _CompletedRun:
    """Trigger the XCom casting example once for all of its assertions."""
    return _trigger_and_wait_for_dag(_XCOM_CASTING_DAG_ID, _JAVA_TASK_TIMEOUT)


@pytest.fixture(scope="module")
def scala_spark_example_run() -> _CompletedRun:
    """Trigger the Scala Spark example once for all of its assertions."""
    return _trigger_and_wait_for_dag(_SCALA_SPARK_DAG_ID, _SPARK_TASK_TIMEOUT)


class TestJavaSDKAnnotationExample:
    """Verify the annotation-based Java SDK example executes correctly."""

    def test_java_tasks_execute_successfully(self, annotation_example_run: _CompletedRun):
        """Both Java stubs in the annotation example must succeed."""
        for task_id in ("extract", "transform"):
            task_instance = annotation_example_run.get_task_instance(task_id)
            assert task_instance.get("state") == "success", (
                f"Java {task_id!r} task did not succeed.\n"
                f"  task state : {task_instance.get('state')!r}\n"
                f"  dag state  : {annotation_example_run.state!r}\n"
                f"  all tasks  : {annotation_example_run.ti_states}"
            )

    def test_transform_xcom_is_numeric_timestamp(self, annotation_example_run: _CompletedRun):
        """The value returned by the Java 'transform' task must be a positive integer."""
        value = annotation_example_run.get_xcom("transform")
        assert isinstance(value, int), (
            f"Expected 'transform' XCom to be an integer (millisecond timestamp), got {value!r}"
        )
        assert value > 0, (
            f"Expected 'transform' XCom to be a positive integer (millisecond timestamp), got {value!r}"
        )

    def test_concurrent_client_calls_succeed(self, annotation_example_run: _CompletedRun):
        """A Java task calling the client from many threads must succeed."""
        concurrent_ti = annotation_example_run.get_task_instance("concurrent")

        assert concurrent_ti.get("state") == "success", (
            f"Java 'concurrent' task did not succeed.\n"
            f"  task state : {concurrent_ti.get('state')!r}\n"
            f"  dag state  : {annotation_example_run.state!r}\n"
            f"  all tasks  : {annotation_example_run.ti_states}"
        )

    def test_load_retried_then_succeeded(self, annotation_example_run: _CompletedRun):
        """``load`` fails once (UP_FOR_RETRY) then succeeds on the second attempt.

        The Java coordinator must return ``RetryTask`` (not terminal ``FAILED``)
        when the task throws with retries left, so the supervisor re-runs it. The
        end state is ``success`` reached on ``try_number`` 2.
        """
        load_ti = annotation_example_run.get_task_instance("load")

        assert load_ti.get("state") == "success", (
            f"Java 'load' task should succeed on retry.\n"
            f"  task state : {load_ti.get('state')!r}\n"
            f"  dag state  : {annotation_example_run.state!r}\n"
            f"  all tasks  : {annotation_example_run.ti_states}"
        )
        assert load_ti.get("try_number") == 2, (
            f"Java 'load' task should have run twice (fail then retry); "
            f"try_number={load_ti.get('try_number')!r}, ti: {load_ti}"
        )

    def test_application_logs_preserve_their_level(self, annotation_example_run: _CompletedRun):
        """A Java task's SLF4J ``logger.info`` must reach the UI as INFO, not ERROR.

        Without the SDK's SLF4J binding the application's logs fall through to
        stderr and the supervisor tags every line ERROR. The binding routes them
        over the logs socket carrying the real level instead.
        """
        # The log under test is emitted only if transform actually ran; assert it
        # succeeded and fetch the attempt that produced the logs (transform does
        # not retry, but read try_number rather than assuming attempt 1).
        transform_ti = annotation_example_run.get_task_instance("transform")
        assert transform_ti.get("state") == "success", (
            f"Java 'transform' task must succeed to emit the log under test.\n"
            f"  task state : {transform_ti.get('state')!r}\n"
            f"  dag state  : {annotation_example_run.state!r}\n"
            f"  all tasks  : {annotation_example_run.ti_states}"
        )

        # transform logs `logger.info("Got variable {}", variable)` -> "Got variable 123".
        record, records = annotation_example_run.wait_for_log_record(
            "transform",
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


class TestJavaSDKUninstantiableTask:
    """Verify a task class the runner cannot instantiate fails with an actionable log.

    The deliberately broken task classes live in the java-test-bundle fixture
    project (served on the dedicated "java-test" queue), not in the user-facing
    example bundle.
    """

    @pytest.mark.parametrize(
        ("task_id", "expected_task_class"),
        [
            (
                "missing_no_arg_constructor",
                "org.apache.airflow.e2e.TestBundleBuilder$MissingNoArgConstructor",
            ),
            (
                "non_static_inner",
                "org.apache.airflow.e2e.TestBundleBuilder$NonStaticInner",
            ),
        ],
    )
    def test_uninstantiable_task_fails_with_actionable_error(self, task_id: str, expected_task_class: str):
        """A task class the runner cannot instantiate fails with a clear log message."""
        completed_run = _trigger_and_wait_for_dag("java_uninstantiable", _JAVA_TASK_TIMEOUT)
        ti = completed_run.get_task_instance(task_id)

        assert ti.get("state") == "failed", (
            f"Java {task_id!r} task should fail cleanly.\n"
            f"  task state : {ti.get('state')!r}\n"
            f"  dag state  : {completed_run.state!r}\n"
            f"  all tasks  : {completed_run.ti_states}"
        )

        record, records = completed_run.wait_for_log_record(
            task_id,
            ti.get("try_number", 1),
            lambda r: str(r.get("event", "")).startswith("Cannot instantiate task class"),
        )
        assert record is not None, (
            f"{task_id!r} should emit a 'Cannot instantiate task class' record; "
            f"events seen: {[r.get('event') for r in records]}"
        )
        assert record.get("event") == (
            "Cannot instantiate task class. "
            "A task class must be public, concrete, and declare a public no-argument constructor"
        ), f"instantiation error should carry the full actionable message; record: {record}"
        assert str(record.get("level", "")).lower() == "error", (
            f"instantiation error should be logged as ERROR, got {record.get('level')!r}; record: {record}"
        )
        assert record.get("taskClass") == expected_task_class, (
            f"instantiation error should name the offending class {expected_task_class!r}, "
            f"got {record.get('taskClass')!r}; record: {record}"
        )


class TestJavaSDKXComCastingExample:
    """Verify numeric XCom values are cast across declared Java types."""

    def test_numeric_xcom_casting(self, xcom_casting_example_run: _CompletedRun):
        """Numeric XComs are read across declared types (int -> long -> double, and a wire
        double back as a float), and a boxed param stays null when its XCom is absent."""
        for task_id in ("widen_to_double", "consume_nullable", "consume_float"):
            task_instance = xcom_casting_example_run.get_task_instance(task_id)
            assert task_instance.get("state") == "success", (
                f"Java {task_id!r} task did not succeed.\n"
                f"  task state : {task_instance.get('state')!r}\n"
                f"  dag state  : {xcom_casting_example_run.state!r}\n"
                f"  all tasks  : {xcom_casting_example_run.ti_states}"
            )


# Mirror the fixed dataset that is the single source of truth in
# ScalaSparkExample.scala (``SalesData.rows``): 5 sales rows whose amounts
# (100+200+300+150+250) sum to 1000. Keep these in sync if that dataset changes.
_SPARK_EXPECTED_ROW_COUNT = 5
_SPARK_EXPECTED_TOTAL_REVENUE = 1000


class TestJavaSDKScalaSparkExample:
    """Verify the Scala + Apache Spark ETL example bundle executes correctly."""

    def test_spark_etl_pipeline(self, scala_spark_example_run: _CompletedRun):
        """The three Scala Spark stubs run in order and pass scalar results via XCom.

        Each runs in its own JVM through ``JavaCoordinator`` with real Spark.
        """
        for task_id in ("spark_extract", "spark_transform", "spark_load"):
            task_instance = scala_spark_example_run.get_task_instance(task_id)
            assert task_instance.get("state") == "success", (
                f"Scala Spark {task_id!r} task did not succeed.\n"
                f"  task state : {task_instance.get('state')!r}\n"
                f"  dag state  : {scala_spark_example_run.state!r}\n"
                f"  all tasks  : {scala_spark_example_run.ti_states}"
            )

        extract_xcom = scala_spark_example_run.get_xcom("spark_extract")
        assert extract_xcom == _SPARK_EXPECTED_ROW_COUNT, (
            f"Expected spark_extract to push row count {_SPARK_EXPECTED_ROW_COUNT}, got {extract_xcom!r}"
        )

        transform_xcom = scala_spark_example_run.get_xcom("spark_transform")
        assert transform_xcom == _SPARK_EXPECTED_TOTAL_REVENUE, (
            f"Expected spark_transform to aggregate total revenue {_SPARK_EXPECTED_TOTAL_REVENUE}, "
            f"got {transform_xcom!r}"
        )

        load_xcom = scala_spark_example_run.get_xcom("spark_load")
        assert load_xcom == _SPARK_EXPECTED_TOTAL_REVENUE, (
            f"Expected spark_load to return total revenue {_SPARK_EXPECTED_TOTAL_REVENUE}, got {load_xcom!r}"
        )
