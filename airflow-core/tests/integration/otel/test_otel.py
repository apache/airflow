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

import json
import logging
import os
import socket
import subprocess
import time
from typing import Any

import pytest
import requests
from sqlalchemy import func, select

from airflow._shared.timezones import timezone
from airflow.dag_processing.bundles.manager import DagBundlesManager
from airflow.dag_processing.dagbag import DagBag
from airflow.models import DagRun
from airflow.models.serialized_dag import SerializedDagModel
from airflow.models.taskinstance import TaskInstance
from airflow.serialization.definitions.dag import SerializedDAG
from airflow.utils.session import create_session
from airflow.utils.state import State

from tests_common.test_utils.dag import create_scheduler_dag
from tests_common.test_utils.otel_utils import (
    dump_airflow_metadata_db,
    extract_metrics_from_output,
)
from tests_common.test_utils.version_compat import AIRFLOW_V_3_0_PLUS, AIRFLOW_V_3_1_PLUS

log = logging.getLogger("integration.otel.test_otel")


def wait_for_otel_collector(host: str, port: int, timeout: int = 120) -> None:
    """
    Wait for the OTel collector to be reachable before running tests.

    This prevents flaky test failures caused by transient DNS resolution issues
    (e.g., 'Temporary failure in name resolution' for breeze-otel-collector).

    Note: If the collector is not reachable after timeout, logs a warning but
    does not fail - allows tests to run and fail naturally if needed.
    """
    deadline = time.monotonic() + timeout
    last_error = None
    while time.monotonic() < deadline:
        try:
            # Test DNS resolution and TCP connectivity
            with socket.create_connection((host, port), timeout=5):
                pass
            log.info("OTel collector at %s:%d is reachable.", host, port)
            return
        except (socket.gaierror, TimeoutError, OSError) as e:
            last_error = e
            log.debug(
                "OTel collector at %s:%d not reachable: %s. Retrying...",
                host,
                port,
                e,
            )
            time.sleep(2)
    log.warning(
        "OTel collector at %s:%d is not reachable after %ds. Last error: %s. "
        "Tests will proceed but may fail if collector is required.",
        host,
        port,
        timeout,
        last_error,
    )


def unpause_trigger_dag_and_get_run_id(dag_id: str) -> str:
    unpause_command = ["airflow", "dags", "unpause", dag_id]

    # Unpause the dag using the cli.
    subprocess.run(unpause_command, check=True, env=os.environ.copy())

    execution_date = timezone.utcnow()
    run_id = f"manual__{execution_date.isoformat()}"

    trigger_command = [
        "airflow",
        "dags",
        "trigger",
        dag_id,
        "--run-id",
        run_id,
        "--logical-date",
        execution_date.isoformat(),
    ]

    # Trigger the dag using the cli.
    subprocess.run(trigger_command, check=True, env=os.environ.copy())

    return run_id


def wait_for_dag_run(dag_id: str, run_id: str, max_wait_time: int):
    # max_wait_time, is the timeout for the DAG run to complete. The value is in seconds.
    start_time = timezone.utcnow().timestamp()

    while timezone.utcnow().timestamp() - start_time < max_wait_time:
        with create_session() as session:
            dag_run = session.scalar(
                select(DagRun).where(
                    DagRun.dag_id == dag_id,
                    DagRun.run_id == run_id,
                )
            )

            if dag_run is None:
                time.sleep(5)
                continue

            dag_run_state = dag_run.state
            log.debug("DAG Run state: %s.", dag_run_state)

            if dag_run_state in [State.SUCCESS, State.FAILED]:
                break
    return dag_run_state


def print_ti_output_for_dag_run(dag_id: str, run_id: str):
    breeze_logs_dir = "/root/airflow/logs"

    # For structured logs, the path is:
    #   '/root/airflow/logs/dag_id=.../run_id=.../task_id=.../attempt=1.log'
    # TODO: if older airflow versions start throwing errors,
    #   then check if the path needs to be adjusted to something like
    #   '/root/airflow/logs/<dag_id>/<task_id>/<run_id>/...'
    dag_run_path = os.path.join(breeze_logs_dir, f"dag_id={dag_id}", f"run_id={run_id}")

    for root, _dirs, files in os.walk(dag_run_path):
        for filename in files:
            if filename.endswith(".log"):
                full_path = os.path.join(root, filename)
                print("\n===== LOG FILE: %s - START =====\n", full_path)
                try:
                    with open(full_path) as f:
                        print(f.read())
                except Exception as e:
                    log.error("Could not read %s: %s", full_path, e)

                print("\n===== END =====\n")


@pytest.mark.integration("otel")
@pytest.mark.backend("postgres")
class TestOtelIntegration:
    """
    This test is using a ConsoleSpanExporter so that it can capture
    the spans from the stdout and run assertions on them.

    It can also be used with otel and jaeger for manual testing.
    To export the spans to otel and visualize them with jaeger,
    - start breeze with '--integration otel'
    - run on the shell 'export use_otel=true'
    - run the test
    - check 'http://localhost:36686/'

    To get a db dump on the stdout, run 'export log_level=debug'.
    """

    test_dir = os.path.dirname(os.path.abspath(__file__))
    dag_folder = os.path.join(test_dir, "dags")

    use_otel = os.getenv("use_otel", default="false")
    log_level = os.getenv("log_level", default="none")

    scheduler_command_args = [
        "airflow",
        "scheduler",
    ]

    apiserver_command_args = [
        "airflow",
        "api-server",
        "--port",
        "8080",
        "--daemon",
    ]

    dags: dict[str, SerializedDAG] = {}

    @classmethod
    def setup_class(cls):
        otel_host = "breeze-otel-collector"
        otel_port = 4318

        # Wait for OTel collector to be reachable before running tests.
        # This prevents flaky test failures caused by transient DNS resolution issues
        # during scheduler handoff (see https://github.com/apache/airflow/issues/61070).
        wait_for_otel_collector(otel_host, otel_port)

        os.environ["AIRFLOW__TRACES__OTEL_ON"] = "True"
        os.environ["OTEL_EXPORTER_OTLP_PROTOCOL"] = "http/protobuf"
        os.environ["OTEL_EXPORTER_OTLP_TRACES_ENDPOINT"] = "http://breeze-otel-collector:4318/v1/traces"

        os.environ["AIRFLOW__SCHEDULER__STANDALONE_DAG_PROCESSOR"] = "False"
        os.environ["AIRFLOW__SCHEDULER__PROCESSOR_POLL_INTERVAL"] = "2"

        # The heartrate is determined by the conf "AIRFLOW__SCHEDULER__SCHEDULER_HEARTBEAT_SEC".
        # By default, the heartrate is 5 seconds. Every iteration of the scheduler loop, checks the
        # time passed since the last heartbeat and if it was longer than the 5 second heartrate,
        # it performs a heartbeat update.
        # If there hasn't been a heartbeat for an amount of time longer than the
        # SCHEDULER_HEALTH_CHECK_THRESHOLD, then the scheduler is considered unhealthy.
        # Approximately, there is a scheduler heartbeat every 5-6 seconds. Set the threshold to 15.
        os.environ["AIRFLOW__SCHEDULER__SCHEDULER_HEALTH_CHECK_THRESHOLD"] = "15"

        os.environ["AIRFLOW__CORE__DAGS_FOLDER"] = f"{cls.dag_folder}"

        os.environ["AIRFLOW__CORE__LOAD_EXAMPLES"] = "False"
        os.environ["AIRFLOW__CORE__PLUGINS_FOLDER"] = "/dev/null"
        os.environ["AIRFLOW__CORE__UNIT_TEST_MODE"] = "False"

        if cls.log_level == "debug":
            log.setLevel(logging.DEBUG)

        # Reset the DB once at the beginning and serialize the dags.
        reset_command = ["airflow", "db", "reset", "--yes"]
        subprocess.run(reset_command, check=True, env=os.environ.copy())

        migrate_command = ["airflow", "db", "migrate"]
        subprocess.run(migrate_command, check=True, env=os.environ.copy())

        cls.dags = cls.serialize_and_get_dags()

    @classmethod
    def serialize_and_get_dags(cls) -> dict[str, SerializedDAG]:
        log.info("Serializing Dags from directory %s", cls.dag_folder)
        # Load DAGs from the dag directory.
        dag_bag = DagBag(dag_folder=cls.dag_folder, include_examples=False)

        dag_ids = dag_bag.dag_ids
        assert len(dag_ids) == 1

        dag_dict: dict[str, SerializedDAG] = {}
        with create_session() as session:
            for dag_id in dag_ids:
                dag = dag_bag.get_dag(dag_id)
                assert dag is not None, f"DAG with ID {dag_id} not found."
                # Sync the DAG to the database.
                if AIRFLOW_V_3_0_PLUS:
                    from airflow.models.dagbundle import DagBundleModel

                    count = session.scalar(
                        select(func.count())
                        .select_from(DagBundleModel)
                        .where(DagBundleModel.name == "testing")
                    )
                    if count == 0:
                        session.add(DagBundleModel(name="testing"))
                        session.commit()
                    SerializedDAG.bulk_write_to_db(
                        bundle_name="testing", bundle_version=None, dags=[dag], session=session
                    )
                    dag_dict[dag_id] = create_scheduler_dag(dag)
                else:
                    dag.sync_to_db(session=session)
                    dag_dict[dag_id] = dag
                # Manually serialize the dag and write it to the db to avoid a db error.
                if AIRFLOW_V_3_1_PLUS:
                    from airflow.serialization.serialized_objects import LazyDeserializedDAG

                    SerializedDagModel.write_dag(
                        LazyDeserializedDAG.from_dag(dag), bundle_name="testing", session=session
                    )
                else:
                    SerializedDagModel.write_dag(dag, bundle_name="testing", session=session)

            session.commit()

        TESTING_BUNDLE_CONFIG = [
            {
                "name": "testing",
                "classpath": "airflow.dag_processing.bundles.local.LocalDagBundle",
                "kwargs": {"path": f"{cls.dag_folder}", "refresh_interval": 1},
            }
        ]

        os.environ["AIRFLOW__DAG_PROCESSOR__DAG_BUNDLE_CONFIG_LIST"] = json.dumps(TESTING_BUNDLE_CONFIG)
        # Initial add
        manager = DagBundlesManager()
        manager.sync_bundles_to_db()

        return dag_dict

    def dag_execution_for_testing_metrics(self, capfd):
        # Metrics.
        os.environ["AIRFLOW__METRICS__OTEL_ON"] = "True"
        os.environ["OTEL_EXPORTER_OTLP_METRICS_ENDPOINT"] = "http://breeze-otel-collector:4318/v1/metrics"
        os.environ["OTEL_METRIC_EXPORT_INTERVAL"] = "5000"

        if self.use_otel != "true":
            os.environ["OTEL_METRICS_EXPORTER"] = "console"

        scheduler_process = None
        apiserver_process = None
        try:
            # Start the processes here and not as fixtures or in a common setup,
            # so that the test can capture their output.
            scheduler_process, apiserver_process = self.start_scheduler()

            dag_id = "otel_test_dag"

            assert len(self.dags) > 0
            dag = self.dags[dag_id]

            assert dag is not None

            run_id = unpause_trigger_dag_and_get_run_id(dag_id=dag_id)

            state = wait_for_dag_run(dag_id=dag_id, run_id=run_id, max_wait_time=90)
            assert state == State.SUCCESS, f"Dag run did not complete successfully. Final state: {state}."

            # The ti span_status is updated while processing the executor events,
            # which is after the dag_run state has been updated.
            time.sleep(10)

            task_dict = dag.task_dict
            task_dict_ids = task_dict.keys()

            for task_id in task_dict_ids:
                ti = self._get_ti(dag_id, run_id, task_id)
                assert ti is not None
                assert ti.state == State.SUCCESS

            print_ti_output_for_dag_run(dag_id=dag_id, run_id=run_id)
        finally:
            # Terminate the processes.

            scheduler_process.terminate()
            scheduler_process.wait()

            scheduler_status = scheduler_process.poll()
            assert scheduler_status is not None, (
                "The scheduler_1 process status is None, which means that it hasn't terminated as expected."
            )

            apiserver_process.terminate()
            apiserver_process.wait()

            apiserver_status = apiserver_process.poll()
            assert apiserver_status is not None, (
                "The apiserver process status is None, which means that it hasn't terminated as expected."
            )

        out, err = capfd.readouterr()
        log.info("out-start --\n%s\n-- out-end", out)
        log.info("err-start --\n%s\n-- err-end", err)

        return out, dag

    def _get_ti(self, dag_id: str, run_id: str, task_id: str) -> Any | None:
        with create_session() as session:
            ti = session.scalar(
                select(TaskInstance).where(
                    TaskInstance.task_id == task_id,
                    TaskInstance.dag_id == dag_id,
                    TaskInstance.run_id == run_id,
                )
            )
        return ti

    @pytest.mark.parametrize(
        ("legacy_names_on_bool", "legacy_names_exported"),
        [
            pytest.param(True, True, id="export_legacy_names"),
            pytest.param(False, False, id="dont_export_legacy_names"),
        ],
    )
    def test_export_legacy_metric_names(self, legacy_names_on_bool, legacy_names_exported, capfd):
        assert isinstance(legacy_names_on_bool, bool)
        os.environ["AIRFLOW__METRICS__LEGACY_NAMES_ON"] = str(legacy_names_on_bool)
        out, dag = self.dag_execution_for_testing_metrics(capfd)
        if self.use_otel != "true":
            assert isinstance(legacy_names_exported, bool)
            output_lines = out.splitlines()
            metrics_dict = extract_metrics_from_output(output_lines)

            legacy_metric_names = [
                f"airflow.dagrun.dependency-check.{dag.dag_id}",
                f"airflow.dagrun.duration.success.{dag.dag_id}",
            ]

            for task_id in dag.task_ids:
                legacy_metric_names.append(f"airflow.dag.{dag.dag_id}.{task_id}.scheduled_duration")

            new_metric_names = [
                "airflow.dagrun.dependency-check",
                "airflow.dagrun.duration.success",
                "airflow.task.scheduled_duration",
            ]

            assert set(new_metric_names).issubset(metrics_dict.keys())

            if legacy_names_exported:
                assert set(legacy_metric_names).issubset(metrics_dict.keys())

    def test_export_metrics_during_process_shutdown(self, capfd):
        out, dag = self.dag_execution_for_testing_metrics(capfd)

        if self.use_otel != "true":
            # Test the metrics from the output.
            metrics_to_check = [
                "airflow.ti_successes",
                "airflow.operator_successes",
                "airflow.executor.running_tasks",
                "airflow.executor.queued_tasks",
                "airflow.executor.open_slots",
            ]
            output_lines = out.splitlines()

            metrics_dict = extract_metrics_from_output(output_lines)

            assert set(metrics_to_check).issubset(metrics_dict.keys())

    @pytest.mark.execution_timeout(90)
    def test_dag_execution_succeeds(self, capfd):
        """The same scheduler will start and finish the dag processing."""
        scheduler_process = None
        apiserver_process = None
        try:
            # Start the processes here and not as fixtures or in a common setup,
            # so that the test can capture their output.
            scheduler_process, apiserver_process = self.start_scheduler()

            dag_id = "otel_test_dag"

            assert len(self.dags) > 0
            dag = self.dags[dag_id]

            assert dag is not None

            run_id = unpause_trigger_dag_and_get_run_id(dag_id=dag_id)

            # Skip the span_status check.
            wait_for_dag_run(dag_id=dag_id, run_id=run_id, max_wait_time=90)

            # The ti span_status is updated while processing the executor events,
            # which is after the dag_run state has been updated.
            time.sleep(10)

            print_ti_output_for_dag_run(dag_id=dag_id, run_id=run_id)
        finally:
            if self.log_level == "debug":
                with create_session() as session:
                    dump_airflow_metadata_db(session)

            # Terminate the processes.
            scheduler_process.terminate()
            scheduler_process.wait()

            scheduler_status = scheduler_process.poll()
            assert scheduler_status is not None, (
                "The scheduler_1 process status is None, which means that it hasn't terminated as expected."
            )

            apiserver_process.terminate()
            apiserver_process.wait()

            apiserver_status = apiserver_process.poll()
            assert apiserver_status is not None, (
                "The apiserver process status is None, which means that it hasn't terminated as expected."
            )

        out, err = capfd.readouterr()
        log.info("out-start --\n%s\n-- out-end", out)
        log.info("err-start --\n%s\n-- err-end", err)

        host = "jaeger"
        service_name = os.environ.get("OTEL_SERVICE_NAME", "test")
        r = requests.get(f"http://{host}:16686/api/traces?service={service_name}")
        data = r.json()

        trace = data["data"][-1]
        spans = trace["spans"]

        def get_span_hierarchy():
            spans_dict = {x["spanID"]: x for x in spans}

            def get_parent_span_id(span):
                parents = [x["spanID"] for x in span["references"] if x["refType"] == "CHILD_OF"]
                if parents:
                    parent_id = parents[0]
                    return spans_dict[parent_id]["operationName"]

            nested = {x["operationName"]: get_parent_span_id(x) for x in spans}
            return nested

        nested = get_span_hierarchy()
        assert nested == {
            "sub_span1": "task_run.task1",
            "task_run.task1": "dag_run.otel_test_dag",
            "dag_run.otel_test_dag": None,
        }

    def start_scheduler(self):
        scheduler_process = subprocess.Popen(
            self.scheduler_command_args,
            env=os.environ.copy(),
            stdout=None,
            stderr=None,
        )

        apiserver_process = subprocess.Popen(
            self.apiserver_command_args,
            env=os.environ.copy(),
            stdout=None,
            stderr=None,
        )

        # Wait to ensure both processes have started.
        time.sleep(10)

        return scheduler_process, apiserver_process
