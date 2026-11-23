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
"""
Performance integration test for the scheduler.
Creates a backlog of Dag runs, starts the Airflow processes and measures how long they take to
finish it, once per test case of the code under test. See README.md.
"""

from __future__ import annotations

import json
import logging
import os
import subprocess
import sys
import time
from typing import TYPE_CHECKING, Any

import pytest

from airflow.configuration import conf
from airflow.utils.state import State

from tests_common.test_utils.integration_setup import (
    OTEL_COLLECTOR_HOST,
    OTEL_COLLECTOR_PORT,
    ProgressReporter,
    compute_usable_cpu_count,
    create_queued_dag_runs,
    ensure_postgres_max_connections,
    export_setting,
    print_to_terminal,
    serialize_and_get_dags,
    start_process,
    start_processes,
    terminate_processes,
    wait_for_dag_runs,
    wait_for_otel_collector,
)

if TYPE_CHECKING:
    from airflow.serialization.definitions.dag import SerializedDAG

log = logging.getLogger("integration.perf.test_perf")

TEST_DIR = os.path.dirname(os.path.abspath(__file__))
DAG_FOLDER = os.path.join(TEST_DIR, "dags")
POLL_USAGE_SCRIPT = os.path.join(TEST_DIR, "poll_usage.py")

# "true" or "false" per test case, read by the code under test.
NEW_CODE_ENV_VAR = "PERF_TEST__NEW_CODE"

# Per Airflow process; Airflow's defaults times the API server workers exceed Postgres's limit.
DB_POOL_SIZE = 3
DB_MAX_OVERFLOW = 5
# Per Celery child, which runs one task at a time.
RESULT_BACKEND_POOL_SIZE = 1
RESULT_BACKEND_MAX_OVERFLOW = 1
# psql, the test process and the usage poller.
DB_CONNECTION_HEADROOM = 20

CPUS = compute_usable_cpu_count()

# The first three are process counts; the rest are Airflow settings, None keeps Airflow's default.
PROFILES: dict[str, dict[str, Any]] = {
    # Sized so the scheduler is the only constraint: every other cap is far above what the workers
    # can execute.
    "dynamic": {
        # More slots per worker rather than more workers: each worker process costs connections.
        "worker_count": 3,
        "worker_concurrency": max(2, CPUS * 2 // 3),
        # One worker saturates well before the scheduler does.
        "api_server_workers": max(4, CPUS // 3),
        "parallelism": 4096,
        "default_pool_task_slot_count": 4096,
        "max_active_tasks_per_dag": 4096,
        # Many runs of one Dag; the default of 16 would throttle them.
        "max_active_runs_per_dag": 4096,
        "max_tis_per_query": None,
    },
    # Fill in as needed; the process counts start at the tools' defaults.
    "custom": {
        "worker_count": 1,
        "worker_concurrency": CPUS,
        "api_server_workers": conf.getint("api", "workers"),
        "parallelism": None,
        "default_pool_task_slot_count": None,
        "max_active_tasks_per_dag": None,
        "max_active_runs_per_dag": None,
        "max_tis_per_query": None,
    },
}


def compute_required_db_connections(sizing: dict[str, Any]) -> int:
    """Peak connections the deployment can open against the metadata DB."""
    # Tasks use the Execution API and never connect.
    scheduler = 1
    airflow_processes = scheduler + sizing["api_server_workers"] + sizing["worker_count"]
    per_airflow_process = DB_POOL_SIZE + DB_MAX_OVERFLOW
    # One result-backend pool per Celery child.
    celery_children = sizing["worker_count"] * sizing["worker_concurrency"]
    per_celery_child = RESULT_BACKEND_POOL_SIZE + RESULT_BACKEND_MAX_OVERFLOW
    return (
        airflow_processes * per_airflow_process + celery_children * per_celery_child + DB_CONNECTION_HEADROOM
    )


@pytest.mark.integration("otel")
@pytest.mark.integration("redis")
@pytest.mark.backend("postgres")
class TestPerformance:
    """One environment per class, one deployment per test case."""

    profile = os.environ.get("PERF_TEST__PROFILE", "dynamic")  # key of PROFILES; sizes the deployment
    sizing: dict[str, Any]
    dags: dict[str, SerializedDAG] = {}

    @classmethod
    def setup_class(cls):
        if cls.profile not in PROFILES:
            pytest.fail(f"profile {cls.profile!r} is not one of {sorted(PROFILES)}")
        cls.sizing = PROFILES[cls.profile]
        log.info("profile %s: %s", cls.profile, cls.sizing)

        wait_for_otel_collector(OTEL_COLLECTOR_HOST, OTEL_COLLECTOR_PORT)
        # Exceeding the limit kills the scheduler mid-run.
        ensure_postgres_max_connections(compute_required_db_connections(cls.sizing))

        # The pytest plugin strips AIRFLOW__* variables; scheduler and API server must share the secret.
        os.environ["AIRFLOW__API_AUTH__JWT_SECRET"] = "test-secret-key-for-testing"
        os.environ["AIRFLOW__API_AUTH__JWT_ISSUER"] = "airflow"

        os.environ["AIRFLOW__CORE__EXECUTOR"] = "CeleryExecutor"
        os.environ["AIRFLOW__CORE__DAGS_FOLDER"] = DAG_FOLDER
        os.environ["AIRFLOW__CORE__LOAD_EXAMPLES"] = "False"
        os.environ["AIRFLOW__CORE__PLUGINS_FOLDER"] = "/dev/null"
        os.environ["AIRFLOW__CORE__UNIT_TEST_MODE"] = "False"

        os.environ["AIRFLOW__DATABASE__SQL_ALCHEMY_POOL_SIZE"] = str(DB_POOL_SIZE)
        os.environ["AIRFLOW__DATABASE__SQL_ALCHEMY_MAX_OVERFLOW"] = str(DB_MAX_OVERFLOW)
        os.environ["AIRFLOW__CELERY__RESULT_BACKEND_SQLALCHEMY_ENGINE_OPTIONS"] = json.dumps(
            {"pool_size": RESULT_BACKEND_POOL_SIZE, "max_overflow": RESULT_BACKEND_MAX_OVERFLOW}
        )
        # The default spawns a ProcessPoolExecutor per publish batch, on the scheduler's heartbeat.
        os.environ["AIRFLOW__CELERY__SYNC_PARALLELISM"] = "1"
        # The default of one second fails publishes behind a busy broker.
        os.environ["AIRFLOW__CELERY__OPERATION_TIMEOUT"] = "300"

        # Before db migrate, which creates the default pool with this slot count.
        export_setting("AIRFLOW__CORE__PARALLELISM", cls.sizing["parallelism"])
        export_setting(
            "AIRFLOW__CORE__DEFAULT_POOL_TASK_SLOT_COUNT", cls.sizing["default_pool_task_slot_count"]
        )
        export_setting("AIRFLOW__CORE__MAX_ACTIVE_TASKS_PER_DAG", cls.sizing["max_active_tasks_per_dag"])
        export_setting("AIRFLOW__CORE__MAX_ACTIVE_RUNS_PER_DAG", cls.sizing["max_active_runs_per_dag"])
        export_setting("AIRFLOW__SCHEDULER__MAX_TIS_PER_QUERY", cls.sizing["max_tis_per_query"])

        os.environ["AIRFLOW__METRICS__OTEL_ON"] = "True"
        os.environ["AIRFLOW__METRICS__OTEL_HOST"] = OTEL_COLLECTOR_HOST
        os.environ["AIRFLOW__METRICS__OTEL_PORT"] = str(OTEL_COLLECTOR_PORT)
        # breeze's OTEL_METRIC_EXPORT_INTERVAL=30000 overrides the Airflow option; Prometheus still
        # scrapes every 30s.
        os.environ["AIRFLOW__METRICS__OTEL_INTERVAL_MILLISECONDS"] = "1000"
        os.environ["OTEL_METRIC_EXPORT_INTERVAL"] = "1000"
        # The dashboard reads the tagged names; legacy names add one series family per task.
        os.environ["AIRFLOW__METRICS__LEGACY_NAMES_ON"] = "False"
        os.environ["AIRFLOW__TRACES__OTEL_ON"] = "True"
        os.environ["OTEL_EXPORTER_OTLP_PROTOCOL"] = "http/protobuf"
        os.environ["OTEL_EXPORTER_OTLP_TRACES_ENDPOINT"] = (
            f"http://{OTEL_COLLECTOR_HOST}:{OTEL_COLLECTOR_PORT}/v1/traces"
        )

        # Once per class: the scheduler ignores finished runs, and run ids carry the test case.
        subprocess.run(["airflow", "db", "reset", "--yes"], check=True, env=os.environ.copy())
        subprocess.run(["airflow", "db", "migrate"], check=True, env=os.environ.copy())

        # Every Dag in the folder; the test picks by id.
        cls.dags = serialize_and_get_dags(dag_folder=DAG_FOLDER)
        assert cls.dags, f"no Dags found in {DAG_FOLDER}"

    def start_usage_poller(self, processes: dict[str, subprocess.Popen]) -> subprocess.Popen:
        """Start poll_usage.py, which pushes CPU, memory and worker slot gauges for ``processes``."""
        slot_budget = self.sizing["worker_count"] * self.sizing["worker_concurrency"]
        command = [sys.executable, POLL_USAGE_SCRIPT, "--slot-budget", str(slot_budget)]
        for role, process in processes.items():
            command.append(f"{role}={process.pid}")
        # Own process group, or terminate_processes would kill pytest with it.
        return start_process(command)

    def execute_dag_runs(
        self,
        *,
        run_id_prefix: str,
        dag_ids: list[str],
        dag_run_count: int,
        run_conf: dict[str, Any],
        timeout_seconds: int,
    ) -> None:
        """Create the Dag runs, start the processes, wait for the runs to finish, stop the processes."""
        dags = {dag_id: self.dags[dag_id] for dag_id in dag_ids}
        # Created before any process exists, so every test case starts from the same rows, all
        # eligible at once, and CLI triggering cannot cap the live runs at its own rate. This is the
        # scheduler under full load, not a production trickle.
        runs = create_queued_dag_runs(
            dags, run_id_prefix=run_id_prefix, dag_run_count=dag_run_count, run_conf=run_conf
        )
        processes, started_at = start_processes(
            worker_count=self.sizing["worker_count"],
            worker_concurrency=self.sizing["worker_concurrency"],
            api_server_workers=self.sizing["api_server_workers"],
        )
        try:
            processes["usage_poller"] = self.start_usage_poller(processes)
            # A guard only: unfinished runs fail below.
            with ProgressReporter(runs):
                states = wait_for_dag_runs(runs, max_wait_time=timeout_seconds)
            elapsed_seconds = time.monotonic() - started_at

            exited = {}
            for role, process in processes.items():
                if process.poll() is not None:
                    exited[role] = process.returncode
            assert not exited, f"processes exited, with these codes: {exited}"

            not_succeeded = {run: state for run, state in states.items() if state != State.SUCCESS}
            assert not not_succeeded, (
                f"{len(not_succeeded)} of {len(runs)} Dag runs are not successful after {timeout_seconds}s: "
                f"{not_succeeded}"
            )
        finally:
            terminate_processes(processes)
        # After the processes are stopped, so their shutdown logging does not bury it.
        print_to_terminal(f"\n{run_id_prefix}: {len(runs)} Dag runs finished in {elapsed_seconds:.0f}s\n")

    @pytest.mark.parametrize(
        "new_code",
        [
            pytest.param(False, id="original_code"),
            pytest.param(True, id="new_code"),
        ],
    )
    def test_scheduler_load(self, new_code: bool):
        """Run many Dag runs of the fan-out Dag at once, with the original code and then the new code."""
        test_case = "new_code" if new_code else "original_code"
        # Inherited by every process started below.
        os.environ[NEW_CODE_ENV_VAR] = str(new_code).lower()

        # The load; each variable overrides its default for one run.
        dag_run_count = int(os.environ.get("PERF_TEST__DAG_RUN_COUNT", "5"))  # queued runs created up front
        mapped_task_count = int(os.environ.get("PERF_TEST__MAPPED_TASK_COUNT", "20"))  # instances per stage
        # Give up on the test case after this.
        timeout_seconds = int(os.environ.get("PERF_TEST__TIMEOUT_SECONDS", "3600"))
        print_to_terminal(
            f"\n\n{test_case}: {dag_run_count} runs of perf_dag, {mapped_task_count} mapped instances per stage, "
            f"timeout {timeout_seconds}s, profile {self.profile}\n"
        )

        self.execute_dag_runs(
            # Run ids carry the test case name.
            run_id_prefix=test_case,
            dag_ids=["perf_dag"],
            dag_run_count=dag_run_count,
            run_conf={"mapped_task_count": mapped_task_count},
            timeout_seconds=timeout_seconds,
        )
