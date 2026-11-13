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
Performance harness: how long an Airflow deployment, started by the test inside breeze, takes to
drain a backlog of dag runs, once per test case of the code under test. See README.md.
"""

from __future__ import annotations

import json
import logging
import os
import subprocess
import sys
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
REPORT_SCRIPT = os.path.join(TEST_DIR, "report_run.py")

# "true" or "false" per test case; the README shows the one-line check the code under test does.
NEW_CODE_ENV_VAR = "PERF_TEST__NEW_CODE"

# Every Airflow process gets this SQLAlchemy pool. Airflow's defaults, multiplied by the API
# server worker count, reach the server's connection limit and kill the scheduler mid-run with
# "sorry, too many clients already"; much lower trades that crash for pool starvation, where a
# checkout blocks for pool_timeout instead.
DB_POOL_SIZE = 3
DB_MAX_OVERFLOW = 5
# Celery builds its result-backend engine per forked child from a separate option, the one pool
# that scales with worker concurrency. A child runs one task at a time and writes one result.
RESULT_BACKEND_POOL_SIZE = 1
RESULT_BACKEND_MAX_OVERFLOW = 1
# For psql, the test process and the sidecar, on top of the computed fleet.
DB_CONNECTION_HEADROOM = 20

CPUS = compute_usable_cpu_count()

# Concurrency budget of the deployment the test starts. The first three keys are processes the
# test starts, so they are always numbers. The rest are Airflow settings; None keeps Airflow's
# default. Select one with ``profile`` below or PERF_TEST__PROFILE in the shell.
PROFILES: dict[str, dict[str, Any]] = {
    # Sized from the machine so that the scheduler is the only constraint. Every cap that is not
    # the scheduler, that is pool slots, parallelism, per-Dag task and run limits, sits far above
    # what the workers can execute, so a task that waits is waiting on the scheduler and not on a
    # slot. Worker slots deliberately exceed the CPU count: an empty task is a process spawn plus
    # Execution API round-trips, so a slot spends most of its life blocked rather than computing.
    # Average task execution time is the tripwire for oversubscription: if it climbs with the slot
    # count, the box is saturated and every measurement inflates uniformly.
    "dynamic": {
        # Concurrency rather than more worker processes on purpose: the Celery result backend is
        # Postgres, so each extra process costs connections without adding throughput.
        "worker_count": 3,
        "worker_concurrency": max(2, CPUS * 2 // 3),
        # One API server worker serves every task's state transitions and heartbeats; it
        # saturates well before the scheduler does and inflates all task durations uniformly.
        "api_server_workers": max(4, CPUS // 3),
        "parallelism": 4096,
        "default_pool_task_slot_count": 4096,
        "max_active_tasks_per_dag": 4096,
        # The backlog is many runs of one Dag; Airflow's default of 16 would start them in
        # batches and measure that throttle instead of the scheduler.
        "max_active_runs_per_dag": 4096,
        "max_tis_per_query": None,
    },
    # Fill in what a specific machine or experiment needs. The process counts start at the
    # tools' own defaults.
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
    # Tasks go through the Execution API and never connect, so the peak is set by the process
    # count and the pool caps, not by the task count.
    scheduler = 1
    airflow_processes = scheduler + sizing["api_server_workers"] + sizing["worker_count"]
    per_airflow_process = DB_POOL_SIZE + DB_MAX_OVERFLOW
    # Every Celery child builds its own result-backend engine, the one pool that scales with slots.
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
        # Running past the server's connection limit kills the scheduler mid-run with "too many
        # clients already"; settling it here, before anything starts, is far cheaper.
        ensure_postgres_max_connections(compute_required_db_connections(cls.sizing))

        # The pytest plugin strips AIRFLOW__*__* variables, including the JWT secret breeze sets.
        # The scheduler and the API server must share one, or token verification fails.
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
        # The default, zero for the CPU count, publishes any batch of two or more workloads
        # through a ProcessPoolExecutor created per call: hundreds of milliseconds of fork and
        # shutdown on the scheduler's synchronous heartbeat, for an I/O-bound publish.
        os.environ["AIRFLOW__CELERY__SYNC_PARALLELISM"] = "1"
        # Publishes queue up behind a busy broker; the default one-second timeout would fail them.
        os.environ["AIRFLOW__CELERY__OPERATION_TIMEOUT"] = "300"

        # Before `db migrate` below: the default pool row is created with the slot count.
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
        # Export every second so the collector always exposes fresh values. The OTel environment
        # variable wins over the Airflow option in the metrics logger, and breeze sets it to 30s.
        # Prometheus still scrapes every 30s, so a stage that fills and drains within one scrape
        # leaves no rate behind: give a run several minutes before reading the rate panels.
        os.environ["AIRFLOW__METRICS__OTEL_INTERVAL_MILLISECONDS"] = "1000"
        os.environ["OTEL_METRIC_EXPORT_INTERVAL"] = "1000"
        # Legacy names put dag_id and task_id in the metric name, one series family per task.
        # The dashboard reads the tagged names.
        os.environ["AIRFLOW__METRICS__LEGACY_NAMES_ON"] = "False"
        os.environ["AIRFLOW__TRACES__OTEL_ON"] = "True"
        os.environ["OTEL_EXPORTER_OTLP_PROTOCOL"] = "http/protobuf"
        os.environ["OTEL_EXPORTER_OTLP_TRACES_ENDPOINT"] = (
            f"http://{OTEL_COLLECTOR_HOST}:{OTEL_COLLECTOR_PORT}/v1/traces"
        )

        # Once per class, not per test case: a later test case starts with the earlier ones' finished
        # runs in the database. The scheduler only examines unfinished runs, so its work per loop is
        # the same, and the report tells the test cases apart by run id.
        subprocess.run(["airflow", "db", "reset", "--yes"], check=True, env=os.environ.copy())
        subprocess.run(["airflow", "db", "migrate"], check=True, env=os.environ.copy())

        # Every Dag in the folder; a test function picks the ones it runs by id.
        cls.dags = serialize_and_get_dags(dag_folder=DAG_FOLDER)
        assert cls.dags, f"no Dags found in {DAG_FOLDER}"

    def start_usage_poller(self, processes: dict[str, subprocess.Popen]) -> subprocess.Popen:
        """Start the sidecar that pushes CPU, memory and worker slot gauges for ``processes``."""
        slot_budget = self.sizing["worker_count"] * self.sizing["worker_concurrency"]
        command = [sys.executable, POLL_USAGE_SCRIPT, "--slot-budget", str(slot_budget)]
        for role, process in processes.items():
            command.append(f"{role}={process.pid}")
        # Its own session as well: terminate_processes signals whole groups, and sharing pytest's
        # would take the test runner down between test cases.
        return start_process(command)

    @staticmethod
    def run_report(dag_ids: list[str]) -> None:
        """Print the per-test-case summary of every run of ``dag_ids`` so far."""
        command = [sys.executable, REPORT_SCRIPT]
        for dag_id in dag_ids:
            command += ["--dag-id", dag_id]
        # check=False: this runs after the assertions, and a broken report must not hide their result.
        result = subprocess.run(command, env=os.environ.copy(), capture_output=True, text=True, check=False)
        if result.returncode == 0:
            print_to_terminal(result.stdout)
        else:
            print_to_terminal(f"report_run.py failed:\n{result.stderr}")

    def execute_dag_runs(
        self,
        *,
        run_id_prefix: str,
        dag_ids: list[str],
        run_count: int,
        run_conf: dict[str, Any],
        timeout_seconds: int,
    ) -> None:
        """Create the dag runs, start the deployment, wait for the runs to finish, stop it, report."""
        dags = {dag_id: self.dags[dag_id] for dag_id in dag_ids}
        # The runs go into the database before any process exists, for two reasons. Every test case
        # then starts from identical facts: the same rows, all queued, all eligible at the same
        # instant, before the scheduler's first loop, so a difference in drain time is the code and
        # not the order or timing in which work arrived. And triggering through the CLI costs
        # seconds per call, which would cap the live runs at the arrival rate and measure the
        # harness instead of the scheduler. The consequence is that this measures the scheduler
        # under the heaviest load the backlog can give it; a steady trickle of runs, as in
        # production, is a different shape, which a test function could model by creating runs
        # while the processes are already up.
        runs = create_queued_dag_runs(
            dags, run_id_prefix=run_id_prefix, run_count=run_count, run_conf=run_conf
        )
        processes = start_processes(
            worker_count=self.sizing["worker_count"],
            worker_concurrency=self.sizing["worker_concurrency"],
            api_server_workers=self.sizing["api_server_workers"],
        )
        try:
            processes["usage_poller"] = self.start_usage_poller(processes)
            # timeout_seconds is only a guard; the assertion below fails a test case that hit it,
            # because a partial drain compares with nothing.
            with ProgressReporter(runs):
                states = wait_for_dag_runs(runs, max_wait_time=timeout_seconds)

            exited = {}
            for role, process in processes.items():
                if process.poll() is not None:
                    exited[role] = process.returncode
            assert not exited, f"processes exited, with these codes: {exited}"

            not_succeeded = {run: state for run, state in states.items() if state != State.SUCCESS}
            assert not not_succeeded, (
                f"{len(not_succeeded)} of {len(runs)} dag_runs are not successful after {timeout_seconds}s: "
                f"{not_succeeded}"
            )
        finally:
            terminate_processes(processes)
            # Also after a failed assertion: the partial numbers say what happened.
            self.run_report(dag_ids)

    @pytest.mark.parametrize(
        "new_code",
        [
            pytest.param(False, id="original_code"),
            pytest.param(True, id="new_code"),
        ],
    )
    def test_scheduler_load(self, new_code: bool):
        """Drain many runs of the fan-out Dag at once, with the original code and then the new code."""
        test_case = "new_code" if new_code else "original_code"
        # Set before any process starts; the code under measurement reads it to pick its path.
        os.environ[NEW_CODE_ENV_VAR] = str(new_code).lower()

        # The load. Edit the defaults here, or set the variable named on each line for one run.
        run_count = int(os.environ.get("PERF_TEST__RUN_COUNT", "5"))  # queued runs created up front
        mapped_task_count = int(os.environ.get("PERF_TEST__MAPPED_TASK_COUNT", "20"))  # instances per stage
        # Give up on the test case after this.
        timeout_seconds = int(os.environ.get("PERF_TEST__TIMEOUT_SECONDS", "3600"))
        print_to_terminal(
            f"{test_case}: {run_count} runs of perf_dag, {mapped_task_count} mapped instances per stage, "
            f"timeout {timeout_seconds}s, profile {self.profile}"
        )

        self.execute_dag_runs(
            # The runs carry the test case name, so the dashboard and the report tell the cases apart.
            run_id_prefix=test_case,
            dag_ids=["perf_dag"],
            run_count=run_count,
            run_conf={"mapped_task_count": mapped_task_count},
            timeout_seconds=timeout_seconds,
        )
