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

import contextlib
import datetime
import json
import logging
import os
import signal
import socket
import subprocess
import threading
import time
import urllib.error
import urllib.request
from collections.abc import Sequence
from typing import Any

from sqlalchemy import func, select, tuple_, update

from airflow.dag_processing.bundles.manager import DagBundlesManager, _load_bundle_config_snapshot
from airflow.dag_processing.dagbag import DagBag
from airflow.models import DagModel, DagRun, TaskInstance
from airflow.models.serialized_dag import SerializedDagModel
from airflow.sdk import timezone
from airflow.serialization.definitions.dag import SerializedDAG
from airflow.utils.session import create_session
from airflow.utils.state import DagRunState, State
from airflow.utils.types import DagRunTriggeredByType, DagRunType

from tests_common.test_utils.dag import create_scheduler_dag
from tests_common.test_utils.version_compat import AIRFLOW_V_3_0_PLUS, AIRFLOW_V_3_1_PLUS

log = logging.getLogger(__name__)

# Breeze service names and ports.
# The otel-collector is assigned a fixed container_name in the compose file,
# so the name is the same in every worktree.
OTEL_COLLECTOR_HOST = "breeze-otel-collector"
OTEL_COLLECTOR_PORT = 4318
API_SERVER_PORT = 8080

FINISHED_TI_STATES = frozenset(state.value for state in State.finished)


def unpause_trigger_dag_and_get_run_id(dag_id: str, conf: dict | None = None) -> str:
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

    if conf:
        trigger_command += ["--conf", json.dumps(conf)]

    # Trigger the dag using the cli.
    subprocess.run(trigger_command, check=True, env=os.environ.copy())

    return run_id


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


def start_scheduler(capture_output: bool = False):
    stdout = None if capture_output else subprocess.DEVNULL
    stderr = None if capture_output else subprocess.DEVNULL

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

    scheduler_process = subprocess.Popen(
        scheduler_command_args,
        env=os.environ.copy(),
        stdout=stdout,
        stderr=stderr,
    )

    apiserver_process = subprocess.Popen(
        apiserver_command_args,
        env=os.environ.copy(),
        stdout=stdout,
        stderr=stderr,
    )

    # Wait to ensure both processes have started.
    time.sleep(10)

    return scheduler_process, apiserver_process


def serialize_and_get_dags(dag_folder) -> dict[str, SerializedDAG]:
    log.info("Serializing Dags from directory %s", dag_folder)
    # Load DAGs from the dag directory.
    dag_bag = DagBag(dag_folder=dag_folder)

    dag_ids = dag_bag.dag_ids

    dag_dict: dict[str, SerializedDAG] = {}
    with create_session() as session:
        for dag_id in dag_ids:
            dag = dag_bag.get_dag(dag_id)
            assert dag is not None, f"DAG with ID {dag_id} not found."
            # Sync the DAG to the database.
            if AIRFLOW_V_3_0_PLUS:
                from airflow.models.dagbundle import DagBundleModel

                count = session.scalar(
                    select(func.count()).select_from(DagBundleModel).where(DagBundleModel.name == "testing")
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
            "kwargs": {"path": f"{dag_folder}", "refresh_interval": 1},
        }
    ]

    os.environ["AIRFLOW__DAG_PROCESSOR__DAG_BUNDLE_CONFIG_LIST"] = json.dumps(TESTING_BUNDLE_CONFIG)
    # The manager caches the bundle config per process, and the pytest plugin already loaded it with
    # the default list; without this the sync below deactivates the "testing" bundle just added, and
    # the scheduler marks its Dags stale a minute later.
    _load_bundle_config_snapshot.cache_clear()
    manager = DagBundlesManager()
    manager.sync_bundles_to_db()

    return dag_dict


def wait_for_dag_runs(runs: list[tuple[str, str]], max_wait_time: int) -> dict[tuple[str, str], str | None]:
    """Wait until every ``(dag_id, run_id)`` in ``runs`` finishes or ``max_wait_time`` elapses; return the states."""
    # One budget for all runs, not one per run.
    deadline = time.monotonic() + max_wait_time
    states: dict[tuple[str, str], str | None] = dict.fromkeys(runs)
    while True:
        with create_session() as session:
            rows = session.execute(
                select(DagRun.dag_id, DagRun.run_id, DagRun.state).where(
                    tuple_(DagRun.dag_id, DagRun.run_id).in_(runs)
                )
            ).all()
        for dag_id, run_id, state in rows:
            states[(dag_id, run_id)] = state
        if all(state in (State.SUCCESS, State.FAILED) for state in states.values()):
            break
        if time.monotonic() >= deadline:
            break
        # Otherwise the loop spins and loads the database under test.
        time.sleep(2)
    return states


def is_process_group_alive(pgid: int) -> bool:
    """Whether any process of the group ``pgid`` still exists."""
    try:
        os.killpg(pgid, 0)
    except ProcessLookupError:
        return False
    return True


def terminate_process(proc: subprocess.Popen, timeout: int = 30, *, process_group: bool = False) -> None:
    """Stop ``proc``, with SIGKILL as the fallback if it is still alive after ``timeout``."""

    def send(sig: signal.Signals) -> None:
        # Children (uvicorn workers, prefork pool) would be orphaned if only the parent was signalled.
        if process_group:
            # proc is the group leader (start_new_session), so the group id equals proc.pid. Signalling
            # the group reaches the children even when proc has already exited and been reaped.
            with contextlib.suppress(ProcessLookupError):
                os.killpg(proc.pid, sig)
        else:
            proc.send_signal(sig)

    deadline = time.monotonic() + timeout
    send(signal.SIGTERM)
    try:
        proc.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        send(signal.SIGKILL)
        proc.wait()
    if not process_group:
        return
    # wait() only checks the leader. If a child is still alive, it will occupy the port and the broker queue.
    while is_process_group_alive(proc.pid) and time.monotonic() < deadline:
        time.sleep(0.5)
    if is_process_group_alive(proc.pid):
        send(signal.SIGKILL)
        while is_process_group_alive(proc.pid):
            time.sleep(0.5)


def print_to_terminal(text: str) -> None:
    """Write ``text`` as plain text to the controlling terminal, or to stdout when there is none."""
    # Plain text past pytest's capture, distinct from the processes' logging.
    try:
        with open("/dev/tty", "w") as tty:
            tty.write(text + "\n")
    except OSError:
        # No terminal: stdout, shown live by pytest -s.
        print(text, flush=True)


def compute_usable_cpu_count() -> int:
    """CPUs this process may actually run on."""
    # Honours a cpuset, which os.cpu_count() does not. Linux only.
    if hasattr(os, "sched_getaffinity"):
        return len(os.sched_getaffinity(0))
    # The whole host.
    return os.cpu_count() or 8


def export_setting(env_var: str, value: int | None) -> None:
    """Export an Airflow setting for the subprocesses; ``None`` keeps Airflow's default."""
    if value is not None:
        os.environ[env_var] = str(value)


def run_psql(sql: str) -> str:
    """Run ``sql`` against the breeze Postgres as its superuser and return the output."""
    result = subprocess.run(
        ["psql", "-h", "postgres", "-U", "postgres", "-d", "postgres", "-tAc", sql],
        env={**os.environ, "PGPASSWORD": "airflow"},
        capture_output=True,
        text=True,
        check=True,
    )
    return result.stdout.strip()


def run_docker_command(*args: str) -> str:
    """Run a ``docker`` command and return its output; breeze mounts the CLI and the socket."""
    result = subprocess.run(["docker", *args], capture_output=True, text=True, check=True)
    return result.stdout.strip()


def find_postgres_container() -> str | None:
    """Return the name of the Postgres container in this breeze container's Compose project, or None when unknown."""
    # Postgres has no fixed container_name, so it is <project>-postgres-1 and the project differs per
    # worktree. This container's labels carry the project, and its id is the hostname.
    try:
        project = run_docker_command(
            "inspect",
            "--format",
            '{{ index .Config.Labels "com.docker.compose.project" }}',
            socket.gethostname(),
        )
        names = run_docker_command(
            "ps",
            "--filter",
            f"label=com.docker.compose.project={project}",
            "--filter",
            "label=com.docker.compose.service=postgres",
            "--format",
            "{{.Names}}",
        )
    except (FileNotFoundError, subprocess.CalledProcessError):
        # No Docker CLI or socket: another environment.
        return None
    return names.splitlines()[0] if project and names else None


def restart_postgres_container(postgres_container: str) -> bool:
    """Restart ``container``; False when that is not possible from here."""
    try:
        run_docker_command("restart", postgres_container)
    except (FileNotFoundError, subprocess.CalledProcessError):
        return False
    return True


def ensure_postgres_max_connections(required: int) -> None:
    """Raise the metadata DB's ``max_connections`` to ``required`` if it is lower, restarting it once."""
    try:
        current = int(run_psql("show max_connections"))
    except FileNotFoundError:
        raise RuntimeError("psql not found: this helper runs inside the breeze environment")
    if current >= required:
        return

    # Persisted in the data volume; takes effect at server start, hence the restart.
    run_psql(f"ALTER SYSTEM SET max_connections = {required}")
    postgres_container = find_postgres_container()
    log.info(
        "Raised postgres max_connections from %s to %s; restarting %s", current, required, postgres_container
    )
    if postgres_container is not None and restart_postgres_container(postgres_container):
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            with contextlib.suppress(subprocess.CalledProcessError):
                if int(run_psql("show max_connections")) >= required:
                    return
            time.sleep(2)
    raise RuntimeError(
        f"postgres max_connections={current} is below the {required} needed. The new value is "
        "persisted and applies after a restart, which could not be done from here: run "
        f"`docker restart {postgres_container or '<breeze project>-postgres-1'}` on the host, then re-run the test."
    )


def start_process(command: list[str]) -> subprocess.Popen:
    """Start ``command`` with the current environment, as the leader of its own process group."""
    # Children (uvicorn workers, prefork pool) are signalled with the parent.
    return subprocess.Popen(command, env=os.environ.copy(), start_new_session=True)


def wait_for_api_server(process: subprocess.Popen, port: int = API_SERVER_PORT, timeout: int = 120) -> None:
    """Block until the API server answers its execution health route, or ``timeout`` passes."""
    # uvicorn accepts connections before its workers can answer; a task starting in that gap times
    # out and fails.
    health_url = f"http://localhost:{port}/execution/health"
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise RuntimeError(f"api_server exited early with code {process.returncode}")
        with contextlib.suppress(urllib.error.URLError, TimeoutError, ConnectionError):
            with urllib.request.urlopen(health_url, timeout=5) as response:
                if response.status == 200:
                    return
        time.sleep(1)
    raise RuntimeError(f"api_server did not answer {health_url} within {timeout}s")


def build_api_server_command(*, port: int, workers: int) -> list[str]:
    """The ``airflow api-server`` command line for ``start_process``."""
    # --daemon would detach the real server from Popen and let it outlive the test.
    return ["airflow", "api-server", "--port", str(port), "--workers", str(workers)]


def build_celery_worker_command(*, hostname: str, concurrency: int) -> list[str]:
    """The ``celery worker`` command line for ``start_process``."""
    return [
        "celery",
        "--app",
        "airflow.providers.celery.executors.celery_executor.app",
        "worker",
        "--pool",
        "prefork",
        "--loglevel",
        "INFO",
        "--concurrency",
        str(concurrency),
        "--hostname",
        hostname,
    ]


def start_processes(
    *,
    worker_count: int,
    worker_concurrency: int,
    api_server_workers: int,
    port: int = API_SERVER_PORT,
    triggerer: bool = False,
) -> tuple[dict[str, subprocess.Popen], float]:
    """Start the processes of a Celery executor setup, keyed by role, plus the scheduler's start time."""
    processes: dict[str, subprocess.Popen] = {}
    try:
        # First: every task's first act is to call it.
        processes["api_server"] = start_process(
            build_api_server_command(port=port, workers=api_server_workers)
        )
        wait_for_api_server(processes["api_server"], port=port)
        for index in range(1, worker_count + 1):
            command = build_celery_worker_command(hostname=f"worker{index}", concurrency=worker_concurrency)
            processes[f"worker_{index}"] = start_process(command)
        # Only for Dags that defer.
        if triggerer:
            processes["triggerer"] = start_process(["airflow", "triggerer"])
        # There isn't any actual execution before the scheduler start,
        # so timing starts here and includes the settle sleep below.
        scheduler_started_at = time.monotonic()
        processes["scheduler"] = start_process(["airflow", "scheduler"])

        # Enough for a misconfigured process to have exited.
        time.sleep(5)
        for role, process in processes.items():
            if process.poll() is not None:
                raise RuntimeError(f"{role} exited early with code {process.returncode}")
    except Exception:
        terminate_processes(processes)
        raise
    return processes, scheduler_started_at


def terminate_processes(processes: dict[str, subprocess.Popen]) -> None:
    """Stop every process group started by :func:`start_process`, SIGKILL as the fallback."""
    for role, process in processes.items():
        terminate_process(process, process_group=True)
        if process.poll() is None:
            raise RuntimeError(f"{role} is still running after termination")


def create_queued_dag_runs(
    dags: dict[str, SerializedDAG], *, run_id_prefix: str, dag_run_count: int, run_conf: dict[str, Any]
) -> list[tuple[str, str]]:
    """Unpause every Dag in ``dags`` and create ``dag_run_count`` queued manual runs of it through the ORM."""
    base = timezone.utcnow()
    runs: list[tuple[str, str]] = []
    with create_session() as session:
        for dag_id, dag in sorted(dags.items()):
            session.execute(update(DagModel).where(DagModel.dag_id == dag_id).values(is_paused=False))
            for run_index in range(1, dag_run_count + 1):
                run_id = f"{run_id_prefix}__{run_index}"
                dag.create_dagrun(
                    run_id=run_id,
                    # (dag_id, logical_date) is unique.
                    logical_date=base + datetime.timedelta(seconds=run_index),
                    data_interval=None,
                    # Same for all, so every run is eligible at once.
                    run_after=base,
                    conf=run_conf,
                    run_type=DagRunType.MANUAL,
                    triggered_by=DagRunTriggeredByType.CLI,
                    state=DagRunState.QUEUED,
                    session=session,
                )
                runs.append((dag_id, run_id))
    log.info("Created %d queued Dag runs across %d Dags with conf %s", len(runs), len(dags), run_conf)
    return runs


def format_state_counts(counts: Sequence[Any]) -> str:
    if not counts:
        return "-"
    parts = [f"{state or 'none'}={count}" for state, count in counts]
    return " ".join(sorted(parts))


class ProgressReporter:
    """
    Show a periodic state summary of ``runs`` while the test is active.

    Apart from providing an update of the test state, it also helps identify
    a frozen run that isn't making any progress.
    """

    def __init__(self, runs: list[tuple[str, str]], interval: int = 15):
        self._dag_ids = sorted({dag_id for dag_id, _ in runs})
        self._run_ids = sorted({run_id for _, run_id in runs})
        self._interval = interval
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._poll, daemon=True)
        self._start_time = 0.0
        self._last_finished = 0

    def __enter__(self) -> ProgressReporter:
        self._start_time = time.monotonic()
        self._thread.start()
        return self

    def __exit__(self, *exc_info) -> None:
        self._stop.set()
        self._thread.join(timeout=self._interval + 5)
        self._report()

    def _poll(self) -> None:
        while not self._stop.wait(self._interval):
            self._report()

    def _report(self) -> None:
        # A failed query must not end the reporting; the next interval tries again.
        try:
            self._query_and_print()
        except Exception as error:
            print_to_terminal(f"[progress] report failed: {error!r}")

    def _query_and_print(self) -> None:
        with create_session() as session:
            ti_counts = session.execute(
                select(TaskInstance.state, func.count())
                .where(TaskInstance.dag_id.in_(self._dag_ids), TaskInstance.run_id.in_(self._run_ids))
                .group_by(TaskInstance.state)
            ).all()
            dr_counts = session.execute(
                select(DagRun.state, func.count())
                .where(DagRun.dag_id.in_(self._dag_ids), DagRun.run_id.in_(self._run_ids))
                .group_by(DagRun.state)
            ).all()

        finished = sum(count for state, count in ti_counts if state in FINISHED_TI_STATES)
        rate = (finished - self._last_finished) / self._interval
        self._last_finished = finished

        print_to_terminal(
            f"[progress {time.monotonic() - self._start_time:5.0f}s] "
            f"dag_runs: {format_state_counts(dr_counts)} | "
            f"tis: {format_state_counts(ti_counts)} | "
            f"finished {finished} ({rate:.1f}/s)"
        )
