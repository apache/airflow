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
# /// script
# requires-python = ">=3.10"
# dependencies = ["apache-airflow-core", "apache-airflow-providers-celery", "cryptography"]
# ///
"""Run the scheduler-hosting experiment in Breeze with an isolated Celery worker and scheduler."""

from __future__ import annotations

import argparse
import configparser
import json
import multiprocessing
import os
import socket
import sqlite3
import statistics
import subprocess
import threading
import time
from contextlib import ExitStack
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

REDIS_IMAGE = "public.ecr.aws/docker/library/redis:7.4-alpine"
ROUTE = "scheduler-parsing"


def write_json(path: Path, value) -> None:
    path.write_text(json.dumps(value, indent=2) + "\n")


def docker(*arguments: str, check: bool = True) -> str:
    result = subprocess.run(["docker", *arguments], check=False, capture_output=True, text=True, timeout=45)
    if check and result.returncode:
        raise RuntimeError(f"Docker {arguments[0]} failed: {result.stderr.strip()}")
    return result.stdout.strip()


def wait_until(predicate, *, timeout: float = 120, check=None):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if value := predicate():
            return value
        if check is not None:
            check()
        time.sleep(0.1)
    raise TimeoutError("Scheduler-hosting checkpoint did not finish")


def stop_process(process) -> None:
    if process.pid is None:
        return
    process.terminate()
    process.join(5)
    if process.is_alive():
        process.kill()
        process.join(5)


def run_api(database: str, public: Path, listener: socket.socket) -> None:
    # These fresh processes import Airflow only after the experiment environment is configured.
    from airflow.dag_processing.executor_manager import _serve_api

    _serve_api(database, public, listener)


def run_provider(database: str, key: bytes, stop, capacity: int) -> None:
    from airflow.api_fastapi.auth.tokens import JWTGenerator
    from airflow.api_fastapi.execution_api.parsing import (
        TOKEN_AUDIENCE,
        TOKEN_ISSUER,
        TOKEN_KEY_ID,
        TOKEN_SCOPE,
    )
    from airflow.dag_processing.executor_runner import ParsingExecutorRunner
    from airflow.dag_processing.parsing_metadata import MetadataOrchestrationStore
    from airflow.executors.workloads import WorkloadType
    from airflow.providers.celery.executors.celery_executor import CeleryExecutor

    generator = JWTGenerator(
        private_key=Ed25519PrivateKey.from_private_bytes(key),
        kid=TOKEN_KEY_ID,
        issuer=TOKEN_ISSUER,
        audience=TOKEN_AUDIENCE,
        algorithm="EdDSA",
        valid_for=600,
    )

    def issue_token(manifest):
        return generator.generate(
            {
                "sub": manifest["workload_id"],
                "scope": TOKEN_SCOPE,
                "attempt_ids": [item["attempt_id"] for item in manifest["definitions"]],
            }
        )

    executor = CeleryExecutor(parallelism=capacity)
    executor.supported_workload_types = frozenset({WorkloadType.PARSE_DAG_DEFINITIONS})
    store = MetadataOrchestrationStore(database)
    runner = ParsingExecutorRunner(store, executor, route=ROUTE, token_issuer=issue_token)
    runner.start()
    try:
        while not stop.poll(0.05):
            runner.tick()
    finally:
        runner.close()
        store.engine.dispose()


class MetricsCapture:
    """Capture the scheduler's StatsD measurements independently of its process."""

    def __init__(self):
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.socket.bind(("0.0.0.0", 0))
        self.socket.settimeout(0.1)
        self.port = self.socket.getsockname()[1]
        self.samples: list[dict] = []
        self.stop = threading.Event()
        self.thread = threading.Thread(target=self.receive, daemon=True)
        self.thread.start()

    def receive(self):
        while not self.stop.is_set():
            try:
                packet, _ = self.socket.recvfrom(65536)
            except TimeoutError:
                continue
            for metric in packet.decode().splitlines():
                name, value = metric.split(":", 1)
                number, kind, *_ = value.split("|")
                self.samples.append(
                    {"at": time.monotonic(), "name": name, "value": float(number), "kind": kind}
                )

    def close(self):
        self.stop.set()
        self.thread.join(2)
        self.socket.close()


def summarize_metrics(samples: list[dict]) -> dict:
    result = {}
    for name in sorted({item["name"] for item in samples}):
        if (
            "parsing_step" not in name
            and "scheduler_loop_duration" not in name
            and "scheduler_heartbeat" not in name
        ):
            continue
        matching = [item for item in samples if item["name"] == name]
        values = sorted(item["value"] for item in matching)
        result[name] = (
            {
                "samples": len(values),
                "median_ms": statistics.median(values),
                "p95_ms": values[int((len(values) - 1) * 0.95)],
                "max_ms": max(values),
            }
            if matching[0]["kind"] == "ms"
            else {"count": sum(values)}
        )
    return result


def get_state(database: Path) -> dict:
    with sqlite3.connect(f"file:{database}?mode=ro", uri=True, timeout=1) as connection:
        return {
            "heartbeat": connection.execute(
                "SELECT max(latest_heartbeat) FROM job WHERE job_type='SchedulerJob'"
            ).fetchone()[0],
            "completed_tasks": connection.execute(
                "SELECT count(*) FROM task_instance WHERE state='success'"
            ).fetchone()[0],
            "accepted": dict(connection.execute("SELECT path, accepted_count FROM parse_sources")),
        }


def record_phase(name: str, database: Path, metrics: MetricsCapture, seconds: float, action=None) -> dict:
    before = get_state(database)
    started = time.monotonic()
    if action is None:
        time.sleep(seconds)
    else:
        action(seconds)
    after = get_state(database)
    phase = {
        "name": name,
        "before": before,
        "after": after,
        "metrics": summarize_metrics([item for item in metrics.samples if item["at"] >= started]),
    }
    if not any(name.endswith("parsing_step_duration") for name in phase["metrics"]):
        raise RuntimeError(f"Missing parsing callback measurements during {name}")
    if after["heartbeat"] == before["heartbeat"] or after["completed_tasks"] <= before["completed_tasks"]:
        raise RuntimeError(f"Scheduler stopped making progress during {name}: {phase}")
    print(json.dumps({"event": "phase", **phase}), flush=True)
    return phase


def contend(database: Path, seconds: float) -> None:
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        with sqlite3.connect(database, timeout=0) as connection:
            try:
                connection.execute("BEGIN IMMEDIATE")
            except sqlite3.OperationalError:
                pass
            else:
                time.sleep(0.04)
        time.sleep(0.08)


def run_experiment(output: Path, phase_seconds: float) -> dict:
    own = json.loads(docker("inspect", os.environ["HOSTNAME"]))[0]
    mounts = {mount["Destination"]: Path(mount["Source"]) for mount in own["Mounts"]}
    host_root = mounts["/opt/airflow/airflow-core"].parent
    host_output = mounts["/files"] / output.relative_to("/files")
    control, source, evidence = (output / name for name in ("control", "worker-bundle", "worker-evidence"))
    for path in (control, source, evidence):
        path.mkdir(mode=0o777)
        path.chmod(0o777)
    database = control / "metadata.sqlite"
    os.environ.update(
        AIRFLOW_HOME=str(control / "api-home"),
        AIRFLOW_CONFIG=str(control / "api.cfg"),
        AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=f"sqlite:///{database}",
        AIRFLOW__CORE__LOAD_EXAMPLES="False",
        AIRFLOW__CORE__EXECUTOR="LocalExecutor",
        AIRFLOW__CORE__MIN_SERIALIZED_DAG_UPDATE_INTERVAL="0",
        AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION="False",
        AIRFLOW__DAG_PROCESSOR__DAG_BUNDLE_CONFIG_LIST="[]",
        AIRFLOW__CELERY__BROKER_URL="redis://poc-broker:6379/0",
        AIRFLOW__CELERY__RESULT_BACKEND="redis://poc-broker:6379/1",
        AIRFLOW__CELERY__SYNC_PARALLELISM="1",
    )
    with (output / "migration.log").open("w") as log:
        subprocess.run(
            ["airflow", "db", "migrate"], check=True, stdout=log, stderr=subprocess.STDOUT, timeout=120
        )
    database.chmod(0o666)
    from sqlalchemy.orm import Session

    from airflow.dag_processing.discovery import discover_python_bundle
    from airflow.dag_processing.orchestrator import ParseOrchestrator
    from airflow.dag_processing.parsing_metadata import MetadataOrchestrationStore
    from airflow.executors.workloads import BundleInfo
    from airflow.models.dagbundle import DagBundleModel

    (source / "fast.py").write_text(
        "from datetime import datetime, timedelta, timezone\nfrom airflow.sdk import DAG\n"
        "from airflow.providers.standard.operators.empty import EmptyOperator\n"
        "with DAG('scheduler_hosted',schedule=timedelta(seconds=2),catchup=False,"
        "start_date=datetime(2025,1,1,tzinfo=timezone.utc),is_paused_upon_creation=False):\n"
        "    EmptyOperator(task_id='tick')\n"
    )
    (source / "slow.py").write_text(
        "import time\nfrom airflow.sdk import DAG\ntime.sleep(3)\n"
        "dag = DAG('scheduler_slow', schedule=None)\n"
    )
    config = {"route": ROUTE, "bundle": "poc", "capacity": 2, "batch_size": 1, "parse_interval": 2}
    write_json(control / "parsing.json", config)
    store = MetadataOrchestrationStore(database)
    with Session(store.engine) as session:
        session.add(DagBundleModel(name="poc", version="v1"))
        session.commit()
    ParseOrchestrator(store, **config).update_inventory(
        BundleInfo(name="poc", version="v1"), discover_python_bundle(source, bundle_name="poc")
    )
    context = multiprocessing.get_context("spawn")
    key = Ed25519PrivateKey.generate()
    public = control / "public.pem"
    public.write_bytes(
        key.public_key().public_bytes(
            serialization.Encoding.PEM, serialization.PublicFormat.SubjectPublicKeyInfo
        )
    )
    listener = socket.socket()
    listener.bind(("0.0.0.0", 0))
    api_port = listener.getsockname()[1]
    api = context.Process(target=run_api, args=(str(database), public, listener), name="parsing-api")
    stop_reader, stop_writer = context.Pipe(duplex=False)
    runner = context.Process(
        target=run_provider,
        args=(
            str(database),
            key.private_bytes(
                serialization.Encoding.Raw, serialization.PrivateFormat.Raw, serialization.NoEncryption()
            ),
            stop_reader,
            2,
        ),
        name="celery-parsing-runner",
    )
    run_id = uuid4().hex[:12]
    network = f"dag-parsing-scheduler-{run_id}"

    with ExitStack() as cleanup:
        cleanup.callback(store.engine.dispose)
        metrics = MetricsCapture()
        cleanup.callback(metrics.close)
        docker("network", "create", network)
        cleanup.callback(docker, "network", "rm", network, check=False)
        docker("network", "connect", "--alias", "poc-api", network, own["Id"])
        cleanup.callback(docker, "network", "disconnect", network, own["Id"], check=False)

        def start_container(
            name: str, image: str, args: list[str], bindings: dict[Path, str], env: list[str]
        ) -> str:
            container_name = f"{network}-{name}"
            cleanup.callback(docker, "rm", "-f", container_name, check=False)
            command = [
                "run",
                "-d",
                "--name",
                container_name,
                "--network",
                network,
                "--user",
                "50000:0",
                "--read-only",
                "--cap-drop=ALL",
                "--security-opt=no-new-privileges",
                "--tmpfs",
                "/tmp:rw,mode=1777",
                "--entrypoint",
                "/usr/bin/env",
            ]
            for src, target in bindings.items():
                command.extend(["--mount", f"type=bind,source={src},target={target}"])
            identifier = docker(
                *command,
                image,
                "-i",
                "PATH=/usr/python/bin:/usr/bin:/bin",
                "HOME=/tmp",
                "USER=airflow",
                "PYTHONPATH=/opt/airflow",
                *env,
                *args,
            )

            def finish():
                with (output / f"{name}.log").open("w") as log:
                    subprocess.run(
                        ["docker", "logs", identifier],
                        stdout=log,
                        stderr=subprocess.STDOUT,
                        check=False,
                        timeout=45,
                    )
                write_json(output / f"{name}-state.json", json.loads(docker("inspect", identifier)))

            cleanup.callback(finish)
            return identifier

        cleanup.callback(docker, "rm", "-f", f"{network}-broker", check=False)
        broker = docker(
            "run",
            "-d",
            "--name",
            f"{network}-broker",
            "--network",
            network,
            "--network-alias",
            "poc-broker",
            REDIS_IMAGE,
            "redis-server",
            "--save",
            "",
            "--appendonly",
            "no",
        )
        bindings = {
            host_root / path: f"/opt/airflow/{path},readonly"
            for path in (
                "airflow-core",
                "task-sdk",
                "shared",
                "providers/celery",
                "providers/common/compat",
                "dev/dag_parsing_poc",
            )
        }
        api.start()
        listener.close()
        cleanup.callback(stop_process, api)
        runner.start()
        cleanup.callback(stop_process, runner)
        worker = start_container(
            "worker",
            own["Image"],
            [
                "/usr/python/bin/python",
                "/opt/airflow/dev/dag_parsing_poc/celery_worker.py",
                "--broker-url",
                "redis://poc-broker:6379/0",
                "--result-backend",
                "redis://poc-broker:6379/1",
                "--api-url",
                f"http://poc-api:{api_port}/execution/",
                "--queue",
                ROUTE,
                "--bundle-name",
                "poc",
                "--bundle-version",
                "v1",
                "--bundle-root",
                "/worker-bundle",
                "--evidence",
                "/worker-evidence/isolation.json",
                "--airflow-home",
                "/tmp/parsing-worker",
                "--concurrency",
                "2",
                "--include-source",
                "--forbidden-path",
                "/control",
                "--forbidden-path",
                "/files",
                "--forbidden-path",
                "/var/run/docker.sock",
            ],
            bindings
            | {
                host_output / "worker-bundle": "/worker-bundle,readonly",
                host_output / "worker-evidence": "/worker-evidence",
            },
            [],
        )

        def check_processes(*containers):
            if not api.is_alive() or not runner.is_alive():
                raise RuntimeError("The parsing API or executor runner exited unexpectedly")
            states = json.loads(docker("inspect", *containers))
            if any(not item["State"]["Running"] for item in states):
                raise RuntimeError("An experiment container exited; see its captured log")

        wait_until(
            lambda: (
                (evidence / "isolation.json").exists()
                and json.loads((evidence / "isolation.json").read_text()).get("stage") == "worker_ready"
            ),
            check=lambda: check_processes(worker),
        )
        scheduler_config = configparser.ConfigParser(interpolation=None)
        scheduler_config.read_dict(
            {
                "core": {
                    "executor": "LocalExecutor",
                    "parallelism": "2",
                    "load_examples": "false",
                    "dags_are_paused_at_creation": "false",
                },
                "database": {"sql_alchemy_conn": "sqlite:////control/metadata.sqlite"},
                "dag_processor": {"dag_bundle_config_list": "[]", "stale_bundle_cleanup_interval": "0"},
                "scheduler": {"scheduler_idle_sleep_time": "0.05", "scheduler_heartbeat_sec": "1"},
                "metrics": {
                    "statsd_on": "true",
                    "statsd_host": "poc-api",
                    "statsd_port": str(metrics.port),
                    "statsd_prefix": "poc_scheduler",
                    "legacy_names_on": "false",
                },
            }
        )
        with (control / "scheduler.cfg").open("w") as stream:
            scheduler_config.write(stream)
        scheduler = start_container(
            "scheduler",
            own["Image"],
            ["airflow", "scheduler", "--skip-serve-logs", "--parsing-config", "/control/parsing.json"],
            bindings | {host_output / "control": "/control"},
            ["AIRFLOW_HOME=/tmp/scheduler", "AIRFLOW_CONFIG=/control/scheduler.cfg"],
        )
        scheduler_mounts = json.loads(docker("inspect", scheduler))[0]["Mounts"]
        if any(Path(mount["Source"]) == host_output / "worker-bundle" for mount in scheduler_mounts):
            raise RuntimeError("Scheduler unexpectedly has the worker's Dag source mount")
        docker("exec", scheduler, "test", "!", "-e", "/worker-bundle")
        worker_isolation = json.loads((evidence / "isolation.json").read_text())
        print(
            json.dumps({"event": "started", "scheduler": scheduler, "worker": worker, "network": network}),
            flush=True,
        )
        wait_until(
            lambda: (
                len(get_state(database)["accepted"]) == 2
                and min(get_state(database)["accepted"].values()) >= 2
                and get_state(database)["completed_tasks"] >= 2
            ),
            check=lambda: check_processes(worker, scheduler),
        )
        phases = [record_phase("healthy_slow_imports", database, metrics, phase_seconds)]
        docker("pause", broker)
        try:
            phases.append(record_phase("broker_unavailable", database, metrics, phase_seconds))
        finally:
            docker("unpause", broker)
        accepted = sum(get_state(database)["accepted"].values())
        wait_until(lambda: sum(get_state(database)["accepted"].values()) > accepted)
        phases.append(
            record_phase(
                "database_contention",
                database,
                metrics,
                phase_seconds,
                lambda duration: contend(database, duration),
            )
        )
        docker("stop", "--time", "10", scheduler)
        wait_until(lambda: not store.get_admissions(ROUTE), timeout=60)
        stop_writer.send_bytes(b"stop")
        runner.join(15)
        if runner.is_alive() or runner.exitcode != 0:
            raise RuntimeError("Celery runner did not drain cleanly")
        with sqlite3.connect(database) as connection:
            tasks = connection.execute(
                "SELECT d.run_after, t.end_date FROM task_instance t JOIN dag_run d "
                "ON d.dag_id=t.dag_id AND d.run_id=t.run_id WHERE t.state='success'"
            ).fetchall()
        latencies = [
            (
                datetime.fromisoformat(end).replace(tzinfo=timezone.utc)
                - datetime.fromisoformat(start).replace(tzinfo=timezone.utc)
            ).total_seconds()
            for start, end in tasks
        ]
        write_json(output / "metrics.json", metrics.samples)
        result = {
            "status": "passed",
            "image": own["Image"],
            "phases": phases,
            "final": get_state(database),
            "active_reservations": len(store.get_admissions(ROUTE)),
            "empty_task_completion_latency_seconds": {
                "median": statistics.median(latencies),
                "max": max(latencies),
            },
            "scheduler_source_mount": False,
            "worker_control_mount": False,
            "scheduler_mounts": scheduler_mounts,
            "worker_isolation": worker_isolation,
            "inventory": "registered outside scheduler; no remote discovery implementation",
            "budget": "25 ms cooperative SQL budget; zero lock wait; filesystem/kernel stalls are not preemptible",
        }
        write_json(output / "summary.json", result)
        return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--phase-seconds", type=float, default=10)
    args = parser.parse_args()
    if args.phase_seconds < 5:
        parser.error("Use at least five seconds per phase")
    output = args.output.resolve()
    if not output.is_relative_to("/files"):
        parser.error("Output must be under Breeze's /files mount")
    output.mkdir(parents=True, exist_ok=False)
    print(json.dumps(run_experiment(output, args.phase_seconds), indent=2))


if __name__ == "__main__":
    main()
