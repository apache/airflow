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
# ///
"""Trusted Celery experiment driver; run inside the current worktree's Breeze image."""

from __future__ import annotations

import argparse
import json
import multiprocessing
import os
import shutil
import socket
import time
from collections import Counter
from pathlib import Path

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from airflow.api_fastapi.auth.tokens import JWTGenerator
from airflow.executors.workloads import WorkloadType
from airflow.executors.workloads.parsing import ParseDagDefinitionsState

from dev.dag_parsing_poc.api import TOKEN_AUDIENCE, TOKEN_ISSUER, TOKEN_KEY_ID
from dev.dag_parsing_poc.run import create_workloads, serve_api, wait_for_api, write_fixtures
from dev.dag_parsing_poc.store import ReceiptStore


def wait_until(predicate, *, description: str, timeout: float = 60) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.1)
    raise TimeoutError(f"Timed out waiting for {description}")


def read_json(path: Path) -> dict:
    return json.loads(path.read_text()) if path.exists() else {}


def read_task_events(output: Path, task_id: str) -> list[dict]:
    path = output / "worker-evidence" / "task-events.jsonl"
    if not path.exists():
        return []
    complete_lines = path.read_text().split("\n")[:-1]
    return [event for line in complete_lines if (event := json.loads(line))["task_id"] == task_id]


def finish_workloads(executor, workloads, *, timeout: float = 120) -> dict:
    terminal = {}
    started = time.monotonic()

    def poll():
        executor.heartbeat()
        for key, (state, info) in executor.get_event_buffer().items():
            if state in {ParseDagDefinitionsState.SUCCESS, ParseDagDefinitionsState.FAILED}:
                terminal[str(key)] = {"state": state.value, "info": str(info) if info else None}
        return len(terminal) == len(workloads)

    wait_until(poll, description="Celery terminal events", timeout=timeout)
    if any(event["state"] != "success" for event in terminal.values()):
        raise RuntimeError(f"Celery workload failed: {terminal}")
    if executor.slots_available != executor.parallelism:
        raise RuntimeError("Terminal workloads did not release executor slots")
    return {"events": terminal, "elapsed_seconds": time.monotonic() - started}


def run_experiment(args, output: Path, generator: JWTGenerator, store: ReceiptStore) -> dict:
    # The Celery provider caches config on import, so configure this fresh driver first.
    os.environ["AIRFLOW__CELERY__BROKER_URL"] = args.broker_url
    os.environ["AIRFLOW__CELERY__RESULT_BACKEND"] = args.result_backend
    os.environ["AIRFLOW__CELERY__SYNC_PARALLELISM"] = "1"
    from airflow.configuration import conf
    from airflow.providers.celery.executors.celery_executor import CeleryExecutor

    source = output / "submitter-bundle"
    worker_root = output / "worker-bundle"
    files = write_fixtures(source, args.definitions, True)
    for path in files[: args.definitions]:
        with path.open("a") as stream:
            stream.write(
                "\nfrom pathlib import Path\n"
                f"with Path('/worker-evidence/{path.stem}.imports').open('a') as marker:\n"
                "    marker.write('imported\\n')\n"
            )
    slow = source / "active_duplicate.py"
    slow.write_text(
        "import time\nfrom pathlib import Path\nfrom airflow.sdk import DAG\n"
        "with Path('/worker-evidence/active_duplicate.imports').open('a') as marker:\n"
        "    marker.write('imported\\n')\n"
        "time.sleep(5)\nwith DAG('celery_active_duplicate', schedule=None):\n    pass\n"
    )
    shutil.copytree(source, worker_root)
    (output / "worker-evidence").mkdir()
    (output / "driver-ready.json").write_text(json.dumps({"port": args.port, "queue": args.queue}) + "\n")
    wait_until(
        lambda: read_json(output / "worker-evidence" / "isolation.json").get("stage") == "worker_ready",
        description="isolated worker startup",
        timeout=180,
    )
    configured_executor = conf.get("core", "executor")
    task_executor = CeleryExecutor(parallelism=2)
    executor = CeleryExecutor(parallelism=1)
    executor.supported_workload_types = frozenset({WorkloadType.PARSE_DAG_DEFINITIONS})
    executor.start()
    try:
        workloads = [
            workload.model_copy(update={"queue": args.queue})
            for workload in create_workloads(
                files, batch_size=args.batch_size, timeout=30, generator=generator
            )
        ]
        for workload in workloads:
            store.register_workload(workload)
            executor.queue_workload(workload, session=None)
        ordinary = finish_workloads(executor, workloads)
        results = [result for workload in workloads for result in store.get_results(workload.workload_id)]
        expected = {"success": args.definitions, "import_error": 1, "timeout": 1}
        outcomes = dict(Counter(result["outcome"] for result in results))
        received_dags = {
            result["relative_path"]: [dag.get("dag", {}).get("dag_id") for dag in result["serialized_dags"]]
            for result in results
            if result["outcome"] == "success"
        }
        if outcomes != expected or received_dags != {
            f"success_{index}.py": [f"executor_parsing_poc_{index}"] for index in range(args.definitions)
        }:
            raise RuntimeError(f"Unexpected parsing output: {outcomes}, {received_dags}")

        task = executor.celery_app.tasks["execute_workload"]
        replay = workloads[0]
        replay_id = str(replay.workload_id)
        before = store.get_attempts(replay.workload_id)
        wait_until(
            lambda: any(event["state"] == "SUCCESS" for event in read_task_events(output, replay_id)),
            description="first batch completion evidence",
        )
        initial_successes = sum(event["state"] == "SUCCESS" for event in read_task_events(output, replay_id))
        task.apply_async(
            args=[replay.model_dump_json()],
            queue=args.queue,
            task_id=replay_id,
            argsrepr="(<redacted parsing workload>,)",
        )
        wait_until(
            lambda: (
                sum(event["state"] == "SUCCESS" for event in read_task_events(output, replay_id))
                > initial_successes
            ),
            description="accepted batch replay",
        )
        if store.get_attempts(replay.workload_id) != before:
            raise RuntimeError("Replay changed accepted receipts")

        active = create_workloads([slow], batch_size=1, timeout=30, generator=generator)[0].model_copy(
            update={"queue": args.queue}
        )
        store.register_workload(active)
        executor.queue_workload(active, session=None)
        executor.heartbeat()
        active_id = str(active.workload_id)
        wait_until(
            lambda: store.get_attempts(active.workload_id)[0]["status"] == "claimed",
            description="original active claim",
        )
        original_execution = store.get_attempts(active.workload_id)[0]["execution_id"]
        task.apply_async(
            args=[active.model_dump_json()],
            queue=args.queue,
            task_id=active_id,
            argsrepr="(<redacted parsing workload>,)",
        )
        wait_until(
            lambda: any(event["state"] == "IGNORED" for event in read_task_events(output, active_id)),
            description="duplicate being ignored",
        )
        duplicate_backend_state = executor.celery_app.AsyncResult(active_id).state
        if duplicate_backend_state in {"FAILURE", "REVOKED"}:
            raise RuntimeError("Duplicate poisoned the original Celery result")
        active_run = finish_workloads(executor, [active])
        accepted = store.get_attempts(active.workload_id)[0]
        if accepted["execution_id"] != original_execution or accepted["status"] != "accepted":
            raise RuntimeError("Duplicate replaced the original execution")
        active_results = store.get_results(active.workload_id)
        if (
            len(active_results) != 1
            or active_results[0]["outcome"] != "success"
            or [dag.get("dag", {}).get("dag_id") for dag in active_results[0]["serialized_dags"]]
            != ["celery_active_duplicate"]
        ):
            raise RuntimeError("Original execution did not publish the expected successful Dag")
        import_counts = {
            path.stem: len(path.read_text().splitlines())
            for path in (output / "worker-evidence").glob("*.imports")
        }
        if len(import_counts) != args.definitions + 1 or any(count != 1 for count in import_counts.values()):
            raise RuntimeError(f"Definitions were imported more than once: {import_counts}")
        if task_executor.slots_available != 2 or conf.get("core", "executor") != configured_executor:
            raise RuntimeError("Parsing changed task routing or instance capacity")
        if WorkloadType.PARSE_DAG_DEFINITIONS in task_executor.supported_workload_types:
            raise RuntimeError("Parsing was enabled by default")
        (output / "results.json").write_text(json.dumps(results, indent=2) + "\n")
        return {
            "mode": "dedicated CeleryExecutor, Redis broker/backend, isolated prefork worker",
            "definitions": len(files),
            "dispatches": len(workloads),
            "batch_size": args.batch_size,
            "parsing_parallelism": 1,
            "outcomes": outcomes,
            "received_dags": received_dags,
            "ordinary": ordinary,
            "accepted_replay_preserved_receipts": True,
            "active_duplicate": active_run,
            "active_duplicate_outcome": active_results[0]["outcome"],
            "duplicate_backend_state": duplicate_backend_state,
            "import_counts": import_counts,
            "independent_task_slots": task_executor.slots_available,
            "isolation": read_json(output / "worker-evidence" / "isolation.json"),
            "limitations": [
                "Development receipt API; no production metadata ingestion or parse-time reads.",
                "Core parser bridge, not the proposed portable SDK importer.",
                "No automatic recovery of abandoned claims or orchestrator restart/adoption.",
                "Separate worker container on the same Docker host, not a multi-host deployment.",
                "No concurrent task benchmark, full manager baseline, Kubernetes or HA proof.",
            ],
        }
    finally:
        executor.end()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--broker-url", required=True)
    parser.add_argument("--result-backend", required=True)
    parser.add_argument("--queue", default="poc-parsing")
    parser.add_argument("--port", type=int, default=8799)
    parser.add_argument("--definitions", type=int, default=2)
    parser.add_argument("--batch-size", type=int, choices=range(1, 101), default=2)
    args = parser.parse_args()
    if args.definitions < 1:
        parser.error("definitions must be positive")
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    key = Ed25519PrivateKey.generate()
    public_key = output / "verification-key.pem"
    public_key.write_bytes(
        key.public_key().public_bytes(
            serialization.Encoding.PEM, serialization.PublicFormat.SubjectPublicKeyInfo
        )
    )
    generator = JWTGenerator(
        private_key=key,
        kid=TOKEN_KEY_ID,
        issuer=TOKEN_ISSUER,
        audience=TOKEN_AUDIENCE,
        algorithm="EdDSA",
        valid_for=360,
    )
    store_path = output / "receipts.sqlite"
    store = ReceiptStore(store_path)
    listener = socket.socket()
    listener.bind(("0.0.0.0", args.port))
    api = multiprocessing.get_context("spawn").Process(
        target=serve_api, args=(str(store_path), str(public_key), listener), daemon=True
    )
    api.start()
    try:
        wait_for_api(f"http://127.0.0.1:{args.port}", api)
        summary = run_experiment(args, output, generator, store)
        (output / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
        print(json.dumps({"output": str(output), "outcomes": summary["outcomes"]}, indent=2))
    finally:
        api.terminate()
        api.join(timeout=5)
        if api.is_alive():
            api.kill()
            api.join(timeout=5)
        listener.close()


if __name__ == "__main__":
    main()
