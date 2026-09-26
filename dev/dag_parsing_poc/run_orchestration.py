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
"""Run repeated SDK parsing cycles with an independently restartable orchestrator."""

from __future__ import annotations

import argparse
import json
import multiprocessing
import os
import shutil
import socket
import time
from dataclasses import asdict
from pathlib import Path
from uuid import uuid4

import uvicorn
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from airflow.api_fastapi.auth.tokens import JWTGenerator
from airflow.api_fastapi.execution_api.parsing import (
    TOKEN_AUDIENCE,
    TOKEN_ISSUER,
    TOKEN_KEY_ID,
    TOKEN_SCOPE,
    create_app,
)
from airflow.configuration import conf
from airflow.dag_processing.discovery import discover_python_bundle
from airflow.dag_processing.executor_runner import ParsingExecutorRunner
from airflow.dag_processing.orchestrator import OrchestrationStore, ParseOrchestrator
from airflow.executors.local_executor import LocalExecutor
from airflow.executors.workloads import BundleInfo, WorkloadType

from dev.dag_parsing_poc.run import wait_for_api

ROUTE = "poc-orchestration"
BUNDLE = BundleInfo(name="poc", version="v1")


def run_host(store_path: str, source: Path, trace: Path, stop, interval: float) -> None:
    orchestrator = ParseOrchestrator(
        OrchestrationStore(store_path),
        route=ROUTE,
        bundle=BUNDLE.name,
        batch_size=3,
        parse_interval=interval,
    )
    while not stop.is_set():
        orchestrator.update_inventory(BUNDLE, discover_python_bundle(source))
        started = time.monotonic()
        result = orchestrator.step()
        with trace.open("a") as stream:
            stream.write(
                json.dumps({**asdict(result), "pid": os.getpid(), "step_seconds": time.monotonic() - started})
                + "\n"
            )
        stop.wait(0.1)


def run_provider(store_path: str, key_bytes: bytes, stop) -> None:
    generator = JWTGenerator(
        private_key=Ed25519PrivateKey.from_private_bytes(key_bytes),
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
                "attempt_ids": [definition["attempt_id"] for definition in manifest["definitions"]],
            }
        )

    executor = LocalExecutor(parallelism=1)
    executor.supported_workload_types = frozenset({WorkloadType.PARSE_DAG_DEFINITIONS})
    runner = ParsingExecutorRunner(
        OrchestrationStore(store_path), executor, route=ROUTE, token_issuer=issue_token
    )
    runner.start()
    try:
        while not stop.is_set():
            runner.tick()
            stop.wait(0.02)
    finally:
        runner.close()


def run_api(store_path: str, public: Path, listener: socket.socket) -> None:
    uvicorn.Server(
        uvicorn.Config(create_app(store_path, public, orchestrated=True), log_level="warning")
    ).run(sockets=[listener])


def wait_until(predicate, processes, *, description: str, timeout: float = 90):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        for process in processes:
            if not process.is_alive():
                raise RuntimeError(f"{process.name} exited with {process.exitcode}: {description}")
        if value := predicate():
            return value
        time.sleep(0.05)
    raise TimeoutError(description)


def run_checkpoint(output: Path, interval: float) -> dict:
    output.mkdir(parents=True, exist_ok=False)
    source, worker = output / "source", output / "worker"
    source.mkdir()
    evidence = output / "imports"
    evidence.mkdir()
    for name in ("a", "b", "broken"):
        (source / f"{name}.py").write_text(
            "from airflow.sdk import DAG\nfrom pathlib import Path\nimport time\n"
            f"with Path({str(evidence / name)!r}).open('a') as marker:\n    marker.write('imported\\n')\n"
            + ("time.sleep(2)\n" if name == "a" else "")
            + (
                "raise ValueError('expected import failure')\n"
                if name == "broken"
                else f"dag = DAG('{name}_v1', schedule=None)\n"
            )
        )
    shutil.copytree(source, worker)
    key = Ed25519PrivateKey.generate()
    public = output / "public.pem"
    public.write_bytes(
        key.public_key().public_bytes(
            serialization.Encoding.PEM, serialization.PublicFormat.SubjectPublicKeyInfo
        )
    )
    store = OrchestrationStore(output / "receipts.sqlite")
    context = multiprocessing.get_context("spawn")
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    url = f"http://127.0.0.1:{listener.getsockname()[1]}"
    os.environ["AIRFLOW__CORE__EXECUTION_API_SERVER_URL"] = url + "/execution/"
    os.environ["AIRFLOW_DAG_PARSING_POC_BUNDLE_ROOTS"] = json.dumps(
        {"poc": {"path": str(worker), "version": "v1"}}
    )
    os.environ["AIRFLOW_DAG_PARSING_POC_LOG_DIR"] = str(output / "worker-logs")
    configured_executor = conf.get("core", "executor")
    task_executor = LocalExecutor(parallelism=2)
    api = context.Process(target=run_api, args=(store.path, public, listener), name="receipt-api")
    provider_stop, host_stop = context.Event(), context.Event()
    provider = context.Process(
        target=run_provider,
        args=(
            store.path,
            key.private_bytes(
                serialization.Encoding.Raw, serialization.PrivateFormat.Raw, serialization.NoEncryption()
            ),
            provider_stop,
        ),
        name="executor-runner",
    )
    trace = output / "steps.jsonl"

    def create_host():
        nonlocal host_stop
        # A killed process may leave its Event's condition locked. Never share it with its successor.
        host_stop = context.Event()
        return context.Process(
            target=run_host, args=(store.path, source, trace, host_stop, interval), name="orchestrator"
        )

    host = create_host()
    api.start()
    listener.close()
    processes = [api]
    host_pids = []
    try:
        wait_for_api(url, api)
        host.start()
        processes.append(host)
        host_pids.append(host.pid)
        admitted = wait_until(
            lambda: store.get_admissions(ROUTE), processes, description="initial admission"
        )[0]
        workload_id = admitted["workload_id"]
        host.terminate()
        host.join(10)
        processes.remove(host)
        host = create_host()
        host.start()
        processes.append(host)
        host_pids.append(host.pid)
        provider.start()
        processes.append(provider)
        wait_until(
            lambda: any(row["status"] == "claimed" for row in store.get_attempts(workload_id)),
            processes,
            description="live claimed import",
        )
        host.terminate()
        host.join(10)
        processes.remove(host)
        host = create_host()
        host.start()
        processes.append(host)
        host_pids.append(host.pid)
        wait_until(
            lambda: (
                not store.get_admissions(ROUTE)
                and all(row["accepted_count"] == 1 for row in store.get_sources(ROUTE, "poc"))
            ),
            processes,
            description="first cycle after two host restarts",
        )
        first = store.get_results(workload_id)
        if sorted(result["outcome"] for result in first) != ["import_error", "success", "success"]:
            raise RuntimeError("Unexpected first cycle results")
        changed = (source / "a.py").read_text().replace("a_v1", "a_v2")
        for root in (worker, source):
            temporary = root / "a.tmp"
            temporary.write_text(changed)
            temporary.replace(root / "a.py")
        wait_until(
            lambda: store.get_sources(ROUTE, "poc")[0]["accepted_count"] == 2,
            processes,
            description="changed definition",
        )
        after_change = store.get_sources(ROUTE, "poc")
        if [row["accepted_count"] for row in after_change] != [2, 1, 1]:
            raise RuntimeError("Unchanged definitions were reimported before their interval")
        admissions = store.get_admissions(ROUTE, include_released=True)
        changed_results = [
            r for a in admissions for r in store.get_results(a["workload_id"]) if r["relative_path"] == "a.py"
        ]
        if not any(d["dag"]["dag_id"] == "a_v2" for r in changed_results for d in r["serialized_dags"]):
            raise RuntimeError("Changed source was not serialized")
        wait_until(
            lambda: (
                all(
                    row["accepted_count"] >= target
                    for row, target in zip(store.get_sources(ROUTE, "poc"), (3, 2, 2))
                )
                and not store.get_admissions(ROUTE)
            ),
            processes,
            description="periodic reparse",
            timeout=interval + 60,
        )
        host_stop.set()
        host.join(10)
        if host.is_alive() or host.exitcode != 0:
            raise RuntimeError("Orchestrator did not stop cleanly")
        processes.remove(host)
        final = store.get_sources(ROUTE, "poc")
        imports = {
            row["path"]: len((evidence / Path(row["path"]).stem).read_text().splitlines()) for row in final
        }
        if imports != {row["path"]: row["accepted_count"] for row in final}:
            raise RuntimeError("Duplicate or unaccepted imports detected")
        if task_executor.slots_available != 2 or conf.get("core", "executor") != configured_executor:
            raise RuntimeError("Task executor capacity or routing changed")
        steps = [json.loads(line) for line in trace.read_text().splitlines()]
        return {
            "status": "passed",
            "mode": "standalone orchestrator + separate LocalExecutor runner + HTTP receipt API",
            "host_pids": host_pids,
            "runner_pid": provider.pid,
            "imports": imports,
            "after_change_counts": {row["path"]: row["accepted_count"] for row in after_change},
            "initial_workload_id": workload_id,
            "first_outcomes": [result["outcome"] for result in first],
            "steps": len(steps),
            "max_step_seconds": max(row["step_seconds"] for row in steps),
            "active_reservations": len(store.get_admissions(ROUTE)),
            "independent_task_slots": task_executor.slots_available,
            "limitations": [
                "Single owner; trusted local discovery; no scheduler hosting or HA.",
                "Runner restart requires external termination reconciliation.",
                "Receipt storage, not production metadata ingestion or Dag deactivation.",
                "Step item bounds do not bound SQLite lock wait or establish performance.",
            ],
        }
    finally:
        host_stop.set()
        provider_stop.set()
        for process in (host, provider):
            if process.pid is not None:
                process.join(40)
                if process.is_alive():
                    process.terminate()
                    process.join(10)
        api.terminate()
        api.join(10)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=Path("/files/dag-parsing-aip/poc-runs"))
    parser.add_argument("--interval", type=float, default=20)
    args = parser.parse_args()
    if args.interval < 15:
        parser.error("Use an interval of at least 15 seconds to separate the checkpoints")
    output = args.output.resolve() / ("orchestration-" + uuid4().hex[:12])
    summary = run_checkpoint(output, args.interval)
    (output / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    print(json.dumps({"output": str(output), **summary}, indent=2))


if __name__ == "__main__":
    main()
