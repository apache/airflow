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
"""Run the standalone LocalExecutor parsing experiment inside Breeze."""

from __future__ import annotations

import argparse
import hashlib
import json
import multiprocessing
import os
import shutil
import socket
import time
from collections import Counter
from datetime import timedelta
from pathlib import Path
from typing import TYPE_CHECKING
from uuid import uuid4
from zipfile import ZipFile

import httpx
import uvicorn
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from airflow._shared.timezones import timezone
from airflow.api_fastapi.auth.tokens import JWTGenerator
from airflow.configuration import conf
from airflow.dag_processing.executor_worker import ParsingAPIClient, compute_source_revision, parse_definition
from airflow.executors.local_executor import LocalExecutor
from airflow.executors.workloads import BundleInfo, WorkloadType
from airflow.executors.workloads.parsing import (
    DagDefinitionAttempt,
    ParseDagDefinitions,
    ParseDagDefinitionsState,
)
from airflow.sdk.importers.zip_importer import ZipMemberDagDefinition

from dev.dag_parsing_poc.api import (
    TOKEN_AUDIENCE,
    TOKEN_ISSUER,
    TOKEN_KEY_ID,
    TOKEN_SCOPE,
    create_app,
)
from dev.dag_parsing_poc.store import ReceiptStore

if TYPE_CHECKING:
    from multiprocessing.process import BaseProcess


def serve_api(store_path: str, public_key_path: str, listener: socket.socket) -> None:
    server = uvicorn.Server(uvicorn.Config(create_app(store_path, public_key_path), log_level="warning"))
    server.run(sockets=[listener])


def wait_for_api(url: str, process: BaseProcess) -> None:
    deadline = time.monotonic() + 30
    with httpx.Client(timeout=1, trust_env=False) as client:
        while time.monotonic() < deadline:
            if not process.is_alive():
                raise RuntimeError(f"Prototype API exited during startup: {process.exitcode}")
            try:
                if client.get(f"{url}/health").status_code == 200:
                    return
            except httpx.TransportError:
                pass
            time.sleep(0.05)
    raise TimeoutError("Prototype API did not become ready")


def write_fixtures(root: Path, count: int, include_failures: bool) -> list[Path]:
    root.mkdir(parents=True)
    files = []
    for index in range(count):
        path = root / f"success_{index}.py"
        path.write_text(
            "from airflow.sdk import DAG, task\n\n"
            f"with DAG('executor_parsing_poc_{index}', schedule=None):\n"
            "    @task\n"
            "    def sample_task():\n"
            "        return 1\n"
            "    sample_task()\n"
        )
        files.append(path)
    if include_failures:
        broken = root / "import_error.py"
        broken.write_text("from airflow.sdk import DAG\nraise ValueError('intentional PoC import failure')\n")
        slow = root / "timeout.py"
        slow.write_text("import time\nfrom airflow.sdk import DAG\ntime.sleep(30)\n")
        files.extend([broken, slow])
    return files


def create_archive(files: list[Path], archive: Path) -> Path:
    with ZipFile(archive, "w") as stream:
        for path in files:
            stream.write(path, arcname=path.name)
    return archive


def create_workloads(
    files: list[Path],
    *,
    batch_size: int,
    timeout: float,
    generator: JWTGenerator,
    archive_path: Path | None = None,
) -> list[ParseDagDefinitions]:
    workloads = []
    archive_revision = compute_source_revision(archive_path) if archive_path else None
    for offset in range(0, len(files), batch_size):
        definitions = tuple(
            DagDefinitionAttempt(
                attempt_id=uuid4(),
                relative_path=f"{archive_path.name}/{path.name}" if archive_path else path.name,
                source_revision=(
                    hashlib.sha256(
                        ZipMemberDagDefinition(zip_path=archive_path, file_path=path.name).read_bytes()
                    ).hexdigest()
                    if archive_path
                    else compute_source_revision(path)
                ),
                timeout_seconds=0.5 if path.name == "timeout.py" else timeout,
                archive_path=archive_path.name if archive_path else None,
                archive_revision=archive_revision,
            )
            for path in files[offset : offset + batch_size]
        )
        workload_id = uuid4()
        now = timezone.utcnow()
        workloads.append(
            ParseDagDefinitions(
                workload_id=workload_id,
                bundle_info=BundleInfo(name="poc", version="v1"),
                definitions=definitions,
                start_deadline=now + timedelta(seconds=240),
                stop_deadline=now + timedelta(seconds=300),
                token=generator.generate(
                    extras={
                        "sub": str(workload_id),
                        "scope": TOKEN_SCOPE,
                        "attempt_ids": [str(attempt.attempt_id) for attempt in definitions],
                    }
                ),
            )
        )
    return workloads


def run_baseline(workloads: list[ParseDagDefinitions], root: Path, url: str, output: Path) -> dict:
    started = time.monotonic()
    results = []
    for workload in workloads:
        with ParsingAPIClient(base_url=f"{url}/execution/", token=workload.token) as client:
            for definition in workload.definitions:
                result = parse_definition(
                    workload,
                    definition,
                    bundle_root=root,
                    client=client,
                    log_dir=output / "baseline-logs",
                    legacy=True,
                )
                results.append(result.model_dump(mode="json"))
    return {
        "label": "serial supervised legacy parser; excludes manager discovery and scheduling",
        "elapsed_seconds": time.monotonic() - started,
        "results": results,
    }


def run_executor(workloads: list[ParseDagDefinitions], *, parallelism: int, max_wait: float) -> dict:
    configured_executor = conf.get("core", "executor")
    task_executor = LocalExecutor(parallelism=2)
    parser = LocalExecutor(parallelism=parallelism)
    parser.supported_workload_types = frozenset({WorkloadType.PARSE_DAG_DEFINITIONS})
    terminal: dict[str, dict[str, str | None]] = {}
    started = time.monotonic()
    parser.start()
    completed = False
    try:
        for workload in workloads:
            parser.queue_workload(workload, session=None)
        while len(terminal) < len(workloads):
            if time.monotonic() - started > max_wait:
                raise TimeoutError("Executor experiment exceeded its bounded wait")
            parser.heartbeat()
            for key, (state, info) in parser.get_event_buffer().items():
                if state in {ParseDagDefinitionsState.SUCCESS, ParseDagDefinitionsState.FAILED}:
                    terminal[str(key)] = {"state": state.value, "info": str(info) if info else None}
            if task_executor.slots_available != 2:
                raise RuntimeError("Parsing changed the independent task executor capacity")
            time.sleep(0.02)
        completed = True
    finally:
        if not completed:
            parser.terminate()
        parser.end()
    if conf.get("core", "executor") != configured_executor:
        raise RuntimeError("Prototype modified configured task executor routing")
    return {
        "elapsed_seconds": time.monotonic() - started,
        "events": terminal,
        "parsing_parallelism": parallelism,
        "independent_task_slots": task_executor.slots_available,
        "configured_task_executor": configured_executor,
        "normal_local_executor_supports_parsing": WorkloadType.PARSE_DAG_DEFINITIONS
        in task_executor.supported_workload_types,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=Path("/files/dag-parsing-aip/poc-runs"))
    parser.add_argument("--definitions", type=int, default=2)
    parser.add_argument("--batch-size", type=int, choices=range(1, 101), default=2)
    parser.add_argument("--parallelism", type=int, default=1)
    parser.add_argument("--timeout", type=float, default=20)
    parser.add_argument("--baseline", action="store_true")
    parser.add_argument("--success-only", action="store_true")
    parser.add_argument("--archive-members", action="store_true")
    args = parser.parse_args()
    if args.archive_members and args.baseline:
        parser.error("The legacy baseline compares ordinary files; run archive members separately")
    if args.definitions < 1 or args.parallelism < 1 or args.timeout <= 0:
        parser.error("definitions, parallelism and timeout must be positive")
    output = args.output.resolve() / uuid4().hex[:12]
    output.mkdir(parents=True)
    submitter_root = output / "submitter-bundle"
    worker_root = output / "worker-bundle"
    files = write_fixtures(submitter_root, args.definitions, not args.success_only)
    archive = create_archive(files, submitter_root / "definitions.zip") if args.archive_members else None
    shutil.copytree(submitter_root, worker_root)
    signing_key = Ed25519PrivateKey.generate()
    public_key_path = output / "verification-key.pem"
    public_key_path.write_bytes(
        signing_key.public_key().public_bytes(
            serialization.Encoding.PEM, serialization.PublicFormat.SubjectPublicKeyInfo
        )
    )
    generator = JWTGenerator(
        private_key=signing_key,
        kid=TOKEN_KEY_ID,
        issuer=TOKEN_ISSUER,
        audience=TOKEN_AUDIENCE,
        algorithm="EdDSA",
        valid_for=360,
    )
    store_path = output / "receipts.sqlite"
    store = ReceiptStore(store_path)
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    url = f"http://127.0.0.1:{listener.getsockname()[1]}"
    api = multiprocessing.get_context("spawn").Process(
        target=serve_api, args=(str(store_path), str(public_key_path), listener), daemon=True
    )
    api.start()
    baseline = None
    try:
        wait_for_api(url, api)
        if args.baseline:
            baseline = run_baseline(
                create_workloads(
                    files, batch_size=args.batch_size, timeout=args.timeout, generator=generator
                ),
                worker_root,
                url,
                output,
            )
        workloads = create_workloads(
            files, batch_size=args.batch_size, timeout=args.timeout, generator=generator, archive_path=archive
        )
        for workload in workloads:
            store.register_workload(workload)
        os.environ["AIRFLOW__CORE__EXECUTION_API_SERVER_URL"] = f"{url}/execution/"
        os.environ["AIRFLOW_DAG_PARSING_POC_BUNDLE_ROOTS"] = json.dumps(
            {"poc": {"path": str(worker_root), "version": "v1"}}
        )
        os.environ["AIRFLOW_DAG_PARSING_POC_LOG_DIR"] = str(output / "worker-logs")
        run = run_executor(workloads, parallelism=args.parallelism, max_wait=310)
        results = [result for workload in workloads for result in store.get_results(workload.workload_id)]
        outcomes = dict(Counter(result["outcome"] for result in results))
        received_dags = {
            result["relative_path"]: [dag.get("dag", {}).get("dag_id") for dag in result["serialized_dags"]]
            for result in results
            if result["outcome"] == "success"
        }
        expected_dags = {
            f"{'definitions.zip/' if archive else ''}success_{index}.py": [f"executor_parsing_poc_{index}"]
            for index in range(args.definitions)
        }
        expected = {"success": args.definitions}
        if not args.success_only:
            expected.update({"import_error": 1, "timeout": 1})
        summary = {
            "mode": "standalone LocalExecutor; development authenticated result API",
            "importer": "SDK",
            "definition_kind": "zip_member" if archive else "file",
            "output": str(output),
            "submitter_root": str(submitter_root),
            "worker_root": str(worker_root),
            "definitions": len(files),
            "batch_size": args.batch_size,
            "dispatches": len(workloads),
            "executor": run,
            "outcomes": outcomes,
            "expected_outcomes": expected,
            "received_dags": received_dags,
            "expected_dags": expected_dags,
            "baseline": baseline,
            "limitations": [
                "Receipts are not production Airflow metadata ingestion.",
                "Baseline excludes the current manager pool, discovery, and scheduling.",
                "Local processes share host trust; remote credential isolation is untested.",
                "Task capacity is inspected; concurrent task execution is not exercised.",
                "Celery, Kubernetes, HA ownership and shared admission are not exercised.",
            ],
        }
        (output / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
        (output / "results.json").write_text(json.dumps(results, indent=2) + "\n")
        print(json.dumps({"output": str(output), "outcomes": outcomes, "expected": expected}, indent=2))
        if (
            outcomes != expected
            or received_dags != expected_dags
            or any(item["state"] != "success" for item in run["events"].values())
        ):
            raise RuntimeError(f"PoC did not produce the expected results; inspect {output}")
    finally:
        api.terminate()
        api.join(timeout=5)
        if api.is_alive():
            api.kill()
            api.join(timeout=5)
        listener.close()


if __name__ == "__main__":
    main()
