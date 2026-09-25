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
"""Measure current Celery recovery gaps using real broker, worker and HTTP processes."""

from __future__ import annotations

import argparse
import json
import multiprocessing
import os
import re
import shutil
import socket
from datetime import timedelta
from pathlib import Path
from uuid import UUID, uuid4

import httpx
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from airflow._shared.timezones import timezone
from airflow.api_fastapi.auth.tokens import JWTGenerator
from airflow.api_fastapi.execution_api.parsing import TOKEN_AUDIENCE, TOKEN_ISSUER, TOKEN_KEY_ID, TOKEN_SCOPE
from airflow.dag_processing.executor_worker import compute_source_revision
from airflow.dag_processing.parsing_state import ReceiptStore
from airflow.executors.workloads import BundleInfo, WorkloadType
from airflow.executors.workloads.parsing import DagDefinitionAttempt, DagDefinitionResult, ParseDagDefinitions

from dev.dag_parsing_poc.run import serve_api, wait_for_api
from dev.dag_parsing_poc.run_celery import read_json, read_task_events, wait_until


def write_json(path: Path, value: dict) -> None:
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(value, indent=2) + "\n")
    temporary.replace(path)


def validate_worker_ready(worker: dict, *, run_id: str, queue: str) -> None:
    if (
        worker.get("stage") != "worker_ready"
        or worker.get("run_id") != run_id
        or worker.get("queue") != queue
    ):
        raise RuntimeError("Worker readiness does not match this recovery run and queue")
    try:
        UUID(worker["worker_id"])
    except (KeyError, ValueError, TypeError) as error:
        raise RuntimeError("Worker readiness has no valid startup identity") from error


def matches_worker_event(event: dict, worker: dict, *, task_id: str, state: str) -> bool:
    return (
        event.get("task_id") == task_id
        and event.get("state") == state
        and all(event.get(key) == worker[key] for key in ("run_id", "worker_id", "container_hostname"))
    )


def read_import_events(path: Path) -> list[dict]:
    return [json.loads(line) for line in path.read_text().split("\n")[:-1]] if path.exists() else []


def validate_replacement(
    control: dict, original: dict, replacement: dict, *, run_id: str, queue: str
) -> None:
    if control.get("run_id") != run_id:
        raise RuntimeError("Docker evidence belongs to a different recovery run")
    for role, worker in (("original", original), ("replacement", replacement)):
        validate_worker_ready(worker, run_id=run_id, queue=queue)
        inspection = control[f"{role}_container"]
        container_id = inspection["Id"]
        hostname = worker.get("container_hostname")
        if (
            not re.fullmatch(r"[0-9a-f]{64}", container_id)
            or hostname != container_id[:12]
            or hostname != inspection["Config"]["Hostname"]
        ):
            raise RuntimeError(f"Docker inspection does not identify the {role} worker's container")
    if (
        control["original_container"]["Id"] == control["replacement_container"]["Id"]
        or original["worker_id"] == replacement["worker_id"]
    ):
        raise RuntimeError("Replacement must be a different worker and container")
    termination = control["original_container"]["State"]
    if termination["Running"] or termination["Pid"] != 0 or termination["ExitCode"] != 137:
        raise RuntimeError("Whole-worker kill was not confirmed by Docker")
    replacement_state = control["replacement_container"]["State"]
    if not replacement_state["Running"] or replacement_state["Pid"] <= 0:
        raise RuntimeError("Replacement worker container is not running")


def delete_running_backend_record(backend, task_id: str) -> int:
    if backend.get_task_meta(task_id, cache=False)["status"] != "STARTED":
        raise RuntimeError("Missing-record scenario requires an existing STARTED result")
    backend_key = backend.get_key_for_task(task_id)
    deleted = backend.client.delete(backend_key)
    if deleted != 1:
        raise RuntimeError("Missing-record scenario must delete exactly one Redis result key")
    if backend.client.exists(backend_key):
        raise RuntimeError("Redis result key still exists after deletion")
    return deleted


def create_executor():
    from airflow.providers.celery.executors.celery_executor import CeleryExecutor

    executor = CeleryExecutor(parallelism=1)
    executor.supported_workload_types = frozenset({WorkloadType.PARSE_DAG_DEFINITIONS})
    executor.start()
    return executor


def inspect_fresh_executor(store_path: str, workload_id: str, output_path: str) -> None:
    executor = create_executor()
    try:
        write_json(
            Path(output_path),
            {
                "pid": os.getpid(),
                "running": len(executor.running),
                "tracked_workloads": len(executor.workloads),
                "available_slots": executor.slots_available,
                "receipts": ReceiptStore(store_path).get_attempts(workload_id),
                "reconstruction_attempted": False,
            },
        )
    finally:
        executor.end()


def create_workload(paths: list[Path], generator: JWTGenerator, *, queue: str, stop_seconds: float):
    definitions = tuple(
        DagDefinitionAttempt(
            attempt_id=uuid4(),
            relative_path=path.name,
            source_revision=compute_source_revision(path),
            timeout_seconds=120 if path.stem == "interrupted" else 10,
        )
        for path in paths
    )
    workload_id = uuid4()
    now = timezone.utcnow()
    return ParseDagDefinitions(
        workload_id=workload_id,
        bundle_info=BundleInfo(name="poc", version="v1"),
        definitions=definitions,
        start_deadline=now + timedelta(seconds=stop_seconds / 2),
        stop_deadline=now + timedelta(seconds=stop_seconds),
        queue=queue,
        token=generator.generate(
            extras={
                "sub": str(workload_id),
                "scope": TOKEN_SCOPE,
                "attempt_ids": [str(definition.attempt_id) for definition in definitions],
            }
        ),
    )


def write_fixtures(output: Path, *, recoverable: bool = False) -> list[Path]:
    source = output / "submitter-bundle"
    source.mkdir()
    paths = []
    for name in ("accepted", "interrupted", "unrelated"):
        path = source / f"{name}.py"
        body = (
            "from pathlib import Path\nimport json\nimport os\nimport socket\nimport time\nfrom airflow.sdk import DAG\n"
            f"with Path('/worker-evidence/{name}.imports').open('a') as marker:\n"
            "    marker.write(json.dumps({\n"
            "        'run_id': os.environ['AIRFLOW_DAG_PARSING_POC_RUN_ID'],\n"
            "        'worker_id': os.environ['AIRFLOW_DAG_PARSING_POC_WORKER_ID'],\n"
            "        'container_hostname': socket.gethostname(),\n"
            "        'task_id': os.environ['AIRFLOW_DAG_PARSING_POC_TASK_ID'],\n"
            "        'state': 'IMPORT_STARTED',\n"
            "    }) + '\\n')\n"
        )
        if name == "interrupted":
            if recoverable:
                body += "if len(Path('/worker-evidence/interrupted.imports').read_text().splitlines()) == 1:\n    time.sleep(120)\n"
            else:
                body += "time.sleep(120)\n"
            body += "Path('/worker-evidence/interrupted.finished').touch()\n"
        body += f"with DAG('recovery_{name}', schedule=None):\n    pass\n"
        path.write_text(body)
        paths.append(path)
    shutil.copytree(source, output / "worker-bundle")
    (output / "worker-evidence").mkdir()
    return paths


def snapshot_executor(executor, workload) -> dict:
    executor.sync()
    events = {
        str(key): {"state": state.value, "info": str(info) if info else None}
        for key, (state, info) in executor.get_event_buffer().items()
    }
    return {
        "backend_state": executor.celery_app.backend.get_task_meta(str(workload.workload_id), cache=False)[
            "status"
        ],
        "available_slots": executor.slots_available,
        "running": len(executor.running),
        "tracked_workloads": len(executor.workloads),
        "queued": sum(len(queue) for queue in executor.executor_queues.values()),
        "events": events,
    }


def check_late_publication(url: str, workload, store: ReceiptStore, original: list[dict]) -> dict:
    accepted_result = store.get_results(workload.workload_id)[0]
    definition = workload.definitions[1]
    late_result = DagDefinitionResult(
        attempt_id=definition.attempt_id,
        relative_path=definition.relative_path,
        source_revision=definition.source_revision,
        outcome="worker_error",
        duration_seconds=0,
        diagnostics=["Synthetic late publication probe after observed worker termination"],
    ).model_dump(mode="json")
    bodies = [
        (0, accepted_result, original[0]["execution_id"], 200),
        (
            0,
            {**accepted_result, "diagnostics": ["conflicting synthetic retry"]},
            original[0]["execution_id"],
            409,
        ),
        (1, late_result, original[1]["execution_id"], 410),
        (1, late_result, str(uuid4()), 409),
    ]
    statuses = []
    with httpx.Client(
        timeout=5, trust_env=False, headers={"Authorization": f"Bearer {workload.token}"}
    ) as client:
        for index, result, execution_id, expected in bodies:
            attempt = workload.definitions[index]
            response = client.post(
                f"{url}/execution/poc/parsing/workloads/{workload.workload_id}/attempts/{attempt.attempt_id}/result",
                json={"execution_id": execution_id, "result": result},
            )
            if response.status_code != expected:
                raise RuntimeError(f"Late publication returned {response.status_code}, expected {expected}")
            if expected == 200 and response.json()["digest"] != original[0]["digest"]:
                raise RuntimeError("Accepted receipt digest changed during replay")
            statuses.append(response.status_code)
    if store.get_attempts(workload.workload_id) != original:
        raise RuntimeError("Late publication changed durable receipts")
    return {
        "accepted_identical_replay": statuses[0],
        "accepted_conflicting_replay": statuses[1],
        "unaccepted_original_execution": statuses[2],
        "unaccepted_wrong_execution": statuses[3],
        "source": "synthetic trusted-driver HTTP requests with a still-valid workload token",
    }


def run_experiment(args, output: Path, store: ReceiptStore, generator: JWTGenerator, url: str) -> dict:
    os.environ["AIRFLOW__CELERY__BROKER_URL"] = args.broker_url
    os.environ["AIRFLOW__CELERY__RESULT_BACKEND"] = args.result_backend
    os.environ["AIRFLOW__CELERY__SYNC_PARALLELISM"] = "1"
    os.environ["AIRFLOW__CELERY_BROKER_TRANSPORT_OPTIONS__VISIBILITY_TIMEOUT"] = "3600"
    files = write_fixtures(output, recoverable=args.recover)
    run_id = str(uuid4())
    write_json(output / "driver-ready.json", {"port": args.port, "queue": args.queue, "run_id": run_id})
    wait_until(
        lambda: read_json(output / "worker-evidence" / "isolation.json").get("stage") == "worker_ready",
        description="original isolated worker",
        timeout=180,
    )
    original_worker = read_json(output / "worker-evidence" / "isolation.json")
    validate_worker_ready(original_worker, run_id=run_id, queue=args.queue)
    workload = create_workload(files[:2], generator, queue=args.queue, stop_seconds=args.deadline_seconds)
    if args.recover:
        from airflow.dag_processing.executor_recovery import PublicationOutcome

        from dev.dag_parsing_poc.recovery_checkpoint import create_coordinator

        coordinator = create_coordinator(store, args.queue, generator)
        coordinator.start()
        coordinator.admit(workload)
        executor = coordinator.executor
    else:
        store.register_workload(workload)
        executor = create_executor()
    try:
        if args.recover:
            if coordinator.dispatch_reserved() != [
                PublicationOutcome(str(workload.workload_id), "published")
            ]:
                raise RuntimeError("Initial workload publication was not acknowledged")
        else:
            executor.queue_workload(workload, session=None)
            executor.heartbeat()
        wait_until(
            lambda: (
                [row["status"] for row in store.get_attempts(workload.workload_id)] == ["accepted", "claimed"]
                and len(read_import_events(output / "worker-evidence" / "interrupted.imports")) == 1
            ),
            description="accepted first definition and active second import",
            timeout=30,
        )
        original = store.get_attempts(workload.workload_id)
        task_id = str(workload.workload_id)
        if not any(
            matches_worker_event(event, original_worker, task_id=task_id, state="STARTED")
            for event in read_task_events(output, task_id)
        ):
            raise RuntimeError("Original delivery evidence does not identify the ready worker")
        for name in ("accepted", "interrupted"):
            imports = read_import_events(output / "worker-evidence" / f"{name}.imports")
            if len(imports) != 1 or not matches_worker_event(
                imports[0], original_worker, task_id=task_id, state="IMPORT_STARTED"
            ):
                raise RuntimeError("Import evidence does not identify the original delivery and worker")
        first = store.get_results(workload.workload_id)[0]
        if first["outcome"] != "success" or [
            item.get("dag", {}).get("dag_id") for item in first["serialized_dags"]
        ] != ["recovery_accepted"]:
            raise RuntimeError("First definition did not publish the expected successful Dag")
        write_json(
            output / "kill-request.json",
            {
                "action": "kill the whole original worker container, save Docker termination evidence, start a replacement",
                "workload_id": str(workload.workload_id),
                "run_id": run_id,
                "original_worker": original_worker,
                "receipts": original,
                "before_kill": snapshot_executor(executor, workload),
                "stop_deadline": workload.stop_deadline.isoformat(),
            },
        )
        wait_until(
            lambda: (output / "replacement-ready.json").exists(),
            description="operator-confirmed worker termination and replacement readiness",
            timeout=180,
        )
        control = read_json(output / "replacement-ready.json")
        replacement_worker = read_json(output / "worker-evidence" / "isolation.json")
        validate_replacement(control, original_worker, replacement_worker, run_id=run_id, queue=args.queue)
        after_kill = snapshot_executor(executor, workload)
        if (after_kill["running"], after_kill["tracked_workloads"]) != (1, 1):
            raise RuntimeError(
                "Worker loss produced terminal provider evidence; missing-record scenario needs a live tracked submission"
            )
        if store.get_attempts(workload.workload_id) != original:
            raise RuntimeError("Partial receipts changed after worker loss")

        backend = executor.celery_app.backend
        deleted = delete_running_backend_record(backend, task_id)
        after_delete = snapshot_executor(executor, workload)
        if after_delete["backend_state"] != "PENDING" or after_delete["available_slots"] != 0:
            raise RuntimeError("Missing backend record did not retain the tracked executor slot")

        if args.recover:
            from dev.dag_parsing_poc.recovery_checkpoint import run_checkpoint

            return run_checkpoint(
                args,
                output,
                store,
                generator,
                workload,
                original,
                original_worker,
                replacement_worker,
                control,
                after_delete,
                deleted,
                url,
            )

        unrelated = create_workload([files[2]], generator, queue=args.queue, stop_seconds=300)
        store.register_workload(unrelated)
        executor.queue_workload(unrelated, session=None)
        executor.heartbeat()
        if len(executor.executor_queues[WorkloadType.PARSE_DAG_DEFINITIONS]) != 1:
            raise RuntimeError("Unrelated work was unexpectedly admitted through an occupied executor")
        task = executor.celery_app.tasks["execute_workload"]
        task.apply_async(
            args=[workload.model_dump_json()],
            queue=args.queue,
            task_id=task_id,
            argsrepr="(<redacted parsing workload>,)",
        )
        wait_until(
            lambda: any(
                matches_worker_event(event, replacement_worker, task_id=task_id, state="IGNORED")
                for event in read_task_events(output, task_id)
            ),
            description="replacement ignoring the unfinished original claim",
            timeout=30,
        )
        after_replay = snapshot_executor(executor, workload)
        if store.get_attempts(workload.workload_id) != original:
            raise RuntimeError("Replacement took over an unfinished claim or changed an accepted receipt")
        if (after_replay["running"], after_replay["tracked_workloads"], after_replay["queued"]) != (1, 1, 1):
            raise RuntimeError("Ignored redelivery changed tracked or queued work")

        fresh_path = output / "fresh-executor.json"
        observer = multiprocessing.get_context("spawn").Process(
            target=inspect_fresh_executor,
            args=(store.path, task_id, str(fresh_path)),
        )
        observer.start()
        observer.join(timeout=30)
        if observer.is_alive():
            observer.kill()
            observer.join(timeout=5)
            raise TimeoutError("Fresh executor process did not exit")
        if observer.exitcode != 0:
            raise RuntimeError("Fresh executor process failed")
        fresh = read_json(fresh_path)
        if fresh["pid"] != observer.pid or fresh["pid"] == os.getpid():
            raise RuntimeError("Executor observation did not come from a fresh spawned process")
        if (fresh["running"], fresh["tracked_workloads"], fresh["available_slots"]) != (0, 0, 1) or fresh[
            "receipts"
        ] != original:
            raise RuntimeError(
                "Fresh executor or reopened receipt state differed from the expected prototype gap"
            )

        wait_until(
            lambda: timezone.utcnow() > workload.stop_deadline,
            description="original stop deadline",
            timeout=args.deadline_seconds + 5,
        )
        late = check_late_publication(url, workload, store, original)
        executor.heartbeat()
        final = snapshot_executor(executor, workload)
        if (final["running"], final["tracked_workloads"], final["queued"]) != (1, 1, 1):
            raise RuntimeError("Expiry changed the unresolved slot or admitted unrelated work")
        counts = {
            path.stem: len(path.read_text().splitlines())
            for path in (output / "worker-evidence").glob("*.imports")
        }
        if (
            counts != {"accepted": 1, "interrupted": 1}
            or (output / "worker-evidence" / "interrupted.finished").exists()
        ):
            raise RuntimeError(f"Unexpected reimport or completion after worker loss: {counts}")
        write_json(output / "accepted-results.json", {"results": store.get_results(workload.workload_id)})
        return {
            "mode": "whole-worker loss, real Redis and HTTP, explicit duplicate injection",
            "workload_id": task_id,
            "run_id": run_id,
            "receipts": original,
            "after_kill": after_kill,
            "deleted_backend_records": deleted,
            "after_backend_deletion": after_delete,
            "after_replay": after_replay,
            "fresh_executor": fresh,
            "after_deadline": final,
            "late_publication": late,
            "import_counts": counts,
            "termination": control,
            "original_worker_isolation": original_worker,
            "replacement_worker_isolation": replacement_worker,
            "limitations": [
                "A fresh executor process was inspected; no scheduler, ownership lease or automatic adoption exists.",
                "Explicit replay does not exercise Redis visibility-timeout redelivery or broker restart.",
                "Late publication requests were synthesized by the trusted driver, not sent by the killed worker.",
                "Docker termination evidence applies only to this killed container, not general Celery cancellation.",
                "This measures missing recovery; it does not add claim retirement, replacement attempts or a durable capacity ledger.",
            ],
        }
    finally:
        executor.end()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--broker-url", required=True)
    parser.add_argument("--result-backend", required=True)
    parser.add_argument("--queue", default="poc-recovery")
    parser.add_argument("--port", type=int, default=8799)
    parser.add_argument("--deadline-seconds", type=float, default=90)
    parser.add_argument("--recover", action="store_true", help="Exercise durable admission and replacement")
    args = parser.parse_args()
    if not 30 <= args.deadline_seconds <= 300:
        parser.error("deadline-seconds must be between 30 and 300")
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    key = Ed25519PrivateKey.generate()
    public_key = output / "verification-key.pem"
    public_key.write_bytes(
        key.public_key().public_bytes(
            serialization.Encoding.PEM,
            serialization.PublicFormat.SubjectPublicKeyInfo,
        )
    )
    generator = JWTGenerator(
        private_key=key,
        kid=TOKEN_KEY_ID,
        issuer=TOKEN_ISSUER,
        audience=TOKEN_AUDIENCE,
        algorithm="EdDSA",
        valid_for=900,
    )
    store = ReceiptStore(output / "receipts.sqlite")
    listener = socket.socket()
    listener.bind(("0.0.0.0", args.port))
    api = multiprocessing.get_context("spawn").Process(
        target=serve_api,
        args=(store.path, str(public_key), listener),
        daemon=True,
    )
    api.start()
    try:
        url = f"http://127.0.0.1:{args.port}"
        wait_for_api(url, api)
        summary = run_experiment(args, output, store, generator, url)
        write_json(output / "summary.json", summary)
        print(json.dumps({"output": str(output), "recovery_implemented": args.recover}, indent=2))
    finally:
        api.terminate()
        api.join(timeout=5)
        if api.is_alive():
            api.kill()
            api.join(timeout=5)
        listener.close()


if __name__ == "__main__":
    main()
