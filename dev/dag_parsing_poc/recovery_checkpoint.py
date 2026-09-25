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
"""Recovery mode for the controlled, single-worker Celery loss experiment."""

from __future__ import annotations

import multiprocessing
import os
import re
from pathlib import Path
from uuid import uuid4

import httpx

from airflow._shared.timezones import timezone
from airflow.executors.workloads import WorkloadType
from airflow.executors.workloads.parsing import DagDefinitionResult

from dev.dag_parsing_poc.coordinator import ParsingRecoveryCoordinator, PublicationOutcome
from dev.dag_parsing_poc.run_celery import read_json, read_task_events, wait_until
from dev.dag_parsing_poc.run_celery_recovery import matches_worker_event, validate_worker_ready, write_json
from dev.dag_parsing_poc.store import ReceiptCapacityError, ReceiptStore


def validate_termination(termination: dict) -> None:
    evidence = termination["evidence"]
    worker, container = evidence["worker"], evidence["container"]
    validate_worker_ready(worker, run_id=evidence["run_id"], queue=evidence["queue"])
    if (
        not re.fullmatch(r"[0-9a-f]{64}", container["Id"])
        or worker["container_hostname"] != container["Id"][:12]
        or worker["container_hostname"] != container["Config"]["Hostname"]
        or container["State"]["Running"]
        or container["State"]["Pid"] != 0
        or container["State"]["ExitCode"] != 137
        or not any(
            matches_worker_event(event, worker, task_id=termination["workload_id"], state="STARTED")
            for event in evidence["events"]
        )
    ):
        raise ValueError("Termination evidence does not identify the stopped workload worker")


def create_coordinator(store, route, generator=None):
    from airflow.providers.celery.executors.celery_executor import CeleryExecutor

    executor = CeleryExecutor(parallelism=1)
    executor.supported_workload_types = frozenset({WorkloadType.PARSE_DAG_DEFINITIONS})

    def issue_token(manifest):
        if generator is None:
            raise RuntimeError("The restart observer cannot issue tokens or dispatch")
        return generator.generate(
            {
                "sub": manifest["workload_id"],
                "scope": "dag-parsing-poc",
                "attempt_ids": [definition["attempt_id"] for definition in manifest["definitions"]],
            }
        )

    return ParsingRecoveryCoordinator(
        store,
        executor,
        route=route,
        capacity=1,
        token_issuer=issue_token,
        termination_validator=validate_termination,
    )


def inspect_restart(store_path: str, route: str, path: str) -> None:
    coordinator = create_coordinator(ReceiptStore(store_path), route)
    coordinator.start()
    coordinator.sync()
    write_json(
        Path(path),
        {
            "pid": os.getpid(),
            "available_slots": coordinator.available_slots,
            "running": len(coordinator.executor.running),
            "queued": sum(map(len, coordinator.executor.executor_queues.values())),
            "reserved": sum(row["state"] == "reserved" for row in coordinator.store.get_admissions(route)),
            "admissions": coordinator.store.get_admissions(route),
            "dispatch_permitted": False,
        },
    )


def observe_restart(store, route, output, name):
    path = output / f"{name}.json"
    child = multiprocessing.get_context("spawn").Process(
        target=inspect_restart,
        args=(store.path, route, str(path)),
    )
    child.start()
    child.join(timeout=30)
    if child.is_alive():
        child.kill()
        child.join(timeout=5)
        raise TimeoutError("Restart observer did not finish")
    if child.exitcode != 0:
        raise RuntimeError("Restart observer failed")
    observation = read_json(path)
    if observation["pid"] != child.pid or child.pid == os.getpid():
        raise RuntimeError("Restart observation did not come from a fresh process")
    return observation


def build_termination(workload_id, attempts, worker, container, output):
    return {
        "kind": "confirmed_worker_termination",
        "workload_id": str(workload_id),
        "execution_ids": sorted({row["execution_id"] for row in attempts if row["execution_id"]}),
        "evidence": {
            "worker": worker,
            "container": container,
            "run_id": worker["run_id"],
            "queue": worker["queue"],
            "events": read_task_events(output, str(workload_id)),
        },
    }


def check_retired_execution(workload, execution_id: str, *, url: str) -> list[int]:
    old = workload.definitions[1]
    stale = DagDefinitionResult(
        attempt_id=old.attempt_id,
        relative_path=old.relative_path,
        source_revision=old.source_revision,
        outcome="success",
        duration_seconds=0,
    )
    with httpx.Client(
        timeout=5, trust_env=False, headers={"Authorization": f"Bearer {workload.token}"}
    ) as client:
        route = f"{url}/execution/poc/parsing/workloads/{workload.workload_id}/attempts/{old.attempt_id}"
        statuses = [
            client.post(f"{route}/claim", json={"execution_id": execution_id}).status_code,
            client.post(
                f"{route}/result",
                json={"execution_id": execution_id, "result": stale.model_dump(mode="json")},
            ).status_code,
        ]
    if statuses != [409, 409]:
        raise RuntimeError(f"Retired execution was not fenced: {statuses}")
    return statuses


def run_checkpoint(
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
):
    before = observe_restart(store, args.queue, output, "restart-before-retirement")
    if (before["available_slots"], before["running"]) != (0, 1):
        raise RuntimeError("Restart forgot the unresolved submission")
    coordinator = create_coordinator(store, args.queue, generator)
    coordinator.start()
    unrelated = workload.model_copy(update={"workload_id": uuid4()})
    try:
        coordinator.admit(unrelated)
    except ReceiptCapacityError:
        pass
    else:
        raise RuntimeError("Restart admitted unrelated work through an occupied route")

    termination = build_termination(
        workload.workload_id,
        original,
        original_worker,
        control["original_container"],
        output,
    )
    now = timezone.utcnow()
    window = workload.stop_deadline - workload.start_deadline
    deadlines = {"start_deadline": now + window, "stop_deadline": now + 2 * window}
    decision = coordinator.recover(workload.workload_id, termination=termination, **deadlines)
    replacement = decision["replacement"]
    if replacement is None or [item["relative_path"] for item in replacement["definitions"]] != [
        "interrupted.py"
    ]:
        raise RuntimeError("Recovery did not preserve the accepted sibling")
    committed = observe_restart(store, args.queue, output, "restart-after-retirement")
    if (committed["available_slots"], committed["running"], committed["reserved"], committed["queued"]) != (
        0,
        0,
        1,
        0,
    ):
        raise RuntimeError("Restart lost the replacement reservation")
    if committed["admissions"][0]["workload_id"] != replacement["workload_id"]:
        raise RuntimeError("Restart reconstructed a different replacement identity")

    coordinator = create_coordinator(store, args.queue, generator)
    coordinator.start()
    if coordinator.recover(workload.workload_id, termination=termination, **deadlines) != decision:
        raise RuntimeError("Duplicate recovery created another decision")
    if (
        coordinator.dispatch_reserved() != [PublicationOutcome(replacement["workload_id"], "published")]
        or coordinator.dispatch_reserved()
    ):
        raise RuntimeError("Replacement dispatch was not unique")
    submitted = observe_restart(store, args.queue, output, "restart-after-dispatch")
    if (submitted["available_slots"], submitted["running"]) != (0, 1):
        raise RuntimeError("Restart lost the submitted replacement")

    def replacement_finished():
        coordinator.sync()
        return len(store.get_results(replacement["workload_id"])) == 1 and any(
            matches_worker_event(
                event, replacement_worker, task_id=replacement["workload_id"], state="SUCCESS"
            )
            for event in read_task_events(output, replacement["workload_id"])
        )

    wait_until(replacement_finished, description="replacement result and worker completion", timeout=50)
    results = store.get_results(replacement["workload_id"])
    if (
        results[0]["outcome"] != "success"
        or results[0]["serialized_dags"][0]["dag"]["dag_id"] != "recovery_interrupted"
    ):
        raise RuntimeError("Replacement did not publish the expected serialized Dag")
    counts = {
        path.stem: len(path.read_text().splitlines())
        for path in (output / "worker-evidence").glob("*.imports")
    }
    if counts != {"accepted": 1, "interrupted": 2}:
        raise RuntimeError(f"Unexpected imports after recovery: {counts}")

    statuses = check_retired_execution(workload, original[1]["execution_id"], url=url)

    write_json(
        output / "completion-stop-request.json",
        {
            "workload_id": replacement["workload_id"],
            "container_id": control["replacement_container"]["Id"],
            "action": "kill the completed replacement worker and save full inspection as completed-worker-inspect.json",
        },
    )
    wait_until(
        lambda: (output / "completed-worker-inspect.json").exists(),
        description="completed worker termination evidence",
        timeout=180,
    )
    inspection = read_json(output / "completed-worker-inspect.json")
    completed = build_termination(
        replacement["workload_id"],
        store.get_attempts(replacement["workload_id"]),
        replacement_worker,
        inspection,
        output,
    )
    completed_decision = coordinator.recover(replacement["workload_id"], termination=completed, **deadlines)
    if completed_decision["replacement"] is not None or coordinator.available_slots != 1:
        raise RuntimeError("Completed work was retried or its capacity was not released")
    after = observe_restart(store, args.queue, output, "restart-after-completion")
    if after["available_slots"] != 1 or after["admissions"]:
        raise RuntimeError("Completed admission reappeared after restart")
    return {
        "mode": "durable admission restoration and atomic retirement before retry",
        "run_id": original_worker["run_id"],
        "before_retirement": before,
        "after_backend_deletion": after_delete,
        "deleted_backend_records": deleted,
        "decision": decision,
        "after_retirement": committed,
        "after_dispatch": submitted,
        "after_completion": after,
        "completed_decision": completed_decision,
        "import_counts": counts,
        "old_execution_http_statuses": statuses,
        "original_results": store.get_results(workload.workload_id),
        "replacement_results": results,
        "termination": termination,
        "completion_termination": completed,
        "limitations": [
            "One trusted coordinator and one controlled worker per queue; not HA or a production route ledger.",
            "Fresh read-only observer processes and new coordinator instances; not scheduler crash recovery.",
            "Operator Docker inspection is trusted; no general Celery termination oracle or cancellation protocol.",
            "Core parsing bridge and development receipt API, not production metadata ingestion.",
        ],
    }
