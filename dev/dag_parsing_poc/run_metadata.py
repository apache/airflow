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
"""Prove isolated Celery parsing can populate metadata and feed the real scheduler."""

from __future__ import annotations

import argparse
import hashlib
import json
import multiprocessing
import os
import socket
from pathlib import Path
from uuid import uuid4


def serve_metadata_api(store_path: str, public_key_path: str, listener: socket.socket) -> None:
    import uvicorn

    from dev.dag_parsing_poc.api import create_app

    server = uvicorn.Server(
        uvicorn.Config(create_app(store_path, public_key_path, persist_metadata=True), log_level="warning")
    )
    server.run(sockets=[listener])


def run_scheduler_checkpoint() -> dict:
    from sqlalchemy import select

    from airflow.executors.local_executor import LocalExecutor
    from airflow.jobs.job import Job
    from airflow.jobs.scheduler_job_runner import SchedulerJobRunner
    from airflow.models.dag import DagModel
    from airflow.models.dagrun import DagRun
    from airflow.models.taskinstance import TaskInstance
    from airflow.utils.session import create_session
    from airflow.utils.state import DagRunState, TaskInstanceState

    runner = SchedulerJobRunner(job=Job(), executors=[LocalExecutor(parallelism=1)])
    with create_session() as session:
        session.add(runner.job)
        session.flush()
        models = list(session.scalars(select(DagModel).where(DagModel.is_paused == False)))  # noqa: E712
        if len(models) != 1 or models[0].next_dagrun is None:
            raise RuntimeError("Remote result did not create one schedulable Dag")
        runner._create_dag_runs(models, session=session)
        session.flush()
        runner._start_queued_dagruns(session=session)
        session.flush()
        dag_run = session.scalar(select(DagRun))
        if dag_run is None or dag_run.state != DagRunState.RUNNING:
            raise RuntimeError("Scheduler did not start the remotely parsed Dag")
        runner._schedule_dag_run(dag_run, session=session)
        session.flush()
        task_instance = session.scalar(select(TaskInstance))
        if task_instance is None or task_instance.state != TaskInstanceState.SCHEDULED:
            raise RuntimeError("Scheduler did not schedule the remotely parsed task")
        return {
            "dag_id": dag_run.dag_id,
            "run_id": dag_run.run_id,
            "run_state": dag_run.state,
            "task_id": task_instance.task_id,
            "task_state": task_instance.state,
            "dag_version_id": str(task_instance.dag_version_id),
        }


def run_checkpoint(args, output: Path) -> dict:
    # Airflow and Celery cache configuration on import. This script starts a fresh interpreter.
    os.environ.update(
        {
            "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN": f"sqlite:///{output / 'metadata.sqlite'}",
            "AIRFLOW__CORE__EXECUTOR": "LocalExecutor",
            "AIRFLOW__CORE__LOAD_EXAMPLES": "False",
            "AIRFLOW__CORE__MIN_SERIALIZED_DAG_UPDATE_INTERVAL": "0",
            "AIRFLOW__CELERY__BROKER_URL": args.broker_url,
            "AIRFLOW__CELERY__RESULT_BACKEND": args.result_backend,
            "AIRFLOW__CELERY__SYNC_PARALLELISM": "1",
        }
    )
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
    from sqlalchemy import select

    from airflow import settings
    from airflow.api_fastapi.auth.tokens import JWTGenerator
    from airflow.executors.workloads import WorkloadType
    from airflow.models import import_all_models
    from airflow.models.base import Base
    from airflow.models.dagbundle import DagBundleModel
    from airflow.models.dagcode import DagCode
    from airflow.models.serialized_dag import SerializedDagModel
    from airflow.providers.celery.executors.celery_executor import CeleryExecutor
    from airflow.utils.db import add_default_pool_if_not_exists, synchronize_log_template
    from airflow.utils.session import create_session

    from dev.dag_parsing_poc.coordinator import ParsingRecoveryCoordinator
    from dev.dag_parsing_poc.metadata import MetadataReceiptStore
    from dev.dag_parsing_poc.run import create_workloads, wait_for_api
    from dev.dag_parsing_poc.run_celery import finish_workloads, read_json, read_task_events, wait_until
    from dev.dag_parsing_poc.run_celery_recovery import validate_worker_ready

    import_all_models()
    Base.metadata.create_all(settings.engine)
    with create_session() as session:
        add_default_pool_if_not_exists(session=session)
        synchronize_log_template(session=session)
        session.add(DagBundleModel(name="poc", version="v1"))
    store = MetadataReceiptStore(output / "metadata.sqlite")
    signing_key = Ed25519PrivateKey.generate()
    public = output / "verification-key.pem"
    public.write_bytes(
        signing_key.public_key().public_bytes(
            serialization.Encoding.PEM, serialization.PublicFormat.SubjectPublicKeyInfo
        )
    )
    generator = JWTGenerator(
        private_key=signing_key,
        kid="dag-parsing-poc",
        issuer="dag-parsing-poc",
        audience="dag-parsing-poc",
        algorithm="EdDSA",
        valid_for=600,
    )
    worker_root = output / "worker-bundle"
    worker_root.mkdir()
    source = worker_root / "scheduled.py"
    source.write_text(
        "from pathlib import Path\nfrom datetime import datetime, timezone\n"
        "from airflow.sdk import DAG, task\n"
        "with Path('/worker-evidence/imports.txt').open('a') as marker:\n"
        "    marker.write('imported\\n')\n"
        "with DAG('remote_metadata_checkpoint', schedule='@once', "
        "start_date=datetime(2026, 1, 1, tzinfo=timezone.utc), is_paused_upon_creation=False):\n"
        "    @task\n    def sample():\n        return 1\n    sample()\n"
    )
    (output / "worker-evidence").mkdir()
    run_id = str(uuid4())
    listener = socket.socket()
    listener.bind(("0.0.0.0", args.port))
    listener.listen()
    api = multiprocessing.get_context("spawn").Process(
        target=serve_metadata_api, args=(store.path, str(public), listener), daemon=True
    )
    api.start()
    listener.close()
    executor = CeleryExecutor(parallelism=1)
    executor.supported_workload_types = frozenset({WorkloadType.PARSE_DAG_DEFINITIONS})

    def issue_token(manifest):
        return generator.generate(
            {
                "sub": str(manifest["workload_id"]),
                "scope": "dag-parsing-poc",
                "attempt_ids": [str(item["attempt_id"]) for item in manifest["definitions"]],
            }
        )

    def reject_recovery(evidence):
        raise RuntimeError("This checkpoint does not exercise worker-loss recovery")

    coordinator = ParsingRecoveryCoordinator(
        store,
        executor,
        route=args.queue,
        capacity=1,
        token_issuer=issue_token,
        termination_validator=reject_recovery,
    )
    try:
        wait_for_api(f"http://localhost:{args.port}", api)
        (output / "driver-ready.json").write_text(
            json.dumps({"port": args.port, "queue": args.queue, "run_id": run_id}) + "\n"
        )
        wait_until(
            lambda: read_json(output / "worker-evidence/isolation.json").get("stage") == "worker_ready",
            description="isolated metadata worker",
            timeout=240,
        )
        worker = read_json(output / "worker-evidence/isolation.json")
        validate_worker_ready(worker, run_id=run_id, queue=args.queue)
        workload = create_workloads([source], batch_size=1, timeout=30, generator=generator)[0].model_copy(
            update={"queue": args.queue}
        )
        coordinator.start()
        coordinator.admit(workload)
        publication = coordinator.dispatch_reserved()
        if len(publication) != 1 or publication[0].status != "published":
            raise RuntimeError("Workload was not published to the dedicated queue")
        events = finish_workloads(executor, [workload])
        results = store.get_results(workload.workload_id)
        if len(results) != 1 or results[0]["outcome"] != "success":
            raise RuntimeError("No successful parse result was accepted")
        with create_session() as session:
            serialized = session.scalar(select(SerializedDagModel))
            code = session.scalar(select(DagCode))
            if serialized is None or code is None or code.source_code != source.read_text():
                raise RuntimeError("Remote source and metadata did not persist together")
            before = (str(serialized.id), str(code.id), code.source_code_hash)
        scheduled = run_scheduler_checkpoint()
        # Redeliver the accepted workload through the real broker; no second import or metadata write.
        task_id = str(workload.workload_id)
        wait_until(
            lambda: any(event["state"] == "SUCCESS" for event in read_task_events(output, task_id)),
            description="first Celery completion",
        )
        executor.celery_app.tasks["execute_workload"].apply_async(
            args=[workload.model_dump_json()],
            queue=args.queue,
            task_id=task_id,
            argsrepr="(<redacted parsing workload>,)",
        )
        wait_until(
            lambda: sum(event["state"] == "SUCCESS" for event in read_task_events(output, task_id)) == 2,
            description="accepted workload redelivery",
        )
        with create_session() as session:
            serialized = session.scalar(select(SerializedDagModel))
            code = session.scalar(select(DagCode))
            if (str(serialized.id), str(code.id), code.source_code_hash) != before:
                raise RuntimeError("Accepted redelivery changed persisted metadata")
        imports = (output / "worker-evidence/imports.txt").read_text().splitlines()
        if imports != ["imported"]:
            raise RuntimeError("Accepted workload was imported more than once")
        return {
            "status": "passed",
            "run_id": run_id,
            "workload_id": task_id,
            "result_count": len(results),
            "source_sha256": hashlib.sha256(source.read_bytes()).hexdigest(),
            "scheduler": scheduled,
            "celery": events,
            "import_count": len(imports),
            "accepted_redelivery_preserved_metadata": True,
            "worker": worker,
        }
    finally:
        executor.end()
        api.terminate()
        api.join(timeout=10)
        if api.is_alive():
            api.kill()
            api.join(timeout=5)
        store.engine.dispose()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--broker-url", default="redis://poc-broker:6379/0")
    parser.add_argument("--result-backend", default="redis://poc-broker:6379/1")
    parser.add_argument("--queue", default="poc-metadata")
    parser.add_argument("--port", default=8799, type=int)
    args = parser.parse_args()
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    result = run_checkpoint(args, output)
    (output / "summary.json").write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
