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
# dependencies = ["apache-airflow-core", "apache-airflow-providers-celery"]
# ///
"""Start the PoC parsing worker in a separately provisioned container with a clean environment."""

from __future__ import annotations

import argparse
import configparser
import importlib.util
import json
import os
import re
import socket
import sys
from pathlib import Path
from typing import TYPE_CHECKING
from urllib.parse import urlsplit
from uuid import UUID, uuid4

if TYPE_CHECKING:
    from collections.abc import Iterable, MutableMapping


class WorkerStartupError(RuntimeError):
    """The dedicated worker's configuration or checked deployment boundary is unsafe."""


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--broker-url", required=True)
    parser.add_argument("--result-backend", required=True)
    parser.add_argument("--api-url", required=True)
    parser.add_argument("--queue", required=True)
    parser.add_argument("--bundle-name", required=True)
    parser.add_argument("--bundle-version", required=True)
    parser.add_argument("--bundle-root", type=Path, required=True)
    parser.add_argument("--airflow-home", type=Path, required=True)
    parser.add_argument("--evidence", type=Path, required=True)
    parser.add_argument("--forbidden-path", type=Path, action="append", required=True)
    parser.add_argument("--concurrency", type=int, default=1)
    parser.add_argument("--run-id", type=UUID, default=None)
    parser.add_argument("--probe-only", action="store_true")
    parser.add_argument("--include-source", action="store_true")
    return parser


def validate_forbidden_paths(paths: Iterable[Path]) -> list[str]:
    checked = []
    for path in paths:
        if not path.is_absolute():
            raise WorkerStartupError("Forbidden paths must be absolute container paths")
        if path.exists() or path.is_symlink():
            raise WorkerStartupError(f"Control-plane path is visible to the parsing worker: {path}")
        checked.append(str(path))
    return checked


def prepare_worker_environment(
    args: argparse.Namespace, *, environ: MutableMapping[str, str], loaded_modules: Iterable[str]
) -> None:
    if any(name == "airflow" or name.startswith("airflow.") for name in loaded_modules):
        raise WorkerStartupError("Start the parsing worker in a fresh interpreter before importing Airflow")
    inherited = sorted(
        name
        for name in environ
        if name.startswith(("AIRFLOW_", "_AIRFLOW", "CELERY_"))
        or name in {"DATABASE_URL", "SQLALCHEMY_DATABASE_URI"}
    )
    if inherited:
        raise WorkerStartupError(
            f"Inherited configuration is not allowed; unset these variables: {inherited}"
        )
    if args.concurrency < 1 or not re.fullmatch(r"[A-Za-z0-9_.-]+", args.queue):
        raise WorkerStartupError("Use positive concurrency and a simple dedicated queue name")
    if args.queue in {"default", "celery"}:
        raise WorkerStartupError("The parsing queue must not be a task default queue")
    for name, value in (("broker", args.broker_url), ("result backend", args.result_backend)):
        parsed = urlsplit(value)
        if parsed.scheme not in {"redis", "rediss"} or not parsed.hostname:
            raise WorkerStartupError(f"The {name} must be an explicit Redis URL")
    api = urlsplit(args.api_url)
    if (
        api.scheme not in {"http", "https"}
        or not api.hostname
        or api.username is not None
        or api.password is not None
        or api.query
        or api.fragment
    ):
        raise WorkerStartupError(
            "The Execution API must be an explicit HTTP URL without credentials or query"
        )
    if not args.bundle_root.is_absolute() or not args.bundle_root.is_dir():
        raise WorkerStartupError("Provide an existing absolute worker bundle root")
    if not args.airflow_home.is_absolute() or not args.evidence.is_absolute():
        raise WorkerStartupError("Worker home and evidence paths must be absolute")
    validate_forbidden_paths(args.forbidden_path)
    args.airflow_home.mkdir(parents=True, exist_ok=False)
    for name in ("dags", "plugins", "config", "logs"):
        (args.airflow_home / name).mkdir()
    configuration = configparser.ConfigParser(interpolation=None)
    configuration.read_dict(
        {
            "core": {
                "executor": "LocalExecutor",
                "load_examples": "false",
                "unit_test_mode": "false",
                "fernet_key": "",
                "execution_api_server_url": args.api_url.rstrip("/") + "/",
            },
            "database": {"sql_alchemy_conn": "sqlite:///:memory:", "sql_alchemy_conn_async": ""},
            "api_auth": {"jwt_secret": "", "jwt_private_key_path": ""},
            "secrets": {"backend": ""},
            "workers": {"secrets_backend": ""},
            "operators": {"default_queue": args.queue},
            "celery": {
                "broker_url": args.broker_url,
                "result_backend": args.result_backend,
                "worker_concurrency": str(args.concurrency),
                "worker_prefetch_multiplier": "1",
                "worker_enable_remote_control": "false",
                "extra_celery_config": "{}",
            },
            "celery_broker_transport_options": {"visibility_timeout": "3600"},
        }
    )
    config_path = args.airflow_home / "airflow.cfg"
    with config_path.open("x") as handle:
        configuration.write(handle)
    config_path.chmod(0o600)
    # Creating the file before import avoids core generating server signing/encryption keys.
    environ.update(
        {
            "AIRFLOW_HOME": str(args.airflow_home),
            "AIRFLOW_CONFIG": str(config_path),
            "_AIRFLOW_PROCESS_CONTEXT": "client",
            "_AIRFLOW__REEXECUTED_PROCESS": "1",
            "AIRFLOW_DAG_PARSING_POC_REMOTE": "1",
            "AIRFLOW_DAG_PARSING_POC_INCLUDE_SOURCE": "1" if args.include_source else "0",
            "AIRFLOW_DAG_PARSING_POC_LOG_DIR": str(args.evidence.parent / "parse-logs"),
            "AIRFLOW_DAG_PARSING_POC_RUN_ID": str(args.run_id or uuid4()),
            "AIRFLOW_DAG_PARSING_POC_WORKER_ID": str(uuid4()),
            "AIRFLOW_DAG_PARSING_POC_BUNDLE_ROOTS": json.dumps(
                {args.bundle_name: {"path": str(args.bundle_root.resolve()), "version": args.bundle_version}}
            ),
        }
    )


def get_mount_evidence() -> list[dict]:
    mountinfo = Path("/proc/self/mountinfo")
    if not mountinfo.exists():
        raise WorkerStartupError("The isolated worker probe requires Linux mount information")
    mounts = []
    for line in mountinfo.read_text().splitlines():
        fields = line.split()
        separator = fields.index("-")
        mounts.append({"path": fields[4], "options": fields[5], "filesystem": fields[separator + 1]})
    return mounts


def get_worker_identity() -> dict[str, str]:
    return {
        "run_id": os.environ["AIRFLOW_DAG_PARSING_POC_RUN_ID"],
        "worker_id": os.environ["AIRFLOW_DAG_PARSING_POC_WORKER_ID"],
        "container_hostname": socket.gethostname(),
    }


def write_evidence(args: argparse.Namespace, app, *, stage: str) -> None:
    from airflow import settings
    from airflow.configuration import conf
    from airflow.dag_processing.executor_worker import validate_remote_credentials

    validate_remote_credentials()
    if settings.engine is not None:
        raise WorkerStartupError("Worker unexpectedly initialized a metadata database engine")
    if app.conf.broker_url != args.broker_url or app.conf.result_backend != args.result_backend:
        raise WorkerStartupError("Celery broker/result configuration differs from explicit Redis endpoints")
    if app.conf.task_default_queue != args.queue:
        raise WorkerStartupError("Celery default queue differs from the dedicated parsing queue")
    task = app.tasks.get("execute_workload")
    if task is None or task.run.__module__ != "airflow.providers.celery.executors.celery_executor_utils":
        raise WorkerStartupError("The actual provider execute_workload task is not registered")
    evidence = {
        **get_worker_identity(),
        "stage": stage,
        "pid": os.getpid(),
        "uid": os.getuid(),
        "configuration_file": os.environ["AIRFLOW_CONFIG"],
        "process_context": os.environ["_AIRFLOW_PROCESS_CONTEXT"],
        "remote_credential_validation": "passed",
        "metadata_engine_initialized": False,
        "metadata_database": "in-memory SQLite sentinel; no deployment metadata database configured",
        "jwt_signing_secret_configured": bool(conf.get("api_auth", "jwt_secret", fallback="")),
        "jwt_private_key_configured": bool(conf.get("api_auth", "jwt_private_key_path", fallback="")),
        "broker_scheme": urlsplit(app.conf.broker_url).scheme,
        "result_backend_scheme": urlsplit(app.conf.result_backend).scheme,
        "queue": args.queue,
        "task_implementation": task.run.__module__,
        "bundle_root": str(args.bundle_root.resolve()),
        "forbidden_paths_absent": validate_forbidden_paths(args.forbidden_path),
        "mounts": get_mount_evidence(),
        "limits": "These checks are deployment evidence, not a sandbox or a complete inventory of secrets.",
    }
    args.evidence.parent.mkdir(parents=True, exist_ok=True)
    temporary = args.evidence.with_suffix(".tmp")
    temporary.write_text(json.dumps(evidence, indent=2) + "\n")
    temporary.replace(args.evidence)


def record_task_state(evidence_path: Path, *, task_id: str, state: str) -> None:
    path = evidence_path.with_name("task-events.jsonl")
    line = (json.dumps({**get_worker_identity(), "task_id": task_id, "state": state}) + "\n").encode("utf-8")
    descriptor = os.open(path, os.O_WRONLY | os.O_APPEND | os.O_CREAT, 0o600)
    try:
        os.write(descriptor, line)
    finally:
        os.close(descriptor)


def main() -> None:
    args = build_argument_parser().parse_args()
    prepare_worker_environment(args, environ=os.environ, loaded_modules=sys.modules)
    if importlib.util.find_spec("airflow_local_settings") is not None:
        raise WorkerStartupError("Unexpected airflow_local_settings is available in the worker runtime")

    from celery.signals import task_postrun, task_prerun, worker_ready
    from kombu import Queue

    from airflow.providers.celery.executors.celery_executor_utils import app

    app.conf.update(
        task_queues=(Queue(args.queue),),
        task_routes={"execute_workload": {"queue": args.queue}},
        task_create_missing_queues=False,
    )
    write_evidence(args, app, stage="configuration_validated")
    if args.probe_only:
        return

    @worker_ready.connect(weak=False)
    def record_ready(**kwargs):
        write_evidence(args, app, stage="worker_ready")

    @task_prerun.connect(weak=False)
    def record_started(task_id, task, **kwargs):
        if task.name == "execute_workload":
            os.environ["AIRFLOW_DAG_PARSING_POC_TASK_ID"] = str(task_id)
            record_task_state(args.evidence, task_id=str(task_id), state="STARTED")

    @task_postrun.connect(weak=False)
    def record_finished(task_id, state, task, **kwargs):
        if task.name == "execute_workload":
            record_task_state(args.evidence, task_id=str(task_id), state=str(state))

    app.worker_main(
        [
            "worker",
            "--pool=prefork",
            f"--concurrency={args.concurrency}",
            f"--queues={args.queue}",
            f"--hostname={args.queue}@%h",
            "--loglevel=INFO",
            "--without-gossip",
            "--without-mingle",
        ]
    )


if __name__ == "__main__":
    main()
