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
# ruff: noqa: S101
from __future__ import annotations

import configparser
import json
from pathlib import Path
from unittest import mock
from uuid import UUID, uuid4

import pytest

from dev.dag_parsing_poc.celery_worker import (
    WorkerStartupError,
    build_argument_parser,
    get_worker_identity,
    prepare_worker_environment,
    record_task_state,
)


@pytest.fixture
def worker_args(tmp_path):
    bundle = tmp_path / "bundle"
    bundle.mkdir()
    return build_argument_parser().parse_args(
        [
            "--broker-url=redis://broker:6379/0",
            "--result-backend=redis://broker:6379/1",
            "--api-url=http://api:8799/execution/",
            "--queue=poc-parsing",
            "--bundle-name=poc",
            "--bundle-version=v1",
            f"--bundle-root={bundle}",
            f"--airflow-home={tmp_path / 'fresh-airflow-home'}",
            f"--evidence={tmp_path / 'isolation.json'}",
            f"--forbidden-path={tmp_path / 'control-plane'}",
        ]
    )


def test_worker_config_is_created_before_airflow_import(worker_args):
    environment = {"PATH": "/usr/local/bin"}
    prepare_worker_environment(worker_args, environ=environment, loaded_modules=[])
    config = configparser.ConfigParser()
    config.read(environment["AIRFLOW_CONFIG"])

    assert config.get("database", "sql_alchemy_conn") == "sqlite:///:memory:"
    assert config.get("database", "sql_alchemy_conn_async") == ""
    assert config.get("core", "fernet_key") == ""
    assert config.get("api_auth", "jwt_secret") == ""
    assert config.get("api_auth", "jwt_private_key_path") == ""
    assert config.get("celery", "result_backend") == worker_args.result_backend
    assert config.get("celery", "broker_url") == worker_args.broker_url
    assert config.get("operators", "default_queue") == worker_args.queue
    assert not config.getboolean("celery", "worker_enable_remote_control")
    assert config.get("core", "execution_api_server_url") == worker_args.api_url
    assert environment["_AIRFLOW__REEXECUTED_PROCESS"] == "1"
    assert environment["_AIRFLOW_PROCESS_CONTEXT"] == "client"
    assert environment["AIRFLOW_DAG_PARSING_POC_REMOTE"] == "1"
    assert environment["AIRFLOW_DAG_PARSING_POC_LOG_DIR"] == str(worker_args.evidence.parent / "parse-logs")
    assert json.loads(environment["AIRFLOW_DAG_PARSING_POC_BUNDLE_ROOTS"]) == {
        "poc": {"path": str(worker_args.bundle_root), "version": "v1"}
    }
    assert Path(environment["AIRFLOW_CONFIG"]).stat().st_mode & 0o777 == 0o600


@pytest.mark.parametrize(
    "name",
    [
        "AIRFLOW_HOME",
        "AIRFLOW_CONFIG",
        "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN",
        "AIRFLOW__API_AUTH__JWT_SECRET",
        "AIRFLOW__API_AUTH__JWT_PRIVATE_KEY_PATH_CMD",
        "CELERY_RESULT_BACKEND",
        "_AIRFLOW_PROCESS_CONTEXT",
        "DATABASE_URL",
    ],
)
def test_inherited_config_is_rejected_without_exposing_values(worker_args, name):
    with pytest.raises(WorkerStartupError, match="Inherited configuration") as captured:
        prepare_worker_environment(worker_args, environ={name: "private-value"}, loaded_modules=[])
    assert name in str(captured.value)
    assert "private-value" not in str(captured.value)
    assert not worker_args.airflow_home.exists()


@pytest.mark.parametrize("module", ["airflow", "airflow.configuration", "airflow.sdk"])
def test_imported_airflow_cannot_be_sanitized_in_place(worker_args, module):
    with pytest.raises(WorkerStartupError, match="fresh interpreter"):
        prepare_worker_environment(worker_args, environ={}, loaded_modules=[module])
    assert not worker_args.airflow_home.exists()


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("broker_url", "amqp://broker/"),
        ("result_backend", "db+postgresql://metadata/airflow"),
        ("api_url", "http://user:private-value@api/execution/"),
        ("queue", "default"),
        ("queue", "celery"),
        ("queue", "parse,default"),
        ("concurrency", 0),
    ],
)
def test_unsafe_launch_configuration_is_rejected(worker_args, field, value):
    setattr(worker_args, field, value)
    with pytest.raises(WorkerStartupError) as captured:
        prepare_worker_environment(worker_args, environ={}, loaded_modules=[])
    assert "private-value" not in str(captured.value)
    assert not worker_args.airflow_home.exists()


@pytest.mark.parametrize("symlink", [False, True])
def test_visible_control_plane_path_is_rejected(worker_args, symlink):
    path = worker_args.forbidden_path[0]
    if symlink:
        path.symlink_to(path.with_name("missing"))
    else:
        path.mkdir()
    with pytest.raises(WorkerStartupError, match="Control-plane path is visible"):
        prepare_worker_environment(worker_args, environ={}, loaded_modules=[])
    assert not worker_args.airflow_home.exists()


def test_existing_worker_home_is_not_reused(worker_args):
    worker_args.airflow_home.mkdir()
    existing = worker_args.airflow_home / "airflow.cfg"
    existing.write_text("previous configuration")
    with pytest.raises(FileExistsError):
        prepare_worker_environment(worker_args, environ={}, loaded_modules=[])
    assert existing.read_text() == "previous configuration"


@pytest.mark.parametrize("explicit_run", [True, False])
def test_worker_environment_identifies_run_and_fresh_worker(worker_args, explicit_run):
    worker_args.run_id = uuid4() if explicit_run else None
    environment = {}
    prepare_worker_environment(worker_args, environ=environment, loaded_modules=[])
    run_id = UUID(environment["AIRFLOW_DAG_PARSING_POC_RUN_ID"])
    worker_id = UUID(environment["AIRFLOW_DAG_PARSING_POC_WORKER_ID"])
    if explicit_run:
        assert run_id == worker_args.run_id
    assert worker_id != run_id


@mock.patch("dev.dag_parsing_poc.celery_worker.socket.gethostname", autospec=True)
def test_task_evidence_records_worker_identity_and_state(gethostname, tmp_path, monkeypatch):
    run_id, worker_id = str(uuid4()), str(uuid4())
    monkeypatch.setenv("AIRFLOW_DAG_PARSING_POC_RUN_ID", run_id)
    monkeypatch.setenv("AIRFLOW_DAG_PARSING_POC_WORKER_ID", worker_id)
    gethostname.return_value = "actual-container-hostname"
    identity = {
        "run_id": run_id,
        "worker_id": worker_id,
        "container_hostname": "actual-container-hostname",
    }
    assert get_worker_identity() == identity
    evidence = tmp_path / "isolation.json"
    record_task_state(evidence, task_id="first-task", state="STARTED")
    record_task_state(evidence, task_id="second-task", state="SUCCESS")
    assert [json.loads(line) for line in (tmp_path / "task-events.jsonl").read_text().splitlines()] == [
        {**identity, "task_id": "first-task", "state": "STARTED"},
        {**identity, "task_id": "second-task", "state": "SUCCESS"},
    ]
