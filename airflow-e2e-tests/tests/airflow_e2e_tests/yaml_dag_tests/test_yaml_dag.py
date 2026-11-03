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
"""
End-to-end test of a Dag written in YAML.

Run with::

    E2E_TEST_MODE=yaml_dag uv run --project airflow-e2e-tests pytest \\
        tests/airflow_e2e_tests/yaml_dag_tests/ -xvs

No Python declares this Dag: ``airflow-e2e-tests/yaml-dags/yaml_s3_roundtrip.yaml`` is dropped
into the Dags folder and the opt-in ``YamlDagImporter`` builds it (see
``conftest._setup_yaml_dag_integration``). The run covers every task body the importer supports:

* ``create_bucket`` / ``write_object`` / ``list_objects`` are provider operators named by ``uses``,
  and ``wait_for_object`` is a sensor. They drive S3 on LocalStack, so the templated ``with:``
  arguments, the ``$t`` markers and the ``extends`` merge are all proven against a real service
  rather than against the serialized Dag alone.
* ``summarize`` is a ``run:`` task. It carries no code at all: the ``golang`` queue routes it to the
  Go coordinator, which runs the handler registered for this dag_id in the packed Go bundle. The
  upstream listing reaches it through ``{$x: list_objects}``.
* ``done`` has no body, so the importer turns it into an ``EmptyOperator`` join.
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass
from datetime import UTC, datetime

import pytest

from airflow_e2e_tests.constants import YAML_DAG_BUCKET
from airflow_e2e_tests.e2e_test_utils.clients import AirflowClient, get_s3_client

# The sensor alone may poke for up to 60 s, and the Go coordinator starts before the
# run: task's first line; allow the same headroom the other lang-SDK suites do.
_RUN_TIMEOUT = 600
# Task logs are written when the task finishes; allow a little slack for them to
# become retrievable through the API after the run reaches a terminal state.
_LOG_FETCH_TIMEOUT = 120

_DAG_ID = "yaml_s3_roundtrip"


@dataclass
class _CompletedRun:
    """The single ``yaml_s3_roundtrip`` run shared across the assertions in this module."""

    client: AirflowClient
    run_id: str
    logical_date: datetime
    state: str
    ti_states: dict[str, str]

    @property
    def object_key(self) -> str:
        """The key the Dag templates from ``{{ ds }}`` and ``{{ ts_nodash }}``; see its ``s3_key``.

        The timestamp keeps every run in its own prefix, so the scheduled runs the
        ``@daily`` schedule produces never land on this run's object.
        """
        ds = self.logical_date.strftime("%Y-%m-%d")
        ts_nodash = self.logical_date.strftime("%Y%m%dT%H%M%S")
        return f"runs/{ds}/{ts_nodash}/payload.json"

    def xcom(self, task_id: str, key: str = "return_value"):
        return self.client.get_xcom_value(dag_id=_DAG_ID, task_id=task_id, run_id=self.run_id, key=key).get(
            "value"
        )

    def logs(self, task_id: str, try_number: int = 1) -> str:
        """Return the concatenated task-log records for *task_id*, retrying until present."""
        deadline = time.monotonic() + _LOG_FETCH_TIMEOUT
        while True:
            resp = self.client.get_task_logs(
                dag_id=_DAG_ID, run_id=self.run_id, task_id=task_id, try_number=try_number
            )
            text = "\n".join(str(entry) for entry in resp.get("content", []))
            if text.strip() or time.monotonic() > deadline:
                return text
            time.sleep(3)


@pytest.fixture(scope="module")
def completed_run() -> _CompletedRun:
    """Trigger ``yaml_s3_roundtrip`` once and wait for it to finish.

    The logical date is passed explicitly because the Dag templates its S3 key from
    ``{{ ds }}``; a run without one renders nothing and the write fails.
    """
    client = AirflowClient()
    logical_date = datetime.now(UTC)
    resp = client.trigger_dag(_DAG_ID, json={"logical_date": logical_date.isoformat()})
    run_id = resp["dag_run_id"]
    state = client.wait_for_dag_run(dag_id=_DAG_ID, run_id=run_id, timeout=_RUN_TIMEOUT)
    ti_resp = client.get_task_instances(dag_id=_DAG_ID, run_id=run_id)
    ti_states = {ti["task_id"]: ti.get("state") for ti in ti_resp.get("task_instances", [])}
    return _CompletedRun(
        client=client,
        run_id=run_id,
        logical_date=logical_date,
        state=state,
        ti_states=ti_states,
    )


def test_every_task_succeeded(completed_run: _CompletedRun):
    """The importer built all six tasks and the run drove each of them to success."""
    expected = {
        "create_bucket": "success",
        "write_object": "success",
        "wait_for_object": "success",
        "list_objects": "success",
        "summarize": "success",
        "done": "success",
    }
    assert completed_run.ti_states == expected


def test_dag_run_succeeded(completed_run: _CompletedRun):
    assert completed_run.state == "success", (
        f"expected the run to succeed; got {completed_run.state!r}. task states: {completed_run.ti_states}"
    )


def test_each_task_body_became_the_operator_it_names(completed_run: _CompletedRun):
    """``uses`` resolves the import path, ``run`` becomes a stub, and no body at all is an EmptyOperator."""
    tasks = {task["task_id"]: task for task in completed_run.client.get_tasks(_DAG_ID).get("tasks", [])}
    operators = {task_id: task["operator_name"] for task_id, task in tasks.items()}

    assert operators == {
        "create_bucket": "S3CreateBucketOperator",
        "write_object": "S3CreateObjectOperator",
        "wait_for_object": "S3KeySensor",
        "list_objects": "S3ListOperator",
        "summarize": "@task.stub",
        "done": "EmptyOperator",
    }
    # The run: task is the only one that leaves the Python runtime, and only its
    # task-level queue sends it there.
    assert tasks["summarize"]["queue"] == "golang"
    assert {task_id for task_id, task in tasks.items() if task["queue"] != "default"} == {"summarize"}


def test_extends_merged_both_task_level_and_operator_arguments(completed_run: _CompletedRun):
    """``extends: [retryable, s3]`` reached the task model, and the tasks that skip it keep the defaults."""
    tasks = {task["task_id"]: task for task in completed_run.client.get_tasks(_DAG_ID).get("tasks", [])}

    assert tasks["create_bucket"]["retries"] == 2
    assert tasks["write_object"]["retries"] == 2
    assert tasks["wait_for_object"]["retries"] == 0
    # The operator argument half of the merge is proven by the run itself: without the
    # aws_conn_id the s3 template contributes, every S3 task would fail.


def test_the_code_view_shows_the_yaml_file(completed_run: _CompletedRun):
    """The Code tab serves the YAML document that declares the Dag."""
    content = completed_run.client.get_dag_source(_DAG_ID)["content"]

    assert "dag_id: yaml_s3_roundtrip" in content
    assert "$schema: https://airflow.apache.org/schemas/dag/" in content


def test_the_object_round_tripped_through_s3(completed_run: _CompletedRun):
    """The templated write really happened: the key and its body carry this run's values."""
    body = (
        get_s3_client()
        .get_object(Bucket=YAML_DAG_BUCKET, Key=completed_run.object_key)["Body"]
        .read()
        .decode()
    )

    assert json.loads(body) == {"run_id": completed_run.run_id}


def test_the_listing_reached_the_code_task(completed_run: _CompletedRun):
    """``{$x: list_objects}`` bound the upstream XCom as the Go handler's argument."""
    assert completed_run.xcom("list_objects") == [completed_run.object_key]
    assert completed_run.xcom("summarize") == {
        "label": "yaml",
        "count": 1,
        "first_key": completed_run.object_key,
    }


def test_the_code_task_ran_on_the_go_runtime(completed_run: _CompletedRun):
    """The handler's own log line reaches Airflow, so the task body ran outside Python."""
    assert "Summarizing the YAML Dag's listing" in completed_run.logs("summarize")
