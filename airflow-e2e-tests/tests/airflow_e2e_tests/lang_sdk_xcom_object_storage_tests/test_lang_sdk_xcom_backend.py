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
End-to-end test of language SDK task XComs stored through the XCom object storage backend.

Run with::

    E2E_TEST_MODE=lang_sdk_xcom_object_storage uv run --project airflow-e2e-tests pytest \\
        tests/airflow_e2e_tests/lang_sdk_xcom_object_storage_tests/ -xvs

The mode stores every XCom in S3 (threshold 0) and runs one Dag definition in each language SDK:
``ts-sdk/example/src/native.ts``, ``go-sdk/example/native/main.go`` and
``airflow-e2e-tests/java-native-bundle``. The database holds each XCom as a reference to its object,
so a task that gets the right value from an upstream task read it through the backend. The objects
in S3 show that the writes went through the backend as well.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from urllib.parse import quote
from uuid import uuid4

import pytest

from airflow_e2e_tests.constants import XCOM_BUCKET
from airflow_e2e_tests.e2e_test_utils.clients import AirflowClient, get_s3_client

# The Dag processor starts node, the Go binary and a JVM to parse, and each task starts one more.
_TIMEOUT = 600

_SKIPMIXIN_KEY = "skipmixin_key"
_TRIGGER_LINK_KEY = "_link_TriggerDagRunLink"
_TRIGGER_RUN_ID_KEY = "trigger_run_id"
_GO_GREETING = "hello from the e2e test"
# What the seed task of the Go Dag pushes: the Variable and the host of the test_http Connection that
# conftest defines.
_GO_NOTE = f"seeded {_GO_GREETING} from example.com"


@dataclass(frozen=True)
class _LangDag:
    dag_id: str
    # The Variables that its tasks read.
    variables: dict[str, str]
    # The Dags that its tasks trigger, which stay paused until a test un-pauses them.
    triggered_dag_ids: tuple[str, ...]
    # The value that the object of each (task_id, key) XCom holds.
    expected_xcoms: dict[tuple[str, str], object]
    # The task whose return value the REST API test reads.
    referenced_task_id: str
    # A branch task, and the tasks that it skips.
    branch_task_id: str
    skipped_task_ids: list[str]
    # The task that triggers the first of the triggered Dags, if the Dag has one.
    trigger_task_id: str | None = None


_LANG_DAGS = [
    _LangDag(
        dag_id="typescript_native_example",
        variables={
            "typescript_native_north_rows": "3",
            "typescript_native_south_rows": "2",
            "typescript_native_cadence": "weekly",
        },
        triggered_dag_ids=("typescript_example",),
        expected_xcoms={
            ("extract.north", "return_value"): {"region": "north", "rows": 3},
            ("extract.south", "return_value"): {"region": "south", "rows": 2},
            # summarize reads both extract XComs, and load_rows reads the region_total that it pushed.
            ("summarize", "return_value"): {"total": 5, "regions": 2},
            ("summarize", "region_total"): 5,
            ("load_rows", "return_value"): {"loaded": 5},
            ("has_rows", "return_value"): True,
            ("publish_weekly", "return_value"): {"cadence": "weekly"},
        },
        referenced_task_id="summarize",
        branch_task_id="has_rows",
        skipped_task_ids=["report_empty"],
        trigger_task_id="trigger_downstream",
    ),
    _LangDag(
        dag_id="go_native_pipeline",
        variables={"go_native_greeting": _GO_GREETING, "go_native_cadence": "weekly"},
        triggered_dag_ids=(),
        expected_xcoms={
            ("extract.regions.north", "return_value"): {"region": "north", "rows": 3},
            ("extract.regions.south", "return_value"): {"region": "south", "rows": 2},
            ("extract.summarize", "return_value"): {"total": 5, "regions": 2},
            ("seed", "seed_note"): _GO_NOTE,
            # cleanup returns the seed_note that it pulled.
            ("cleanup", "return_value"): _GO_NOTE,
            ("anyRows", "return_value"): True,
        },
        referenced_task_id="extract.summarize",
        branch_task_id="anyRows",
        skipped_task_ids=["reportEmpty"],
    ),
    _LangDag(
        dag_id="java_native_e2e",
        variables={},
        triggered_dag_ids=("java_native_target_e2e",),
        expected_xcoms={
            ("extract", "return_value"): 42,
            # transform returns twice the extract value that it pulled, and load fails unless it gets 84.
            ("transform", "return_value"): 84,
            ("has_rows", "return_value"): True,
        },
        referenced_task_id="transform",
        branch_task_id="has_rows",
        skipped_task_ids=["report_few"],
        trigger_task_id="trigger_downstream",
    ),
]

_by_dag = pytest.mark.parametrize("lang_dag", _LANG_DAGS, ids=lambda lang_dag: lang_dag.dag_id)


@dataclass
class _CompletedRun:
    dag_id: str
    run_id: str
    state: str
    ti_ids: dict[str, str]
    stored_object_keys: set[str]

    def build_object_key(self, task_id: str, key: str) -> str:
        """Return where the backend stores the XCom: under the task instance, with the key quoted."""
        prefix = f"xcom/{self.dag_id}/{self.run_id}/{task_id}"
        return f"{prefix}/ti-{self.ti_ids[task_id]}/{quote(key, safe='')}.json"


def _list_object_keys(dag_id: str, run_id: str) -> set[str]:
    paginator = get_s3_client().get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=XCOM_BUCKET, Prefix=f"xcom/{dag_id}/{run_id}/")
    return {obj["Key"] for page in pages for obj in page.get("Contents", [])}


@pytest.fixture(scope="module")
def client() -> AirflowClient:
    return AirflowClient()


@pytest.fixture(scope="module")
def completed_runs(client: AirflowClient) -> dict[str, _CompletedRun]:
    """Trigger the three Dags at once, with the Variables they read, and wait for their runs."""
    for lang_dag in _LANG_DAGS:
        for dag_id in (lang_dag.dag_id, *lang_dag.triggered_dag_ids):
            client.wait_for_dag(dag_id, timeout=_TIMEOUT)
        for key, value in lang_dag.variables.items():
            client.set_variable(key, value)
        # A run that a task triggers would stay queued while its Dag is paused.
        for dag_id in lang_dag.triggered_dag_ids:
            client.un_pause_dag(dag_id)

    run_ids = {
        lang_dag.dag_id: client.trigger_dag(
            lang_dag.dag_id,
            json={
                "dag_run_id": f"test_lang_sdk_xcom_backend_{uuid4()}",
                "logical_date": datetime.now(UTC).isoformat(),
            },
        )["dag_run_id"]
        for lang_dag in _LANG_DAGS
    }
    runs = {}
    for dag_id, run_id in run_ids.items():
        state = client.wait_for_dag_run(dag_id=dag_id, run_id=run_id, timeout=_TIMEOUT)
        task_instances = client.get_task_instances(dag_id, run_id)["task_instances"]
        runs[dag_id] = _CompletedRun(
            dag_id=dag_id,
            run_id=run_id,
            state=state,
            ti_ids={task_instance["task_id"]: task_instance["id"] for task_instance in task_instances},
            stored_object_keys=_list_object_keys(dag_id, run_id),
        )
    return runs


@_by_dag
def test_dag_run_succeeds(completed_runs: dict[str, _CompletedRun], lang_dag: _LangDag):
    run = completed_runs[lang_dag.dag_id]
    assert run.state == "success", f"Dag {lang_dag.dag_id} did not succeed. Final state: {run.state}"


@pytest.mark.parametrize(
    ("lang_dag", "task_id", "key"),
    [
        pytest.param(lang_dag, task_id, key, id=f"{lang_dag.dag_id}-{task_id}-{key}")
        for lang_dag in _LANG_DAGS
        for task_id, key in lang_dag.expected_xcoms
    ],
)
def test_xcom_value_is_stored_in_s3(
    completed_runs: dict[str, _CompletedRun], lang_dag: _LangDag, task_id: str, key: str
):
    run = completed_runs[lang_dag.dag_id]
    object_key = run.build_object_key(task_id, key)
    assert object_key in run.stored_object_keys, (
        f"expected {object_key!r} in S3 bucket {XCOM_BUCKET!r}, found {sorted(run.stored_object_keys)}"
    )

    body = get_s3_client().get_object(Bucket=XCOM_BUCKET, Key=object_key)["Body"].read()
    assert json.loads(body) == lang_dag.expected_xcoms[(task_id, key)], (
        f"unexpected value of the {key!r} XCom of {task_id!r}: {body!r}"
    )


@_by_dag
def test_database_holds_a_reference_to_the_object(
    client: AirflowClient, completed_runs: dict[str, _CompletedRun], lang_dag: _LangDag
):
    """The REST API returns what the XCom row holds, which is where the object is and not its value."""
    run = completed_runs[lang_dag.dag_id]
    stored = client.get_xcom_value(
        dag_id=lang_dag.dag_id,
        task_id=lang_dag.referenced_task_id,
        run_id=run.run_id,
        key="return_value",
    ).get("value")

    object_key = run.build_object_key(lang_dag.referenced_task_id, "return_value")
    message = f"expected a reference to {object_key!r} in S3, got {stored!r}"
    assert isinstance(stored, str), message
    assert stored.startswith("s3://"), message
    assert stored.endswith(f"/{object_key}"), message


@_by_dag
def test_skipped_tasks_xcom_bypasses_the_backend(
    client: AirflowClient, completed_runs: dict[str, _CompletedRun], lang_dag: _LangDag
):
    """A branch task records the tasks it skips under ``skipmixin_key``, which the database holds as it is."""
    run = completed_runs[lang_dag.dag_id]
    assert not [key for key in run.stored_object_keys if _SKIPMIXIN_KEY in key], (
        f"found {_SKIPMIXIN_KEY!r} objects in S3: {sorted(run.stored_object_keys)}"
    )

    stored = client.get_xcom_value(
        dag_id=lang_dag.dag_id, task_id=lang_dag.branch_task_id, run_id=run.run_id, key=_SKIPMIXIN_KEY
    ).get("value")
    assert stored == {"skipped": lang_dag.skipped_task_ids}


@pytest.mark.parametrize(
    ("lang_dag", "trigger_task_id"),
    [
        pytest.param(lang_dag, lang_dag.trigger_task_id, id=lang_dag.dag_id)
        for lang_dag in _LANG_DAGS
        if lang_dag.trigger_task_id
    ],
)
def test_extra_link_xcom_bypasses_the_backend(
    client: AirflowClient, completed_runs: dict[str, _CompletedRun], lang_dag: _LangDag, trigger_task_id: str
):
    """The UI reads an operator extra link from the XCom row as it is, so the row holds the link itself."""
    run = completed_runs[lang_dag.dag_id]
    link_object_key = run.build_object_key(trigger_task_id, _TRIGGER_LINK_KEY)
    assert link_object_key not in run.stored_object_keys, (
        f"found {link_object_key!r} in S3: {sorted(run.stored_object_keys)}"
    )

    run_id_object_key = run.build_object_key(trigger_task_id, _TRIGGER_RUN_ID_KEY)
    body = get_s3_client().get_object(Bucket=XCOM_BUCKET, Key=run_id_object_key)["Body"].read()
    triggered_run_id = json.loads(body)

    stored = client.get_xcom_value(
        dag_id=lang_dag.dag_id, task_id=trigger_task_id, run_id=run.run_id, key=_TRIGGER_LINK_KEY
    ).get("value")
    expected_suffix = f"/dags/{lang_dag.triggered_dag_ids[0]}/runs/{triggered_run_id}"
    message = f"expected the link to end with {expected_suffix!r}, got {stored!r}"
    assert isinstance(stored, str), message
    assert not stored.startswith("s3://"), message
    assert stored.endswith(expected_suffix), message
