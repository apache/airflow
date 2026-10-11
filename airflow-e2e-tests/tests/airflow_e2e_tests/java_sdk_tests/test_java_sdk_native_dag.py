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
End-to-end test of Dags declared entirely in Java.

Run with::

    E2E_TEST_MODE=java_sdk uv run --project airflow-e2e-tests pytest \\
        tests/airflow_e2e_tests/java_sdk_tests/test_java_sdk_native_dag.py -xvs

No Python file declares these Dags. The ``airflow-e2e-tests/java-native-bundle`` JAR sits in the Dag
bundle. There are four ``JavaCoordinator``s, so ``[sdk] dag_bundle_to_coordinator`` picks the
``java-native`` one to parse it: the Dag processor runs the JAR through it. Each task sets
``queue="java-native"``, which routes to ``java-jdk``. That coordinator runs the same JAR from the Dag's
own bundle, whatever its ``task_handler_bundle_name`` says. ``java_native_e2e`` is declared with the interface
API, ``java_native_annotation_e2e`` with annotations. Both also exercise a task group, an ``If``, a
``Switch`` and a ``TriggerDagRun`` of ``java_native_target_e2e``, the third Dag the JAR declares.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime

import pytest

from airflow_e2e_tests.e2e_test_utils.clients import AirflowClient

# The Dag processor starts a JVM to parse the JAR, and each task starts another.
_JAVA_TASK_TIMEOUT = 600

# The queue the Dag's tasks set, which ``queue_to_coordinator`` routes to ``java-jdk``.
_QUEUE = "java-native"

# The Dag ``trigger_downstream`` starts a run of; not triggered through the client itself.
_TARGET_DAG_ID = "java_native_target_e2e"

# Both native Dags wire an identical If/Switch/task-group/trigger shape onto their own
# extract -> transform -> load chain, so their graphs and skip sets match exactly; only
# the transform value (84 vs 63) differs, and both land on the same side of each decider.
_CONTROL_FLOW_DOWNSTREAM: dict[str, set[str]] = {
    "transform": {"load", "has_rows", "pick_report"},
    "load": {"trigger_downstream"},
    "has_rows": {"report_many", "report_few"},
    "report_many": set(),
    "report_few": set(),
    "pick_report": {"report_long", "report_short"},
    "report_long": set(),
    "report_short": set(),
    "trigger_downstream": set(),
    "checks.audit": set(),
}
# The Else side of the If, and the Case the Switch does not choose.
_CONTROL_FLOW_SKIPPED = frozenset({"report_few", "report_long"})


@dataclass(frozen=True)
class _NativeDag:
    dag_id: str
    downstream: dict[str, set[str]]
    return_values: dict[str, int | bool | str]
    # Marks the Java source of the file that declares this Dag; each Dag's own source file,
    # not always the bundle's main class, since the bundle embeds one source per Dag.
    source_marker: str
    skipped: frozenset[str] = frozenset()


_NATIVE_DAGS = [
    _NativeDag(
        dag_id="java_native_e2e",
        downstream={"extract": {"transform", "checks.audit"}, **_CONTROL_FLOW_DOWNSTREAM},
        return_values={"extract": 42, "transform": 84, "has_rows": True, "pick_report": "report_short"},
        source_marker="public class NativeBundleBuilder",
        skipped=_CONTROL_FLOW_SKIPPED,
    ),
    _NativeDag(
        dag_id="java_native_annotation_e2e",
        downstream={"extract": {"transform", "checks.audit"}, **_CONTROL_FLOW_DOWNSTREAM},
        # transform(extracted, lit(1.5)).
        return_values={"extract": 42, "transform": 63, "has_rows": True, "pick_report": "report_short"},
        source_marker="public class AnnotationDag",
        skipped=_CONTROL_FLOW_SKIPPED,
    ),
]

_by_dag_id = pytest.mark.parametrize("native_dag", _NATIVE_DAGS, ids=lambda d: d.dag_id)


@dataclass
class _CompletedRun:
    run_id: str
    state: str
    ti_states: dict[str, str]


@pytest.fixture(scope="module")
def parsed_dags() -> AirflowClient:
    """A client that has waited for the Dag processor to register every Dag from the JAR."""
    client = AirflowClient()
    for dag_id in (*[native_dag.dag_id for native_dag in _NATIVE_DAGS], _TARGET_DAG_ID):
        client.wait_for_dag(dag_id, timeout=_JAVA_TASK_TIMEOUT)
    # trigger_downstream never goes through the client, so nothing else un-pauses its
    # target; a run of a still-paused Dag can leave its tasks queued instead of running.
    client.un_pause_dag(_TARGET_DAG_ID)
    return client


@pytest.fixture(scope="module")
def completed_runs(parsed_dags: AirflowClient) -> dict[str, _CompletedRun]:
    """Trigger both Dags at once, then wait for both runs."""
    client = parsed_dags
    run_ids = {
        native_dag.dag_id: client.trigger_dag(
            native_dag.dag_id, json={"logical_date": datetime.now(UTC).isoformat()}
        )["dag_run_id"]
        for native_dag in _NATIVE_DAGS
    }
    runs = {}
    for dag_id, run_id in run_ids.items():
        state = client.wait_for_dag_run(dag_id=dag_id, run_id=run_id, timeout=_JAVA_TASK_TIMEOUT)
        ti_resp = client.get_task_instances(dag_id=dag_id, run_id=run_id)
        runs[dag_id] = _CompletedRun(
            run_id=run_id,
            state=state,
            ti_states={ti["task_id"]: ti.get("state") for ti in ti_resp.get("task_instances", [])},
        )
    return runs


@_by_dag_id
def test_the_graph_is_the_one_java_declared(parsed_dags: AirflowClient, native_dag: _NativeDag):
    tasks = parsed_dags.get_tasks(native_dag.dag_id).get("tasks", [])

    assert {task["task_id"]: set(task["downstream_task_ids"]) for task in tasks} == native_dag.downstream


@_by_dag_id
def test_every_task_sets_the_native_queue(parsed_dags: AirflowClient, native_dag: _NativeDag):
    """The Java DSL has no Dag-level queue, so each task sets its own."""
    tasks = parsed_dags.get_tasks(native_dag.dag_id).get("tasks", [])

    assert {task["task_id"]: task["queue"] for task in tasks} == dict.fromkeys(native_dag.downstream, _QUEUE)


@_by_dag_id
def test_the_dag_source_is_the_bundle_main_class(parsed_dags: AirflowClient, native_dag: _NativeDag):
    """
    The Code view shows the Java source the JAR embeds, not the JAR read as text.

    The bundle embeds one source file per Dag, so the Code view shows the file that actually
    declares each Dag, not always the bundle's main class.
    """
    content = parsed_dags.get_dag_source(native_dag.dag_id)["content"]

    assert native_dag.source_marker in content


@_by_dag_id
def test_dag_run_succeeded(completed_runs: dict[str, _CompletedRun], native_dag: _NativeDag):
    run = completed_runs[native_dag.dag_id]

    assert run.state == "success", (
        f"expected the run to succeed; got {run.state!r}. task states: {run.ti_states}"
    )
    # load throws unless transform's value reached it, so its success proves that hop.
    expected = {
        task_id: "skipped" if task_id in native_dag.skipped else "success"
        for task_id in native_dag.downstream
    }
    assert run.ti_states == expected


@_by_dag_id
def test_xcoms_flow_between_java_tasks(
    parsed_dags: AirflowClient, completed_runs: dict[str, _CompletedRun], native_dag: _NativeDag
):
    run_id = completed_runs[native_dag.dag_id].run_id
    for task_id, expected in native_dag.return_values.items():
        value = parsed_dags.get_xcom_value(
            dag_id=native_dag.dag_id, task_id=task_id, run_id=run_id, key="return_value"
        ).get("value")
        assert value == expected, f"{native_dag.dag_id}.{task_id} returned {value!r}"


@_by_dag_id
def test_trigger_downstream_run_succeeded(
    parsed_dags: AirflowClient, completed_runs: dict[str, _CompletedRun], native_dag: _NativeDag
):
    """``trigger_downstream`` pushes the triggered run's ID; follow it and check it too succeeded."""
    run_id = completed_runs[native_dag.dag_id].run_id
    triggered_run_id = parsed_dags.get_xcom_value(
        dag_id=native_dag.dag_id, task_id="trigger_downstream", run_id=run_id, key="trigger_run_id"
    ).get("value")
    assert triggered_run_id, f"{native_dag.dag_id}.trigger_downstream pushed no run ID"

    state = parsed_dags.wait_for_dag_run(
        dag_id=_TARGET_DAG_ID, run_id=triggered_run_id, timeout=_JAVA_TASK_TIMEOUT
    )
    assert state == "success", (
        f"expected the {_TARGET_DAG_ID} run {triggered_run_id} to succeed; got {state!r}"
    )
