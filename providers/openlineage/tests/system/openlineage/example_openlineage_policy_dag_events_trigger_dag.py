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
DAG exercising the emission policy for Dag rules, including the Dag run COMPLETE and FAIL events.

A Dag run's own COMPLETE and FAIL events come after its checking task, so the Dag triggers child Dags and waits for
them, and its checking task reads their events. The children end in ``__notrigger`` so the harness does not
trigger them itself, and the failing ones fail their trigger task, which the checking task does not depend on.

- ``ok`` and ``fail`` have no rule: START and COMPLETE, and START and FAIL, are emitted
- ``off_ok`` and ``off_fail``: ``emit_dag_events: false`` silences the Dag run events, not the task events
- ``on_emit_off_ok`` and ``on_emit_off_fail``: ``emit_dag_events: true`` beats ``emit: false``, so the Dag run START and
  COMPLETE or FAIL are emitted, and the task events are silenced, as ``emits_ol_events`` reports too
- ``emit_off``: ``emit: false`` silences the Dag run events and the task events
- ``task_events_on_emit_off``: ``emit_task_events: true`` beats ``emit: false`` for the task events, which stay
- ``task_events_off``: ``emit_task_events: false`` silences the task events only
- ``locked_off``: a locked ``emit_dag_events: false`` blocks the authored ``emit_dag_events=True``

The rules are in ``AIRFLOW__OPENLINEAGE__EMISSION_POLICY`` of ``airflow-e2e-tests/docker/openlineage.yml``.
"""

from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.providers.openlineage.api.emission_policy import extend_global_openlineage_emission_policy
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.trigger_rule import TriggerRule

from system.openlineage.constants import DEFAULT_DAGRUN_TIMEOUT
from system.openlineage.expected_events import get_expected_event_file_path
from system.openlineage.operator import OpenLineageTestOperator


def _build_child_dag(dag_id: str, bash_command: str) -> DAG:
    with DAG(
        dagrun_timeout=DEFAULT_DAGRUN_TIMEOUT,
        dag_id=dag_id,
        start_date=datetime(2021, 1, 1),
        schedule=None,
        catchup=False,
        default_args={"retries": 0},
    ) as child_dag:
        BashOperator(task_id="run", bash_command=bash_command)
    return child_dag


CHILD_COMMANDS = {
    "ok": "echo ok",
    "fail": "exit 1;",
    "off_ok": "echo ok",
    "off_fail": "exit 1;",
    "on_emit_off_ok": "echo ok",
    "on_emit_off_fail": "exit 1;",
    "emit_off": "echo ok",
    "task_events_on_emit_off": "echo ok",
    "task_events_off": "echo ok",
    "locked_off": "echo ok",
}
CHILDREN = {
    name: _build_child_dag(f"openlineage_policy_dag_events_{name}_child__notrigger", command)
    for name, command in CHILD_COMMANDS.items()
}
extend_global_openlineage_emission_policy(CHILDREN["locked_off"], emit_dag_events=True)


DAG_ID = "openlineage_policy_dag_events_trigger_dag"

with DAG(
    dagrun_timeout=DEFAULT_DAGRUN_TIMEOUT,
    dag_id=DAG_ID,
    start_date=datetime(2021, 1, 1),
    schedule=None,
    catchup=False,
    default_args={"retries": 0},
) as dag:
    triggers = [
        TriggerDagRunOperator(
            task_id=f"trigger_{child.dag_id}",
            trigger_dag_id=child.dag_id,
            trigger_run_id=f"{child.dag_id}_triggered_by_{DAG_ID}_{datetime.now().isoformat()}",
            wait_for_completion=True,
            poke_interval=5,
        )
        for child in CHILDREN.values()
    ]
    # A failed child fails its trigger task, so the checking task waits for all of them instead
    all_triggered = EmptyOperator(task_id="all_triggered", trigger_rule=TriggerRule.ALL_DONE)
    check_events = OpenLineageTestOperator(
        task_id="check_events", file_path=get_expected_event_file_path(DAG_ID)
    )
    triggers >> all_triggered >> check_events


from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: contributing-docs/testing/system_tests.rst)
test_run = get_test_run(dag)
