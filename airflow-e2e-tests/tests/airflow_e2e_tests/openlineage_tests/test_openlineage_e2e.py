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
from __future__ import annotations

from pathlib import Path

import pytest
import requests

from airflow_e2e_tests.e2e_test_utils.clients import AirflowClient
from airflow_e2e_tests.openlineage_tests.harness import (
    OpenLineageE2ERunner,
    console,
    discover_expected_dag_ids,
)
from airflow_e2e_tests.openlineage_tests.task_logs import find_log_paths, print_task_log


def _print_failure_logs(runner: OpenLineageE2ERunner, logs_folder: Path, failed: dict[str, str]) -> None:
    """Print the relevant task logs for each failed DAG, so CI output doesn't require the log artifact."""
    for dag_id, state in failed.items():
        if state == "missing":
            console.print(f"[yellow]{dag_id} never loaded — check the dag-processor logs, not task logs.[/]")
            continue
        run_id = runner.retry_run_id if dag_id in runner.retried_dag_ids else runner.run_id
        try:
            task_states = runner.get_task_states(dag_id, run_id)
        except requests.exceptions.RequestException as exc:
            console.print(f"[yellow]Could not fetch task states for {dag_id}: {exc}[/]")
            task_states = {}
        # check_events always included: it shows what was actually compared, even if a different
        # task is what actually failed the DAG. Every other non-success task is shown too, EXCEPT a
        # routine SKIPPED task (e.g. a branch not taken) never ran and has no log — showing it would
        # look like a false-positive failure. A task SKIPPED by dagrun_timeout cutting off a
        # still-running task, though, usually does have a partial log, so still show those.
        tasks_to_show = {"check_events"}
        for task_id, ti_state in task_states.items():
            if ti_state == "success" or task_id in tasks_to_show:
                continue
            if ti_state == "skipped" and not find_log_paths(logs_folder, dag_id, run_id, task_id):
                continue
            tasks_to_show.add(task_id)
        for task_id in sorted(tasks_to_show):
            print_task_log(console, logs_folder, dag_id, run_id, task_id)


@pytest.mark.execution_timeout(900)  # 15 min
def test_all_openlineage_dags_succeed(compose_instance, airflow_dags_path, airflow_logs_path):
    """
    Trigger every OpenLineage system-test DAG in the deployment and require each run to succeed.

    The terminal ``OpenLineageTestOperator`` task inside each DAG validates the emitted OpenLineage
    events against the expected templates, so a ``success`` run state means the lineage matched.
    """
    expected_dag_ids = discover_expected_dag_ids(airflow_dags_path)
    assert expected_dag_ids, "No expected OpenLineage DAG ids were discovered from the prepared dags"

    runner = OpenLineageE2ERunner(AirflowClient())
    statuses = runner.run(expected_dag_ids)

    console.print(f"[blue]OpenLineage e2e results ({len(statuses)} DAGs): {statuses}")
    failed = {dag_id: state for dag_id, state in sorted(statuses.items()) if state != "success"}
    if failed:
        _print_failure_logs(runner, airflow_logs_path, failed)
    assert not failed, f"OpenLineage e2e DAG runs were not successful: {failed}"
