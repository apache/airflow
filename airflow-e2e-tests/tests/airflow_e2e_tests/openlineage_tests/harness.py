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
Drive all OpenLineage Dags in the deployed stack and collect their final run state.

Talks to the local Airflow REST API v2 through the shared ``AirflowClient`` (same auth and retry
setup as the other airflow-e2e-tests suites). The actual OpenLineage event validation happens inside
each Dag's terminal ``OpenLineageTestOperator`` task, so a run that ends ``success`` means its
emitted events matched the expected templates.
"""

from __future__ import annotations

import datetime as dt
import re
import time
from collections.abc import Callable, Iterable
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import TYPE_CHECKING

import requests
from rich.console import Console

if TYPE_CHECKING:
    from airflow_e2e_tests.e2e_test_utils.clients import AirflowClient

console = Console(width=400, color_system="standard")

WARMUP_DAG_ID = "openlineage_warmup_dag"
# Requests sent to the API server at once when unpausing, triggering or cleaning up
API_CONCURRENCY = 8
DAG_RUN_POLL_SECONDS = 2
# The local API server answers in well under a second, even while it serves the tasks of every Dag
REQUEST_TIMEOUT_SECONDS = 5
# Shared wall-clock budget for one _trigger_and_wait() call, covering every Dag triggered in that
# batch — bounds total polling time regardless of how many Dags are involved
BATCH_TIMEOUT_SECONDS = 360  # 6 min, each dagrun timeout is set to 5 minutes

# Dags that take longer to complete — trigger these first so they run while the rest are triggered.
LONG_RUNNING_DAG_IDS = (
    "openlineage_defer_simple_dag",
    # Its sensor must start well within SYSTEM_TESTS_OL_TIMEDELTA of the trigger to be rescheduled
    "openlineage_sensor_reschedule_dag",
    # Run their loop iterations, retries and mapped instances one after another
    "openlineage_task_loop_dag",
    "openlineage_task_loop_groups_dag",
    # Change, clear and wait for child Dag runs one step after another
    "openlineage_task_loop_state_dag",
    "openlineage_manual_state_dag",
    "openlineage_clear_dag",
    "openlineage_task_outcomes_dag",
)

# Child Dags spawned by TriggerDagRunOperator carry this marker; they must not be triggered or
# state-checked directly (their parent triggers them).
NO_TRIGGER_MARKER = "__notrigger"


def discover_expected_dag_ids(dags_folder: Path) -> set[str]:
    """
    Top-level ``DAG_ID = "..."`` declarations across the prepared example Dags.

    Used as a coverage check: every expected Dag must actually load in the deployment (a missing one
    signals an import/parse failure rather than a test failure).
    """
    pattern = re.compile(r'^DAG_ID\s*=\s*["\'](?P<dag_id>[^"\']+)["\']')
    dag_ids: set[str] = set()
    for dag_file in (dags_folder / "system" / "openlineage").glob("example_openlineage_*.py"):
        for line in dag_file.read_text().splitlines():
            match = pattern.match(line)
            if match:
                dag_ids.add(match.group("dag_id"))
    return dag_ids


class OpenLineageE2ERunner:
    """Triggers all OpenLineage Dags against a running deployment and collects their final states."""

    def __init__(self, client: AirflowClient):
        self.client = client
        now = dt.datetime.now(tz=dt.UTC)
        self.run_id = f"ci_triggered_{now.isoformat()}"
        self.retry_run_id = f"{self.run_id}_retry1"
        # dag_id -> whether its final state came from retry_run_id rather than run_id; populated by run().
        self.retried_dag_ids: set[str] = set()

    def _request(self, method: str, endpoint: str, **kwargs):
        return self.client._make_request(
            method=method, endpoint=endpoint, timeout=REQUEST_TIMEOUT_SECONDS, **kwargs
        )

    def _get_all(self, endpoint: str, key: str) -> list[dict]:
        """Every item of a list endpoint, which returns at most ``[api] maximum_page_limit`` (100) per request."""
        items: list[dict] = []
        separator = "&" if "?" in endpoint else "?"
        while True:
            body = self._request("GET", f"{endpoint}{separator}limit=100&offset={len(items)}")
            items += body[key]
            if not body[key] or len(items) >= body["total_entries"]:
                return items

    def list_dags(self) -> dict[str, bool]:
        """dag_id -> whether it is paused."""
        return {dag["dag_id"]: dag["is_paused"] for dag in self._get_all("dags", "dags")}

    def get_import_errors(self) -> dict[str, str]:
        """Dag file name -> stack trace of its import error."""
        return {
            error["filename"].rsplit("/", 1)[-1]: error.get("stack_trace", "")
            for error in self._get_all("importErrors", "import_errors")
        }

    def get_task_states(self, dag_id: str, run_id: str) -> dict[str, str]:
        """task_id -> state for every task instance in a dag run."""
        task_instances = self._get_all(f"dags/{dag_id}/dagRuns/{run_id}/taskInstances", "task_instances")
        return {ti["task_id"]: ti["state"] for ti in task_instances}

    def wait_for_dags_loaded(
        self, expected_dag_ids: set[str], timeout: int = 180, poll_interval: int = 3
    ) -> dict[str, bool]:
        """
        Poll until the dag-processor has parsed every expected Dag, or failed to import its file.

        The warmup Dag alone is not enough: on a slow runner the other files may still wait in the parsing queue,
        and a Dag not listed yet would never be triggered.
        """
        deadline = time.monotonic() + timeout
        dags: dict[str, bool] = {}
        while time.monotonic() < deadline:
            dags = self.list_dags()
            not_loaded = expected_dag_ids - dags.keys()
            if WARMUP_DAG_ID in dags and not_loaded:
                failed_files = self.get_import_errors().keys()
                not_loaded = {dag_id for dag_id in not_loaded if f"example_{dag_id}.py" not in failed_files}
            if WARMUP_DAG_ID in dags and not not_loaded:
                console.print(f"[green]Dags loaded ({len(dags)} found)")
                return dags
            console.print(
                f"[yellow]Waiting for Dags to load (have {len(dags)}, still parsing {len(not_loaded)})..."
            )
            time.sleep(poll_interval)
        if WARMUP_DAG_ID not in dags:
            raise RuntimeError(
                f"Dags did not load within {timeout}s (warmup Dag missing; have {sorted(dags)})."
            )
        console.print(f"[red]Dags not loaded within {timeout}s: {sorted(expected_dag_ids - dags.keys())}")
        return dags

    def _run_concurrently(self, func: Callable, items: Iterable) -> list:
        self.client.token  # Fetched once, before the threads share it
        with ThreadPoolExecutor(max_workers=API_CONCURRENCY) as executor:
            return list(executor.map(func, items))

    def unpause_dag(self, dag_id: str) -> None:
        try:
            self._request("PATCH", f"dags/{dag_id}", json={"is_paused": False})
        except requests.RequestException as exc:
            console.print(f"[red]Failed to unpause Dag `{dag_id}`: {exc}")

    def trigger_dag_run(self, dag_id: str, run_id: str) -> bool:
        now = dt.datetime.now(tz=dt.UTC).isoformat()
        payload = {"dag_run_id": run_id, "logical_date": now, "conf": {}}
        try:
            self._request("POST", f"dags/{dag_id}/dagRuns", json=payload)
        except requests.RequestException as exc:
            console.print(f"[red]Failed to trigger Dag `{dag_id}`: {exc}")
            return False
        return True

    def get_dag_run(self, dag_id: str, run_id: str) -> dict | None:
        try:
            return self._request("GET", f"dags/{dag_id}/dagRuns/{run_id}")
        except requests.RequestException as exc:
            # A single failed status check must not end the test: the Dag run is checked again next round
            console.print(f"[yellow]Could not get the state of `{dag_id}`, checking again: {exc}")
            return None

    def wait_for_dag_runs(self, dag_ids: list[str], run_id: str, timeout: int) -> dict[str, str]:
        """Poll every pending Dag run each round until all have finished; those left at the deadline time out."""
        deadline = time.monotonic() + timeout
        statuses: dict[str, str] = {}
        pending = list(dag_ids)
        while pending and time.monotonic() < deadline:
            dag_runs = self._run_concurrently(lambda dag_id: self.get_dag_run(dag_id, run_id), pending)
            for dag_id, dag_run in zip(pending, dag_runs):
                if dag_run and dag_run["state"] not in ("running", "queued"):
                    statuses[dag_id] = dag_run["state"]
                    duration = ""
                    if dag_run.get("start_date") and dag_run.get("end_date"):
                        seconds = (
                            dt.datetime.fromisoformat(dag_run["end_date"])
                            - dt.datetime.fromisoformat(dag_run["start_date"])
                        ).total_seconds()
                        duration = f" after {seconds:.0f}s"
                    console.print(f"[blue]Dag `{dag_id}` finished in state: {dag_run['state']}{duration}")
            pending = [dag_id for dag_id in pending if dag_id not in statuses]
            if pending:
                time.sleep(DAG_RUN_POLL_SECONDS)
        for dag_id in pending:
            console.print(f"[red]Dag `{dag_id}` did not finish within {timeout}s; marking as timed_out.")
            statuses[dag_id] = "timed_out"
        return statuses

    def wait_for_running_dag_runs(self, timeout: int = 180) -> None:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            running = self._get_all("dags/~/dagRuns?state=running&state=queued", "dag_runs")
            if not running:
                return
            console.print(f"[yellow]Waiting for {len(running)} Dag run(s) started on their own to finish...")
            time.sleep(DAG_RUN_POLL_SECONDS)
        console.print(f"[red]Dag runs still running after {timeout}s, starting the test runs anyway")

    def clear_airflow_variables(self) -> None:
        keys = [variable["key"] for variable in self._get_all("variables", "variables")]
        if keys:
            # Skip the keys deleted meanwhile, e.g. by check_events of a cron Dag's auto-run
            action = {"action": "delete", "entities": keys, "action_on_non_existence": "skip"}
            self._request("PATCH", "variables", json={"actions": [action]})

    def warmup(self, dags: dict[str, bool]) -> None:
        """Unpause the Dags still paused (none, as the deployment creates them unpaused) and run the warmup Dag."""
        if WARMUP_DAG_ID not in dags:
            raise KeyError(f"Warmup Dag `{WARMUP_DAG_ID}` not found in deployment.")
        self._run_concurrently(self.unpause_dag, [dag_id for dag_id, is_paused in dags.items() if is_paused])
        if not self.trigger_dag_run(WARMUP_DAG_ID, self.run_id):
            raise RuntimeError(f"Failed to trigger warmup Dag `{WARMUP_DAG_ID}`")
        self.wait_for_dag_runs([WARMUP_DAG_ID], self.run_id, timeout=300)

    def _trigger_and_wait(
        self, dag_ids: list[str], run_id: str, batch_timeout: int = BATCH_TIMEOUT_SECONDS
    ) -> dict[str, str]:
        statuses: dict[str, str] = {}
        # The long-running Dags first, so they are not queued behind the others
        long_running = [d for d in LONG_RUNNING_DAG_IDS if d in dag_ids]
        others = [d for d in dag_ids if d not in LONG_RUNNING_DAG_IDS]
        for group in (long_running, others):
            triggered = self._run_concurrently(lambda dag_id: self.trigger_dag_run(dag_id, run_id), group)
            statuses.update({dag_id: "trigger_error" for dag_id, ok in zip(group, triggered) if not ok})
        triggered_ids = [dag_id for dag_id in long_running + others if dag_id not in statuses]
        statuses.update(self.wait_for_dag_runs(triggered_ids, run_id, timeout=batch_timeout))
        return statuses

    def run(self, expected_dag_ids: set[str]) -> dict[str, str]:
        """Run the full cycle and return ``{dag_id: final_state}`` for every triggered Dag."""
        dags = self.wait_for_dags_loaded(expected_dag_ids)
        self.warmup(dags)
        # Scheduled Dags run on their own once loaded. Their check_events deletes the Variables it checked, those of
        # the test runs of the same Dag too, so wait for them to finish before clearing their events.
        self.wait_for_running_dag_runs()
        self.clear_airflow_variables()

        triggerable = [
            dag_id for dag_id in dags if dag_id != WARMUP_DAG_ID and NO_TRIGGER_MARKER not in dag_id
        ]
        statuses = self._trigger_and_wait(triggerable, self.run_id)
        self.clear_airflow_variables()

        failed = [dag_id for dag_id, state in statuses.items() if state != "success"]
        if failed:
            self.retried_dag_ids = set(failed)
            console.print(f"[yellow]Retrying {len(failed)} failed Dag(s) once: {failed}")
            retry_statuses = self._trigger_and_wait(failed, self.retry_run_id)
            self.clear_airflow_variables()
            recovered = sorted(dag_id for dag_id, state in retry_statuses.items() if state == "success")
            if recovered:
                # The retry counts as a pass, but surface it so first-run flakiness is not hidden.
                console.print(f"[yellow]⚠ Dags that passed only on retry (flaky first run): {recovered}")
            statuses.update(retry_statuses)

        for dag_id in expected_dag_ids:
            if dag_id not in statuses:
                statuses[dag_id] = "missing"

        return statuses
