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
Drive all OpenLineage DAGs in the deployed stack and collect their final run state.

Adapted from the OpenLineage dags-dashboard ``trigger_dag.py`` integration harness: it talks to a
local Airflow REST API v2 (rather than an Astro deployment), authenticates with a SimpleAuthManager
bearer token, and reports through logs only. The actual OpenLineage event validation happens inside
each DAG's terminal ``OpenLineageTestOperator`` task, so a run that ends ``success`` means its
emitted events matched the expected templates.
"""

from __future__ import annotations

import datetime as dt
import re
import time
from pathlib import Path

import requests
from constants import console
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

WARMUP_DAG_ID = "openlineage_warmup_dag"
TRIGGER_DELAY_SECONDS = 2
# Shared wall-clock budget for one _trigger_and_wait() call, covering every DAG triggered in that
# batch — bounds total polling time regardless of how many DAGs are involved
BATCH_TIMEOUT_SECONDS = 360  # 6 min, each dagrun timeout is set to 5 minutes

# DAGs that take longer to complete — trigger these first so they run while the rest are triggered.
LONG_RUNNING_DAG_IDS = ("openlineage_defer_simple_dag",)

# Child DAGs spawned by TriggerDagRunOperator carry this marker; they must not be triggered or
# state-checked directly (their parent triggers them).
NO_TRIGGER_MARKER = "__notrigger"


def discover_expected_dag_ids(dags_folder: Path) -> set[str]:
    """
    Top-level ``DAG_ID = "..."`` declarations across the prepared example DAGs.

    Used as a coverage check: every expected DAG must actually load in the deployment (a missing one
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


def _session_with_retries() -> requests.Session:
    retry = Retry(
        total=10,
        connect=5,
        read=10,
        backoff_factor=2,
        raise_on_status=False,
        allowed_methods=["GET", "POST", "PATCH", "DELETE"],
        status_forcelist=[429, 500, 502, 503, 504],
    )
    session = requests.Session()
    session.mount("http://", HTTPAdapter(max_retries=retry))
    session.mount("https://", HTTPAdapter(max_retries=retry))
    return session


class OpenLineageE2ERunner:
    """Triggers all OpenLineage DAGs against a running deployment and collects their final states."""

    def __init__(self, api_base_url: str, headers: dict[str, str]):
        self.api_url = f"{api_base_url}/api/v2"
        self.headers = headers
        self.session = _session_with_retries()
        now = dt.datetime.now(tz=dt.timezone.utc)
        self.run_id = f"ci_triggered_{now.isoformat()}"
        self.retry_run_id = f"{self.run_id}_retry1"
        # dag_id -> whether its final state came from retry_run_id rather than run_id; populated by run().
        self.retried_dag_ids: set[str] = set()

    def wait_for_airflow_api(self, max_attempts: int = 30, poll_interval: int = 10) -> None:
        for attempt in range(1, max_attempts + 1):
            try:
                response = requests.get(f"{self.api_url}/dags?limit=1", headers=self.headers, timeout=15)
                if response.status_code < 500:
                    console.print(f"[green]Airflow API ready after {attempt} attempt(s)")
                    return
            except requests.exceptions.RequestException as exc:
                console.print(f"[yellow]Airflow API not ready (attempt {attempt}/{max_attempts}): {exc}")
            time.sleep(poll_interval)
        raise RuntimeError(f"Airflow API did not become ready after {max_attempts} attempts.")

    def list_dags(self) -> list[str]:
        response = self.session.get(f"{self.api_url}/dags?limit=500", headers=self.headers)
        response.raise_for_status()
        return [dag["dag_id"] for dag in response.json()["dags"]]

    def get_task_states(self, dag_id: str, run_id: str) -> dict[str, str]:
        """task_id -> state for every task instance in a dag run."""
        response = self.session.get(
            f"{self.api_url}/dags/{dag_id}/dagRuns/{run_id}/taskInstances", headers=self.headers
        )
        response.raise_for_status()
        return {ti["task_id"]: ti["state"] for ti in response.json()["task_instances"]}

    def wait_for_dags_loaded(self, timeout: int = 180, poll_interval: int = 5) -> list[str]:
        """Poll until the dag-processor has parsed the DAGs (the warmup DAG is the readiness marker)."""
        deadline = time.monotonic() + timeout
        dag_ids: list[str] = []
        while time.monotonic() < deadline:
            dag_ids = self.list_dags()
            if WARMUP_DAG_ID in dag_ids:
                console.print(f"[green]DAGs loaded ({len(dag_ids)} found)")
                return dag_ids
            console.print(f"[yellow]Waiting for DAGs to load (have {len(dag_ids)})...")
            time.sleep(poll_interval)
        raise RuntimeError(f"DAGs did not load within {timeout}s (warmup DAG missing; have {dag_ids}).")

    def unpause_dag(self, dag_id: str) -> None:
        response = self.session.patch(
            f"{self.api_url}/dags/{dag_id}", headers=self.headers, json={"is_paused": False}
        )
        if response.status_code != 200:
            console.print(f"[red]Failed to unpause DAG `{dag_id}`: {response.text}")

    def trigger_dag_run(self, dag_id: str, run_id: str) -> bool:
        now = dt.datetime.now(tz=dt.timezone.utc).isoformat()
        payload = {"dag_run_id": run_id, "logical_date": now, "conf": {}}
        response = self.session.post(
            f"{self.api_url}/dags/{dag_id}/dagRuns", headers=self.headers, json=payload
        )
        if response.status_code not in (200, 201):
            console.print(f"[red]Failed to trigger DAG `{dag_id}`: {response.text}")
            return False
        return True

    def wait_for_dag_run_to_complete(self, dag_id: str, run_id: str, timeout: int = 600) -> str:
        url = f"{self.api_url}/dags/{dag_id}/dagRuns/{run_id}"
        deadline = time.monotonic() + timeout
        state = "unknown"
        while time.monotonic() < deadline:
            response = self.session.get(url, headers=self.headers)
            response.raise_for_status()
            state = response.json()["state"]
            if state not in ("running", "queued"):
                break
            time.sleep(15)
        console.print(f"[blue]DAG `{dag_id}` finished in state: {state}")
        return state

    def clear_airflow_variables(self) -> None:
        response = self.session.get(f"{self.api_url}/variables?limit=500", headers=self.headers)
        response.raise_for_status()
        keys = [variable["key"] for variable in response.json()["variables"]]
        for key in keys:
            self.session.delete(f"{self.api_url}/variables/{key}", headers=self.headers)

    def warmup(self, dag_ids: list[str]) -> None:
        """Unpause all DAGs and run the warmup DAG so the worker is confirmed ready."""
        if WARMUP_DAG_ID not in dag_ids:
            raise KeyError(f"Warmup DAG `{WARMUP_DAG_ID}` not found in deployment.")
        for dag_id in dag_ids:
            self.unpause_dag(dag_id)
            time.sleep(TRIGGER_DELAY_SECONDS)
        if not self.trigger_dag_run(WARMUP_DAG_ID, self.run_id):
            raise RuntimeError(f"Failed to trigger warmup DAG `{WARMUP_DAG_ID}`")
        self.wait_for_dag_run_to_complete(WARMUP_DAG_ID, self.run_id)

    def _trigger_and_wait(
        self, dag_ids: list[str], run_id: str, batch_timeout: int = BATCH_TIMEOUT_SECONDS
    ) -> dict[str, str]:
        statuses: dict[str, str] = {}
        ordered = [d for d in LONG_RUNNING_DAG_IDS if d in dag_ids]
        ordered += [d for d in dag_ids if d not in LONG_RUNNING_DAG_IDS]
        for dag_id in ordered:
            if not self.trigger_dag_run(dag_id, run_id):
                statuses[dag_id] = "trigger_error"
            time.sleep(TRIGGER_DELAY_SECONDS)
        deadline = time.monotonic() + batch_timeout
        for dag_id in ordered:
            if dag_id in statuses:
                continue
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                console.print(
                    f"[red]Batch deadline exceeded before checking `{dag_id}`; marking as timed_out."
                )
                statuses[dag_id] = "timed_out"
                continue
            statuses[dag_id] = self.wait_for_dag_run_to_complete(dag_id, run_id, timeout=int(remaining))
        return statuses

    def run(self, expected_dag_ids: set[str]) -> dict[str, str]:
        """Run the full cycle and return ``{dag_id: final_state}`` for every triggered DAG."""
        self.wait_for_airflow_api()
        dag_ids = self.wait_for_dags_loaded()
        if not dag_ids:
            raise ValueError("No DAGs found in the deployment.")

        self.warmup(dag_ids)
        # Auto-runs of cron/timetable DAGs may have started while unpaused — clear their events so
        # the test runs start from a clean slate.
        time.sleep(10)
        self.clear_airflow_variables()

        triggerable = [
            dag_id for dag_id in dag_ids if dag_id != WARMUP_DAG_ID and NO_TRIGGER_MARKER not in dag_id
        ]
        statuses = self._trigger_and_wait(triggerable, self.run_id)
        self.clear_airflow_variables()

        failed = [dag_id for dag_id, state in statuses.items() if state != "success"]
        if failed:
            self.retried_dag_ids = set(failed)
            console.print(f"[yellow]Retrying {len(failed)} failed DAG(s) once: {failed}")
            retry_statuses = self._trigger_and_wait(failed, self.retry_run_id)
            self.clear_airflow_variables()
            recovered = sorted(dag_id for dag_id, state in retry_statuses.items() if state == "success")
            if recovered:
                # The retry counts as a pass, but surface it so first-run flakiness is not hidden.
                console.print(f"[yellow]⚠ DAGs that passed only on retry (flaky first run): {recovered}")
            statuses.update(retry_statuses)

        for dag_id in expected_dag_ids:
            if dag_id not in statuses:
                statuses[dag_id] = "missing"

        return statuses
