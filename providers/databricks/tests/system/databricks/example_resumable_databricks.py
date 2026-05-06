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
AIP-96 demonstration — Databricks resumable operator.

A subclass of ``DatabricksSubmitRunOperator`` that survives worker
disruption (pod eviction, OOM kill, node drain) by:

  1. Persisting the Databricks ``run_id`` via AIP-103 ``task_state``
     immediately after submit.
  2. Converting SIGTERM into ``AirflowTaskCheckpointed`` so the worker
     transitions to ``CHECKPOINTED`` instead of running ``on_kill``'s
     default ``cancel_run`` path.
  3. On the next attempt, reading the prior ``run_id`` from
     ``task_state`` and reconnecting (skipping submit).

This is the v1 integration pattern from AIP-96 v2: roughly 8 lines of
wrapper around the existing operator's submit + poll. The pattern shows
what a real provider integration looks like — not a synthetic example.

NOTE: this file lives under ``tests/system/databricks/`` to match how
other Databricks operator examples are organized in the repo. It is
illustrative and stacks on top of the AIP-96 PR set (#66402, #66410,
#66445); not for merge in this form. Once AIP-96 is accepted, the
upstream-eligible shape would be either a new operator class shipped
alongside ``DatabricksSubmitRunOperator`` in
``providers/databricks/.../operators/databricks.py``, or a
``resumable=True`` flag merged into the existing operator.
"""

from __future__ import annotations

import datetime
import signal
from typing import TYPE_CHECKING

from airflow.providers.databricks.operators.databricks import (
    DatabricksSubmitRunOperator,
    _handle_databricks_operator_execution,
    _handle_deferrable_databricks_operator_execution,
)
from airflow.providers.databricks.utils.databricks import normalise_json_content
from airflow.sdk import DAG
from airflow.sdk.exceptions import AirflowTaskCheckpointed

if TYPE_CHECKING:
    from airflow.sdk import Context


class ResumableDatabricksSubmitRunOperator(DatabricksSubmitRunOperator):
    """
    Databricks submit-run operator that survives worker disruption.

    Differences from the parent operator:

    - On the first attempt, persists ``self.run_id`` to AIP-103
      ``task_state`` immediately after submit.
    - On the next attempt (after CHECKPOINTED), reads the prior
      ``run_id`` from ``task_state`` and skips ``submit_run``,
      reconnecting to the existing Databricks job.
    - Installs a SIGTERM handler during execute that raises
      ``AirflowTaskCheckpointed`` instead of letting the default
      ``on_kill`` cancel the Databricks run.
    - Overrides ``on_kill`` to be a no-op when checkpoint-style
      preservation is desired (the default still cancels otherwise).
    """

    RESUME_KEY = "databricks_run_id"

    def execute(self, context: Context):
        # AIP-103: read prior run_id, if any.
        prior_run_id = context["task_state"].get(self.RESUME_KEY)

        if prior_run_id is not None:
            self.log.info("Resuming Databricks run_id=%s from prior attempt", prior_run_id)
            self.run_id = prior_run_id
        else:
            # First attempt: submit and persist.
            if (
                "pipeline_task" in self.json
                and self.json["pipeline_task"].get("pipeline_id") is None
                and self.json["pipeline_task"].get("pipeline_name")
            ):
                pipeline_name = self.json["pipeline_task"]["pipeline_name"]
                self.json["pipeline_task"]["pipeline_id"] = self._hook.find_pipeline_id_by_name(pipeline_name)
                del self.json["pipeline_task"]["pipeline_name"]
            json_normalised = normalise_json_content(self.json)
            self.run_id = self._hook.submit_run(json_normalised)
            context["task_state"].set(self.RESUME_KEY, self.run_id)
            self.log.info("Submitted Databricks run_id=%s and persisted to task_state", self.run_id)

        # Install a SIGTERM handler that signals checkpoint instead of
        # cancel-on-kill. A future framework-level helper could install
        # this automatically — see AIP-96 v2 reviewer questions.
        def _on_sigterm(signum, frame):
            raise AirflowTaskCheckpointed(checkpoint_data={"run_id": self.run_id})

        prior_handler = signal.signal(signal.SIGTERM, _on_sigterm)
        try:
            if self.deferrable:
                _handle_deferrable_databricks_operator_execution(self, self._hook, self.log, context)
            else:
                _handle_databricks_operator_execution(self, self._hook, self.log, context)
        finally:
            signal.signal(signal.SIGTERM, prior_handler)

        # Clear the resume key on success so a future DAG run starts fresh.
        context["task_state"].delete(self.RESUME_KEY)

    def on_kill(self):
        # Default DatabricksSubmitRunOperator.on_kill cancels the run.
        # In the resumable variant we PRESERVE the run so the next
        # attempt can reconnect via task_state. The SIGTERM handler in
        # execute() raises AirflowTaskCheckpointed before this fires;
        # this method is a safety net for non-SIGTERM kill paths.
        if self.run_id:
            self.log.info(
                "Task with run_id=%s killed; preserving the Databricks run "
                "for next-attempt reconnection (run_id stored in task_state).",
                self.run_id,
            )


# ---------------------------------------------------------------------------
# Example DAG using the resumable operator. The notebook task path and
# cluster spec are illustrative; replace with real values to run against
# a Databricks workspace.
# ---------------------------------------------------------------------------

with DAG(
    dag_id="example_resumable_databricks",
    description="AIP-96 demo: Databricks job survives worker disruption mid-execution",
    schedule=None,
    start_date=datetime.datetime(2026, 5, 1),
    catchup=False,
    tags=["example", "aip-96", "resumable", "databricks"],
) as dag:
    new_cluster_spec = {
        "spark_version": "13.3.x-scala2.12",
        "node_type_id": "i3.xlarge",
        "num_workers": 2,
    }

    notebook_task = {
        "notebook_path": "/Users/example@example.com/aip96_long_running_demo",
    }

    resumable_databricks_run = ResumableDatabricksSubmitRunOperator(
        task_id="resumable_databricks_run",
        databricks_conn_id="databricks_default",
        new_cluster=new_cluster_spec,
        notebook_task=notebook_task,
        retries=2,
        retry_delay=datetime.timedelta(seconds=30),
    )
