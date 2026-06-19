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
Example Dag that demonstrates the canonical AIP-103 task state store pattern: a task submits a
long-running external job, stores the job handle in task state store, and polls
until completion.

The first attempt always fails after submitting the job (simulating a
worker crash / connection to external system being lost). The retry reads
the job ID from task state store and reattaches to the already-running job instead
of submitting a duplicate.
"""

from __future__ import annotations

import random
import string
import time
from datetime import datetime, timedelta, timezone

from airflow.sdk import DAG, task
from airflow.sdk.execution_time.context import NEVER_EXPIRE


def _submit_job() -> str:
    """Simulate submitting an external job. Returns a job ID."""
    time.sleep(1)
    return "job-" + "".join(random.choices(string.ascii_lowercase + string.digits, k=8))


def _poll_job(job_id: str) -> dict:
    """Simulate polling an external job until complete."""
    time.sleep(1)
    return {"job_id": job_id, "status": "succeeded", "rows_written": random.randint(100, 10_000)}


with DAG(
    dag_id="example_task_state_store",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["example", "task-state-store"],
    doc_md=__doc__,
):

    @task(retries=2, retry_delay=timedelta(seconds=5))
    def run_job(task_state_store=None, ti=None):
        job_id = task_state_store.get("job_id")
        if job_id:
            print(f"Try {ti.try_number}: reattaching to existing job: {job_id}")
        else:
            job_id = _submit_job()
            # Store with NEVER_EXPIRE so the job ID survives across all retries.
            task_state_store.set("job_id", job_id, retention=NEVER_EXPIRE)
            task_state_store.set("submitted_at", datetime.now(tz=timezone.utc).isoformat())
            print(f"Try {ti.try_number}: submitted job: {job_id}")

            # Simulate a crash after submission on the first attempt.
            # The retry will reattach to the same job instead of submitting a duplicate.
            raise RuntimeError(
                f"Simulated failure after submitting {job_id}. The next retry will reattach to this job."
            )

        task_state_store.set("status", "running")
        result = _poll_job(job_id)
        task_state_store.set("status", "complete")
        task_state_store.set("result", result)

        print(f"Try {ti.try_number}: job complete — {result['rows_written']} rows written")
        return result["rows_written"]

    run_job()
