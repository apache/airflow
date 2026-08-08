#
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
Example DAG demonstrating ExecutorCallback (DeadlineAlert) support in the Kubernetes Executor.

The deadline is fixed to a date in the past so the callback fires immediately after each
DAG run is created, exercising the full callback-pod lifecycle on Kubernetes.
"""

from __future__ import annotations

import time
from datetime import datetime, timedelta, timezone

import pendulum

from airflow.sdk import DAG, task

try:
    from airflow.sdk.definitions.callback import SyncCallback
    from airflow.sdk.definitions.deadline import DeadlineAlert, DeadlineReference

    _DEADLINE_AVAILABLE = True
except ImportError:
    _DEADLINE_AVAILABLE = False


def deadline_callback(context, **kwargs):
    """Simple deadline alert callback – logs context and exits cleanly."""
    dag_run = context.get("dag_run", {})
    dag_id = dag_run.get("dag_id", "unknown")
    run_id = dag_run.get("dag_run_id", "unknown")
    print(f"[deadline_callback] Deadline alert fired for dag_id={dag_id} run_id={run_id}")


def slow_deadline_callback(context, **kwargs):
    """Slow deadline alert callback that sleeps 30s – used for scheduler-restart tests."""
    dag_run = context.get("dag_run", {})
    dag_id = dag_run.get("dag_id", "unknown")
    run_id = dag_run.get("dag_run_id", "unknown")
    print(f"[slow_deadline_callback] Starting for dag_id={dag_id} run_id={run_id}")
    time.sleep(30)
    print(f"[slow_deadline_callback] Done for dag_id={dag_id} run_id={run_id}")


def failing_deadline_callback(**_):
    """Intentionally raises – used by the K8s executor failure integration test."""
    raise RuntimeError("Intentional callback failure for testing")


_PAST_DEADLINE = datetime(2020, 1, 1, tzinfo=timezone.utc)

if _DEADLINE_AVAILABLE:
    with DAG(
        dag_id="example_deadline_callback",
        schedule=None,
        start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
        catchup=False,
        tags=["example", "deadline", "callback"],
        deadline=DeadlineAlert(
            reference=DeadlineReference.FIXED_DATETIME(_PAST_DEADLINE),
            interval=timedelta(hours=1),
            callback=SyncCallback("airflow.example_dags.example_deadline_callback.deadline_callback"),
        ),
    ) as dag:

        @task
        def dummy_task():
            """Placeholder task; the interesting work happens in the deadline callback."""
            print("dummy_task executed")

        dummy_task()

    with DAG(
        dag_id="example_deadline_callback_slow",
        schedule=None,
        start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
        catchup=False,
        tags=["example", "deadline", "callback"],
        deadline=DeadlineAlert(
            reference=DeadlineReference.FIXED_DATETIME(_PAST_DEADLINE),
            interval=timedelta(hours=1),
            callback=SyncCallback("airflow.example_dags.example_deadline_callback.slow_deadline_callback"),
        ),
    ) as dag_slow:

        @task
        def dummy_task_slow():
            """Placeholder task for the slow-callback DAG."""
            print("dummy_task_slow executed")

        dummy_task_slow()

    with DAG(
        dag_id="example_deadline_callback_failing",
        schedule=None,
        start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
        catchup=False,
        tags=["example", "deadline", "callback"],
        deadline=DeadlineAlert(
            reference=DeadlineReference.FIXED_DATETIME(_PAST_DEADLINE),
            interval=timedelta(hours=1),
            callback=SyncCallback("airflow.example_dags.example_deadline_callback.failing_deadline_callback"),
        ),
    ) as dag_failing:

        @task
        def dummy_task_failing():
            """Placeholder task for the failing-callback DAG."""
            print("dummy_task_failing executed")

        dummy_task_failing()
