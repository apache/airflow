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
"""Example Dag demonstrating a :class:`~airflow.sdk.SyncCallback` run by an executor as a deadline callback."""

from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta

import pendulum

from airflow.sdk import DAG, DeadlineAlert, DeadlineReference, SyncCallback, task

log = logging.getLogger(__name__)


def notify_deadline_missed(context: dict, **kwargs):
    """Sync callback run by the executor, for example in a pod on KubernetesExecutor."""
    dag_run = context.get("dag_run")
    log.warning("Deadline missed for dag_run=%s", dag_run)


with DAG(
    dag_id="example_deadline_callback",
    description="Demonstrates a SyncCallback deadline callback run by the executor.",
    schedule=None,
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    tags=["example", "deadline", "callback"],
    deadline=DeadlineAlert(
        # A fixed past datetime makes the deadline pass as soon as the Dag run is created.
        reference=DeadlineReference.FIXED_DATETIME(datetime(2020, 1, 1, tzinfo=UTC)),
        interval=timedelta(hours=1),
        callback=SyncCallback(notify_deadline_missed),
        name="example_deadline_callback",
    ),
) as dag:

    @task
    def hello_deadline():
        log.info("Hello from a Dag whose deadline has already passed!")

    hello_deadline()
