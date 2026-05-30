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
"""Example Dag demonstrating :class:`~airflow.sdk.DeadlineAlert` usage."""

from __future__ import annotations

import logging
import time
from datetime import timedelta

import pendulum

# [START example_deadline_alert]
from airflow.sdk import DAG, AsyncCallback, DeadlineAlert, DeadlineReference, task

log = logging.getLogger(__name__)


async def notify_deadline_missed(**kwargs):
    """Async callback executed by the Triggerer when the deadline is missed."""
    context = kwargs.get("context", {})
    dag_run = context.get("dag_run")
    log.warning("Deadline missed for dag_run=%s", dag_run)


with DAG(
    dag_id="example_deadline_alert",
    description="Demonstrates DeadlineAlert with DAGRUN_QUEUED_AT reference.",
    schedule=None,
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    tags=["example", "deadline"],
    deadline=DeadlineAlert(
        reference=DeadlineReference.DAGRUN_QUEUED_AT,
        interval=timedelta(seconds=30),
        callback=AsyncCallback(notify_deadline_missed),
        name="example_deadline",
    ),
) as dag:

    @task
    def hello_deadline():
        log.info("Hello from a Dag with a deadline!")
        # Sleep past the deadline (sync, not a deferred sensor) to keep the demo focused on DeadlineAlert.
        time.sleep(60)

    hello_deadline()
# [END example_deadline_alert]
