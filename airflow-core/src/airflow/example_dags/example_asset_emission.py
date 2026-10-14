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
"""Experimental immediate emission from tasks and Dag callbacks, including partitions."""

from __future__ import annotations

import pendulum

from airflow.sdk import DAG, Asset, PartitionedAssetTimetable, get_current_context, task

published = Asset(name="immediate-published-partitions", uri="example://immediate/published")
failures = Asset(name="immediate-failed-runs", uri="example://immediate/failures")


def emit_failed_run(context):
    """Do not require a representative task instance in the Dag callback context."""
    receipt = failures.emit(
        extra={"reason": "Dag failed", "run_id": context["run_id"]},
        partition_key="2026-10-01",
        partition_date=pendulum.datetime(2026, 10, 1, tz="UTC"),
    )
    print(f"Dag callback persisted event {receipt.id} at {receipt.timestamp}")


with DAG(dag_id="example_immediate_asset_task", schedule=None, catchup=False, tags=["example"]):

    @task(retries=0)
    def publish_partitions_then_fail():
        # No outlets: immediate emission does not require task success, and an
        # outlet here would produce an additional success-only event.
        for day in (1, 2):
            date = pendulum.datetime(2026, 10, day, tz="UTC")
            receipt = published.emit(
                extra={"rows": 100 * day},
                partition_key=date.format("YYYY-MM-DD"),
                partition_date=date,
            )
            print(f"Persisted partition {date.date()}: event {receipt.id}")
        raise ValueError("Deliberate failure after publishing two durable events")

    publish_partitions_then_fail()


with DAG(
    dag_id="example_immediate_asset_dag_callback",
    schedule=None,
    catchup=False,
    on_failure_callback=emit_failed_run,
    tags=["example"],
):

    @task(retries=0)
    def fail():
        raise ValueError("Deliberate failure to invoke the Dag failure callback")

    fail()


@task
def show_partition_events():
    context = get_current_context()
    run = context["dag_run"]
    print(f"Consumer partition: {run.partition_key}, partition date: {run.partition_date}")
    for events in context["triggering_asset_events"].values():
        for event in events:
            print(
                f"Source: {event.source_dag_id}/{event.source_run_id}, "
                f"task: {event.source_task_id}, key: {event.partition_key}, extra: {event.extra}"
            )


with DAG(
    dag_id="example_immediate_asset_task_consumer",
    schedule=PartitionedAssetTimetable(assets=published),
    catchup=False,
    tags=["example"],
):
    show_partition_events()

with DAG(
    dag_id="example_immediate_asset_callback_consumer",
    schedule=PartitionedAssetTimetable(assets=failures),
    catchup=False,
    tags=["example"],
):
    show_partition_events()
