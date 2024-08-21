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
Example DAG for demonstrating the behavior of the AssetAlias feature in Airflow, including conditional and
dataset expression-based scheduling.

Notes on usage:

Turn on all the DAGs.

Before running any DAG, the schedule of the "asset_alias_example_alias_consumer_with_no_taskflow" DAG will show as "unresolved AssetAlias".
This is expected because the asset alias has not been resolved into any dataset yet.

Once the "dataset_s3_bucket_producer_with_no_taskflow" DAG is triggered, the "dataset_s3_bucket_consumer_with_no_taskflow" DAG should be triggered upon completion.
This is because the asset alias "example-alias-no-taskflow" is used to add a dataset event to the dataset "s3://bucket/my-task-with-no-taskflow"
during the "produce_asset_events_through_asset_alias_with_no_taskflow" task. Also, the schedule of the "asset_alias_example_alias_consumer_with_no_taskflow" DAG should change to "Dataset" as
the asset alias "example-alias-no-taskflow" is now resolved to the dataset "s3://bucket/my-task-with-no-taskflow" and this DAG should also be triggered.
"""

from __future__ import annotations

import pendulum

from airflow import DAG
from airflow.assets import AssetAlias, Dataset
from airflow.operators.python import PythonOperator

with DAG(
    dag_id="dataset_s3_bucket_producer_with_no_taskflow",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["producer", "dataset"],
):

    def produce_asset_events():
        pass

    PythonOperator(
        task_id="produce_asset_events",
        outlets=[Dataset("s3://bucket/my-task-with-no-taskflow")],
        python_callable=produce_asset_events,
    )


with DAG(
    dag_id="asset_alias_example_alias_producer_with_no_taskflow",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["producer", "asset-alias"],
):

    def produce_asset_events_through_asset_alias_with_no_taskflow(*, outlet_events=None):
        bucket_name = "bucket"
        object_path = "my-task"
        outlet_events["example-alias-no-taskflow"].add(Dataset(f"s3://{bucket_name}/{object_path}"))

    PythonOperator(
        task_id="produce_asset_events_through_asset_alias_with_no_taskflow",
        outlets=[AssetAlias("example-alias-no-taskflow")],
        python_callable=produce_asset_events_through_asset_alias_with_no_taskflow,
    )

with DAG(
    dag_id="dataset_s3_bucket_consumer_with_no_taskflow",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    schedule=[Dataset("s3://bucket/my-task-with-no-taskflow")],
    catchup=False,
    tags=["consumer", "dataset"],
):

    def consume_asset_event():
        pass

    PythonOperator(task_id="consume_asset_event", python_callable=consume_asset_event)

with DAG(
    dag_id="asset_alias_example_alias_consumer_with_no_taskflow",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    schedule=[AssetAlias("example-alias-no-taskflow")],
    catchup=False,
    tags=["consumer", "asset-alias"],
):

    def consume_asset_event_from_asset_alias(*, inlet_events=None):
        for event in inlet_events[AssetAlias("example-alias-no-taskflow")]:
            print(event)

    PythonOperator(
        task_id="consume_asset_event_from_asset_alias",
        python_callable=consume_asset_event_from_asset_alias,
        inlets=[AssetAlias("example-alias-no-taskflow")],
    )
