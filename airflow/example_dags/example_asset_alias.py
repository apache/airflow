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
asset expression-based scheduling.

Notes on usage:

Turn on all the DAGs.

Before running any DAG, the schedule of the "asset_alias_example_alias_consumer" DAG will show as "Unresolved AssetAlias".
This is expected because the asset alias has not been resolved into any asset yet.

Once the "asset_s3_bucket_producer" DAG is triggered, the "asset_s3_bucket_consumer" DAG should be triggered upon completion.
This is because the asset alias "example-alias" is used to add an asset event to the asset "s3://bucket/my-task"
during the "produce_asset_events_through_asset_alias" task.
As the DAG "asset-alias-consumer" relies on asset alias "example-alias" which was previously unresolved,
the DAG "asset-alias-consumer" (along with all the DAGs in the same file) will be re-parsed and
thus update its schedule to the asset "s3://bucket/my-task" and will also be triggered.
"""

from __future__ import annotations

import pendulum

from airflow import DAG
from airflow.assets import AssetAlias, Dataset
from airflow.decorators import task

with DAG(
    dag_id="asset_s3_bucket_producer",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["producer", "asset"],
):

    @task(outlets=[Dataset("s3://bucket/my-task")])
    def produce_asset_events():
        pass

    produce_asset_events()

with DAG(
    dag_id="asset_alias_example_alias_producer",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["producer", "asset-alias"],
):

    @task(outlets=[AssetAlias("example-alias")])
    def produce_asset_events_through_asset_alias(*, outlet_events=None):
        bucket_name = "bucket"
        object_path = "my-task"
        outlet_events["example-alias"].add(Dataset(f"s3://{bucket_name}/{object_path}"))

    produce_asset_events_through_asset_alias()

with DAG(
    dag_id="asset_s3_bucket_consumer",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    schedule=[Dataset("s3://bucket/my-task")],
    catchup=False,
    tags=["consumer", "asset"],
):

    @task
    def consume_asset_event():
        pass

    consume_asset_event()

with DAG(
    dag_id="asset_alias_example_alias_consumer",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    schedule=[AssetAlias("example-alias")],
    catchup=False,
    tags=["consumer", "asset-alias"],
):

    @task(inlets=[AssetAlias("example-alias")])
    def consume_asset_event_from_asset_alias(*, inlet_events=None):
        for event in inlet_events[AssetAlias("example-alias")]:
            print(event)

    consume_asset_event_from_asset_alias()
