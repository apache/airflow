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
Example DAG for demonstrating the behavior of the DatasetAlias feature in Airflow, including conditional and
dataset expression-based scheduling.

Notes on usage:

Turn on all the DAGs.

Before running any DAG, the schedule of the "dataset_alias_example_alias_consumer" DAG will show as "Unresolved DatasetAlias".
This is expected because the dataset alias has not been resolved into any dataset yet.

Once the "dataset_s3_bucket_producer" DAG is triggered, the "dataset_s3_bucket_consumer" DAG should be triggered upon completion.
This is because the dataset alias "example-alias" is used to add a dataset event to the dataset "s3://bucket/my-task"
during the "produce_dataset_events_through_dataset_alias" task.
As the DAG "dataset-alias-consumer" relies on dataset alias "example-alias" which was previously unresolved,
the DAG "dataset-alias-consumer" (along with all the DAGs in the same file) will be re-parsed and
thus update its schedule to the dataset "s3://bucket/my-task" and will also be triggered.
"""

from __future__ import annotations

import pendulum

from airflow import DAG
from airflow.assets import Dataset, DatasetAlias
from airflow.decorators import task

with DAG(
    dag_id="dataset_s3_bucket_producer",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["producer", "dataset"],
):

    @task(outlets=[Dataset("s3://bucket/my-task")])
    def produce_dataset_events():
        pass

    produce_dataset_events()

with DAG(
    dag_id="dataset_alias_example_alias_producer",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["producer", "dataset-alias"],
):

    @task(outlets=[DatasetAlias("example-alias")])
    def produce_dataset_events_through_dataset_alias(*, outlet_events=None):
        bucket_name = "bucket"
        object_path = "my-task"
        outlet_events["example-alias"].add(Dataset(f"s3://{bucket_name}/{object_path}"))

    produce_dataset_events_through_dataset_alias()

with DAG(
    dag_id="dataset_s3_bucket_consumer",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    schedule=[Dataset("s3://bucket/my-task")],
    catchup=False,
    tags=["consumer", "dataset"],
):

    @task
    def consume_dataset_event():
        pass

    consume_dataset_event()

with DAG(
    dag_id="dataset_alias_example_alias_consumer",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    schedule=[DatasetAlias("example-alias")],
    catchup=False,
    tags=["consumer", "dataset-alias"],
):

    @task(inlets=[DatasetAlias("example-alias")])
    def consume_dataset_event_from_dataset_alias(*, inlet_events=None):
        for event in inlet_events[DatasetAlias("example-alias")]:
            print(event)

    consume_dataset_event_from_dataset_alias()
