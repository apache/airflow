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
Example DAG for demonstrating the behavior of the Datasets feature in Airflow, including conditional and
dataset expression-based scheduling.

Notes on usage:

Turn on all the DAGs.

dataset_produces_1 is scheduled to run daily. Once it completes, it triggers several DAGs due to its dataset
being updated. dataset_consumes_1 is triggered immediately, as it depends solely on the dataset produced by
dataset_produces_1. consume_1_or_2_with_dataset_expressions will also be triggered, as its condition of
either dataset_produces_1 or dataset_produces_2 being updated is satisfied with dataset_produces_1.

dataset_consumes_1_and_2 will not be triggered after dataset_produces_1 runs because it requires the dataset
from dataset_produces_2, which has no schedule and must be manually triggered.

After manually triggering dataset_produces_2, several DAGs will be affected. dataset_consumes_1_and_2 should
run because both its dataset dependencies are now met. consume_1_and_2_with_dataset_expressions will be
triggered, as it requires both dataset_produces_1 and dataset_produces_2 datasets to be updated.
consume_1_or_2_with_dataset_expressions will be triggered again, since it's conditionally set to run when
either dataset is updated.

consume_1_or_both_2_and_3_with_dataset_expressions demonstrates complex dataset dependency logic.
This DAG triggers if dataset_produces_1 is updated or if both dataset_produces_2 and dag3_dataset
are updated. This example highlights the capability to combine updates from multiple datasets with logical
expressions for advanced scheduling.

conditional_dataset_and_time_based_timetable illustrates the integration of time-based scheduling with
dataset dependencies. This DAG is configured to execute either when both dataset_produces_1 and
dataset_produces_2 datasets have been updated or according to a specific cron schedule, showcasing
Airflow's versatility in handling mixed triggers for dataset and time-based scheduling.

The DAGs dataset_consumes_1_never_scheduled and dataset_consumes_unknown_never_scheduled will not run
automatically as they depend on datasets that do not get updated or are not produced by any scheduled tasks.
"""

from __future__ import annotations

import pendulum

from airflow.datasets import Dataset
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.timetables.datasets import DatasetOrTimeSchedule
from airflow.timetables.trigger import CronTriggerTimetable

# [START dataset_def]
dag1_dataset = Dataset("s3://dag1/output_1.txt", extra={"hi": "bye"})
# [END dataset_def]
dag2_dataset = Dataset("s3://dag2/output_1.txt", extra={"hi": "bye"})
dag3_dataset = Dataset("s3://dag3/output_3.txt", extra={"hi": "bye"})

with DAG(
    dag_id="dataset_produces_1",
    catchup=False,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    schedule="@daily",
    tags=["produces", "dataset-scheduled"],
) as dag1:
    # [START task_outlet]
    BashOperator(outlets=[dag1_dataset], task_id="producing_task_1", bash_command="sleep 5")
    # [END task_outlet]

with DAG(
    dag_id="dataset_produces_2",
    catchup=False,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    schedule=None,
    tags=["produces", "dataset-scheduled"],
) as dag2:
    BashOperator(outlets=[dag2_dataset], task_id="producing_task_2", bash_command="sleep 5")

# [START dag_dep]
with DAG(
    dag_id="dataset_consumes_1",
    catchup=False,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    schedule=[dag1_dataset],
    tags=["consumes", "dataset-scheduled"],
) as dag3:
    # [END dag_dep]
    BashOperator(
        outlets=[Dataset("s3://consuming_1_task/dataset_other.txt")],
        task_id="consuming_1",
        bash_command="sleep 5",
    )

with DAG(
    dag_id="dataset_consumes_1_and_2",
    catchup=False,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    schedule=[dag1_dataset, dag2_dataset],
    tags=["consumes", "dataset-scheduled"],
) as dag4:
    BashOperator(
        outlets=[Dataset("s3://consuming_2_task/dataset_other_unknown.txt")],
        task_id="consuming_2",
        bash_command="sleep 5",
    )

with DAG(
    dag_id="dataset_consumes_1_never_scheduled",
    catchup=False,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    schedule=[
        dag1_dataset,
        Dataset("s3://unrelated/this-dataset-doesnt-get-triggered"),
    ],
    tags=["consumes", "dataset-scheduled"],
) as dag5:
    BashOperator(
        outlets=[Dataset("s3://consuming_2_task/dataset_other_unknown.txt")],
        task_id="consuming_3",
        bash_command="sleep 5",
    )

with DAG(
    dag_id="dataset_consumes_unknown_never_scheduled",
    catchup=False,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    schedule=[
        Dataset("s3://unrelated/dataset3.txt"),
        Dataset("s3://unrelated/dataset_other_unknown.txt"),
    ],
    tags=["dataset-scheduled"],
) as dag6:
    BashOperator(
        task_id="unrelated_task",
        outlets=[Dataset("s3://unrelated_task/dataset_other_unknown.txt")],
        bash_command="sleep 5",
    )

with DAG(
    dag_id="consume_1_and_2_with_dataset_expressions",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    schedule=(dag1_dataset & dag2_dataset),
) as dag5:
    BashOperator(
        outlets=[Dataset("s3://consuming_2_task/dataset_other_unknown.txt")],
        task_id="consume_1_and_2_with_dataset_expressions",
        bash_command="sleep 5",
    )
with DAG(
    dag_id="consume_1_or_2_with_dataset_expressions",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    schedule=(dag1_dataset | dag2_dataset),
) as dag6:
    BashOperator(
        outlets=[Dataset("s3://consuming_2_task/dataset_other_unknown.txt")],
        task_id="consume_1_or_2_with_dataset_expressions",
        bash_command="sleep 5",
    )
with DAG(
    dag_id="consume_1_or_both_2_and_3_with_dataset_expressions",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    schedule=(dag1_dataset | (dag2_dataset & dag3_dataset)),
) as dag7:
    BashOperator(
        outlets=[Dataset("s3://consuming_2_task/dataset_other_unknown.txt")],
        task_id="consume_1_or_both_2_and_3_with_dataset_expressions",
        bash_command="sleep 5",
    )
with DAG(
    dag_id="conditional_dataset_and_time_based_timetable",
    catchup=False,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    schedule=DatasetOrTimeSchedule(
        timetable=CronTriggerTimetable("0 1 * * 3", timezone="UTC"), datasets=(dag1_dataset & dag2_dataset)
    ),
    tags=["dataset-time-based-timetable"],
) as dag8:
    BashOperator(
        outlets=[Dataset("s3://dataset_time_based/dataset_other_unknown.txt")],
        task_id="conditional_dataset_and_time_based_timetable",
        bash_command="sleep 5",
    )
