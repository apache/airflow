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
Example DAG for demonstrating the behavior of the Assets feature in Airflow, including conditional and
asset expression-based scheduling.

Notes on usage:

Turn on all the DAGs.

asset_produces_1 is scheduled to run daily. Once it completes, it triggers several DAGs due to its asset
being updated. asset_consumes_1 is triggered immediately, as it depends solely on the asset produced by
asset_produces_1. consume_1_or_2_with_asset_expressions will also be triggered, as its condition of
either asset_produces_1 or asset_produces_2 being updated is satisfied with asset_produces_1.

asset_consumes_1_and_2 will not be triggered after asset_produces_1 runs because it requires the asset
from asset_produces_2, which has no schedule and must be manually triggered.

After manually triggering asset_produces_2, several DAGs will be affected. asset_consumes_1_and_2 should
run because both its asset dependencies are now met. consume_1_and_2_with_asset_expressions will be
triggered, as it requires both asset_produces_1 and asset_produces_2 assets to be updated.
consume_1_or_2_with_asset_expressions will be triggered again, since it's conditionally set to run when
either asset is updated.

consume_1_or_both_2_and_3_with_asset_expressions demonstrates complex asset dependency logic.
This DAG triggers if asset_produces_1 is updated or if both asset_produces_2 and dag3_asset
are updated. This example highlights the capability to combine updates from multiple assets with logical
expressions for advanced scheduling.

conditional_asset_and_time_based_timetable illustrates the integration of time-based scheduling with
asset dependencies. This DAG is configured to execute either when both asset_produces_1 and
asset_produces_2 assets have been updated or according to a specific cron schedule, showcasing
Airflow's versatility in handling mixed triggers for asset and time-based scheduling.

The DAGs asset_consumes_1_never_scheduled and asset_consumes_unknown_never_scheduled will not run
automatically as they depend on assets that do not get updated or are not produced by any scheduled tasks.
"""

from __future__ import annotations

import pendulum

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, Asset
from airflow.timetables.assets import AssetOrTimeSchedule
from airflow.timetables.trigger import CronTriggerTimetable

# [START asset_def]
dag1_asset = Asset("s3://dag1/output_1.txt", extra={"hi": "bye"})
# [END asset_def]
dag2_asset = Asset("s3://dag2/output_1.txt", extra={"hi": "bye"})
dag3_asset = Asset("s3://dag3/output_3.txt", extra={"hi": "bye"})

with DAG(
    dag_id="asset_produces_1",
    catchup=False,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    schedule="@daily",
    tags=["produces", "asset-scheduled"],
) as dag1:
    # [START task_outlet]
    BashOperator(outlets=[dag1_asset], task_id="producing_task_1", bash_command="sleep 5")
    # [END task_outlet]

with DAG(
    dag_id="asset_produces_2",
    catchup=False,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    schedule=None,
    tags=["produces", "asset-scheduled"],
) as dag2:
    BashOperator(outlets=[dag2_asset], task_id="producing_task_2", bash_command="sleep 5")

# [START dag_dep]
with DAG(
    dag_id="asset_consumes_1",
    catchup=False,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    schedule=[dag1_asset],
    tags=["consumes", "asset-scheduled"],
) as dag3:
    # [END dag_dep]
    BashOperator(
        outlets=[Asset("s3://consuming_1_task/asset_other.txt")],
        task_id="consuming_1",
        bash_command="sleep 5",
    )

with DAG(
    dag_id="asset_consumes_1_and_2",
    catchup=False,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    schedule=[dag1_asset, dag2_asset],
    tags=["consumes", "asset-scheduled"],
) as dag4:
    BashOperator(
        outlets=[Asset("s3://consuming_2_task/asset_other_unknown.txt")],
        task_id="consuming_2",
        bash_command="sleep 5",
    )

with DAG(
    dag_id="asset_consumes_1_never_scheduled",
    catchup=False,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    schedule=[
        dag1_asset,
        Asset("s3://unrelated/this-asset-doesnt-get-triggered"),
    ],
    tags=["consumes", "asset-scheduled"],
) as dag5:
    BashOperator(
        outlets=[Asset("s3://consuming_2_task/asset_other_unknown.txt")],
        task_id="consuming_3",
        bash_command="sleep 5",
    )

with DAG(
    dag_id="asset_consumes_unknown_never_scheduled",
    catchup=False,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    schedule=[
        Asset("s3://unrelated/asset3.txt"),
        Asset("s3://unrelated/asset_other_unknown.txt"),
    ],
    tags=["asset-scheduled"],
) as dag6:
    BashOperator(
        task_id="unrelated_task",
        outlets=[Asset("s3://unrelated_task/asset_other_unknown.txt")],
        bash_command="sleep 5",
    )

with DAG(
    dag_id="consume_1_and_2_with_asset_expressions",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    schedule=(dag1_asset & dag2_asset),
) as dag5:
    BashOperator(
        outlets=[Asset("s3://consuming_2_task/asset_other_unknown.txt")],
        task_id="consume_1_and_2_with_asset_expressions",
        bash_command="sleep 5",
    )
with DAG(
    dag_id="consume_1_or_2_with_asset_expressions",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    schedule=(dag1_asset | dag2_asset),
) as dag6:
    BashOperator(
        outlets=[Asset("s3://consuming_2_task/asset_other_unknown.txt")],
        task_id="consume_1_or_2_with_asset_expressions",
        bash_command="sleep 5",
    )
with DAG(
    dag_id="consume_1_or_both_2_and_3_with_asset_expressions",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    schedule=(dag1_asset | (dag2_asset & dag3_asset)),
) as dag7:
    BashOperator(
        outlets=[Asset("s3://consuming_2_task/asset_other_unknown.txt")],
        task_id="consume_1_or_both_2_and_3_with_asset_expressions",
        bash_command="sleep 5",
    )
with DAG(
    dag_id="conditional_asset_and_time_based_timetable",
    catchup=False,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    schedule=AssetOrTimeSchedule(
        timetable=CronTriggerTimetable("0 1 * * 3", timezone="UTC"), assets=(dag1_asset & dag2_asset)
    ),
    tags=["asset-time-based-timetable"],
) as dag8:
    BashOperator(
        outlets=[Asset("s3://asset_time_based/asset_other_unknown.txt")],
        task_id="conditional_asset_and_time_based_timetable",
        bash_command="sleep 5",
    )
