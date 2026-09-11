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
from __future__ import annotations

from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.sensors.asset import AssetPartitionSensor
from airflow.sdk import DAG, Asset, timezone

hourly_sales = Asset(uri="s3://warehouse/sales/hourly", name="hourly_sales")

with DAG(
    dag_id="example_asset_partition_sensor",
    start_date=timezone.datetime(2026, 1, 1),
    schedule="@hourly",
    catchup=False,
    tags=["example", "asset", "partition"],
) as dag:
    # [START howto_sensor_asset_partition]
    # ``after`` scopes the lookup to this run's interval. Without it, an event carrying the same
    # partition key from an earlier run would satisfy the wait — harmless for keys that are unique
    # per event (a timestamp), but wrong for keys that repeat (a region code).
    wait_for_hourly_partition = AssetPartitionSensor(
        task_id="wait_for_hourly_partition",
        asset=hourly_sales,
        partition_key="{{ data_interval_start.strftime('%Y-%m-%dT%H') }}",
        after="{{ data_interval_start }}",
        deferrable=True,
    )
    # [END howto_sensor_asset_partition]

    summarize = EmptyOperator(task_id="summarize_hourly_sales")

    wait_for_hourly_partition >> summarize
