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
"""Example DAG demonstrating the usage of the ``AssetEventSensor``."""

from __future__ import annotations

from typing import Any

from airflow.providers.common.compat.sdk import AirflowOptionalProviderFeatureException
from airflow.providers.standard.version_compat import AIRFLOW_V_3_4_PLUS

if not AIRFLOW_V_3_4_PLUS:
    raise AirflowOptionalProviderFeatureException("AssetEventSensor needs Airflow 3.4+.")

import pendulum

from airflow.providers.standard.sensors.asset import AssetEventSensor
from airflow.sdk import DAG, Asset

my_asset = Asset("s3://bucket/my-file.csv")


# [START example_asset_event_process_result]
def latest_event_only(events: list[Any]) -> list[Any]:
    """Keep only the most recent event (must be a top-level importable function in deferrable mode)."""
    return events[-1:]


# [END example_asset_event_process_result]


with DAG(
    dag_id="example_asset_sensor",
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["example"],
) as dag:
    # [START example_asset_event_sensor]
    wait_for_asset_event = AssetEventSensor(
        task_id="wait_for_asset_event",
        asset=my_asset,
    )
    # [END example_asset_event_sensor]

    # [START example_asset_event_sensor_filtered]
    wait_for_partition_events = AssetEventSensor(
        task_id="wait_for_partition_events",
        asset=my_asset,
        partition_key="2021-01-01",
        expected_count=3,
    )
    # [END example_asset_event_sensor_filtered]

    # [START example_asset_event_sensor_async]
    wait_for_asset_event_async = AssetEventSensor(
        task_id="wait_for_asset_event_async",
        asset=my_asset,
        process_result=latest_event_only,
        deferrable=True,
    )
    # [END example_asset_event_sensor_async]

    [wait_for_asset_event, wait_for_partition_events, wait_for_asset_event_async]
