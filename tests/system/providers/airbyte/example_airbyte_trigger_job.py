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

"""Example DAG demonstrating the usage of the AirbyteTriggerSyncOperator."""

import os
from datetime import datetime, timedelta
from typing import cast

from airflow import DAG
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from airflow.providers.airbyte.sensors.airbyte import AirbyteJobSensor

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
DAG_ID = "example_airbyte_operator"
CONN_ID = '15bc3800-82e4-48c3-a32d-620661273f28'

with DAG(
    dag_id=DAG_ID,
    schedule=None,
    start_date=datetime(2021, 1, 1),
    dagrun_timeout=timedelta(minutes=60),
    tags=['example'],
    catchup=False,
) as dag:

    # [START howto_operator_airbyte_synchronous]
    sync_source_destination = AirbyteTriggerSyncOperator(
        task_id='airbyte_sync_source_dest_example',
        connection_id=CONN_ID,
    )
    # [END howto_operator_airbyte_synchronous]

    # [START howto_operator_airbyte_asynchronous]
    async_source_destination = AirbyteTriggerSyncOperator(
        task_id='airbyte_async_source_dest_example',
        connection_id=CONN_ID,
        asynchronous=True,
    )

    airbyte_sensor = AirbyteJobSensor(
        task_id='airbyte_sensor_source_dest_example',
        airbyte_job_id=cast(int, async_source_destination.output),
    )
    # [END howto_operator_airbyte_asynchronous]

    # Task dependency created via `XComArgs`:
    #   async_source_destination >> airbyte_sensor

from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
