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

"""Example DAG demonstrating the usage of the AirflowFlyteOperator."""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.flyte.operators.flyte import AirflowFlyteOperator
from airflow.providers.flyte.sensors.flyte import AirflowFlyteSensor

with DAG(
    dag_id="example_flyte_operator",
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    dagrun_timeout=timedelta(minutes=60),
    tags=["example"],
    catchup=False,
) as dag:

    # [START howto_operator_flyte_synchronous]
    sync_predictions = AirflowFlyteOperator(
        task_id="flyte_example_sync",
        flyte_conn_id="flyte_conn_example",
        project="flytesnacks",
        domain="development",
        launchplan_name="core.basic.lp.my_wf",
        assumable_iam_role="default",
        kubernetes_service_account="demo",
        version="v1",
        inputs={"val": 19},
        timeout=timedelta(seconds=3600),
        asynchronous=False,
    )
    # [END howto_operator_flyte_synchronous]

    # [START howto_operator_flyte_asynchronous]
    async_predictions = AirflowFlyteOperator(
        task_id="flyte_example_async",
        flyte_conn_id="flyte_conn_example",
        project="flytesnacks",
        domain="development",
        launchplan_name="core.basic.lp.my_wf",
        max_parallelism=2,
        raw_data_prefix="s3://flyte-demo/raw_data",
        assumable_iam_role="default",
        kubernetes_service_account="demo",
        version="v1",
        inputs={"val": 19},
        timeout=timedelta(seconds=3600),
        asynchronous=True,
    )

    predictions_sensor = AirflowFlyteSensor(
        task_id="predictions_sensor",
        execution_name=async_predictions.output,
        project="flytesnacks",
        domain="development",
        flyte_conn_id="flyte_conn_example",
    )
    # [END howto_operator_flyte_asynchronous]

    # Task dependency created via `XComArgs`:
    async_predictions >> predictions_sensor
