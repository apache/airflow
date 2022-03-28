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
"""
This is an example DAG which uses the DatabricksSubmitRunOperator.
In this example, we create two tasks which execute sequentially.
The first task is to run a notebook at the workspace path "/test"
and the second task is to run a JAR uploaded to DBFS. Both,
tasks use new clusters.

Because we have set a downstream dependency on the notebook task,
the spark jar task will NOT run until the notebook task completes
successfully.

The definition of a successful run is if the run has a result_state of "SUCCESS".
For more information about the state of a run refer to
https://docs.databricks.com/api/latest/jobs.html#runstate
"""

from datetime import datetime

from airflow import DAG
from airflow.providers.delta_sharing.sensors.delta_sharing import DeltaSharingSensor

default_args = {
    'owner': 'airflow',
    'delta_sharing_conn_id': 'demo_share',
}

with DAG(
    dag_id='example_delta_sharing',
    schedule_interval='@daily',
    start_date=datetime(2021, 1, 1),
    default_args=default_args,
    tags=['example'],
    catchup=False,
) as dag:
    # [START howto_delta_sharing_sensor]
    # Example of using Delta Sharing Sensor to wait for changes in the table.

    check_nytaxi = DeltaSharingSensor(
        task_id='check_nytaxi',
        share="delta_sharing",
        schema="default",
        table="nyctaxi_2019",
        timeout=60,
        #    mode='reschedule',
    )
    # [END howto_delta_sharing_sensor]
