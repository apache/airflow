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

import os
from datetime import datetime

from airflow import DAG
from airflow.models.baseoperator import chain
from airflow.providers.amazon.aws.operators.quicksight import QuickSightCreateIngestionOperator
from airflow.providers.amazon.aws.sensors.quicksight import QuickSightSensor

DATA_SET_ID = os.getenv("DATA_SET_ID", "data-set-id")
INGESTION_ID = os.getenv("INGESTION_ID", "ingestion-id")

with DAG(
    dag_id="example_quicksight",
    start_date=datetime(2021, 1, 1),
    tags=["example"],
    catchup=False,
) as dag:
    # Create and Start the QuickSight SPICE data ingestion
    # and does not wait for its completion
    # [START howto_operator_quicksight_create_ingestion]
    quicksight_create_ingestion_no_waiting = QuickSightCreateIngestionOperator(
        task_id="quicksight_create_ingestion_no_waiting",
        data_set_id=DATA_SET_ID,
        ingestion_id=INGESTION_ID,
        wait_for_completion=False,
    )
    # [END howto_operator_quicksight_create_ingestion]

    # The following task checks the status of the QuickSight SPICE ingestion
    # job until it succeeds.
    # [START howto_sensor_quicksight]
    quicksight_job_status = QuickSightSensor(
        task_id="quicksight_job_status",
        data_set_id=DATA_SET_ID,
        ingestion_id=INGESTION_ID,
    )
    # [END howto_sensor_quicksight]

    chain(quicksight_create_ingestion_no_waiting, quicksight_job_status)
